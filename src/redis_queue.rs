use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use redis::{
    aio::{ConnectionManager, MultiplexedConnection},
    streams::{
        StreamAutoClaimOptions, StreamAutoClaimReply, StreamKey, StreamReadOptions, StreamReadReply,
    },
    AsyncCommands, Client, Value,
};
use tokio::sync::Mutex;

use crate::{config::RedisConfig, types::TransferReq};

#[derive(Clone)]
pub struct RedisContext {
    client: Client,
    general_conn: Arc<Mutex<ConnectionManager>>,
    pub stream_key: String,
    pub consumer_group: String,
}

impl RedisContext {
    pub async fn new(config: &RedisConfig) -> Result<Self> {
        let client = Client::open(config.url.clone())?;
        let mut init_conn = client.get_multiplexed_async_connection().await?;
        let group_res: Result<(), redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&config.stream_key)
            .arg(&config.consumer_group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut init_conn)
            .await;
        drop(init_conn);

        if let Err(err) = group_res {
            if err.code() != Some("BUSYGROUP") {
                return Err(err.into());
            }
        }

        let manager = ConnectionManager::new(client.clone()).await?;

        Ok(Self {
            client,
            general_conn: Arc::new(Mutex::new(manager)),
            stream_key: config.stream_key.clone(),
            consumer_group: config.consumer_group.clone(),
        })
    }

    pub async fn enqueue(&self, req: &TransferReq) -> Result<()> {
        let payload = serde_json::to_string(req)?;
        let mut conn = self.general_conn.lock().await;
        redis::cmd("XADD")
            .arg(&self.stream_key)
            .arg("*")
            .arg("payload")
            .arg(payload)
            .query_async::<()>(&mut *conn)
            .await?;
        Ok(())
    }

    pub async fn reopen_connection(&self) -> Result<MultiplexedConnection> {
        Ok(self.client.get_multiplexed_async_connection().await?)
    }

    pub async fn ack(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.general_conn.lock().await;
        conn.xack::<_, _, _, ()>(&self.stream_key, &self.consumer_group, ids)
            .await?;
        Ok(())
    }

    pub async fn requeue(&self, entries: Vec<StreamMessage>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut conn = self.general_conn.lock().await;
        let mut pipe = redis::pipe();
        pipe.atomic();
        for msg in entries {
            let mut req = msg.transfer;
            req.attempts += 1;
            let payload = serde_json::to_string(&req)?;
            pipe.cmd("XADD")
                .arg(&self.stream_key)
                .arg("*")
                .arg("payload")
                .arg(payload)
                .ignore();
            pipe.cmd("XACK")
                .arg(&self.stream_key)
                .arg(&self.consumer_group)
                .arg(&msg.redis_id)
                .ignore();
        }
        let _: () = pipe.query_async(&mut *conn).await?;
        Ok(())
    }

    pub fn parse_stream_read_reply(&self, reply: StreamReadReply) -> Result<Vec<StreamMessage>> {
        let mut out = Vec::new();
        for StreamKey { ids, .. } in reply.keys {
            for entry in ids {
                if let Some(value) = entry.map.get("payload") {
                    let transfer = decode_transfer(value)?;
                    out.push(StreamMessage {
                        redis_id: entry.id.clone(),
                        transfer,
                    });
                }
            }
        }
        Ok(out)
    }

    pub fn parse_auto_claim_reply(
        &self,
        reply: StreamAutoClaimReply,
    ) -> Result<(String, Vec<StreamMessage>)> {
        let mut out = Vec::new();
        for entry in reply.claimed {
            if let Some(value) = entry.map.get("payload") {
                let transfer = decode_transfer(value)?;
                out.push(StreamMessage {
                    redis_id: entry.id.clone(),
                    transfer,
                });
            }
        }
        Ok((reply.next_stream_id, out))
    }

    pub async fn set_transfer_status(
        &self,
        transfer_ids: &[String],
        tx_hash: &str,
    ) -> Result<()> {
        if transfer_ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.general_conn.lock().await;
        let mut pipe = redis::pipe();
        pipe.atomic();
        for transfer_id in transfer_ids {
            let key = format!("ftrelay:transfer:{}", transfer_id);
            pipe.cmd("HSET")
                .arg(&key)
                .arg("tx_hash")
                .arg(tx_hash)
                .ignore();
            pipe.cmd("EXPIRE")
                .arg(&key)
                .arg(86400) // 24 hours
                .ignore();
        }
        let _: () = pipe.query_async(&mut *conn).await?;
        Ok(())
    }

    pub async fn get_transfer_status(&self, transfer_id: &str) -> Result<Option<String>> {
        let key = format!("ftrelay:transfer:{}", transfer_id);
        let mut conn = self.general_conn.lock().await;
        let tx_hash: Option<String> = conn.hget(&key, "tx_hash").await?;
        Ok(tx_hash)
    }
}

#[derive(Debug, Clone)]
pub struct StreamMessage {
    pub redis_id: String,
    pub transfer: TransferReq,
}

pub struct RedisWorker {
    pub ctx: Arc<RedisContext>,
    pub consumer_name: String,
    pub connection: MultiplexedConnection,
}

impl RedisWorker {
    pub async fn new(ctx: Arc<RedisContext>, consumer_name: String) -> Result<Self> {
        let connection = ctx.reopen_connection().await?;
        Ok(Self {
            ctx,
            consumer_name,
            connection,
        })
    }

    pub async fn claim_stale(
        &mut self,
        min_idle_ms: u64,
        count: usize,
    ) -> Result<Vec<StreamMessage>> {
        let mut cursor = "0-0".to_string();
        let mut claimed = Vec::new();

        loop {
            let opts = StreamAutoClaimOptions::default().count(count);
            let reply: StreamAutoClaimReply = self
                .connection
                .xautoclaim_options(
                    &self.ctx.stream_key,
                    &self.ctx.consumer_group,
                    &self.consumer_name,
                    min_idle_ms,
                    &cursor,
                    opts,
                )
                .await?;

            let (next_cursor, entries) = self.ctx.parse_auto_claim_reply(reply)?;
            cursor = next_cursor;

            if entries.is_empty() {
                if cursor == "0-0" {
                    break;
                }
                continue;
            }

            claimed.extend(entries);

            if cursor == "0-0" {
                break;
            }
        }

        Ok(claimed)
    }

    pub async fn read_batch(
        &mut self,
        target_count: usize,
        linger_ms: u64,
    ) -> Result<Vec<StreamMessage>> {
        let linger = Duration::from_millis(linger_ms);
        let mut collected = Vec::new();
        let mut deadline = Instant::now() + linger;

        loop {
            let remaining = target_count.saturating_sub(collected.len());
            if remaining == 0 {
                break;
            }

            let mut opts = StreamReadOptions::default()
                .count(remaining)
                .group(&self.ctx.consumer_group, &self.consumer_name);

            let block_ms = if collected.is_empty() {
                linger_ms as usize
            } else if linger_ms == 0 {
                0
            } else {
                deadline
                    .checked_duration_since(Instant::now())
                    .map(|d| d.as_millis().min(usize::MAX as u128) as usize)
                    .unwrap_or(0)
            };

            if block_ms > 0 {
                opts = opts.block(block_ms);
            }

            let reply: StreamReadReply = self
                .connection
                .xread_options(&[&self.ctx.stream_key], &[">"], &opts)
                .await?;

            let mut batch = self.ctx.parse_stream_read_reply(reply)?;

            if batch.is_empty() {
                if collected.is_empty() {
                    continue;
                }
                break;
            }

            let fetched = batch.len();
            collected.append(&mut batch);
            log::debug!(
                "redis_worker collected {} transfers (total={}/{})",
                fetched,
                collected.len(),
                target_count
            );

            if linger_ms == 0 {
                break;
            }

            deadline = Instant::now() + linger;
        }

        Ok(collected)
    }
}

fn decode_transfer(value: &Value) -> Result<TransferReq> {
    match value {
        Value::BulkString(bytes) => Ok(serde_json::from_slice(bytes)?),
        Value::SimpleString(s) => Ok(serde_json::from_str(s)?),
        other => Err(anyhow!(
            "unexpected Redis payload type for transfer: {:?}",
            other
        )),
    }
}
