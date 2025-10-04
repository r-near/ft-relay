use anyhow::{anyhow, Result};
use redis::{
    aio::ConnectionManager,
    streams::{
        StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamReadOptions, StreamReadReply,
    },
    AsyncCommands, Client, Value,
};
use tokio::sync::Mutex;

use crate::{config::RedisSettings, types::TransferReq};

#[derive(Clone)]
pub struct RedisQueue {
    client: Client,
    conn: std::sync::Arc<Mutex<ConnectionManager>>,
    settings: RedisSettings,
}

impl RedisQueue {
    pub async fn new(settings: RedisSettings) -> Result<Self> {
        let client = Client::open(settings.url.clone())?;
        let mut init_conn = client.get_multiplexed_async_connection().await?;

        // Create consumer group if it doesn't exist
        let group_res: Result<(), redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&settings.stream_key)
            .arg(&settings.consumer_group)
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
            conn: std::sync::Arc::new(Mutex::new(manager)),
            settings,
        })
    }

    pub async fn push(&self, req: &TransferReq) -> Result<()> {
        let payload = serde_json::to_string(req)?;
        let mut conn = self.conn.lock().await;
        redis::cmd("XADD")
            .arg(self.stream_key())
            .arg("*")
            .arg("payload")
            .arg(payload)
            .query_async::<()>(&mut *conn)
            .await?;
        Ok(())
    }

    pub async fn pop_batch(
        &self,
        consumer_name: &str,
        count: usize,
        linger_ms: u64,
    ) -> Result<Vec<(String, TransferReq)>> {
        use std::time::{Duration, Instant};

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut collected = Vec::new();
        let linger = Duration::from_millis(linger_ms);
        let mut deadline = Instant::now() + linger;

        loop {
            let remaining = count.saturating_sub(collected.len());
            if remaining == 0 {
                break;
            }

            let mut opts = StreamReadOptions::default()
                .count(remaining)
                .group(self.consumer_group(), consumer_name);

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

            let reply: StreamReadReply = conn
                .xread_options(&[self.stream_key()], &[">"], &opts)
                .await?;

            let batch = decode_read_reply(reply)?;

            if batch.is_empty() {
                if collected.is_empty() {
                    continue;
                }
                break;
            }

            collected.extend(batch);

            if linger_ms == 0 {
                break;
            }

            deadline = Instant::now() + linger;
        }

        Ok(collected)
    }

    pub async fn reclaim_stale(
        &self,
        consumer_name: &str,
        min_idle_ms: u64,
        count: usize,
    ) -> Result<Vec<(String, TransferReq)>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut cursor = "0-0".to_string();
        let mut claimed = Vec::new();

        loop {
            let opts = StreamAutoClaimOptions::default().count(count);
            let reply: StreamAutoClaimReply = conn
                .xautoclaim_options(
                    self.stream_key(),
                    self.consumer_group(),
                    consumer_name,
                    min_idle_ms,
                    &cursor,
                    opts,
                )
                .await?;

            cursor = reply.next_stream_id;

            decode_entries(reply.claimed, &mut claimed)?;

            if cursor == "0-0" {
                break;
            }
        }

        Ok(claimed)
    }

    pub async fn ack(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().await;
        conn.xack::<_, _, _, ()>(self.stream_key(), self.consumer_group(), ids)
            .await?;
        Ok(())
    }

    pub async fn retry(&self, id: &str, transfer: &TransferReq) -> Result<()> {
        let payload = serde_json::to_string(transfer)?;
        let mut conn = self.conn.lock().await;

        redis::pipe()
            .atomic()
            .cmd("XADD")
            .arg(self.stream_key())
            .arg("*")
            .arg("payload")
            .arg(payload)
            .ignore()
            .cmd("XACK")
            .arg(self.stream_key())
            .arg(self.consumer_group())
            .arg(id)
            .ignore()
            .query_async::<()>(&mut *conn)
            .await?;

        Ok(())
    }

    pub async fn set_status(&self, transfer_ids: &[String], tx_hash: &str) -> Result<()> {
        if transfer_ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().await;
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

    pub async fn get_status(&self, transfer_id: &str) -> Result<Option<String>> {
        let key = format!("ftrelay:transfer:{}", transfer_id);
        let mut conn = self.conn.lock().await;
        let tx_hash: Option<String> = conn.hget(&key, "tx_hash").await?;
        Ok(tx_hash)
    }

    pub fn redis_settings(&self) -> &RedisSettings {
        &self.settings
    }

    fn stream_key(&self) -> &str {
        &self.settings.stream_key
    }

    fn consumer_group(&self) -> &str {
        &self.settings.consumer_group
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

fn decode_read_reply(reply: StreamReadReply) -> Result<Vec<(String, TransferReq)>> {
    let mut out = Vec::new();
    for key in reply.keys {
        decode_entries(key.ids, &mut out)?;
    }
    Ok(out)
}

fn decode_entries(entries: Vec<StreamId>, out: &mut Vec<(String, TransferReq)>) -> Result<()> {
    for entry in entries {
        if let Some(value) = entry.map.get("payload") {
            let transfer = decode_transfer(value)?;
            out.push((entry.id.clone(), transfer));
        }
    }
    Ok(())
}
