use anyhow::Result;
use log::{info, warn};
use redis::aio::ConnectionManager;
use std::sync::Arc;
use uuid::Uuid;

use crate::access_key_pool::AccessKeyPool;
use crate::nonce_manager::NonceManager;
use crate::redis_helpers as rh;
use crate::rpc_client::NearRpcClient;
use crate::types::{AccountId, Event, Status, TransferMessage};

const MAX_RETRIES: u32 = 10;
const MAX_BATCH_SIZE: usize = 100;

pub struct TransferWorkerRuntime {
    pub redis_conn: ConnectionManager,
    pub rpc_client: Arc<NearRpcClient>,
    pub access_key_pool: Arc<AccessKeyPool>,
    pub nonce_manager: NonceManager,
    pub relay_account: AccountId,
    pub token: AccountId,
    pub env: String,
}

pub struct TransferWorkerContext {
    pub runtime: Arc<TransferWorkerRuntime>,
    pub linger_ms: u64,
}

pub async fn transfer_worker_loop(ctx: TransferWorkerContext) -> Result<()> {
    let consumer_name = format!("transfer-{}", Uuid::new_v4());
    let stream_key = format!("ftrelay:{}:xfer", ctx.runtime.env);
    let consumer_group = format!("ftrelay:{}:xfer_workers", ctx.runtime.env);

    let mut conn = ctx.runtime.redis_conn.clone();
    let _: Result<String, _> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream_key)
        .arg(&consumer_group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await;

    info!("Transfer worker {} started", consumer_name);

    loop {
        let mut conn = ctx.runtime.redis_conn.clone();

        let result: Result<redis::streams::StreamReadReply, _> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&consumer_group)
            .arg(&consumer_name)
            .arg("COUNT")
            .arg(MAX_BATCH_SIZE)
            .arg("BLOCK")
            .arg(ctx.linger_ms as usize)
            .arg("STREAMS")
            .arg(&stream_key)
            .arg(">")
            .query_async(&mut conn)
            .await;

        let reply = match result {
            Ok(r) => r,
            Err(_) => continue,
        };

        let mut batch = Vec::new();

        for stream in reply.keys {
            for id_data in stream.ids {
                let stream_id = id_data.id.clone();

                let data = match id_data.map.get("data") {
                    Some(redis::Value::BulkString(bytes)) => match std::str::from_utf8(bytes) {
                        Ok(s) => s,
                        Err(_) => continue,
                    },
                    _ => continue,
                };

                let msg: TransferMessage = match serde_json::from_str(data) {
                    Ok(m) => m,
                    Err(_) => {
                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(&stream_key)
                            .arg(&consumer_group)
                            .arg(&stream_id)
                            .query_async(&mut conn)
                            .await;
                        continue;
                    }
                };

                batch.push((stream_id, msg));
            }
        }

        if batch.is_empty() {
            continue;
        }

        process_batch(&ctx, batch).await?;
    }
}

async fn process_batch(
    ctx: &TransferWorkerContext,
    batch: Vec<(String, TransferMessage)>,
) -> Result<()> {
    let mut conn = ctx.runtime.redis_conn.clone();
    let stream_key = format!("ftrelay:{}:xfer", ctx.runtime.env);
    let consumer_group = format!("ftrelay:{}:xfer_workers", ctx.runtime.env);

    let mut transfers = Vec::new();
    for (stream_id, msg) in &batch {
        match rh::get_transfer_state(&mut conn, &msg.transfer_id).await? {
            Some(transfer) => transfers.push((stream_id.clone(), msg.clone(), transfer)),
            None => {
                warn!("Transfer {} not found", msg.transfer_id);
                let _: Result<(), _> = redis::cmd("XACK")
                    .arg(&stream_key)
                    .arg(&consumer_group)
                    .arg(stream_id)
                    .query_async(&mut conn)
                    .await;
            }
        }
    }

    if transfers.is_empty() {
        return Ok(());
    }

    info!("Processing batch of {} transfers", transfers.len());

    let leased_key = match ctx.runtime.access_key_pool.lease().await {
        Ok(key) => key,
        Err(e) => {
            warn!("Failed to lease access key: {:?}, will retry", e);
            for (stream_id, msg, _) in &transfers {
                let _: Result<(), _> = redis::cmd("XACK")
                    .arg(&stream_key)
                    .arg(&consumer_group)
                    .arg(stream_id)
                    .query_async(&mut conn)
                    .await;

                let retry_count = msg.retry_count + 1;
                if retry_count < MAX_RETRIES {
                    let _ = rh::enqueue_transfer(
                        &mut conn,
                        &ctx.runtime.env,
                        &msg.transfer_id,
                        retry_count,
                    )
                    .await;
                }
            }
            return Ok(());
        }
    };

    let nonce = ctx
        .runtime
        .nonce_manager
        .clone()
        .get_next_nonce(&leased_key.key_id)
        .await?;

    let mut receivers = Vec::new();
    for (_, _, transfer) in &transfers {
        receivers.push((transfer.receiver_id.clone(), transfer.amount.clone()));
    }

    let result = ctx
        .runtime
        .rpc_client
        .submit_batch_transfer(
            &ctx.runtime.relay_account,
            &ctx.runtime.token,
            receivers,
            &leased_key.secret_key,
            nonce,
        )
        .await;

    drop(leased_key);

    match result {
        Ok(tx_hash) => {
            let tx_hash_str = tx_hash.to_string();
            info!(
                "Submitted batch of {} transfers, tx: {}",
                transfers.len(),
                tx_hash_str
            );

            for (stream_id, msg, _) in &transfers {
                rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Submitted).await?;
                rh::update_tx_hash(&mut conn, &msg.transfer_id, &tx_hash_str).await?;
                rh::log_event(
                    &mut conn,
                    &msg.transfer_id,
                    Event::new("SUBMITTED").with_tx_hash(tx_hash_str.clone()),
                )
                .await?;
                rh::enqueue_verification(&mut conn, &ctx.runtime.env, &msg.transfer_id, &tx_hash_str, 0)
                    .await?;
                rh::log_event(&mut conn, &msg.transfer_id, Event::new("QUEUED_VERIFICATION"))
                    .await?;

                let _: Result<(), _> = redis::cmd("XACK")
                    .arg(&stream_key)
                    .arg(&consumer_group)
                    .arg(stream_id)
                    .query_async(&mut conn)
                    .await;
            }

            Ok(())
        }
        Err(e) => {
            warn!("Failed to submit batch: {:?}", e);

            for (stream_id, msg, _) in &transfers {
                let _: Result<(), _> = redis::cmd("XACK")
                    .arg(&stream_key)
                    .arg(&consumer_group)
                    .arg(stream_id)
                    .query_async(&mut conn)
                    .await;

                let retry_count = msg.retry_count + 1;
                if retry_count < MAX_RETRIES {
                    let _ = rh::increment_retry_count(&mut conn, &msg.transfer_id).await;
                    let _ = rh::enqueue_transfer(
                        &mut conn,
                        &ctx.runtime.env,
                        &msg.transfer_id,
                        retry_count,
                    )
                    .await;
                } else {
                    let _ = rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Failed)
                        .await;
                    let _ = rh::log_event(
                        &mut conn,
                        &msg.transfer_id,
                        Event::new("FAILED").with_reason(format!(
                            "Submission failed after {} retries: {:?}",
                            retry_count, e
                        )),
                    )
                    .await;
                }
            }

            Ok(())
        }
    }
}
