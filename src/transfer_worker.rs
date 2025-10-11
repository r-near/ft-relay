use anyhow::Result;
use log::{debug, info, warn};
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
        let loop_start = std::time::Instant::now();
        let mut conn = ctx.runtime.redis_conn.clone();

        let pop_start = std::time::Instant::now();
        let batch: Vec<(String, TransferMessage)> = match rh::pop_batch(
            &mut conn,
            &stream_key,
            &consumer_group,
            &consumer_name,
            MAX_BATCH_SIZE,
            ctx.linger_ms,
        )
        .await
        {
            Ok(b) => {
                let pop_duration = pop_start.elapsed();
                debug!("[METRIC] op=pop_batch duration_ms={} msg_count={}", pop_duration.as_millis(), b.len());
                debug!("[TIMING] pop_batch took {}ms for {} messages", pop_duration.as_millis(), b.len());
                b
            }
            Err(e) => {
                warn!("Error popping batch: {:?}", e);
                continue;
            }
        };

        if batch.is_empty() {
            continue;
        }

        let batch_process_start = std::time::Instant::now();
        process_batch(&ctx, &stream_key, &consumer_group, batch).await?;
        let batch_process_duration = batch_process_start.elapsed();
        let loop_duration = loop_start.elapsed();
        debug!("[METRIC] op=batch_process duration_ms={}", batch_process_duration.as_millis());
        debug!("[METRIC] op=loop_total duration_ms={}", loop_duration.as_millis());
    }
}

async fn process_batch(
    ctx: &TransferWorkerContext,
    stream_key: &str,
    consumer_group: &str,
    batch: Vec<(String, TransferMessage)>,
) -> Result<()> {
    let batch_start = std::time::Instant::now();
    debug!("[METRIC] op=process_batch_start batch_size={}", batch.len());
    let mut conn = ctx.runtime.redis_conn.clone();

    let fetch_start = std::time::Instant::now();
    
    // Use pipelining to fetch all transfer states in ONE request (10-100x faster!)
    let transfer_ids: Vec<String> = batch.iter().map(|(_, msg)| msg.transfer_id.clone()).collect();
    let states = rh::get_transfer_states_batch(&mut conn, &transfer_ids).await?;
    
    let mut transfers = Vec::new();
    for ((stream_id, msg), state) in batch.iter().zip(states.iter()) {
        match state {
            Some(transfer) => transfers.push((stream_id.clone(), msg.clone(), transfer.clone())),
            None => {
                warn!("Transfer {} not found", msg.transfer_id);
                let _ = rh::ack_message(&mut conn, stream_key, consumer_group, stream_id).await;
            }
        }
    }
    let fetch_duration = fetch_start.elapsed();
    debug!("[METRIC] op=fetch_states duration_ms={} count={}", fetch_duration.as_millis(), transfers.len());
    debug!("[TIMING] Fetched {} transfer states in {}ms (pipelined)", transfers.len(), fetch_duration.as_millis());

    if transfers.is_empty() {
        return Ok(());
    }

    debug!("Processing batch of {} transfers", transfers.len());
    let build_transfers_duration = batch_start.elapsed();
    debug!("[METRIC] op=build_transfers duration_ms={} count={}", build_transfers_duration.as_millis(), transfers.len());

    let lease_start = std::time::Instant::now();
    let leased_key = match ctx.runtime.access_key_pool.lease().await {
        Ok(key) => key,
        Err(e) => {
            warn!("Failed to lease access key: {:?}, will retry", e);
            for (stream_id, msg, _) in &transfers {
                let _ = rh::ack_message(&mut conn, stream_key, consumer_group, stream_id).await;

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

    let lease_duration = lease_start.elapsed();
    debug!("[METRIC] op=key_lease duration_ms={}", lease_duration.as_millis());
    debug!("[TIMING] Key lease took {}ms", lease_duration.as_millis());

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

    let rpc_start = std::time::Instant::now();
    let result = ctx
        .runtime
        .rpc_client
        .submit_batch_transfer_async(
            &ctx.runtime.relay_account,
            &ctx.runtime.token,
            receivers,
            &leased_key.secret_key,
            nonce,
        )
        .await;
    let rpc_duration = rpc_start.elapsed();
    debug!("[METRIC] op=rpc_broadcast duration_ms={} batch_size={}", rpc_duration.as_millis(), transfers.len());
    debug!("[TIMING] RPC broadcast took {}ms", rpc_duration.as_millis());

    drop(leased_key);

    match result {
        Ok(tx_hash) => {
            let tx_hash_str = tx_hash.to_string();
            info!(
                "Batch of {} transfers submitted, tx: {} (verification async)",
                transfers.len(),
                tx_hash_str
            );

            // Pipeline all Redis updates: mark submitted, set tx_hash, map tx->transfers,
            // enqueue verification, and ack messages.
            let verify_stream = format!("ftrelay:{}:verify", ctx.runtime.env);
            let mut pipe = redis::pipe();
            pipe.atomic();
            let now = chrono::Utc::now().to_rfc3339();

            for (stream_id, msg, _) in &transfers {
                let transfer_key = format!("transfer:{}", msg.transfer_id);
                let event_key = format!("transfer:{}:ev", msg.transfer_id);
                let tx_transfers_key = format!("tx:{}:transfers", tx_hash_str);

                // Update transfer status + timestamps
                pipe.hset(&transfer_key, "status", Status::Submitted.as_str());
                pipe.hset(&transfer_key, "updated_at", &now);
                // Update tx_hash
                pipe.hset(&transfer_key, "tx_hash", &tx_hash_str);

                // Map transfer to tx
                pipe.sadd(&tx_transfers_key, &msg.transfer_id);
                pipe.expire(&tx_transfers_key, 86400);

                // Events: SUBMITTED + EXPIRE
                let ev_submitted = serde_json::to_string(&Event::new("SUBMITTED").with_tx_hash(tx_hash_str.clone()))?;
                pipe.lpush(&event_key, ev_submitted);
                pipe.expire(&event_key, 86400);

                // Enqueue verification
                let verify_msg = crate::types::VerificationMessage { transfer_id: msg.transfer_id.clone(), tx_hash: tx_hash_str.clone(), retry_count: 0 };
                let verify_json = serde_json::to_string(&verify_msg)?;
                pipe.cmd("XADD")
                    .arg(&verify_stream)
                    .arg("*")
                    .arg("data")
                    .arg(&verify_json);

                // Event: QUEUED_VERIFICATION
                let ev_qv = serde_json::to_string(&Event::new("QUEUED_VERIFICATION"))?;
                pipe.lpush(&event_key, ev_qv);
                pipe.expire(&event_key, 86400);

                // Ack
                pipe.cmd("XACK")
                    .arg(stream_key)
                    .arg(consumer_group)
                    .arg(stream_id);
            }

            pipe.query_async::<()>(&mut conn).await?;
            Ok(())
        }
        Err(e) => {
            warn!("Failed to submit batch: {:?}", e);

            for (stream_id, msg, _) in &transfers {
                let _ = rh::ack_message(&mut conn, stream_key, consumer_group, stream_id).await;

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
