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
        .submit_batch_transfer(
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
        Ok((tx_hash, outcome)) => {
            let tx_hash_str = tx_hash.to_string();
            
            // Check if transaction succeeded (already Final, no need to verify)
            let is_success = matches!(
                outcome.status,
                near_primitives::views::FinalExecutionStatus::SuccessValue(_)
            );

            if is_success {
                info!(
                    "Batch of {} transfers succeeded, tx: {}",
                    transfers.len(),
                    tx_hash_str
                );
            } else {
                warn!(
                    "Batch of {} transfers uncertain, tx: {} - will verify",
                    transfers.len(),
                    tx_hash_str
                );
            }

            if is_success {
                // Transaction succeeded - mark as completed (skip verification)
                let redis_update_start = std::time::Instant::now();
                
                // Pipeline ALL Redis updates for the batch (one round-trip instead of 900!)
                let mut pipe = redis::pipe();
                pipe.atomic(); // Execute all commands together
                
                let now = chrono::Utc::now().to_rfc3339();
                for (stream_id, msg, _) in &transfers {
                    let transfer_key = format!("transfer:{}", msg.transfer_id);
                    let event_key = format!("transfer:{}:ev", msg.transfer_id);
                    let tx_transfers_key = format!("tx:{}:transfers", tx_hash_str);
                    
                    // Update transfer status (2 HSETs)
                    pipe.hset(&transfer_key, "status", "Completed");
                    pipe.hset(&transfer_key, "updated_at", &now);
                    pipe.hset(&transfer_key, "completed_at", &now);
                    
                    // Update tx_hash (1 HSET)
                    pipe.hset(&transfer_key, "tx_hash", &tx_hash_str);
                    
                    // Add transfer to tx mapping (1 SADD + 1 EXPIRE)
                    pipe.sadd(&tx_transfers_key, &msg.transfer_id);
                    pipe.expire(&tx_transfers_key, 86400);
                    
                    // Log event (1 LPUSH + 1 EXPIRE)
                    let event = Event::new("COMPLETED").with_tx_hash(tx_hash_str.clone());
                    let serialized = serde_json::to_string(&event)?;
                    pipe.lpush(&event_key, serialized);
                    pipe.expire(&event_key, 86400);
                    
                    // ACK message (1 XACK)
                    pipe.cmd("XACK")
                        .arg(stream_key)
                        .arg(consumer_group)
                        .arg(stream_id);
                }
                
                // Execute entire pipeline in ONE network round-trip!
                pipe.query_async::<()>(&mut conn).await?;
                
                let redis_update_duration = redis_update_start.elapsed();
                debug!("[METRIC] op=redis_updates duration_ms={} count={}", redis_update_duration.as_millis(), transfers.len());
                debug!("[TIMING] Pipelined Redis updates for {} transfers took {}ms ({:.1}ms per transfer)", 
                    transfers.len(), 
                    redis_update_duration.as_millis(),
                    redis_update_duration.as_millis() as f64 / transfers.len() as f64
                );
            } else {
                // Transaction failed or uncertain - send to verification
                for (stream_id, msg, _) in &transfers {
                    rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Submitted)
                        .await?;
                    rh::update_tx_hash(&mut conn, &msg.transfer_id, &tx_hash_str).await?;
                    rh::add_transfer_to_tx(&mut conn, &tx_hash_str, &msg.transfer_id).await?;
                    rh::log_event(
                        &mut conn,
                        &msg.transfer_id,
                        Event::new("SUBMITTED").with_tx_hash(tx_hash_str.clone()),
                    )
                    .await?;
                    rh::enqueue_verification(
                        &mut conn,
                        &ctx.runtime.env,
                        &msg.transfer_id,
                        &tx_hash_str,
                        0,
                    )
                    .await?;
                    rh::log_event(
                        &mut conn,
                        &msg.transfer_id,
                        Event::new("QUEUED_VERIFICATION"),
                    )
                    .await?;
                    let _ = rh::ack_message(&mut conn, stream_key, consumer_group, stream_id).await;
                }
            }

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
