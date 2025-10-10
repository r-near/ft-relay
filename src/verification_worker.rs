use anyhow::Result;
use log::{info, warn};
use near_primitives::hash::CryptoHash;
use redis::aio::ConnectionManager;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::redis_helpers as rh;
use crate::rpc_client::{NearRpcClient, TxStatus};
use crate::types::{AccountId, Event, Status, VerificationMessage};

const MAX_VERIFICATION_RETRIES: u32 = 20;

pub struct VerificationWorkerRuntime {
    pub redis_conn: ConnectionManager,
    pub rpc_client: Arc<NearRpcClient>,
    pub relay_account: AccountId,
    pub env: String,
}

pub struct VerificationWorkerContext {
    pub runtime: Arc<VerificationWorkerRuntime>,
    pub linger_ms: u64,
}

pub async fn verification_worker_loop(ctx: VerificationWorkerContext) -> Result<()> {
    let consumer_name = format!("verification-{}", Uuid::new_v4());
    let stream_key = format!("ftrelay:{}:verify", ctx.runtime.env);
    let consumer_group = format!("ftrelay:{}:verify_workers", ctx.runtime.env);

    let mut conn = ctx.runtime.redis_conn.clone();
    let _: Result<String, _> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream_key)
        .arg(&consumer_group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await;

    info!("Verification worker {} started", consumer_name);

    loop {
        let mut conn = ctx.runtime.redis_conn.clone();

        let batch: Vec<(String, VerificationMessage)> = match rh::pop_batch(
            &mut conn,
            &stream_key,
            &consumer_group,
            &consumer_name,
            50,
            ctx.linger_ms,
        )
        .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!("Error popping batch: {:?}", e);
                continue;
            }
        };

        for (stream_id, msg) in batch {
            match verify_transaction(&ctx, &msg).await {
                Ok(VerificationResult::Completed) => {
                    let _ = rh::ack_message(&mut conn, &stream_key, &consumer_group, &stream_id).await;
                }
                Ok(VerificationResult::Failed(reason)) => {
                    warn!("Transfer {} failed: {}", msg.transfer_id, reason);
                    let _ = rh::ack_message(&mut conn, &stream_key, &consumer_group, &stream_id).await;
                }
                Ok(VerificationResult::Pending) => {
                    let retry_count = msg.retry_count + 1;
                    
                    let _ = rh::ack_message(&mut conn, &stream_key, &consumer_group, &stream_id).await;

                    if retry_count < MAX_VERIFICATION_RETRIES {
                        let _ = rh::increment_retry_count(&mut conn, &msg.transfer_id).await;
                        let _ = rh::enqueue_verification(
                            &mut conn,
                            &ctx.runtime.env,
                            &msg.transfer_id,
                            &msg.tx_hash,
                            retry_count,
                        )
                        .await;
                    } else {
                        let _ = rh::update_transfer_status(
                            &mut conn,
                            &msg.transfer_id,
                            Status::Failed,
                        )
                        .await;
                        let _ = rh::log_event(
                            &mut conn,
                            &msg.transfer_id,
                            Event::new("FAILED")
                                .with_reason(format!(
                                    "Verification timeout after {} checks",
                                    retry_count
                                )),
                        )
                        .await;
                    }
                }
                Err(e) => {
                    warn!(
                        "Error checking tx status for {}: {:?}",
                        msg.transfer_id, e
                    );
                    
                    let _ = rh::ack_message(&mut conn, &stream_key, &consumer_group, &stream_id).await;

                    let retry_count = msg.retry_count + 1;
                    if retry_count < MAX_VERIFICATION_RETRIES {
                        let _ = rh::enqueue_verification(
                            &mut conn,
                            &ctx.runtime.env,
                            &msg.transfer_id,
                            &msg.tx_hash,
                            retry_count,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

enum VerificationResult {
    Completed,
    Failed(String),
    Pending,
}
async fn verify_transaction(
    ctx: &VerificationWorkerContext,
    msg: &VerificationMessage,
) -> Result<VerificationResult> {
    let mut conn = ctx.runtime.redis_conn.clone();
    
    // Fast path: Check if tx_hash status is already cached in Redis
    // If cached as completed/failed, the first worker already updated ALL transfers
    // So we can just ACK and skip (no need to update again)
    if let Some(cached_status) = rh::get_tx_status(&mut conn, &msg.tx_hash).await? {
        info!("Tx {} status already cached: {} (transfer {} already updated by first worker)", 
              msg.tx_hash, cached_status, msg.transfer_id);
        
        match cached_status.as_str() {
            "completed" => return Ok(VerificationResult::Completed),
            "failed" => return Ok(VerificationResult::Failed("Cached failure".to_string())),
            _ => {} // "pending" - fall through to RPC check
        }
    }

    // Wait ~6 seconds (2-3 blocks) before first RPC check
    tokio::time::sleep(Duration::from_secs(6)).await;

    let tx_hash = CryptoHash::from_str(&msg.tx_hash)
        .map_err(|e| anyhow::anyhow!("Invalid tx hash: {:?}", e))?;

    // Check RPC once for this tx_hash
    match ctx
        .runtime
        .rpc_client
        .check_tx_status(&tx_hash, &ctx.runtime.relay_account)
        .await?
    {
        TxStatus::Success(_outcome) => {
            info!("Tx {} completed successfully", msg.tx_hash);
            
            // Cache the result so other transfers skip RPC
            rh::set_tx_status(&mut conn, &msg.tx_hash, "completed").await?;
            
            // Get all transfers for this tx and update them all
            let transfer_ids = rh::get_tx_transfers(&mut conn, &msg.tx_hash).await?;
            info!("Updating {} transfers for tx {}", transfer_ids.len(), msg.tx_hash);
            
            for transfer_id in transfer_ids {
                rh::update_transfer_status(&mut conn, &transfer_id, Status::Completed).await?;
                rh::log_event(&mut conn, &transfer_id, Event::new("COMPLETED")).await?;
            }
            
            Ok(VerificationResult::Completed)
        }
        TxStatus::Failed(reason) => {
            warn!("Tx {} failed on-chain: {}", msg.tx_hash, reason);
            
            // Cache the result
            rh::set_tx_status(&mut conn, &msg.tx_hash, "failed").await?;
            
            // Get all transfers for this tx and mark them as failed
            let transfer_ids = rh::get_tx_transfers(&mut conn, &msg.tx_hash).await?;
            
            for transfer_id in transfer_ids {
                rh::update_transfer_status(&mut conn, &transfer_id, Status::Failed).await?;
                rh::log_event(
                    &mut conn,
                    &transfer_id,
                    Event::new("FAILED").with_reason(reason.clone()),
                )
                .await?;
            }
            
            Ok(VerificationResult::Failed(reason))
        }
        TxStatus::Pending => {
            // Cache as pending (short lived)
            rh::set_tx_status(&mut conn, &msg.tx_hash, "pending").await?;
            Ok(VerificationResult::Pending)
        }
    }
}
