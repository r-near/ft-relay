use ::redis::aio::ConnectionManager;
use anyhow::{self, Result};
use log::{debug, info, warn};
use near_kit::{CryptoHash, ExecutionStatus, Near, TxExecutionStatus};
use std::sync::Arc;

use crate::redis::{events, keys, state, streams, transfers, verification};
use crate::types::{AccountId, Event, Status, VerificationTxMessage};

use super::shared;

const MAX_VERIFICATION_RETRIES: u32 = 20;

pub struct VerificationWorkerRuntime {
    pub redis_conn: ConnectionManager,
    pub near: Arc<Near>,
    pub relay_account: AccountId,
    pub env: String,
}

pub struct VerificationWorkerContext {
    pub runtime: Arc<VerificationWorkerRuntime>,
}

pub async fn verification_worker_loop(ctx: VerificationWorkerContext) -> Result<()> {
    let consumer_name = shared::consumer_name("verification");
    let stream_key = keys::verification_stream(&ctx.runtime.env);
    let consumer_group = keys::verification_consumer_group(&ctx.runtime.env);

    let mut conn = ctx.runtime.redis_conn.clone();
    shared::ensure_consumer_group(&mut conn, &stream_key, &consumer_group).await?;

    info!("Verification worker {} started", consumer_name);

    loop {
        let mut conn = ctx.runtime.redis_conn.clone();

        let messages: Vec<(String, VerificationTxMessage)> = match streams::pop_batch(
            &mut conn,
            &stream_key,
            &consumer_group,
            &consumer_name,
            1,
            50,
        )
        .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!("Error popping verification batch: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
        };

        if messages.is_empty() {
            continue;
        }

        let (stream_id, msg) = &messages[0];
        info!(
            "[VERIFY] Checking tx {} (retry #{})",
            msg.tx_hash, msg.retry_count
        );
        match verify_transaction(&ctx, msg).await {
            Ok(VerificationResult::Completed) => {
                let _ =
                    streams::ack_message(&mut conn, &stream_key, &consumer_group, stream_id).await;
            }
            Ok(VerificationResult::Failed(reason)) => {
                warn!(
                    "[VERIFY] Tx {} failed: {} — re-enqueuing transfers for resubmission",
                    msg.tx_hash, reason
                );

                let _ =
                    streams::ack_message(&mut conn, &stream_key, &consumer_group, stream_id).await;

                let transfer_ids = transfers::get_tx_transfers(&mut conn, &msg.tx_hash)
                    .await
                    .unwrap_or_default();
                if !transfer_ids.is_empty() {
                    info!(
                        "[VERIFY] Re-enqueueing {} transfer(s) from failed tx {}",
                        transfer_ids.len(),
                        msg.tx_hash
                    );
                }

                for tid in transfer_ids {
                    let _ = state::increment_retry_count(&mut conn, &tid).await;
                    let _ = state::update_transfer_status(&mut conn, &tid, Status::QueuedTransfer)
                        .await;
                    let _ = events::log_event(
                        &mut conn,
                        &tid,
                        Event::new("RETRY_TRANSFER").with_reason(reason.clone()),
                    )
                    .await;
                    let _ = transfers::enqueue_transfer(&mut conn, &ctx.runtime.env, &tid, 0).await;
                    let _ = events::log_event(&mut conn, &tid, Event::new("QUEUED_TRANSFER")).await;
                }
                let _ = verification::clear_tx_pending_verification(
                    &mut conn,
                    &ctx.runtime.env,
                    &msg.tx_hash,
                )
                .await;
            }
            Ok(VerificationResult::Pending) => {
                let retry_count = msg.retry_count + 1;

                let _ =
                    streams::ack_message(&mut conn, &stream_key, &consumer_group, stream_id).await;

                if retry_count < MAX_VERIFICATION_RETRIES {
                    let _ = verification::enqueue_tx_verification_retry(
                        &mut conn,
                        &ctx.runtime.env,
                        &msg.tx_hash,
                        retry_count,
                    )
                    .await;
                } else {
                    let transfer_ids = transfers::get_tx_transfers(&mut conn, &msg.tx_hash)
                        .await
                        .unwrap_or_default();
                    for tid in transfer_ids {
                        let _ =
                            state::update_transfer_status(&mut conn, &tid, Status::Failed).await;
                        let _ = events::log_event(
                            &mut conn,
                            &tid,
                            Event::new("FAILED").with_reason(format!(
                                "Verification timeout after {} checks",
                                retry_count
                            )),
                        )
                        .await;
                    }
                    let _ = verification::clear_tx_pending_verification(
                        &mut conn,
                        &ctx.runtime.env,
                        &msg.tx_hash,
                    )
                    .await;
                }
            }
            Err(e) => {
                warn!("Error checking tx status for {}: {:?}", msg.tx_hash, e);

                let _ =
                    streams::ack_message(&mut conn, &stream_key, &consumer_group, stream_id).await;

                let retry_count = msg.retry_count + 1;
                if retry_count < MAX_VERIFICATION_RETRIES {
                    let _ = verification::enqueue_tx_verification_retry(
                        &mut conn,
                        &ctx.runtime.env,
                        &msg.tx_hash,
                        retry_count,
                    )
                    .await;
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
    msg: &VerificationTxMessage,
) -> Result<VerificationResult> {
    let mut conn = ctx.runtime.redis_conn.clone();

    if let Some(cached_status) = verification::get_tx_status(&mut conn, &msg.tx_hash).await? {
        debug!(
            "Tx {} status already cached: {} (already handled)",
            msg.tx_hash, cached_status
        );

        match cached_status.as_str() {
            "completed" => return Ok(VerificationResult::Completed),
            "failed" => return Ok(VerificationResult::Failed("Cached failure".to_string())),
            _ => {}
        }
    }

    let tx_hash: CryptoHash = msg
        .tx_hash
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid tx hash: {:?}", e))?;

    let sender_id: near_kit::AccountId = ctx
        .runtime
        .relay_account
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid sender account: {:?}", e))?;

    // Check transaction status using near-kit's RPC
    let result = ctx
        .runtime
        .near
        .rpc()
        .tx_status(&tx_hash, &sender_id, TxExecutionStatus::Final)
        .await;

    match result {
        Ok(outcome) => {
            if outcome.is_success() {
                info!("[VERIFY] Tx {} completed successfully", msg.tx_hash);

                verification::set_tx_status(&mut conn, &msg.tx_hash, "completed").await?;

                let transfer_ids = transfers::get_tx_transfers(&mut conn, &msg.tx_hash).await?;
                info!(
                    "Updating {} transfers for tx {}",
                    transfer_ids.len(),
                    msg.tx_hash
                );

                for transfer_id in transfer_ids {
                    state::update_transfer_status(&mut conn, &transfer_id, Status::Completed)
                        .await?;
                    events::log_event(&mut conn, &transfer_id, Event::new("COMPLETED")).await?;
                }

                verification::clear_tx_pending_verification(
                    &mut conn,
                    &ctx.runtime.env,
                    &msg.tx_hash,
                )
                .await?;

                Ok(VerificationResult::Completed)
            } else if outcome.is_failure() {
                // Extract failure reason from status
                let reason = match &outcome.status {
                    Some(ExecutionStatus::Failure(val)) => format!("{}", val),
                    _ => "Unknown failure".to_string(),
                };
                warn!("[VERIFY] Tx {} failed on-chain: {}", msg.tx_hash, reason);

                verification::set_tx_status(&mut conn, &msg.tx_hash, "failed").await?;

                let transfer_ids = transfers::get_tx_transfers(&mut conn, &msg.tx_hash).await?;

                for transfer_id in transfer_ids {
                    state::update_transfer_status(&mut conn, &transfer_id, Status::Failed).await?;
                    events::log_event(
                        &mut conn,
                        &transfer_id,
                        Event::new("FAILED").with_reason(reason.clone()),
                    )
                    .await?;
                }

                verification::clear_tx_pending_verification(
                    &mut conn,
                    &ctx.runtime.env,
                    &msg.tx_hash,
                )
                .await?;

                Ok(VerificationResult::Failed(reason))
            } else {
                // Still pending/unknown
                verification::set_tx_status(&mut conn, &msg.tx_hash, "pending").await?;
                Ok(VerificationResult::Pending)
            }
        }
        Err(e) => {
            let err_str = format!("{:?}", e);
            if err_str.contains("UNKNOWN_TRANSACTION") || err_str.contains("does not exist") {
                verification::set_tx_status(&mut conn, &msg.tx_hash, "pending").await?;
                Ok(VerificationResult::Pending)
            } else if err_str.contains("TimeoutError") || err_str.contains("TIMEOUT_ERROR") {
                debug!("RPC timeout checking tx status - will retry");
                verification::set_tx_status(&mut conn, &msg.tx_hash, "pending").await?;
                Ok(VerificationResult::Pending)
            } else {
                Err(anyhow::anyhow!(
                    "Failed to check transaction status: {:?}",
                    e
                ))
            }
        }
    }
}
