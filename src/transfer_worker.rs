use anyhow::Result;
use log::{debug, info, warn};
use near_api::{NetworkConfig, Signer};
use near_api_types::AccountId;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::views::FinalExecutionOutcomeView;
use redis::AsyncCommands;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{
    config::{FT_TRANSFER_DEPOSIT, FT_TRANSFER_GAS_PER_ACTION, MAX_GAS_PER_TX},
    observability::{BatchId, ObservabilityContext, TxStatus, TxStatusKind},
    redis_helpers,
    rpc_backoff,
    stream_queue::StreamQueue,
    transfer_states::{ReadyToSend, Transfer},
};

const MAX_NONCE_RETRIES: u32 = 3;

pub struct TransferWorkerRuntime {
    pub redis_client: redis::Client,
    pub redis_conn: redis::aio::ConnectionManager,
    pub ready_queue: Arc<StreamQueue<Transfer<ReadyToSend>>>,
    pub signer: Arc<Signer>,
    pub signer_account: AccountId,
    pub token: AccountId,
    pub network: NetworkConfig,
    pub semaphore: Arc<Semaphore>,
    pub obs_ctx: Arc<ObservabilityContext>,
}

pub struct TransferWorkerContext {
    pub runtime: Arc<TransferWorkerRuntime>,
    pub linger_ms: u64,
    pub batch_submit_delay_ms: u64, // Delay after each batch submission to throttle RPC
}

/// Transfer worker - processes ready transfers (accounts already registered)
pub async fn transfer_worker_loop(ctx: TransferWorkerContext) -> Result<()> {
    let consumer_name = format!("transfer-{}", Uuid::new_v4());
    let max_transfers_per_tx = (MAX_GAS_PER_TX / FT_TRANSFER_GAS_PER_ACTION) as usize;

    info!("transfer worker started: {}", consumer_name);

    loop {
        // Pull batch from ready stream with retry on connection errors
        let batch = match ctx
            .runtime
            .ready_queue
            .pop_batch(&consumer_name, max_transfers_per_tx, ctx.linger_ms)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("Connection reset") || err_str.contains("Broken pipe") {
                    warn!("Connection error in worker loop, retrying in 1s: {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                } else {
                    return Err(e);
                }
            }
        };

        if !batch.is_empty() {
            process_transfer_batch(ctx.runtime.clone(), batch, ctx.batch_submit_delay_ms).await;
        }
    }
}

async fn process_transfer_batch(
    runtime: Arc<TransferWorkerRuntime>,
    batch: Vec<(String, Transfer<ReadyToSend>)>,
    batch_submit_delay_ms: u64,
) {
    tokio::spawn(async move {
        let _permit = runtime.semaphore.acquire().await.unwrap();
        let batch_id = BatchId::new();
        let transfer_count = batch.len();

        // Build transfer actions
        let mut actions = Vec::new();

        for (_, transfer) in &batch {
            let data = transfer.data();
            let transfer_args = json!({
                "receiver_id": data.receiver_id,
                "amount": data.amount,
                "memo": data.memo
            })
            .to_string()
            .into_bytes();

            actions.push(Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ft_transfer".to_string(),
                args: transfer_args,
                gas: FT_TRANSFER_GAS_PER_ACTION,
                deposit: FT_TRANSFER_DEPOSIT,
            })));
        }

        // Submit transaction
        let result = submit_with_nonce_retry(
            &runtime.signer,
            &runtime.signer_account,
            &runtime.token,
            actions,
            &runtime.network,
            batch_id,
            transfer_count,
            &runtime.obs_ctx,
        )
        .await;

        match result {
            Ok(outcome) => {
                let tx_hash = outcome.transaction_outcome.id.to_string();
                info!("✅ Batch {} submitted: tx_hash={}", batch_id, tx_hash);

                // Check overall transaction status
                match &outcome.status {
                    near_primitives::views::FinalExecutionStatus::SuccessValue(_) => {
                        runtime.obs_ctx.inc_metric("succeeded").await;
                        info!("tx {} succeeded: {} transfers (batch: {})", tx_hash, batch.len(), batch_id);

                        // All transfers succeeded - collect IDs
                        let redis_ids: Vec<String> =
                            batch.iter().map(|(id, _)| id.clone()).collect();
                        let transfer_ids: Vec<String> = batch
                            .iter()
                            .map(|(_, t)| t.data().transfer_id.clone())
                            .collect();

                        // Mark all as completed
                        let mut conn = match runtime.redis_client.get_multiplexed_async_connection().await {
                            Ok(c) => c,
                            Err(err) => {
                                warn!("failed to get redis connection: {err:?}");
                                return;
                            }
                        };

                        // Check for duplicates and mark all transfers as completed (batched for efficiency)
                        let transfer_data: Vec<(String, String)> = batch
                            .iter()
                            .map(|(_, t)| (t.data().transfer_id.clone(), tx_hash.clone()))
                            .collect();
                        
                        match runtime.obs_ctx.check_and_mark_batch(&transfer_data).await {
                            Ok(duplicates) => {
                                if !duplicates.is_empty() {
                                    for (transfer_id, existing_tx) in &duplicates {
                                        warn!(
                                            "⚠️  DUPLICATE: transfer {} already completed by tx {}... (batch: unknown), now completed again by tx {}... (batch: {})",
                                            transfer_id,
                                            &existing_tx[..16.min(existing_tx.len())],
                                            &tx_hash[..16.min(tx_hash.len())],
                                            batch_id
                                        );
                                        runtime.obs_ctx.inc_metric("duplicates").await;
                                    }
                                    warn!("⚠️  Batch {} had {} duplicate transfers!", batch_id, duplicates.len());
                                }
                            }
                            Err(err) => {
                                warn!("failed to check/mark batch duplicates: {err:?}");
                            }
                        }
                        
                        // Also set in old status format for compatibility
                        let token_str = runtime.token.to_string();
                        if let Err(err) = redis_helpers::set_transfers_completed(
                            &mut conn,
                            &token_str,
                            &transfer_ids,
                            &tx_hash,
                        )
                        .await
                        {
                            warn!("failed to set transfer status: {err:?}");
                        }

                        // Ack all transfers
                        if let Err(err) = runtime.ready_queue.ack(&redis_ids).await {
                            warn!("failed to ack transfers: {err:?}");
                        }
                    }
                    near_primitives::views::FinalExecutionStatus::Failure(tx_err) => {
                        runtime.obs_ctx.inc_metric("failed").await;
                        warn!(
                            "tx {} failed ({} transfers, batch: {}): {:?}",
                            tx_hash,
                            batch.len(),
                            batch_id,
                            tx_err
                        );

                        let err_str = format!("{:?}", tx_err);

                        // Check if failure is due to unregistered account(s)
                        if err_str.contains("account") && err_str.contains("not registered") {
                            info!(
                                "batch failed due to unregistered account(s), re-enqueueing all as pending"
                            );
                            handle_unregistered_retry(&runtime, &batch).await;
                        } else {
                            // Other failure - explicitly retry with attempt tracking
                            warn!(
                                "batch failed with error, will retry with backoff: {:?}",
                                tx_err
                            );
                            handle_transfer_retry(&runtime, &batch, batch_id).await;
                        }
                    }
                    status => {
                        warn!("tx {} unexpected status: {:?}", tx_hash, status);
                        handle_transfer_retry(&runtime, &batch, batch_id).await;
                    }
                }
            }
            Err(submission_err) => {
                let tx_hash = submission_err.tx_hash.clone();
                let err_str = submission_err.error.to_string();

                // PreSign failures happen before blockchain submission - safe to retry
                if err_str.contains("PreSignError") {
                    runtime.obs_ctx.inc_metric("failed").await;
                    warn!("Batch {} presign failed ({} transfers) - will retry", batch_id, transfer_count);
                    handle_transfer_retry(&runtime, &batch, batch_id).await;
                } else {
                    // All other errors happened AFTER transaction submission
                    // We have a tx_hash from the submission error
                    // FINANCIAL SAFETY: Hand off to status checker for async verification
                    
                    info!("Batch {} (tx_hash={}) has ambiguous status, enqueueing for verification...", batch_id, tx_hash);
                    
                    // Create pending verification batch
                    let transfer_data: Vec<crate::transfer_states::TransferData> = batch.iter()
                        .map(|(_, t)| t.data().clone())
                        .collect();
                    
                    let pending = crate::status_checker::PendingVerificationBatch {
                        batch_id: batch_id.to_string(),
                        tx_hash: tx_hash.clone(),
                        attempts: 0,
                        submitted_at: chrono::Utc::now().timestamp(),
                        transfer_data,
                    };
                    
                    // Enqueue for async verification (non-blocking)
                    match crate::status_checker::enqueue_for_verification(
                        &mut runtime.redis_conn.clone(),
                        &runtime.token,
                        pending,
                    ).await {
                        Ok(_) => {
                            info!("✅ Batch {} enqueued for async verification", batch_id);
                            // ACK from ready queue - status checker will handle it from here
                            let redis_ids: Vec<String> = batch.iter().map(|(id, _)| id.clone()).collect();
                            if let Err(ack_err) = runtime.ready_queue.ack(&redis_ids).await {
                                warn!("failed to ack batch sent to verification: {ack_err:?}");
                            }
                        }
                        Err(e) => {
                            warn!("Failed to enqueue batch {} for verification: {:?} - will retry batch", batch_id, e);
                            runtime.obs_ctx.inc_metric("failed").await;
                            handle_transfer_retry(&runtime, &batch, batch_id).await;
                        }
                    }
                }
            }
        }
        
        // Apply throttling delay if configured (to stay under RPC rate limits)
        if batch_submit_delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(batch_submit_delay_ms)).await;
        }
    });
}

/// Handle transfers that failed due to unregistered accounts
/// Converts back to PendingRegistration state with attempt tracking
async fn handle_unregistered_retry(
    runtime: &Arc<TransferWorkerRuntime>,
    batch: &[(String, Transfer<ReadyToSend>)],
) {
    const MAX_REGISTRATION_RETRY_ATTEMPTS: u32 = 10;

    let mut conn = match runtime.redis_client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(err) => {
            warn!("failed to get redis connection: {err:?}");
            return;
        }
    };

    let token_str = runtime.token.to_string();
    let mut redis_ids_to_ack = Vec::new();
    let mut exhausted_count = 0;
    let mut requeued_count = 0;

    for (redis_id, transfer) in batch {
        // Check attempt count before converting to pending
        if transfer.data().attempts >= MAX_REGISTRATION_RETRY_ATTEMPTS {
            exhausted_count += 1;
            redis_ids_to_ack.push(redis_id.clone());
            // TODO: Push to dead letter queue
            continue;
        }

        let mut pending_transfer = transfer.clone().into_pending_registration();
        pending_transfer.data.attempts += 1; // Track registration retry attempts

        if let Err(err) = redis_helpers::push_to_pending_list(
            &mut conn,
            &token_str,
            &pending_transfer,
        )
        .await
        {
            warn!(
                "failed to re-enqueue transfer {} as pending: {err:?}",
                transfer.data().transfer_id
            );
        } else {
            requeued_count += 1;
            redis_ids_to_ack.push(redis_id.clone());
        }
    }

    // Log summary
    if requeued_count > 0 {
        info!(
            "re-enqueued {} transfer(s) for registration retry",
            requeued_count
        );
    }
    if exhausted_count > 0 {
        warn!(
            "{} transfer(s) exceeded max registration retry attempts ({})",
            exhausted_count, MAX_REGISTRATION_RETRY_ATTEMPTS
        );
    }

    // ACK all at once
    if !redis_ids_to_ack.is_empty() {
        if let Err(err) = runtime.ready_queue.ack(&redis_ids_to_ack).await {
            warn!("failed to ack {} transfers: {err:?}", redis_ids_to_ack.len());
        }
    }
}

/// Handle failed transfers by explicitly re-enqueuing with attempt tracking
async fn handle_transfer_retry(
    runtime: &Arc<TransferWorkerRuntime>,
    batch: &[(String, Transfer<ReadyToSend>)],
    original_batch_id: BatchId,
) {
    const MAX_RETRY_ATTEMPTS: u32 = 10;

    let mut redis_ids_to_ack = Vec::new();
    let mut exhausted_count = 0;
    let mut requeued_count = 0;

    for (redis_id, transfer) in batch {
        let failed = transfer.clone().mark_failed();

        if let Some(retry_transfer) = failed.retry(MAX_RETRY_ATTEMPTS) {
            // Re-push to ready queue for retry
            if let Err(err) = runtime.ready_queue.push(&retry_transfer).await {
                warn!(
                    "failed to re-enqueue transfer {} for retry: {err:?}",
                    transfer.data().transfer_id
                );
            } else {
                requeued_count += 1;
                redis_ids_to_ack.push(redis_id.clone());
                
                // Track that this transfer is being retried from original_batch_id
                let transfer_id = &transfer.data().transfer_id;
                let retry_marker = format!("retry:{}:{}", original_batch_id, transfer_id);
                if let Ok(mut conn) = runtime.redis_client.get_multiplexed_async_connection().await {
                    let _: Result<(), _> = conn.set_ex::<_, _, ()>(&retry_marker, "retrying", 3600).await;
                }
            }
        } else {
            exhausted_count += 1;
            redis_ids_to_ack.push(redis_id.clone());
            // TODO: Could push to dead letter queue here
        }
    }

    // Log summary
    if requeued_count > 0 {
        info!("re-enqueued {} transfer(s) for retry", requeued_count);
    }
    if exhausted_count > 0 {
        warn!(
            "{} transfer(s) exceeded max retry attempts ({})",
            exhausted_count, MAX_RETRY_ATTEMPTS
        );
    }

    // ACK all at once
    if !redis_ids_to_ack.is_empty() {
        if let Err(err) = runtime.ready_queue.ack(&redis_ids_to_ack).await {
            warn!("failed to ack {} transfers: {err:?}", redis_ids_to_ack.len());
        }
    }
}

// Transaction verification moved to status_checker module for async processing

// Custom error type that includes tx_hash for verification
#[derive(Debug)]
struct SubmissionError {
    tx_hash: String,
    error: anyhow::Error,
}

async fn submit_with_nonce_retry(
    signer: &Arc<Signer>,
    signer_account: &AccountId,
    token: &AccountId,
    actions: Vec<Action>,
    network: &NetworkConfig,
    batch_id: BatchId,
    transfer_count: usize,
    obs_ctx: &Arc<ObservabilityContext>,
) -> Result<FinalExecutionOutcomeView, SubmissionError> {
    let mut nonce_retry = 0;

    loop {
        // Check for RPC backoff before making any RPC calls
        // Use a temporary Redis connection for backoff check
        if let Ok(redis_client) = redis::Client::open("redis://127.0.0.1:6379") {
            if let Ok(mut conn) = redis::aio::ConnectionManager::new(redis_client).await {
                if let Err(err) = rpc_backoff::check_and_wait_for_backoff(&mut conn).await {
                    warn!("backoff check failed: {err:?}");
                }
            }
        }
        
        // Presign the transaction so we can get the hash before sending
        let tx_builder = near_api::Transaction::construct(signer_account.clone(), token.clone())
            .add_actions(actions.clone())
            .with_signer(signer.clone());

        let presigned = match tx_builder.presign_with(network).await {
            Ok(p) => p,
            Err(err) => {
                let err_str = err.to_string();
                
                // Check if this is a rate limit error
                if err_str.contains("TooManyRequests") || err_str.contains("rate limit") {
                    if let Ok(redis_client) = redis::Client::open("redis://127.0.0.1:6379") {
                        if let Ok(mut conn) = redis::aio::ConnectionManager::new(redis_client).await {
                            let _ = rpc_backoff::record_rate_limit_hit(&mut conn).await;
                        }
                    }
                    warn!("Batch {} presign failed ({} transfers) - will retry", batch_id, transfer_count);
                } else {
                    warn!("failed to presign transaction: {err:?}");
                }
                
                // Presign failures happen BEFORE blockchain submission
                // Mark as retriable by wrapping in a custom error type
                // The outer handler will see "PreSignError" and can safely retry
                return Err(SubmissionError {
                    tx_hash: "unknown".to_string(),
                    error: anyhow::anyhow!("PreSignError: {}", err_str),
                });
            }
        };

        // Get the transaction hash before sending (without consuming presigned)
        let tx_hash = match &presigned.tr {
            near_api::advanced::TransactionableOrSigned::Signed((signed_tx, _)) => {
                signed_tx.get_hash().to_string()
            }
            _ => "unknown".to_string(),
        };

        // Track submission
        obs_ctx.inc_metric("submitted").await;
        let submitted_at = chrono::Utc::now().timestamp();
        let _ = obs_ctx.track_tx_status(TxStatus {
            tx_hash: tx_hash.clone(),
            batch_id: batch_id.to_string(),
            transfer_count,
            status: TxStatusKind::Submitted,
            submitted_at,
            completed_at: None,
            error: None,
            verified_onchain: false,
        }).await;

        let result = presigned.send_to(network).await;

        match result {
            Ok(tx) => {
                // Record RPC success for backoff recovery
                if let Ok(redis_client) = redis::Client::open("redis://127.0.0.1:6379") {
                    if let Ok(mut conn) = redis::aio::ConnectionManager::new(redis_client).await {
                        let _ = rpc_backoff::record_success(&mut conn).await;
                    }
                }
                
                // Update to succeeded
                let _ = obs_ctx.track_tx_status(TxStatus {
                    tx_hash: tx_hash.clone(),
                    batch_id: batch_id.to_string(),
                    transfer_count,
                    status: TxStatusKind::Succeeded,
                    submitted_at,
                    completed_at: Some(chrono::Utc::now().timestamp()),
                    error: None,
                    verified_onchain: false,
                }).await;
                return Ok(tx);
            }
            Err(err) => {
                let err_str = err.to_string();
                
                // Check if this is a rate limit error
                if err_str.contains("TooManyRequests") || err_str.contains("rate limit") {
                    if let Ok(redis_client) = redis::Client::open("redis://127.0.0.1:6379") {
                        if let Ok(mut conn) = redis::aio::ConnectionManager::new(redis_client).await {
                            let _ = rpc_backoff::record_rate_limit_hit(&mut conn).await;
                        }
                    }
                }

                // Check if it's an InvalidNonce error
                if err_str.contains("InvalidNonce") && nonce_retry < MAX_NONCE_RETRIES {
                    nonce_retry += 1;
                    
                    // Only log first retry at WARN, rest at DEBUG
                    if nonce_retry == 1 {
                        warn!(
                            "InvalidNonce for batch {} (retry {}/{}), resyncing nonce",
                            batch_id, nonce_retry, MAX_NONCE_RETRIES
                        );
                    } else {
                        debug!(
                            "Nonce resync retry {}/{} for batch {}",
                            nonce_retry, MAX_NONCE_RETRIES, batch_id
                        );
                    }

                    // Force nonce resync
                    match signer.get_public_key().await {
                        Ok(public_key) => {
                            if let Err(fetch_err) = signer
                                .fetch_tx_nonce(signer_account.clone(), public_key, network)
                                .await
                            {
                                warn!("failed to resync nonce: {fetch_err:?}");
                            } else if nonce_retry == 1 {
                                info!("Nonce resynced for batch {}", batch_id);
                            } else {
                                debug!("Nonce resynced for batch {}", batch_id);
                            }
                        }
                        Err(e) => {
                            warn!("failed to get public key for nonce resync: {e:?}");
                        }
                    }

                    // Small delay before retry
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    continue;
                }

                // Not a nonce issue or retries exhausted
                // Track failure
                let status_kind = if err_str.contains("TimeoutError") {
                    TxStatusKind::Timeout
                } else {
                    TxStatusKind::Failed
                };
                
                let _ = obs_ctx.track_tx_status(TxStatus {
                    tx_hash: tx_hash.clone(),
                    batch_id: batch_id.to_string(),
                    transfer_count,
                    status: status_kind,
                    submitted_at,
                    completed_at: Some(chrono::Utc::now().timestamp()),
                    error: Some(err_str.clone()),
                    verified_onchain: false,
                }).await;
                
                if !err_str.contains("InvalidNonce") {
                    warn!("transaction {} failed (batch: {}): {}", tx_hash, batch_id, err_str);
                }
                
                return Err(SubmissionError {
                    tx_hash: tx_hash.clone(),
                    error: err.into(),
                });
            }
        }
    }
}
