use anyhow::Result;
use log::{info, warn};
use near_api::{NetworkConfig, Signer};
use near_api_types::AccountId;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::views::FinalExecutionOutcomeView;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{
    config::{FT_TRANSFER_DEPOSIT, FT_TRANSFER_GAS_PER_ACTION, MAX_GAS_PER_TX},
    redis_helpers,
    stream_queue::StreamQueue,
    transfer_states::{ReadyToSend, Transfer},
};

const MAX_NONCE_RETRIES: u32 = 3;

pub struct TransferWorkerRuntime {
    pub redis_client: redis::Client,
    pub ready_queue: Arc<StreamQueue<Transfer<ReadyToSend>>>,
    pub signer: Arc<Signer>,
    pub signer_account: AccountId,
    pub token: AccountId,
    pub network: NetworkConfig,
    pub semaphore: Arc<Semaphore>,
}

pub struct TransferWorkerContext {
    pub runtime: Arc<TransferWorkerRuntime>,
    pub linger_ms: u64,
}

/// Transfer worker - processes ready transfers (accounts already registered)
pub async fn transfer_worker_loop(ctx: TransferWorkerContext) -> Result<()> {
    let consumer_name = format!("transfer-{}", Uuid::new_v4());
    let max_transfers_per_tx = (MAX_GAS_PER_TX / FT_TRANSFER_GAS_PER_ACTION) as usize;

    info!("transfer worker started: {}", consumer_name);

    loop {
        // Pull batch from ready stream
        let batch = ctx
            .runtime
            .ready_queue
            .pop_batch(&consumer_name, max_transfers_per_tx, ctx.linger_ms)
            .await?;

        if !batch.is_empty() {
            process_transfer_batch(ctx.runtime.clone(), batch).await;
        }
    }
}

async fn process_transfer_batch(
    runtime: Arc<TransferWorkerRuntime>,
    batch: Vec<(String, Transfer<ReadyToSend>)>,
) {
    tokio::spawn(async move {
        let _permit = runtime.semaphore.acquire().await.unwrap();

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
        match submit_with_nonce_retry(
            &runtime.signer,
            &runtime.signer_account,
            &runtime.token,
            actions,
            &runtime.network,
        )
        .await
        {
            Ok(outcome) => {
                let tx_hash = outcome.transaction_outcome.id.to_string();

                // Check overall transaction status
                match &outcome.status {
                    near_primitives::views::FinalExecutionStatus::SuccessValue(_) => {
                        info!("tx {} succeeded: {} transfers", tx_hash, batch.len());

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
                        warn!(
                            "tx {} failed ({} transfers): {:?}",
                            tx_hash,
                            batch.len(),
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
                            handle_transfer_retry(&runtime, &batch).await;
                        }
                    }
                    status => {
                        warn!("tx {} unexpected status: {:?}", tx_hash, status);
                        handle_transfer_retry(&runtime, &batch).await;
                    }
                }
            }
            Err(err) => {
                warn!("failed to submit batch: {err:?}");
                handle_transfer_retry(&runtime, &batch).await;
            }
        }
    });
}

/// Handle transfers that failed due to unregistered accounts
/// Converts back to PendingRegistration state with attempt tracking
async fn handle_unregistered_retry(
    runtime: &Arc<TransferWorkerRuntime>,
    batch: &[(String, Transfer<ReadyToSend>)],
) {
    const MAX_REGISTRATION_RETRY_ATTEMPTS: u32 = 3;

    let mut conn = match runtime.redis_client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(err) => {
            warn!("failed to get redis connection: {err:?}");
            return;
        }
    };

    let token_str = runtime.token.to_string();

    for (redis_id, transfer) in batch {
        // Check attempt count before converting to pending
        if transfer.data().attempts >= MAX_REGISTRATION_RETRY_ATTEMPTS {
            warn!(
                "transfer {} exceeded max registration retry attempts ({}), giving up",
                transfer.data().transfer_id,
                MAX_REGISTRATION_RETRY_ATTEMPTS
            );
            // TODO: Push to dead letter queue
            if let Err(err) = runtime.ready_queue.ack(&[redis_id.clone()]).await {
                warn!("failed to ack exhausted transfer: {err:?}");
            }
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
            info!(
                "re-enqueued transfer {} for registration (attempt {})",
                transfer.data().transfer_id,
                pending_transfer.data.attempts
            );
        }

        // ACK immediately - we've handled it
        if let Err(err) = runtime.ready_queue.ack(&[redis_id.clone()]).await {
            warn!("failed to ack transfer after re-enqueue: {err:?}");
        }
    }
}

/// Handle failed transfers by explicitly re-enqueuing with attempt tracking
async fn handle_transfer_retry(
    runtime: &Arc<TransferWorkerRuntime>,
    batch: &[(String, Transfer<ReadyToSend>)],
) {
    const MAX_RETRY_ATTEMPTS: u32 = 5;

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
                info!(
                    "re-enqueued transfer {} for retry (attempt {})",
                    transfer.data().transfer_id,
                    retry_transfer.data().attempts
                );
            }
        } else {
            warn!(
                "transfer {} exceeded max retry attempts ({}), giving up",
                transfer.data().transfer_id,
                MAX_RETRY_ATTEMPTS
            );
            // TODO: Could push to dead letter queue here
        }

        // ACK immediately - we've handled it explicitly
        if let Err(err) = runtime.ready_queue.ack(&[redis_id.clone()]).await {
            warn!("failed to ack transfer after retry handling: {err:?}");
        }
    }
}

async fn submit_with_nonce_retry(
    signer: &Arc<Signer>,
    signer_account: &AccountId,
    token: &AccountId,
    actions: Vec<Action>,
    network: &NetworkConfig,
) -> Result<FinalExecutionOutcomeView> {
    let mut nonce_retry = 0;

    loop {
        let result = near_api::Transaction::construct(signer_account.clone(), token.clone())
            .add_actions(actions.clone())
            .with_signer(signer.clone())
            .send_to(network)
            .await;

        match result {
            Ok(tx) => {
                return Ok(tx);
            }
            Err(err) => {
                let err_str = err.to_string();

                // Check if it's an InvalidNonce error
                if err_str.contains("InvalidNonce") && nonce_retry < MAX_NONCE_RETRIES {
                    nonce_retry += 1;
                    warn!(
                        "InvalidNonce detected (retry {}/{}), forcing nonce resync from chain",
                        nonce_retry, MAX_NONCE_RETRIES
                    );

                    // Force nonce resync
                    match signer.get_public_key().await {
                        Ok(public_key) => {
                            if let Err(fetch_err) = signer
                                .fetch_tx_nonce(signer_account.clone(), public_key, network)
                                .await
                            {
                                warn!("failed to resync nonce: {fetch_err:?}");
                            } else {
                                info!("nonce resync successful, retrying transaction");
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
                return Err(err.into());
            }
        }
    }
}
