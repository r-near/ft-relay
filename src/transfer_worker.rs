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
    queue::TransferQueue,
    transfer_states::{ReadyToSend, Transfer},
};

const MAX_NONCE_RETRIES: u32 = 3;

pub struct TransferWorkerRuntime {
    pub queue: Arc<TransferQueue>,
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
            .queue
            .pop_ready_batch(&consumer_name, max_transfers_per_tx, ctx.linger_ms)
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
                // When batching multiple actions to the same contract, they execute atomically
                // Either all succeed or all fail together
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
                        if let Err(err) = runtime.queue.set_status(&transfer_ids, &tx_hash).await {
                            warn!("failed to set transfer status: {err:?}");
                        }

                        // Ack all transfers
                        if let Err(err) = runtime.queue.ack(&redis_ids).await {
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

                            // Re-enqueue all as pending and ack
                            for (redis_id, transfer) in &batch {
                                let pending_transfer = transfer.clone().into_pending_registration();
                                if let Err(err) =
                                    runtime.queue.push_pending(&pending_transfer).await
                                {
                                    warn!("failed to re-enqueue transfer as pending: {err:?}");
                                } else if let Err(err) = runtime.queue.ack(&[redis_id.clone()]).await {
                                    warn!("failed to ack re-enqueued transfer: {err:?}");
                                }
                            }
                        } else {
                            // Other failure - don't ack, let Redis redeliver for retry
                            warn!(
                                "batch failed with error, will retry via Redis redelivery: {:?}",
                                tx_err
                            );
                        }
                    }
                    status => {
                        warn!("tx {} unexpected status: {:?}", tx_hash, status);
                        // Don't ack - let them retry
                    }
                }
            }
            Err(err) => {
                warn!("failed to submit batch: {err:?}");
                // Don't ack - let them be redelivered
            }
        }
    });
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
