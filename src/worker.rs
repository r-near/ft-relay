use std::sync::Arc;

use anyhow::Result;
use log::{info, warn};
use near_api::{NetworkConfig, Signer, Transaction};
use near_api_types::AccountId;
use near_primitives::action::{Action, FunctionCallAction};
use serde_json::json;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{
    config::{FT_TRANSFER_DEPOSIT, FT_TRANSFER_GAS_PER_ACTION},
    redis_queue::RedisQueue,
    types::TransferReq,
};

const REDIS_CLAIM_MIN_IDLE_MS: u64 = 1;
const MAX_RETRY_ATTEMPTS: u32 = 10;
const MAX_NONCE_RETRIES: u32 = 3;

#[derive(Clone)]
pub struct WorkerRuntime {
    pub queue: Arc<RedisQueue>,
    pub signer: Arc<Signer>,
    pub signer_account: AccountId,
    pub token: AccountId,
    pub network: NetworkConfig,
    pub semaphore: Arc<Semaphore>,
}

pub struct WorkerContext {
    pub runtime: Arc<WorkerRuntime>,
    pub batch_size: usize,
    pub linger_ms: u64,
}

pub async fn worker_loop(ctx: WorkerContext) -> Result<()> {
    let consumer_name = format!("relay-{}", Uuid::new_v4());

    // Reclaim stale messages on startup
    let reclaimed = ctx
        .runtime
        .queue
        .reclaim_stale(&consumer_name, REDIS_CLAIM_MIN_IDLE_MS, ctx.batch_size)
        .await?;
    if !reclaimed.is_empty() {
        info!("claimed {} stale transfers from Redis", reclaimed.len());
        process_batch(ctx.runtime.clone(), reclaimed).await;
    }

    // Main loop
    loop {
        let messages = ctx
            .runtime
            .queue
            .pop_batch(&consumer_name, ctx.batch_size, ctx.linger_ms)
            .await?;

        if !messages.is_empty() {
            process_batch(ctx.runtime.clone(), messages).await;
        }
    }
}

async fn process_batch(runtime: Arc<WorkerRuntime>, mut batch: Vec<(String, TransferReq)>) {
    tokio::spawn(async move {
        let _permit = runtime.semaphore.acquire().await.unwrap();

        // Increment attempts for all transfers
        for (_, transfer) in batch.iter_mut() {
            transfer.attempts += 1;
        }

        let transfers: Vec<&TransferReq> = batch.iter().map(|(_, t)| t).collect();
        let redis_ids: Vec<&str> = batch.iter().map(|(id, _)| id.as_str()).collect();
        let transfer_ids: Vec<String> = batch.iter().map(|(_, t)| t.transfer_id.clone()).collect();

        match submit_batch(
            &runtime.signer,
            &runtime.signer_account,
            &runtime.token,
            &transfers,
            &runtime.network,
        )
        .await
        {
            Ok(tx_hash) => {
                log::debug!("submitted {} transfers", transfers.len());

                if let Err(err) = runtime.queue.set_status(&transfer_ids, &tx_hash).await {
                    warn!("failed to set transfer status: {err:?}");
                }

                let owned_ids: Vec<String> = redis_ids.iter().map(|s| s.to_string()).collect();
                if let Err(err) = runtime.queue.ack(&owned_ids).await {
                    warn!("failed to ack transfers: {err:?}");
                }
            }
            Err(err) => {
                warn!("batch submission failed: {err:?}");

                // Retry or terminate each transfer
                for (redis_id, transfer) in batch {
                    if transfer.attempts >= MAX_RETRY_ATTEMPTS {
                        warn!(
                            "transfer {} reached max retries ({}/{}), marking as terminal",
                            transfer.transfer_id, transfer.attempts, MAX_RETRY_ATTEMPTS
                        );
                        if let Err(err) = runtime.queue.ack(&[redis_id]).await {
                            warn!("failed to ack terminal transfer: {err:?}");
                        }
                    } else {
                        log::debug!(
                            "transfer {} will retry (attempt {}/{})",
                            transfer.transfer_id,
                            transfer.attempts,
                            MAX_RETRY_ATTEMPTS
                        );
                        if let Err(err) = runtime.queue.retry(&redis_id, &transfer).await {
                            warn!("failed to retry transfer: {err:?}");
                        }
                    }
                }
            }
        }
    });
}

async fn submit_batch(
    signer: &Arc<Signer>,
    signer_account: &AccountId,
    token: &AccountId,
    batch: &[&TransferReq],
    network: &NetworkConfig,
) -> Result<String> {
    let mut actions = Vec::with_capacity(batch.len());

    for transfer in batch {
        let args = json!({
            "receiver_id": transfer.receiver_id,
            "amount": transfer.amount,
            "memo": transfer.memo
        })
        .to_string()
        .into_bytes();

        actions.push(Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ft_transfer".to_string(),
            args,
            gas: FT_TRANSFER_GAS_PER_ACTION,
            deposit: FT_TRANSFER_DEPOSIT,
        })));
    }

    let tx_hash = submit_with_nonce_retry(signer, signer_account, token, actions, network).await?;
    info!("tx submitted: {} (actions={})", tx_hash, batch.len());

    Ok(tx_hash)
}

async fn submit_with_nonce_retry(
    signer: &Arc<Signer>,
    signer_account: &AccountId,
    token: &AccountId,
    actions: Vec<Action>,
    network: &NetworkConfig,
) -> Result<String> {
    let mut nonce_retry = 0;

    loop {
        let result = Transaction::construct(signer_account.clone(), token.clone())
            .add_actions(actions.clone())
            .with_signer(signer.clone())
            .send_to(network)
            .await;

        match result {
            Ok(tx) => {
                return Ok(tx.transaction_outcome.id.to_string());
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
