use std::sync::Arc;

use anyhow::{anyhow, Result};
use log::{info, warn};
use near_api::{NetworkConfig, Signer, Transaction};
use near_api_types::AccountId;
use near_primitives::action::{Action, FunctionCallAction};
use serde_json::json;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{
    config::{FT_TRANSFER_DEPOSIT, FT_TRANSFER_GAS_PER_ACTION, MAX_TOTAL_PREPAID_GAS},
    redis_queue::{RedisContext, RedisWorker, StreamMessage},
    types::TransferReq,
};

const REDIS_CLAIM_MIN_IDLE_MS: u64 = 1;
const MAX_RETRY_ATTEMPTS: u32 = 10;

#[derive(Clone)]
pub struct WorkerContext {
    pub redis: Arc<RedisContext>,
    pub signer: Arc<Signer>,
    pub signer_account: AccountId,
    pub token: AccountId,
    pub network: NetworkConfig,
    pub batch_size: usize,
    pub linger_ms: u64,
    pub semaphore: Arc<Semaphore>,
}

pub async fn worker_loop(ctx: WorkerContext) -> Result<()> {
    let consumer_name = format!("relay-{}", Uuid::new_v4());
    let mut worker = RedisWorker::new(ctx.redis.clone(), consumer_name).await?;

    let reclaimed = worker
        .claim_stale(REDIS_CLAIM_MIN_IDLE_MS, ctx.batch_size)
        .await?;
    if !reclaimed.is_empty() {
        info!("claimed {} stale transfers from Redis", reclaimed.len());
        dispatch_messages(reclaimed, &ctx);
    }

    loop {
        let messages = worker.read_batch(ctx.batch_size, ctx.linger_ms).await?;
        if messages.is_empty() {
            continue;
        }

        dispatch_messages(messages, &ctx);
    }
}

fn dispatch_messages(mut messages: Vec<StreamMessage>, ctx: &WorkerContext) {
    while !messages.is_empty() {
        let take = std::cmp::min(ctx.batch_size, messages.len());
        let chunk: Vec<StreamMessage> = messages.drain(..take).collect();
        log::debug!(
            "dispatching chunk with {} transfers (queue_remaining={})",
            chunk.len(),
            messages.len()
        );
        let ctx_clone = ctx.clone();

        tokio::spawn(async move {
            let semaphore = ctx_clone.semaphore.clone();
            let permit = semaphore.acquire_owned().await.unwrap();
            if let Err(err) = process_chunk(chunk, ctx_clone).await {
                warn!("chunk processing failed: {err:?}");
            }
            drop(permit);
        });
    }
}

async fn process_chunk(mut chunk: Vec<StreamMessage>, ctx: WorkerContext) -> Result<()> {
    if chunk.is_empty() {
        return Ok(());
    }

    let mut requests = Vec::with_capacity(chunk.len());
    for msg in chunk.iter_mut() {
        msg.transfer.attempts += 1;
        requests.push(msg.transfer.clone());
    }

    match transfer_batch(
        ctx.signer.clone(),
        ctx.signer_account.clone(),
        ctx.token.clone(),
        &requests,
        &ctx.network,
    )
    .await
    {
        Ok(tx_hash) => {
            log::debug!("submitted {} transfers", requests.len());
            let transfer_ids: Vec<String> = chunk
                .iter()
                .map(|m| m.transfer.transfer_id.clone())
                .collect();
            let redis_ids: Vec<String> = chunk.into_iter().map(|m| m.redis_id).collect();

            // Store tx_hash for all transfer_ids in this batch
            if let Err(err) = ctx.redis.set_transfer_status(&transfer_ids, &tx_hash).await {
                warn!("failed to set transfer status: {err:?}");
            }

            ctx.redis.ack(&redis_ids).await?;
        }
        Err(err) => {
            warn!("batch submission failed: {err:?}");
            let mut retryable = Vec::new();
            let mut terminal_ids = Vec::new();

            for msg in chunk.into_iter() {
                let attempts = msg.transfer.attempts;
                let reached_limit = attempts >= MAX_RETRY_ATTEMPTS;

                if reached_limit {
                    warn!(
                        "transfer {} reached max retries ({}/{}), marking as terminal",
                        msg.transfer.transfer_id, attempts, MAX_RETRY_ATTEMPTS
                    );
                    terminal_ids.push(msg.redis_id);
                } else {
                    log::debug!(
                        "transfer {} will retry (attempt {}/{})",
                        msg.transfer.transfer_id, attempts, MAX_RETRY_ATTEMPTS
                    );
                    retryable.push(msg);
                }
            }

            if !terminal_ids.is_empty() {
                info!("acking {} terminal transfer(s)", terminal_ids.len());
                ctx.redis.ack(&terminal_ids).await?;
            }

            if !retryable.is_empty() {
                info!("requeuing {} transfer(s) for retry", retryable.len());
                ctx.redis.requeue(retryable).await?;
            }
        }
    }

    Ok(())
}

async fn transfer_batch(
    signer: Arc<Signer>,
    signer_account: AccountId,
    token: AccountId,
    batch: &[TransferReq],
    network: &NetworkConfig,
) -> Result<String> {
    if FT_TRANSFER_GAS_PER_ACTION > MAX_TOTAL_PREPAID_GAS {
        return Err(anyhow!(
            "configured gas per action ({}) exceeds transaction prepaid gas limit ({})",
            FT_TRANSFER_GAS_PER_ACTION,
            MAX_TOTAL_PREPAID_GAS
        ));
    }

    let max_actions_per_tx = std::cmp::max(
        1,
        (MAX_TOTAL_PREPAID_GAS / FT_TRANSFER_GAS_PER_ACTION) as usize,
    );

    let mut last_tx_hash = String::new();

    for (chunk_idx, chunk) in batch.chunks(max_actions_per_tx).enumerate() {
        let mut actions = Vec::with_capacity(chunk.len());
        for r in chunk {
            let args = json!({
                "receiver_id": r.receiver_id,
                "amount": r.amount,
                "memo": r.memo
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

        // Retry loop for nonce issues
        const MAX_NONCE_RETRIES: u32 = 3;
        let mut nonce_retry = 0;

        loop {
            let result = Transaction::construct(signer_account.clone(), token.clone())
                .add_actions(actions.clone())
                .with_signer(signer.clone())
                .send_to(network)
                .await;

            match result {
                Ok(tx) => {
                    last_tx_hash = tx.transaction_outcome.id.to_string();
                    info!(
                        "tx submitted: {} (chunk={} actions={})",
                        last_tx_hash,
                        chunk_idx + 1,
                        chunk.len()
                    );
                    break;
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

                        // Force nonce resync by calling fetch_tx_nonce
                        // This will fetch current nonce from chain and update internal cache with fetch_max
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

    Ok(last_tx_hash)
}
