use ::redis::aio::ConnectionManager;
use ::redis::AsyncCommands;
use anyhow::{anyhow, Result};
use log::{debug, info, warn};
use near_kit::{Gas, Near, NearToken, TxExecutionStatus};
use std::collections::HashMap;
use std::sync::Arc;

use crate::redis::{accounts, events, keys, state, streams, transfers};
use crate::types::{AccountId, Event, RegistrationMessage, Status};

use super::shared;

const MAX_RETRIES: u32 = 10;
const MAX_REGISTRATIONS_PER_BATCH: usize = 50;

// Gas per storage_deposit action
const STORAGE_DEPOSIT_GAS: Gas = Gas::tgas(5);
// Storage deposit amount (0.00125 NEAR = 1,250,000,000,000,000,000,000 yocto)
const STORAGE_DEPOSIT_AMOUNT: NearToken = NearToken::yocto(1_250_000_000_000_000_000_000);

pub struct RegistrationWorkerRuntime {
    pub redis_conn: ConnectionManager,
    pub near: Arc<Near>,
    pub relay_account: AccountId,
    pub token: AccountId,
    pub env: String,
}

pub struct RegistrationWorkerContext {
    pub runtime: Arc<RegistrationWorkerRuntime>,
    pub linger_ms: u64,
}

pub async fn registration_worker_loop(ctx: RegistrationWorkerContext) -> Result<()> {
    let consumer_name = shared::consumer_name("registration");
    let stream_key = keys::registration_stream(&ctx.runtime.env);
    let consumer_group = keys::registration_consumer_group(&ctx.runtime.env);

    let mut conn = ctx.runtime.redis_conn.clone();
    shared::ensure_consumer_group(&mut conn, &stream_key, &consumer_group).await?;

    info!("Registration worker {} started", consumer_name);

    loop {
        let mut conn = ctx.runtime.redis_conn.clone();

        debug!(
            "[REG_WORKER] {} polling for batch (linger: {}ms)",
            consumer_name, ctx.linger_ms
        );

        let batch: Vec<(String, RegistrationMessage)> = match streams::pop_batch(
            &mut conn,
            &stream_key,
            &consumer_group,
            &consumer_name,
            MAX_REGISTRATIONS_PER_BATCH,
            ctx.linger_ms,
        )
        .await
        {
            Ok(b) => {
                if !b.is_empty() {
                    debug!(
                        "[REG_WORKER] {} got batch of {} registration job(s)",
                        consumer_name,
                        b.len()
                    );
                }
                b
            }
            Err(e) => {
                warn!(
                    "[REG_WORKER] {} error popping batch: {:?}",
                    consumer_name, e
                );
                continue;
            }
        };

        if batch.is_empty() {
            continue;
        }

        match process_registration_batch(&ctx, &batch, &stream_key, &consumer_group).await {
            Ok(()) => {}
            Err(e) => {
                warn!("Batch processing error: {:?}", e);
                for (stream_id, msg) in &batch {
                    let _ =
                        streams::ack_message(&mut conn, &stream_key, &consumer_group, stream_id)
                            .await;
                    let retry_count = msg.retry_count + 1;
                    if retry_count < MAX_RETRIES {
                        let registration_msg = RegistrationMessage {
                            account: msg.account.clone(),
                            retry_count,
                        };
                        let serialized = serde_json::to_string(&registration_msg).unwrap();
                        let _: Result<String, _> = conn
                            .xadd(&stream_key, "*", &[("data", serialized.as_str())])
                            .await;
                    } else {
                        warn!(
                            "Account {} registration failed after {} retries",
                            msg.account, retry_count
                        );
                    }
                }
            }
        }
    }
}

async fn process_registration_batch(
    ctx: &RegistrationWorkerContext,
    batch: &[(String, RegistrationMessage)],
    stream_key: &str,
    consumer_group: &str,
) -> Result<()> {
    let mut conn = ctx.runtime.redis_conn.clone();

    let mut account_to_jobs: HashMap<AccountId, Vec<String>> = HashMap::new();
    for (stream_id, msg) in batch {
        account_to_jobs
            .entry(msg.account.clone())
            .or_default()
            .push(stream_id.clone());
    }

    let accounts: Vec<AccountId> = account_to_jobs.keys().cloned().collect();
    info!(
        "[REG_WORKER] Registering {} account(s) in batch: {:?}",
        accounts.len(),
        accounts
    );

    if accounts.is_empty() {
        return Ok(());
    }

    // Build a multi-action transaction with all storage_deposit calls
    let mut call_builder = ctx
        .runtime
        .near
        .transaction(&ctx.runtime.token)
        .call("storage_deposit")
        .args(serde_json::json!({
            "account_id": &accounts[0],
            "registration_only": true,
        }))
        .gas(STORAGE_DEPOSIT_GAS)
        .deposit(STORAGE_DEPOSIT_AMOUNT);

    for account in accounts.iter().skip(1) {
        let args = serde_json::json!({
            "account_id": account,
            "registration_only": true,
        });

        call_builder = call_builder
            .call("storage_deposit")
            .args(args)
            .gas(STORAGE_DEPOSIT_GAS)
            .deposit(STORAGE_DEPOSIT_AMOUNT);
    }

    let result = call_builder
        .wait_until(TxExecutionStatus::Final)
        .send()
        .await;

    match result {
        Ok(outcome) => {
            let tx_hash = outcome
                .transaction_hash()
                .map(|h| h.to_string())
                .unwrap_or_else(|| "unknown".to_string());

            if !outcome.is_success() {
                warn!("Registration batch tx {} failed - will retry", tx_hash);
                return Err(anyhow!("Registration failed"));
            }

            info!(
                "[REG_WORKER] Registered {} accounts with tx {}",
                accounts.len(),
                tx_hash
            );
        }
        Err(e) => {
            let err_str = format!("{:?}", e);
            // If accounts are already registered, that's fine (idempotent)
            if !err_str.contains("already") && !err_str.contains("exist") {
                return Err(anyhow!("Failed to register accounts: {:?}", e));
            }
            debug!("Some accounts already registered (idempotent)");
        }
    }

    let mut all_waiting: Vec<String> = Vec::new();
    for (account, job_ids) in account_to_jobs {
        let waiting_transfers =
            accounts::complete_account_registration(&mut conn, &account).await?;

        if !waiting_transfers.is_empty() {
            info!(
                "[REG_WORKER] Account {} has {} waiting transfers",
                account,
                waiting_transfers.len()
            );
        } else {
            debug!("[REG_WORKER] Account {} has no waiting transfers", account);
        }

        all_waiting.extend(waiting_transfers);

        for job_id in job_ids {
            streams::ack_message(&mut conn, stream_key, consumer_group, &job_id).await?;
        }
    }

    if !all_waiting.is_empty() {
        let start = std::time::Instant::now();
        info!(
            "[REG_WORKER] Forwarding {} waiting transfers across registered accounts",
            all_waiting.len()
        );
        transfers::forward_transfers_batch(&mut conn, &ctx.runtime.env, &all_waiting).await?;
        let dur = start.elapsed();
        info!(
            "[REG_WORKER] Forwarded {} transfers in {}ms",
            all_waiting.len(),
            dur.as_millis()
        );
    }

    Ok(())
}

#[allow(dead_code)]
async fn forward_transfer_to_ready_stream(
    ctx: &RegistrationWorkerContext,
    conn: &mut ConnectionManager,
    transfer_id: &str,
) -> Result<()> {
    state::update_transfer_status(conn, transfer_id, Status::Registered).await?;
    events::log_event(conn, transfer_id, Event::new("REGISTERED")).await?;
    transfers::enqueue_transfer(conn, &ctx.runtime.env, transfer_id, 0).await?;
    events::log_event(conn, transfer_id, Event::new("QUEUED_TRANSFER")).await?;
    Ok(())
}
