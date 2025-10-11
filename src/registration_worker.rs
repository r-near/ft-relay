use anyhow::Result;
use log::{debug, info, warn};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::access_key_pool::AccessKeyPool;
use crate::nonce_manager::NonceManager;
use crate::redis_helpers as rh;
use crate::rpc_client::NearRpcClient;
use crate::types::{AccountId, Event, RegistrationMessage, Status};

const MAX_RETRIES: u32 = 10;
const MAX_REGISTRATIONS_PER_BATCH: usize = 50;

pub struct RegistrationWorkerRuntime {
    pub redis_conn: ConnectionManager,
    pub rpc_client: Arc<NearRpcClient>,
    pub access_key_pool: Arc<AccessKeyPool>,
    pub nonce_manager: NonceManager,
    pub relay_account: AccountId,
    pub token: AccountId,
    pub env: String,
}

pub struct RegistrationWorkerContext {
    pub runtime: Arc<RegistrationWorkerRuntime>,
    pub linger_ms: u64,
}

pub async fn registration_worker_loop(ctx: RegistrationWorkerContext) -> Result<()> {
    let consumer_name = format!("registration-{}", Uuid::new_v4());
    let stream_key = format!("ftrelay:{}:reg", ctx.runtime.env);
    let consumer_group = format!("ftrelay:{}:reg_workers", ctx.runtime.env);

    let mut conn = ctx.runtime.redis_conn.clone();
    let _: Result<String, _> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream_key)
        .arg(&consumer_group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await;

    info!("Registration worker {} started", consumer_name);

    loop {
        let mut conn = ctx.runtime.redis_conn.clone();

        debug!(
            "[REG_WORKER] {} polling for batch (linger: {}ms)",
            consumer_name, ctx.linger_ms
        );

        let batch: Vec<(String, RegistrationMessage)> = match rh::pop_batch(
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
                warn!("[REG_WORKER] {} error popping batch: {:?}", consumer_name, e);
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
                // Re-enqueue account registration jobs with retry
                for (stream_id, msg) in &batch {
                    let _ =
                        rh::ack_message(&mut conn, &stream_key, &consumer_group, stream_id).await;
                    let retry_count = msg.retry_count + 1;
                    if retry_count < MAX_RETRIES {
                        // Re-queue the account registration job
                        let registration_msg = RegistrationMessage {
                            account: msg.account.clone(),
                            retry_count,
                        };
                        let serialized = serde_json::to_string(&registration_msg).unwrap();
                        let _: Result<String, _> = conn
                            .xadd::<_, _, _, _, String>(
                                &stream_key,
                                "*",
                                &[("data", serialized.as_str())],
                            )
                            .await;
                    } else {
                        warn!(
                            "Account {} registration failed after {} retries",
                            msg.account, retry_count
                        );
                        // Note: Waiting transfers will remain in waiting list
                        // They can be manually recovered or will timeout
                    }
                }
            }
        }
    }
}

/// Process a batch of account registration jobs - SIMPLE VERSION
/// Queue is naturally deduped by Lua script, so just register all and forward!
async fn process_registration_batch(
    ctx: &RegistrationWorkerContext,
    batch: &[(String, RegistrationMessage)],
    stream_key: &str,
    consumer_group: &str,
) -> Result<()> {
    let mut conn = ctx.runtime.redis_conn.clone();

    // Extract unique accounts from batch (only for deduping retries)
    let mut account_to_jobs: HashMap<AccountId, Vec<String>> = HashMap::new();
    for (stream_id, msg) in batch {
        account_to_jobs
            .entry(msg.account.clone())
            .or_insert_with(Vec::new)
            .push(stream_id.clone());
    }

    let accounts: Vec<AccountId> = account_to_jobs.keys().cloned().collect();
    info!(
        "[REG_WORKER] Registering {} account(s) in batch: {:?}",
        accounts.len(),
        accounts
    );

    // Register all accounts in one batch transaction (idempotent - blockchain handles "already exists")
    let leased_key = ctx.runtime.access_key_pool.lease().await?;
    let nonce = ctx
        .runtime
        .nonce_manager
        .clone()
        .get_next_nonce(&leased_key.key_id)
        .await?;

    let result = ctx
        .runtime
        .rpc_client
        .register_accounts_batch(
            &ctx.runtime.relay_account,
            &ctx.runtime.token,
            accounts.clone(),
            &leased_key.secret_key,
            nonce,
        )
        .await;

    drop(leased_key);

    // Check if registration succeeded (or was already registered)
    match result {
        Ok((tx_hash, outcome)) => {
            let is_success = matches!(
                outcome.status,
                near_primitives::views::FinalExecutionStatus::SuccessValue(_)
            );

            if !is_success {
                warn!("Registration batch tx {} failed - will retry", tx_hash);
                return Err(anyhow::anyhow!("Registration failed"));
            }

            info!(
                "[REG_WORKER] ✅ Registered {} accounts with tx {}",
                accounts.len(),
                tx_hash
            );
        }
        Err(e) => {
            let err_str = format!("{:?}", e);
            // "Already exists" errors are fine - account was registered by another worker
            if !err_str.contains("already") && !err_str.contains("exist") {
                return Err(anyhow::anyhow!("Failed to register accounts: {:?}", e));
            }
            debug!("Some accounts already registered (idempotent)");
        }
    }

    // Atomically complete registration for all accounts (Lua script - no races!)
    // Collect all waiting transfers across accounts and forward them in one pipelined shot
    let mut all_waiting: Vec<String> = Vec::new();
    for (account, job_ids) in account_to_jobs {
        // Lua script atomically: marks registered + gets waiting transfers + cleans up
        let waiting_transfers = rh::complete_account_registration(&mut conn, &account).await?;

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

        // Ack all registration jobs for this account
        for job_id in job_ids {
            rh::ack_message(&mut conn, stream_key, consumer_group, &job_id).await?;
        }
    }

    if !all_waiting.is_empty() {
        let start = std::time::Instant::now();
        info!(
            "[REG_WORKER] Forwarding {} waiting transfers across registered accounts",
            all_waiting.len()
        );
        rh::forward_transfers_batch(&mut conn, &ctx.runtime.env, &all_waiting).await?;
        let dur = start.elapsed();
        info!(
            "[REG_WORKER] Forwarded {} transfers in {}ms",
            all_waiting.len(),
            dur.as_millis()
        );
    }

    Ok(())
}

/// Helper: Forward a transfer to the ready-for-transfer stream
async fn forward_transfer_to_ready_stream(
    ctx: &RegistrationWorkerContext,
    conn: &mut redis::aio::ConnectionManager,
    transfer_id: &str,
) -> Result<()> {
    rh::update_transfer_status(conn, transfer_id, Status::Registered).await?;
    rh::log_event(conn, transfer_id, Event::new("REGISTERED")).await?;
    rh::enqueue_transfer(conn, &ctx.runtime.env, transfer_id, 0).await?;
    rh::log_event(conn, transfer_id, Event::new("QUEUED_TRANSFER")).await?;
    Ok(())
}
