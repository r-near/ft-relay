use anyhow::Result;
use log::{debug, info, warn};
use redis::aio::ConnectionManager;
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
            Ok(b) => b,
            Err(e) => {
                debug!("No registration messages: {:?}", e);
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
                // Re-enqueue with retry
                for (stream_id, msg) in &batch {
                    let _ = rh::ack_message(&mut conn, &stream_key, &consumer_group, stream_id).await;
                    let retry_count = msg.retry_count + 1;
                    if retry_count < MAX_RETRIES {
                        let _ = rh::enqueue_registration(
                            &mut conn,
                            &ctx.runtime.env,
                            &msg.transfer_id,
                            retry_count,
                        )
                        .await;
                    } else {
                        let _ = rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Failed).await;
                        let _ = rh::log_event(
                            &mut conn,
                            &msg.transfer_id,
                            Event::new("FAILED").with_reason(format!(
                                "Registration failed after {} retries",
                                retry_count
                            )),
                        )
                        .await;
                    }
                }
            }
        }
    }
}

/// Process a batch of registration requests with deduplication
async fn process_registration_batch(
    ctx: &RegistrationWorkerContext,
    batch: &[(String, RegistrationMessage)],
    stream_key: &str,
    consumer_group: &str,
) -> Result<()> {
    let mut conn = ctx.runtime.redis_conn.clone();

    // Group transfers by account (deduplication within batch)
    let mut account_to_transfers: HashMap<AccountId, Vec<(String, RegistrationMessage)>> = HashMap::new();
    
    for (stream_id, msg) in batch {
        let transfer = match rh::get_transfer_state(&mut conn, &msg.transfer_id).await? {
            Some(t) => t,
            None => {
                warn!("Transfer {} not found", msg.transfer_id);
                rh::ack_message(&mut conn, stream_key, consumer_group, stream_id).await?;
                continue;
            }
        };

        account_to_transfers
            .entry(transfer.receiver_id.clone())
            .or_insert_with(Vec::new)
            .push((stream_id.clone(), msg.clone()));
    }

    if account_to_transfers.is_empty() {
        return Ok(());
    }

    // Separate already-registered (fast path) from needs-registration
    let mut fast_path_accounts = Vec::new();
    let mut needs_registration = Vec::new();

    for (account, transfers) in &account_to_transfers {
        if rh::is_account_registered(&mut conn, account).await? {
            debug!("Account {} already registered (fast path)", account);
            fast_path_accounts.push((account.clone(), transfers.clone()));
        } else {
            needs_registration.push((account.clone(), transfers.clone()));
        }
    }

    // Process fast path (already registered) - just enqueue for transfer
    for (_account, transfers) in fast_path_accounts {
        for (stream_id, msg) in transfers {
            rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Registered).await?;
            rh::log_event(&mut conn, &msg.transfer_id, Event::new("REGISTERED")).await?;
            rh::enqueue_transfer(&mut conn, &ctx.runtime.env, &msg.transfer_id, 0).await?;
            rh::log_event(&mut conn, &msg.transfer_id, Event::new("QUEUED_TRANSFER")).await?;
            rh::ack_message(&mut conn, stream_key, consumer_group, &stream_id).await?;
        }
    }

    if needs_registration.is_empty() {
        return Ok(());
    }

    // Batch register all unique accounts that need registration
    // Final check: re-verify accounts aren't registered (race condition protection)
    let mut final_needs_registration = Vec::new();
    let mut newly_registered = Vec::new();
    
    for (account, transfers) in needs_registration {
        if rh::is_account_registered(&mut conn, &account).await? {
            // Registered by another worker while we were processing
            debug!("Account {} registered by another worker during batch prep", account);
            newly_registered.push((account, transfers));
        } else {
            final_needs_registration.push((account, transfers));
        }
    }
    
    // Process accounts that got registered by other workers
    for (_account, transfers) in newly_registered {
        for (stream_id, msg) in transfers {
            rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Registered).await?;
            rh::log_event(&mut conn, &msg.transfer_id, Event::new("REGISTERED")).await?;
            rh::enqueue_transfer(&mut conn, &ctx.runtime.env, &msg.transfer_id, 0).await?;
            rh::log_event(&mut conn, &msg.transfer_id, Event::new("QUEUED_TRANSFER")).await?;
            rh::ack_message(&mut conn, stream_key, consumer_group, &stream_id).await?;
        }
    }
    
    if final_needs_registration.is_empty() {
        return Ok(());
    }
    
    let unique_accounts: Vec<AccountId> = final_needs_registration.iter().map(|(acc, _)| acc.clone()).collect();
    
    info!("Registering {} unique account(s) in batch", unique_accounts.len());

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
            unique_accounts.clone(),
            &leased_key.secret_key,
            nonce,
        )
        .await;

    drop(leased_key);

    match result {
        Ok((tx_hash, outcome)) => {
            let tx_hash_str = tx_hash.to_string();
            
            // Check if transaction succeeded (already Final)
            let is_success = matches!(
                outcome.status,
                near_primitives::views::FinalExecutionStatus::SuccessValue(_)
            );

            if !is_success {
                warn!("Registration batch tx {} has uncertain status - will retry", tx_hash_str);
                return Err(anyhow::anyhow!("Registration uncertain"));
            }

            info!("Registered {} accounts with tx {}", unique_accounts.len(), tx_hash_str);

            // Mark all accounts as registered and process their transfers
            for (account, transfers) in final_needs_registration {
                rh::mark_account_registered(&mut conn, &account).await?;

                for (stream_id, msg) in transfers {
                    rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Registered).await?;
                    rh::log_event(
                        &mut conn,
                        &msg.transfer_id,
                        Event::new("REGISTERED").with_tx_hash(tx_hash_str.clone()),
                    )
                    .await?;
                    rh::enqueue_transfer(&mut conn, &ctx.runtime.env, &msg.transfer_id, 0).await?;
                    rh::log_event(&mut conn, &msg.transfer_id, Event::new("QUEUED_TRANSFER")).await?;
                    rh::ack_message(&mut conn, stream_key, consumer_group, &stream_id).await?;
                }
            }

            Ok(())
        }
        Err(e) => {
            let err_str = format!("{:?}", e);

            // If accounts are already registered on-chain, treat as success
            if err_str.contains("already") || err_str.contains("exist") {
                info!("Batch registration: accounts already registered (on-chain)");

                for (account, transfers) in final_needs_registration {
                    rh::mark_account_registered(&mut conn, &account).await?;

                    for (stream_id, msg) in transfers {
                        rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Registered).await?;
                        rh::log_event(&mut conn, &msg.transfer_id, Event::new("REGISTERED")).await?;
                        rh::enqueue_transfer(&mut conn, &ctx.runtime.env, &msg.transfer_id, 0).await?;
                        rh::log_event(&mut conn, &msg.transfer_id, Event::new("QUEUED_TRANSFER")).await?;
                        rh::ack_message(&mut conn, stream_key, consumer_group, &stream_id).await?;
                    }
                }

                Ok(())
            } else {
                warn!("Failed to register batch: {:?}", e);
                Err(e)
            }
        }
    }
}
