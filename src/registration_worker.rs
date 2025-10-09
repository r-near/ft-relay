use anyhow::Result;
use log::{info, warn};
use near_api::{NetworkConfig, Signer};
use near_api_types::AccountId;
use near_primitives::action::{Action, FunctionCallAction};
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    config::{MAX_GAS_PER_TX, STORAGE_DEPOSIT_AMOUNT, STORAGE_DEPOSIT_GAS_PER_ACTION},
    redis_helpers,
    stream_queue::{RegistrationRequest, StreamQueue},
};

const MAX_REGISTRATIONS_PER_TX: usize =
    (MAX_GAS_PER_TX / STORAGE_DEPOSIT_GAS_PER_ACTION) as usize;

pub struct RegistrationWorkerRuntime {
    pub redis_client: redis::Client,
    pub registration_queue: Arc<StreamQueue<RegistrationRequest>>,
    pub ready_queue: Arc<StreamQueue<crate::transfer_states::Transfer<crate::transfer_states::ReadyToSend>>>,
    pub signer: Arc<Signer>,
    pub signer_account: AccountId,
    pub token: AccountId,
    pub network: NetworkConfig,
}

pub struct RegistrationWorkerContext {
    pub runtime: Arc<RegistrationWorkerRuntime>,
    pub linger_ms: u64,
}

/// Registration worker - processes registration requests from the stream
pub async fn registration_worker_loop(ctx: RegistrationWorkerContext) -> Result<()> {
    let consumer_name = format!("registration-{}", ::uuid::Uuid::new_v4());
    info!("registration worker started: {}", consumer_name);

    loop {
        // Pop a batch of registration requests from stream (with PEL autoclaim + lingering)
        let batch = ctx
            .runtime
            .registration_queue
            .pop_batch(&consumer_name, MAX_REGISTRATIONS_PER_TX, ctx.linger_ms)
            .await?;

        if !batch.is_empty() {
            info!("claimed {} registration request(s)", batch.len());
            process_registration_batch(&ctx.runtime, batch).await;
        }
    }
}

async fn process_registration_batch(
    runtime: &Arc<RegistrationWorkerRuntime>,
    batch: Vec<(String, RegistrationRequest)>,
) {
    let runtime = runtime.clone();
    tokio::spawn(async move {
        if let Err(err) = process_registration_batch_inner(&runtime, &batch).await {
            warn!("registration batch failed: {err:?}");
            handle_registration_retry(&runtime, &batch).await;
        }
    });
}

async fn process_registration_batch_inner(
    runtime: &Arc<RegistrationWorkerRuntime>,
    batch: &[(String, RegistrationRequest)],
) -> Result<()> {
    info!("registering {} account(s)", batch.len());

    // Extract account_ids and redis_ids
    let redis_ids: Vec<String> = batch.iter().map(|(id, _)| id.clone()).collect();
    let account_ids: Vec<String> = batch
        .iter()
        .map(|(_, req)| req.account_id.clone())
        .collect();

    // Build transaction with storage_deposit actions
    let mut actions = Vec::new();
    let mut unique_accounts: HashSet<String> = HashSet::new();

    for account_id in &account_ids {
        // Deduplicate within batch
        if unique_accounts.contains(account_id) {
            continue;
        }
        unique_accounts.insert(account_id.clone());

        let deposit_args = json!({
            "account_id": account_id,
            "registration_only": true
        })
        .to_string()
        .into_bytes();

        actions.push(Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "storage_deposit".to_string(),
            args: deposit_args,
            gas: STORAGE_DEPOSIT_GAS_PER_ACTION,
            deposit: STORAGE_DEPOSIT_AMOUNT,
        })));
    }

    // Submit transaction
    let tx = near_api::Transaction::construct(
        runtime.signer_account.clone(),
        runtime.token.clone(),
    )
    .add_actions(actions)
    .with_signer(runtime.signer.clone())
    .send_to(&runtime.network)
    .await?;

    let tx_hash = tx.transaction_outcome.id.to_string();

    // Check if transaction succeeded
    use near_primitives::views::FinalExecutionStatus;
    match &tx.status {
        FinalExecutionStatus::SuccessValue(_) => {
            info!(
                "storage_deposit tx succeeded: {} ({} accounts)",
                tx_hash,
                unique_accounts.len()
            );
        }
        FinalExecutionStatus::Failure(err) => {
            warn!("storage_deposit tx failed: {} - {:?}", tx_hash, err);
            return Err(anyhow::anyhow!("storage_deposit tx failed: {:?}", err));
        }
        status => {
            warn!("storage_deposit tx unexpected status: {:?}", status);
            return Err(anyhow::anyhow!("unexpected tx status: {:?}", status));
        }
    }

    // Only pop pending transfers if tx succeeded
    let mut conn = runtime.redis_client.get_multiplexed_async_connection().await?;
    let token_str = runtime.token.to_string();

    // For each account, move pending transfers to ready stream
    for account_id in &unique_accounts {
        // Get all pending transfers for this account
        let pending_transfers =
            redis_helpers::pop_pending_transfers(&mut conn, &token_str, account_id).await?;

        if pending_transfers.is_empty() {
            continue;
        }

        info!(
            "moving {} pending transfer(s) for {} to ready stream",
            pending_transfers.len(),
            account_id
        );

        // Transform to ReadyToSend
        let ready_transfers: Vec<crate::transfer_states::Transfer<crate::transfer_states::ReadyToSend>> = pending_transfers
            .into_iter()
            .map(|t| t.mark_registered())
            .collect();

        // Mark as registered and enqueue all transfers atomically
        redis_helpers::mark_registered_and_push_to_stream(
            &mut conn,
            &token_str,
            runtime.ready_queue.stream_key(),
            account_id,
            ready_transfers,
        )
        .await?;
    }

    // Remove accounts from pending registration set (cleanup for deduplication)
    let pending_reg_key = format!("registration_pending:{}", token_str);
    let account_ids_vec: Vec<&str> = unique_accounts.iter().map(|s| s.as_str()).collect();
    if !account_ids_vec.is_empty() {
        use redis::AsyncCommands;
        let _: () = conn.srem(&pending_reg_key, &account_ids_vec).await?;
    }

    // ACK all registration requests after successful processing
    runtime.registration_queue.ack(&redis_ids).await?;

    Ok(())
}

/// Handle failed registration attempts by explicitly re-enqueuing
/// Note: RegistrationRequest doesn't track attempts since registration is idempotent
async fn handle_registration_retry(
    runtime: &Arc<RegistrationWorkerRuntime>,
    batch: &[(String, RegistrationRequest)],
) {
    for (redis_id, request) in batch {
        // Re-push to registration queue for retry
        if let Err(err) = runtime.registration_queue.push(request).await {
            warn!(
                "failed to re-enqueue registration for {}: {err:?}",
                request.account_id
            );
        } else {
            info!("re-enqueued registration request for {}", request.account_id);
        }

        // ACK immediately - we've handled it explicitly
        if let Err(err) = runtime.registration_queue.ack(&[redis_id.clone()]).await {
            warn!("failed to ack registration after re-enqueue: {err:?}");
        }
    }
}
