use anyhow::Result;
use log::{info, warn};
use near_api::{NetworkConfig, Signer};
use near_api_types::AccountId;
use near_primitives::action::{Action, FunctionCallAction};
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use uuid;

use crate::{
    config::{MAX_GAS_PER_TX, STORAGE_DEPOSIT_AMOUNT, STORAGE_DEPOSIT_GAS_PER_ACTION},
    queue::TransferQueue,
    transfer_states::Transfer,
};

const MAX_REGISTRATIONS_PER_TX: usize = (MAX_GAS_PER_TX / STORAGE_DEPOSIT_GAS_PER_ACTION) as usize; // ~60 with 5 TGas each

pub struct RegistrationWorkerRuntime {
    pub queue: Arc<TransferQueue>,
    pub signer: Arc<Signer>,
    pub signer_account: AccountId,
    pub token: AccountId,
    pub network: NetworkConfig,
}

pub struct RegistrationWorkerContext {
    pub runtime: Arc<RegistrationWorkerRuntime>,
    pub poll_interval_ms: u64,
}

/// Dedicated worker that processes storage deposits
/// Uses Redis Streams with consumer groups for distributed work
pub async fn registration_worker_loop(ctx: RegistrationWorkerContext) -> Result<()> {
    let consumer_name = format!("registration-{}", uuid::Uuid::new_v4());
    info!("registration worker started: {}", consumer_name);

    loop {
        // Pop a batch of registration requests from stream (with PEL autoclaim)
        let batch = ctx.runtime.queue
            .pop_registration_batch(&consumer_name, MAX_REGISTRATIONS_PER_TX)
            .await?;

        if batch.is_empty() {
            // No registration requests, sleep briefly
            tokio::time::sleep(Duration::from_millis(ctx.poll_interval_ms)).await;
            continue;
        }

        info!(
            "claimed {} registration request(s)",
            batch.len()
        );

        process_registration_batch(&ctx.runtime, batch).await;
    }
}

async fn process_registration_batch(
    runtime: &Arc<RegistrationWorkerRuntime>,
    batch: Vec<(String, String)>,
) {
    let runtime = runtime.clone();
    tokio::spawn(async move {
        if let Err(err) = process_registration_batch_inner(&runtime, batch).await {
            warn!("registration batch failed: {err:?}");
            // Failed batches will be autoclaimed from PEL later
        }
    });
}

async fn process_registration_batch_inner(
    runtime: &Arc<RegistrationWorkerRuntime>,
    batch: Vec<(String, String)>,
) -> Result<()> {
    info!("registering {} account(s)", batch.len());

    // Extract account_ids and redis_ids
    let redis_ids: Vec<String> = batch.iter().map(|(id, _)| id.clone()).collect();
    let account_ids: Vec<String> = batch.iter().map(|(_, acc)| acc.clone()).collect();

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
            warn!(
                "storage_deposit tx failed: {} - {:?}",
                tx_hash, err
            );
            return Err(anyhow::anyhow!("storage_deposit tx failed: {:?}", err));
        }
        status => {
            warn!("storage_deposit tx unexpected status: {:?}", status);
            return Err(anyhow::anyhow!("unexpected tx status: {:?}", status));
        }
    }

    // Only pop pending transfers if tx succeeded
    // For each account, move pending transfers to ready stream
    for account_id in &unique_accounts {
        // Get all pending transfers for this account
        let pending_transfers = runtime
            .queue
            .pop_pending_transfers(account_id)
            .await?;

        if pending_transfers.is_empty() {
            continue;
        }

        info!(
            "moving {} pending transfer(s) for {} to ready stream",
            pending_transfers.len(),
            account_id
        );

        // Transform to ReadyToSend and enqueue
        let ready_transfers: Vec<Transfer<crate::transfer_states::ReadyToSend>> =
            pending_transfers
                .into_iter()
                .map(|t| t.mark_registered())
                .collect();

        // Mark as registered and enqueue all transfers atomically
        runtime
            .queue
            .mark_registered_and_enqueue(account_id, ready_transfers)
            .await?;
    }

    // ACK all registration requests after successful processing
    runtime.queue.ack_registrations(&redis_ids).await?;

    Ok(())
}
