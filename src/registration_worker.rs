use anyhow::Result;
use log::{info, warn};
use near_api::{NetworkConfig, Signer};
use near_api_types::AccountId;
use near_primitives::action::{Action, FunctionCallAction};
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

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
/// Multiple workers can run concurrently since storage_deposit is idempotent
pub async fn registration_worker_loop(ctx: RegistrationWorkerContext) -> Result<()> {
    info!("registration worker started (poll interval: {}ms)", ctx.poll_interval_ms);

    loop {
        // Get all accounts needing registration
        let accounts = ctx.runtime.queue.get_accounts_needing_registration().await?;

        if accounts.is_empty() {
            // No accounts need registration, sleep briefly
            tokio::time::sleep(Duration::from_millis(ctx.poll_interval_ms)).await;
            continue;
        }

        info!(
            "found {} account(s) needing registration",
            accounts.len()
        );

        // Process in batches of up to MAX_REGISTRATIONS_PER_TX
        for batch in accounts.chunks(MAX_REGISTRATIONS_PER_TX) {
            if let Err(err) = process_registration_batch(&ctx.runtime, batch).await {
                warn!("failed to process registration batch: {err:?}");
                // Continue to next batch even if this one fails
            }
        }
    }
}

async fn process_registration_batch(
    runtime: &Arc<RegistrationWorkerRuntime>,
    accounts: &[String],
) -> Result<()> {
    info!("registering {} account(s)", accounts.len());

    // Build transaction with storage_deposit actions
    let mut actions = Vec::new();
    let mut unique_accounts: HashSet<&str> = HashSet::new();

    for account_id in accounts {
        // Deduplicate within batch
        if unique_accounts.contains(account_id.as_str()) {
            continue;
        }
        unique_accounts.insert(account_id);

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
    for account_id in unique_accounts {
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

    Ok(())
}
