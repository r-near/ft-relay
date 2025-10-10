use anyhow::Result;
use log::{debug, info, warn};
use near_api::NetworkConfig;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    observability::ObservabilityContext,
    redis_helpers,
    stream_queue::StreamQueue,
    transfer_states::{ReadyToSend, Transfer, TransferData},
};

const MAX_VERIFICATION_ATTEMPTS: u32 = 120; // Much higher since it's async

/// A batch of transfers with associated tx_hash awaiting verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingVerificationBatch {
    pub batch_id: String,
    pub tx_hash: String,
    pub attempts: u32,
    pub submitted_at: i64,
    pub transfer_data: Vec<TransferData>,
}

// Implement Display for better logging
impl std::fmt::Display for PendingVerificationBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Batch {} (tx: {}..., {} transfers, attempt {})",
            self.batch_id,
            &self.tx_hash[..16.min(self.tx_hash.len())],
            self.transfer_data.len(),
            self.attempts
        )
    }
}

/// Verify transaction status by querying the RPC directly
async fn verify_transaction_status(
    tx_hash: &str,
    network: &NetworkConfig,
) -> Result<bool> {
    let client = reqwest::Client::new();
    
    let rpc_url = network.rpc_endpoints.first()
        .ok_or_else(|| anyhow::anyhow!("No RPC endpoints configured"))?
        .url
        .to_string();
    
    let request_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "tx",
        "params": {
            "tx_hash": tx_hash,
            "sender_account_id": "dontcare"
        }
    });
    
    let response = client
        .post(&rpc_url)
        .json(&request_body)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;
    
    let json_response: serde_json::Value = response.json().await?;
    
    if let Some(result) = json_response.get("result") {
        if let Some(status) = result.get("status") {
            if status.get("SuccessValue").is_some() || status.get("SuccessReceiptId").is_some() {
                return Ok(true);
            }
            if status.get("Failure").is_some() {
                return Ok(false);
            }
        }
    }
    
    if let Some(_error) = json_response.get("error") {
        return Err(anyhow::anyhow!("Transaction status unknown"));
    }
    
    Err(anyhow::anyhow!("Unable to parse transaction status"))
}

pub struct StatusCheckerRuntime {
    pub redis_conn: redis::aio::ConnectionManager,
    pub ready_queue: Arc<StreamQueue<Transfer<ReadyToSend>>>,
    pub network: NetworkConfig,
    pub obs_ctx: Arc<ObservabilityContext>,
    pub token: near_api_types::AccountId,
}

async fn check_pending_verification(
    runtime: Arc<StatusCheckerRuntime>,
    pending: PendingVerificationBatch,
) {
    info!("🔍 Status checker: Verifying {}", pending);

    match verify_transaction_status(&pending.tx_hash, &runtime.network).await {
        Ok(true) => {
            // Transaction succeeded on-chain
            runtime.obs_ctx.inc_metric("succeeded").await;
            info!("✅ Status checker: {} verified SUCCEEDED on-chain", pending);

            // Mark transfers as completed
            let mut conn = runtime.redis_conn.clone();
            let token_str = runtime.token.to_string();
            let transfer_ids: Vec<String> = pending.transfer_data.iter()
                .map(|t| t.transfer_id.clone())
                .collect();
            
            let _ = redis_helpers::set_transfers_completed(
                &mut conn,
                &token_str,
                &transfer_ids,
                "verified_onchain",
            ).await;

            // ACK from verification queue (it's already been processed)
            // No need to ACK - we'll just not re-enqueue
        }
        Ok(false) => {
            // Transaction failed on-chain - safe to retry
            runtime.obs_ctx.inc_metric("failed").await;
            let batch_id_display = pending.to_string();
            info!("❌ Status checker: {} verified FAILED on-chain - re-enqueueing for retry", batch_id_display);

            // Re-enqueue to ready queue for retry
            let transfers: Vec<Transfer<ReadyToSend>> = pending.transfer_data.into_iter()
                .map(|data| Transfer::<ReadyToSend>::from_data(data))
                .collect();
            
            for transfer in transfers {
                if let Err(e) = runtime.ready_queue.push(&transfer).await {
                    warn!("Failed to re-enqueue transfer from {}: {:?}", batch_id_display, e);
                }
            }
        }
        Err(e) => {
            // Could not verify - retry verification later
            let new_attempts = pending.attempts + 1;
            
            if new_attempts < MAX_VERIFICATION_ATTEMPTS {
                // Exponential backoff: wait before re-enqueueing
                let wait_secs = (new_attempts as u64).min(30);
                debug!("⚠️  Status checker: {} verification failed: {} - will retry in {}s", 
                    pending, e, wait_secs);
                
                tokio::time::sleep(tokio::time::Duration::from_secs(wait_secs)).await;

                // Re-enqueue to verification queue with incremented attempts
                let updated = PendingVerificationBatch {
                    attempts: new_attempts,
                    ..pending
                };
                
                let updated_display = updated.to_string();
                if let Err(e) = enqueue_for_verification(&mut runtime.redis_conn.clone(), &runtime.token, updated).await {
                    warn!("Failed to re-enqueue verification for {}: {:?}", updated_display, e);
                }
            } else {
                // Exhausted all attempts - critical failure
                runtime.obs_ctx.inc_metric("failed").await;
                warn!("⚠️  CRITICAL: {} unverifiable after {} attempts - LOST {} transfers",
                    pending, MAX_VERIFICATION_ATTEMPTS, pending.transfer_data.len());
                
                // Could either:
                // 1. ACK and lose transfers (conservative - no duplicates)
                // 2. Retry transaction (risky - may create duplicates)
                // For now: log critically and lose (no duplicates guarantee)
            }
        }
    }
}

/// Enqueue a batch for status verification
pub async fn enqueue_for_verification(
    conn: &mut redis::aio::ConnectionManager,
    token: &near_api_types::AccountId,
    pending: PendingVerificationBatch,
) -> Result<()> {
    let queue_key = format!("verification:{}", token);
    
    let serialized = serde_json::to_string(&pending)?;
    conn.rpush(&queue_key, serialized).await?;
    
    Ok(())
}

pub async fn status_checker_task(runtime: Arc<StatusCheckerRuntime>) {
    let queue_key = format!("verification:{}", runtime.token);
    
    info!("Status checker worker started for token {}", runtime.token);
    
    loop {
        let mut conn = runtime.redis_conn.clone();
        
        // Block waiting for items (BLPOP with 5 second timeout)
        match conn.blpop::<_, Vec<String>>(&queue_key, 5.0).await {
            Ok(result) => {
                if result.len() == 2 {
                    let serialized = &result[1];
                    match serde_json::from_str::<PendingVerificationBatch>(serialized) {
                        Ok(pending) => {
                            let runtime_clone = runtime.clone();
                            tokio::spawn(async move {
                                check_pending_verification(runtime_clone, pending).await;
                            });
                        }
                        Err(e) => {
                            warn!("Failed to deserialize pending verification batch: {:?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                // Timeout or error - just continue
                if e.to_string().contains("timed out") {
                    // Normal timeout, continue
                } else {
                    debug!("BLPOP error: {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}
