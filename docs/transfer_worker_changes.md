# Transfer Worker Changes for Better Observability

## Key Changes

### 1. Add BatchId to all operations

```rust
async fn process_transfer_batch(
    runtime: Arc<TransferWorkerRuntime>,
    obs: Arc<ObservabilityContext>,
    batch: Vec<(String, Transfer<ReadyToSend>)>,
) {
    tokio::spawn(async move {
        let batch_id = BatchId::new();
        let transfer_count = batch.len();
        
        // Build actions...
        
        // Submit with observability
        match submit_with_observability(
            &runtime.signer,
            &runtime.signer_account,
            &runtime.token,
            actions,
            &runtime.network,
            batch_id,
            transfer_count,
            &obs,
        ).await {
            Ok(outcome) => {
                // Track success
                obs.inc_metric("succeeded").await;
                
                // Check for duplicates
                for (_, transfer) in &batch {
                    let transfer_id = &transfer.data().transfer_id;
                    if let Ok(Some(existing_tx)) = obs.check_duplicate(transfer_id).await {
                        warn!("⚠️  Duplicate detected: transfer {} already completed by tx {}", 
                              transfer_id, existing_tx);
                        obs.inc_metric("duplicates").await;
                    } else {
                        let tx_hash = outcome.transaction_outcome.id.to_string();
                        let _ = obs.mark_completed(transfer_id, &tx_hash).await;
                    }
                }
                
                // ... handle success
            }
            Err(err) => {
                let err_str = err.to_string();
                
                if err_str.contains("TimeoutError") {
                    obs.inc_metric("timeout").await;
                    handle_timeout_with_verification(
                        &runtime, 
                        &obs, 
                        &batch, 
                        batch_id, 
                        transfer_count
                    ).await;
                } else {
                    obs.inc_metric("failed").await;
                    handle_transfer_retry(&runtime, &obs, &batch, batch_id).await;
                }
            }
        }
    });
}
```

### 2. Reduce Nonce Retry Log Spam

```rust
async fn submit_with_observability(
    signer: &Arc<Signer>,
    signer_account: &AccountId,
    token: &AccountId,
    actions: Vec<Action>,
    network: &NetworkConfig,
    batch_id: BatchId,
    transfer_count: usize,
    obs: &Arc<ObservabilityContext>,
) -> Result<FinalExecutionOutcomeView> {
    let mut nonce_retry = 0;

    loop {
        let presigned = match tx_builder.presign_with(network).await {
            Ok(p) => p,
            Err(err) => return Err(err.into()),
        };

        let tx_hash = /* extract hash */;
        
        // Track submission
        obs.inc_metric("submitted").await;
        obs.track_tx_status(TxStatus {
            tx_hash: tx_hash.clone(),
            batch_id: batch_id.to_string(),
            transfer_count,
            status: TxStatusKind::Submitted,
            submitted_at: chrono::Utc::now().timestamp(),
            completed_at: None,
            error: None,
            verified_onchain: false,
        }).await?;

        let result = presigned.send_to(network).await;

        match result {
            Ok(tx) => {
                // Update to succeeded
                obs.track_tx_status(TxStatus {
                    tx_hash: tx_hash.clone(),
                    batch_id: batch_id.to_string(),
                    transfer_count,
                    status: TxStatusKind::Succeeded,
                    submitted_at: 0, // filled by previous
                    completed_at: Some(chrono::Utc::now().timestamp()),
                    error: None,
                    verified_onchain: false,
                }).await?;
                
                return Ok(tx);
            }
            Err(err) => {
                let err_str = err.to_string();
                
                if err_str.contains("InvalidNonce") && nonce_retry < MAX_NONCE_RETRIES {
                    nonce_retry += 1;
                    
                    // Only log first retry at WARN, rest at DEBUG
                    if nonce_retry == 1 {
                        warn!("InvalidNonce for batch {} (retry {}/{}), resyncing nonce", 
                              batch_id, nonce_retry, MAX_NONCE_RETRIES);
                    } else {
                        debug!("Nonce resync retry {}/{} for batch {}", 
                               nonce_retry, MAX_NONCE_RETRIES, batch_id);
                    }

                    // Resync nonce...
                    
                    if nonce_retry == 1 {
                        info!("Nonce resynced for batch {}", batch_id);
                    } else {
                        debug!("Nonce resynced for batch {}", batch_id);
                    }
                    
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    continue;
                }

                // Track failure
                obs.track_tx_status(TxStatus {
                    tx_hash: tx_hash.clone(),
                    batch_id: batch_id.to_string(),
                    transfer_count,
                    status: if err_str.contains("TimeoutError") { 
                        TxStatusKind::Timeout 
                    } else { 
                        TxStatusKind::Failed 
                    },
                    submitted_at: 0,
                    completed_at: Some(chrono::Utc::now().timestamp()),
                    error: Some(err_str),
                    verified_onchain: false,
                }).await?;
                
                return Err(err.into());
            }
        }
    }
}
```

### 3. Verify Transaction Before Retry

```rust
async fn handle_timeout_with_verification(
    runtime: &Arc<TransferWorkerRuntime>,
    obs: &Arc<ObservabilityContext>,
    batch: &[(String, Transfer<ReadyToSend>)],
    batch_id: BatchId,
    transfer_count: usize,
) {
    // Wait for potential finalization
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Try to check on-chain (you'd need to implement this)
    // For now, just log and retry
    warn!("⏱️  Batch {} timed out ({} transfers), will retry after verification check", 
          batch_id, transfer_count);
    
    // TODO: Implement actual on-chain verification
    // if let Ok(outcome) = check_tx_onchain(&tx_hash, &runtime.network).await {
    //     info!("✅ Batch {} verified on-chain despite timeout", batch_id);
    //     obs.track_tx_status(...verified_onchain: true...).await;
    //     handle_successful_outcome(runtime, batch, outcome, &tx_hash).await;
    //     return;
    // }
    
    // Track retry
    obs.inc_metric("retried").await;
    obs.track_retry(&batch_id, "pending_retry").await;
    
    handle_transfer_retry(runtime, obs, batch, batch_id).await;
}
```

### 4. Add Periodic Summary

```rust
// In main.rs or lib.rs
pub async fn start_relay_with_observability(...) {
    let obs_ctx = Arc::new(ObservabilityContext::new(redis_client.clone()));
    
    // Start summary logger
    let obs_ctx_clone = obs_ctx.clone();
    tokio::spawn(async move {
        summary_logger_task(obs_ctx_clone).await;
    });
    
    // Pass obs_ctx to workers...
}
```

## Example Log Output

### Before (noisy):
```
[WARN] transaction 4Q2nZ7CAarvTzeANw84jtRw5ycGovwPFyyZhMwxkZsbs failed: ...
[WARN] InvalidNonce detected (retry 1/3), forcing nonce resync from chain
[INFO] nonce resync successful, retrying transaction
[WARN] transaction FXFwX5KptXTc9KvLNLrAspcS2bvbRDxeyxTAKTAH24WF failed: ...
[WARN] InvalidNonce detected (retry 1/3), forcing nonce resync from chain
[INFO] nonce resync successful, retrying transaction
...
```

### After (clean):
```
[WARN] InvalidNonce for batch a3f5c2e1 (retry 1/3), resyncing nonce
[INFO] Nonce resynced for batch a3f5c2e1
[INFO] tx EiT8vA6B... succeeded: 100 transfers (batch: a3f5c2e1)
[SUMMARY] last_10s: submitted=47, succeeded=45, timeout=2, retried=2, duplicates=0
[WARN] ⏱️  Batch d7f3b4a2 timed out (100 transfers), will retry after verification check
[AUDIT] {"event":"tx_timeout","timestamp":1728508902,"tx_hash":"Hez8VaQ...","batch_id":"d7f3b4a2","transfer_count":100}
```

## Usage

### Run with observability:
```bash
# Start relay
cargo run --release

# In another terminal, dump state:
cargo run --bin relay-dump

# View audit log:
grep AUDIT logs/relay.log | jq .
```

### Test reconciliation:
```rust
// At end of test
let metrics = obs_ctx.get_metrics().await;
let audit_events = obs_ctx.audit_log.read().await;

println!("\n╔════════════════════════════════════════╗");
println!("║  RECONCILIATION REPORT                 ║");
println!("╚════════════════════════════════════════╝");
println!("Submitted:  {}", metrics.submitted);
println!("Succeeded:  {}", metrics.succeeded);
println!("Timeout:    {}", metrics.timeout);
println!("Retried:    {}", metrics.retried);
println!("Duplicates: {}", metrics.duplicates);

if metrics.duplicates > 0 {
    println!("\n⚠️  WARNING: {} duplicate transfers detected", metrics.duplicates);
    println!("Check audit log for details");
}
```
