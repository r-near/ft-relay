# Observability Improvements for Retry/Timeout Scenarios

## Problem
Currently it's difficult to:
- Trace timeouts → retries → outcomes
- Detect duplicates
- Understand what happened without extensive log analysis
- Distinguish signal from noise in logs

## Proposed Solutions

### 1. **Structured Audit Log** (Separate from debug logs)
A separate `audit.log` file with only critical lifecycle events in JSON format.

**Events to log:**
```json
// Transaction submitted
{"event": "tx_submitted", "ts": "...", "tx_hash": "...", "batch_id": "...", "transfer_count": 100}

// Transaction succeeded
{"event": "tx_succeeded", "ts": "...", "tx_hash": "...", "batch_id": "...", "transfer_count": 100, "duration_ms": 1234}

// Transaction timed out on RPC
{"event": "tx_timeout", "ts": "...", "tx_hash": "...", "batch_id": "...", "transfer_count": 100, "will_retry": true}

// Retry enqueued
{"event": "retry_enqueued", "ts": "...", "original_tx_hash": "...", "batch_id": "...", "transfer_count": 100, "retry_attempt": 1}

// Transaction verified on-chain after timeout
{"event": "tx_verified_onchain", "ts": "...", "tx_hash": "...", "batch_id": "...", "transfer_count": 100, "verified_after_timeout": true}

// Duplicate detected
{"event": "duplicate_detected", "ts": "...", "transfer_id": "...", "original_tx": "...", "duplicate_tx": "..."}
```

### 2. **Redis Transaction Tracking**
Track all transaction attempts and outcomes in Redis.

**Data structures:**

```redis
# Transaction status by hash
tx:status:{tx_hash} -> JSON {
  "tx_hash": "...",
  "batch_id": "...", 
  "transfer_count": 100,
  "status": "submitted|succeeded|timeout|failed",
  "submitted_at": "...",
  "completed_at": "...",
  "error": "...",
  "verified_onchain": true/false
}

# Track retries for a batch
batch:retries:{batch_id} -> LIST [tx_hash1, tx_hash2, ...]

# Track which transfers completed (for duplicate detection)
transfer:completed:{transfer_id} -> tx_hash

# Metrics counters (for quick stats)
metrics:submitted -> COUNTER
metrics:succeeded -> COUNTER
metrics:timeout -> COUNTER
metrics:retried -> COUNTER
metrics:duplicates -> COUNTER
```

### 3. **Reduce Log Spam**

**Current spam sources:**
- Nonce retry logs (every InvalidNonce shows full retry)
- Stack traces for common errors

**Fix:**
```rust
// Only log first nonce retry at WARN, rest at DEBUG
if nonce_retry == 1 {
    warn!("InvalidNonce detected, forcing nonce resync (attempt {}/{})", nonce_retry, MAX_NONCE_RETRIES);
} else {
    debug!("Nonce resync retry {}/{}", nonce_retry, MAX_NONCE_RETRIES);
}

// No stack traces for expected errors
warn!("tx {} failed: {:?}", tx_hash, err);  // Instead of err.backtrace()
```

### 4. **Batch ID for Correlation**
Assign each batch a UUID so we can trace it through retries:

```rust
struct BatchId(Uuid);

// When processing batch
let batch_id = BatchId(Uuid::new_v4());

// All logs include batch_id
info!("processing batch {}: {} transfers", batch_id.0, batch.len());
warn!("batch {} timed out, retrying", batch_id.0);
```

### 5. **Periodic Summary Logs**
Every 10s or 100 batches, log a summary:

```
[SUMMARY] last_10s: submitted=47, succeeded=45, timeout=2, retried=2, duplicates=0, pending=123
```

### 6. **Transaction Verification Before Retry**
When timeout occurs, check on-chain before retrying:

```rust
async fn handle_timeout_error(...) {
    // Log the timeout
    audit_log!("tx_timeout", tx_hash, batch_id, transfer_count);
    
    // Wait for potential finalization
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Check if tx actually succeeded
    if let Ok(outcome) = check_tx_onchain(tx_hash).await {
        audit_log!("tx_verified_onchain", tx_hash, batch_id, transfer_count);
        handle_success(outcome);
        return;
    }
    
    // Safe to retry
    audit_log!("retry_enqueued", tx_hash, batch_id, transfer_count);
    retry_batch();
}
```

### 7. **Redis Dump Tool**
Simple CLI to dump transaction state:

```bash
cargo run --bin relay-dump

# Output:
Summary:
  Submitted: 60000
  Succeeded: 59880
  Timeout: 3
  Retried: 3
  Duplicates: 0
  Pending: 0

Timeout Transactions:
  Hez8VaQ... -> retried as 9op7q7b... (succeeded)
  8ruaAog... -> retried as CjF9AS5... (succeeded)
  2rTcUmN... -> retried as HYu9M1U... (partial: 80/100)

Potential Issues:
  ⚠ 75 transfers completed twice (duplicates)
```

### 8. **Test Summary Report**
At end of test, print a reconciliation report:

```
╔════════════════════════════════════════════════════════════╗
║  TRANSFER RECONCILIATION REPORT                             ║
╚════════════════════════════════════════════════════════════╝

HTTP Requests:          60,000
Logged Successes:       59,880  (599 txs)
Timeouts:               3       (300 transfers)
Retries:                3       (300 transfers)

On-chain Total:         60,075
Expected:               60,000
Duplicates:             +75

Timeout Details:
  Hez8VaQ... → 100 transfers → timed out → 40 succeeded on-chain
  8ruaAog... → 100 transfers → timed out → 35 succeeded on-chain  
  2rTcUmN... → 100 transfers → timed out → 120 succeeded on-chain

Retry Outcomes:
  First retry  → 25 succeeded (duplicates)
  Second retry → 30 succeeded (duplicates)
  Third retry  → 20 succeeded (duplicates), 80 failed

⚠ WARNING: 75 duplicate transfers detected
```

## Implementation Priority

**Phase 1: Quick wins (1-2 hours)**
1. Add batch_id to all operations
2. Reduce nonce retry log spam
3. Add periodic summary logs
4. Track tx status in Redis

**Phase 2: Core improvements (2-4 hours)**
5. Implement audit log
6. Add transaction verification before retry
7. Duplicate detection

**Phase 3: Tooling (1-2 hours)**
8. Redis dump CLI tool
9. Test reconciliation report

## Example Implementation

See the proposed code changes in:
- `src/observability.rs` (new file)
- `src/transfer_worker.rs` (modifications)
- `src/bin/relay-dump.rs` (new CLI tool)
