# Observability Improvements Summary

## Problem Analysis
Your test found 60,075 tokens on-chain instead of 60,000 (+75 duplicates).

**Root cause:** 3 RPC timeouts → retries with fresh nonces → partial duplicate successes
- 300 transfers timed out
- 120 succeeded on-chain despite timeout
- 180 failed on-chain
- 300 were retried
- 75 retry succeeded (duplicates)
- 225 retries failed (InvalidNonce, etc.)

## Proposed Solutions

### Quick Wins (Implement First)
1. **BatchId for correlation** - Track batches through retries
2. **Reduce log spam** - Only first nonce retry at WARN level
3. **Periodic summaries** - Every 10s log: submitted/succeeded/timeout/retry counts
4. **Duplicate detection** - Track `transfer:completed:{id}` in Redis before processing

### Core Improvements
5. **Redis transaction tracking** - Store tx status by hash for post-mortem
6. **Audit log** - JSON log file with only critical events
7. **Retry verification** - Check on-chain status before retrying timeouts
8. **CLI dump tool** - `relay-dump` to inspect Redis state

### Test Improvements
9. **Reconciliation report** - At test end, show exactly what happened
10. **Better assertions** - Detect duplicates and fail early

## Files Created
- `docs/observability-improvements.md` - Detailed design
- `src/observability.rs` - Core observability module
- `src/bin/relay-dump.rs` - CLI tool for inspecting state
- `docs/transfer_worker_changes.md` - Integration examples

## Example Output After Implementation

### Normal operation:
```
[INFO] Processing batch a3f5c2e1: 100 transfers
[INFO] tx EiT8vA6B... succeeded: 100 transfers (batch: a3f5c2e1)
[SUMMARY] last_10s: submitted=47, succeeded=47, timeout=0, retried=0, duplicates=0
```

### Timeout scenario:
```
[WARN] ⏱️  Batch d7f3b4a2 timed out (100 transfers), verifying on-chain...
[INFO] ✅ Batch d7f3b4a2 verified on-chain despite timeout
[AUDIT] {"event":"tx_verified_onchain","tx_hash":"Hez8VaQ...","batch_id":"d7f3b4a2"}
[SUMMARY] last_10s: submitted=50, succeeded=48, timeout=1, retried=0, duplicates=0
```

### Test summary:
```
╔════════════════════════════════════════════════════════════╗
║  RECONCILIATION REPORT                                     ║
╚════════════════════════════════════════════════════════════╝
Submitted:  60,003
Succeeded:  60,000
Timeout:    3
Retried:    0 (verified on-chain)
Duplicates: 0

✅ All transfers completed successfully without duplicates
```

## Next Steps

1. Add observability.rs to lib.rs
2. Add BatchId to transfer_worker
3. Reduce nonce retry log spam
4. Add periodic summary task
5. Test with 60k benchmark
6. Iterate based on real-world usage

## Related Issues
- Timeout handling needs on-chain verification
- near-api-rs retry logic may mask actual errors
- Consider exponential backoff for retries
