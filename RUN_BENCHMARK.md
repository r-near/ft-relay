# Running the Integrated Benchmark

## Prerequisites

1. **Redis running** on `localhost:6379`
2. **Environment variables** configured (or defaults will be used)

## Quick Start

```bash
# Optional: Set your testnet credentials
export RELAY_TOKEN=your-token.testnet
export RELAY_ACCOUNT_ID=your-relay.testnet
export RELAY_PRIVATE_KEYS=ed25519:your_key1,ed25519:your_key2
export RELAY_RPC_URL=https://rpc.testnet.near.org
export REDIS_URL=redis://127.0.0.1:6379

# Run the benchmark (starts relay service automatically)
cargo test --test integrated_benchmark -- --ignored --nocapture 2>&1 | tee ./benchmark_results.log
```

## What the Test Does

1. **Flushes Redis** to start clean
2. **Starts relay service** programmatically on `127.0.0.1:3031`
3. **Waits for service** to be ready (health check)
4. **Submits 100 transfers** concurrently via HTTP API
   - Each with unique idempotency key
   - Measures submission rate
5. **Waits 30 seconds** for pipeline processing
6. **Checks status** of all transfers
7. **Reports metrics**:
   - Completion rate
   - RPC calls total
   - RPC calls per transfer
   - End-to-end throughput

## Expected Results

- **Submission success**: >95%
- **Completion rate**: Varies based on actual RPC connectivity
- **RPC calls**: Dramatically less than number of transfers due to:
  - Block hash caching (1 hour)
  - Batch transactions (up to 100 per tx)
  - Verification caching (1 RPC check per tx_hash)

## Example Output

```
╔════════════════════════════════════════════════╗
║  Integrated Benchmark: HTTP API + Relay Service  ║
║  Testing 100 transfers with full pipeline        ║
╚════════════════════════════════════════════════╝

✅ Redis flushed
📋 Configuration:
   Token: usdt.testnet
   Account: relay.testnet
   RPC: https://rpc.testnet.near.org
   Bind: 127.0.0.1:3031

🚀 Starting relay service...
✅ Relay service ready after 1000ms

📤 Submitting 100 transfers...

✅ Submission complete:
   Success: 100/100
   Failed: 0
   Time: 245ms
   Rate: 408.2 req/sec

⏳ Waiting 30 seconds for pipeline processing...

🔍 Checking transfer statuses...

📊 Final Metrics:
   RPC calls total: 45
   Redis status: connected
   RPC calls per completed transfer: 0.47

📈 Final Results:
   ✅ Completed: 96/100 (96.0%)
   ❌ Failed: 0
   ⏳ In Progress: 4
   ❓ Other: 0

⏱️  Total Time: 30.3s
   End-to-end throughput: 3.2 tx/sec

✅ Test passed! System is working correctly.
```

## Troubleshooting

### "Relay service failed to start"
- Ensure Redis is running: `redis-cli ping` should return `PONG`
- Check port 3031 is available: `lsof -i :3031`

### Low completion rate
- Check RPC connectivity: `curl https://rpc.testnet.near.org/status`
- Verify credentials are valid
- Check Redis for error logs in transfer events

### High RPC call count
- Verify block hash caching is working (should see cache hits in logs)
- Check verification workers are using tx_hash caching
- Review metrics to ensure batching is happening

## Advanced Usage

### Increase transfer count
Edit `NUM_TRANSFERS` in `tests/integrated_benchmark.rs`

### Change bind address
Edit `BIND_ADDR` constant to use different port

### Enable debug logging
```bash
RUST_LOG=debug cargo test --test integrated_benchmark -- --ignored --nocapture
```
