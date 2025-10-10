# Testing the FT Relay Service

## Manual Testing (Recommended)

Since the integrated benchmark test requires near-sandbox which has API compatibility issues, 
we recommend manual testing with a real testnet deployment:

### 1. Prerequisites

- Redis running on `localhost:6379`
- NEAR testnet account with FT contract deployed
- Access keys for the relay account

### 2. Setup Environment

Create `.env` file:
```bash
RELAY_ACCOUNT_ID=your-relay.testnet
RELAY_TOKEN=your-token.testnet  
RELAY_PRIVATE_KEYS=ed25519:key1,ed25519:key2,ed25519:key3
RELAY_RPC_URL=https://rpc.testnet.near.org
REDIS_URL=redis://127.0.0.1:6379
```

### 3. Start the Service

```bash
cargo run
```

You should see:
```
Starting FT Relay Service
Token: your-token.testnet
Relay account: your-relay.testnet
Access keys: 3
...
Spawning 5 registration worker(s)
Spawning 2 transfer worker(s)
Spawning 5 verification worker(s)
HTTP server listening on http://127.0.0.1:3030
```

### 4. Test Single Transfer

```bash
curl -X POST http://localhost:3030/v1/transfer \
  -H 'X-Idempotency-Key: test-transfer-1' \
  -H 'Content-Type: application/json' \
  -d '{
    "receiver_id": "alice.testnet",
    "amount": "1000000000000000000"
  }'
```

Expected response:
```json
{
  "transfer_id": "test-transfer-1",
  "status": "QUEUED_REGISTRATION",
  "receiver_id": "alice.testnet",
  "amount": "1000000000000000000",
  "created_at": "2025-01-15T10:00:00Z",
  "retry_count": 0
}
```

### 5. Check Transfer Status

```bash
curl http://localhost:3030/v1/transfer/test-transfer-1
```

You'll see the full audit trail:
```json
{
  "transfer_id": "test-transfer-1",
  "status": "COMPLETED",
  "receiver_id": "alice.testnet",
  "amount": "1000000000000000000",
  "tx_hash": "HYNz7B...3k",
  "created_at": "2025-01-15T10:00:00Z",
  "completed_at": "2025-01-15T10:00:42Z",
  "events": [
    {"time": "2025-01-15T10:00:00Z", "event": "RECEIVED"},
    {"time": "2025-01-15T10:00:01Z", "event": "QUEUED_REGISTRATION"},
    {"time": "2025-01-15T10:00:05Z", "event": "REGISTERED", "tx_hash": "ABC..."},
    {"time": "2025-01-15T10:00:06Z", "event": "QUEUED_TRANSFER"},
    {"time": "2025-01-15T10:00:10Z", "event": "SUBMITTED", "tx_hash": "HYNz7B...3k"},
    {"time": "2025-01-15T10:00:11Z", "event": "QUEUED_VERIFICATION"},
    {"time": "2025-01-15T10:00:15Z", "event": "COMPLETED"}
  ]
}
```

### 6. Check Metrics

```bash
curl http://localhost:3030/health
```

Response:
```json
{
  "status": "healthy",
  "redis": "connected",
  "metrics": {
    "rpc_calls_total": 45
  }
}
```

### 7. Test Idempotency

Submit the same transfer again:
```bash
curl -X POST http://localhost:3030/v1/transfer \
  -H 'X-Idempotency-Key: test-transfer-1' \
  -H 'Content-Type: application/json' \
  -d '{
    "receiver_id": "alice.testnet",
    "amount": "1000000000000000000"
  }'
```

Should return the existing transfer state (same response as status check).

### 8. Test Idempotency Conflict

Try same key with different parameters:
```bash
curl -X POST http://localhost:3030/v1/transfer \
  -H 'X-Idempotency-Key: test-transfer-1' \
  -H 'Content-Type: application/json' \
  -d '{
    "receiver_id": "bob.testnet",
    "amount": "2000000000000000000"
  }'
```

Should return `409 Conflict`:
```json
{
  "error": "Idempotency key already used with different parameters"
}
```

### 9. Benchmark (100 transfers)

Create a simple script `benchmark.sh`:
```bash
#!/bin/bash
for i in {1..100}; do
  curl -X POST http://localhost:3030/v1/transfer \
    -H "X-Idempotency-Key: bench-$i" \
    -H 'Content-Type: application/json' \
    -d "{\"receiver_id\":\"user$i.testnet\",\"amount\":\"1000000000000000000\"}" &
done
wait
echo "Submitted 100 transfers"
```

Run it:
```bash
chmod +x benchmark.sh
time ./benchmark.sh
```

Then check completion:
```bash
# Check a few statuses
for i in {1..10}; do
  curl -s http://localhost:3030/v1/transfer/bench-$i | jq '.status'
done

# Check metrics
curl -s http://localhost:3030/health | jq '.metrics'
```

## Expected Performance

- **Submission rate**: 500-1000 req/sec
- **RPC calls per 100 transfers**: ~10-20 (due to batching and caching)
  - Block hash: 1 (cached 1 hour)
  - Batch broadcast: 1-2 (up to 100 transfers per tx)
  - Status checks: 1-2 (cached per tx_hash)
- **End-to-end latency**: 10-30 seconds (depends on NEAR network)
- **Success rate**: >95% under normal conditions

## Monitoring

Watch the logs for insights:
```bash
RUST_LOG=info cargo run
```

Key log messages:
- `Registered alice.testnet with tx ABC123` - Registration complete
- `Submitted batch of 100 transfers, tx: XYZ789` - Batch submitted
- `Tx XYZ789 completed successfully` - Batch verified
- `Updating 100 transfers for tx XYZ789` - Bulk status update

## Troubleshooting

### High RPC call count
- Check if block hash caching is working
- Verify batching is happening (should see "batch of N transfers")
- Check verification caching (should see "already cached" messages)

### Low completion rate
- Check RPC connectivity: `curl https://rpc.testnet.near.org/status`
- Verify account has sufficient balance
- Check Redis for error events in transfer state

### Transfers stuck in QUEUED_* state
- Check worker logs for errors
- Verify Redis streams have consumers: `redis-cli XINFO GROUPS ftrelay:testnet:reg`
- Check access key lease availability in Redis

