# FT Relay

A Rust-powered HTTP relay that batches NEP-141 `ft_transfer` calls into NEAR transactions. Use it whenever you need to fan-in high volumes of fungible token transfers onto NEAR with a simple HTTP interface.

---

## Features

- **Transfer endpoints** – `POST /v1/transfer` to queue transfers, `GET /v1/transfer/:id` to check status and get tx_hash.
- **Signer pool** – rotate across multiple function-call access keys to avoid nonce contention.
- **Micro-batching** – pack up to `BATCH_SIZE` transfers into a single transaction while respecting the 300 TGas ceiling.
- **Async pipeline** – bounded queue + semaphore to backpressure inflight batches.
- **Durable queue** – Redis Streams persist every transfer until the worker acknowledges it.
- **Sandbox-friendly** – integration suites spin up `near-sandbox`, deploy the FT contract, and verify final balances.
- **Docker-ready** – minimal two-stage container for production deployment.

---

## Architecture at a Glance

![ft-relay architecture diagram](docs/diagrams/architecture.svg)

- The HTTP handler writes each request to a Redis Stream and immediately returns a `transfer_id`.
- The async worker consumes from the stream’s consumer group, batches transfers, and submits NEAR transactions.
- Gas accounting ensures we never exceed NEAR’s 300 TGas prepaid limit (`90` transfers × `40 TGas`).
- The signer pool is backed by `near-api-rs` and can host multiple secret keys for high concurrency.

---

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) 1.86 (pinned in CI).
- `near-sandbox` dependencies (the integration tests download and run it automatically).
- [Redis](https://redis.io/) 8 or newer, reachable from the relay.

---

## Getting Started

1. **Clone and configure**

   ```bash
   git clone https://github.com/r-near/ft-relay.git
   cd ft-relay

   cp .env.example .env
   # edit ACCOUNT_ID, PRIVATE_KEYS, RPC_URL, batching knobs
   ```

2. **Run the relay**

   ```bash
   cargo run --release -- \
     --token your-ft-contract.testnet
   ```

   The server listens on `0.0.0.0:8080` unless you set `BIND_ADDR`.

3. **Send a transfer**
```bash
curl -X POST http://localhost:8080/v1/transfer \
  -H 'Content-Type: application/json' \
  -d '{"receiver_id":"alice.testnet","amount":"1000000000000000000"}'
```
Responses include a durable identifier you can poll later:

```json
{"status":"queued","transfer_id":"6b81f45e-5c7c-4c84-987d-3cf6c3e4232a"}
```

---

## Configuration (.env)

All configuration except the FT contract ID comes from environment variables. The CLI flag `--token` remains mandatory so you can point the same deployment at different contracts.

| Variable               | Required | Description                                                                                           |
| ---------------------- | -------- | ----------------------------------------------------------------------------------------------------- |
| `ACCOUNT_ID`           | ✅       | NEAR account that owns the function-call access keys.                                                 |
| `PRIVATE_KEYS`         | ✅       | Comma-separated list of ed25519 secret keys (`ed25519:...`). Use multiple keys for higher throughput. |
| `RPC_URL`              | ✅       | NEAR RPC endpoint (sandbox/testnet/mainnet).                                                          |
| `BIND_ADDR`            | ❌       | HTTP bind address (`0.0.0.0:8080` by default).                                                        |
| `BATCH_SIZE`           | ❌       | Max logical transfers per batch (default `90`).                                                       |
| `BATCH_LINGER_MS`      | ❌       | Max time to wait for a batch to fill (default `20ms`).                                                |
| `MAX_INFLIGHT_BATCHES` | ❌       | Inflight batch semaphore (default `200`).                                                             |
| `RUST_LOG`             | ❌       | Standard Rust logging spec (`info,ft_relay=info`).                                                    |
| `REDIS_URL`            | ❌       | Connection string for Redis (default `redis://127.0.0.1:6379`).                                       |
| `REDIS_STREAM_KEY`     | ❌       | Stream key for pending transfers (default `ftrelay:pending`).                                        |
| `REDIS_CONSUMER_GROUP` | ❌       | Consumer group name used by the batch worker (default `ftrelay:batcher`).                             |

> ⚠️ Use function-call restricted keys that can only call your FT contract. Never ship full-access secrets in production.

---

## API Reference

### `POST /v1/transfer`

**Body**

```json
{
  "receiver_id": "alice.testnet",
  "amount": "1000000000000000000",
  "memo": "optional"
}
```

- `receiver_id` – NEAR account to receive tokens (must be registered).
- `amount` – Stringified yocto-token amount.
- `memo` – Optional memo forwarded to the FT contract.

**Response**

```json
{
  "status": "queued",
  "transfer_id": "6b81f45e-5c7c-4c84-987d-3cf6c3e4232a"
}
```

HTTP `503` signals the internal queue is saturated.

### `GET /v1/transfer/:id`

Query the status of a previously submitted transfer.

**Request**

```bash
curl http://localhost:8080/v1/transfer/6b81f45e-5c7c-4c84-987d-3cf6c3e4232a
```

**Response (pending)**

```json
{
  "transfer_id": "6b81f45e-5c7c-4c84-987d-3cf6c3e4232a",
  "status": "pending"
}
```

**Response (completed)**

```json
{
  "transfer_id": "6b81f45e-5c7c-4c84-987d-3cf6c3e4232a",
  "status": "completed",
  "tx_hash": "HMeo3DYSuAmXWxuTotFzWMac5bcePhHeRqfCDLRNBs9Y"
}
```

Status records are stored in Redis for 24 hours after completion.

---

## Troubleshooting

- **Sandbox kernel parameter warnings** – `near-sandbox` may warn about TCP buffer sizes on Linux. Adjust via `scripts/set_kernel_params.sh` if you need peak throughput.
- **Nonce errors** – Add more keys to `PRIVATE_KEYS` or ensure the signer account isn’t used elsewhere.
- **Gas exceeded** – The relay automatically chunks batches, but if you change `FT_TRANSFER_GAS_PER_ACTION`, keep `gas * BATCH_SIZE ≤ 300 TGas`.
- **Redis connectivity** – The server returns `500` if it cannot enqueue into Redis; verify `REDIS_URL` and that the stream/group exist.

---

## Testing

The project includes comprehensive test suites:

**Run all tests serially** (required to avoid Redis conflicts):
```bash
cargo test --all --locked -- --test-threads=1 --nocapture
```

**Run ignored integration/benchmark tests serially**:
```bash
cargo test --all --locked -- --ignored --nocapture --test-threads=1
```

**Test types**:
- **Unit tests** – Fast, in-memory validation
- **Integration tests** – Sandbox-based with real NEAR nodes
- **Testnet tests** – Live testnet benchmarks (require `TESTNET_RPC_URL` in `.env`)

> ⚠️ Always use `--test-threads=1` to run tests serially and avoid Redis/sandbox conflicts.

---

## Roadmap & Caveats

- No finality tracking – we optimistically submit and rely on NEAR RPC to process transactions.
- Idempotency and retry logic are minimal; upstream callers should implement their own safeguards.
- Redis availability is required; if Redis is down the relay rejects new transfers.

Contributions and issue reports are welcome!
