# FT Relay Service - Complete Rearchitecture

**Status:** Planning → Implementation  
**Date:** 2025-10-10  
**Goal:** 100 tx/second throughput with zero duplicates, full observability, and true idempotency

---

## Table of Contents

1. [Overview & Goals](#overview--goals)
2. [Architecture Overview](#architecture-overview)
3. [Core Components](#core-components)
4. [Data Structures (Redis)](#data-structures-redis)
5. [API Specification](#api-specification)
6. [Worker Flows](#worker-flows)
7. [Registration Deduplication](#registration-deduplication)
8. [Access Key & Nonce Management](#access-key--nonce-management)
9. [Error Handling & Retries](#error-handling--retries)
10. [Implementation Checklist](#implementation-checklist)

---

## Overview & Goals

### Problems with Current Architecture

1. **Hidden RPC Overhead**: `near-api-rs` fetches nonces from RPC on every transaction (wasted capacity)
2. **Uncontrolled Retries**: Internal retry logic we can't observe or control
3. **No Idempotency**: Cannot safely retry HTTP requests
4. **Opaque Lifecycle**: No way to answer "What happened to transfer X?"
5. **Access Key Contention**: Multiple workers stepping on each other's nonces

### New Architecture Goals

- ✅ **100 tx/second sustained throughput** (60k in 10 minutes)
- ✅ **Zero duplicates** (maintained from current system)
- ✅ **True idempotency** (HTTP header-based)
- ✅ **Full observability** (audit trail for every transfer)
- ✅ **Manual nonce management** (Redis-based, no RPC overhead)
- ✅ **Access key pooling** (lease/release pattern, no contention)
- ✅ **Direct RPC control** (no hidden behavior)

### Performance Targets

- **API Latency**: < 50ms (just Redis write + stream enqueue)
- **End-to-End**: < 30 seconds (HTTP request → transfer confirmed)
- **Throughput**: 100-1000 tx/second (limited only by RPC capacity)
- **Durability**: Redis AOF persistence (no data loss)

---

## Architecture Overview

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **State Store** | Redis (with AOF) | Transfer state, nonces, audit trail |
| **Message Queue** | Redis Streams | 3-stream pipeline (registration → transfer → verification) |
| **RPC Client** | near-jsonrpc-client | Direct JSON-RPC, no hidden behavior |
| **Web Framework** | Axum | HTTP API with idempotency headers |
| **Signing** | near-crypto | Ed25519 transaction signing |

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         HTTP API Layer                           │
│  POST /transfer (X-Idempotency-Key header)                      │
│  GET /transfer/{key} (returns state + audit trail)              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Redis (State Store)                         │
│  • transfer:{key} → Hash (status, tx_hash, receiver, amount)    │
│  • transfer:{key}:events → List (audit trail)                   │
│  • registered_accounts → Set (deduplication)                    │
│  • nonce:{key_id} → Integer (atomic counter)                    │
│  • access_key:lease:{key_id} → String (distributed lock)        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Redis Streams (3 Queues)                      │
│  ftrelay:{env}:registration → Accounts needing registration     │
│  ftrelay:{env}:transfer → Ready for transfer submission         │
│  ftrelay:{env}:verification → Awaiting status confirmation      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                        Worker Pool                               │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐│
│  │ Registration     │  │ Transfer         │  │ Verification  ││
│  │ Workers (5)      │  │ Workers (10)     │  │ Workers (5)   ││
│  │                  │  │                  │  │               ││
│  │ • Check Redis    │  │ • Lease key      │  │ • Check tx    ││
│  │ • Register once  │  │ • Get nonce      │  │ • Update state││
│  │ • Mark in Redis  │  │ • Batch 100      │  │ • Ack stream  ││
│  └──────────────────┘  └──────────────────┘  └───────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   Access Key Pool Manager                        │
│  • Redis SET NX EX for lease (10 sec TTL)                       │
│  • Round-robin or random selection                              │
│  • Auto-release on crash (TTL expires)                          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Nonce Manager                               │
│  • Redis INCR per access key (atomic)                           │
│  • Increment BEFORE signing (holes are acceptable)              │
│  • No RPC calls for nonces                                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              Custom RPC Client (Thin Wrapper)                    │
│  • near-jsonrpc-client (no hidden behavior)                     │
│  • Block hash caching (1 hour, valid for 24 hours)              │
│  • Direct transaction broadcast (no retries)                    │
│  • Transaction status checking                                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                         NEAR RPC                                 │
│  • Transaction submission                                        │
│  • Status queries                                                │
└─────────────────────────────────────────────────────────────────┘
```

### Transfer Lifecycle States

```
RECEIVED              → HTTP request accepted, idempotency check passed
  ↓
QUEUED_REGISTRATION   → Enqueued to registration stream
  ↓
REGISTERED            → Account confirmed registered (or skipped if already done)
  ↓
QUEUED_TRANSFER       → Enqueued to transfer stream
  ↓
SUBMITTED             → Transaction signed and broadcasted to RPC
  ↓
QUEUED_VERIFICATION   → Enqueued to verification stream
  ↓
COMPLETED             → Transaction confirmed on-chain
  ↓ (or on any error)
FAILED                → Max retries exceeded or permanent error
```


---

## Core Components

### 1. HTTP API Server

**File:** `src/http.rs`

**Responsibilities:**
- Accept transfer requests with idempotency headers
- Validate input (account format, amount)
- Check idempotency (return existing transfer if duplicate)
- Store transfer state in Redis
- Enqueue to registration stream
- Return transfer ID to client

**Endpoints:**

```rust
POST /transfer
Headers: X-Idempotency-Key: <unique-key>
Body: {
  "receiver_id": "alice.near",
  "amount": "1000000000000000000000000"  // yoctoNEAR
}
Response: {
  "transfer_id": "idem-key-123",
  "status": "RECEIVED"
}

GET /transfer/{idempotency_key}
Response: {
  "transfer_id": "idem-key-123",
  "status": "COMPLETED",
  "receiver_id": "alice.near",
  "amount": "1000000000000000000000000",
  "tx_hash": "abc123...",
  "created_at": "2025-10-10T10:00:00Z",
  "completed_at": "2025-10-10T10:00:15Z",
  "retry_count": 2,
  "events": [
    {"time": "2025-10-10T10:00:00Z", "event": "RECEIVED"},
    {"time": "2025-10-10T10:00:01Z", "event": "QUEUED_REGISTRATION"},
    {"time": "2025-10-10T10:00:05Z", "event": "REGISTERED"},
    {"time": "2025-10-10T10:00:06Z", "event": "QUEUED_TRANSFER"},
    {"time": "2025-10-10T10:00:10Z", "event": "SUBMITTED", "tx_hash": "abc123..."},
    {"time": "2025-10-10T10:00:11Z", "event": "QUEUED_VERIFICATION"},
    {"time": "2025-10-10T10:00:15Z", "event": "COMPLETED"}
  ]
}
```

### 2. Registration Worker

**File:** `src/registration_worker.rs`

**Responsibilities:**
- Pull from registration stream
- Check if account already registered (Redis SET)
- Acquire distributed lock if not registered
- Register account on-chain (if needed)
- Mark as registered in Redis SET
- Release lock
- Enqueue to transfer stream

**Key Logic:**
- Fast path: Skip registration if already in Redis SET
- Distributed lock prevents duplicate registrations
- Idempotent: Handle "already registered" error gracefully

### 3. Transfer Worker

**File:** `src/transfer_worker.rs`

**Responsibilities:**
- Pull batch of transfers from stream (up to 100)
- Lease an access key from pool
- Get nonce for that key (Redis INCR)
- Build batch transaction (100 actions max)
- Sign transaction with access key
- Submit to RPC
- Release access key
- Enqueue all transfers to verification stream

**Batch Constraints:**
- Max 100 actions per transaction (NEAR protocol limit)
- Max 300 TGas per transaction
- ft_transfer costs ~3 TGas per action
- Safe batch size: 100 actions

### 4. Verification Worker

**File:** `src/verification_worker.rs`

**Responsibilities:**
- Pull from verification stream
- Wait ~6 seconds (2-3 blocks)
- Check transaction status via RPC
- If successful: Mark transfer as COMPLETED
- If pending: Requeue for later check
- If failed: Retry or mark as FAILED

### 5. Access Key Pool Manager

**File:** `src/access_key_pool.rs`

**Responsibilities:**
- Maintain pool of access keys from config
- Lease key to worker (SET NX EX 10 seconds)
- Release key (DEL)
- Handle auto-release on worker crash (TTL)

**API:**
```rust
pub struct AccessKeyPool {
    keys: Vec<AccessKey>,
    redis: ConnectionManager,
}

impl AccessKeyPool {
    async fn lease(&self) -> Result<LeasedKey>;
    async fn release(&self, key_id: &str) -> Result<()>;
}

pub struct LeasedKey {
    pub key_id: String,
    pub secret_key: SecretKey,
    pub public_key: PublicKey,
    // Auto-releases on drop
}
```

### 6. Nonce Manager

**File:** `src/nonce_manager.rs`

**Responsibilities:**
- Get next nonce for access key (Redis INCR)
- Nonce holes are acceptable (NEAR tolerates gaps < 1M)
- No RPC calls needed

**API:**
```rust
pub struct NonceManager {
    redis: ConnectionManager,
}

impl NonceManager {
    async fn get_next_nonce(&self, key_id: &str) -> Result<u64> {
        // INCR nonce:{key_id}
    }
}
```

### 7. RPC Client Wrapper

**File:** `src/rpc_client.rs`

**Responsibilities:**
- Wrap near-jsonrpc-client
- Cache block hash (1 hour TTL, valid for 24 hours)
- Broadcast signed transactions (no retries)
- Check transaction status
- Parse errors

**API:**
```rust
pub struct NearRpcClient {
    client: JsonRpcClient,
    block_hash_cache: Arc<RwLock<(CryptoHash, Instant)>>,
}

impl NearRpcClient {
    async fn get_block_hash(&self) -> Result<CryptoHash>;
    async fn broadcast_tx(&self, signed_tx: SignedTransaction) -> Result<CryptoHash>;
    async fn check_tx_status(&self, tx_hash: CryptoHash, sender: AccountId) -> Result<TxStatus>;
    async fn register_account(&self, account: &AccountId, token: &AccountId) -> Result<CryptoHash>;
    async fn get_access_key(&self, account: &AccountId, public_key: &PublicKey) -> Result<AccessKeyView>;
}

pub enum TxStatus {
    Pending,
    Success(FinalExecutionOutcomeView),
    Failed(String),
}
```

**Note on `register_account`:**
Calls `storage_deposit` on the FT contract to register the account. This is a NEAR FT standard method that allocates storage for the account on the token contract. The relay account pays the storage deposit (~0.0125 NEAR per account).
```

### 8. Redis Helper

**File:** `src/redis_helpers.rs`

**Responsibilities:**
- Transfer state management (Hash operations)
- Audit event logging (List operations)
- Stream operations (XADD, XREADGROUP)
- Registration tracking (SET operations)
- Atomic operations (INCR, SET NX)

**API:**
```rust
pub struct RedisHelper {
    conn: ConnectionManager,
}

impl RedisHelper {
    // Transfer state
    async fn store_transfer_state(&self, key: &str, state: TransferState) -> Result<()>;
    async fn get_transfer_state(&self, key: &str) -> Result<Option<TransferState>>;
    async fn update_transfer_status(&self, key: &str, status: Status, tx_hash: Option<String>) -> Result<()>;
    async fn update_tx_hash(&self, key: &str, tx_hash: &str) -> Result<()>;
    async fn increment_retry_count(&self, key: &str) -> Result<u32>;  // Returns new count
    
    // Audit trail
    async fn log_event(&self, key: &str, event: Event) -> Result<()>;
    async fn get_events(&self, key: &str) -> Result<Vec<Event>>;
    
    // Registration tracking
    async fn is_registered(&self, account: &str) -> Result<bool>;
    async fn mark_registered(&self, account: &str) -> Result<()>;
    
    // Distributed locks
    async fn set_nx_ex(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool>;
    async fn del(&self, key: &str) -> Result<()>;
    
    // Streams (reads current retry_count from transfer state)
    async fn enqueue_registration(&self, transfer_id: &str) -> Result<()>;
    async fn enqueue_transfer(&self, transfer_id: &str) -> Result<()>;
    async fn enqueue_verification(&self, transfer_id: &str, tx_hash: &str) -> Result<()>;
}
```


---

## Data Structures (Redis)

### Transfer State (Hash)

```redis
HSET transfer:{idempotency_key}
  status "SUBMITTED"
  receiver_id "alice.near"
  amount "1000000000000000000000000"
  tx_hash "abc123..."
  created_at "2025-10-10T10:00:00Z"
  updated_at "2025-10-10T10:00:10Z"
  completed_at ""
  retry_count "2"
  error_message ""
```

**TTL:** 24 hours (configurable)

### Audit Events (List)

```redis
LPUSH transfer:{idempotency_key}:events
  '{"time":"2025-10-10T10:00:00Z","event":"RECEIVED"}'
  '{"time":"2025-10-10T10:00:01Z","event":"QUEUED_REGISTRATION"}'
  '{"time":"2025-10-10T10:00:05Z","event":"REGISTERED"}'
  '{"time":"2025-10-10T10:00:06Z","event":"QUEUED_TRANSFER"}'
  '{"time":"2025-10-10T10:00:10Z","event":"SUBMITTED","tx_hash":"abc123..."}'
  '{"time":"2025-10-10T10:00:11Z","event":"QUEUED_VERIFICATION"}'
  '{"time":"2025-10-10T10:00:15Z","event":"COMPLETED"}'
```

**TTL:** Same as transfer state (24 hours)

### Registered Accounts (Set)

```redis
SADD registered_accounts
  "alice.near"
  "bob.near"
  "charlie.near"
```

**TTL:** None (persists forever, grows over time)

### Registration Locks (String)

```redis
SET register_lock:alice.near "worker-uuid-123" EX 30
```

**TTL:** 30 seconds (auto-release if worker crashes)

### Access Key Leases (String)

```redis
SET access_key:lease:ed25519:abc123 "worker-uuid-456" NX EX 10
```

**TTL:** 10 seconds (auto-release if worker crashes)

### Nonce Counters (Integer)

```redis
SET nonce:ed25519:abc123 12345
INCR nonce:ed25519:abc123  # Returns 12346
```

**TTL:** None (persists forever)

**Initialization:** Set to 0 on first use, or fetch from RPC on startup

### Block Hash Cache (Hash)

```redis
HSET block_hash:latest
  hash "abc123..."
  cached_at "2025-10-10T10:00:00Z"
```

**TTL:** 1 hour (but block hash valid for 24 hours)

### Redis Streams

**Design:** 3 streams total (simple is better!)

**Registration Stream:**
```redis
XADD ftrelay:testnet:registration * transfer_id "idem-key-123" retry_count "0"
```
**Consumer Group:** `registration_workers`

**Transfer Stream:**
```redis
XADD ftrelay:testnet:transfer * transfer_id "idem-key-123" retry_count "0"
```
**Consumer Group:** `transfer_workers`

**Verification Stream:**
```redis
XADD ftrelay:testnet:verification * transfer_id "idem-key-123" tx_hash "abc123..." retry_count "0"
```
**Consumer Group:** `verification_workers`

**Retry Strategy:**
- On failure: ACK original message, push back to **same stream** with incremented retry_count
- On success: ACK message, move to next stage
- retry_count in message payload tracks attempts
- Simple, explicit, no separate retry queues needed

### Redis Persistence Configuration

**Required:** AOF (Append-Only File) + RDB snapshots

```conf
# redis.conf
appendonly yes
appendfsync everysec
save 900 1
save 300 10
save 60 10000
```

This ensures:
- No data loss on crash (AOF replays all writes)
- Fast restarts (RDB provides base snapshot)


---

## API Specification

### POST /transfer

Submit a new transfer request with idempotency.

**Request:**
```http
POST /transfer HTTP/1.1
Host: localhost:3000
Content-Type: application/json
X-Idempotency-Key: request-abc-123

{
  "receiver_id": "alice.near",
  "amount": "1000000000000000000000000"
}
```

**Response (201 Created):**
```json
{
  "transfer_id": "request-abc-123",
  "status": "RECEIVED",
  "receiver_id": "alice.near",
  "amount": "1000000000000000000000000",
  "created_at": "2025-10-10T10:00:00Z"
}
```

**Response (200 OK - Idempotency Hit):**
```json
{
  "transfer_id": "request-abc-123",
  "status": "COMPLETED",
  "receiver_id": "alice.near",
  "amount": "1000000000000000000000000",
  "tx_hash": "abc123...",
  "created_at": "2025-10-10T10:00:00Z",
  "completed_at": "2025-10-10T10:00:15Z"
}
```

**Response (400 Bad Request):**
```json
{
  "error": "Invalid receiver account ID"
}
```

**Response (409 Conflict):**
```json
{
  "error": "Idempotency key already used with different parameters"
}
```

**Note:** The `X-Idempotency-Key` header value becomes the `transfer_id`. Throughout the system, we use `transfer_id` and `idempotency_key` interchangeably - they are the same value.

### GET /transfer/{idempotency_key}

Query transfer status and full audit trail.

**Request:**
```http
GET /transfer/request-abc-123 HTTP/1.1
Host: localhost:3000
```

**Response (200 OK):**
```json
{
  "transfer_id": "request-abc-123",
  "status": "COMPLETED",
  "receiver_id": "alice.near",
  "amount": "1000000000000000000000000",
  "tx_hash": "abc123...",
  "created_at": "2025-10-10T10:00:00Z",
  "updated_at": "2025-10-10T10:00:15Z",
  "completed_at": "2025-10-10T10:00:15Z",
  "retry_count": 2,
  "events": [
    {
      "time": "2025-10-10T10:00:00Z",
      "event": "RECEIVED"
    },
    {
      "time": "2025-10-10T10:00:01Z",
      "event": "QUEUED_REGISTRATION"
    },
    {
      "time": "2025-10-10T10:00:05Z",
      "event": "REGISTERED"
    },
    {
      "time": "2025-10-10T10:00:06Z",
      "event": "QUEUED_TRANSFER"
    },
    {
      "time": "2025-10-10T10:00:10Z",
      "event": "SUBMITTED",
      "tx_hash": "abc123..."
    },
    {
      "time": "2025-10-10T10:00:11Z",
      "event": "QUEUED_VERIFICATION"
    },
    {
      "time": "2025-10-10T10:00:15Z",
      "event": "COMPLETED"
    }
  ]
}
```

**Response (404 Not Found):**
```json
{
  "error": "Transfer not found"
}
```

### GET /health

Health check endpoint.

**Response (200 OK):**
```json
{
  "status": "healthy",
  "redis": "connected",
  "rpc": "connected"
}
```


---

## Worker Flows

### Registration Worker Flow

```rust
async fn registration_worker_loop(ctx: Context) -> Result<()> {
    let consumer_name = format!("registration-{}", Uuid::new_v4());
    
    loop {
        // 1. Pull from registration stream
        let messages = ctx.stream
            .pop_batch(&consumer_name, 50, ctx.linger_ms)
            .await?;
        
        if messages.is_empty() {
            continue;
        }
        
        for (stream_id, transfer_id) in messages {
            // 2. Get transfer state
            let transfer = ctx.redis.get_transfer_state(&transfer_id).await?;
            let account = &transfer.receiver_id;
            
            // 3. FAST PATH: Check if already registered
            if ctx.redis.is_registered(account).await? {
                ctx.redis.update_status(&transfer_id, Status::Registered).await?;
                ctx.redis.log_event(&transfer_id, Event::Registered).await?;
                ctx.redis.enqueue_transfer(&transfer_id).await?;
                ctx.stream.ack(stream_id).await?;
                continue;
            }
            
            // 4. Try to acquire distributed lock
            let lock_key = format!("register_lock:{}", account);
            let acquired = ctx.redis.set_nx_ex(&lock_key, &consumer_name, 30).await?;
            
            if !acquired {
                // Another worker is handling this account
                // ACK and re-enqueue to same stream (will retry)
                ctx.stream.ack(stream_id).await?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                ctx.redis.enqueue_registration(&transfer_id).await?;
                continue;
            }
            
            // 5. Double-check registration (race condition safety)
            if ctx.redis.is_registered(account).await? {
                ctx.redis.del(&lock_key).await?;
                ctx.redis.update_status(&transfer_id, Status::Registered).await?;
                ctx.redis.log_event(&transfer_id, Event::Registered).await?;
                ctx.redis.enqueue_transfer(&transfer_id).await?;
                ctx.stream.ack(stream_id).await?;
                continue;
            }
            
            // 6. Actually register the account
            match ctx.rpc.register_account(account, &ctx.token).await {
                Ok(tx_hash) => {
                    info!("Registered {} with tx {}", account, tx_hash);
                    ctx.redis.mark_registered(account).await?;
                    ctx.redis.del(&lock_key).await?;
                    ctx.redis.update_status(&transfer_id, Status::Registered).await?;
                    ctx.redis.log_event(&transfer_id, Event::Registered).await?;
                    ctx.redis.enqueue_transfer(&transfer_id).await?;
                    ctx.stream.ack(stream_id).await?;
                }
                Err(e) if e.is_already_registered() => {
                    // Already registered (race condition on-chain)
                    info!("Account {} already registered", account);
                    ctx.redis.mark_registered(account).await?;
                    ctx.redis.del(&lock_key).await?;
                    ctx.redis.update_status(&transfer_id, Status::Registered).await?;
                    ctx.redis.log_event(&transfer_id, Event::Registered).await?;
                    ctx.redis.enqueue_transfer(&transfer_id).await?;
                    ctx.stream.ack(stream_id).await?;
                }
                Err(e) => {
                    // Registration failed
                    warn!("Failed to register {}: {:?}", account, e);
                    ctx.redis.del(&lock_key).await?;
                    
                    // Check retry count
                    let retry_count = transfer.retry_count + 1;
                    ctx.stream.ack(stream_id).await?; // Always ACK
                    
                    if retry_count < MAX_RETRIES {
                        ctx.redis.increment_retry_count(&transfer_id).await?;
                        ctx.redis.enqueue_registration(&transfer_id).await?; // Re-enqueue to same stream
                    } else {
                        ctx.redis.update_status(&transfer_id, Status::Failed).await?;
                        ctx.redis.log_event(&transfer_id, Event::Failed { 
                            reason: format!("Registration failed after {} retries", retry_count) 
                        }).await?;
                    }
                }
            }
        }
    }
}
```

### Transfer Worker Flow

```rust
async fn transfer_worker_loop(ctx: Context) -> Result<()> {
    let consumer_name = format!("transfer-{}", Uuid::new_v4());
    let max_batch_size = 100; // NEAR protocol limit
    
    loop {
        // 1. Pull batch from transfer stream
        let messages = ctx.stream
            .pop_batch(&consumer_name, max_batch_size, ctx.linger_ms)
            .await?;
        
        if messages.is_empty() {
            continue;
        }
        
        // 2. Lease an access key from pool
        let leased_key = match ctx.access_key_pool.lease().await {
            Ok(key) => key,
            Err(e) => {
                warn!("Failed to lease access key: {:?}, will retry", e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        
        // 3. Get nonce for this key (Redis INCR - atomic)
        let nonce = ctx.nonce_manager.get_next_nonce(&leased_key.key_id).await?;
        
        // 4. Fetch fresh block hash (cached for 1 hour)
        let block_hash = ctx.rpc.get_block_hash().await?;
        
        // 5. Load transfer states for all messages
        let mut transfers = Vec::new();
        for (stream_id, transfer_id) in &messages {
            let transfer = ctx.redis.get_transfer_state(transfer_id).await?;
            transfers.push((stream_id.clone(), transfer_id.clone(), transfer));
        }
        
        // 6. Build batch transaction (100 actions)
        let actions: Vec<Action> = transfers.iter()
            .map(|(_, _, transfer)| {
                Action::FunctionCall(FunctionCallAction {
                    method_name: "ft_transfer".to_string(),
                    args: json!({
                        "receiver_id": transfer.receiver_id,
                        "amount": transfer.amount
                    }).to_string().into_bytes(),
                    gas: 3_000_000_000_000, // 3 TGas per transfer
                    deposit: 1, // 1 yoctoNEAR attachment
                })
            })
            .collect();
        
        let transaction = Transaction {
            signer_id: ctx.relay_account.clone(),
            public_key: leased_key.public_key.clone(),
            nonce,
            receiver_id: ctx.token.clone(),
            block_hash,
            actions,
        };
        
        // 7. Sign transaction
        let signed_tx = transaction.sign(&leased_key.secret_key);
        
        // 8. Submit to RPC
        match ctx.rpc.broadcast_tx(signed_tx).await {
            Ok(tx_hash) => {
                info!("Submitted batch of {} transfers, tx: {}", transfers.len(), tx_hash);
                
                // 9. Update all transfers to SUBMITTED and enqueue for verification
                for (stream_id, transfer_id, _) in &transfers {
                    ctx.redis.update_status(transfer_id, Status::Submitted).await?;
                    ctx.redis.update_tx_hash(transfer_id, &tx_hash.to_string()).await?;
                    ctx.redis.log_event(transfer_id, Event::Submitted { 
                        tx_hash: tx_hash.to_string() 
                    }).await?;
                    ctx.redis.enqueue_verification(transfer_id, &tx_hash.to_string()).await?;
                    ctx.stream.ack(stream_id).await?;
                }
            }
            Err(e) => {
                warn!("Failed to submit batch: {:?}", e);
                
                // 10. Handle failure - ACK and re-enqueue with retry count
                for (stream_id, transfer_id, transfer) in &transfers {
                    let retry_count = transfer.retry_count + 1;
                    ctx.stream.ack(stream_id).await?; // Always ACK
                    
                    if retry_count < MAX_RETRIES {
                        ctx.redis.increment_retry_count(transfer_id).await?;
                        ctx.redis.enqueue_transfer(transfer_id).await?; // Re-enqueue to same stream
                    } else {
                        ctx.redis.update_status(transfer_id, Status::Failed).await?;
                        ctx.redis.log_event(transfer_id, Event::Failed {
                            reason: format!("Submission failed after {} retries: {:?}", retry_count, e)
                        }).await?;
                    }
                }
            }
        }
        
        // 11. Release access key (leased_key drops here, auto-releases)
        drop(leased_key);
    }
}
```

### Verification Worker Flow

```rust
async fn verification_worker_loop(ctx: Context) -> Result<()> {
    let consumer_name = format!("verification-{}", Uuid::new_v4());
    
    loop {
        // 1. Pull from verification stream
        let messages = ctx.stream
            .pop_batch(&consumer_name, 50, ctx.linger_ms)
            .await?;
        
        if messages.is_empty() {
            continue;
        }
        
        for (stream_id, data) in messages {
            let transfer_id = data.get("transfer_id").unwrap();
            let tx_hash = data.get("tx_hash").unwrap();
            
            // 2. Wait ~6 seconds (2-3 blocks) before checking
            let transfer = ctx.redis.get_transfer_state(transfer_id).await?;
            let submitted_at = transfer.updated_at;
            let elapsed = Instant::now().duration_since(submitted_at);
            
            if elapsed < Duration::from_secs(6) {
                let wait_time = Duration::from_secs(6) - elapsed;
                tokio::time::sleep(wait_time).await;
            }
            
            // 3. Check transaction status
            match ctx.rpc.check_tx_status(&tx_hash, &ctx.relay_account).await? {
                TxStatus::Success(outcome) => {
                    info!("Transfer {} completed successfully", transfer_id);
                    ctx.redis.update_status(transfer_id, Status::Completed).await?;
                    ctx.redis.log_event(transfer_id, Event::Completed).await?;
                    ctx.stream.ack(stream_id).await?;
                }
                TxStatus::Failed(reason) => {
                    warn!("Transfer {} failed: {}", transfer_id, reason);
                    ctx.redis.update_status(transfer_id, Status::Failed).await?;
                    ctx.redis.log_event(transfer_id, Event::Failed { reason }).await?;
                    ctx.stream.ack(stream_id).await?;
                }
                TxStatus::Pending => {
                    // Still pending, ACK and re-enqueue for later check
                    let retry_count = transfer.retry_count + 1;
                    ctx.stream.ack(stream_id).await?; // Always ACK
                    
                    if retry_count < MAX_VERIFICATION_RETRIES {
                        ctx.redis.increment_retry_count(transfer_id).await?;
                        ctx.redis.enqueue_verification(transfer_id, tx_hash).await?; // Re-enqueue to same stream
                    } else {
                        warn!("Transfer {} timed out after {} checks", transfer_id, retry_count);
                        ctx.redis.update_status(transfer_id, Status::Failed).await?;
                        ctx.redis.log_event(transfer_id, Event::Failed {
                            reason: "Verification timeout".to_string()
                        }).await?;
                    }
                }
            }
        }
    }
}
```


---

## Registration Deduplication

### The Problem

Multiple transfers for the same receiver arriving simultaneously:

```
T=0ms:  Transfer 1 for alice.near → Not registered → Queue registration
T=1ms:  Transfer 2 for alice.near → Not registered → Queue registration (DUPLICATE!)
T=2ms:  Transfer 3 for alice.near → Not registered → Queue registration (DUPLICATE!)
```

Without deduplication, we'd try to register `alice.near` three times, wasting RPC calls and potentially failing.

### The Solution: Redis SET + Distributed Lock

**Three-Layer Defense:**

1. **Fast Path Check** (Redis SET): `SISMEMBER registered_accounts alice.near`
   - If true → Skip directly to transfer queue (no lock needed)
   - If false → Proceed to lock acquisition
   
2. **Distributed Lock** (Redis SET NX): `SET register_lock:alice.near worker-123 NX EX 30`
   - Only one worker can acquire the lock
   - Other workers wait 1 second and requeue (by then registration completes)
   - Auto-expires after 30 seconds (handles worker crashes)

3. **Double-Check After Lock** (Redis SET again): `SISMEMBER registered_accounts alice.near`
   - Another worker might have completed registration while we waited for lock
   - Prevents unnecessary registration even if we won the lock race

### Flow Diagram

```
Transfer arrives for alice.near
        ↓
SISMEMBER registered_accounts alice.near
        ↓
    Is member?
   ↙         ↘
 YES          NO
  ↓            ↓
Skip to    SET register_lock:alice.near worker-123 NX EX 30
transfer       ↓
queue      Acquired lock?
          ↙            ↘
        YES             NO
         ↓               ↓
    Double-check    Wait 1 sec,
    SISMEMBER       requeue transfer
         ↓
    Still not
    registered?
    ↙          ↘
  YES           NO
   ↓             ↓
Register     Release lock,
on-chain     skip to transfer
   ↓
Success or
AlreadyRegistered
   ↓
SADD registered_accounts alice.near
   ↓
Release lock
   ↓
Enqueue to transfer stream
```

### Edge Cases Handled

| Scenario | Handling |
|----------|----------|
| Worker crashes during registration | Lock auto-expires (30s), next worker retries |
| Registration succeeds but Redis update fails | Next attempt gets "AlreadyRegistered" error, we catch it and update Redis |
| Account pre-registered outside our system | "AlreadyRegistered" error caught, marked in Redis |
| Multiple workers try to register same account | Only one gets lock, others wait and see it's done |
| Registration fails with retriable error | Lock released, transfer requeued with retry count |

### Performance Characteristics

- **Fast path** (already registered): 1 Redis call (SISMEMBER)
- **Registration needed**: 4-5 Redis calls + 1 RPC call
- **Lock contention**: O(1) wait time (~1 second) before retry


---

## Access Key & Nonce Management

### Access Key Pool

**Configuration:**
```bash
RELAY_ACCESS_KEYS='["ed25519:key1...", "ed25519:key2...", "ed25519:key3..."]'
```

Parsed into:
```rust
pub struct AccessKeyPool {
    keys: Vec<AccessKey>,
    redis: ConnectionManager,
}

pub struct AccessKey {
    pub key_id: String,        // Hash of public key (unique identifier)
    pub secret_key: SecretKey,
    pub public_key: PublicKey,
}
```

### Leasing Mechanism (Local List + SET NX - Production-Proven Pattern)

**Goal:** Ensure only one worker uses an access key at a time (prevents nonce collisions).

**Design Decision:** After thorough review, the simplest and most robust approach is:
- Local list of keys (in memory)
- Single atomic Redis operation (SET NX)
- Ownership token for safe release
- TTL handles all cleanup (no reaper needed)

**Redis Data Structures:**
```redis
# Only lease markers exist in Redis (minimal state)
access_key:lease:{key_id} = {ownership_token} EX 30
```

**Implementation:**
```rust
pub struct AccessKeyPool {
    keys: Vec<AccessKey>,  // Keys stored in memory
    redis: ConnectionManager,
}

impl AccessKeyPool {
    pub async fn lease(&self) -> Result<LeasedKey> {
        loop {
            // Pick random key (spreads load evenly)
            let key = self.keys.choose(&mut rand::thread_rng()).unwrap();
            let lease_key = format!("access_key:lease:{}", key.key_id);
            let token = Uuid::new_v4().to_string();
            
            // Single atomic operation (SET NX)
            let acquired: bool = redis::cmd("SET")
                .arg(&lease_key)
                .arg(&token)
                .arg("NX")  // Only if not exists (atomic!)
                .arg("EX")
                .arg(30)    // 30 second TTL (auto-cleanup)
                .query_async(&mut self.redis)
                .await?;
            
            if acquired {
                return Ok(LeasedKey {
                    key_id: key.key_id.clone(),
                    secret_key: key.secret_key.clone(),
                    public_key: key.public_key.clone(),
                    lease_key,
                    token,  // Store for verification on release
                    redis: self.redis.clone(),
                });
            }
            
            // Key was busy, try another (with 50 keys, ~2 iterations average)
        }
    }
}

pub struct LeasedKey {
    key_id: String,
    secret_key: SecretKey,
    public_key: PublicKey,
    lease_key: String,
    token: String,  // Ownership token
    redis: ConnectionManager,
}

impl Drop for LeasedKey {
    fn drop(&mut self) {
        // Atomic release with ownership verification (Lua script)
        let script = r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        "#;
        
        let lease_key = self.lease_key.clone();
        let token = self.token.clone();
        let mut redis = self.redis.clone();
        
        tokio::spawn(async move {
            let _: Result<i32, _> = redis::Script::new(script)
                .key(&lease_key)
                .arg(&token)
                .invoke_async(&mut redis)
                .await;
        });
    }
}
```

**Why this approach?**

**Attempted complexity:**
We initially tried BRPOPLPUSH with Redis LISTs. Detailed production review revealed **13 serious issues**:
1. Two-step non-atomic lease (race window between BRPOPLPUSH and SET)
2. SET without ownership token (can be overwritten)
3. No verification on release (any worker can return any key)
4. Non-atomic release (multiple Redis commands can interleave)
5. TTL expiring mid-work (needs heartbeat or longer TTL)
6. Missing revert on failed lease mark (key stranded in leased list)
7. Timeout handling bug (BRPOPLPUSH returns Option)
8. Blocking command blocks entire connection
9. Redis Cluster hash slot issues (needs hash tags)
10. Duplicate entries risk (LISTs don't enforce uniqueness)
11. Complex reaper logic needed
12. State coordination races
13. More Redis state to manage

**Simple solution:**
- ✅ Single atomic operation (SET NX - inherently atomic)
- ✅ Ownership token prevents foreign/late releases
- ✅ No LIST coordination races
- ✅ No reaper needed (TTL auto-expires)
- ✅ No duplicate keys possible
- ✅ No cluster hash tag issues
- ✅ No blocking connection needed
- ✅ Loop is negligible (50 keys, ~80% first-try hit rate)
- ✅ Minimal Redis state (only active leases)
- ✅ Production-proven pattern (used by Redis itself for distributed locks)

**Loop performance:**
- 50 keys, 10 workers → ~20% leased at any time
- 80% probability of first-try success
- Average 1.25 iterations per lease
- Entirely CPU-bound (no I/O in loop)

**This is genuinely simpler** - trading a trivial CPU loop for elimination of 13 production bugs.

### Nonce Management

**Goal:** Each access key has its own nonce counter. Atomically increment before each transaction.

**Implementation:**
```rust
pub struct NonceManager {
    redis: ConnectionManager,
}

impl NonceManager {
    pub async fn get_next_nonce(&mut self, key_id: &str) -> Result<u64> {
        let nonce_key = format!("nonce:{}", key_id);
        
        // Atomic increment (returns new value)
        let nonce: u64 = redis::cmd("INCR")
            .arg(&nonce_key)
            .query_async(&mut self.redis)
            .await?;
        
        Ok(nonce)
    }
    
    pub async fn initialize_nonce(&mut self, key_id: &str, initial_nonce: u64) -> Result<()> {
        let nonce_key = format!("nonce:{}", key_id);
        
        // Set only if not exists
        redis::cmd("SET")
            .arg(&nonce_key)
            .arg(initial_nonce)
            .arg("NX")
            .query_async(&mut self.redis)
            .await?;
        
        Ok(())
    }
}
```

### Initialization on Startup

```rust
async fn initialize_nonces(
    rpc: &NearRpcClient,
    nonce_manager: &mut NonceManager,
    access_keys: &[AccessKey],
    relay_account: &AccountId,
) -> Result<()> {
    for key in access_keys {
        // Check if nonce already exists in Redis
        let nonce_key = format!("nonce:{}", key.key_id);
        let exists: bool = redis::cmd("EXISTS")
            .arg(&nonce_key)
            .query_async(&mut nonce_manager.redis)
            .await?;
        
        if !exists {
            // Fetch current nonce from RPC
            let access_key_view = rpc.get_access_key(
                relay_account,
                &key.public_key
            ).await?;
            
            // Initialize Redis with RPC nonce
            nonce_manager.initialize_nonce(
                &key.key_id,
                access_key_view.nonce
            ).await?;
            
            info!("Initialized nonce for key {} to {}", key.key_id, access_key_view.nonce);
        }
    }
    
    Ok(())
}
```

### Nonce Hole Handling

**NEAR Protocol Behavior:**
- Accepts any nonce > last_used_nonce
- Tolerates gaps < 1,000,000 (1M)
- Our approach: Increment nonce BEFORE signing
- If submission fails, nonce has a "hole" (unused nonce)
- This is acceptable and doesn't affect subsequent transactions

**Example:**
```
Initial nonce: 100
Worker A: INCR nonce:key1 → 101, sign tx, submit → FAILS (RPC error)
Worker B: INCR nonce:key1 → 102, sign tx, submit → SUCCESS

Result: Nonce 101 is "skipped" but transaction with nonce 102 succeeds.
```

### Concurrency Safety

**Scenario:** 10 workers trying to submit transactions simultaneously

**Without Access Key Leasing:**
```
Worker 1: Uses key1, nonce 100
Worker 2: Uses key1, nonce 100 (COLLISION!)
Worker 3: Uses key1, nonce 100 (COLLISION!)
Result: 2 transactions fail with "InvalidNonce"
```

**With Access Key Leasing:**
```
Worker 1: Leases key1 (lock acquired), nonce 100
Worker 2: Tries key1 (locked), tries key2 (lock acquired), nonce 50
Worker 3: Tries key1 (locked), tries key2 (locked), tries key3 (lock acquired), nonce 75
Result: All use different keys, no collisions
```

### Performance Optimization

**With 50 access keys:**
- 10 concurrent workers can all lease unique keys
- No waiting (unless > 50 workers)
- Each key has independent nonce counter
- Throughput: Limited only by RPC, not by nonce management

**Lease Duration:**
- 10 seconds (enough for sign + submit)
- Auto-releases if worker crashes
- Prevents indefinite locks


---

## Error Handling & Retries

### Retry Strategy

**Principles:**
1. Let Redis Streams handle retries (XREADGROUP with XPENDING)
2. No manual retry loops (simpler code, natural backoff)
3. Track retry_count in transfer state
4. Max retries before marking as FAILED

### Retry Limits

| Worker Type | Max Retries | Reason |
|-------------|-------------|--------|
| Registration | 10 | Account registration is expensive, give it time |
| Transfer | 10 | RPC might be temporarily overloaded |
| Verification | 20 | Transaction might take longer to confirm |

### Error Categories

#### 1. Retriable Errors

**Registration Worker:**
- RPC connection error
- RPC timeout
- RPC rate limit
- "Nonce too large" (wait and retry)

**Transfer Worker:**
- Failed to lease access key (all keys busy)
- RPC connection error
- RPC timeout
- RPC rate limit
- "Nonce too large" (should not happen with our leasing, but retry)

**Verification Worker:**
- RPC connection error
- RPC timeout
- Transaction still pending

**Action:** Increment retry_count, NACK message (Redis will redeliver)

#### 2. Permanent Errors

**Registration Worker:**
- Invalid account ID format
- Account does not exist (can't register non-existent account)

**Transfer Worker:**
- Invalid transaction (shouldn't happen with our construction)
- Insufficient balance in relay account

**Verification Worker:**
- Transaction failed on-chain (e.g., insufficient storage)
- Transaction not found after 20 checks

**Action:** Mark as FAILED, ACK message (remove from queue)

### Error Handling Flow

```rust
match result {
    Ok(success) => {
        // Update state, log event, enqueue next step, ACK message
        stream.ack(stream_id).await?;
        // ... move to next stage
    }
    Err(e) if e.is_retriable() => {
        let retry_count = transfer.retry_count + 1;
        stream.ack(stream_id).await?; // Always ACK (explicit control)
        
        if retry_count < MAX_RETRIES {
            redis.increment_retry_count(&transfer_id).await?;
            // Re-enqueue to SAME stream with incremented retry_count
            redis.enqueue_to_stream(&stream_name, &transfer_id).await?;
        } else {
            redis.update_status(&transfer_id, Status::Failed).await?;
            redis.log_event(&transfer_id, Event::Failed {
                reason: format!("Max retries ({}) exceeded", MAX_RETRIES)
            }).await?;
        }
    }
    Err(e) => {
        // Permanent error
        stream.ack(stream_id).await?;
        redis.update_status(&transfer_id, Status::Failed).await?;
        redis.log_event(&transfer_id, Event::Failed {
            reason: format!("{:?}", e)
        }).await?;
    }
}
```

**Key Points:**
- Always ACK messages (never leave in PEL)
- On retriable error: ACK, increment retry_count, re-enqueue to same stream
- On permanent error or max retries: ACK and mark as failed
- Simple, explicit, predictable

### Observability

**Every error is logged:**
```rust
ctx.redis.log_event(&transfer_id, Event::Error {
    error_type: "RpcTimeout",
    message: "Request timed out after 30s",
    retry_count: 3,
    will_retry: true,
}).await?;
```

**Transfer state includes:**
- `retry_count`: Number of attempts
- `error_message`: Last error encountered
- `updated_at`: When last retry occurred

**Querying problematic transfers:**
```bash
# Redis CLI
KEYS transfer:* | while read key; do
  STATUS=$(redis-cli HGET $key status)
  RETRY=$(redis-cli HGET $key retry_count)
  if [ "$RETRY" -gt 5 ]; then
    echo "$key: $STATUS (retries: $RETRY)"
  fi
done
```

### Circuit Breaker (Future Enhancement)

Not implemented initially, but can be added:

```rust
pub struct CircuitBreaker {
    failure_threshold: usize,
    success_threshold: usize,
    timeout: Duration,
    state: Arc<Mutex<CircuitState>>,
}

enum CircuitState {
    Closed,           // Normal operation
    Open { until: Instant },  // Blocking requests
    HalfOpen { successes: usize },  // Testing recovery
}
```

**Trigger conditions:**
- 10 consecutive RPC failures → Open circuit for 30s
- After 30s → HalfOpen, allow 3 test requests
- If 3 successes → Close circuit
- If any failure → Open again for 60s (exponential backoff)


---

## Implementation Checklist

### Phase 1: Foundation (Est. 1.5 hours)

#### 1.1 Project Setup
- [ ] Create new branch `rearchitecture`
- [ ] Update `Cargo.toml` dependencies:
  - [ ] Add `near-jsonrpc-client = "0.18"`
  - [ ] Add `near-primitives = "0.32"`
  - [ ] Keep `near-crypto = "0.32"` (for signing)
  - [ ] Remove `near-api-rs` (if present)
  - [ ] Add `axum = "0.7"` (HTTP framework)
  - [ ] Add `redis = { version = "0.24", features = ["tokio-comp", "connection-manager"] }`
  - [ ] Add `tokio = { version = "1", features = ["full"] }`
  - [ ] Add `serde = { version = "1", features = ["derive"] }`
  - [ ] Add `serde_json = "1"`
  - [ ] Add `anyhow = "1"`
  - [ ] Add `uuid = { version = "1", features = ["v4"] }`
  - [ ] Add `chrono = { version = "0.4", features = ["serde"] }`
  - [ ] Add `rand = "0.9"` (for random key selection in access key pool)

#### 1.2 Core Data Structures
- [ ] Create `src/types.rs`:
  - [ ] `TransferState` struct (status, receiver_id, amount, tx_hash, timestamps, retry_count)
  - [ ] `Status` enum (Received, QueuedRegistration, Registered, QueuedTransfer, Submitted, QueuedVerification, Completed, Failed)
  - [ ] `Event` struct (time, event_type, metadata)
  - [ ] `AccessKey` struct (key_id, secret_key, public_key)

#### 1.3 Configuration
- [ ] Create `src/config.rs`:
  - [ ] `RelayConfig` struct
  - [ ] Environment variable parsing
  - [ ] Validation (account IDs, keys, URLs)
  - [ ] Default values

**Environment Variables:**
```bash
REDIS_URL=redis://localhost:6379
NEAR_RPC_URL=https://rpc.testnet.near.org
NEAR_NETWORK=testnet
FT_TOKEN_ADDRESS=token.testnet
RELAY_ACCOUNT_ID=relay.testnet
RELAY_ACCESS_KEYS='["ed25519:...", "ed25519:..."]'
SERVER_PORT=3000
MAX_RETRIES=10
MAX_VERIFICATION_RETRIES=20
```

#### 1.4 Redis Helper
- [ ] Create `src/redis_helpers.rs`:
  - [ ] `RedisHelper` struct with ConnectionManager
  - [ ] `store_transfer_state()` - HSET
  - [ ] `get_transfer_state()` - HGETALL
  - [ ] `update_status()` - HSET status field
  - [ ] `update_tx_hash()` - HSET tx_hash field
  - [ ] `increment_retry_count()` - HINCRBY retry_count
  - [ ] `log_event()` - LPUSH to events list
  - [ ] `get_events()` - LRANGE events list
  - [ ] `is_registered()` - SISMEMBER registered_accounts
  - [ ] `mark_registered()` - SADD registered_accounts
  - [ ] `set_nx_ex()` - SET with NX and EX flags
  - [ ] Stream operations:
    - [ ] `enqueue_registration()` - XADD to registration stream
    - [ ] `enqueue_transfer()` - XADD to transfer stream
    - [ ] `enqueue_verification()` - XADD to verification stream

#### 1.5 RPC Client Wrapper
- [ ] Create `src/rpc_client.rs`:
  - [ ] `NearRpcClient` struct
  - [ ] `get_block_hash()` - Cached for 1 hour
  - [ ] `broadcast_tx()` - Direct submit, no retries
  - [ ] `check_tx_status()` - Query transaction outcome
  - [ ] `get_access_key()` - Fetch nonce from RPC (initialization only)
  - [ ] `TxStatus` enum (Pending, Success, Failed)
  - [ ] Error parsing and classification

#### 1.6 Access Key Pool
- [ ] Create `src/access_key_pool.rs`:
  - [ ] `AccessKeyPool` struct (stores keys in Vec)
  - [ ] `lease()` - Loop with random selection + SET NX
  - [ ] `LeasedKey` struct with ownership token
  - [ ] Drop implementation - Lua script for verified release
  - [ ] No Redis initialization needed (keys stay in memory)
  - [ ] No reaper needed (TTL handles cleanup)

#### 1.7 Nonce Manager
- [ ] Create `src/nonce_manager.rs`:
  - [ ] `NonceManager` struct
  - [ ] `get_next_nonce()` - Redis INCR
  - [ ] `initialize_nonce()` - SET NX from RPC value
  - [ ] Startup initialization routine

### Phase 2: Workers (Est. 2 hours)

#### 2.1 Stream Queue Abstraction
- [ ] Create `src/stream_queue.rs`:
  - [ ] `StreamQueue` struct (wrapper around Redis Stream)
  - [ ] `create_consumer_group()` - XGROUP CREATE
  - [ ] `pop_batch()` - XREADGROUP with BLOCK and COUNT
  - [ ] `ack()` - XACK message
  - [ ] `enqueue()` - XADD message (for retries back to same stream)

#### 2.2 Registration Worker
- [ ] Create `src/registration_worker.rs`:
  - [ ] `RegistrationWorkerContext` struct
  - [ ] `registration_worker_loop()` main function
  - [ ] Fast path check (SISMEMBER)
  - [ ] Distributed lock acquisition
  - [ ] Double-check after lock
  - [ ] On-chain registration call
  - [ ] Handle "AlreadyRegistered" error
  - [ ] Retry logic
  - [ ] Event logging

#### 2.3 Transfer Worker
- [ ] Create `src/transfer_worker.rs`:
  - [ ] `TransferWorkerContext` struct
  - [ ] `transfer_worker_loop()` main function
  - [ ] Batch pulling (up to 100 messages)
  - [ ] Access key leasing
  - [ ] Nonce fetching
  - [ ] Block hash fetching
  - [ ] Batch transaction construction:
    - [ ] Multiple `ft_transfer` actions
    - [ ] Gas calculation (3 TGas × num_actions)
    - [ ] 1 yoctoNEAR deposit per action
  - [ ] Transaction signing
  - [ ] RPC submission
  - [ ] Success handling (enqueue to verification)
  - [ ] Failure handling (retry logic)
  - [ ] Access key release

#### 2.4 Verification Worker
- [ ] Create `src/verification_worker.rs`:
  - [ ] `VerificationWorkerContext` struct
  - [ ] `verification_worker_loop()` main function
  - [ ] Wait logic (~6 seconds from submission)
  - [ ] RPC status check
  - [ ] Success handling (mark completed)
  - [ ] Failure handling (mark failed)
  - [ ] Pending handling (requeue)
  - [ ] Timeout handling (max retries)

### Phase 3: HTTP API (Est. 1.5 hours)

#### 3.1 API Server
- [ ] Create `src/http.rs`:
  - [ ] `ApiState` struct (shared state: Redis, config)
  - [ ] Server initialization with Axum
  - [ ] Middleware for request logging

#### 3.2 POST /transfer Endpoint
- [ ] Request validation:
  - [ ] Extract `X-Idempotency-Key` header (required)
  - [ ] Parse JSON body
  - [ ] Validate receiver account ID format
  - [ ] Validate amount (positive, within limits)
- [ ] Idempotency check:
  - [ ] Check if transfer exists in Redis
  - [ ] If exists, return existing state
  - [ ] If different params, return 409 Conflict
- [ ] Create transfer:
  - [ ] Generate transfer state
  - [ ] Store in Redis (HSET)
  - [ ] Log RECEIVED event
  - [ ] Enqueue to registration stream
  - [ ] Log QUEUED_REGISTRATION event
  - [ ] Return 201 Created with transfer_id

#### 3.3 GET /transfer/{key} Endpoint
- [ ] Fetch transfer state from Redis (HGET)
- [ ] Fetch event log from Redis (LRANGE)
- [ ] Return 404 if not found
- [ ] Return 200 with full state + events

#### 3.4 GET /health Endpoint
- [ ] Check Redis connection (PING)
- [ ] Check RPC connection (lightweight query)
- [ ] Return 200 with status

### Phase 4: Main Orchestration (Est. 1 hour)

#### 4.1 Main Entry Point
- [ ] Create `src/main.rs`:
  - [ ] Load configuration from environment
  - [ ] Initialize Redis connection pool
  - [ ] Initialize RPC client
  - [ ] Initialize access key pool
  - [ ] Initialize nonce manager
  - [ ] Fetch initial nonces from RPC (one-time)
  - [ ] Create Redis consumer groups (registration, transfer, verification)
  - [ ] Spawn HTTP server task
  - [ ] Spawn registration workers (5 instances)
  - [ ] Spawn transfer workers (10 instances)
  - [ ] Spawn verification workers (5 instances)
  - [ ] Wait for shutdown signal (Ctrl+C)

#### 4.2 Error Handling
- [ ] Graceful worker panic handling
- [ ] Worker restart logic (optional)
- [ ] Signal handling (SIGTERM, SIGINT)

### Phase 5: Testing (Est. 2 hours)

#### 5.1 Unit Tests
- [ ] `redis_helpers_test.rs` - Test Redis operations
- [ ] `access_key_pool_test.rs` - Test leasing logic
- [ ] `nonce_manager_test.rs` - Test atomic increments

#### 5.2 Integration Tests (Sandbox)
- [ ] `tests/sandbox_basic.rs`:
  - [ ] Single transfer test
  - [ ] Idempotency test (same key, same params)
  - [ ] Conflict test (same key, different params)
  - [ ] Batch test (100 transfers)
  - [ ] Large test (1000 transfers)

#### 5.3 Integration Tests (Testnet)
- [ ] `tests/testnet_smoke_small.rs` (5k transfers)
- [ ] `tests/testnet_smoke.rs` (60k transfers, 10-minute target)
- [ ] Verify zero duplicates
- [ ] Verify idempotency works
- [ ] Verify all transfers complete

#### 5.4 Load Testing
- [ ] Simulate 100 tx/sec sustained load
- [ ] Monitor Redis memory usage
- [ ] Monitor RPC error rates
- [ ] Verify worker scaling (add more transfer workers)

### Phase 6: Documentation & Deployment (Est. 1 hour)

#### 6.1 Documentation
- [ ] Update README.md with new architecture
- [ ] API documentation (OpenAPI/Swagger)
- [ ] Deployment guide (Docker Compose)
- [ ] Configuration reference
- [ ] Troubleshooting guide

#### 6.2 Docker Compose
- [ ] Update `docker-compose.yml`:
  - [ ] Redis with AOF persistence
  - [ ] ft-relay service with new binary
  - [ ] Environment variables

#### 6.3 Deployment
- [ ] Build Docker image
- [ ] Deploy to staging
- [ ] Smoke test on staging
- [ ] Deploy to production

---

## Success Criteria

- [ ] **100 tx/second sustained throughput** (tested)
- [ ] **Zero duplicates** (verified in tests)
- [ ] **True idempotency** (same key = same transfer)
- [ ] **Full observability** (GET /transfer/{key} shows complete audit trail)
- [ ] **60k transfers in < 10 minutes** (testnet test passes)
- [ ] **No nonce collisions** (access key leasing working)
- [ ] **No manual RPC calls for nonces** (Redis INCR only)
- [ ] **Graceful error handling** (retries work, failures logged)

---

## Estimated Timeline

| Phase | Tasks | Time | Running Total |
|-------|-------|------|---------------|
| Phase 1 | Foundation | 1.5h | 1.5h |
| Phase 2 | Workers | 2h | 3.5h |
| Phase 3 | HTTP API | 1.5h | 5h |
| Phase 4 | Main | 1h | 6h |
| Phase 5 | Testing | 2h | 8h |
| Phase 6 | Docs & Deploy | 1h | 9h |

**Total:** ~9 hours (conservative estimate)

---

## Ready to Build!

This document serves as the complete blueprint for the rearchitecture. Follow the checklist phase by phase, checking off items as they're completed.

**Next Step:** Create branch and start with Phase 1.1 (Project Setup).

