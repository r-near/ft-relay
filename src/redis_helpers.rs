use anyhow::Result;
use chrono::Utc;
use redis::aio::ConnectionLike;
use redis::AsyncCommands;
use serde::de::DeserializeOwned;

use crate::types::{Event, Status, TransferMessage, TransferState, VerificationMessage, VerificationTxMessage};

pub async fn store_transfer_state<C>(conn: &mut C, state: &TransferState) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}", state.transfer_id);
    conn.hset_multiple::<_, _, _, ()>(
        &key,
        &[
            ("status", state.status.as_str()),
            ("receiver_id", state.receiver_id.as_str()),
            ("amount", state.amount.as_str()),
            ("created_at", &state.created_at.to_rfc3339()),
            ("updated_at", &state.updated_at.to_rfc3339()),
            ("retry_count", &state.retry_count.to_string()),
        ],
    )
    .await?;
    if let Some(ref tx_hash) = state.tx_hash {
        conn.hset::<_, _, _, ()>(&key, "tx_hash", tx_hash).await?;
    }
    if let Some(ref completed_at) = state.completed_at {
        conn.hset::<_, _, _, ()>(&key, "completed_at", &completed_at.to_rfc3339())
            .await?;
    }
    if let Some(ref error) = state.error_message {
        conn.hset::<_, _, _, ()>(&key, "error_message", error)
            .await?;
    }
    conn.expire::<_, ()>(&key, 86400).await?;
    Ok(())
}

/// Fetch multiple transfer states in one pipelined request (10-100x faster than sequential)
pub async fn get_transfer_states_batch<C>(
    conn: &mut C,
    transfer_ids: &[String],
) -> Result<Vec<Option<TransferState>>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    if transfer_ids.is_empty() {
        return Ok(Vec::new());
    }

    let pipeline_start = std::time::Instant::now();

    // Build pipeline with all HGETALL commands
    let mut pipe = redis::pipe();
    for transfer_id in transfer_ids {
        let key = format!("transfer:{}", transfer_id);
        pipe.hgetall(&key);
    }

    // Execute pipeline (single round-trip for all fetches!)
    let results: Vec<std::collections::HashMap<String, String>> = pipe.query_async(conn).await?;

    let pipeline_duration = pipeline_start.elapsed();
    log::debug!(
        "[REDIS_TIMING] Pipelined HGETALL for {} transfer states took {}ms ({:.1}ms per transfer)",
        transfer_ids.len(),
        pipeline_duration.as_millis(),
        pipeline_duration.as_millis() as f64 / transfer_ids.len() as f64
    );

    // Parse results
    let mut states = Vec::new();
    for (transfer_id, data) in transfer_ids.iter().zip(results.iter()) {
        if data.is_empty() {
            states.push(None);
            continue;
        }

        let status_str = match data.get("status") {
            Some(s) => s,
            None => {
                states.push(None);
                continue;
            }
        };
        let status: Status = serde_json::from_str(&format!("\"{}\"", status_str))?;
        let receiver_id = match data.get("receiver_id") {
            Some(r) => r.clone(),
            None => {
                states.push(None);
                continue;
            }
        };
        let amount = match data.get("amount") {
            Some(a) => a.clone(),
            None => {
                states.push(None);
                continue;
            }
        };
        let created_at = match data.get("created_at") {
            Some(c) => c.parse()?,
            None => {
                states.push(None);
                continue;
            }
        };
        let updated_at = data
            .get("updated_at")
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| Utc::now());
        let retry_count = data
            .get("retry_count")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        states.push(Some(TransferState {
            transfer_id: transfer_id.clone(),
            status,
            receiver_id,
            amount,
            tx_hash: data.get("tx_hash").cloned(),
            created_at,
            updated_at,
            completed_at: data.get("completed_at").and_then(|s| s.parse().ok()),
            retry_count,
            error_message: data.get("error_message").cloned(),
        }));
    }

    Ok(states)
}

pub async fn get_transfer_state<C>(conn: &mut C, transfer_id: &str) -> Result<Option<TransferState>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let hgetall_start = std::time::Instant::now();
    let key = format!("transfer:{}", transfer_id);
    let data: std::collections::HashMap<String, String> = conn.hgetall(&key).await?;
    let hgetall_duration = hgetall_start.elapsed();
    if hgetall_duration.as_millis() > 5 {
        log::debug!(
            "[REDIS_TIMING] HGETALL for transfer state took {}ms",
            hgetall_duration.as_millis()
        );
    }

    // If no data, key doesn't exist
    if data.is_empty() {
        return Ok(None);
    }
    let status_str = data
        .get("status")
        .ok_or_else(|| anyhow::anyhow!("Missing status"))?;
    let status: Status = serde_json::from_str(&format!("\"{}\"", status_str))?;
    let receiver_id = data
        .get("receiver_id")
        .ok_or_else(|| anyhow::anyhow!("Missing receiver"))?
        .clone();
    let amount = data
        .get("amount")
        .ok_or_else(|| anyhow::anyhow!("Missing amount"))?
        .clone();
    let created_at = data
        .get("created_at")
        .ok_or_else(|| anyhow::anyhow!("Missing created"))?
        .parse()?;
    let updated_at = data
        .get("updated_at")
        .ok_or_else(|| anyhow::anyhow!("Missing updated"))?
        .parse()
        .unwrap_or_else(|_| Utc::now());
    let retry_count = data
        .get("retry_count")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    Ok(Some(TransferState {
        transfer_id: transfer_id.to_string(),
        status,
        receiver_id,
        amount,
        tx_hash: data.get("tx_hash").cloned(),
        created_at,
        updated_at,
        completed_at: data.get("completed_at").and_then(|s| s.parse().ok()),
        retry_count,
        error_message: data.get("error_message").cloned(),
    }))
}

pub async fn update_transfer_status<C>(
    conn: &mut C,
    transfer_id: &str,
    status: Status,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}", transfer_id);
    conn.hset::<_, _, _, ()>(&key, "status", status.as_str())
        .await?;
    conn.hset::<_, _, _, ()>(&key, "updated_at", &Utc::now().to_rfc3339())
        .await?;
    if status == Status::Completed {
        conn.hset::<_, _, _, ()>(&key, "completed_at", &Utc::now().to_rfc3339())
            .await?;
    }
    Ok(())
}

pub async fn update_tx_hash<C>(conn: &mut C, transfer_id: &str, tx_hash: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}", transfer_id);
    conn.hset::<_, _, _, ()>(&key, "tx_hash", tx_hash).await?;
    conn.hset::<_, _, _, ()>(&key, "updated_at", &Utc::now().to_rfc3339())
        .await?;
    Ok(())
}

pub async fn increment_retry_count<C>(conn: &mut C, transfer_id: &str) -> Result<u32>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}", transfer_id);
    let new_count: i64 = conn.hincr(&key, "retry_count", 1).await?;
    conn.hset::<_, _, _, ()>(&key, "updated_at", &Utc::now().to_rfc3339())
        .await?;
    Ok(new_count as u32)
}

pub async fn log_event<C>(conn: &mut C, transfer_id: &str, event: Event) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}:ev", transfer_id);
    let serialized = serde_json::to_string(&event)?;
    conn.lpush::<_, _, ()>(&key, serialized).await?;
    conn.expire::<_, ()>(&key, 86400).await?;
    Ok(())
}

pub async fn get_events<C>(conn: &mut C, transfer_id: &str) -> Result<Vec<Event>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}:ev", transfer_id);
    let serialized_events: Vec<String> = conn.lrange(&key, 0, -1).await?;
    let mut events = Vec::new();
    for s in serialized_events {
        if let Ok(event) = serde_json::from_str(&s) {
            events.push(event);
        }
    }
    events.reverse();
    Ok(events)
}

pub async fn is_account_registered<C>(conn: &mut C, account: &str) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let is_member: bool = conn.sismember("registered_accounts", account).await?;
    Ok(is_member)
}

pub async fn mark_account_registered<C>(conn: &mut C, account: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    conn.sadd::<_, _, ()>("registered_accounts", account)
        .await?;
    Ok(())
}

/// Atomically add account to pending registrations set.
/// Returns true if this is the FIRST time (account was added), false if already pending.
pub async fn mark_account_pending_registration<C>(conn: &mut C, account: &str) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    // SADD returns the number of elements added (1 = new, 0 = already exists)
    let added: i32 = conn.sadd("pending_registrations", account).await?;
    Ok(added == 1)
}

/// Atomically add transfer to waiting list for account registration.
/// Returns true if this is the FIRST transfer for this account (should queue registration).
/// Uses Lua script to ensure atomicity - no race conditions!
pub async fn add_transfer_waiting_for_registration<C>(
    conn: &mut C,
    account: &str,
    transfer_id: &str,
) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let script = redis::Script::new(
        r#"
        local account = ARGV[1]
        local transfer_id = ARGV[2]
        
        -- Add to waiting list
        redis.call('LPUSH', 'waiting_transfers:' .. account, transfer_id)
        
        -- Try to add to pending set (returns 1 if first, 0 if already exists)
        local is_first = redis.call('SADD', 'pending_registration_accounts', account)
        
        return is_first
        "#,
    );

    let is_first: i32 = script
        .arg(account)
        .arg(transfer_id)
        .invoke_async(conn)
        .await?;
    Ok(is_first == 1)
}

/// Atomically complete account registration: mark as registered, get waiting transfers, cleanup.
/// Returns list of transfer IDs that were waiting for this account.
/// Uses Lua script to ensure atomicity - zero race window!
pub async fn complete_account_registration<C>(conn: &mut C, account: &str) -> Result<Vec<String>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let script = redis::Script::new(
        r#"
        local account = ARGV[1]
        
        -- Mark as registered
        redis.call('SADD', 'registered_accounts', account)
        redis.call('SREM', 'pending_registration_accounts', account)
        
        -- Get all waiting transfers
        local waiting = redis.call('LRANGE', 'waiting_transfers:' .. account, 0, -1)
        
        -- Delete the waiting list
        redis.call('DEL', 'waiting_transfers:' .. account)
        
        -- Return all waiting transfer IDs
        return waiting
        "#,
    );

    let waiting_transfers: Vec<String> = script.arg(account).invoke_async(conn).await?;
    Ok(waiting_transfers)
}

pub async fn acquire_lock<C>(
    conn: &mut C,
    lock_key: &str,
    value: &str,
    ttl_seconds: u64,
) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let acquired: bool = redis::cmd("SET")
        .arg(lock_key)
        .arg(value)
        .arg("NX")
        .arg("EX")
        .arg(ttl_seconds)
        .query_async(conn)
        .await?;
    Ok(acquired)
}

pub async fn release_lock<C>(conn: &mut C, lock_key: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    conn.del::<_, ()>(lock_key).await?;
    Ok(())
}

// Note: enqueue_registration removed - registration jobs now handled directly in HTTP handler
// with Lua script for atomic deduplication

pub async fn enqueue_transfer<C>(
    conn: &mut C,
    env: &str,
    transfer_id: &str,
    retry_count: u32,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let xadd_start = std::time::Instant::now();
    let stream_key = format!("ftrelay:{}:xfer", env);
    let msg = TransferMessage {
        transfer_id: transfer_id.to_string(),
        retry_count,
    };
    let serialized = serde_json::to_string(&msg)?;
    conn.xadd::<_, _, _, _, ()>(&stream_key, "*", &[("data", serialized.as_str())])
        .await?;
    let xadd_duration = xadd_start.elapsed();
    if xadd_duration.as_millis() > 10 {
        log::debug!(
            "[REDIS_TIMING] XADD to transfer stream took {}ms",
            xadd_duration.as_millis()
        );
    }
    Ok(())
}

/// Efficiently forward a batch of transfers to the ready-for-transfer stream
/// Also updates status and logs events in a single pipelined round trip.
pub async fn forward_transfers_batch<C>(
    conn: &mut C,
    env: &str,
    transfer_ids: &[String],
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    if transfer_ids.is_empty() {
        return Ok(());
    }

    let stream_key = format!("ftrelay:{}:xfer", env);
    let now = Utc::now().to_rfc3339();
    let registered_event = serde_json::to_string(&Event::new("REGISTERED"))?;
    let queued_event = serde_json::to_string(&Event::new("QUEUED_TRANSFER"))?;

    let mut pipe = redis::pipe();
    for transfer_id in transfer_ids {
        let state_key = format!("transfer:{}", transfer_id);
        let ev_key = format!("transfer:{}:ev", transfer_id);

        // Update status + timestamps
        pipe.hset(&state_key, "status", Status::Registered.as_str()).ignore();
        pipe.hset(&state_key, "updated_at", &now).ignore();

        // Log REGISTERED event
        pipe.lpush(&ev_key, &registered_event).ignore();

        // Enqueue to transfer stream
        let msg = TransferMessage {
            transfer_id: transfer_id.clone(),
            retry_count: 0,
        };
        let serialized = serde_json::to_string(&msg)?;
        pipe.cmd("XADD")
            .arg(&stream_key)
            .arg("*")
            .arg("data")
            .arg(&serialized)
            .ignore();

        // Log QUEUED_TRANSFER event
        pipe.lpush(&ev_key, &queued_event).ignore();
        // Keep events key TTL consistent with single push path
        pipe.cmd("EXPIRE").arg(&ev_key).arg(86400).ignore();
    }

    pipe.query_async::<()>(conn).await?;
    Ok(())
}

pub async fn enqueue_verification<C>(
    conn: &mut C,
    env: &str,
    transfer_id: &str,
    tx_hash: &str,
    retry_count: u32,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let stream_key = format!("ftrelay:{}:verify", env);
    let msg = VerificationMessage {
        transfer_id: transfer_id.to_string(),
        tx_hash: tx_hash.to_string(),
        retry_count,
    };
    let serialized = serde_json::to_string(&msg)?;
    conn.xadd::<_, _, _, _, ()>(&stream_key, "*", &[("data", serialized.as_str())])
        .await?;
    Ok(())
}

/// Enqueue a verification job for a tx hash ONCE (deduped via a pending set). Returns true if enqueued.
pub async fn enqueue_tx_verification_once<C>(
    conn: &mut C,
    env: &str,
    tx_hash: &str,
    retry_count: u32,
) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let set_key = format!("ftrelay:{}:pending_verification_txs", env);
    let stream_key = format!("ftrelay:{}:verify", env);
    let msg = VerificationTxMessage { tx_hash: tx_hash.to_string(), retry_count };
    let serialized = serde_json::to_string(&msg)?;

    let script = redis::Script::new(
        r#"
        local set_key = KEYS[1]
        local stream_key = KEYS[2]
        local tx_hash = ARGV[1]
        local payload = ARGV[2]
        local added = redis.call('SADD', set_key, tx_hash)
        if added == 1 then
            redis.call('XADD', stream_key, '*', 'data', payload)
            return 1
        else
            return 0
        end
        "#,
    );

    let enqueued: i32 = script
        .key(&set_key)
        .key(&stream_key)
        .arg(tx_hash)
        .arg(&serialized)
        .invoke_async(conn)
        .await?;
    Ok(enqueued == 1)
}

/// Enqueue a retry verification job for a tx hash (does not dedupe message creation).
pub async fn enqueue_tx_verification_retry<C>(
    conn: &mut C,
    env: &str,
    tx_hash: &str,
    retry_count: u32,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let stream_key = format!("ftrelay:{}:verify", env);
    let msg = VerificationTxMessage { tx_hash: tx_hash.to_string(), retry_count };
    let serialized = serde_json::to_string(&msg)?;
    conn.xadd::<_, _, _, _, ()>(&stream_key, "*", &[("data", serialized.as_str())])
        .await?;
    Ok(())
}

/// Clear a tx from the pending verification set (call on terminal states)
pub async fn clear_tx_pending_verification<C>(conn: &mut C, env: &str, tx_hash: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let set_key = format!("ftrelay:{}:pending_verification_txs", env);
    conn.srem::<_, _, ()>(&set_key, tx_hash).await?;
    Ok(())
}

// Track which transfers are associated with a tx_hash
pub async fn add_transfer_to_tx<C>(conn: &mut C, tx_hash: &str, transfer_id: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("tx:{}:transfers", tx_hash);
    conn.sadd::<_, _, ()>(&key, transfer_id).await?;
    conn.expire::<_, ()>(&key, 86400).await?; // 24 hour TTL
    Ok(())
}

// Get all transfers associated with a tx_hash
pub async fn get_tx_transfers<C>(conn: &mut C, tx_hash: &str) -> Result<Vec<String>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("tx:{}:transfers", tx_hash);
    let members: Vec<String> = conn.smembers(&key).await?;
    Ok(members)
}

// Store tx_hash verification status (to avoid re-checking RPC)
pub async fn set_tx_status<C>(conn: &mut C, tx_hash: &str, status: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("tx:{}:status", tx_hash);
    conn.set::<_, _, ()>(&key, status).await?;
    conn.expire::<_, ()>(&key, 86400).await?; // 24 hour TTL
    Ok(())
}

// Get tx_hash verification status (returns None if not cached)
pub async fn get_tx_status<C>(conn: &mut C, tx_hash: &str) -> Result<Option<String>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("tx:{}:status", tx_hash);
    let status: Option<String> = conn.get(&key).await?;
    Ok(status)
}

/// Pop a batch of messages from a Redis stream using XREADGROUP with linger-based batching
///
/// This function actively accumulates messages up to max_count over a linger_ms period:
/// 1. Keep reading from stream with short BLOCK timeouts (10ms)
/// 2. Accumulate messages until either:
///    - Batch is full (max_count reached)
///    - OR linger_ms total time has elapsed
/// 3. Return whatever we accumulated
///
/// The linger_ms parameter controls the maximum time to wait for batch accumulation.
/// With multiple workers competing, this approach ensures we actually accumulate messages
/// instead of sleeping while other workers grab them.
pub async fn pop_batch<C, T>(
    conn: &mut C,
    stream_key: &str,
    consumer_group: &str,
    consumer_name: &str,
    max_count: usize,
    linger_ms: u64,
) -> Result<Vec<(String, T)>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
    T: DeserializeOwned,
{
    let start = std::time::Instant::now();
    let mut batch: Vec<(String, T)> = Vec::new();
    let mut total_read_time_ms = 0u128;
    let mut read_count = 0;

    // // Check queue depth before starting
    // let queue_depth: u64 = redis::cmd("XLEN")
    //     .arg(stream_key)
    //     .query_async(conn)
    //     .await
    //     .unwrap_or(0);
    // log::debug!("[METRIC] op=queue_depth depth={} stream={}", queue_depth, stream_key);

    // Keep accumulating until batch is full or linger time expires
    // ALWAYS wait the full linger time to give messages a chance to arrive
    while batch.len() < max_count && start.elapsed().as_millis() < linger_ms as u128 {
        let remaining = max_count - batch.len();
        let time_left = linger_ms.saturating_sub(start.elapsed().as_millis() as u64);

        // Use short block timeout (10ms) to keep checking for new messages
        let block_ms = time_left.min(10);

        let read_start = std::time::Instant::now();
        let mut additional = read_stream_batch(
            conn,
            stream_key,
            consumer_group,
            consumer_name,
            remaining,
            block_ms,
        )
        .await?;
        let read_duration = read_start.elapsed();
        total_read_time_ms += read_duration.as_millis();
        read_count += 1;

        // If we got messages, append them
        if !additional.is_empty() {
            batch.append(&mut additional);
        }
        // If no messages right now, keep looping - more might arrive
        // We ALWAYS wait the full linger period to accumulate
    }

    if !batch.is_empty() {
        log::debug!(
            "[REDIS_TIMING] pop_batch: {} msgs in {}ms total ({} reads, {}ms in XREADGROUP)",
            batch.len(),
            start.elapsed().as_millis(),
            read_count,
            total_read_time_ms
        );
    }

    Ok(batch)
}

/// Internal helper to read from a stream and parse messages
async fn read_stream_batch<C, T>(
    conn: &mut C,
    stream_key: &str,
    consumer_group: &str,
    consumer_name: &str,
    max_count: usize,
    block_ms: u64,
) -> Result<Vec<(String, T)>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
    T: DeserializeOwned,
{
    let result: Result<redis::streams::StreamReadReply, _> = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(consumer_group)
        .arg(consumer_name)
        .arg("COUNT")
        .arg(max_count)
        .arg("BLOCK")
        .arg(block_ms as usize)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(conn)
        .await;

    let reply = match result {
        Ok(r) => r,
        Err(_) => return Ok(Vec::new()), // Timeout or no messages
    };

    let mut batch = Vec::new();

    for stream in reply.keys {
        for id_data in stream.ids {
            let stream_id = id_data.id.clone();

            let data = match id_data.map.get("data") {
                Some(redis::Value::BulkString(bytes)) => match std::str::from_utf8(bytes) {
                    Ok(s) => s,
                    Err(e) => {
                        log::warn!("Failed to parse stream data as UTF-8: {:?}", e);
                        // ACK and skip malformed message
                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(stream_key)
                            .arg(consumer_group)
                            .arg(&stream_id)
                            .query_async(conn)
                            .await;
                        continue;
                    }
                },
                _ => {
                    log::warn!("Stream message missing 'data' field");
                    // ACK and skip malformed message
                    let _: Result<(), _> = redis::cmd("XACK")
                        .arg(stream_key)
                        .arg(consumer_group)
                        .arg(&stream_id)
                        .query_async(conn)
                        .await;
                    continue;
                }
            };

            let msg: T = match serde_json::from_str(data) {
                Ok(m) => m,
                Err(e) => {
                    log::warn!("Failed to deserialize message: {:?}", e);
                    // ACK and skip malformed message
                    let _: Result<(), _> = redis::cmd("XACK")
                        .arg(stream_key)
                        .arg(consumer_group)
                        .arg(&stream_id)
                        .query_async(conn)
                        .await;
                    continue;
                }
            };

            batch.push((stream_id, msg));
        }
    }

    Ok(batch)
}

/// Acknowledge a message in a Redis stream
pub async fn ack_message<C>(
    conn: &mut C,
    stream_key: &str,
    consumer_group: &str,
    stream_id: &str,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let _: u64 = redis::cmd("XACK")
        .arg(stream_key)
        .arg(consumer_group)
        .arg(stream_id)
        .query_async(conn)
        .await?;
    Ok(())
}
