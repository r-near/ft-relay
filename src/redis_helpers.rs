use anyhow::Result;
use chrono::Utc;
use redis::aio::ConnectionLike;
use redis::AsyncCommands;
use serde::de::DeserializeOwned;

use crate::types::{Event, RegistrationMessage, Status, TransferMessage, TransferState, VerificationMessage};

pub async fn store_transfer_state<C>(conn: &mut C, state: &TransferState) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}", state.transfer_id);
    conn.hset_multiple::<_, _, _, ()>(&key, &[
        ("status", state.status.as_str()),
        ("receiver_id", state.receiver_id.as_str()),
        ("amount", state.amount.as_str()),
        ("created_at", &state.created_at.to_rfc3339()),
        ("updated_at", &state.updated_at.to_rfc3339()),
        ("retry_count", &state.retry_count.to_string()),
    ]).await?;
    if let Some(ref tx_hash) = state.tx_hash {
        conn.hset::<_, _, _, ()>(&key, "tx_hash", tx_hash).await?;
    }
    if let Some(ref completed_at) = state.completed_at {
        conn.hset::<_, _, _, ()>(&key, "completed_at", &completed_at.to_rfc3339()).await?;
    }
    if let Some(ref error) = state.error_message {
        conn.hset::<_, _, _, ()>(&key, "error_message", error).await?;
    }
    conn.expire::<_, ()>(&key, 86400).await?;
    Ok(())
}

pub async fn get_transfer_state<C>(conn: &mut C, transfer_id: &str) -> Result<Option<TransferState>>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}", transfer_id);
    let exists: bool = conn.exists(&key).await?;
    if !exists { return Ok(None); }

    let data: std::collections::HashMap<String, String> = conn.hgetall(&key).await?;
    let status_str = data.get("status").ok_or_else(|| anyhow::anyhow!("Missing status"))?;
    let status: Status = serde_json::from_str(&format!("\"{}\"", status_str))?;
    let receiver_id = data.get("receiver_id").ok_or_else(|| anyhow::anyhow!("Missing receiver"))?.clone();
    let amount = data.get("amount").ok_or_else(|| anyhow::anyhow!("Missing amount"))?.clone();
    let created_at = data.get("created_at").ok_or_else(|| anyhow::anyhow!("Missing created"))?.parse()?;
    let updated_at = data.get("updated_at").ok_or_else(|| anyhow::anyhow!("Missing updated"))?.parse().unwrap_or_else(|_| Utc::now());
    let retry_count = data.get("retry_count").and_then(|s| s.parse().ok()).unwrap_or(0);

    Ok(Some(TransferState {
        transfer_id: transfer_id.to_string(), status, receiver_id, amount,
        tx_hash: data.get("tx_hash").cloned(), created_at, updated_at,
        completed_at: data.get("completed_at").and_then(|s| s.parse().ok()),
        retry_count, error_message: data.get("error_message").cloned(),
    }))
}

pub async fn update_transfer_status<C>(conn: &mut C, transfer_id: &str, status: Status) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}", transfer_id);
    conn.hset::<_, _, _, ()>(&key, "status", status.as_str()).await?;
    conn.hset::<_, _, _, ()>(&key, "updated_at", &Utc::now().to_rfc3339()).await?;
    if status == Status::Completed {
        conn.hset::<_, _, _, ()>(&key, "completed_at", &Utc::now().to_rfc3339()).await?;
    }
    Ok(())
}

pub async fn update_tx_hash<C>(conn: &mut C, transfer_id: &str, tx_hash: &str) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}", transfer_id);
    conn.hset::<_, _, _, ()>(&key, "tx_hash", tx_hash).await?;
    conn.hset::<_, _, _, ()>(&key, "updated_at", &Utc::now().to_rfc3339()).await?;
    Ok(())
}

pub async fn increment_retry_count<C>(conn: &mut C, transfer_id: &str) -> Result<u32>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}", transfer_id);
    let new_count: i64 = conn.hincr(&key, "retry_count", 1).await?;
    conn.hset::<_, _, _, ()>(&key, "updated_at", &Utc::now().to_rfc3339()).await?;
    Ok(new_count as u32)
}

pub async fn log_event<C>(conn: &mut C, transfer_id: &str, event: Event) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}:ev", transfer_id);
    let serialized = serde_json::to_string(&event)?;
    conn.lpush::<_, _, ()>(&key, serialized).await?;
    conn.expire::<_, ()>(&key, 86400).await?;
    Ok(())
}

pub async fn get_events<C>(conn: &mut C, transfer_id: &str) -> Result<Vec<Event>>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}:ev", transfer_id);
    let serialized_events: Vec<String> = conn.lrange(&key, 0, -1).await?;
    let mut events = Vec::new();
    for s in serialized_events {
        if let Ok(event) = serde_json::from_str(&s) { events.push(event); }
    }
    events.reverse();
    Ok(events)
}

pub async fn is_account_registered<C>(conn: &mut C, account: &str) -> Result<bool>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let is_member: bool = conn.sismember("registered_accounts", account).await?;
    Ok(is_member)
}

pub async fn mark_account_registered<C>(conn: &mut C, account: &str) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    conn.sadd::<_, _, ()>("registered_accounts", account).await?;
    Ok(())
}

pub async fn acquire_lock<C>(conn: &mut C, lock_key: &str, value: &str, ttl_seconds: u64) -> Result<bool>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let acquired: bool = redis::cmd("SET")
        .arg(lock_key).arg(value).arg("NX").arg("EX").arg(ttl_seconds)
        .query_async(conn).await?;
    Ok(acquired)
}

pub async fn release_lock<C>(conn: &mut C, lock_key: &str) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    conn.del::<_, ()>(lock_key).await?;
    Ok(())
}

pub async fn enqueue_registration<C>(conn: &mut C, env: &str, transfer_id: &str, retry_count: u32) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let stream_key = format!("ftrelay:{}:reg", env);
    let msg = RegistrationMessage { transfer_id: transfer_id.to_string(), retry_count };
    let serialized = serde_json::to_string(&msg)?;
    conn.xadd::<_, _, _, _, ()>(&stream_key, "*", &[("data", serialized.as_str())]).await?;
    Ok(())
}

pub async fn enqueue_transfer<C>(conn: &mut C, env: &str, transfer_id: &str, retry_count: u32) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let stream_key = format!("ftrelay:{}:xfer", env);
    let msg = TransferMessage { transfer_id: transfer_id.to_string(), retry_count };
    let serialized = serde_json::to_string(&msg)?;
    conn.xadd::<_, _, _, _, ()>(&stream_key, "*", &[("data", serialized.as_str())]).await?;
    Ok(())
}

pub async fn enqueue_verification<C>(conn: &mut C, env: &str, transfer_id: &str, tx_hash: &str, retry_count: u32) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let stream_key = format!("ftrelay:{}:verify", env);
    let msg = VerificationMessage { transfer_id: transfer_id.to_string(), tx_hash: tx_hash.to_string(), retry_count };
    let serialized = serde_json::to_string(&msg)?;
    conn.xadd::<_, _, _, _, ()>(&stream_key, "*", &[("data", serialized.as_str())]).await?;
    Ok(())
}

// Track which transfers are associated with a tx_hash
pub async fn add_transfer_to_tx<C>(conn: &mut C, tx_hash: &str, transfer_id: &str) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("tx:{}:transfers", tx_hash);
    conn.sadd::<_, _, ()>(&key, transfer_id).await?;
    conn.expire::<_, ()>(&key, 86400).await?;  // 24 hour TTL
    Ok(())
}

// Get all transfers associated with a tx_hash
pub async fn get_tx_transfers<C>(conn: &mut C, tx_hash: &str) -> Result<Vec<String>>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("tx:{}:transfers", tx_hash);
    let members: Vec<String> = conn.smembers(&key).await?;
    Ok(members)
}

// Store tx_hash verification status (to avoid re-checking RPC)
pub async fn set_tx_status<C>(conn: &mut C, tx_hash: &str, status: &str) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("tx:{}:status", tx_hash);
    conn.set::<_, _, ()>(&key, status).await?;
    conn.expire::<_, ()>(&key, 86400).await?;  // 24 hour TTL
    Ok(())
}

// Get tx_hash verification status (returns None if not cached)
pub async fn get_tx_status<C>(conn: &mut C, tx_hash: &str) -> Result<Option<String>>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("tx:{}:status", tx_hash);
    let status: Option<String> = conn.get(&key).await?;
    Ok(status)
}

/// Pop a batch of messages from a Redis stream using XREADGROUP with linger-based batching
/// 
/// This helper function implements efficient batch accumulation:
/// 1. First XREADGROUP with BLOCK=1ms (quick check for available messages)
/// 2. If batch is not full, sleep for linger_ms to allow more messages to arrive
/// 3. Second XREADGROUP to pick up any additional messages
/// 4. Return the combined batch
/// 
/// The linger_ms parameter controls how long to wait for additional messages
/// when the initial batch is not full, allowing workers to accumulate larger
/// batches for better throughput.
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
    // First attempt: quick check with minimal blocking
    let mut batch = read_stream_batch(conn, stream_key, consumer_group, consumer_name, max_count, 1).await?;

    // If we got some messages but batch isn't full, linger to accumulate more
    if !batch.is_empty() && batch.len() < max_count {
        tokio::time::sleep(tokio::time::Duration::from_millis(linger_ms)).await;
        
        // Second attempt: pick up any messages that arrived during linger
        let remaining = max_count - batch.len();
        let mut additional = read_stream_batch(conn, stream_key, consumer_group, consumer_name, remaining, 1).await?;
        batch.append(&mut additional);
    } else if batch.is_empty() {
        // If no messages at all, block longer to avoid busy-waiting
        batch = read_stream_batch(conn, stream_key, consumer_group, consumer_name, max_count, linger_ms).await?;
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
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let _: u64 = redis::cmd("XACK")
        .arg(stream_key)
        .arg(consumer_group)
        .arg(stream_id)
        .query_async(conn)
        .await?;
    Ok(())
}
