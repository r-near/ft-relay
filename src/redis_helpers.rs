use anyhow::Result;
use chrono::Utc;
use redis::aio::ConnectionLike;
use redis::AsyncCommands;

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
    conn.xadd(&stream_key, "*", &[("data", serialized.as_str())]).await?;
    Ok(())
}

pub async fn enqueue_transfer<C>(conn: &mut C, env: &str, transfer_id: &str, retry_count: u32) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let stream_key = format!("ftrelay:{}:xfer", env);
    let msg = TransferMessage { transfer_id: transfer_id.to_string(), retry_count };
    let serialized = serde_json::to_string(&msg)?;
    conn.xadd(&stream_key, "*", &[("data", serialized.as_str())]).await?;
    Ok(())
}

pub async fn enqueue_verification<C>(conn: &mut C, env: &str, transfer_id: &str, tx_hash: &str, retry_count: u32) -> Result<()>
where C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let stream_key = format!("ftrelay:{}:verify", env);
    let msg = VerificationMessage { transfer_id: transfer_id.to_string(), tx_hash: tx_hash.to_string(), retry_count };
    let serialized = serde_json::to_string(&msg)?;
    conn.xadd(&stream_key, "*", &[("data", serialized.as_str())]).await?;
    Ok(())
}
