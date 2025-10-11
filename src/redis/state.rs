use ::redis::aio::ConnectionLike;
use ::redis::AsyncCommands;
use anyhow::Result;
use chrono::Utc;

use crate::types::{Status, TransferState};

use super::keys;

pub async fn store_transfer_state<C>(conn: &mut C, state: &TransferState) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = keys::transfer_state(&state.transfer_id);
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

    let mut pipe = ::redis::pipe();
    for transfer_id in transfer_ids {
        let key = keys::transfer_state(transfer_id);
        pipe.hgetall(&key);
    }

    let results: Vec<std::collections::HashMap<String, String>> = pipe.query_async(conn).await?;

    let pipeline_duration = pipeline_start.elapsed();
    log::debug!(
        "[REDIS_TIMING] Pipelined HGETALL for {} transfer states took {}ms ({:.1}ms per transfer)",
        transfer_ids.len(),
        pipeline_duration.as_millis(),
        pipeline_duration.as_millis() as f64 / transfer_ids.len() as f64
    );

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
            .unwrap_or_else(Utc::now);
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
    let key = keys::transfer_state(transfer_id);
    let data: std::collections::HashMap<String, String> = conn.hgetall(&key).await?;
    let hgetall_duration = hgetall_start.elapsed();
    if hgetall_duration.as_millis() > 5 {
        log::debug!(
            "[REDIS_TIMING] HGETALL for transfer state took {}ms",
            hgetall_duration.as_millis()
        );
    }

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
    let key = keys::transfer_state(transfer_id);
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
    let key = keys::transfer_state(transfer_id);
    conn.hset::<_, _, _, ()>(&key, "tx_hash", tx_hash).await?;
    conn.hset::<_, _, _, ()>(&key, "updated_at", &Utc::now().to_rfc3339())
        .await?;
    Ok(())
}

pub async fn increment_retry_count<C>(conn: &mut C, transfer_id: &str) -> Result<u32>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = keys::transfer_state(transfer_id);
    let new_count: i64 = conn.hincr(&key, "retry_count", 1).await?;
    conn.hset::<_, _, _, ()>(&key, "updated_at", &Utc::now().to_rfc3339())
        .await?;
    Ok(new_count as u32)
}
