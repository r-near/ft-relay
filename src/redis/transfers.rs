use ::redis::aio::ConnectionLike;
use ::redis::AsyncCommands;
use anyhow::Result;
use chrono::Utc;

use crate::types::{Event, Status, TransferMessage};

use super::events;
use super::keys;

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
    let stream_key = keys::transfer_stream(env);
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

    let stream_key = keys::transfer_stream(env);
    let now = Utc::now().to_rfc3339();
    let registered_event = serde_json::to_string(&Event::new("REGISTERED"))?;
    let queued_event = serde_json::to_string(&Event::new("QUEUED_TRANSFER"))?;

    let mut pipe = ::redis::pipe();
    for transfer_id in transfer_ids {
        let state_key = keys::transfer_state(transfer_id);
        let ev_key = keys::transfer_events(transfer_id);

        pipe.hset(&state_key, "status", Status::Registered.as_str())
            .ignore();
        pipe.hset(&state_key, "updated_at", &now).ignore();

        pipe.lpush(&ev_key, &registered_event).ignore();

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

        pipe.lpush(&ev_key, &queued_event).ignore();
        pipe.cmd("EXPIRE").arg(&ev_key).arg(86400).ignore();
    }

    pipe.query_async::<()>(conn).await?;
    Ok(())
}

pub async fn add_transfer_to_tx<C>(conn: &mut C, tx_hash: &str, transfer_id: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = keys::tx_transfers(tx_hash);
    conn.sadd::<_, _, ()>(&key, transfer_id).await?;
    conn.expire::<_, ()>(&key, 86400).await?;
    Ok(())
}

pub async fn get_tx_transfers<C>(conn: &mut C, tx_hash: &str) -> Result<Vec<String>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = keys::tx_transfers(tx_hash);
    conn.smembers(&key).await.map_err(Into::into)
}

pub async fn log_and_enqueue_transfer<C>(conn: &mut C, env: &str, transfer_id: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    events::log_event(conn, transfer_id, Event::new("QUEUED_TRANSFER")).await?;
    enqueue_transfer(conn, env, transfer_id, 0).await
}
