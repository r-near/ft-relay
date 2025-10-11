use ::redis::aio::ConnectionLike;
use ::redis::AsyncCommands;
use anyhow::Result;

use crate::types::Event;

use super::keys;

pub async fn log_event<C>(conn: &mut C, transfer_id: &str, event: Event) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = keys::transfer_events(transfer_id);
    let serialized = serde_json::to_string(&event)?;
    conn.lpush::<_, _, ()>(&key, serialized).await?;
    conn.expire::<_, ()>(&key, 86400).await?;
    Ok(())
}

pub async fn get_events<C>(conn: &mut C, transfer_id: &str) -> Result<Vec<Event>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = keys::transfer_events(transfer_id);
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
