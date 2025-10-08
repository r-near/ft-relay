use anyhow::Result;
use redis::aio::ConnectionLike;
use redis::AsyncCommands;

use crate::transfer_states::{PendingRegistration, ReadyToSend, Transfer};

/// Check if an account is registered for a token
pub async fn is_registered<C>(
    conn: &mut C,
    token: &str,
    account_id: &str,
) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("registered:{}", token);
    let is_member: bool = conn.sismember(&key, account_id).await?;
    Ok(is_member)
}

/// Push a transfer to the pending list for an account
pub async fn push_to_pending_list<C>(
    conn: &mut C,
    token: &str,
    transfer: &Transfer<PendingRegistration>,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let account_id = &transfer.data().receiver_id;
    let transfer_id = &transfer.data().transfer_id;
    let pending_key = format!("pending:{}:{}", token, account_id);
    let status_key = format!("transfer:{}:{}", token, transfer_id);

    let serialized = transfer.serialize()?;

    // Use pipeline for atomicity
    let mut pipe = redis::pipe();
    pipe.atomic();
    pipe.rpush(&pending_key, &serialized);
    pipe.set_ex(&status_key, "pending_registration", 3600);
    pipe.query_async::<()>(conn).await?;

    Ok(())
}

/// Pop all pending transfers for an account (destructive read)
pub async fn pop_pending_transfers<C>(
    conn: &mut C,
    token: &str,
    account_id: &str,
) -> Result<Vec<Transfer<PendingRegistration>>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let pending_key = format!("pending:{}:{}", token, account_id);

    // Get all pending transfers and delete the list
    let serialized_list: Vec<String> = redis::cmd("LRANGE")
        .arg(&pending_key)
        .arg(0)
        .arg(-1)
        .query_async(conn)
        .await?;

    conn.del::<_, ()>(&pending_key).await?;

    // Deserialize all transfers
    let mut transfers = Vec::new();
    for s in serialized_list {
        if let Ok(transfer) = Transfer::<PendingRegistration>::deserialize(&s) {
            transfers.push(transfer);
        }
    }

    Ok(transfers)
}

/// Set transfer status for tracking
pub async fn set_transfer_status<C>(
    conn: &mut C,
    token: &str,
    transfer_id: &str,
    status: &str,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = format!("transfer:{}:{}", token, transfer_id);
    conn.set_ex::<_, _, ()>(&key, status, 3600).await?;
    Ok(())
}

/// Batch set transfer completion status for multiple transfers
pub async fn set_transfers_completed<C>(
    conn: &mut C,
    token: &str,
    transfer_ids: &[String],
    tx_hash: &str,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    if transfer_ids.is_empty() {
        return Ok(());
    }

    let mut pipe = redis::pipe();
    pipe.atomic();
    for transfer_id in transfer_ids {
        let key = format!("status:{}:{}", token, transfer_id);
        pipe.set_ex(&key, tx_hash, 86400);
    }
    pipe.query_async::<()>(conn).await?;

    Ok(())
}

/// Atomically mark account as registered and push transfers to ready stream
pub async fn mark_registered_and_push_to_stream<C>(
    conn: &mut C,
    token: &str,
    ready_stream_key: &str,
    account_id: &str,
    transfers: Vec<Transfer<ReadyToSend>>,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let registered_key = format!("registered:{}", token);

    let mut pipe = redis::pipe();
    pipe.atomic();
    pipe.sadd(&registered_key, account_id);

    for transfer in &transfers {
        let serialized = transfer.serialize()?;
        let transfer_id = &transfer.data().transfer_id;
        let status_key = format!("transfer:{}:{}", token, transfer_id);

        pipe.xadd(ready_stream_key, "*", &[("data", serialized.as_str())]);
        pipe.set_ex(&status_key, "ready", 3600);
    }

    pipe.query_async::<()>(conn).await?;

    Ok(())
}
