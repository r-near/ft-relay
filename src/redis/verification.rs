use ::redis::aio::ConnectionLike;
use ::redis::AsyncCommands;
use anyhow::Result;

use crate::types::{VerificationMessage, VerificationTxMessage};

use super::keys;

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
    let stream_key = keys::verification_stream(env);
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

pub async fn enqueue_tx_verification_once<C>(
    conn: &mut C,
    env: &str,
    tx_hash: &str,
    retry_count: u32,
) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let set_key = keys::pending_verification_txs(env);
    let stream_key = keys::verification_stream(env);
    let msg = VerificationTxMessage {
        tx_hash: tx_hash.to_string(),
        retry_count,
    };
    let serialized = serde_json::to_string(&msg)?;

    let script = ::redis::Script::new(
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

pub async fn enqueue_tx_verification_retry<C>(
    conn: &mut C,
    env: &str,
    tx_hash: &str,
    retry_count: u32,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let stream_key = keys::verification_stream(env);
    let msg = VerificationTxMessage {
        tx_hash: tx_hash.to_string(),
        retry_count,
    };
    let serialized = serde_json::to_string(&msg)?;
    conn.xadd::<_, _, _, _, ()>(&stream_key, "*", &[("data", serialized.as_str())])
        .await?;
    Ok(())
}

pub async fn clear_tx_pending_verification<C>(conn: &mut C, env: &str, tx_hash: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let set_key = keys::pending_verification_txs(env);
    conn.srem::<_, _, ()>(&set_key, tx_hash).await?;
    Ok(())
}

pub async fn set_tx_status<C>(conn: &mut C, tx_hash: &str, status: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = keys::tx_status(tx_hash);
    conn.set::<_, _, ()>(&key, status).await?;
    conn.expire::<_, ()>(&key, 86400).await?;
    Ok(())
}

pub async fn get_tx_status<C>(conn: &mut C, tx_hash: &str) -> Result<Option<String>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let key = keys::tx_status(tx_hash);
    conn.get(&key).await.map_err(Into::into)
}
