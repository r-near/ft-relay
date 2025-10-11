use ::redis::aio::ConnectionLike;
use ::redis::AsyncCommands;
use anyhow::Result;

pub async fn acquire_lock<C>(
    conn: &mut C,
    lock_key: &str,
    value: &str,
    ttl_seconds: u64,
) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let acquired: bool = ::redis::cmd("SET")
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
