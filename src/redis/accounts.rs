use ::redis::aio::ConnectionLike;
use ::redis::AsyncCommands;
use anyhow::Result;

use super::keys;

pub async fn is_account_registered<C>(conn: &mut C, account: &str) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    conn.sismember(keys::registered_accounts(), account)
        .await
        .map_err(Into::into)
}

pub async fn mark_account_registered<C>(conn: &mut C, account: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    conn.sadd::<_, _, ()>(keys::registered_accounts(), account)
        .await
        .map_err(Into::into)
}

pub async fn mark_account_pending_registration<C>(conn: &mut C, account: &str) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let added: i32 = conn.sadd(keys::pending_registrations(), account).await?;
    Ok(added == 1)
}

pub async fn add_transfer_waiting_for_registration<C>(
    conn: &mut C,
    account: &str,
    transfer_id: &str,
) -> Result<bool>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let script = ::redis::Script::new(
        r#"
        local account = ARGV[1]
        local transfer_id = ARGV[2]

        redis.call('LPUSH', 'waiting_transfers:' .. account, transfer_id)

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

pub async fn complete_account_registration<C>(conn: &mut C, account: &str) -> Result<Vec<String>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let script = ::redis::Script::new(
        r#"
        local account = ARGV[1]

        redis.call('SADD', 'registered_accounts', account)
        redis.call('SREM', 'pending_registration_accounts', account)

        local waiting = redis.call('LRANGE', 'waiting_transfers:' .. account, 0, -1)

        redis.call('DEL', 'waiting_transfers:' .. account)

        return waiting
        "#,
    );

    let waiting_transfers: Vec<String> = script.arg(account).invoke_async(conn).await?;
    Ok(waiting_transfers)
}
