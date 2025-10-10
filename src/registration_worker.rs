use anyhow::Result;
use log::{info, warn};
use redis::aio::ConnectionManager;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::access_key_pool::AccessKeyPool;
use crate::nonce_manager::NonceManager;
use crate::redis_helpers as rh;
use crate::rpc_client::NearRpcClient;
use crate::types::{AccountId, Event, RegistrationMessage, Status};

const MAX_RETRIES: u32 = 10;

pub struct RegistrationWorkerRuntime {
    pub redis_conn: ConnectionManager,
    pub rpc_client: Arc<NearRpcClient>,
    pub access_key_pool: Arc<AccessKeyPool>,
    pub nonce_manager: NonceManager,
    pub relay_account: AccountId,
    pub token: AccountId,
    pub env: String,
}

pub struct RegistrationWorkerContext {
    pub runtime: Arc<RegistrationWorkerRuntime>,
    pub linger_ms: u64,
}

pub async fn registration_worker_loop(ctx: RegistrationWorkerContext) -> Result<()> {
    let consumer_name = format!("registration-{}", Uuid::new_v4());
    let stream_key = format!("ftrelay:{}:reg", ctx.runtime.env);
    let consumer_group = format!("ftrelay:{}:reg_workers", ctx.runtime.env);

    let mut conn = ctx.runtime.redis_conn.clone();
    let _: Result<String, _> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream_key)
        .arg(&consumer_group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await;

    info!("Registration worker {} started", consumer_name);

    loop {
        let mut conn = ctx.runtime.redis_conn.clone();

        let result: Result<redis::streams::StreamReadReply, _> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&consumer_group)
            .arg(&consumer_name)
            .arg("COUNT")
            .arg(50)
            .arg("BLOCK")
            .arg(ctx.linger_ms as usize)
            .arg("STREAMS")
            .arg(&stream_key)
            .arg(">")
            .query_async(&mut conn)
            .await;

        let reply = match result {
            Ok(r) => r,
            Err(_) => continue,
        };

        for stream in reply.keys {
            for id_data in stream.ids {
                let stream_id = id_data.id.clone();

                let data = match id_data.map.get("data") {
                    Some(redis::Value::BulkString(bytes)) => {
                        match std::str::from_utf8(bytes) {
                            Ok(s) => s,
                            Err(_) => continue,
                        }
                    }
                    _ => continue,
                };

                let msg: RegistrationMessage = match serde_json::from_str(data) {
                    Ok(m) => m,
                    Err(_) => {
                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(&stream_key)
                            .arg(&consumer_group)
                            .arg(&stream_id)
                            .query_async(&mut conn)
                            .await;
                        continue;
                    }
                };

                match process_registration(&ctx, &msg).await {
                    Ok(_) => {
                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(&stream_key)
                            .arg(&consumer_group)
                            .arg(&stream_id)
                            .query_async(&mut conn)
                            .await;
                    }
                    Err(e) => {
                        warn!("Error processing registration for {}: {:?}", msg.transfer_id, e);
                        
                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(&stream_key)
                            .arg(&consumer_group)
                            .arg(&stream_id)
                            .query_async(&mut conn)
                            .await;

                        let retry_count = msg.retry_count + 1;
                        if retry_count < MAX_RETRIES {
                            let _ = rh::increment_retry_count(&mut conn, &msg.transfer_id).await;
                            let _ = rh::enqueue_registration(
                                &mut conn,
                                &ctx.runtime.env,
                                &msg.transfer_id,
                                retry_count,
                            )
                            .await;
                        } else {
                            let _ = rh::update_transfer_status(
                                &mut conn,
                                &msg.transfer_id,
                                Status::Failed,
                            )
                            .await;
                            let _ = rh::log_event(
                                &mut conn,
                                &msg.transfer_id,
                                Event::new("FAILED").with_reason(format!(
                                    "Registration failed after {} retries",
                                    retry_count
                                )),
                            )
                            .await;
                        }
                    }
                }
            }
        }
    }
}

async fn process_registration(
    ctx: &RegistrationWorkerContext,
    msg: &RegistrationMessage,
) -> Result<()> {
    let mut conn = ctx.runtime.redis_conn.clone();

    let transfer = match rh::get_transfer_state(&mut conn, &msg.transfer_id).await? {
        Some(t) => t,
        None => {
            warn!("Transfer {} not found", msg.transfer_id);
            return Ok(());
        }
    };

    let account = &transfer.receiver_id;

    if rh::is_account_registered(&mut conn, account).await? {
        info!("Account {} already registered (fast path)", account);
        rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Registered).await?;
        rh::log_event(&mut conn, &msg.transfer_id, Event::new("REGISTERED")).await?;
        rh::enqueue_transfer(&mut conn, &ctx.runtime.env, &msg.transfer_id, 0).await?;
        rh::log_event(&mut conn, &msg.transfer_id, Event::new("QUEUED_TRANSFER")).await?;
        return Ok(());
    }

    let lock_key = format!("register_lock:{}", account);
    let lock_value = Uuid::new_v4().to_string();
    let acquired = rh::acquire_lock(&mut conn, &lock_key, &lock_value, 30).await?;

    if !acquired {
        tokio::time::sleep(Duration::from_millis(500)).await;
        return Err(anyhow::anyhow!("Lock not acquired, will retry"));
    }

    if rh::is_account_registered(&mut conn, account).await? {
        info!("Account {} registered by another worker", account);
        rh::release_lock(&mut conn, &lock_key).await?;
        rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Registered).await?;
        rh::log_event(&mut conn, &msg.transfer_id, Event::new("REGISTERED")).await?;
        rh::enqueue_transfer(&mut conn, &ctx.runtime.env, &msg.transfer_id, 0).await?;
        rh::log_event(&mut conn, &msg.transfer_id, Event::new("QUEUED_TRANSFER")).await?;
        return Ok(());
    }

    info!("Registering account {} on-chain", account);

    let leased_key = ctx.runtime.access_key_pool.lease().await?;

    let nonce = ctx
        .runtime
        .nonce_manager
        .clone()
        .get_next_nonce(&leased_key.key_id)
        .await?;

    let result = ctx
        .runtime
        .rpc_client
        .register_account(
            &ctx.runtime.relay_account,
            &ctx.runtime.token,
            account,
            &leased_key.secret_key,
            nonce,
        )
        .await;

    drop(leased_key);

    match result {
        Ok(tx_hash) => {
            info!("Registered {} with tx {}", account, tx_hash);
            rh::mark_account_registered(&mut conn, account).await?;
            rh::release_lock(&mut conn, &lock_key).await?;
            rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Registered).await?;
            rh::log_event(&mut conn, &msg.transfer_id, Event::new("REGISTERED")).await?;
            rh::enqueue_transfer(&mut conn, &ctx.runtime.env, &msg.transfer_id, 0).await?;
            rh::log_event(&mut conn, &msg.transfer_id, Event::new("QUEUED_TRANSFER")).await?;
            Ok(())
        }
        Err(e) => {
            let err_str = format!("{:?}", e);
            
            if err_str.contains("already") || err_str.contains("exist") {
                info!("Account {} already registered (on-chain)", account);
                rh::mark_account_registered(&mut conn, account).await?;
                rh::release_lock(&mut conn, &lock_key).await?;
                rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Registered)
                    .await?;
                rh::log_event(&mut conn, &msg.transfer_id, Event::new("REGISTERED")).await?;
                rh::enqueue_transfer(&mut conn, &ctx.runtime.env, &msg.transfer_id, 0).await?;
                rh::log_event(&mut conn, &msg.transfer_id, Event::new("QUEUED_TRANSFER")).await?;
                Ok(())
            } else {
                warn!("Failed to register {}: {:?}", account, e);
                rh::release_lock(&mut conn, &lock_key).await?;
                Err(e)
            }
        }
    }
}
