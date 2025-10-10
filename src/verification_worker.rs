use anyhow::Result;
use log::{info, warn};
use near_primitives::hash::CryptoHash;
use redis::aio::ConnectionManager;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::redis_helpers as rh;
use crate::rpc_client::{NearRpcClient, TxStatus};
use crate::types::{AccountId, Event, Status, VerificationMessage};

const MAX_VERIFICATION_RETRIES: u32 = 20;

pub struct VerificationWorkerRuntime {
    pub redis_conn: ConnectionManager,
    pub rpc_client: Arc<NearRpcClient>,
    pub relay_account: AccountId,
    pub env: String,
}

pub struct VerificationWorkerContext {
    pub runtime: Arc<VerificationWorkerRuntime>,
    pub linger_ms: u64,
}

pub async fn verification_worker_loop(ctx: VerificationWorkerContext) -> Result<()> {
    let consumer_name = format!("verification-{}", Uuid::new_v4());
    let stream_key = format!("ftrelay:{}:verify", ctx.runtime.env);
    let consumer_group = format!("ftrelay:{}:verify_workers", ctx.runtime.env);

    let mut conn = ctx.runtime.redis_conn.clone();
    let _: Result<String, _> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream_key)
        .arg(&consumer_group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await;

    info!("Verification worker {} started", consumer_name);

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

                let msg: VerificationMessage = match serde_json::from_str(data) {
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

                match verify_transaction(&ctx, &msg).await {
                    Ok(VerificationResult::Completed) => {
                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(&stream_key)
                            .arg(&consumer_group)
                            .arg(&stream_id)
                            .query_async(&mut conn)
                            .await;
                    }
                    Ok(VerificationResult::Failed(reason)) => {
                        warn!("Transfer {} failed: {}", msg.transfer_id, reason);
                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(&stream_key)
                            .arg(&consumer_group)
                            .arg(&stream_id)
                            .query_async(&mut conn)
                            .await;
                    }
                    Ok(VerificationResult::Pending) => {
                        let retry_count = msg.retry_count + 1;
                        
                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(&stream_key)
                            .arg(&consumer_group)
                            .arg(&stream_id)
                            .query_async(&mut conn)
                            .await;

                        if retry_count < MAX_VERIFICATION_RETRIES {
                            let _ = rh::increment_retry_count(&mut conn, &msg.transfer_id).await;
                            let _ = rh::enqueue_verification(
                                &mut conn,
                                &ctx.runtime.env,
                                &msg.transfer_id,
                                &msg.tx_hash,
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
                                Event::new("FAILED")
                                    .with_reason(format!(
                                        "Verification timeout after {} checks",
                                        retry_count
                                    )),
                            )
                            .await;
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Error checking tx status for {}: {:?}",
                            msg.transfer_id, e
                        );
                        
                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(&stream_key)
                            .arg(&consumer_group)
                            .arg(&stream_id)
                            .query_async(&mut conn)
                            .await;

                        let retry_count = msg.retry_count + 1;
                        if retry_count < MAX_VERIFICATION_RETRIES {
                            let _ = rh::enqueue_verification(
                                &mut conn,
                                &ctx.runtime.env,
                                &msg.transfer_id,
                                &msg.tx_hash,
                                retry_count,
                            )
                            .await;
                        }
                    }
                }
            }
        }
    }
}

enum VerificationResult {
    Completed,
    Failed(String),
    Pending,
}

async fn verify_transaction(
    ctx: &VerificationWorkerContext,
    msg: &VerificationMessage,
) -> Result<VerificationResult> {
    tokio::time::sleep(Duration::from_secs(6)).await;

    let tx_hash = CryptoHash::from_str(&msg.tx_hash).map_err(|e| anyhow::anyhow!("Invalid tx hash: {:?}", e))?;

    match ctx
        .runtime
        .rpc_client
        .check_tx_status(&tx_hash, &ctx.runtime.relay_account)
        .await?
    {
        TxStatus::Success(_outcome) => {
            info!("Transfer {} completed successfully", msg.transfer_id);
            
            let mut conn = ctx.runtime.redis_conn.clone();
            rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Completed).await?;
            rh::log_event(&mut conn, &msg.transfer_id, Event::new("COMPLETED")).await?;
            
            Ok(VerificationResult::Completed)
        }
        TxStatus::Failed(reason) => {
            warn!("Transfer {} failed on-chain: {}", msg.transfer_id, reason);
            
            let mut conn = ctx.runtime.redis_conn.clone();
            rh::update_transfer_status(&mut conn, &msg.transfer_id, Status::Failed).await?;
            rh::log_event(
                &mut conn,
                &msg.transfer_id,
                Event::new("FAILED").with_reason(reason.clone()),
            )
            .await?;
            
            Ok(VerificationResult::Failed(reason))
        }
        TxStatus::Pending => {
            Ok(VerificationResult::Pending)
        }
    }
}
