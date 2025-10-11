mod access_key_pool;
mod config;
pub mod http;
mod nonce_manager;
pub mod redis_helpers;
mod registration_worker;
mod rpc_client;
mod transfer_worker;
pub mod types;
mod verification_worker;

pub use access_key_pool::{AccessKeyPool, LeasedKey};
pub use config::{CliArgs, RedisSettings, RelayConfig, RelayConfigBuilder};
pub use nonce_manager::NonceManager;
pub use rpc_client::{NearRpcClient, TxStatus};
pub use types::*;

use anyhow::Result;
use log::{debug, info, warn};
use std::sync::Arc;
use tokio::signal;

pub async fn run(config: RelayConfig) -> Result<()> {
    let RelayConfig {
        token,
        account_id,
        secret_keys,
        rpc_url,
        batch_linger_ms,
        batch_submit_delay_ms: _,
        max_inflight_batches: _,
        max_workers,
        max_registration_workers,
        max_verification_workers,
        bind_addr,
        redis,
    } = config;

    info!("Starting FT Relay Service");
    info!("Token: {}", token);
    info!("Relay account: {}", account_id);
    info!("Access keys: {}", secret_keys.len());
    info!("RPC URL: {}", rpc_url);

    let mut access_keys = Vec::new();
    for key_str in &secret_keys {
        let secret_key: near_crypto::SecretKey = key_str.parse()?;
        access_keys.push(AccessKey::from_secret_key(secret_key));
    }

    // Create separate Redis client for HTTP handler (independent TCP connection)
    let http_redis_client = redis::Client::open(redis.url.as_str())?;
    let redis_conn = redis::aio::ConnectionManager::new(http_redis_client).await?;

    let env = if token.contains(".testnet") {
        "testnet"
    } else if token.contains(".near") {
        "mainnet"
    } else {
        "sandbox"
    };

    // Dedicated Redis connection for RPC stats
    let rpc_redis_client = redis::Client::open(redis.url.as_str())?;
    let rpc_redis_conn = redis::aio::ConnectionManager::new(rpc_redis_client).await?;

    let rpc_client = Arc::new(NearRpcClient::new(&rpc_url, rpc_redis_conn, env.to_string()));
    let access_key_pool = Arc::new(AccessKeyPool::new(access_keys.clone(), redis_conn.clone()));

    let mut nonce_manager = NonceManager::new(redis_conn.clone());

    info!("Initializing nonces from RPC for {} access keys...", access_keys.len());
    
    // Fetch all access keys at once (much faster than one-by-one)
    let access_key_list = rpc_client
        .get_access_key_list(&account_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch access key list during initialization: {}", e))?;
    
    info!("Fetched {} access keys from RPC, matching against our {} keys...", access_key_list.len(), access_keys.len());
    
    // Build a map of public_key -> nonce for quick lookup
    let mut nonce_map = std::collections::HashMap::new();
    for key_info in &access_key_list {
        nonce_map.insert(key_info.public_key.to_string(), key_info.access_key.nonce);
    }
    
    let mut initialized_count = 0;
    let mut already_cached = 0;
    
    for key in &access_keys {
        if !nonce_manager.is_initialized(&key.key_id).await? {
            let public_key_str = key.public_key.to_string();
            match nonce_map.get(&public_key_str) {
                Some(&nonce) => {
                    nonce_manager
                        .initialize_nonce(&key.key_id, nonce)
                        .await?;
                    debug!("Initialized nonce for key {} to {}", key.key_id, nonce);
                    initialized_count += 1;
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "Access key {} not found in account {}. This key may not exist on-chain.",
                        public_key_str, account_id
                    ));
                }
            }
        } else {
            debug!("Key {} already initialized, skipping", key.key_id);
            already_cached += 1;
        }
    }
    
    info!("✅ Nonce initialization complete: {} keys initialized, {} already cached", initialized_count, already_cached);

    let router = http::build_router(redis_conn.clone(), env.to_string(), token.clone());
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
        info!(
            "HTTP server listening on http://{}",
            listener.local_addr().unwrap()
        );
        axum::serve(listener, router).await.unwrap();
    });

    info!(
        "Spawning {} registration worker(s)",
        max_registration_workers
    );
    for idx in 0..max_registration_workers {
        // Each worker gets its OWN Redis client (separate TCP connection, not multiplexed)
        let worker_redis_client = redis::Client::open(redis.url.as_str())?;
        let worker_conn = redis::aio::ConnectionManager::new(worker_redis_client).await?;
        let runtime = Arc::new(registration_worker::RegistrationWorkerRuntime {
            redis_conn: worker_conn.clone(),
            rpc_client: rpc_client.clone(),
            access_key_pool: access_key_pool.clone(),
            nonce_manager: NonceManager::new(worker_conn),
            relay_account: account_id.clone(),
            token: token.clone(),
            env: env.to_string(),
        });

        let ctx = registration_worker::RegistrationWorkerContext {
            runtime,
            linger_ms: batch_linger_ms,
        };

        tokio::spawn(async move {
            if let Err(err) = registration_worker::registration_worker_loop(ctx).await {
                warn!(
                    "Registration worker {} terminated with error: {:?}",
                    idx, err
                );
            }
        });
    }

    info!("Spawning {} transfer worker(s)", max_workers);
    for idx in 0..max_workers {
        // Each worker gets its OWN Redis client (separate TCP connection, not multiplexed)
        let worker_redis_client = redis::Client::open(redis.url.as_str())?;
        let worker_conn = redis::aio::ConnectionManager::new(worker_redis_client).await?;
        let runtime = Arc::new(transfer_worker::TransferWorkerRuntime {
            redis_conn: worker_conn.clone(),
            rpc_client: rpc_client.clone(),
            access_key_pool: access_key_pool.clone(),
            nonce_manager: NonceManager::new(worker_conn),
            relay_account: account_id.clone(),
            token: token.clone(),
            env: env.to_string(),
        });

        let ctx = transfer_worker::TransferWorkerContext {
            runtime,
            linger_ms: batch_linger_ms,
        };

        tokio::spawn(async move {
            if let Err(err) = transfer_worker::transfer_worker_loop(ctx).await {
                warn!("Transfer worker {} terminated with error: {:?}", idx, err);
            }
        });
    }

    info!(
        "Spawning {} verification worker(s)",
        max_verification_workers
    );
    for idx in 0..max_verification_workers {
        // Each worker gets its OWN Redis client (separate TCP connection, not multiplexed)
        let worker_redis_client = redis::Client::open(redis.url.as_str())?;
        let worker_conn = redis::aio::ConnectionManager::new(worker_redis_client).await?;
        let runtime = Arc::new(verification_worker::VerificationWorkerRuntime {
            redis_conn: worker_conn,
            rpc_client: rpc_client.clone(),
            relay_account: account_id.clone(),
            env: env.to_string(),
        });

        let ctx = verification_worker::VerificationWorkerContext { runtime };

        tokio::spawn(async move {
            if let Err(err) = verification_worker::verification_worker_loop(ctx).await {
                warn!(
                    "Verification worker {} terminated with error: {:?}",
                    idx, err
                );
            }
        });
    }

    info!("All workers started. Press Ctrl+C to shutdown.");

    signal::ctrl_c().await?;
    info!("Shutdown signal received, exiting");
    Ok(())
}
