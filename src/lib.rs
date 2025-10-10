mod access_key_pool;
mod config;
pub mod http;
mod nonce_manager;
pub mod redis_helpers;
mod registration_worker;
mod rpc_client;
pub mod types;
mod transfer_worker;
mod verification_worker;

pub use access_key_pool::{AccessKeyPool, LeasedKey};
pub use config::{CliArgs, RedisSettings, RelayConfig, RelayConfigBuilder};
pub use nonce_manager::NonceManager;
pub use rpc_client::{NearRpcClient, TxStatus};
pub use types::*;

use anyhow::Result;
use log::{info, warn};
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

    let redis_client = redis::Client::open(redis.url.as_str())?;
    
    // Create shared Redis connection manager wrapped in Arc<Mutex<>> for safe concurrent access
    // ConnectionManager itself multiplexes over a single connection, so we need mutex to serialize operations
    let conn_manager = redis::aio::ConnectionManager::new(redis_client.clone()).await?;
    let redis_conn = Arc::new(tokio::sync::Mutex::new(conn_manager.clone()));
    
    let rpc_client = Arc::new(NearRpcClient::new(&rpc_url));
    let access_key_pool = Arc::new(AccessKeyPool::new(access_keys.clone(), conn_manager.clone()));

    let mut nonce_manager = NonceManager::new(conn_manager.clone());

    info!("Initializing nonces from RPC...");
    for key in &access_keys {
        if !nonce_manager.is_initialized(&key.key_id).await? {
            let access_key_view = rpc_client.get_access_key(&account_id, &key.public_key).await?;
            nonce_manager
                .initialize_nonce(&key.key_id, access_key_view.nonce)
                .await?;
            info!("Initialized nonce for key {} to {}", key.key_id, access_key_view.nonce);
        }
    }

    let env = if token.contains(".testnet") {
        "testnet"
    } else if token.contains(".near") {
        "mainnet"
    } else {
        "sandbox"
    };

    let router = http::build_router(redis_conn.clone(), env.to_string(), token.clone());
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
        info!("HTTP server listening on http://{}", listener.local_addr().unwrap());
        axum::serve(listener, router).await.unwrap();
    });

    info!("Spawning {} registration worker(s)", max_registration_workers);
    for idx in 0..max_registration_workers {
        let runtime = Arc::new(registration_worker::RegistrationWorkerRuntime {
            redis_conn: conn_manager.clone(),
            rpc_client: rpc_client.clone(),
            access_key_pool: access_key_pool.clone(),
            nonce_manager: NonceManager::new(conn_manager.clone()),
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
                warn!("Registration worker {} terminated with error: {:?}", idx, err);
            }
        });
    }

    info!("Spawning {} transfer worker(s)", max_workers);
    for idx in 0..max_workers {
        let runtime = Arc::new(transfer_worker::TransferWorkerRuntime {
            redis_conn: conn_manager.clone(),
            rpc_client: rpc_client.clone(),
            access_key_pool: access_key_pool.clone(),
            nonce_manager: NonceManager::new(conn_manager.clone()),
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

    info!("Spawning {} verification worker(s)", max_verification_workers);
    for idx in 0..max_verification_workers {
        let runtime = Arc::new(verification_worker::VerificationWorkerRuntime {
            redis_conn: conn_manager.clone(),
            rpc_client: rpc_client.clone(),
            relay_account: account_id.clone(),
            env: env.to_string(),
        });

        let ctx = verification_worker::VerificationWorkerContext {
            runtime,
            linger_ms: batch_linger_ms,
        };

        tokio::spawn(async move {
            if let Err(err) = verification_worker::verification_worker_loop(ctx).await {
                warn!("Verification worker {} terminated with error: {:?}", idx, err);
            }
        });
    }

    info!("All workers started. Press Ctrl+C to shutdown.");

    signal::ctrl_c().await?;
    info!("Shutdown signal received, exiting");
    Ok(())
}
