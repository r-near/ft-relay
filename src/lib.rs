mod config;
pub mod http;
pub mod near;
pub mod redis;
pub mod types;
pub mod workers;

pub use config::{CliArgs, RedisSettings, RelayConfig, RelayConfigBuilder};
pub use near::{AccessKeyPool, LeasedKey, NearRpcClient, NonceManager, TxStatus};
pub use types::*;

use ::redis::aio::ConnectionManager;
use ::redis::Client;
use anyhow::{anyhow, Result};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::signal;
use workers::{registration, transfer, verification};

pub async fn run(config: RelayConfig) -> Result<()> {
    let RelayConfig {
        token,
        account_id,
        secret_keys,
        rpc_url,
        batch_linger_ms,
        transfer_workers,
        registration_workers,
        verification_workers,
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

    let redis_url = redis.url.clone();
    let redis_conn = create_redis_connection(&redis_url).await?;
    let rpc_conn = create_redis_connection(&redis_url).await?;
    let env = infer_environment(&token);

    let rpc_client = Arc::new(NearRpcClient::new(&rpc_url, rpc_conn, env.clone()));
    let access_key_pool = Arc::new(AccessKeyPool::new(access_keys.clone(), redis_conn.clone()));
    let mut nonce_manager = NonceManager::new(redis_conn.clone());

    info!(
        "Initializing nonces from RPC for {} access keys...",
        access_keys.len()
    );
    let nonce_stats = initialize_nonces(
        &access_keys,
        &mut nonce_manager,
        rpc_client.as_ref(),
        &account_id,
    )
    .await?;
    info!(
        "Fetched {} access keys from RPC, matching against our {} keys...",
        nonce_stats.onchain_keys,
        access_keys.len()
    );
    info!(
        "Nonce initialization complete: {} keys initialized, {} already cached",
        nonce_stats.initialized, nonce_stats.already_cached
    );

    spawn_http_server(
        bind_addr.clone(),
        redis_conn.clone(),
        env.clone(),
        token.clone(),
    );

    let worker_config = WorkerSpawnConfig {
        redis_url: &redis_url,
        rpc_client: rpc_client.clone(),
        access_key_pool: access_key_pool.clone(),
        relay_account: &account_id,
        token: &token,
        env: &env,
        batch_linger_ms,
    };

    info!("Spawning {} registration worker(s)", registration_workers);
    spawn_registration_workers(registration_workers, &worker_config).await?;

    info!("Spawning {} transfer worker(s)", transfer_workers);
    spawn_transfer_workers(transfer_workers, &worker_config).await?;

    info!("Spawning {} verification worker(s)", verification_workers);
    spawn_verification_workers(
        verification_workers,
        &redis_url,
        rpc_client.clone(),
        &account_id,
        &env,
    )
    .await?;

    info!("All workers started. Press Ctrl+C to shutdown.");

    signal::ctrl_c().await?;
    info!("Shutdown signal received, exiting");
    Ok(())
}

struct NonceInitStats {
    onchain_keys: usize,
    initialized: usize,
    already_cached: usize,
}

struct WorkerSpawnConfig<'a> {
    redis_url: &'a str,
    rpc_client: Arc<NearRpcClient>,
    access_key_pool: Arc<AccessKeyPool>,
    relay_account: &'a AccountId,
    token: &'a AccountId,
    env: &'a str,
    batch_linger_ms: u64,
}

async fn create_redis_connection(url: &str) -> Result<ConnectionManager> {
    let client = Client::open(url)?;
    Ok(ConnectionManager::new(client).await?)
}

fn infer_environment(token: &str) -> String {
    if token.contains(".testnet") {
        "testnet".to_string()
    } else if token.contains(".near") {
        "mainnet".to_string()
    } else {
        "sandbox".to_string()
    }
}

fn spawn_http_server(
    bind_addr: String,
    redis_conn: ConnectionManager,
    env: String,
    token: AccountId,
) {
    tokio::spawn(async move {
        let router = http::build_router(redis_conn, env, token);
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
        info!(
            "HTTP server listening on http://{}",
            listener.local_addr().unwrap()
        );
        axum::serve(listener, router).await.unwrap();
    });
}

async fn initialize_nonces(
    access_keys: &[AccessKey],
    nonce_manager: &mut NonceManager,
    rpc_client: &NearRpcClient,
    account_id: &AccountId,
) -> Result<NonceInitStats> {
    let access_key_list = rpc_client
        .get_access_key_list(account_id)
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to fetch access key list during initialization: {}",
                e
            )
        })?;

    let mut nonce_map = HashMap::new();
    for key_info in &access_key_list {
        nonce_map.insert(key_info.public_key.to_string(), key_info.access_key.nonce);
    }

    let mut initialized = 0;
    let mut already_cached = 0;

    for key in access_keys {
        if !nonce_manager.is_initialized(&key.key_id).await? {
            let public_key_str = key.public_key.to_string();
            match nonce_map.get(&public_key_str) {
                Some(&nonce) => {
                    nonce_manager.initialize_nonce(&key.key_id, nonce).await?;
                    debug!("Initialized nonce for key {} to {}", key.key_id, nonce);
                    initialized += 1;
                }
                None => {
                    return Err(anyhow!(
                        "Access key {} not found in account {}. This key may not exist on-chain.",
                        public_key_str,
                        account_id
                    ));
                }
            }
        } else {
            debug!("Key {} already initialized, skipping", key.key_id);
            already_cached += 1;
        }
    }

    Ok(NonceInitStats {
        onchain_keys: access_key_list.len(),
        initialized,
        already_cached,
    })
}

async fn spawn_registration_workers(count: usize, cfg: &WorkerSpawnConfig<'_>) -> Result<()> {
    for idx in 0..count {
        let worker_conn = create_redis_connection(cfg.redis_url).await?;
        let runtime = Arc::new(registration::RegistrationWorkerRuntime {
            redis_conn: worker_conn.clone(),
            rpc_client: cfg.rpc_client.clone(),
            access_key_pool: cfg.access_key_pool.clone(),
            nonce_manager: NonceManager::new(worker_conn),
            relay_account: cfg.relay_account.clone(),
            token: cfg.token.clone(),
            env: cfg.env.to_string(),
        });

        let ctx = registration::RegistrationWorkerContext {
            runtime,
            linger_ms: cfg.batch_linger_ms,
        };

        let worker_index = idx;
        tokio::spawn(async move {
            if let Err(err) = registration::registration_worker_loop(ctx).await {
                warn!(
                    "Registration worker {} terminated with error: {:?}",
                    worker_index, err
                );
            }
        });
    }

    Ok(())
}

async fn spawn_transfer_workers(count: usize, cfg: &WorkerSpawnConfig<'_>) -> Result<()> {
    for idx in 0..count {
        let worker_conn = create_redis_connection(cfg.redis_url).await?;
        let runtime = Arc::new(transfer::TransferWorkerRuntime {
            redis_conn: worker_conn.clone(),
            rpc_client: cfg.rpc_client.clone(),
            access_key_pool: cfg.access_key_pool.clone(),
            nonce_manager: NonceManager::new(worker_conn),
            relay_account: cfg.relay_account.clone(),
            token: cfg.token.clone(),
            env: cfg.env.to_string(),
        });

        let ctx = transfer::TransferWorkerContext {
            runtime,
            linger_ms: cfg.batch_linger_ms,
        };

        let worker_index = idx;
        tokio::spawn(async move {
            if let Err(err) = transfer::transfer_worker_loop(ctx).await {
                warn!(
                    "Transfer worker {} terminated with error: {:?}",
                    worker_index, err
                );
            }
        });
    }

    Ok(())
}

async fn spawn_verification_workers(
    count: usize,
    redis_url: &str,
    rpc_client: Arc<NearRpcClient>,
    account_id: &AccountId,
    env: &str,
) -> Result<()> {
    for idx in 0..count {
        let worker_conn = create_redis_connection(redis_url).await?;
        let runtime = Arc::new(verification::VerificationWorkerRuntime {
            redis_conn: worker_conn,
            rpc_client: rpc_client.clone(),
            relay_account: account_id.clone(),
            env: env.to_string(),
        });

        let ctx = verification::VerificationWorkerContext { runtime };

        let worker_index = idx;
        tokio::spawn(async move {
            if let Err(err) = verification::verification_worker_loop(ctx).await {
                warn!(
                    "Verification worker {} terminated with error: {:?}",
                    worker_index, err
                );
            }
        });
    }

    Ok(())
}
