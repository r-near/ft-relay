mod config;
pub mod http;
pub mod redis;
pub mod types;
pub mod workers;

pub use config::{CliArgs, RedisSettings, RelayConfig, RelayConfigBuilder};
pub use types::*;

use ::redis::aio::ConnectionManager;
use ::redis::Client;
use anyhow::Result;
use log::info;
use near_kit::{Near, RotatingSigner, SecretKey};
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

    // Parse secret keys
    let keys: Vec<SecretKey> = secret_keys
        .iter()
        .map(|k| k.parse())
        .collect::<Result<Vec<_>, _>>()?;

    // Create RotatingSigner - this handles key rotation and nonce management automatically
    let signer = RotatingSigner::new(&account_id, keys)?;

    // Create the Near client
    let near = Arc::new(Near::custom(&rpc_url).signer(signer).build());

    let redis_url = redis.url.clone();
    let redis_conn = create_redis_connection(&redis_url).await?;
    let env = infer_environment(&token);

    spawn_http_server(
        bind_addr.clone(),
        redis_conn.clone(),
        env.clone(),
        token.clone(),
    );

    let worker_config = WorkerSpawnConfig {
        redis_url: &redis_url,
        near: near.clone(),
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
        near.clone(),
        &account_id,
        &env,
    )
    .await?;

    info!("All workers started. Press Ctrl+C to shutdown.");

    signal::ctrl_c().await?;
    info!("Shutdown signal received, exiting");
    Ok(())
}

struct WorkerSpawnConfig<'a> {
    redis_url: &'a str,
    near: Arc<Near>,
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

async fn spawn_registration_workers(count: usize, cfg: &WorkerSpawnConfig<'_>) -> Result<()> {
    for idx in 0..count {
        let worker_conn = create_redis_connection(cfg.redis_url).await?;
        let runtime = Arc::new(registration::RegistrationWorkerRuntime {
            redis_conn: worker_conn,
            near: cfg.near.clone(),
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
                log::warn!(
                    "Registration worker {} terminated with error: {:?}",
                    worker_index,
                    err
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
            redis_conn: worker_conn,
            near: cfg.near.clone(),
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
                log::warn!(
                    "Transfer worker {} terminated with error: {:?}",
                    worker_index,
                    err
                );
            }
        });
    }

    Ok(())
}

async fn spawn_verification_workers(
    count: usize,
    redis_url: &str,
    near: Arc<Near>,
    account_id: &AccountId,
    env: &str,
) -> Result<()> {
    for idx in 0..count {
        let worker_conn = create_redis_connection(redis_url).await?;
        let runtime = Arc::new(verification::VerificationWorkerRuntime {
            redis_conn: worker_conn,
            near: near.clone(),
            relay_account: account_id.clone(),
            env: env.to_string(),
        });

        let ctx = verification::VerificationWorkerContext { runtime };

        let worker_index = idx;
        tokio::spawn(async move {
            if let Err(err) = verification::verification_worker_loop(ctx).await {
                log::warn!(
                    "Verification worker {} terminated with error: {:?}",
                    worker_index,
                    err
                );
            }
        });
    }

    Ok(())
}
