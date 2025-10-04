mod config;
pub mod http;
mod redis_queue;
mod types;
mod worker;

pub use config::{CliArgs, RedisConfig, RelayConfig};
pub use redis_queue::RedisContext;

use std::sync::Arc;

use anyhow::{anyhow, Result};
use log::{info, warn};
use near_api::{NetworkConfig, RPCEndpoint, Signer};
use tokio::{signal, sync::Semaphore};

pub async fn run(config: RelayConfig) -> Result<()> {
    let account_id = config.account_id.clone();
    let secret_keys = config.secret_keys.clone();

    let network = NetworkConfig {
        rpc_endpoints: vec![RPCEndpoint::new(config.rpc_url.parse()?)],
        ..NetworkConfig::testnet()
    };

    let first_key = secret_keys
        .first()
        .ok_or_else(|| anyhow!("No secret keys configured"))?;
    let signer = Signer::new(Signer::from_secret_key(first_key.parse()?))?;

    for key_str in secret_keys.iter().skip(1) {
        let key_signer = Signer::from_secret_key(key_str.parse()?);
        signer.add_signer_to_pool(key_signer).await?;
    }

    info!("Initialized signer with {} keys in pool", secret_keys.len());

    let redis = Arc::new(RedisContext::new(&config.redis).await?);
    let router = http::build_router(redis.clone());

    let bind_addr = config.bind_addr.clone();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
        info!("listening on http://{}", listener.local_addr().unwrap());
        axum::serve(listener, router).await.unwrap();
    });

    let semaphore = Arc::new(Semaphore::new(config.max_inflight_batches));
    let worker_count = std::cmp::max(1, secret_keys.len());
    info!("spawning {worker_count} worker(s)");
    for idx in 0..worker_count {
        let worker_ctx = worker::WorkerContext {
            redis: redis.clone(),
            signer: signer.clone(),
            signer_account: account_id.clone(),
            token: config.token.clone(),
            network: network.clone(),
            batch_size: config.batch_size,
            linger_ms: config.batch_linger_ms,
            semaphore: semaphore.clone(),
        };

        tokio::spawn(async move {
            if let Err(err) = worker::worker_loop(worker_ctx).await {
                warn!("worker loop {idx} exited: {err:?}");
            }
        });
    }

    signal::ctrl_c().await?;
    info!("shutdown signal received, exiting");
    Ok(())
}
