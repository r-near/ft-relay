mod config;
pub mod http;
mod redis_queue;
mod types;
mod worker;

pub use config::{CliArgs, RedisConfig, RelayConfig};
pub use redis_queue::RedisContext;

use std::sync::Arc;

use anyhow::Result;
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

    let redis = Arc::new(RedisContext::new(&config.redis).await?);
    let router = http::build_router(redis.clone());

    let bind_addr = config.bind_addr.clone();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
        info!("listening on http://{}", listener.local_addr().unwrap());
        axum::serve(listener, router).await.unwrap();
    });

    let semaphore = Arc::new(Semaphore::new(config.max_inflight_batches));
    let worker_count = std::cmp::max(1, std::cmp::min(config.max_workers, secret_keys.len()));

    // Calculate keys per worker (distribute evenly)
    let keys_per_worker = secret_keys.len() / worker_count;
    let mut extra_keys = secret_keys.len() % worker_count;

    info!(
        "spawning {} worker(s), distributing {} access key(s) (~{} keys per worker)",
        worker_count,
        secret_keys.len(),
        keys_per_worker
    );

    let mut key_offset = 0;
    for idx in 0..worker_count {
        // Distribute extra keys to first workers
        let keys_for_this_worker = if extra_keys > 0 {
            extra_keys -= 1;
            keys_per_worker + 1
        } else {
            keys_per_worker
        };

        // Create a dedicated signer for this worker with its subset of keys
        let worker_keys: Vec<String> = secret_keys
            .iter()
            .skip(key_offset)
            .take(keys_for_this_worker)
            .cloned()
            .collect();
        key_offset += keys_for_this_worker;

        if worker_keys.is_empty() {
            warn!("worker {idx} has no keys, skipping");
            continue;
        }

        let first_key = worker_keys[0].parse()?;
        let worker_signer = Signer::new(Signer::from_secret_key(first_key))?;

        for key_str in worker_keys.iter().skip(1) {
            let key_signer = Signer::from_secret_key(key_str.parse()?);
            worker_signer.add_signer_to_pool(key_signer).await?;
        }

        info!(
            "worker {idx} initialized with {} key(s) and independent nonce cache",
            worker_keys.len()
        );

        let worker_ctx = worker::WorkerContext {
            redis: redis.clone(),
            signer: worker_signer,
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
