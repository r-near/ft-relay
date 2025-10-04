mod config;
pub mod http;
mod redis_queue;
mod types;
mod worker;

pub use config::{RedisSettings, RelayConfig, RelayConfigBuilder};
pub use redis_queue::RedisQueue;

use std::sync::Arc;

use anyhow::Result;
use log::{info, warn};
use near_api::{NetworkConfig, RPCEndpoint, Signer};
use tokio::{signal, sync::Semaphore};

pub async fn run(config: RelayConfig) -> Result<()> {
    let RelayConfig {
        token,
        account_id,
        secret_keys,
        rpc_url,
        batch_size,
        batch_linger_ms,
        max_inflight_batches,
        max_workers,
        bind_addr,
        redis,
    } = config;

    let network = NetworkConfig {
        rpc_endpoints: vec![RPCEndpoint::new(rpc_url.parse()?)],
        ..NetworkConfig::testnet()
    };

    let queue = Arc::new(RedisQueue::new(redis.clone()).await?);

    let router = http::build_router(queue.clone());

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
        info!("listening on http://{}", listener.local_addr().unwrap());
        axum::serve(listener, router).await.unwrap();
    });

    // Create shared signer pool with all keys
    let first_key = secret_keys[0].parse()?;
    let signer = Signer::new(Signer::from_secret_key(first_key))?;

    for key_str in secret_keys.iter().skip(1) {
        let key_signer = Signer::from_secret_key(key_str.parse()?);
        signer.add_signer_to_pool(key_signer).await?;
    }

    info!(
        "initialized shared signer pool with {} key(s)",
        secret_keys.len()
    );

    let semaphore = Arc::new(Semaphore::new(max_inflight_batches));

    // Spawn workers sharing the same signer pool
    let runtime = Arc::new(worker::WorkerRuntime {
        queue: queue.clone(),
        signer: signer.clone(),
        signer_account: account_id.clone(),
        token: token.clone(),
        network: network.clone(),
        semaphore: semaphore.clone(),
    });

    info!("spawning {} worker(s)", max_workers);
    for idx in 0..max_workers {
        let ctx = worker::WorkerContext {
            batch_size,
            linger_ms: batch_linger_ms,
            runtime: runtime.clone(),
        };

        tokio::spawn(async move {
            if let Err(err) = worker::worker_loop(ctx).await {
                warn!("worker {idx} exited: {err:?}");
            }
        });
    }

    signal::ctrl_c().await?;
    info!("shutdown signal received, exiting");
    Ok(())
}
