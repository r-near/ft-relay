mod config;
pub mod http;
mod queue;
mod registration_worker;
mod transfer_states;
mod transfer_worker;

pub use config::{CliArgs, RedisSettings, RelayConfig, RelayConfigBuilder};
pub use queue::TransferQueue;

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

    let queue = Arc::new(
        TransferQueue::new(
            &redis.url,
            &token.to_string(),
            &redis.stream_key,
            &redis.consumer_group,
        )
        .await?,
    );

    let router = http::build_router(queue.clone(), token.clone());

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

    // Spawn registration worker (single dedicated worker)
    let registration_runtime = Arc::new(registration_worker::RegistrationWorkerRuntime {
        queue: queue.clone(),
        signer: signer.clone(),
        signer_account: account_id.clone(),
        token: token.clone(),
        network: network.clone(),
    });

    info!("spawning {} registration worker(s)", config::DEFAULT_MAX_REGISTRATION_WORKERS);
    for idx in 0..config::DEFAULT_MAX_REGISTRATION_WORKERS {
        let ctx = registration_worker::RegistrationWorkerContext {
            runtime: registration_runtime.clone(),
            poll_interval_ms: batch_linger_ms,
        };
        tokio::spawn(async move {
            if let Err(err) = registration_worker::registration_worker_loop(ctx).await {
                warn!("registration worker {idx} terminated with error: {err:?}");
            }
        });
    }

    // Spawn transfer workers
    let transfer_runtime = Arc::new(transfer_worker::TransferWorkerRuntime {
        queue: queue.clone(),
        signer: signer.clone(),
        signer_account: account_id.clone(),
        token: token.clone(),
        network: network.clone(),
        semaphore: semaphore.clone(),
    });

    info!("spawning {} transfer worker(s)", max_workers);
    for idx in 0..max_workers {
        let ctx = transfer_worker::TransferWorkerContext {
            linger_ms: batch_linger_ms,
            runtime: transfer_runtime.clone(),
        };

        tokio::spawn(async move {
            if let Err(err) = transfer_worker::transfer_worker_loop(ctx).await {
                warn!("transfer worker {idx} terminated with error: {err:?}");
            }
        });
    }

    signal::ctrl_c().await?;
    info!("shutdown signal received, exiting");
    Ok(())
}
