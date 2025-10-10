mod config;
pub mod http;
pub mod observability;
pub mod redis_helpers;
mod registration_worker;
mod rpc_backoff;
mod status_checker;
pub mod stream_queue;
pub mod transfer_states;
mod transfer_worker;

pub use config::{CliArgs, RedisSettings, RelayConfig, RelayConfigBuilder};
pub use observability::{BatchId, ObservabilityContext};

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
        batch_submit_delay_ms,
        max_inflight_batches,
        max_workers,
        bind_addr,
        redis,
    } = config;

    let network = NetworkConfig {
        rpc_endpoints: vec![RPCEndpoint::new(rpc_url.parse()?)],
        ..NetworkConfig::testnet()
    };

    // Create Redis client for shared operations
    let redis_client = redis::Client::open(redis.url.as_str())?;

    // Create connection manager for HTTP server (prevents connection exhaustion under load)
    let http_redis_conn = redis::aio::ConnectionManager::new(redis_client.clone()).await?;

    // Create ready transfer stream queue
    let ready_queue = Arc::new(
        stream_queue::StreamQueue::new(
            &redis.url,
            redis.stream_key.clone(),
            redis.consumer_group.clone(),
        )
        .await?,
    );

    // Create registration request stream queue
    let registration_stream_key = format!("registration_requests:{}", token);
    let registration_consumer_group = format!("registration_requests:{}:group", token);
    let registration_queue = Arc::new(
        stream_queue::StreamQueue::new(
            &redis.url,
            registration_stream_key,
            registration_consumer_group,
        )
        .await?,
    );

    let router = http::build_router(
        http_redis_conn,
        ready_queue.clone(),
        registration_queue.clone(),
        token.clone(),
    );

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

    // Create observability context
    let obs_ctx = Arc::new(observability::ObservabilityContext::new(redis_client.clone()));
    
    // Start periodic summary logger
    let obs_ctx_clone = obs_ctx.clone();
    tokio::spawn(async move {
        observability::summary_logger_task(obs_ctx_clone).await;
    });

    // Spawn registration worker(s)
    let registration_runtime = Arc::new(registration_worker::RegistrationWorkerRuntime {
        redis_client: redis_client.clone(),
        registration_queue: registration_queue.clone(),
        ready_queue: ready_queue.clone(),
        signer: signer.clone(),
        signer_account: account_id.clone(),
        token: token.clone(),
        network: network.clone(),
    });

    info!(
        "spawning {} registration worker(s)",
        config::DEFAULT_MAX_REGISTRATION_WORKERS
    );
    for idx in 0..config::DEFAULT_MAX_REGISTRATION_WORKERS {
        let ctx = registration_worker::RegistrationWorkerContext {
            runtime: registration_runtime.clone(),
            linger_ms: batch_linger_ms,
        };
        tokio::spawn(async move {
            if let Err(err) = registration_worker::registration_worker_loop(ctx).await {
                warn!("registration worker {idx} terminated with error: {err:?}");
            }
        });
    }

    // Spawn status checker workers (for async transaction verification)
    // Use separate connection manager to avoid exhausting Redis connections
    let status_checker_conn = redis::aio::ConnectionManager::new(redis_client.clone()).await?;
    let status_checker_runtime = Arc::new(status_checker::StatusCheckerRuntime {
        redis_conn: status_checker_conn,
        ready_queue: ready_queue.clone(),
        network: network.clone(),
        obs_ctx: obs_ctx.clone(),
        token: token.clone(),
    });

    let num_status_checkers = (max_workers / 2).max(2); // At least 2 status checkers
    info!("spawning {} status checker worker(s)", num_status_checkers);
    for _idx in 0..num_status_checkers {
        let runtime = status_checker_runtime.clone();
        tokio::spawn(async move {
            status_checker::status_checker_task(runtime).await;
        });
    }

    // Spawn transfer workers
    // Use separate connection manager for enqueuing to verification queue
    let transfer_redis_conn = redis::aio::ConnectionManager::new(redis_client.clone()).await?;
    let transfer_runtime = Arc::new(transfer_worker::TransferWorkerRuntime {
        redis_client: redis_client.clone(),
        redis_conn: transfer_redis_conn,
        ready_queue: ready_queue.clone(),
        signer: signer.clone(),
        signer_account: account_id.clone(),
        token: token.clone(),
        network: network.clone(),
        semaphore: semaphore.clone(),
        obs_ctx: obs_ctx.clone(),
    });

    info!("spawning {} transfer worker(s)", max_workers);
    for idx in 0..max_workers {
        let ctx = transfer_worker::TransferWorkerContext {
            linger_ms: batch_linger_ms,
            batch_submit_delay_ms,
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
