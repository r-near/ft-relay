use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use dotenv::dotenv;
use ft_relay::{RedisConfig, RelayConfig};
use futures::stream::{self, StreamExt};
use reqwest::StatusCode;
use serde_json::json;

mod common;

use common::{
    allocate_bind_addr, default_faucet_wait, default_receiver_deposit, BalanceSummary,
    HarnessConfig, TestnetHarness,
};

const TRANSFER_AMOUNT: &str = "1000000000000000000"; // 1 token
const MINI_BENCH_REQUESTS: usize = 250;
const MINI_BENCH_CONCURRENCY: usize = 25;
const KILO_BENCH_REQUESTS: usize = 1_000;
const KILO_BENCH_CONCURRENCY: usize = 100;

fn test_redis_config() -> RedisConfig {
    RedisConfig {
        url: std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
        stream_key: "ftrelay:testnet:pending".to_string(),
        consumer_group: "ftrelay:testnet:batcher".to_string(),
    }
}

#[tokio::test]
#[ignore] // Run manually with: cargo test --test testnet_smoke smoke -- --ignored --nocapture
async fn faucet_smoke_test() -> Result<()> {
    env_logger::try_init().ok();
    dotenv().ok();

    let harness = TestnetHarness::new(HarnessConfig {
        label: "smoke",
        receiver_count: 1,
        receiver_deposit: default_receiver_deposit(),
        signer_pool_size: 1,
        faucet_wait: default_faucet_wait(),
    })
    .await?;

    let result = run_smoke_scenario(&harness).await;
    let teardown = harness.teardown().await;
    result?;
    teardown?;
    Ok(())
}

#[tokio::test]
#[ignore] // Run manually with: cargo test --test testnet_smoke mini -- --ignored --nocapture
async fn mini_benchmark_test() -> Result<()> {
    env_logger::try_init().ok();
    dotenv().ok();

    let harness = TestnetHarness::new(HarnessConfig {
        label: "mini",
        receiver_count: 5,
        receiver_deposit: default_receiver_deposit(),
        signer_pool_size: 2,
        faucet_wait: default_faucet_wait(),
    })
    .await?;

    let result = run_benchmark(
        &harness,
        MINI_BENCH_REQUESTS,
        MINI_BENCH_CONCURRENCY,
        "Mini",
    )
    .await;
    let teardown = harness.teardown().await;
    result?;
    teardown?;
    Ok(())
}

#[tokio::test]
#[ignore] // Run manually with: cargo test --test testnet_smoke kilo -- --ignored --nocapture
async fn thousand_benchmark_test() -> Result<()> {
    env_logger::try_init().ok();
    dotenv().ok();

    let harness = TestnetHarness::new(HarnessConfig {
        label: "kilo",
        receiver_count: 10,
        receiver_deposit: default_receiver_deposit(),
        signer_pool_size: 3,
        faucet_wait: default_faucet_wait(),
    })
    .await?;

    let result = run_benchmark(&harness, KILO_BENCH_REQUESTS, KILO_BENCH_CONCURRENCY, "1K").await;
    let teardown = harness.teardown().await;
    result?;
    teardown?;
    Ok(())
}

async fn run_smoke_scenario(harness: &TestnetHarness) -> Result<()> {
    let receiver = harness
        .receivers
        .first()
        .expect("smoke harness should create one receiver");

    let initial_balance = harness.fetch_balance(&receiver.account_id).await?;
    println!(
        "Initial receiver balance for {}: {}",
        receiver.account_id, initial_balance
    );
    ensure!(initial_balance == "0", "expected zero starting balance");

    harness
        .ft_transfer(&receiver.account_id, TRANSFER_AMOUNT)
        .await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let final_balance = harness.fetch_balance(&receiver.account_id).await?;
    println!(
        "Final receiver balance for {}: {}",
        receiver.account_id, final_balance
    );
    ensure!(
        final_balance == TRANSFER_AMOUNT,
        "receiver balance mismatch: expected {TRANSFER_AMOUNT}, got {final_balance}"
    );

    Ok(())
}

async fn run_benchmark(
    harness: &TestnetHarness,
    total_requests: usize,
    concurrency: usize,
    label: &str,
) -> Result<()> {
    ensure!(total_requests > 0, "total_requests must be > 0");
    ensure!(concurrency > 0, "concurrency must be > 0");

    let initial_totals = harness.collect_balances().await?;
    ensure!(
        initial_totals.total_tokens == 0,
        "expected zero initial tokens across receivers"
    );

    let bind_addr = allocate_bind_addr()?;

    let config = RelayConfig {
        token: harness.owner.account_id.clone(),
        account_id: harness.owner.account_id.clone(),
        secret_keys: harness.relay_secret_keys(),
        rpc_url: harness.rpc_url.clone(),
        batch_size: 90,
        batch_linger_ms: 20,
        max_inflight_batches: 300,
        bind_addr: bind_addr.clone(),
        redis: test_redis_config(),
    };

    let relay_handle = tokio::spawn(async move {
        if let Err(err) = ft_relay::run(config).await {
            eprintln!("Relay server error: {err:?}");
        }
    });

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let endpoint = format!("http://{bind_addr}/v1/transfer");
    let receiver_ids = harness.receiver_ids();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;

    println!(
        "\nStarting {label} benchmark: {} requests across {} receivers",
        total_requests,
        receiver_ids.len()
    );

    let start = Instant::now();

    let results = stream::iter(0..total_requests)
        .map(|i| {
            let client = client.clone();
            let endpoint = endpoint.clone();
            let receiver_id = receiver_ids[i % receiver_ids.len()].clone();
            async move {
                let payload = json!({
                    "receiver_id": receiver_id,
                    "amount": TRANSFER_AMOUNT,
                });

                match client.post(&endpoint).json(&payload).send().await {
                    Ok(resp) if resp.status() == StatusCode::OK => 1usize,
                    Ok(resp) => {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        eprintln!("HTTP {} for request {}: {}", status, i, body);
                        0
                    }
                    Err(err) => {
                        eprintln!("Request {} error: {err}", i);
                        0
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    let accepted: usize = results.into_iter().sum();
    let elapsed = start.elapsed();
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        total_requests as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("\n{label} Benchmark Results:");
    println!("  Requests sent: {}", total_requests);
    println!("  Accepted: {}", accepted);
    println!("  Duration: {:.2?}", elapsed);
    println!("  Throughput: {:.2} req/sec", throughput);
    println!(
        "  HTTP success rate: {:.2}%",
        (accepted as f64 / total_requests as f64) * 100.0
    );

    tokio::time::sleep(Duration::from_secs(5)).await;

    let final_totals = harness.collect_balances().await?;
    let expected_total = accepted as u128 * 1_000_000_000_000_000_000u128;
    print_balance_summary(&final_totals, expected_total);

    relay_handle.abort();
    let _ = relay_handle.await;

    Ok(())
}

fn print_balance_summary(summary: &BalanceSummary, expected_total: u128) {
    println!("\nOn-chain Balances:");
    for (account, balance) in &summary.per_receiver {
        println!("  {:<40} {:>20} yocto", account, balance);
    }
    println!("  Total tokens: {} yocto", summary.total_tokens);
    if expected_total > 0 {
        let pct = summary.total_tokens as f64 / expected_total as f64 * 100.0;
        println!("  On-chain success rate: {:.2}%", pct);
    }
}
