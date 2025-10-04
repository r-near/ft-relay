use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use dotenv::dotenv;
use ft_relay::{RedisConfig, RelayConfig};
use futures::stream::{self, StreamExt};
use near_api_types::NearToken;
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
const MEGA_BENCH_REQUESTS: usize = 60_000;
const MEGA_BENCH_CONCURRENCY: usize = 100;

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
        max_workers: 3,
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

#[tokio::test]
#[ignore] // Run manually with: cargo test --test testnet_smoke sixty_k -- --ignored --nocapture
async fn sixty_k_benchmark_test() -> Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default()
            .default_filter_or("info,ft_relay=info,near_api=warn,tracing::span=warn"),
    )
    .is_test(true)
    .try_init()
    .ok();
    dotenv().ok();

    // Flush Redis before test
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    redis::cmd("FLUSHALL")
        .query_async::<()>(&mut redis_conn)
        .await?;
    println!("✅ Redis flushed\n");

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  TESTNET 60K BENCHMARK: 60,000 Transfers in 10 Minutes    ║");
    println!("║  Target: ≥100 transfers/second sustained                   ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Use fewer receivers due to 10N testnet faucet limit
    // For 60k transfers, use just 5 receivers to spread out the load
    // Each receiver gets 60k/5 = 12k transfers = 12k tokens
    // Use 2 faucet accounts to get ~20N total budget:
    //   - Owner gets 10N from faucet + ~9N from donor = ~19N
    //   - 5 receivers × 0.2N = 1N
    //   - Leaves ~18N for gas to handle 60k transfers
    // Use 50 access keys for excellent nonce headroom (~1,200 nonces per key)
    let harness = TestnetHarness::new(HarnessConfig {
        label: "60k",
        receiver_count: 5,
        receiver_deposit: NearToken::from_millinear(200), // 0.2N per receiver
        signer_pool_size: 50,                             // Large key pool, limited by max_workers=3
        faucet_wait: default_faucet_wait(),
    })
    .await?;

    println!("✅ Testnet harness initialized");
    println!("   Owner: {}", harness.owner.account_id);
    println!("   Receivers: {}", harness.receivers.len());
    println!("   Signer pool size: 50 (max_workers: 3)\n");

    let result = run_60k_benchmark(&harness).await;
    let teardown = harness.teardown().await;
    result?;
    teardown?;
    Ok(())
}

async fn run_60k_benchmark(harness: &TestnetHarness) -> Result<()> {
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
        max_inflight_batches: 500, // Higher for 60k
        max_workers: 3,
        bind_addr: bind_addr.clone(),
        redis: test_redis_config(),
    };

    println!("Server Configuration:");
    println!("  Batch size: {}", config.batch_size);
    println!("  Batch linger: {}ms", config.batch_linger_ms);
    println!("  Max inflight batches: {}", config.max_inflight_batches);
    println!("  Access keys: {}\n", config.secret_keys.len());

    let relay_handle = tokio::spawn(async move {
        if let Err(err) = ft_relay::run(config).await {
            eprintln!("❌ Relay server error: {err:?}");
        }
    });

    tokio::time::sleep(Duration::from_millis(1000)).await;
    println!("✅ Relay server started\n");

    let endpoint = format!("http://{bind_addr}/v1/transfer");
    let receiver_ids = harness.receiver_ids();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  Starting benchmark: {} transfers", MEGA_BENCH_REQUESTS);
    println!("║  Concurrent HTTP workers: {}", MEGA_BENCH_CONCURRENCY);
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let start = Instant::now();
    let requests_per_worker = MEGA_BENCH_REQUESTS / MEGA_BENCH_CONCURRENCY;

    let tasks: Vec<_> = (0..MEGA_BENCH_CONCURRENCY)
        .map(|worker_id| {
            let client = client.clone();
            let endpoint = endpoint.clone();
            let receiver_ids = receiver_ids.clone();
            let receiver_count = receiver_ids.len();

            tokio::spawn(async move {
                let mut worker_success = 0;
                let start_idx = worker_id * requests_per_worker;
                let end_idx = if worker_id == MEGA_BENCH_CONCURRENCY - 1 {
                    MEGA_BENCH_REQUESTS
                } else {
                    start_idx + requests_per_worker
                };

                for i in start_idx..end_idx {
                    let receiver_id = &receiver_ids[i % receiver_count];
                    let payload = serde_json::json!({
                        "receiver_id": receiver_id,
                        "amount": TRANSFER_AMOUNT,
                    });

                    match client.post(&endpoint).json(&payload).send().await {
                        Ok(resp) if resp.status() == StatusCode::OK => worker_success += 1,
                        Ok(resp) => {
                            if worker_id == 0 && worker_success == 0 {
                                eprintln!(
                                    "❌ Worker {} first request failed: status {}",
                                    worker_id,
                                    resp.status()
                                );
                            }
                        }
                        Err(e) => {
                            if worker_id == 0 && worker_success == 0 {
                                eprintln!("❌ Worker {} request error: {}", worker_id, e);
                            }
                        }
                    }

                    // Progress indicator every 10k requests from worker 0
                    if worker_id == 0 && i > 0 && i % 10_000 == 0 {
                        let elapsed = start.elapsed();
                        let current_rate = i as f64 / elapsed.as_secs_f64();
                        println!(
                            "  Progress: {} requests in {:?} ({:.1} req/sec)",
                            i, elapsed, current_rate
                        );
                    }
                }

                worker_success
            })
        })
        .collect();

    let mut accepted = 0;
    for task in tasks {
        accepted += task.await.unwrap_or(0);
    }

    let elapsed = start.elapsed();
    let throughput = MEGA_BENCH_REQUESTS as f64 / elapsed.as_secs_f64();

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  HTTP REQUEST RESULTS                                      ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("  Requests sent: {}", MEGA_BENCH_REQUESTS);
    println!("  Accepted: {}", accepted);
    println!("  Failed: {}", MEGA_BENCH_REQUESTS - accepted);
    println!("  Duration: {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.2} req/sec", throughput);
    println!("  Target: ≥100 req/sec");
    let http_success_pct = (accepted as f64 / MEGA_BENCH_REQUESTS as f64) * 100.0;
    println!("  HTTP success rate: {:.2}%", http_success_pct);

    if throughput >= 100.0 {
        println!("  Status: ✅ PASSED");
    } else {
        println!("  Status: ❌ FAILED");
    }

    ensure!(
        accepted >= MEGA_BENCH_REQUESTS,
        "Not all HTTP requests accepted: {}/{}",
        accepted,
        MEGA_BENCH_REQUESTS
    );

    ensure!(
        throughput >= 100.0,
        "Throughput {:.2} req/sec below 100 req/sec requirement",
        throughput
    );

    ensure!(
        elapsed.as_secs() <= 600,
        "Took {:.2}s, should complete within 600s (10 minutes)",
        elapsed.as_secs_f64()
    );

    // Poll for transactions to finalize on testnet (with retries for 100% success)
    println!("\n⏳ Polling for NEAR testnet to finalize balances...");

    let expected_total = accepted as u128 * 1_000_000_000_000_000_000u128;
    let poll_interval = Duration::from_secs(5);
    let max_polls = 60; // ~5 minutes max wait

    let mut final_totals: Option<BalanceSummary> = None;

    for poll in 1..=max_polls {
        let totals = harness.collect_balances().await?;
        let success_rate = totals.total_tokens as f64 / expected_total as f64 * 100.0;

        println!(
            "  Poll {}: {} tokens ({:.2}% complete)",
            poll,
            totals.total_tokens / 1_000_000_000_000_000_000,
            success_rate
        );

        if totals.total_tokens >= expected_total {
            println!(
                "  ✅ Target reached after {} polls (~{}s)",
                poll,
                poll * poll_interval.as_secs()
            );
            final_totals = Some(totals);
            break;
        }

        if poll == max_polls {
            println!(
                "  ⚠️  Reached polling cap (~{}s); proceeding with current totals",
                poll * poll_interval.as_secs()
            );
            final_totals = Some(totals);
            break;
        }

        tokio::time::sleep(poll_interval).await;
    }

    let final_totals = final_totals.expect("should have collected balances");

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  ON-CHAIN VERIFICATION                                     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!(
        "  Expected total: {} tokens",
        expected_total / 1_000_000_000_000_000_000
    );
    println!(
        "  Actual total:   {} tokens",
        final_totals.total_tokens / 1_000_000_000_000_000_000
    );

    let on_chain_success_pct = final_totals.total_tokens as f64 / expected_total as f64 * 100.0;
    println!("  On-chain success rate: {:.2}%", on_chain_success_pct);

    println!("\nReceiver breakdown:");
    for (account, balance) in &final_totals.per_receiver {
        println!(
            "  {:<45} {} tokens",
            account,
            balance / 1_000_000_000_000_000_000
        );
    }

    ensure!(
        on_chain_success_pct >= 99.0,
        "On-chain success rate {:.2}% below 99% minimum (polling should reach 100%)",
        on_chain_success_pct
    );

    println!("\n🎉 ╔════════════════════════════════════════════════════════════╗");
    println!("   ║  TESTNET 60K BENCHMARK: ✅ PASSED                          ║");
    println!(
        "   ║  Successfully handled 60,000 transfers at {:.0}+ req/sec      ║",
        throughput
    );
    println!("   ║  All requirements satisfied!                               ║");
    println!("   ╚════════════════════════════════════════════════════════════╝");

    relay_handle.abort();
    let _ = relay_handle.await;

    Ok(())
}
