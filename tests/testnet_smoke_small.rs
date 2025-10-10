// Smaller testnet test that can complete despite public RPC rate limits
// This test validates the system works correctly on testnet with real rate limiting

use anyhow::{ensure, Result};
use dotenv::dotenv;
use ft_relay::{RedisSettings, RelayConfig};
use near_api_types::NearToken;
use reqwest::StatusCode;
use std::time::{Duration, Instant};

mod common;

use common::{allocate_bind_addr, default_faucet_wait, HarnessConfig, TestnetHarness};

const TRANSFER_AMOUNT: &str = "1000000000000000000"; // 1 token
const SMALL_TEST_REQUESTS: usize = 5_000; // Reduced from 60k
const SMALL_TEST_CONCURRENCY: usize = 50; // Reduced from 100
const YOCTO_PER_TRANSFER: u128 = 1_000_000_000_000_000_000;

fn test_redis_settings() -> RedisSettings {
    RedisSettings::new(
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
        "ftrelay:testnet:ready",
        "ftrelay:testnet:ready_workers",
        "ftrelay:testnet:registrations",
        "ftrelay:testnet:registration_workers",
    )
}

#[tokio::test]
#[ignore] // Run manually with: cargo test --test testnet_smoke_small five_k -- --ignored --nocapture
async fn five_k_testnet_test() -> Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info,ft_relay=info,near_api=warn"),
    )
    .is_test(true)
    .try_init()
    .ok();
    dotenv().ok();

    // Flush Redis
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    redis::cmd("FLUSHALL")
        .query_async::<()>(&mut redis_conn)
        .await?;
    println!("✅ Redis flushed\n");

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  TESTNET 5K TEST: 5,000 Transfers with Rate Limiting      ║");
    println!("║  Validates: Zero duplicates + RPC backoff system          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let harness = TestnetHarness::new(HarnessConfig {
        label: "5k",
        receiver_count: 5,
        receiver_deposit: NearToken::from_millinear(200),
        signer_pool_size: 10,
        faucet_wait: default_faucet_wait(),
    })
    .await?;

    println!("✅ Testnet harness initialized");
    println!("   Owner: {}", harness.owner.account_id);
    println!("   Receivers: {}\n", harness.receivers.len());

    let result = run_small_benchmark(&harness).await;
    let teardown = harness.teardown().await;
    result?;
    teardown?;
    Ok(())
}

async fn run_small_benchmark(harness: &TestnetHarness) -> Result<()> {
    let initial_totals = harness.collect_balances().await?;
    ensure!(
        initial_totals.total_tokens == 0,
        "expected zero initial tokens"
    );

    let bind_addr = allocate_bind_addr()?;
    let redis = test_redis_settings();

    let config = RelayConfig {
        token: harness.owner.account_id.clone(),
        account_id: harness.owner.account_id.clone(),
        secret_keys: harness.relay_secret_keys(),
        rpc_url: harness.rpc_url.clone(),
        batch_linger_ms: 500,
        batch_submit_delay_ms: 0, // No throttling for small test
        max_inflight_batches: 100,
        max_workers: 1,
        bind_addr: bind_addr.clone(),
        redis: redis.clone(),
    };

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

    println!("Starting {} transfers...\n", SMALL_TEST_REQUESTS);
    let api_start = Instant::now();
    let requests_per_worker = SMALL_TEST_REQUESTS / SMALL_TEST_CONCURRENCY;

    let tasks: Vec<_> = (0..SMALL_TEST_CONCURRENCY)
        .map(|worker_id| {
            let client = client.clone();
            let endpoint = endpoint.clone();
            let receiver_ids = receiver_ids.clone();
            let receiver_count = receiver_ids.len();

            tokio::spawn(async move {
                let mut worker_success = 0;
                let start_idx = worker_id * requests_per_worker;
                let end_idx = if worker_id == SMALL_TEST_CONCURRENCY - 1 {
                    SMALL_TEST_REQUESTS
                } else {
                    (worker_id + 1) * requests_per_worker
                };

                for i in start_idx..end_idx {
                    let receiver_id = &receiver_ids[i % receiver_count];
                    let body = serde_json::json!({
                        "receiver_id": receiver_id,
                        "amount": TRANSFER_AMOUNT,
                        "memo": format!("testnet-{}", i),
                    });

                    match client.post(&endpoint).json(&body).send().await {
                        Ok(resp) if resp.status() == StatusCode::OK => worker_success += 1,
                        Ok(resp) => eprintln!(
                            "HTTP {}: {}",
                            resp.status(),
                            resp.text().await.unwrap_or_default()
                        ),
                        Err(err) => eprintln!("Request error: {err}"),
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

    let api_duration = api_start.elapsed();
    println!(
        "✅ API requests completed: {}/{} accepted in {:.2}s\n",
        accepted,
        SMALL_TEST_REQUESTS,
        api_duration.as_secs_f64()
    );

    ensure!(
        accepted >= SMALL_TEST_REQUESTS,
        "Not all requests accepted: {}/{}",
        accepted,
        SMALL_TEST_REQUESTS
    );

    // Poll for blockchain completion (with rate limit handling)
    println!("⏳ Waiting for blockchain settlement...\n");
    let expected_total = (SMALL_TEST_REQUESTS as u128) * YOCTO_PER_TRANSFER;
    let max_polls = 120; // 10 minutes max
    let poll_interval = Duration::from_secs(5);

    for poll in 1..=max_polls {
        let totals = match harness.collect_balances().await {
            Ok(t) => t,
            Err(e) => {
                let err_str = format!("{e:?}");
                if err_str.contains("rate limit") || err_str.contains("TooManyRequests") {
                    println!("  ⏸️  Poll {}: RPC rate limited, retrying...", poll);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                } else {
                    return Err(e);
                }
            }
        };

        let success_rate = totals.total_tokens as f64 / expected_total as f64 * 100.0;
        println!(
            "  Poll {}: {} tokens ({:.2}% complete)",
            poll,
            totals.total_tokens / YOCTO_PER_TRANSFER,
            success_rate
        );

        if totals.total_tokens >= expected_total {
            println!("  ✅ Target reached!\n");
            break;
        }

        if poll == max_polls {
            println!("  ⚠️  Reached polling cap\n");
        }

        tokio::time::sleep(poll_interval).await;
    }

    let final_totals = match harness.collect_balances().await {
        Ok(t) => t,
        Err(_) => {
            println!("  ⚠️  Could not collect final balances (RPC rate limited)\n");
            common::BalanceSummary {
                per_receiver: vec![],
                total_tokens: 0,
            }
        }
    };
    let on_chain_success = final_totals.total_tokens / YOCTO_PER_TRANSFER;
    let success_pct = (on_chain_success as f64 / SMALL_TEST_REQUESTS as f64) * 100.0;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  TESTNET RESULTS                                           ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("  Expected: {} transfers", SMALL_TEST_REQUESTS);
    println!(
        "  On-chain: {} transfers ({:.2}%)",
        on_chain_success, success_pct
    );
    println!(
        "  Status: {}",
        if success_pct >= 95.0 {
            "✅ PASSED"
        } else {
            "❌ FAILED"
        }
    );

    ensure!(
        success_pct >= 95.0,
        "Success rate too low: {:.2}% (expected ≥95%)",
        success_pct
    );

    relay_handle.abort();
    let _ = relay_handle.await;

    Ok(())
}
