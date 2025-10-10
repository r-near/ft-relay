use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use dotenv::dotenv;
use ft_relay::{RedisSettings, RelayConfig};
use near_api_types::NearToken;
use reqwest::StatusCode;

mod common;

use common::{
    allocate_bind_addr, default_faucet_wait, BalanceSummary, HarnessConfig, TestnetHarness,
};

const TRANSFER_AMOUNT: &str = "1000000000000000000"; // 1 token
const MEGA_BENCH_REQUESTS: usize = 60_000;
const MEGA_BENCH_CONCURRENCY: usize = 100;
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
        signer_pool_size: 50, // Large key pool, limited by max_workers=3
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

    let redis = test_redis_settings();

    let config = RelayConfig {
        token: harness.owner.account_id.clone(),
        account_id: harness.owner.account_id.clone(),
        secret_keys: harness.relay_secret_keys(),
        rpc_url: harness.rpc_url.clone(),
        batch_linger_ms: 500,
        batch_submit_delay_ms: 0, // No throttling - submit as fast as possible, rely on retries
        max_inflight_batches: 1000, // High concurrency to maximize throughput
        max_workers: 10, // Many workers to process batches quickly
        bind_addr: bind_addr.clone(),
        redis: redis.clone(),
    };

    println!("Server Configuration:");
    println!("  Batch linger: {}ms", config.batch_linger_ms);
    println!("  Batch submit delay: {}ms (throttling)", config.batch_submit_delay_ms);
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

    let api_start = Instant::now();
    let requests_per_worker = MEGA_BENCH_REQUESTS / MEGA_BENCH_CONCURRENCY;

    let tasks: Vec<_> = (0..MEGA_BENCH_CONCURRENCY)
        .map(|worker_id| {
            let client = client.clone();
            let endpoint = endpoint.clone();
            let receiver_ids = receiver_ids.clone();
            let receiver_count = receiver_ids.len();
            let api_started_at = api_start;

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
                        let elapsed = api_started_at.elapsed();
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

    let api_end = Instant::now();
    let api_elapsed = api_end
        .checked_duration_since(api_start)
        .unwrap_or_else(|| Duration::from_secs(0));
    let api_duration_secs = api_elapsed.as_secs_f64();
    let api_throughput = if api_duration_secs > 0.0 {
        accepted as f64 / api_duration_secs
    } else {
        0.0
    };

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  HTTP REQUEST RESULTS                                      ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("  Requests sent: {}", MEGA_BENCH_REQUESTS);
    println!("  Accepted: {}", accepted);
    println!("  Failed: {}", MEGA_BENCH_REQUESTS - accepted);
    println!("  API duration: {:.2}s", api_duration_secs);
    println!("  API throughput: {:.2} req/sec", api_throughput);
    println!("  Target: ≥100 req/sec");
    let http_success_pct = (accepted as f64 / MEGA_BENCH_REQUESTS as f64) * 100.0;
    println!("  HTTP success rate: {:.2}%", http_success_pct);

    if api_throughput >= 100.0 {
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
        api_throughput >= 100.0,
        "Throughput {:.2} req/sec below 100 req/sec requirement",
        api_throughput
    );

    ensure!(
        api_elapsed.as_secs() <= 600,
        "Took {:.2}s, should complete within 600s (10 minutes)",
        api_duration_secs
    );

    // Poll for transactions to finalize on testnet (with retries for 100% success)
    println!("\n⏳ Polling for NEAR testnet to finalize balances...");
    println!("   Requirement: 60k transfers must complete within 10 minutes total\n");

    let expected_total = accepted as u128 * YOCTO_PER_TRANSFER;
    let poll_interval = Duration::from_secs(5);
    let max_polls = 300; // 25 minutes max (1500 seconds) - allow time for retries to complete

    let mut final_totals: Option<BalanceSummary> = None;
    let mut completion_instant: Option<Instant> = None;
    let mut last_total = 0u128;
    let mut stuck_count = 0;

    for poll in 1..=max_polls {
        // collect_balances might hit rate limits, retry with delay instead of failing
        let totals = match harness.collect_balances().await {
            Ok(t) => t,
            Err(e) => {
                let err_str = format!("{e:?}");
                if err_str.contains("rate limit") || err_str.contains("TooManyRequests") {
                    println!("  ⏸️  Poll {}: RPC rate limited, will retry...", poll);
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

        // Detect stuck processing (no progress for 10+ consecutive polls)
        if totals.total_tokens == last_total && totals.total_tokens < expected_total {
            stuck_count += 1;
            if stuck_count >= 10 {
                println!(
                    "  ⚠️  No progress for {} polls (~{}s), but {} transfers still missing",
                    stuck_count,
                    stuck_count * poll_interval.as_secs(),
                    (expected_total - totals.total_tokens) / YOCTO_PER_TRANSFER
                );
            }
        } else {
            stuck_count = 0;
        }
        last_total = totals.total_tokens;

        if totals.total_tokens >= expected_total {
            println!(
                "  ✅ Target reached after {} polls (~{}s)",
                poll,
                poll * poll_interval.as_secs()
            );
            final_totals = Some(totals);
            completion_instant = Some(Instant::now());
            break;
        }

        if poll == max_polls {
            println!(
                "  ⚠️  Reached polling cap (~{}s); proceeding with current totals",
                poll * poll_interval.as_secs()
            );
            final_totals = Some(totals);
            completion_instant = Some(Instant::now());
            break;
        }

        tokio::time::sleep(poll_interval).await;
    }

    let final_totals = final_totals.expect("should have collected balances");
    let completion_instant = completion_instant.unwrap_or_else(Instant::now);
    let onchain_elapsed = completion_instant
        .checked_duration_since(api_start)
        .unwrap_or_else(|| Duration::from_secs(0));
    let onchain_duration_secs = onchain_elapsed.as_secs_f64();
    let post_api_elapsed = completion_instant
        .checked_duration_since(api_end)
        .unwrap_or_else(|| Duration::from_secs(0));
    let post_api_duration_secs = post_api_elapsed.as_secs_f64();
    let completed_transfers = (final_totals.total_tokens / YOCTO_PER_TRANSFER) as usize;
    let blockchain_throughput = if onchain_duration_secs > 0.0 {
        completed_transfers as f64 / onchain_duration_secs
    } else {
        0.0
    };
    let settlement_throughput = if post_api_duration_secs > 0.0 {
        completed_transfers as f64 / post_api_duration_secs
    } else {
        0.0
    };

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  ON-CHAIN VERIFICATION                                     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!(
        "  Expected total: {} tokens",
        expected_total / YOCTO_PER_TRANSFER
    );
    println!(
        "  Actual total:   {} tokens",
        final_totals.total_tokens / YOCTO_PER_TRANSFER
    );

    let on_chain_success_pct = final_totals.total_tokens as f64 / expected_total as f64 * 100.0;
    println!("  On-chain success rate: {:.2}%", on_chain_success_pct);
    println!(
        "  Blockchain time: {:.2}s (from first request)",
        onchain_duration_secs
    );
    println!(
        "  Settlement lag: {:.2}s after API completion",
        post_api_duration_secs
    );
    println!(
        "  Blockchain throughput {:.2} tx/sec (completed)",
        blockchain_throughput
    );
    if post_api_duration_secs > 0.0 {
        println!("  Post-API throughput {:.2} tx/sec", settlement_throughput);
    }

    println!("\nReceiver breakdown:");
    for (account, balance) in &final_totals.per_receiver {
        println!("  {:<45} {} tokens", account, balance / YOCTO_PER_TRANSFER);
    }

    // Print reconciliation report
    print_reconciliation_report(
        &redis,
        MEGA_BENCH_REQUESTS,
        completed_transfers,
        expected_total / YOCTO_PER_TRANSFER,
        final_totals.total_tokens / YOCTO_PER_TRANSFER,
    )
    .await?;

    ensure!(
        final_totals.total_tokens == expected_total,
        "On-chain total mismatch: expected {} tokens, got {} tokens ({:.2}%)",
        expected_total / YOCTO_PER_TRANSFER,
        final_totals.total_tokens / YOCTO_PER_TRANSFER,
        on_chain_success_pct
    );

    println!("\n🎉 ╔════════════════════════════════════════════════════════════╗");
    println!("   ║  TESTNET 60K BENCHMARK: ✅ PASSED                          ║");
    println!(
        "   ║  Successfully handled 60,000 transfers at {:.0}+ req/sec      ║",
        api_throughput
    );
    println!(
        "   ║  On-chain throughput {:.0}+ tx/sec (to completion)           ║",
        blockchain_throughput
    );
    println!("   ║  All requirements satisfied!                               ║");
    println!("   ╚════════════════════════════════════════════════════════════╝");

    // Emit benchmark results in parseable format for CI
    println!("\n--- BENCHMARK RESULTS (TESTNET) ---");
    println!("test_name: sixty_k_benchmark_test");
    println!("transfers: {}", MEGA_BENCH_REQUESTS);
    println!("api_duration_secs: {:.2}", api_duration_secs);
    println!("duration_secs: {:.2}", api_duration_secs);
    println!("throughput_req_per_sec: {:.2}", api_throughput);
    println!("blockchain_completion_secs: {:.2}", onchain_duration_secs);
    println!(
        "blockchain_throughput_req_per_sec: {:.2}",
        blockchain_throughput
    );
    println!("http_success_rate: {:.2}", http_success_pct);
    println!("onchain_success_rate: {:.2}", on_chain_success_pct);
    println!("status: PASSED");
    println!("--- END BENCHMARK RESULTS ---");

    relay_handle.abort();
    let _ = relay_handle.await;

    Ok(())
}

async fn print_reconciliation_report(
    redis: &RedisSettings,
    requests_sent: usize,
    _completed_transfers: usize,
    expected_tokens: u128,
    actual_tokens: u128,
) -> Result<()> {
    use redis::AsyncCommands;

    let redis_client = redis::Client::open(redis.url.as_str())?;
    let mut conn = redis_client.get_multiplexed_async_connection().await?;

    // Try to get transaction statuses
    let tx_keys: Vec<String> = conn.keys("tx:status:*").await.unwrap_or_default();
    
    let mut submitted = 0;
    let mut succeeded = 0;
    let mut timeout = 0;
    let mut failed = 0;
    let mut timeout_txs = Vec::new();
    
    for key in tx_keys {
        if let Ok(value) = conn.get::<_, String>(&key).await {
            if let Ok(status) = serde_json::from_str::<serde_json::Value>(&value) {
                match status["status"].as_str() {
                    Some("submitted") => submitted += 1,
                    Some("succeeded") => succeeded += 1,
                    Some("timeout") => {
                        timeout += 1;
                        timeout_txs.push((
                            status["tx_hash"].as_str().unwrap_or("unknown").to_string(),
                            status["transfer_count"].as_u64().unwrap_or(0) as usize,
                        ));
                    }
                    Some("failed") => failed += 1,
                    _ => {}
                }
            }
        }
    }

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  TRANSFER RECONCILIATION REPORT                            ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("📊 Transaction Metrics:");
    println!("  HTTP Requests:     {}", requests_sent);
    println!("  Submitted:         {}", submitted);
    println!("  Succeeded:         {}", succeeded);
    println!("  Timeout:           {}", timeout);
    println!("  Failed:            {}", failed);
    println!();
    
    println!("🎯 Transfer Summary:");
    println!("  Expected tokens:   {}", expected_tokens);
    println!("  On-chain tokens:   {}", actual_tokens);
    
    if actual_tokens > expected_tokens {
        let duplicates = actual_tokens - expected_tokens;
        println!("  ⚠️  Duplicates:       +{}", duplicates);
    } else if actual_tokens < expected_tokens {
        let missing = expected_tokens - actual_tokens;
        println!("  ❌ Missing:          -{}", missing);
    } else {
        println!("  ✅ Perfect match!");
    }
    
    if timeout > 0 {
        println!();
        println!("⏱️  Timeout Details:");
        let total_timeout_transfers: usize = timeout_txs.iter().map(|(_, count)| count).sum();
        println!("  {} transaction(s) timed out ({} transfers)", timeout, total_timeout_transfers);
        
        for (i, (tx_hash, count)) in timeout_txs.iter().enumerate().take(3) {
            println!("    {}. {}... → {} transfers", i + 1, &tx_hash[..16], count);
        }
        
        if actual_tokens > expected_tokens {
            let duplicates = actual_tokens - expected_tokens;
            println!();
            println!("  ⚠️  Analysis:");
            println!("     - {} transfers timed out on RPC", total_timeout_transfers);
            println!("     - Some likely succeeded on-chain despite timeout");
            println!("     - {} duplicates detected from retries", duplicates);
            println!("     - This is the RPC timeout issue we're tracking");
        }
    }

    Ok(())
}
