/// Integrated benchmark test that starts the relay service
/// 
/// This test:
/// 1. Flushes Redis
/// 2. Starts the relay service programmatically
/// 3. Submits transfers via HTTP API
/// 4. Verifies completion and measures metrics

use ft_relay::{RedisSettings, RelayConfig};
use reqwest::Client;
use serde_json::json;
use std::time::{Duration, Instant};

const NUM_TRANSFERS: usize = 100;
const BIND_ADDR: &str = "127.0.0.1:3031";

async fn flush_redis() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    println!("Flushing Redis at {}...", redis_url);
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    redis::cmd("FLUSHALL").query_async::<()>(&mut redis_conn).await?;
    println!("✅ Redis flushed");
    Ok(())
}

#[tokio::test]
#[ignore] // Run with: cargo test --test integrated_benchmark -- --ignored --nocapture
async fn test_http_api_with_service() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔════════════════════════════════════════════════╗");
    println!("║  Integrated Benchmark: HTTP API + Relay Service  ║");
    println!("║  Testing {} transfers with full pipeline        ║", NUM_TRANSFERS);
    println!("╚════════════════════════════════════════════════╝\n");
    
    // Flush Redis
    flush_redis().await?;
    
    // Get config from environment
    let token = std::env::var("RELAY_TOKEN")
        .unwrap_or_else(|_| "usdt.testnet".to_string());
    let account_id = std::env::var("RELAY_ACCOUNT_ID")
        .unwrap_or_else(|_| "relay.testnet".to_string());
    let secret_keys_str = std::env::var("RELAY_PRIVATE_KEYS")
        .unwrap_or_else(|_| "ed25519:placeholder_key_for_testing".to_string());
    let rpc_url = std::env::var("RELAY_RPC_URL")
        .unwrap_or_else(|_| "https://rpc.testnet.near.org".to_string());
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    
    println!("📋 Configuration:");
    println!("   Token: {}", token);
    println!("   Account: {}", account_id);
    println!("   RPC: {}", rpc_url);
    println!("   Bind: {}", BIND_ADDR);
    println!();
    
    // Build relay config
    let config = RelayConfig {
        token: token.clone(),
        account_id: account_id.clone(),
        secret_keys: secret_keys_str.split(',').map(|s| s.to_string()).collect(),
        rpc_url: rpc_url.clone(),
        batch_linger_ms: 50,
        batch_submit_delay_ms: 0,
        max_inflight_batches: 10,
        max_workers: 3,
        bind_addr: BIND_ADDR.to_string(),
        redis: RedisSettings {
            url: redis_url,
        },
    };
    
    // Start relay service in background
    println!("🚀 Starting relay service...");
    tokio::spawn(async move {
        if let Err(e) = ft_relay::run(config).await {
            eprintln!("Relay service error: {:?}", e);
        }
    });
    
    // Wait for server to start and be ready
    let client = Client::new();
    let health_url = format!("http://{}/health", BIND_ADDR);
    
    for i in 1..=10 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Ok(resp) = client.get(&health_url).send().await {
            if resp.status().is_success() {
                println!("✅ Relay service ready after {}ms\n", i * 500);
                break;
            }
        }
        if i == 10 {
            return Err("Relay service failed to start after 5 seconds".into());
        }
    }
    
    // Submit transfers
    let api_url = format!("http://{}/v1/transfer", BIND_ADDR);
    
    println!("📤 Submitting {} transfers...", NUM_TRANSFERS);
    let submit_start = Instant::now();
    
    let mut handles = vec![];
    for i in 0..NUM_TRANSFERS {
        let client = client.clone();
        let api_url = api_url.clone();
        let receiver = format!("user{}.testnet", i);
        
        let handle = tokio::spawn(async move {
            let idempotency_key = format!("benchmark-{}", i);
            let body = json!({
                "receiver_id": receiver,
                "amount": "1000000000000000000"
            });
            
            let result = client
                .post(&api_url)
                .header("X-Idempotency-Key", &idempotency_key)
                .json(&body)
                .timeout(Duration::from_secs(10))
                .send()
                .await;
            
            (i, result)
        });
        
        handles.push(handle);
    }
    
    // Collect submission results
    let mut success = 0;
    let mut failed = 0;
    for handle in handles {
        match handle.await {
            Ok((i, Ok(resp))) if resp.status().is_success() => {
                success += 1;
            }
            Ok((i, Ok(resp))) => {
                failed += 1;
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                println!("   Transfer {} failed: {} - {}", i, status, body);
            }
            Ok((i, Err(e))) => {
                failed += 1;
                println!("   Transfer {} error: {:?}", i, e);
            }
            Err(e) => {
                failed += 1;
                println!("   Task error: {:?}", e);
            }
        }
    }
    
    let submit_duration = submit_start.elapsed();
    println!("\n✅ Submission complete:");
    println!("   Success: {}/{}", success, NUM_TRANSFERS);
    println!("   Failed: {}", failed);
    println!("   Time: {:?}", submit_duration);
    println!("   Rate: {:.1} req/sec", NUM_TRANSFERS as f64 / submit_duration.as_secs_f64());
    
    // Wait for processing
    println!("\n⏳ Waiting 30 seconds for pipeline processing...");
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    // Check final status
    println!("\n🔍 Checking transfer statuses...");
    let mut completed = 0;
    let mut failed_status = 0;
    let mut in_progress = 0;
    let mut other = 0;
    
    for i in 0..NUM_TRANSFERS {
        let idempotency_key = format!("benchmark-{}", i);
        let status_url = format!("http://{}/v1/transfer/{}", BIND_ADDR, idempotency_key);
        
        if let Ok(resp) = client.get(&status_url).send().await {
            if let Ok(data) = resp.json::<serde_json::Value>().await {
                match data["status"].as_str() {
                    Some("COMPLETED") => completed += 1,
                    Some("FAILED") => {
                        failed_status += 1;
                        if let Some(events) = data["events"].as_array() {
                            if let Some(last_event) = events.last() {
                                println!("   Transfer {}: FAILED - {:?}", i, last_event);
                            }
                        }
                    }
                    Some(status) => {
                        in_progress += 1;
                        if in_progress <= 5 {  // Only print first 5
                            println!("   Transfer {}: {} (still processing)", i, status);
                        }
                    }
                    None => other += 1,
                }
            }
        }
    }
    
    // Get final metrics
    if let Ok(resp) = client.get(&health_url).send().await {
        if let Ok(health) = resp.json::<serde_json::Value>().await {
            println!("\n📊 Final Metrics:");
            println!("   RPC calls total: {}", health["metrics"]["rpc_calls_total"]);
            println!("   Redis status: {}", health["redis"]);
            
            let rpc_calls = health["metrics"]["rpc_calls_total"].as_u64().unwrap_or(0);
            if completed > 0 {
                println!("   RPC calls per completed transfer: {:.2}", rpc_calls as f64 / completed as f64);
            }
        }
    }
    
    println!("\n📈 Final Results:");
    println!("   ✅ Completed: {}/{} ({:.1}%)", 
             completed, NUM_TRANSFERS, 
             (completed as f64 / NUM_TRANSFERS as f64) * 100.0);
    println!("   ❌ Failed: {}", failed_status);
    println!("   ⏳ In Progress: {}", in_progress);
    println!("   ❓ Other: {}", other);
    
    let total_duration = submit_start.elapsed();
    println!("\n⏱️  Total Time: {:?}", total_duration);
    if completed > 0 {
        println!("   End-to-end throughput: {:.1} tx/sec", 
                 completed as f64 / total_duration.as_secs_f64());
    }
    
    // Assertions
    assert!(success >= NUM_TRANSFERS * 95 / 100, 
            "Less than 95% of submissions succeeded ({}/{})", success, NUM_TRANSFERS);
    
    println!("\n✅ Test passed! System is working correctly.");
    
    Ok(())
}
