/// Simple HTTP API benchmark test
/// 
/// Tests the relay service HTTP API with idempotency

use reqwest::Client;
use serde_json::json;
use std::time::{Duration, Instant};

const NUM_TRANSFERS: usize = 100;

async fn flush_redis() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    redis::cmd("FLUSHALL").query_async::<()>(&mut redis_conn).await?;
    Ok(())
}

#[tokio::test]
#[ignore] // Run with: cargo test --test integrated_benchmark -- --ignored --nocapture
async fn test_http_api_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🚀 HTTP API Benchmark Test");
    println!("Testing {} transfer submissions with idempotency\n", NUM_TRANSFERS);
    
    flush_redis().await?;
    
    // NOTE: This test requires the relay service to be running
    // Start with: cargo run
    let bind_addr = "127.0.0.1:3030";
    let client = Client::new();
    let api_url = format!("http://{}/v1/transfer", bind_addr);
    
    // Check if service is running
    let health_url = format!("http://{}/health", bind_addr);
    match client.get(&health_url).send().await {
        Ok(_) => println!("✅ Relay service detected at {}\n", bind_addr),
        Err(_) => {
            println!("❌ Relay service not running at {}", bind_addr);
            println!("   Please start with: cargo run");
            return Ok(());
        }
    }
    
    // Submit transfers
    println!("📤 Submitting {} transfers...", NUM_TRANSFERS);
    let start = Instant::now();
    
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
            
            client
                .post(&api_url)
                .header("X-Idempotency-Key", idempotency_key)
                .json(&body)
                .send()
                .await
        });
        
        handles.push(handle);
    }
    
    // Wait for all submissions
    let mut success = 0;
    let mut failed = 0;
    for handle in handles {
        match handle.await {
            Ok(Ok(resp)) if resp.status().is_success() => success += 1,
            _ => failed += 1,
        }
    }
    
    let submit_duration = start.elapsed();
    println!("✅ Submitted: {} success, {} failed in {:?}", success, failed, submit_duration);
    println!("   Rate: {:.1} req/sec\n", NUM_TRANSFERS as f64 / submit_duration.as_secs_f64());
    
    // Wait for processing
    println!("⏳ Waiting 30 seconds for processing...");
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    // Check status
    println!("\n🔍 Checking transfer statuses...");
    let mut completed = 0;
    let mut failed_status = 0;
    let mut other = 0;
    
    for i in 0..NUM_TRANSFERS {
        let idempotency_key = format!("benchmark-{}", i);
        let status_url = format!("http://{}/v1/transfer/{}", bind_addr, idempotency_key);
        
        if let Ok(resp) = client.get(&status_url).send().await {
            if let Ok(data) = resp.json::<serde_json::Value>().await {
                match data["status"].as_str() {
                    Some("COMPLETED") => completed += 1,
                    Some("FAILED") => failed_status += 1,
                    _ => other += 1,
                }
            }
        }
    }
    
    // Get metrics
    if let Ok(resp) = client.get(&health_url).send().await {
        if let Ok(health) = resp.json::<serde_json::Value>().await {
            println!("\n📊 Metrics:");
            println!("   RPC calls: {}", health["metrics"]["rpc_calls_total"]);
        }
    }
    
    println!("\n📈 Results:");
    println!("   Completed: {}/{}", completed, NUM_TRANSFERS);
    println!("   Failed: {}", failed_status);
    println!("   Other: {}", other);
    
    let total_duration = start.elapsed();
    println!("\n⏱️  Total time: {:?}", total_duration);
    
    Ok(())
}
