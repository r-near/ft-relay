use redis::AsyncCommands;
use std::time::Instant;

#[tokio::test]
#[ignore]
async fn test_simple_redis_xadd_performance() {
    // Flush Redis first
    let redis_url = "redis://127.0.0.1:6379";
    let client = redis::Client::open(redis_url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    
    let _: () = redis::cmd("FLUSHALL")
        .query_async(&mut conn)
        .await
        .unwrap();
    println!("✅ Redis flushed");
    
    // Test 1: Direct XADD performance (no server)
    println!("\n=== Test 1: Direct XADD Performance ===");
    let stream_key = "ftrelay:sandbox:reg";
    
    let start = Instant::now();
    for i in 0..100 {
        conn.xadd::<_, _, _, _, ()>(
            stream_key,
            "*",
            &[("transfer_id", format!("test-{}", i).as_str()), ("retry_count", "0")],
        )
        .await
        .unwrap();
    }
    let elapsed = start.elapsed();
    
    println!("✅ 100 XADD operations completed in {:?}", elapsed);
    println!("   Average: {:?} per operation", elapsed / 100);
    println!("   Throughput: {:.0} ops/sec", 100.0 / elapsed.as_secs_f64());
    
    // Test 2: ConnectionManager clone behavior
    println!("\n=== Test 2: ConnectionManager Clone Performance ===");
    let conn_manager = redis::aio::ConnectionManager::new(client.clone()).await.unwrap();
    
    let start = Instant::now();
    for i in 0..100 {
        let mut cloned = conn_manager.clone();
        cloned.xadd::<_, _, _, _, ()>(
            "test_stream",
            "*",
            &[("data", format!("msg-{}", i).as_str())],
        )
        .await
        .unwrap();
    }
    let elapsed = start.elapsed();
    
    println!("✅ 100 XADD with clone per operation: {:?}", elapsed);
    println!("   Average: {:?} per operation", elapsed / 100);
    println!("   Throughput: {:.0} ops/sec", 100.0 / elapsed.as_secs_f64());
    
    // Test 3: Concurrent operations
    println!("\n=== Test 3: Concurrent XADD (10 parallel tasks) ===");
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for batch in 0..10 {
        let mut conn = conn_manager.clone();
        let handle = tokio::spawn(async move {
            let task_start = Instant::now();
            for i in 0..10 {
                conn.xadd::<_, _, _, _, ()>(
                    "concurrent_stream",
                    "*",
                    &[("data", format!("batch-{}-msg-{}", batch, i).as_str())],
                )
                .await
                .unwrap();
            }
            task_start.elapsed()
        });
        handles.push(handle);
    }
    
    for (i, handle) in handles.into_iter().enumerate() {
        let task_elapsed = handle.await.unwrap();
        println!("   Task {} took {:?}", i, task_elapsed);
    }
    
    let total_elapsed = start.elapsed();
    println!("✅ 100 concurrent XADD (10 tasks × 10 ops): {:?}", total_elapsed);
    println!("   Throughput: {:.0} ops/sec", 100.0 / total_elapsed.as_secs_f64());
    
    // Test 4: Heavy concurrency (100 parallel tasks, 1 op each)
    println!("\n=== Test 4: Heavy Concurrency (100 parallel tasks, 1 op each) ===");
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for i in 0..100 {
        let mut conn = conn_manager.clone();
        let handle = tokio::spawn(async move {
            let task_start = Instant::now();
            conn.xadd::<_, _, _, _, ()>(
                "heavy_concurrent_stream",
                "*",
                &[("data", format!("msg-{}", i).as_str())],
            )
            .await
            .unwrap();
            task_start.elapsed()
        });
        handles.push(handle);
    }
    
    let mut min_time = std::time::Duration::from_secs(999);
    let mut max_time = std::time::Duration::from_secs(0);
    let mut total_time = std::time::Duration::from_secs(0);
    
    for handle in handles {
        let task_elapsed = handle.await.unwrap();
        total_time += task_elapsed;
        min_time = min_time.min(task_elapsed);
        max_time = max_time.max(task_elapsed);
    }
    
    let total_elapsed = start.elapsed();
    let avg_time = total_time / 100;
    
    println!("✅ 100 concurrent XADD (100 tasks × 1 op): {:?}", total_elapsed);
    println!("   Min task time: {:?}", min_time);
    println!("   Avg task time: {:?}", avg_time);
    println!("   Max task time: {:?}", max_time);
    println!("   Throughput: {:.0} ops/sec", 100.0 / total_elapsed.as_secs_f64());
    
    // Check stream lengths
    println!("\n=== Stream Lengths ===");
    let len: usize = conn.xlen(stream_key).await.unwrap();
    println!("  {}: {} messages", stream_key, len);
    
    let len: usize = conn.xlen("test_stream").await.unwrap();
    println!("  test_stream: {} messages", len);
    
    let len: usize = conn.xlen("concurrent_stream").await.unwrap();
    println!("  concurrent_stream: {} messages", len);
    
    let len: usize = conn.xlen("heavy_concurrent_stream").await.unwrap();
    println!("  heavy_concurrent_stream: {} messages", len);
}

#[tokio::test]
#[ignore]
async fn test_http_endpoint_timing() {
    use ft_relay::RelayConfig;
    use near_workspaces::{network::Sandbox, Account, Contract, Worker};
    
    println!("\n=== HTTP Endpoint Timing Test ===");
    
    // Flush Redis
    let redis_url = "redis://127.0.0.1:6379";
    let client = redis::Client::open(redis_url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHALL")
        .query_async(&mut conn)
        .await
        .unwrap();
    println!("✅ Redis flushed");
    
    // Start sandbox
    let worker = near_workspaces::sandbox().await.unwrap();
    let wasm = std::fs::read("./resources/fungible_token.wasm").unwrap();
    let contract = worker.dev_deploy(&wasm).await.unwrap();
    
    let token_id = contract.id().clone();
    let relay_account = worker.dev_create_account().await.unwrap();
    
    // Initialize FT contract
    contract
        .call("new_default_meta")
        .args_json(serde_json::json!({
            "owner_id": relay_account.id(),
            "total_supply": "1000000000000000000000000000"
        }))
        .transact()
        .await
        .unwrap()
        .unwrap();
    
    println!("✅ FT contract deployed at {}", token_id);
    
    // Generate 1 access key for simplicity
    let key = near_workspaces::types::SecretKey::from_random(near_workspaces::types::KeyType::ED25519);
    let pk = key.public_key();
    
    relay_account
        .batch(relay_account.id())
        .add_key(pk.clone(), near_workspaces::AccessKey {
            nonce: 0,
            permission: near_workspaces::AccessKeyPermission::FullAccess,
        })
        .transact()
        .await
        .unwrap()
        .unwrap();
    
    println!("✅ Access key added");
    
    // Build config
    let config = RelayConfig {
        token: token_id.parse().unwrap(),
        account_id: relay_account.id().parse().unwrap(),
        secret_keys: vec![key.to_string()],
        rpc_url: worker.rpc_addr(),
        batch_linger_ms: 1000,
        batch_submit_delay_ms: 0,
        max_inflight_batches: 500,
        max_workers: 1,
        max_registration_workers: 1,
        max_verification_workers: 1,
        bind_addr: "127.0.0.1:18084".to_string(),
        redis: ft_relay::RedisSettings {
            url: redis_url.to_string(),
            stream_key: "ftrelay:test".to_string(),
            consumer_group: "ftrelay:test:group".to_string(),
            registration_stream_key: "ftrelay:test:reg".to_string(),
            registration_consumer_group: "ftrelay:test:reg:group".to_string(),
            transfer_stream_key: "ftrelay:test:transfer".to_string(),
            transfer_consumer_group: "ftrelay:test:transfer:group".to_string(),
            verification_stream_key: "ftrelay:test:verification".to_string(),
            verification_consumer_group: "ftrelay:test:verification:group".to_string(),
        },
    };
    
    // Start server
    tokio::spawn(async move {
        ft_relay::run(config).await.unwrap();
    });
    
    // Wait for server
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    println!("✅ Server started on http://127.0.0.1:18084");
    
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    
    // Test 1: Sequential requests
    println!("\n--- Sequential Requests (10) ---");
    let start = Instant::now();
    
    for i in 0..10 {
        let req_start = Instant::now();
        let response = http_client
            .post("http://127.0.0.1:18084/v1/transfer")
            .header("X-Idempotency-Key", format!("seq-test-{}", i))
            .json(&serde_json::json!({
                "receiver_id": "alice.near",
                "amount": "1000000000000000000000000"
            }))
            .send()
            .await
            .unwrap();
        
        let status = response.status();
        let req_elapsed = req_start.elapsed();
        println!("  Request {}: {} - {:?}", i, status, req_elapsed);
    }
    
    let total = start.elapsed();
    println!("✅ 10 sequential requests: {:?}", total);
    println!("   Average: {:?} per request", total / 10);
    println!("   Throughput: {:.1} req/sec", 10.0 / total.as_secs_f64());
    
    // Test 2: Concurrent requests
    println!("\n--- Concurrent Requests (20) ---");
    let start = Instant::now();
    let mut handles = vec![];
    
    for i in 0..20 {
        let client = http_client.clone();
        let handle = tokio::spawn(async move {
            let req_start = Instant::now();
            let response = client
                .post("http://127.0.0.1:18084/v1/transfer")
                .header("X-Idempotency-Key", format!("concurrent-test-{}", i))
                .json(&serde_json::json!({
                    "receiver_id": "bob.near",
                    "amount": "2000000000000000000000000"
                }))
                .send()
                .await
                .unwrap();
            
            (response.status(), req_start.elapsed())
        });
        handles.push(handle);
    }
    
    for (i, handle) in handles.into_iter().enumerate() {
        let (status, elapsed) = handle.await.unwrap();
        println!("  Request {}: {} - {:?}", i, status, elapsed);
    }
    
    let total = start.elapsed();
    println!("✅ 20 concurrent requests: {:?}", total);
    println!("   Throughput: {:.1} req/sec", 20.0 / total.as_secs_f64());
    
    // Check Redis stream
    println!("\n--- Redis Stream Check ---");
    let len: usize = conn.xlen("ftrelay:test:reg").await.unwrap();
    println!("  ftrelay:test:reg: {} messages", len);
    
    println!("\n✅ All tests completed!");
}
