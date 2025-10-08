use anyhow::Result;
use ft_relay::{http::build_router, TransferQueue};
use redis::aio::ConnectionManager;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_transfer_flow_returns_status() -> Result<()> {
    // Unique Redis namespace per test run.
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let namespace = Uuid::new_v4().to_string();
    let ready_stream_key = format!("ftrelay:test:{}:ready", namespace);
    let ready_consumer_group = format!("ftrelay:test:{}:ready_workers", namespace);
    let token: near_api_types::AccountId = "test.near".parse().unwrap();

    let queue = TransferQueue::new(
        &redis_url,
        token.as_ref(),
        &ready_stream_key,
        &ready_consumer_group,
    )
    .await?;
    let queue = std::sync::Arc::new(queue);

    // Flush Redis before test
    let redis_client = redis::Client::open(redis_url.clone())?;
    let mut redis_conn = ConnectionManager::new(redis_client.clone()).await?;
    redis::cmd("FLUSHALL")
        .query_async::<()>(&mut redis_conn)
        .await?;

    // Start HTTP server on an ephemeral port.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = build_router(queue.clone(), token.clone());

    tokio::spawn(async move {
        let _ = axum::serve(listener, server)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let client = reqwest::Client::new();
    let mut transfer_ids = Vec::new();

    for i in 0..5 {
        let payload = serde_json::json!({
            "receiver_id": format!("user{}.near", i),
            "amount": "100",
            "memo": format!("memo-{}", i),
        });

        let resp = client
            .post(format!("http://{}/v1/transfer", addr))
            .json(&payload)
            .send()
            .await?;

        assert!(resp.status().is_success());
        let body: Value = resp.json().await?;
        assert_eq!(body["status"], "queued");
        let transfer_id = body["transfer_id"]
            .as_str()
            .expect("transfer_id present")
            .to_string();
        transfer_ids.push(transfer_id);
    }

    // With the new architecture, transfers to unregistered accounts go to pending lists
    // Let's just verify we can query status

    // Test the GET /v1/transfer/:id endpoint - should return pending_registration
    for transfer_id in &transfer_ids {
        let resp = client
            .get(format!("http://{}/v1/transfer/{}", addr, transfer_id))
            .send()
            .await?;

        assert!(resp.status().is_success());
        let body: Value = resp.json().await?;
        // Should return pending_registration since accounts aren't registered
        assert!(
            body["status"] == "pending_registration" || body["status"] == "pending",
            "Expected pending_registration or pending, got: {}",
            body["status"]
        );
        assert_eq!(body["transfer_id"].as_str().unwrap(), transfer_id);
    }

    // Simulate worker setting tx_hash for first transfer
    let test_tx_hash = "ABC123DEF456HASH";
    queue
        .set_status(&[transfer_ids[0].clone()], test_tx_hash)
        .await?;

    // Test GET endpoint returns tx_hash when completed
    let resp = client
        .get(format!("http://{}/v1/transfer/{}", addr, transfer_ids[0]))
        .send()
        .await?;

    assert!(resp.status().is_success());
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], "completed");
    assert_eq!(body["transfer_id"].as_str().unwrap(), &transfer_ids[0]);
    assert_eq!(body["tx_hash"].as_str().unwrap(), test_tx_hash);

    // Clean up Redis artifacts created by the test.
    let _: () = redis::cmd("DEL")
        .arg(&ready_stream_key)
        .query_async(&mut redis_conn)
        .await?;

    // Clean up transfer status hashes
    for transfer_id in &transfer_ids {
        let key = format!("status:{}:{}", token, transfer_id);
        let _: () = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut redis_conn)
            .await?;
    }

    // Shut down the server.
    let _ = shutdown_tx.send(());

    Ok(())
}
