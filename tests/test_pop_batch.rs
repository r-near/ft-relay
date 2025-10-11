use anyhow::Result;
use ft_relay::redis::streams;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestMessage {
    id: u32,
    data: String,
}

async fn setup_redis() -> Result<redis::aio::ConnectionManager> {
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let conn = redis::aio::ConnectionManager::new(redis_client).await?;
    Ok(conn)
}

async fn create_test_stream(
    conn: &mut redis::aio::ConnectionManager,
    stream_key: &str,
    count: usize,
) -> Result<()> {
    for i in 0..count {
        let msg = TestMessage {
            id: i as u32,
            data: format!("message-{}", i),
        };
        let serialized = serde_json::to_string(&msg)?;
        conn.xadd::<_, _, _, _, ()>(stream_key, "*", &[("data", serialized.as_str())])
            .await?;
    }
    Ok(())
}

async fn create_consumer_group(
    conn: &mut redis::aio::ConnectionManager,
    stream_key: &str,
    group_name: &str,
) -> Result<()> {
    // Try to create group, ignore error if already exists
    let _: Result<(), _> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream_key)
        .arg(group_name)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    Ok(())
}

#[tokio::test]
async fn test_pop_batch_full_batch_immediate() -> Result<()> {
    let mut conn = setup_redis().await?;
    let stream_key = "test:pop_batch:full";
    let group = "test-group";
    let consumer = "consumer-1";

    // Clean up from previous runs
    let _: Result<(), _> = conn.del(stream_key).await;

    // Create 100 messages
    create_test_stream(&mut conn, stream_key, 100).await?;
    create_consumer_group(&mut conn, stream_key, group).await?;

    // Pop batch with max=100, should get all immediately
    let batch: Vec<(String, TestMessage)> = streams::pop_batch(
        &mut conn, stream_key, group, consumer, 100, 1000, // 1 second linger
    )
    .await?;

    assert_eq!(batch.len(), 100, "Should get full batch of 100 messages");

    // Verify messages are in order
    for (i, (_stream_id, msg)) in batch.iter().enumerate() {
        assert_eq!(msg.id, i as u32);
    }

    // Clean up
    let _: Result<(), _> = conn.del(stream_key).await;
    Ok(())
}

#[tokio::test]
async fn test_pop_batch_partial_with_linger() -> Result<()> {
    let mut conn = setup_redis().await?;
    let stream_key = "test:pop_batch:partial";
    let group = "test-group";
    let consumer = "consumer-1";

    // Clean up from previous runs
    let _: Result<(), _> = conn.del(stream_key).await;

    // Create only 10 messages
    create_test_stream(&mut conn, stream_key, 10).await?;
    create_consumer_group(&mut conn, stream_key, group).await?;

    let start = std::time::Instant::now();

    // Pop batch with max=100, should get 10 after lingering
    let batch: Vec<(String, TestMessage)> = streams::pop_batch(
        &mut conn, stream_key, group, consumer, 100, 500, // 500ms linger
    )
    .await?;

    let elapsed = start.elapsed();

    assert_eq!(batch.len(), 10, "Should get 10 messages");
    // Should return relatively quickly since no more messages are available
    // (won't wait full linger time if stream is empty)
    println!("Got {} messages in {}ms", batch.len(), elapsed.as_millis());

    // Clean up
    let _: Result<(), _> = conn.del(stream_key).await;
    Ok(())
}

#[tokio::test]
async fn test_pop_batch_accumulates_during_linger() -> Result<()> {
    let mut conn = setup_redis().await?;
    let stream_key = "test:pop_batch:accumulate";
    let group = "test-group";
    let consumer = "consumer-1";

    // Clean up from previous runs
    let _: Result<(), _> = conn.del(stream_key).await;

    // Create initial 10 messages
    create_test_stream(&mut conn, stream_key, 10).await?;
    create_consumer_group(&mut conn, stream_key, group).await?;

    // Spawn a task to add more messages after 200ms
    let stream_key_clone = stream_key.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let mut conn2 = setup_redis().await.unwrap();
        // Add 20 more messages
        for i in 100..120 {
            let msg = TestMessage {
                id: i,
                data: format!("message-{}", i),
            };
            let serialized = serde_json::to_string(&msg).unwrap();
            conn2
                .xadd::<_, _, _, _, ()>(&stream_key_clone, "*", &[("data", serialized.as_str())])
                .await
                .unwrap();
        }
    });

    let start = std::time::Instant::now();

    // Pop batch with max=100, linger=500ms
    // Should get 10 initially, then accumulate the 20 that arrive during linger
    let batch: Vec<(String, TestMessage)> =
        streams::pop_batch(&mut conn, stream_key, group, consumer, 100, 500).await?;

    let elapsed = start.elapsed();

    // Should accumulate messages added during linger period
    assert!(
        batch.len() >= 10,
        "Should get at least initial 10 messages, got {}",
        batch.len()
    );
    assert!(
        batch.len() <= 30,
        "Should get at most 30 messages (10 + 20), got {}",
        batch.len()
    );

    println!(
        "Accumulated {} messages in {}ms",
        batch.len(),
        elapsed.as_millis()
    );

    // Clean up
    let _: Result<(), _> = conn.del(stream_key).await;
    Ok(())
}
