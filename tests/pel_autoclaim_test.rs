use anyhow::Result;
use ft_relay::TransferQueue;
use ft_relay::transfer_states::{ReadyToSend, Transfer, TransferData};
use redis::AsyncCommands;
use std::time::Duration;
use uuid::Uuid;

/// Test that messages that aren't ACKed get autoclaimed and redelivered
#[tokio::test]
async fn test_pel_autoclaim() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let redis_url = "redis://127.0.0.1:6379";
    let token = format!("test-token-{}", Uuid::new_v4());
    let stream_key = format!("ready:{}", token);
    let group_name = format!("ready:{}:group", token);

    println!("Creating queue for token: {}", token);
    // Create queue
    let queue = TransferQueue::new(&redis_url, &token, &stream_key, &group_name).await?;

    // Push a test transfer
    let transfer_id = Uuid::new_v4().to_string();
    let transfer = Transfer::<ReadyToSend>::from_data(TransferData {
        transfer_id: transfer_id.clone(),
        receiver_id: "receiver.testnet".parse()?,
        amount: "100".to_string(),
        memo: None,
        attempts: 0,
        enqueued_at: 0,
    });
    println!("Pushing transfer: {}", transfer_id);
    queue.push_ready(&transfer).await?;

    // Consumer 1: Read but don't ACK (simulating a crash)
    let consumer1 = format!("consumer-{}", Uuid::new_v4());
    println!("Consumer1 ({}) attempting to read...", consumer1);
    let batch = queue.pop_ready_batch(&consumer1, 10, 100).await?;
    println!("Consumer1 read {} messages", batch.len());
    assert_eq!(batch.len(), 1, "Should read 1 message");
    assert_eq!(batch[0].1.data().transfer_id, transfer_id);

    // Don't ACK - simulating worker crash
    println!("Consumer1 read message but didn't ACK (simulating crash)");

    // Consumer 2: Try to read immediately (should get nothing - message not idle yet)
    let consumer2 = format!("consumer-{}", Uuid::new_v4());
    let batch2 = queue.pop_ready_batch(&consumer2, 10, 100).await?;
    assert_eq!(batch2.len(), 0, "Should get nothing - message not idle yet");

    println!("Consumer2 got nothing (message not idle yet)");

    // Wait for message to become idle (30s + buffer)
    println!("Waiting 35s for message to become idle...");
    tokio::time::sleep(Duration::from_secs(35)).await;

    // Consumer 2: Try again - should autoclaim the idle message
    let batch3 = queue.pop_ready_batch(&consumer2, 10, 100).await?;
    assert_eq!(batch3.len(), 1, "Should autoclaim the idle message");
    assert_eq!(batch3[0].1.data().transfer_id, transfer_id);

    println!("Consumer2 successfully autoclaimed the idle message!");

    // ACK it this time
    queue.ack(&[batch3[0].0.clone()]).await?;

    // Verify it's gone
    let batch4 = queue.pop_ready_batch(&consumer2, 10, 100).await?;
    assert_eq!(batch4.len(), 0, "Should be empty after ACK");

    // Cleanup
    let client = queue.get_client();
    let mut conn = client.get_multiplexed_async_connection().await?;
    conn.del::<_, ()>(&stream_key).await?;

    println!("✅ PEL autoclaim test passed!");
    Ok(())
}
