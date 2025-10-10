/// Test to reproduce the race condition in pop_pending_transfers
/// This test demonstrates that transfers can be lost when HTTP handlers
/// add new transfers between LRANGE and DEL operations.
use ft_relay::redis_helpers;
use ft_relay::transfer_states::{PendingRegistration, Transfer};
use near_api_types::AccountId;
use redis::AsyncCommands;
use std::time::{SystemTime, UNIX_EPOCH};

fn create_test_transfer(id: u32) -> Transfer<PendingRegistration> {
    let receiver_id: AccountId = "test.near".parse().unwrap();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    Transfer::new(
        format!("transfer_{}", id),
        receiver_id,
        "1000000".to_string(),
        Some(format!("test memo {}", id)),
        timestamp,
    )
}

#[tokio::test]
async fn test_pop_pending_transfers_race_condition() {
    // Connect to Redis
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let token = "test_token_race";
    let account_id = "test_account.near";
    let pending_key = format!("pending:{}:{}", token, account_id);

    // Clear any existing data
    let _: () = conn.del(&pending_key).await.unwrap();

    println!("\n=== Setting up race condition test ===");

    // Add initial 10 transfers
    println!("Adding 10 initial transfers to pending list...");
    for i in 0..10 {
        let transfer = create_test_transfer(i);
        let _: () = conn
            .rpush(&pending_key, transfer.serialize().unwrap())
            .await
            .unwrap();
    }

    // Spawn a task that continuously adds transfers during the pop operation
    let client_clone = client.clone();
    let pending_key_clone = pending_key.clone();
    let race_task = tokio::spawn(async move {
        let mut conn = client_clone
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        println!("Race task: Starting to add 11 transfers concurrently...");
        for i in 100..111 {
            // Small delay to spread out the additions during the pop operation
            tokio::time::sleep(tokio::time::Duration::from_micros(200)).await;
            let transfer = create_test_transfer(i);
            let _: () = conn
                .rpush(&pending_key_clone, transfer.serialize().unwrap())
                .await
                .unwrap();
        }
        println!("Race task: Finished adding 11 transfers");
    });

    // Give the race task a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;

    // Pop transfers using the potentially buggy implementation
    println!("Main task: Calling pop_pending_transfers...");
    let popped = redis_helpers::pop_pending_transfers(&mut conn, token, account_id)
        .await
        .unwrap();
    println!(
        "Main task: pop_pending_transfers returned {} items",
        popped.len()
    );

    // Wait for race task to finish
    race_task.await.unwrap();

    // Check how many items are left in the list
    let remaining: usize = conn.llen(&pending_key).await.unwrap();

    println!("\n=== Race Condition Test Results ===");
    println!("Transfers popped: {}", popped.len());
    println!("Transfers remaining in Redis: {}", remaining);
    println!("Total accounted for: {}", popped.len() + remaining);
    println!("Expected total: 21 (10 initial + 11 from race task)");

    let lost = 21 - (popped.len() + remaining);
    if lost > 0 {
        println!("\n⚠️  RACE CONDITION DETECTED!");
        println!("Lost {} transfers due to LRANGE+DEL race condition", lost);
        println!("\nWhat happened:");
        println!("1. pop_pending_transfers called LRANGE (read all items)");
        println!("2. Race task added {} new items with RPUSH", lost);
        println!("3. pop_pending_transfers called DEL (deleted everything including new items)");
        println!(
            "4. Those {} new items were deleted without being returned",
            lost
        );
    } else {
        println!("\n✅ No race condition detected (or implementation is fixed)");
    }

    // Clean up
    let _: () = conn.del(&pending_key).await.unwrap();

    // Assert that we expect the race condition with current code
    // This should FAIL with buggy code, showing transfers were lost
    assert_eq!(
        popped.len() + remaining,
        21,
        "\n\n❌ RACE CONDITION CONFIRMED: Lost {} transfers!\n\
         Expected 21 total (10 initial + 11 concurrent), but only {} were accounted for.\n\
         This is the bug we need to fix with atomic Lua script.\n",
        lost,
        popped.len() + remaining
    );
}

#[tokio::test]
async fn test_pop_pending_transfers_no_items() {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let token = "test_token_empty";
    let account_id = "empty_account.near";
    let pending_key = format!("pending:{}:{}", token, account_id);

    // Ensure list is empty
    let _: () = conn.del(&pending_key).await.unwrap();

    // Pop from empty list should return empty vec
    let result = redis_helpers::pop_pending_transfers(&mut conn, token, account_id)
        .await
        .unwrap();

    assert_eq!(result.len(), 0, "Should return empty vec for empty list");
}

#[tokio::test]
async fn test_pop_pending_transfers_basic() {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let client = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let token = "test_token_basic";
    let account_id = "basic_account.near";
    let pending_key = format!("pending:{}:{}", token, account_id);

    // Clear and add 5 transfers
    let _: () = conn.del(&pending_key).await.unwrap();
    for i in 0..5 {
        let transfer = create_test_transfer(i);
        let _: () = conn
            .rpush(&pending_key, transfer.serialize().unwrap())
            .await
            .unwrap();
    }

    // Pop all transfers
    let result = redis_helpers::pop_pending_transfers(&mut conn, token, account_id)
        .await
        .unwrap();

    assert_eq!(result.len(), 5, "Should return all 5 transfers");

    // List should be empty after pop
    let remaining: usize = conn.llen(&pending_key).await.unwrap();
    assert_eq!(remaining, 0, "List should be empty after pop");
}
