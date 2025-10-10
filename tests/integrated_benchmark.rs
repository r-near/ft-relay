/// Fully integrated benchmark test
///
/// This test:
/// 1. Starts near-sandbox programmatically
/// 2. Deploys the FT contract
/// 3. Starts the relay API server (with registration workers)
/// 4. Runs benchmark with actual FT transfers
/// 5. Verifies balances changed correctly (registration happens automatically)
use ft_relay::{RedisSettings, RelayConfig};
use near_crypto::SecretKey;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::{
    query::QueryResponseKind, transactions::RpcSendTransactionRequest,
};
use near_primitives::account::AccessKeyPermission;
use near_primitives::action::{Action, AddKeyAction, DeployContractAction, FunctionCallAction};
use near_primitives::transaction::{SignedTransaction, Transaction, TransactionV0};
use near_primitives::types::{BlockReference, Finality};
use near_primitives::views::TxExecutionStatus;
use near_sandbox::{GenesisAccount, Sandbox, SandboxConfig};
use serde_json::json;
use std::error::Error;
use std::time::{Duration, Instant};

const FT_WASM_PATH: &str = "resources/fungible_token.wasm";
const YOCTO_PER_TRANSFER: u128 = 1_000_000_000_000_000_000;

/// Flush Redis before test
async fn flush_redis() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    println!("Flushing Redis at {}...", redis_url);
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    redis::cmd("FLUSHALL")
        .query_async::<()>(&mut redis_conn)
        .await?;
    println!("✅ Redis flushed");
    Ok(())
}

/// Deploy and initialize the FT contract
async fn setup_ft_contract(
    sandbox: &Sandbox,
    owner: &GenesisAccount,
) -> Result<(), Box<dyn std::error::Error>> {
    let wasm_bytes = std::fs::read(FT_WASM_PATH)?;

    let client = JsonRpcClient::connect(&sandbox.rpc_addr);
    let secret_key: SecretKey = owner.private_key.parse()?;

    // Get block hash
    let block_request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };
    let block = client.call(block_request).await?;
    let block_hash = block.header.hash;

    // Get nonce
    let access_key_request = methods::query::RpcQueryRequest {
        block_reference: BlockReference::Finality(Finality::Final),
        request: near_primitives::views::QueryRequest::ViewAccessKey {
            account_id: owner.account_id.clone(),
            public_key: secret_key.public_key(),
        },
    };
    let access_key_response = client.call(access_key_request).await?;
    let nonce = match access_key_response.kind {
        QueryResponseKind::AccessKey(access_key) => access_key.nonce + 1,
        _ => return Err("Unexpected response type".into()),
    };

    // Deploy + initialize in one transaction
    let deploy_action = Action::DeployContract(DeployContractAction { code: wasm_bytes });

    let init_args = json!({
        "owner_id": owner.account_id,
        "total_supply": "1000000000000000000000000000", // 1B tokens
        "metadata": {
            "spec": "ft-1.0.0",
            "name": "Test Token",
            "symbol": "TEST",
            "decimals": 18
        }
    })
    .to_string()
    .into_bytes();

    let init_action = Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "new".to_string(),
        args: init_args,
        gas: 50_000_000_000_000, // 50 Tgas
        deposit: 0,
    }));

    let transaction = Transaction::V0(TransactionV0 {
        signer_id: owner.account_id.clone(),
        public_key: secret_key.public_key(),
        nonce,
        receiver_id: owner.account_id.clone(),
        block_hash,
        actions: vec![deploy_action, init_action],
    });

    let signature = secret_key.sign(transaction.get_hash_and_size().0.as_ref());
    let signed_tx = SignedTransaction::new(signature, transaction);

    let broadcast_request = RpcSendTransactionRequest {
        signed_transaction: signed_tx,
        wait_until: TxExecutionStatus::Final,
    };
    let response = client.call(broadcast_request).await?;

    // Check if transaction succeeded - final_execution_outcome contains the actual execution status
    if let Some(outcome) = response.final_execution_outcome {
        match outcome.into_outcome().status {
            near_primitives::views::FinalExecutionStatus::SuccessValue(_) => {
                // Wait a bit for state to settle
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                Ok(())
            }
            status => Err(format!("Transaction failed: {:?}", status).into()),
        }
    } else {
        Err("No execution outcome returned".into())
    }
}

#[tokio::test]
#[ignore] // Run with: cargo test --test integrated_benchmark bounty -- --ignored --nocapture
async fn test_bounty_requirement_60k() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(
        env_logger::Env::default()
            .default_filter_or("info,ft_relay=info,near_api=warn,tracing::span=warn"),
    )
    .is_test(true)
    .try_init()
    .ok();
    flush_redis().await?;

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  BOUNTY REQUIREMENT TEST: 60,000 Transfers in 10 Minutes  ║");
    println!("║  Target: ≥100 transfers/second sustained for 10 minutes   ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Start sandbox with multiple receivers
    let ft_owner = GenesisAccount::generate_with_name("ft.sandbox".parse()?);

    // Create a modest receiver set for predictable distribution
    let receiver_count: usize = 20;
    let mut receivers = Vec::new();
    println!("Generating {} receiver accounts...", receiver_count);
    for i in 0..receiver_count {
        receivers.push(GenesisAccount::generate_with_name(
            format!("recv{}.sandbox", i).parse()?,
        ));
    }

    let mut accounts = vec![ft_owner.clone()];
    accounts.extend(receivers.clone());

    println!("Starting sandbox with {} accounts...", accounts.len());
    let sandbox = Sandbox::start_sandbox_with_config(SandboxConfig {
        additional_accounts: accounts,
        ..Default::default()
    })
    .await?;

    println!("✅ Sandbox started at {}", sandbox.rpc_addr);

    // Deploy and setup
    setup_ft_contract(&sandbox, &ft_owner).await?;
    println!("✅ FT contract deployed and initialized");

    // Generate multiple access keys for better throughput (real-world usage pattern)
    println!("\nGenerating 3 access keys for key pooling...");
    let mut secret_keys = vec![ft_owner.private_key.to_string()];

    // Generate 2 new keypairs upfront
    let new_keys: Vec<SecretKey> = (0..2)
        .map(|_| SecretKey::from_random(near_crypto::KeyType::ED25519))
        .collect();

    println!("Adding 2 new access keys in a single batch transaction...");
    for (i, key) in new_keys.iter().enumerate() {
        println!("  Key {}: {}", i + 1, key.public_key());
    }

    let client = JsonRpcClient::connect(&sandbox.rpc_addr);
    let owner_secret_key: SecretKey = ft_owner.private_key.parse()?;

    // Get initial nonce
    let access_key_request = methods::query::RpcQueryRequest {
        block_reference: BlockReference::Finality(Finality::Final),
        request: near_primitives::views::QueryRequest::ViewAccessKey {
            account_id: ft_owner.account_id.clone(),
            public_key: owner_secret_key.public_key(),
        },
    };
    let access_key_response = client.call(access_key_request).await?;
    let current_nonce = match access_key_response.kind {
        QueryResponseKind::AccessKey(access_key) => access_key.nonce + 1,
        _ => return Err("Unexpected response type".into()),
    };

    // Get block hash
    let block_request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };
    let block = client.call(block_request).await?;
    let block_hash = block.header.hash;

    // Create AddKey actions for all new keys
    let add_key_actions: Vec<Action> = new_keys
        .iter()
        .map(|key| {
            Action::AddKey(Box::new(AddKeyAction {
                public_key: key.public_key(),
                access_key: near_primitives::account::AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FullAccess,
                },
            }))
        })
        .collect();

    // Submit all keys in a single transaction
    let transaction = Transaction::V0(TransactionV0 {
        signer_id: ft_owner.account_id.clone(),
        public_key: owner_secret_key.public_key(),
        nonce: current_nonce,
        receiver_id: ft_owner.account_id.clone(),
        block_hash,
        actions: add_key_actions,
    });

    let signature = owner_secret_key.sign(transaction.get_hash_and_size().0.as_ref());
    let signed_tx = SignedTransaction::new(signature, transaction);

    let broadcast_request = RpcSendTransactionRequest {
        signed_transaction: signed_tx,
        wait_until: TxExecutionStatus::Final,
    };
    let response = client.call(broadcast_request).await?;

    if let Some(outcome) = response.final_execution_outcome {
        match outcome.into_outcome().status {
            near_primitives::views::FinalExecutionStatus::SuccessValue(_) => {
                println!("  ✅ All keys added successfully in batch");
                // Add all new keys to the pool
                for key in new_keys {
                    secret_keys.push(key.to_string());
                }
            }
            status => {
                return Err(format!("Batch add key transaction failed: {:?}", status).into());
            }
        }
    } else {
        return Err("No execution outcome returned for batch add key transaction".into());
    }

    // Verify all keys are actually on-chain before proceeding
    println!(
        "\nVerifying all {} access keys are on-chain...",
        secret_keys.len()
    );
    for (idx, key_str) in secret_keys.iter().enumerate() {
        let verify_key: SecretKey = key_str.parse()?;
        let verify_request = methods::query::RpcQueryRequest {
            block_reference: BlockReference::Finality(Finality::Final),
            request: near_primitives::views::QueryRequest::ViewAccessKey {
                account_id: ft_owner.account_id.clone(),
                public_key: verify_key.public_key(),
            },
        };

        match client.call(verify_request).await {
            Ok(resp) => {
                if let QueryResponseKind::AccessKey(_) = resp.kind {
                    println!("  ✅ Key {}: {} verified", idx + 1, verify_key.public_key());
                }
            }
            Err(e) => {
                return Err(
                    format!("Failed to verify key {}: {:?}", verify_key.public_key(), e).into(),
                );
            }
        }
    }

    println!(
        "✅ All {} access keys configured and verified",
        secret_keys.len()
    );

    // Start relay server with optimized config for high throughput
    let redis = test_redis_settings();

    let config = RelayConfig {
        token: ft_owner.account_id.to_string(),
        account_id: ft_owner.account_id.to_string(),
        secret_keys,
        rpc_url: sandbox.rpc_addr.clone(),
        batch_linger_ms: 1000,       // Fast batching
        batch_submit_delay_ms: 0,    // No throttling needed for sandbox
        max_inflight_batches: 500,   // High concurrency
        max_workers: 3,              // 3 transfer workers
        max_registration_workers: 5, // 5 registration workers (more uniform flow for dupes)
        max_verification_workers: 1, // 1 verification worker
        bind_addr: "127.0.0.1:18082".to_string(),
        redis,
    };

    println!("\nServer Configuration:");
    println!("  Batch linger: {}ms", config.batch_linger_ms);
    println!("  Max inflight batches: {}", config.max_inflight_batches);
    println!("  Access keys: {}", config.secret_keys.len());

    let server_handle = tokio::spawn(async move {
        match ft_relay::run(config).await {
            Ok(_) => println!("Server exited normally"),
            Err(e) => {
                eprintln!("\n❌ Relay server error: {:?}", e);
                panic!("Server failed: {:?}", e);
            }
        }
    });

    // Wait for server to be ready with health check
    let client = reqwest::Client::new();
    println!("Waiting for server to be ready...");
    for attempt in 1..=10 {
        match client.get("http://127.0.0.1:18082/health").send().await {
            Ok(r) if r.status().is_success() => {
                println!("✅ Server is ready (attempt {})\n", attempt);
                break;
            }
            _ if attempt < 10 => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            _ => {
                panic!("Server health check failed after 10 attempts");
            }
        }
    }

    // Send 60,000 transfer requests
    let total_requests = 60_000;
    let concurrent_workers = 100; // Number of parallel HTTP workers (reduced to avoid overwhelming server)

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  Starting benchmark: {} transfers", total_requests);
    println!("║  Concurrent HTTP workers: {}", concurrent_workers);
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let api_start = Instant::now();
    let mut tasks = Vec::new();
    let batch_size = total_requests / concurrent_workers;

    for worker_id in 0..concurrent_workers {
        let client = client.clone();
        let receivers = receivers.clone();
        let start_idx = worker_id * batch_size;
        let end_idx = if worker_id == concurrent_workers - 1 {
            total_requests
        } else {
            start_idx + batch_size
        };

        let api_started_at = api_start;
        let task = tokio::spawn(async move {
            let mut worker_success = 0;
            let mut worker_errors = 0;
            for i in start_idx..end_idx {
                if worker_id == 0 && i % 1000 == 0 {
                    println!(
                        "Worker {} at request {}/{}",
                        worker_id,
                        i - start_idx,
                        end_idx - start_idx
                    );
                }
                let receiver_id = receivers[i % receiver_count].account_id.clone();

                // Generate unique idempotency key for this request
                let idempotency_key = uuid::Uuid::new_v4().to_string();

                match client
                    .post("http://127.0.0.1:18082/v1/transfer")
                    .header("X-Idempotency-Key", &idempotency_key)
                    .json(&json!({
                        "receiver_id": receiver_id,
                        "amount": "1000000000000000000" // 1 token each
                    }))
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => worker_success += 1,
                    Ok(r) => {
                        if worker_id == 0 && worker_success == 0 {
                            eprintln!(
                                "❌ Worker {} request failed with status {}: {:?}",
                                worker_id,
                                r.status(),
                                r.text().await
                            );
                        }
                    }
                    Err(e) => {
                        worker_errors += 1;
                        if worker_id == 0 && worker_errors < 5 {
                            eprintln!("❌ Worker {} request error: {:?}", worker_id, e);
                            if let Some(source) = e.source() {
                                eprintln!("   Caused by: {:?}", source);
                            }
                        }
                    }
                }

                // Print progress every 10k requests
                if i > 0 && i % 10_000 == 0 && worker_id == 0 {
                    let elapsed = api_started_at.elapsed();
                    let current_rate = i as f64 / elapsed.as_secs_f64();
                    println!(
                        "  Progress: {} requests in {:?} ({:.1} req/sec)",
                        i, elapsed, current_rate
                    );
                }
            }
            (worker_success, worker_errors)
        });
        tasks.push(task);
    }

    let mut total_success = 0;
    let mut total_errors = 0;
    for task in tasks {
        let (success, errors) = task.await.unwrap_or((0, 0));
        total_success += success;
        total_errors += errors;
    }

    let api_end = Instant::now();
    let api_elapsed = api_end
        .checked_duration_since(api_start)
        .unwrap_or_else(|| Duration::from_secs(0));
    let api_duration_secs = api_elapsed.as_secs_f64();
    let api_throughput = if api_duration_secs > 0.0 {
        total_success as f64 / api_duration_secs
    } else {
        0.0
    };

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  HTTP REQUEST RESULTS                                      ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("  Total requests:  {}", total_requests);
    println!("  Accepted:        {}", total_success);
    println!("  Errors:          {}", total_errors);
    println!(
        "  Other:           {}",
        total_requests - total_success - total_errors
    );
    println!("  API duration:    {:.2}s", api_duration_secs);
    println!("  API throughput:  {:.2} req/sec", api_throughput);
    println!("  Target:          ≥100 req/sec");

    if api_throughput >= 100.0 {
        println!("  Status:          ✅ PASSED");
    } else {
        println!("  Status:          ❌ FAILED");
    }

    // Check if server is still running
    if server_handle.is_finished() {
        panic!("❌ Server task died during benchmark!");
    }

    // Verify Redis received all accepted requests
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  REDIS QUEUE VERIFICATION                                  ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    // Wait for all HTTP requests to be added to Redis AND
    // for registration workers AND transfer workers to finish processing
    println!("  Waiting for all workers to complete processing...");

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;

    // Poll until count stabilizes (indicating all workers finished)
    let mut prev_count = 0u64;
    let mut stable_count = 0;
    for _ in 0..60 {
        // Max 60 seconds
        tokio::time::sleep(Duration::from_secs(1)).await;

        let stream_info: redis::Value = redis::cmd("XINFO")
            .arg("STREAM")
            .arg("ftrelay:sandbox:verify")
            .query_async(&mut redis_conn)
            .await?;

        let mut entries_added = 0u64;
        if let redis::Value::Array(info) = stream_info {
            for i in (0..info.len()).step_by(2) {
                if let Some(redis::Value::BulkString(key)) = info.get(i) {
                    if key == b"entries-added" {
                        if let Some(redis::Value::Int(count)) = info.get(i + 1) {
                            entries_added = *count as u64;
                            break;
                        }
                    }
                }
            }
        }

        if entries_added == prev_count {
            stable_count += 1;
            if stable_count >= 3 {
                // Stable for 3 seconds
                println!("  Count stabilized at {} entries", entries_added);
                break;
            }
        } else {
            stable_count = 0;
        }
        prev_count = entries_added;

        if entries_added >= total_success as u64 {
            println!("  All {} entries accounted for!", total_success);
            break;
        }
    }

    // Get stream info to see total entries added
    let stream_info: redis::Value = redis::cmd("XINFO")
        .arg("STREAM")
        .arg("ftrelay:sandbox:verify")
        .query_async(&mut redis_conn)
        .await?;

    let mut entries_added = 0u64;
    if let redis::Value::Array(info) = stream_info {
        for i in (0..info.len()).step_by(2) {
            if let Some(redis::Value::BulkString(key)) = info.get(i) {
                if key == b"entries-added" {
                    if let Some(redis::Value::Int(count)) = info.get(i + 1) {
                        entries_added = *count as u64;
                        break;
                    }
                }
            }
        }
    }

    let pending_count: u64 = redis::cmd("XLEN")
        .arg("ftrelay:sandbox:verify")
        .query_async(&mut redis_conn)
        .await?;

    println!("  HTTP 200 responses:  {}", total_success);
    println!("  Redis entries added: {}", entries_added);
    println!("  Currently pending:   {}", pending_count);

    if entries_added != total_success as u64 {
        println!("  Status:              ❌ MISMATCH");
        panic!(
            "❌ Redis verification failed: {} HTTP requests accepted but only {} entries in Redis (lost {} transfers)",
            total_success,
            entries_added,
            total_success as u64 - entries_added
        );
    } else {
        println!("  Status:              ✅ VERIFIED");
    }

    println!("\n⏳ Waiting for NEAR to finalize balances...");

    let client = JsonRpcClient::connect(&sandbox.rpc_addr);

    let expected_total = total_requests as u128 * YOCTO_PER_TRANSFER;
    let poll_interval = Duration::from_secs(3);
    let max_polls = 60; // ~3 minutes max wait

    let mut final_balances = Vec::new();
    let mut final_total: u128 = 0;
    let mut final_success_rate = 0.0;
    let mut completion_instant: Option<Instant> = None;

    for poll in 1..=max_polls {
        let mut balances = Vec::with_capacity(receiver_count);
        let mut total_balance: u128 = 0;

        for receiver in &receivers {
            let balance_args = json!({
                "account_id": receiver.account_id
            })
            .to_string()
            .into_bytes();

            let call_request = methods::query::RpcQueryRequest {
                block_reference: BlockReference::Finality(Finality::Final),
                request: near_primitives::views::QueryRequest::CallFunction {
                    account_id: ft_owner.account_id.clone(),
                    method_name: "ft_balance_of".to_string(),
                    args: near_primitives::types::FunctionArgs::from(balance_args),
                },
            };

            let response = client.call(call_request).await?;
            let balance: u128 = match response.kind {
                QueryResponseKind::CallResult(result) => {
                    let balance_str: String = serde_json::from_slice(&result.result)?;
                    balance_str.parse().unwrap_or(0)
                }
                _ => 0,
            };

            total_balance += balance;
            balances.push(balance);
        }

        final_success_rate = (total_balance as f64 / expected_total as f64) * 100.0;

        println!(
            "  Poll {}: {} tokens ({:.2}% complete)",
            poll,
            total_balance / YOCTO_PER_TRANSFER,
            final_success_rate
        );

        if total_balance >= expected_total {
            println!(
                "  ✅ Target reached after {} polls (~{}s)",
                poll,
                poll * poll_interval.as_secs()
            );
            final_balances = balances;
            final_total = total_balance;
            completion_instant = Some(Instant::now());
            break;
        }

        if poll == max_polls {
            println!(
                "  ⚠️  Reached polling cap (~{}s); proceeding with current totals",
                poll * poll_interval.as_secs()
            );
            final_balances = balances;
            final_total = total_balance;
            completion_instant = Some(Instant::now());
            break;
        }

        tokio::time::sleep(poll_interval).await;
    }

    let expected_per_receiver = (total_requests / receiver_count) as u128 * YOCTO_PER_TRANSFER;

    let completion_instant = completion_instant.unwrap_or_else(Instant::now);
    let onchain_elapsed = completion_instant
        .checked_duration_since(api_start)
        .unwrap_or_else(|| Duration::from_secs(0));
    let onchain_duration_secs = onchain_elapsed.as_secs_f64();
    let post_api_elapsed = completion_instant
        .checked_duration_since(api_end)
        .unwrap_or_else(|| Duration::from_secs(0));
    let post_api_duration_secs = post_api_elapsed.as_secs_f64();
    let completed_transfers = (final_total / YOCTO_PER_TRANSFER) as usize;
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
    println!(
        "║  ON-CHAIN VERIFICATION (all {} receivers)               ║",
        receiver_count
    );
    println!("╚════════════════════════════════════════════════════════════╝");
    println!(
        "  Expected total:     {} tokens",
        expected_total / YOCTO_PER_TRANSFER
    );
    println!(
        "  Actual total:       {} tokens",
        final_total / YOCTO_PER_TRANSFER
    );
    println!("  Success rate:       {:.2}%", final_success_rate);
    println!(
        "  Blockchain time:    {:.2}s (from first request)",
        onchain_duration_secs
    );
    println!(
        "  Settlement lag:     {:.2}s after API completion",
        post_api_duration_secs
    );
    println!(
        "  Blockchain throughput {:.2} tx/sec (completed)",
        blockchain_throughput
    );
    if post_api_duration_secs > 0.0 {
        println!("  Post-API throughput {:.2} tx/sec", settlement_throughput);
    }

    println!("\n  Receiver breakdown:");
    for (idx, (receiver, balance)) in receivers.iter().zip(final_balances.iter()).enumerate() {
        println!(
            "    {:>2}: {:<20} {} tokens (expected: {})",
            idx,
            receiver.account_id,
            balance / YOCTO_PER_TRANSFER,
            expected_per_receiver / YOCTO_PER_TRANSFER
        );
    }

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  FINAL BOUNTY REQUIREMENT CHECK                            ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("  ✓ Total transfers:   {} / 60,000", total_success);
    println!(
        "  ✓ API throughput:    {:.2} / ≥100 req/sec",
        api_throughput
    );
    println!(
        "  ✓ API duration:      {:.2}s / ≤600s (10 min)",
        api_duration_secs
    );
    println!(
        "  - On-chain throughput {:.2} tx/sec",
        blockchain_throughput
    );
    let http_success_rate = (total_success as f64 / total_requests as f64) * 100.0;
    println!("  ✓ HTTP success:      {:.2}%", http_success_rate);
    println!("  ✓ On-chain success:  {:.2}%", final_success_rate);

    // Assert bounty requirements
    assert!(
        total_success >= 60_000,
        "❌ Failed: Only {} of 60,000 requests accepted",
        total_success
    );

    assert!(
        api_throughput >= 100.0,
        "❌ Failed: Throughput {:.2} req/sec is below 100 req/sec requirement",
        api_throughput
    );

    assert!(
        api_elapsed.as_secs() <= 600,
        "❌ Failed: Took {:.2}s, should complete within 600s (10 minutes)",
        api_duration_secs
    );

    assert!(
        final_success_rate >= 100.0,
        "❌ Failed: Only {:.2}% of transactions confirmed on-chain (expected 100%)",
        final_success_rate
    );

    println!("\n🎉 ╔════════════════════════════════════════════════════════════╗");
    println!("   ║  BOUNTY REQUIREMENTS: ✅ PASSED                            ║");
    println!(
        "   ║  Successfully handled 60,000 transfers at {:.0}+ req/sec     ║",
        api_throughput
    );
    println!("   ║  All requirements satisfied!                               ║");
    println!("   ╚════════════════════════════════════════════════════════════╝");

    // Emit benchmark results in parseable format for CI
    println!("\n--- BENCHMARK RESULTS (SANDBOX) ---");
    println!("test_name: test_bounty_requirement_60k");
    println!("transfers: {}", total_requests);
    println!("api_duration_secs: {:.2}", api_duration_secs);
    println!("duration_secs: {:.2}", api_duration_secs);
    println!("throughput_req_per_sec: {:.2}", api_throughput);
    println!("blockchain_completion_secs: {:.2}", onchain_duration_secs);
    println!(
        "blockchain_throughput_req_per_sec: {:.2}",
        blockchain_throughput
    );
    println!("http_success_rate: {:.2}", http_success_rate);
    println!("onchain_success_rate: {:.2}", final_success_rate);
    println!("status: PASSED");
    println!("--- END BENCHMARK RESULTS ---");

    Ok(())
}
fn test_redis_settings() -> RedisSettings {
    RedisSettings::new(
        "redis://127.0.0.1:6379",
        "unused",  // Old stream names not used anymore
        "unused",
        "unused",
        "unused",
    )
}
