use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use dotenv::dotenv;
use ft_relay::{RedisSettings, RelayConfig};
use near_crypto::SecretKey;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::{
    query::QueryResponseKind, transactions::RpcSendTransactionRequest,
};
use near_primitives::account::AccessKeyPermission;
use near_primitives::action::{Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, TransferAction};
use near_primitives::transaction::{SignedTransaction, Transaction, TransactionV0};
use near_primitives::types::{AccountId, BlockReference, Finality};
use near_primitives::views::TxExecutionStatus;
use serde_json::json;

const TRANSFER_AMOUNT: &str = "1000000000000000000"; // 1 token
const MEGA_BENCH_REQUESTS: usize = 60_000;
const MEGA_BENCH_CONCURRENCY: usize = 100;
const YOCTO_PER_TRANSFER: u128 = 1_000_000_000_000_000_000;

fn test_redis_settings() -> RedisSettings {
    RedisSettings::new(
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
        "unused",
        "unused",
        "unused",
        "unused",
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
    let redis_client = redis::Client::open(redis_url.clone())?;
    let mut redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    redis::cmd("FLUSHALL")
        .query_async::<()>(&mut redis_conn)
        .await?;
    println!("✅ Redis flushed\n");

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  TESTNET 60K BENCHMARK: 60,000 Transfers in 10 Minutes    ║");
    println!("║  Target: ≥100 transfers/second sustained                   ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Load configuration from environment (same as main application)
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL must be set");

    let owner_account_id: AccountId = std::env::var("ACCOUNT_ID")
        .expect("ACCOUNT_ID must be set")
        .parse()?;

    let token_account_id: AccountId = std::env::var("TOKEN")
        .expect("TOKEN must be set (FT contract address)")
        .parse()?;

    let private_keys_str =
        std::env::var("PRIVATE_KEYS").expect("PRIVATE_KEYS must be set (comma-separated list)");

    println!("Account:  {}", owner_account_id);
    println!("Token:    {}", token_account_id);
    println!("RPC:      {}", rpc_url);

    // Parse secret keys
    let mut secret_keys = Vec::new();
    for key_str in private_keys_str.split(',') {
        let key_str = key_str.trim();
        if key_str.is_empty() {
            continue;
        }
        key_str.parse::<SecretKey>()?; // Validate
        secret_keys.push(key_str.to_string());
    }

    ensure!(!secret_keys.is_empty(), "No PRIVATE_KEYS provided");
    println!("Keys:     {} loaded\n", secret_keys.len());

    let owner_secret = secret_keys[0].parse::<SecretKey>()?;
    let owner_public = owner_secret.public_key();

    // Get current nonce for creating receiver accounts
    let client = JsonRpcClient::connect(&rpc_url);

    let access_key_request = methods::query::RpcQueryRequest {
        block_reference: BlockReference::Finality(Finality::Final),
        request: near_primitives::views::QueryRequest::ViewAccessKey {
            account_id: owner_account_id.clone(),
            public_key: owner_public.clone(),
        },
    };
    let access_key_response = client.call(access_key_request).await?;
    let mut nonce = match access_key_response.kind {
        QueryResponseKind::AccessKey(access_key) => access_key.nonce + 1,
        _ => return Err(anyhow::anyhow!("Unexpected response type")),
    };

    // Create 5 receiver accounts with random suffix to avoid conflicts
    println!("Creating 5 receiver accounts...");
    let receiver_count = 5;
    let mut receivers = Vec::new();

    // Add randomness to avoid account name conflicts between test runs
    let test_run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        % 1_000_000; // Last 6 digits of timestamp

    let block_request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };
    let block = client.call(block_request).await?;
    let mut block_hash = block.header.hash;

    for i in 0..receiver_count {
        let receiver_id: AccountId =
            format!("r{}-{}.{}", test_run_id, i, owner_account_id.as_str()).parse()?;
        let receiver_secret = SecretKey::from_random(near_crypto::KeyType::ED25519);
        let receiver_public = receiver_secret.public_key();

        let create_actions = vec![
            Action::CreateAccount(CreateAccountAction {}),
            Action::Transfer(TransferAction {
                deposit: 200_000_000_000_000_000_000_000, // 0.2 NEAR
            }),
            Action::AddKey(Box::new(AddKeyAction {
                public_key: receiver_public.clone(),
                access_key: near_primitives::account::AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FullAccess,
                },
            })),
        ];

        let transaction = Transaction::V0(TransactionV0 {
            signer_id: owner_account_id.clone(),
            public_key: owner_public.clone(),
            nonce,
            receiver_id: receiver_id.clone(),
            block_hash,
            actions: create_actions,
        });

        let signature = owner_secret.sign(transaction.get_hash_and_size().0.as_ref());
        let signed_tx = SignedTransaction::new(signature, transaction);

        let broadcast_request = RpcSendTransactionRequest {
            signed_transaction: signed_tx,
            wait_until: TxExecutionStatus::Final,
        };
        let response = client.call(broadcast_request).await?;

        if let Some(outcome) = response.final_execution_outcome {
            match outcome.into_outcome().status {
                near_primitives::views::FinalExecutionStatus::SuccessValue(_) => {
                    println!("  ✅ Created receiver: {}", receiver_id);
                }
                status => return Err(anyhow::anyhow!("Create account failed: {:?}", status)),
            }
        }

        receivers.push(receiver_id);
        nonce += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Refresh block hash every few transactions
        if i % 3 == 2 {
            let block_request = methods::block::RpcBlockRequest {
                block_reference: BlockReference::Finality(Finality::Final),
            };
            let block = client.call(block_request).await?;
            block_hash = block.header.hash;
        }
    }

    println!("✅ Setup complete, starting benchmark...\n");

    // Run the benchmark
    let result = run_60k_benchmark(
        &rpc_url,
        &owner_account_id,
        &token_account_id,
        secret_keys,
        receivers.clone(),
        &mut redis_conn,
    )
    .await;

    // Clean up receiver accounts (always runs, even if test failed)
    println!("\n🧹 Cleaning up receiver accounts...");
    let cleanup_result = cleanup_receiver_accounts(
        &client,
        &owner_account_id,
        &owner_secret,
        &owner_public,
        &receivers,
    )
    .await;
    
    match cleanup_result {
        Ok(count) => println!("✅ Deleted {} receiver accounts", count),
        Err(e) => println!("⚠️  Cleanup warning: {:?}", e),
    }

    result
}

async fn cleanup_receiver_accounts(
    client: &JsonRpcClient,
    owner_account_id: &AccountId,
    owner_secret: &SecretKey,
    owner_public: &near_crypto::PublicKey,
    receivers: &[AccountId],
) -> Result<usize> {
    if receivers.is_empty() {
        return Ok(0);
    }

    // Get current nonce
    let access_key_request = methods::query::RpcQueryRequest {
        block_reference: BlockReference::Finality(Finality::Final),
        request: near_primitives::views::QueryRequest::ViewAccessKey {
            account_id: owner_account_id.clone(),
            public_key: owner_public.clone(),
        },
    };
    let access_key_response = client.call(access_key_request).await?;
    let _nonce = match access_key_response.kind {
        QueryResponseKind::AccessKey(access_key) => access_key.nonce + 1,
        _ => return Err(anyhow::anyhow!("Unexpected response type")),
    };

    // Get block hash
    let block_request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };
    let block = client.call(block_request).await?;
    let mut block_hash = block.header.hash;

    let mut deleted = 0;
    
    // Delete accounts one by one
    for (idx, receiver_id) in receivers.iter().enumerate() {
        // DeleteAccount action transfers remaining balance to beneficiary
        let delete_action = Action::DeleteAccount(DeleteAccountAction {
            beneficiary_id: owner_account_id.clone(),
        });

        let transaction = Transaction::V0(TransactionV0 {
            signer_id: receiver_id.clone(),
            public_key: owner_public.clone(),
            nonce: 1, // These are new accounts, so nonce starts at 1
            receiver_id: receiver_id.clone(),
            block_hash,
            actions: vec![delete_action],
        });

        let signature = owner_secret.sign(transaction.get_hash_and_size().0.as_ref());
        let signed_tx = SignedTransaction::new(signature, transaction);

        let broadcast_request = RpcSendTransactionRequest {
            signed_transaction: signed_tx,
            wait_until: TxExecutionStatus::Final,
        };

        match client.call(broadcast_request).await {
            Ok(response) => {
                if let Some(outcome) = response.final_execution_outcome {
                    match outcome.into_outcome().status {
                        near_primitives::views::FinalExecutionStatus::SuccessValue(_) => {
                            deleted += 1;
                            println!("  🗑️  Deleted: {}", receiver_id);
                        }
                        status => {
                            println!("  ⚠️  Failed to delete {}: {:?}", receiver_id, status);
                        }
                    }
                }
            }
            Err(e) => {
                println!("  ⚠️  Failed to delete {}: {:?}", receiver_id, e);
            }
        }

        // Refresh block hash every few deletions
        if idx % 3 == 2 && idx + 1 < receivers.len() {
            let block_request = methods::block::RpcBlockRequest {
                block_reference: BlockReference::Finality(Finality::Final),
            };
            if let Ok(block) = client.call(block_request).await {
                block_hash = block.header.hash;
            }
        }

        // Small delay between deletions
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(deleted)
}

async fn run_60k_benchmark(
    rpc_url: &str,
    owner_account_id: &AccountId,
    token_account_id: &AccountId,
    secret_keys: Vec<String>,
    receivers: Vec<AccountId>,
    redis_conn: &mut redis::aio::ConnectionManager,
) -> Result<()> {
    // Allocate a bind address for the relay server
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let bind_addr = listener.local_addr()?;
    drop(listener);
    let bind_addr_str = format!("127.0.0.1:{}", bind_addr.port());

    let redis = test_redis_settings();

    let config = RelayConfig {
        token: token_account_id.to_string(),
        account_id: owner_account_id.to_string(),
        secret_keys,
        rpc_url: rpc_url.to_string(),
        batch_linger_ms: 1000,
        batch_submit_delay_ms: 0,
        max_inflight_batches: 1000,
        max_workers: 30,
        max_registration_workers: 1,
        max_verification_workers: 10,
        bind_addr: bind_addr_str.clone(),
        redis: redis.clone(),
    };

    println!("Server Configuration:");
    println!("  Batch linger: {}ms", config.batch_linger_ms);
    println!("  Max inflight batches: {}", config.max_inflight_batches);
    println!("  Access keys: {}\n", config.secret_keys.len());

    let relay_handle = tokio::spawn(async move {
        if let Err(err) = ft_relay::run(config).await {
            eprintln!("❌ Relay server error: {err:?}");
        }
    });

    // Wait for server health
    let health_client = reqwest::Client::new();
    println!("Waiting for server to be ready...");
    for attempt in 1..=20 {
        match health_client
            .get(format!("http://{}/health", bind_addr_str))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                println!("✅ Server is ready (attempt {})\n", attempt);
                break;
            }
            _ if attempt < 20 => tokio::time::sleep(Duration::from_millis(500)).await,
            _ => panic!("Server health check failed after 20 attempts"),
        }
    }

    let endpoint = format!("http://{}/v1/transfer", bind_addr_str);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  Starting benchmark: {} transfers", MEGA_BENCH_REQUESTS);
    println!("║  Concurrent HTTP workers: {}", MEGA_BENCH_CONCURRENCY);
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let api_start = Instant::now();
    let requests_per_worker = MEGA_BENCH_REQUESTS / MEGA_BENCH_CONCURRENCY;
    let receiver_count = receivers.len();

    let tasks: Vec<_> = (0..MEGA_BENCH_CONCURRENCY)
        .map(|worker_id| {
            let client = client.clone();
            let endpoint = endpoint.clone();
            let receivers = receivers.clone();
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
                    let receiver_id = &receivers[i % receiver_count];
                    let payload = json!({
                        "receiver_id": receiver_id,
                        "amount": TRANSFER_AMOUNT,
                    });
                    let idempotency_key = uuid::Uuid::new_v4().to_string();

                    match client
                        .post(&endpoint)
                        .header("X-Idempotency-Key", &idempotency_key)
                        .json(&payload)
                        .send()
                        .await
                    {
                        Ok(resp) if resp.status().is_success() => worker_success += 1,
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

    // Verify Redis
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  REDIS QUEUE VERIFICATION                                  ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("  Verifying transfer state keys...");

    let expected = accepted as u64;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut cursor: u64 = 0;
        let mut state_count: u64 = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("transfer:*")
                .arg("COUNT")
                .arg(2000)
                .query_async(redis_conn)
                .await?;
            state_count += keys.iter().filter(|k| !k.ends_with(":ev")).count() as u64;
            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }
        if state_count >= expected {
            println!("  All {} transfer states observed!", expected);
            break;
        }
    }

    // Wait for transfer worker to finish processing
    println!("\n⏳ Waiting for transfer worker to finish...");
    let xfer_stream = "ftrelay:testnet:xfer";
    let xfer_group = "ftrelay:testnet:xfer_workers";

    for _ in 0..240 {
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Check stream length (pending messages in stream)
        let stream_len: u64 = redis::cmd("XLEN")
            .arg(xfer_stream)
            .query_async(redis_conn)
            .await
            .unwrap_or(0);

        // Check consumer group pending count
        let mut group_pending: u64 = 0;
        if let Ok(groups) = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(xfer_stream)
            .query_async::<redis::Value>(redis_conn)
            .await
        {
            if let redis::Value::Array(items) = groups {
                for grp in items {
                    if let redis::Value::Array(kv) = grp {
                        let mut name: Option<String> = None;
                        let mut pending: Option<u64> = None;
                        let mut i = 0;
                        while i + 1 < kv.len() {
                            if let redis::Value::BulkString(k) = &kv[i] {
                                if k == b"name" {
                                    if let redis::Value::BulkString(v) = &kv[i + 1] {
                                        name = std::str::from_utf8(v).ok().map(|s| s.to_string());
                                    }
                                }
                                if k == b"pending" {
                                    if let redis::Value::Int(v) = &kv[i + 1] {
                                        pending = Some(*v as u64);
                                    }
                                }
                            }
                            i += 2;
                        }
                        if let (Some(n), Some(p)) = (name, pending) {
                            if n == xfer_group {
                                group_pending = p;
                                break;
                            }
                        }
                    }
                }
            }
        }

        if stream_len == 0 && group_pending == 0 {
            println!("  ✅ Transfer queue drained");
            break;
        }
    }

    // Poll for on-chain finalization
    println!("\n⏳ Polling for NEAR testnet to finalize balances...");
    let client_rpc = JsonRpcClient::connect(rpc_url);
    let expected_total = accepted as u128 * YOCTO_PER_TRANSFER;
    let poll_interval = Duration::from_secs(5);
    let max_polls = 300;

    let mut final_total = 0u128;
    let mut completion_instant: Option<Instant> = None;

    for poll in 1..=max_polls {
        let mut total_balance: u128 = 0;

        for receiver in &receivers {
            let balance_args = json!({
                "account_id": receiver
            })
            .to_string()
            .into_bytes();

            let call_request = methods::query::RpcQueryRequest {
                block_reference: BlockReference::Finality(Finality::Final),
                request: near_primitives::views::QueryRequest::CallFunction {
                    account_id: owner_account_id.clone(),
                    method_name: "ft_balance_of".to_string(),
                    args: near_primitives::types::FunctionArgs::from(balance_args),
                },
            };

            match client_rpc.call(call_request).await {
                Ok(response) => {
                    let balance: u128 = match response.kind {
                        QueryResponseKind::CallResult(result) => {
                            let balance_str: String = serde_json::from_slice(&result.result)?;
                            balance_str.parse().unwrap_or(0)
                        }
                        _ => 0,
                    };
                    total_balance += balance;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }
        }

        let success_rate = (total_balance as f64 / expected_total as f64) * 100.0;
        println!(
            "  Poll {}: {} tokens ({:.2}% complete)",
            poll,
            total_balance / YOCTO_PER_TRANSFER,
            success_rate
        );

        if total_balance >= expected_total {
            println!(
                "  ✅ Target reached after {} polls (~{}s)",
                poll,
                poll * poll_interval.as_secs()
            );
            final_total = total_balance;
            completion_instant = Some(Instant::now());
            break;
        }

        if poll == max_polls {
            println!(
                "  ⚠️  Reached polling cap (~{}s); proceeding with current totals",
                poll * poll_interval.as_secs()
            );
            final_total = total_balance;
            completion_instant = Some(Instant::now());
            break;
        }

        tokio::time::sleep(poll_interval).await;
    }

    let completion_instant = completion_instant.unwrap_or_else(Instant::now);
    let onchain_elapsed = completion_instant
        .checked_duration_since(api_start)
        .unwrap_or_else(|| Duration::from_secs(0));
    let onchain_duration_secs = onchain_elapsed.as_secs_f64();
    let completed_transfers = (final_total / YOCTO_PER_TRANSFER) as usize;
    let blockchain_throughput = if onchain_duration_secs > 0.0 {
        completed_transfers as f64 / onchain_duration_secs
    } else {
        0.0
    };

    let on_chain_success_pct = final_total as f64 / expected_total as f64 * 100.0;

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  ON-CHAIN VERIFICATION                                     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!(
        "  Expected total: {} tokens",
        expected_total / YOCTO_PER_TRANSFER
    );
    println!(
        "  Actual total:   {} tokens",
        final_total / YOCTO_PER_TRANSFER
    );
    println!("  On-chain success rate: {:.2}%", on_chain_success_pct);
    println!("  Blockchain time: {:.2}s", onchain_duration_secs);
    println!(
        "  Blockchain throughput {:.2} tx/sec",
        blockchain_throughput
    );

    ensure!(
        final_total == expected_total,
        "On-chain total mismatch: expected {} tokens, got {} tokens ({:.2}%)",
        expected_total / YOCTO_PER_TRANSFER,
        final_total / YOCTO_PER_TRANSFER,
        on_chain_success_pct
    );

    println!("\n🎉 ╔════════════════════════════════════════════════════════════╗");
    println!("   ║  TESTNET 60K BENCHMARK: ✅ PASSED                          ║");
    println!("   ║  Successfully handled 60,000 transfers                     ║");
    println!("   ╚════════════════════════════════════════════════════════════╝");

    println!("\n--- BENCHMARK RESULTS (TESTNET) ---");
    println!("test_name: sixty_k_benchmark_test");
    println!("transfers: {}", MEGA_BENCH_REQUESTS);
    println!("api_duration_secs: {:.2}", api_duration_secs);
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
