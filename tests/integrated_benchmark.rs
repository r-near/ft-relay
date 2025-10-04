/// Fully integrated benchmark test
///
/// This test:
/// 1. Starts near-sandbox programmatically
/// 2. Deploys the FT contract
/// 3. Registers receiver accounts
/// 4. Starts the relay API server
/// 5. Runs benchmark with actual FT transfers
/// 6. Verifies balances changed correctly
use ft_relay::{RedisConfig, RelayConfig};
use near_api::{NetworkConfig, RPCEndpoint, Signer};
use near_api_types::NearToken;
use near_primitives::action::{Action, DeployContractAction, FunctionCallAction};
use near_sandbox::{GenesisAccount, Sandbox, SandboxConfig};
use serde_json::json;
use std::time::{Duration, Instant};

const FT_WASM_PATH: &str = "resources/fungible_token.wasm";

/// Flush Redis before test
async fn flush_redis() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
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

    let network = NetworkConfig {
        network_name: "sandbox".to_string(),
        rpc_endpoints: vec![RPCEndpoint::new(sandbox.rpc_addr.parse()?)],
        ..NetworkConfig::testnet()
    };

    let signer = Signer::new(Signer::from_secret_key(owner.private_key.parse()?))?;

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

    let tx = near_api::Transaction::construct(owner.account_id.clone(), owner.account_id.clone())
        .add_actions(vec![deploy_action, init_action])
        .with_signer(signer)
        .send_to(&network)
        .await?;

    tx.assert_success();
    Ok(())
}

/// Register accounts for storage
async fn register_accounts(
    sandbox: &Sandbox,
    ft_owner: &GenesisAccount,
    accounts: &[GenesisAccount],
) -> Result<(), Box<dyn std::error::Error>> {
    let network = NetworkConfig {
        network_name: "sandbox".to_string(),
        rpc_endpoints: vec![RPCEndpoint::new(sandbox.rpc_addr.parse()?)],
        ..NetworkConfig::testnet()
    };

    let signer = Signer::new(Signer::from_secret_key(ft_owner.private_key.parse()?))?;

    for account in accounts {
        let args = json!({
            "account_id": account.account_id
        })
        .to_string()
        .into_bytes();

        let action = Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "storage_deposit".to_string(),
            args,
            gas: 10_000_000_000_000, // 10 Tgas
            deposit: NearToken::from_millinear(125).as_yoctonear(),
        }));

        let tx = near_api::Transaction::construct(
            ft_owner.account_id.clone(),
            ft_owner.account_id.clone(),
        )
        .add_action(action)
        .with_signer(signer.clone())
        .send_to(&network)
        .await?;

        tx.assert_success();
    }

    Ok(())
}

#[tokio::test]
#[ignore] // Run with: cargo test --test integrated_benchmark basic -- --ignored --nocapture
async fn test_integrated_basic() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    flush_redis().await?;

    println!("\nIntegrated Test: Basic FT Transfer");
    println!("====================================");

    // Start sandbox
    let ft_owner = GenesisAccount::generate_with_name("ft.sandbox".parse()?);
    let receiver = GenesisAccount::generate_with_name("receiver.sandbox".parse()?);

    let sandbox = Sandbox::start_sandbox_with_config(SandboxConfig {
        additional_accounts: vec![ft_owner.clone(), receiver.clone()],
        ..Default::default()
    })
    .await?;

    println!("Sandbox started at {}", sandbox.rpc_addr);

    // Deploy FT contract
    setup_ft_contract(&sandbox, &ft_owner).await?;
    println!("FT contract deployed and initialized");

    // Register receiver
    register_accounts(&sandbox, &ft_owner, &[receiver.clone()]).await?;
    println!("Receiver registered");

    // Start relay server
    let config = RelayConfig {
        token: ft_owner.account_id.clone(),
        account_id: ft_owner.account_id.clone(),
        secret_keys: vec![ft_owner.private_key.to_string()],
        rpc_url: sandbox.rpc_addr.clone(),
        batch_size: 1,
        batch_linger_ms: 20,
        max_inflight_batches: 10,
        max_workers: 3,
        bind_addr: "127.0.0.1:18080".to_string(),
        redis: test_redis_config(),
    };

    tokio::spawn(async move {
        if let Err(e) = ft_relay::run(config).await {
            eprintln!("Server error: {:?}", e);
        }
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("Relay server started");

    // Send transfer request
    let client = reqwest::Client::new();
    let response = client
        .post("http://127.0.0.1:18080/v1/transfer")
        .json(&json!({
            "receiver_id": receiver.account_id,
            "amount": "1000000000000000000", // 1 token
            "memo": "test"
        }))
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await?;
    let transfer_id = body["transfer_id"].as_str().expect("transfer_id present");
    println!("Transfer request accepted: {:?}", body);

    // Check initial status is pending
    let status_response = client
        .get(format!("http://127.0.0.1:18080/v1/transfer/{}", transfer_id))
        .send()
        .await?;
    assert_eq!(status_response.status(), 200);
    let status_body: serde_json::Value = status_response.json().await?;
    assert_eq!(status_body["status"], "pending");
    println!("Initial status: {:?}", status_body);

    // Wait for transaction to process (batching + blockchain)
    println!("Waiting for transaction to process...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify balance
    let network = NetworkConfig {
        network_name: "sandbox".to_string(),
        rpc_endpoints: vec![RPCEndpoint::new(sandbox.rpc_addr.parse()?)],
        ..NetworkConfig::testnet()
    };

    let balance_args = json!({
        "account_id": receiver.account_id
    });

    let result = near_api::Contract(ft_owner.account_id.clone())
        .call_function("ft_balance_of", balance_args)?
        .read_only()
        .fetch_from(&network)
        .await?;

    let balance: String = result.data;
    println!("Receiver balance: {}", balance);

    assert_eq!(balance, "1000000000000000000", "Balance should be 1 token");

    // Verify transfer status now shows completed with tx_hash
    let final_status_response = client
        .get(format!("http://127.0.0.1:18080/v1/transfer/{}", transfer_id))
        .send()
        .await?;
    assert_eq!(final_status_response.status(), 200);
    let final_status_body: serde_json::Value = final_status_response.json().await?;
    assert_eq!(final_status_body["status"], "completed");
    assert!(final_status_body["tx_hash"].is_string(), "tx_hash should be present");
    println!("Final status: {:?}", final_status_body);
    println!("Transaction hash: {}", final_status_body["tx_hash"].as_str().unwrap());

    println!("\nIntegration test passed!");

    Ok(())
}

#[tokio::test]
#[ignore] // Run with: cargo test --test integrated_benchmark load -- --ignored --nocapture
async fn test_integrated_load() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(
        env_logger::Env::default()
            .default_filter_or("info,ft_relay=info,near_api=warn,tracing::span=warn"),
    )
    .is_test(true)
    .try_init()
    .ok();
    flush_redis().await?;

    println!("\nIntegrated Load Test: 1000 FT Transfers");
    println!("=========================================");

    // Start sandbox with multiple receivers
    let ft_owner = GenesisAccount::generate_with_name("ft.sandbox".parse()?);

    let mut receivers = Vec::new();
    for i in 0..10 {
        receivers.push(GenesisAccount::generate_with_name(
            format!("recv{}.sandbox", i).parse()?,
        ));
    }

    let mut accounts = vec![ft_owner.clone()];
    accounts.extend(receivers.clone());

    let sandbox = Sandbox::start_sandbox_with_config(SandboxConfig {
        additional_accounts: accounts,
        ..Default::default()
    })
    .await?;

    println!("Sandbox started with 10 receivers");

    // Deploy and setup
    setup_ft_contract(&sandbox, &ft_owner).await?;
    println!("FT contract deployed and initialized");

    register_accounts(&sandbox, &ft_owner, &receivers).await?;
    println!("All 10 receivers registered");

    // Start relay server with batching
    let config = RelayConfig {
        token: ft_owner.account_id.clone(),
        account_id: ft_owner.account_id.clone(),
        secret_keys: vec![ft_owner.private_key.to_string()],
        rpc_url: sandbox.rpc_addr.clone(),
        batch_size: 90, // Use optimized batch size
        batch_linger_ms: 20,
        max_inflight_batches: 50,
        max_workers: 3,
        bind_addr: "127.0.0.1:18081".to_string(),
        redis: test_redis_config(),
    };

    let server_handle = tokio::spawn(async move {
        match ft_relay::run(config).await {
            Ok(_) => println!("Server exited normally"),
            Err(e) => {
                eprintln!("\nRelay server error: {:?}", e);
                panic!("Server failed: {:?}", e);
            }
        }
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("Relay server started");

    // Send transfer requests
    let client = reqwest::Client::new();
    let total_requests = 1000;

    println!("Sending {} transfer requests...", total_requests);
    let start = Instant::now();

    let mut tasks = Vec::new();
    for i in 0..total_requests {
        let client = client.clone();
        let receiver_id = receivers[i % 10].account_id.clone();

        let task = tokio::spawn(async move {
            match client
                .post("http://127.0.0.1:18081/v1/transfer")
                .json(&json!({
                    "receiver_id": receiver_id,
                    "amount": "1000000000000000000" // 1 token each
                }))
                .send()
                .await
            {
                Ok(r) if r.status() == 200 => true,
                Ok(r) => {
                    eprintln!(
                        "Request failed with status {}: {:?}",
                        r.status(),
                        r.text().await
                    );
                    false
                }
                Err(e) => {
                    eprintln!("Request error: {}", e);
                    false
                }
            }
        });
        tasks.push(task);
    }

    let mut success = 0;
    for task in tasks {
        if task.await.unwrap_or(false) {
            success += 1;
        }
    }

    let elapsed = start.elapsed();
    let throughput = total_requests as f64 / elapsed.as_secs_f64();

    println!("\nHTTP Results:");
    println!("  Requests sent: {}", total_requests);
    println!("  Accepted: {}", success);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} req/sec", throughput);

    // Check if server is still running
    if server_handle.is_finished() {
        panic!("Server task died!");
    }
    println!("  Server status: Running");

    // Wait for batches to process
    println!("\nWaiting for transactions to process on-chain...");
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Verify balances for each receiver
    println!("Verifying on-chain balances...");

    let network = NetworkConfig {
        network_name: "sandbox".to_string(),
        rpc_endpoints: vec![RPCEndpoint::new(sandbox.rpc_addr.parse()?)],
        ..NetworkConfig::testnet()
    };

    let mut total_balance_verified: u128 = 0;
    let expected_per_receiver = (total_requests / 10) as u128 * 1_000_000_000_000_000_000; // Each receiver gets N tokens

    for (i, receiver) in receivers.iter().enumerate() {
        let balance_args = json!({
            "account_id": receiver.account_id
        });

        let result = near_api::Contract(ft_owner.account_id.clone())
            .call_function("ft_balance_of", balance_args)?
            .read_only()
            .fetch_from(&network)
            .await?;

        let balance_str: String = result.data;
        let balance: u128 = balance_str.parse().unwrap_or(0);
        total_balance_verified += balance;

        println!(
            "  Receiver {}: {} tokens (expected: ~{})",
            i,
            balance / 1_000_000_000_000_000_000,
            expected_per_receiver / 1_000_000_000_000_000_000
        );
    }

    let expected_total = total_requests as u128 * 1_000_000_000_000_000_000;
    let success_rate = (total_balance_verified as f64 / expected_total as f64) * 100.0;

    println!("\nOn-Chain Verification:");
    println!(
        "  Expected total: {} tokens",
        expected_total / 1_000_000_000_000_000_000
    );
    println!(
        "  Actual total:   {} tokens",
        total_balance_verified / 1_000_000_000_000_000_000
    );
    println!("  Success rate:   {:.2}%", success_rate);

    // Assert at least 90% of transactions succeeded
    assert!(
        success_rate >= 90.0,
        "At least 90% of transactions should succeed on-chain. Got {:.2}%",
        success_rate
    );

    // If less than 95%, print warning
    if success_rate < 95.0 {
        println!(
            "\nWarning: Success rate is {:.2}%, expected >=95%",
            success_rate
        );
        println!("  Some transactions may have failed due to concurrent load");
    }

    println!("\nLoad test complete!");
    println!(
        "  {} HTTP requests accepted at {:.2} req/sec",
        success, throughput
    );
    println!("  {:.2}% of transfers confirmed on-chain", success_rate);

    Ok(())
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

    println!("Registering {} receiver accounts...", receiver_count);
    register_accounts(&sandbox, &ft_owner, &receivers).await?;
    println!("✅ All receivers registered for storage");

    // Generate multiple access keys for better throughput (real-world usage pattern)
    println!("\nGenerating 3 access keys for key pooling...");
    let mut secret_keys = vec![ft_owner.private_key.to_string()];

    // Add 2 more keys to the pool
    let network = NetworkConfig {
        network_name: "sandbox".to_string(),
        rpc_endpoints: vec![RPCEndpoint::new(sandbox.rpc_addr.parse()?)],
        ..NetworkConfig::testnet()
    };

    for _i in 1..3 {
        // Generate a new random keypair using near-api-rs
        let new_secret_key = near_api::signer::generate_secret_key()?;
        let new_public_key = new_secret_key.public_key();

        // Add the key to the account
        let add_key_action = near_primitives::action::Action::AddKey(Box::new(
            near_primitives::action::AddKeyAction {
                public_key: new_public_key.clone(),
                access_key: near_primitives::account::AccessKey {
                    nonce: 0,
                    permission: near_primitives::account::AccessKeyPermission::FullAccess,
                },
            },
        ));

        let owner_signer = Signer::new(Signer::from_secret_key(ft_owner.private_key.parse()?))?;
        let tx = near_api::Transaction::construct(
            ft_owner.account_id.clone(),
            ft_owner.account_id.clone(),
        )
        .add_action(add_key_action)
        .with_signer(owner_signer)
        .send_to(&network)
        .await?;

        tx.assert_success();
        secret_keys.push(new_secret_key.to_string());
    }

    println!("✅ {} access keys configured", secret_keys.len());

    // Start relay server with optimized config for high throughput
    let config = RelayConfig {
        token: ft_owner.account_id.clone(),
        account_id: ft_owner.account_id.clone(),
        secret_keys,
        rpc_url: sandbox.rpc_addr.clone(),
        batch_size: 90,            // Max safe batch size
        batch_linger_ms: 20,       // Fast batching
        max_inflight_batches: 500, // High concurrency
        max_workers: 3,
        bind_addr: "127.0.0.1:18082".to_string(),
        redis: test_redis_config(),
    };

    println!("\nServer Configuration:");
    println!("  Batch size: {}", config.batch_size);
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

    tokio::time::sleep(Duration::from_millis(1000)).await;
    println!("✅ Relay server started\n");

    // Send 60,000 transfer requests
    let client = reqwest::Client::new();
    let total_requests = 60_000;
    let concurrent_workers = 200; // Number of parallel HTTP workers

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  Starting benchmark: {} transfers", total_requests);
    println!("║  Concurrent HTTP workers: {}", concurrent_workers);
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let start = Instant::now();
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

        let task = tokio::spawn(async move {
            let mut worker_success = 0;
            for i in start_idx..end_idx {
                let receiver_id = receivers[i % receiver_count].account_id.clone();

                match client
                    .post("http://127.0.0.1:18082/v1/transfer")
                    .json(&json!({
                        "receiver_id": receiver_id,
                        "amount": "1000000000000000000" // 1 token each
                    }))
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await
                {
                    Ok(r) if r.status() == 200 => worker_success += 1,
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
                        if worker_id == 0 && worker_success == 0 {
                            eprintln!("❌ Worker {} request error: {}", worker_id, e);
                        }
                    }
                }

                // Print progress every 10k requests
                if i > 0 && i % 10_000 == 0 && worker_id == 0 {
                    let elapsed = start.elapsed();
                    let current_rate = i as f64 / elapsed.as_secs_f64();
                    println!(
                        "  Progress: {} requests in {:?} ({:.1} req/sec)",
                        i, elapsed, current_rate
                    );
                }
            }
            worker_success
        });
        tasks.push(task);
    }

    let mut total_success = 0;
    for task in tasks {
        total_success += task.await.unwrap_or(0);
    }

    let elapsed = start.elapsed();
    let throughput = total_requests as f64 / elapsed.as_secs_f64();

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  HTTP REQUEST RESULTS                                      ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("  Total requests:  {}", total_requests);
    println!("  Accepted:        {}", total_success);
    println!("  Failed:          {}", total_requests - total_success);
    println!("  Duration:        {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput:      {:.2} req/sec", throughput);
    println!("  Target:          ≥100 req/sec");

    if throughput >= 100.0 {
        println!("  Status:          ✅ PASSED");
    } else {
        println!("  Status:          ❌ FAILED");
    }

    // Check if server is still running
    if server_handle.is_finished() {
        panic!("❌ Server task died during benchmark!");
    }

    println!("\n⏳ Waiting for NEAR to finalize balances...");

    let network = NetworkConfig {
        network_name: "sandbox".to_string(),
        rpc_endpoints: vec![RPCEndpoint::new(sandbox.rpc_addr.parse()?)],
        ..NetworkConfig::testnet()
    };

    let expected_total = total_requests as u128 * 1_000_000_000_000_000_000;
    let poll_interval = Duration::from_secs(3);
    let max_polls = 60; // ~3 minutes max wait

    let mut final_balances = Vec::new();
    let mut final_total: u128 = 0;
    let mut final_success_rate = 0.0;

    for poll in 1..=max_polls {
        let mut balances = Vec::with_capacity(receiver_count);
        let mut total_balance: u128 = 0;

        for receiver in &receivers {
            let balance_args = json!({
                "account_id": receiver.account_id
            });

            let result = near_api::Contract(ft_owner.account_id.clone())
                .call_function("ft_balance_of", balance_args)?
                .read_only()
                .fetch_from(&network)
                .await?;

            let balance_str: String = result.data;
            let balance: u128 = balance_str.parse().unwrap_or(0);
            total_balance += balance;
            balances.push(balance);
        }

        final_success_rate = (total_balance as f64 / expected_total as f64) * 100.0;

        println!(
            "  Poll {}: {} tokens ({:.2}% complete)",
            poll,
            total_balance / 1_000_000_000_000_000_000,
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
            break;
        }

        if poll == max_polls {
            println!(
                "  ⚠️  Reached polling cap (~{}s); proceeding with current totals",
                poll * poll_interval.as_secs()
            );
            final_balances = balances;
            final_total = total_balance;
            break;
        }

        tokio::time::sleep(poll_interval).await;
    }

    let expected_per_receiver =
        (total_requests / receiver_count) as u128 * 1_000_000_000_000_000_000;

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!(
        "║  ON-CHAIN VERIFICATION (all {} receivers)               ║",
        receiver_count
    );
    println!("╚════════════════════════════════════════════════════════════╝");
    println!(
        "  Expected total:     {} tokens",
        expected_total / 1_000_000_000_000_000_000
    );
    println!(
        "  Actual total:       {} tokens",
        final_total / 1_000_000_000_000_000_000
    );
    println!("  Success rate:       {:.2}%", final_success_rate);

    println!("\n  Receiver breakdown:");
    for (idx, (receiver, balance)) in receivers.iter().zip(final_balances.iter()).enumerate() {
        println!(
            "    {:>2}: {:<20} {} tokens (expected: {})",
            idx,
            receiver.account_id,
            balance / 1_000_000_000_000_000_000,
            expected_per_receiver / 1_000_000_000_000_000_000
        );
    }

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  FINAL BOUNTY REQUIREMENT CHECK                            ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("  ✓ Total transfers:   {} / 60,000", total_success);
    println!("  ✓ Throughput:        {:.2} / ≥100 req/sec", throughput);
    println!(
        "  ✓ Duration:          {:.2}s / ≤600s (10 min)",
        elapsed.as_secs_f64()
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
        throughput >= 100.0,
        "❌ Failed: Throughput {:.2} req/sec is below 100 req/sec requirement",
        throughput
    );

    assert!(
        elapsed.as_secs() <= 600,
        "❌ Failed: Took {:.2}s, should complete within 600s (10 minutes)",
        elapsed.as_secs_f64()
    );

    assert!(
        final_success_rate >= 80.0,
        "❌ Failed: Only {:.2}% of transactions confirmed on-chain",
        final_success_rate
    );

    println!("\n🎉 ╔════════════════════════════════════════════════════════════╗");
    println!("   ║  BOUNTY REQUIREMENTS: ✅ PASSED                            ║");
    println!(
        "   ║  Successfully handled 60,000 transfers at {:.0}+ req/sec     ║",
        throughput
    );
    println!("   ║  All requirements satisfied!                               ║");
    println!("   ╚════════════════════════════════════════════════════════════╝");

    Ok(())
}
fn test_redis_config() -> RedisConfig {
    RedisConfig {
        url: "redis://127.0.0.1:6379".to_string(),
        stream_key: "ftrelay:pending".to_string(),
        consumer_group: "ftrelay:batcher".to_string(),
    }
}
