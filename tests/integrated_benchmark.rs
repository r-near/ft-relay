/// Fully integrated benchmark test
///
/// This test:
/// 1. Starts near-sandbox programmatically
/// 2. Deploys the FT contract
/// 3. Starts the relay API server (with registration workers)
/// 4. Runs benchmark with actual FT transfers
/// 5. Verifies balances changed correctly (registration happens automatically)
use ft_relay::RelayConfig;
use near_crypto::SecretKey;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::{
    query::QueryResponseKind, transactions::RpcSendTransactionRequest,
};
use near_primitives::account::AccessKeyPermission;
use near_primitives::action::{Action, AddKeyAction, DeployContractAction, FunctionCallAction};
use near_primitives::transaction::{SignedTransaction, Transaction, TransactionV0};
use near_primitives::types::{AccountId, BlockReference, Finality};
use near_primitives::views::TxExecutionStatus;
use near_sandbox::{GenesisAccount, Sandbox, SandboxConfig};
use serde_json::json;
use std::time::Duration;

mod common;
use common::{
    flush_redis, print_benchmark_summary, redis_settings_from_env, run_benchmark, BenchmarkPlan,
};

const FT_WASM_PATH: &str = "resources/fungible_token.wasm";
const YOCTO_PER_TRANSFER: u128 = 1_000_000_000_000_000_000;

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
    let redis_settings = redis_settings_from_env();
    let redis_url = redis_settings.url.clone();
    flush_redis(&redis_url).await?;
    println!(
        "Starting sandbox benchmark: {} transfers (concurrency {})",
        60_000, 100
    );

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

    println!("Sandbox started at {}", sandbox.rpc_addr);

    // Deploy and setup
    setup_ft_contract(&sandbox, &ft_owner).await?;
    println!("FT contract deployed and initialized");

    // Generate multiple access keys for better throughput (real-world usage pattern)
    println!("\nGenerating 75 access keys for key pooling...");
    let mut secret_keys = vec![ft_owner.private_key.to_string()];

    // Generate 75 new keypairs upfront
    let new_keys: Vec<SecretKey> = (0..75)
        .map(|_| SecretKey::from_random(near_crypto::KeyType::ED25519))
        .collect();

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
                println!("  All keys added successfully in batch");
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

    println!(
        "All {} access keys configured and verified",
        secret_keys.len()
    );

    let receiver_ids: Vec<AccountId> = receivers
        .iter()
        .map(|receiver| receiver.account_id.clone())
        .collect();

    let relay_config = RelayConfig {
        token: ft_owner.account_id.to_string(),
        account_id: ft_owner.account_id.to_string(),
        secret_keys,
        rpc_url: sandbox.rpc_addr.clone(),
        batch_linger_ms: 100,
        transfer_workers: 30,
        registration_workers: 1,
        verification_workers: 10,
        bind_addr: "127.0.0.1:18082".to_string(),
        redis: redis_settings.clone(),
    };

    let plan = BenchmarkPlan::new(
        "sandbox_60k",
        relay_config,
        redis_url.clone(),
        "ftrelay:sandbox",
        60_000,
        receiver_ids.clone(),
        ft_owner.account_id.clone(),
        sandbox.rpc_addr.clone(),
        "1000000000000000000",
        YOCTO_PER_TRANSFER,
    )
    .with_concurrency(100)
    .with_min_throughput(100.0)
    .with_max_duration(Duration::from_secs(600))
    .with_polling(Duration::from_secs(3), 60);

    let outcome = run_benchmark(plan).await?;

    let expected_per_receiver = if receiver_ids.is_empty() {
        0
    } else {
        (outcome.http.accepted / receiver_ids.len()) as u128 * YOCTO_PER_TRANSFER
    };

    print_benchmark_summary(
        "sandbox_60k",
        &outcome,
        Some(expected_per_receiver),
        YOCTO_PER_TRANSFER,
    );

    Ok(())
}
