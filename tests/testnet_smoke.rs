use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use dotenv::dotenv;
use ft_relay::RelayConfig;
use near_crypto::SecretKey;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::{
    query::QueryResponseKind, transactions::RpcSendTransactionRequest,
};
use near_primitives::account::AccessKeyPermission;
use near_primitives::action::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, TransferAction,
};
use near_primitives::transaction::{SignedTransaction, Transaction, TransactionV0};
use near_primitives::types::{AccountId, BlockReference, Finality};
use near_primitives::views::TxExecutionStatus;

mod common;
use common::{
    flush_redis, print_benchmark_summary, redis_settings_from_env, run_benchmark, BenchmarkPlan,
};

const TRANSFER_AMOUNT: &str = "1000000000000000000"; // 1 token
const MEGA_BENCH_REQUESTS: usize = 60_000;
const MEGA_BENCH_CONCURRENCY: usize = 100;
const YOCTO_PER_TRANSFER: u128 = 1_000_000_000_000_000_000;
const RECEIVER_ACCOUNT_DEPOSIT: u128 = 200_000_000_000_000_000_000_000;

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

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    flush_redis(&redis_url).await?;
    println!(
        "Running testnet benchmark: {} transfers (concurrency {})",
        MEGA_BENCH_REQUESTS, MEGA_BENCH_CONCURRENCY
    );

    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL must be set");
    let owner_account_id: AccountId = std::env::var("ACCOUNT_ID")
        .expect("ACCOUNT_ID must be set")
        .parse()?;
    let token_account_id: AccountId = std::env::var("TOKEN")
        .expect("TOKEN must be set (FT contract address)")
        .parse()?;
    let private_keys_str =
        std::env::var("PRIVATE_KEYS").expect("PRIVATE_KEYS must be set (comma-separated list)");

    println!("Account: {}", owner_account_id);
    println!("Token: {}", token_account_id);
    println!("RPC endpoint: {}", rpc_url);

    let mut secret_keys = Vec::new();
    for key_str in private_keys_str.split(',') {
        let key_str = key_str.trim();
        if key_str.is_empty() {
            continue;
        }
        key_str.parse::<SecretKey>()?;
        secret_keys.push(key_str.to_string());
    }

    ensure!(!secret_keys.is_empty(), "No PRIVATE_KEYS provided");
    let owner_secret = secret_keys[0].parse::<SecretKey>()?;
    let owner_public = owner_secret.public_key();

    let client = JsonRpcClient::connect(&rpc_url);
    let receivers =
        create_receiver_accounts(&client, &owner_account_id, &owner_secret, &owner_public, 5)
            .await?;
    println!("Provisioned {} receiver accounts", receivers.len());

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let bind_addr = listener.local_addr()?;
    drop(listener);
    let bind_addr_str = format!("127.0.0.1:{}", bind_addr.port());

    let relay_config = RelayConfig {
        token: token_account_id.to_string(),
        account_id: owner_account_id.to_string(),
        secret_keys,
        rpc_url: rpc_url.clone(),
        batch_linger_ms: 1000,
        transfer_workers: 30,
        registration_workers: 1,
        verification_workers: 10,
        bind_addr: bind_addr_str,
        redis: redis_settings_from_env(),
    };

    let plan = BenchmarkPlan::new(
        "testnet_60k",
        relay_config,
        redis_url.clone(),
        "ftrelay:testnet",
        MEGA_BENCH_REQUESTS,
        receivers.clone(),
        token_account_id.clone(),
        rpc_url.clone(),
        TRANSFER_AMOUNT,
        YOCTO_PER_TRANSFER,
    )
    .with_concurrency(MEGA_BENCH_CONCURRENCY)
    .with_min_throughput(100.0)
    .with_max_duration(Duration::from_secs(600))
    .with_polling(Duration::from_secs(5), 300)
    .with_queue_drain(Duration::from_secs(1), 600);

    let result = run_benchmark(plan).await;

    println!("\nCleaning up receiver accounts...");
    let cleanup_result = cleanup_receiver_accounts(
        &client,
        &owner_account_id,
        &owner_secret,
        &owner_public,
        &receivers,
    )
    .await;

    match cleanup_result {
        Ok(count) => println!("Deleted {} receiver accounts", count),
        Err(e) => println!("Cleanup warning: {:?}", e),
    }

    if let Ok(outcome) = &result {
        let expected_per_receiver = if receivers.is_empty() {
            0
        } else {
            (outcome.http.accepted / receivers.len()) as u128 * YOCTO_PER_TRANSFER
        };

        let expected = if expected_per_receiver == 0 {
            None
        } else {
            Some(expected_per_receiver)
        };

        print_benchmark_summary("testnet_60k", outcome, expected, YOCTO_PER_TRANSFER);
    }

    result.map(|_| ())
}

async fn create_receiver_accounts(
    client: &JsonRpcClient,
    owner_account_id: &AccountId,
    owner_secret: &SecretKey,
    owner_public: &near_crypto::PublicKey,
    count: usize,
) -> Result<Vec<AccountId>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let access_key_request = methods::query::RpcQueryRequest {
        block_reference: BlockReference::Finality(Finality::Final),
        request: near_primitives::views::QueryRequest::ViewAccessKey {
            account_id: owner_account_id.clone(),
            public_key: owner_public.clone(),
        },
    };
    let access_key_response = client.call(access_key_request).await?;
    let mut next_nonce = match access_key_response.kind {
        QueryResponseKind::AccessKey(access_key) => access_key.nonce + 1,
        _ => return Err(anyhow!("Unexpected response type")),
    };

    let mut block_hash = client
        .call(methods::block::RpcBlockRequest {
            block_reference: BlockReference::Finality(Finality::Final),
        })
        .await?
        .header
        .hash;

    let run_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        % 1_000_000;

    let mut receivers = Vec::with_capacity(count);
    for index in 0..count {
        let receiver_id: AccountId =
            format!("r{}-{}.{}", run_suffix, index, owner_account_id).parse()?;
        let receiver_secret = SecretKey::from_random(near_crypto::KeyType::ED25519);
        let receiver_public = receiver_secret.public_key();

        let create_actions = vec![
            Action::CreateAccount(CreateAccountAction {}),
            Action::Transfer(TransferAction {
                deposit: RECEIVER_ACCOUNT_DEPOSIT,
            }),
            Action::AddKey(Box::new(AddKeyAction {
                public_key: receiver_public,
                access_key: near_primitives::account::AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FullAccess,
                },
            })),
        ];

        let transaction = Transaction::V0(TransactionV0 {
            signer_id: owner_account_id.clone(),
            public_key: owner_public.clone(),
            nonce: next_nonce,
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
                    receivers.push(receiver_id);
                }
                status => {
                    return Err(anyhow!("Create account failed: {:?}", status));
                }
            }
        } else {
            return Err(anyhow!(
                "Receiver creation did not return an execution outcome"
            ));
        }

        next_nonce += 1;
        if index % 3 == 2 {
            block_hash = client
                .call(methods::block::RpcBlockRequest {
                    block_reference: BlockReference::Finality(Finality::Final),
                })
                .await?
                .header
                .hash;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Ok(receivers)
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
        _ => return Err(anyhow!("Unexpected response type")),
    };

    let block_request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };
    let block = client.call(block_request).await?;
    let mut block_hash = block.header.hash;

    let mut deleted = 0;
    for (idx, receiver_id) in receivers.iter().enumerate() {
        let delete_action = Action::DeleteAccount(DeleteAccountAction {
            beneficiary_id: owner_account_id.clone(),
        });

        let transaction = Transaction::V0(TransactionV0 {
            signer_id: receiver_id.clone(),
            public_key: owner_public.clone(),
            nonce: 1,
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
                            println!("  Deleted: {}", receiver_id);
                        }
                        status => {
                            println!("  Failed to delete {}: {:?}", receiver_id, status);
                        }
                    }
                }
            }
            Err(e) => {
                println!("  Failed to delete {}: {:?}", receiver_id, e);
            }
        }

        if idx % 3 == 2 && idx + 1 < receivers.len() {
            let block_request = methods::block::RpcBlockRequest {
                block_reference: BlockReference::Finality(Finality::Final),
            };
            if let Ok(block) = client.call(block_request).await {
                block_hash = block.header.hash;
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(deleted)
}
