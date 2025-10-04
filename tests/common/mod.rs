use std::{
    net::TcpListener,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, ensure, Context, Result};
use near_api::{Contract, NetworkConfig, RPCEndpoint, Signer, Transaction};
use near_api_types::{AccountId, NearToken};
use near_crypto::SecretKey;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::action::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeployContractAction,
    FunctionCallAction, TransferAction,
};
use reqwest::StatusCode;
use serde_json::json;

const FAUCET_URL: &str = "https://helper.nearprotocol.com/account";
const FT_WASM_PATH: &str = "resources/fungible_token.wasm";
const STORAGE_DEPOSIT: NearToken = NearToken::from_millinear(125);
const DEFAULT_FAUCET_WAIT: Duration = Duration::from_secs(2);
const DEFAULT_RECEIVER_DEPOSIT: NearToken = NearToken::from_millinear(500);
const DEFAULT_BENEFICIARY: &str = "testnet";

static ACCOUNT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct OwnerCtx {
    pub account_id: AccountId,
    primary_secret: SecretKey,
    signer: Arc<Signer>,
    signer_keys: Vec<SecretKey>,
}

#[derive(Clone)]
pub struct ReceiverCtx {
    pub account_id: AccountId,
    pub secret_key: SecretKey,
}

pub struct HarnessConfig<'a> {
    pub label: &'a str,
    pub receiver_count: usize,
    pub receiver_deposit: NearToken,
    pub signer_pool_size: usize,
    pub faucet_wait: Duration,
}

impl<'a> Default for HarnessConfig<'a> {
    fn default() -> Self {
        Self {
            label: "harness",
            receiver_count: 1,
            receiver_deposit: DEFAULT_RECEIVER_DEPOSIT,
            signer_pool_size: 1,
            faucet_wait: DEFAULT_FAUCET_WAIT,
        }
    }
}

pub struct TestnetHarness {
    pub network: NetworkConfig,
    pub owner: OwnerCtx,
    pub receivers: Vec<ReceiverCtx>,
    pub beneficiary: AccountId,
    pub rpc_url: String,
}

impl TestnetHarness {
    pub async fn new(config: HarnessConfig<'_>) -> Result<Self> {
        ensure!(
            config.receiver_count > 0,
            "receiver_count must be greater than zero"
        );
        ensure!(
            config.signer_pool_size >= 1,
            "signer_pool_size must be at least 1"
        );

        let rpc_url = std::env::var("TESTNET_RPC_URL")
            .or_else(|_| std::env::var("RPC_URL"))
            .context(
                "Set TESTNET_RPC_URL or RPC_URL in the environment (e.g. via .env) with the authenticated testnet RPC endpoint",
            )?;

        let beneficiary: AccountId = DEFAULT_BENEFICIARY
            .parse()
            .context("default beneficiary account id should be valid")?;

        let seq = ACCOUNT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time available")
            .as_millis();
        let pid = std::process::id();
        let owner_id_str = format!("dev-{now_ms}-{pid}-{}-{}.testnet", config.label, seq);
        let owner_id: AccountId = owner_id_str
            .parse()
            .context("generated account ID should be valid")?;

        let owner_secret_key = near_api::signer::generate_secret_key()?;
        let owner_public_key = owner_secret_key.public_key();

        request_faucet_account(&owner_id_str, &owner_public_key.to_string()).await?;
        tokio::time::sleep(config.faucet_wait).await;

        let network = NetworkConfig {
            network_name: "testnet".to_string(),
            rpc_endpoints: vec![RPCEndpoint::new(rpc_url.parse()?)],
            ..NetworkConfig::testnet()
        };

        let owner_signer = Signer::new(Signer::from_secret_key(owner_secret_key.clone()))?;
        deploy_ft_contract(&network, &owner_signer, &owner_id).await?;

        // Create receiver accounts and register storage
        let mut receivers = Vec::with_capacity(config.receiver_count);
        for idx in 0..config.receiver_count {
            let receiver = create_receiver_account(
                &network,
                &owner_signer,
                &owner_id,
                idx,
                config.receiver_deposit,
            )
            .await?;
            register_storage(&network, &owner_signer, &owner_id, &receiver.account_id).await?;
            receivers.push(receiver);
        }

        // Prepare signer pool (primary key + optional extra keys)
        let mut signer_keys = vec![owner_secret_key.clone()];
        if config.signer_pool_size > 1 {
            let mut extra_keys = Vec::with_capacity(config.signer_pool_size - 1);
            for _ in 0..(config.signer_pool_size - 1) {
                extra_keys.push(near_api::signer::generate_secret_key()?);
            }
            add_function_access_keys(&network, &owner_signer, &owner_id, &extra_keys).await?;
            for key in &extra_keys {
                owner_signer
                    .add_signer_to_pool(Signer::from_secret_key(key.clone()))
                    .await?;
            }
            signer_keys.extend(extra_keys);
        }

        Ok(Self {
            network,
            owner: OwnerCtx {
                account_id: owner_id,
                primary_secret: owner_secret_key,
                signer: owner_signer,
                signer_keys,
            },
            receivers,
            beneficiary,
            rpc_url,
        })
    }

    pub fn relay_secret_keys(&self) -> Vec<String> {
        self.owner
            .signer_keys
            .iter()
            .map(|k| k.to_string())
            .collect()
    }

    pub async fn ft_transfer(&self, receiver_id: &AccountId, amount: &str) -> Result<()> {
        let args = json!({
            "receiver_id": receiver_id,
            "amount": amount,
        })
        .to_string()
        .into_bytes();

        let action = Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ft_transfer".to_string(),
            args,
            gas: 30_000_000_000_000,
            deposit: 1,
        }));

        let tx =
            Transaction::construct(self.owner.account_id.clone(), self.owner.account_id.clone())
                .add_action(action)
                .with_signer(self.owner.signer.clone())
                .send_to(&self.network)
                .await
                .context("failed to submit ft_transfer")?;

        tx.assert_success();
        println!("Transferred {} yocto tokens to {}", amount, receiver_id);
        Ok(())
    }

    pub async fn fetch_balance(&self, receiver_id: &AccountId) -> Result<String> {
        let args = json!({ "account_id": receiver_id });

        let result = Contract(self.owner.account_id.clone())
            .call_function("ft_balance_of", args)?
            .read_only()
            .fetch_from(&self.network)
            .await?;

        Ok(result.data)
    }

    pub async fn collect_balances(&self) -> Result<BalanceSummary> {
        let mut per_receiver = Vec::with_capacity(self.receivers.len());
        let mut total: u128 = 0;

        for receiver in &self.receivers {
            let balance_str = self.fetch_balance(&receiver.account_id).await?;
            let balance: u128 = balance_str
                .parse()
                .context("failed to parse balance into u128")?;
            total += balance;
            per_receiver.push((receiver.account_id.clone(), balance));
        }

        Ok(BalanceSummary {
            per_receiver,
            total_tokens: total,
        })
    }

    pub async fn teardown(self) -> Result<()> {
        let mut first_error: Option<anyhow::Error> = None;

        for receiver in &self.receivers {
            if let Err(err) = delete_account(
                &self.network,
                &receiver.account_id,
                &receiver.secret_key,
                self.owner.account_id.clone(),
            )
            .await
            {
                eprintln!(
                    "Failed to delete receiver account {}: {err:?}",
                    receiver.account_id
                );
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        if let Err(err) = delete_account(
            &self.network,
            &self.owner.account_id,
            &self.owner.primary_secret,
            self.beneficiary.clone(),
        )
        .await
        {
            eprintln!(
                "Failed to delete faucet account {}: {err:?}",
                self.owner.account_id
            );
            if first_error.is_none() {
                first_error = Some(err);
            }
        }

        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    pub fn receiver_ids(&self) -> Vec<AccountId> {
        self.receivers
            .iter()
            .map(|r| r.account_id.clone())
            .collect()
    }
}

#[derive(Debug)]
pub struct BalanceSummary {
    pub per_receiver: Vec<(AccountId, u128)>,
    pub total_tokens: u128,
}

pub fn allocate_bind_addr() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(format!("127.0.0.1:{}", addr.port()))
}

async fn request_faucet_account(account_id: &str, public_key: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .post(FAUCET_URL)
        .json(&json!({
            "newAccountId": account_id,
            "newAccountPublicKey": public_key,
        }))
        .send()
        .await
        .context("failed to reach faucet helper")?;

    if response.status() != StatusCode::OK {
        return Err(anyhow!(
            "faucet returned {}: {}",
            response.status(),
            response.text().await.unwrap_or_default()
        ));
    }
    Ok(())
}

async fn deploy_ft_contract(
    network: &NetworkConfig,
    signer: &Arc<Signer>,
    account_id: &AccountId,
) -> Result<()> {
    let wasm = tokio::fs::read(FT_WASM_PATH)
        .await
        .with_context(|| format!("failed to read {}", FT_WASM_PATH))?;

    let init_args = json!({
        "owner_id": account_id,
        "total_supply": "1000000000000000000000000000",
        "metadata": {
            "spec": "ft-1.0.0",
            "name": "Benchmark Token",
            "symbol": "BENCH",
            "decimals": 18
        }
    })
    .to_string()
    .into_bytes();

    let deploy_action = Action::DeployContract(DeployContractAction { code: wasm });
    let init_action = Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "new".to_string(),
        args: init_args,
        gas: 50_000_000_000_000,
        deposit: 0,
    }));

    let tx = Transaction::construct(account_id.clone(), account_id.clone())
        .add_actions(vec![deploy_action, init_action])
        .with_signer(signer.clone())
        .send_to(network)
        .await
        .context("failed to deploy FT contract")?;

    tx.assert_success();
    println!("FT contract deployed to {}", account_id);
    Ok(())
}

async fn create_receiver_account(
    network: &NetworkConfig,
    signer: &Arc<Signer>,
    owner_id: &AccountId,
    index: usize,
    deposit: NearToken,
) -> Result<ReceiverCtx> {
    let receiver_id_str = format!("recv{:02}.{}", index, owner_id.as_str());
    let receiver_id: AccountId = receiver_id_str.parse()?;
    let receiver_secret = near_api::signer::generate_secret_key()?;
    let receiver_public = receiver_secret.public_key();

    let actions = vec![
        Action::CreateAccount(CreateAccountAction {}),
        Action::Transfer(TransferAction {
            deposit: deposit.as_yoctonear(),
        }),
        Action::AddKey(Box::new(AddKeyAction {
            public_key: receiver_public,
            access_key: AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FullAccess,
            },
        })),
    ];

    let tx = Transaction::construct(owner_id.clone(), receiver_id.clone())
        .add_actions(actions)
        .with_signer(signer.clone())
        .send_to(network)
        .await
        .context("failed to create receiver subaccount")?;

    tx.assert_success();
    println!("Receiver account {} created", receiver_id);

    Ok(ReceiverCtx {
        account_id: receiver_id,
        secret_key: receiver_secret,
    })
}

async fn register_storage(
    network: &NetworkConfig,
    signer: &Arc<Signer>,
    owner_id: &AccountId,
    receiver_id: &AccountId,
) -> Result<()> {
    let args = json!({ "account_id": receiver_id })
        .to_string()
        .into_bytes();

    let action = Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "storage_deposit".to_string(),
        args,
        gas: 10_000_000_000_000,
        deposit: STORAGE_DEPOSIT.as_yoctonear(),
    }));

    let tx = Transaction::construct(owner_id.clone(), owner_id.clone())
        .add_action(action)
        .with_signer(signer.clone())
        .send_to(network)
        .await
        .context("failed to register receiver for storage")?;

    tx.assert_success();
    println!("Receiver {} registered for storage", receiver_id);
    Ok(())
}

async fn add_function_access_keys(
    network: &NetworkConfig,
    signer: &Arc<Signer>,
    owner_id: &AccountId,
    keys: &[SecretKey],
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }

    let mut actions = Vec::with_capacity(keys.len());
    for key in keys {
        actions.push(Action::AddKey(Box::new(AddKeyAction {
            public_key: key.public_key(),
            access_key: AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FullAccess,
            },
        })));
    }

    let tx = Transaction::construct(owner_id.clone(), owner_id.clone())
        .add_actions(actions)
        .with_signer(signer.clone())
        .send_to(network)
        .await
        .context("failed to add signer keys to owner")?;

    tx.assert_success();
    println!("Added {} additional signer keys", keys.len());
    Ok(())
}

async fn delete_account(
    network: &NetworkConfig,
    account_id: &AccountId,
    secret_key: &SecretKey,
    beneficiary_id: AccountId,
) -> Result<()> {
    let signer = Signer::new(Signer::from_secret_key(secret_key.clone()))?;
    let delete_action = Action::DeleteAccount(DeleteAccountAction { beneficiary_id });

    let tx = Transaction::construct(account_id.clone(), account_id.clone())
        .add_action(delete_action)
        .with_signer(signer)
        .send_to(network)
        .await
        .context("failed to delete account")?;

    tx.assert_success();
    println!("Account {} deleted", account_id);
    Ok(())
}

pub fn default_receiver_deposit() -> NearToken {
    DEFAULT_RECEIVER_DEPOSIT
}

pub fn default_faucet_wait() -> Duration {
    DEFAULT_FAUCET_WAIT
}
