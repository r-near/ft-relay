use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::{extract::State, routing::post, Json, Router};
use clap::Parser;
use log::{info, warn};
use near_api::{NetworkConfig, Signer, Transaction};
use near_api_types::AccountId;
use near_primitives::action::{Action, FunctionCallAction};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    sync::{mpsc, Semaphore},
    time::Duration,
};

const FT_TRANSFER_DEPOSIT: u128 = 1; // yoctoNEAR
const FT_TRANSFER_GAS_PER_ACTION: u64 = 3_000_000_000_000; // 3 Tgas (actual usage ~0.22 TGas, 10x safety margin)
const MAX_TOTAL_PREPAID_GAS: u64 = 300_000_000_000_000; // protocol hard limit per transaction
const DEFAULT_BATCH_SIZE: usize = 90; // Can fit 100 actions at 3TGas each, use 90 for safety
const DEFAULT_BATCH_LINGER_MS: u64 = 20;
const DEFAULT_MAX_INFLIGHT_BATCHES: usize = 200;

#[derive(Parser, Debug, Clone)]
pub struct CliArgs {
    /// Token (FT contract) account ID
    #[clap(long)]
    token: AccountId,

    /// RPC URL (overrides .env)
    #[clap(long)]
    rpc_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TransferBody {
    receiver_id: AccountId,
    amount: String,
    #[serde(default)]
    memo: Option<String>,
}

#[derive(Debug, Clone)]
struct TransferReq {
    receiver_id: AccountId,
    amount: String,
    memo: Option<String>,
}

#[derive(Clone)]
struct AppState {
    token: AccountId,
    signer_account: AccountId,
    // shared signer with key pool
    signer: Arc<Signer>,
    // network config
    network: NetworkConfig,
    // in-memory queue
    tx: mpsc::Sender<TransferReq>,
}

fn load_signers_from_env() -> Result<(AccountId, Vec<String>)> {
    let account_id: AccountId = std::env::var("ACCOUNT_ID")?.parse()?;
    let keys = std::env::var("PRIVATE_KEYS")?; // comma-separated: ed25519:...,ed25519:...
    let mut out = Vec::new();
    for k in keys.split(',') {
        let k = k.trim();
        if k.is_empty() {
            continue;
        }
        out.push(k.to_string());
    }
    if out.is_empty() {
        anyhow::bail!("No PRIVATE_KEYS provided");
    }
    Ok((account_id, out))
}

fn parse_env<T: std::str::FromStr>(key: &str) -> Result<Option<T>, anyhow::Error>
where
    T::Err: Into<anyhow::Error>,
{
    match std::env::var(key) {
        Ok(value) => {
            let parsed = value.parse().map_err(|e: T::Err| e.into())?;
            Ok(Some(parsed))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Configuration for the relay server
#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub token: AccountId,
    pub account_id: AccountId,
    pub secret_keys: Vec<String>,
    pub rpc_url: String,
    pub batch_size: usize,
    pub batch_linger_ms: u64,
    pub max_inflight_batches: usize,
    pub bind_addr: String,
}

impl RelayConfig {
    pub fn from_env_and_args(args: CliArgs) -> Result<Self> {
        let rpc_url = args
            .rpc_url
            .or_else(|| std::env::var("RPC_URL").ok())
            .ok_or_else(|| anyhow::anyhow!("RPC_URL not provided"))?;

        let (account_id, secret_keys) = load_signers_from_env()?;

        let batch_size = parse_env("BATCH_SIZE")?.unwrap_or(DEFAULT_BATCH_SIZE);
        let batch_linger_ms = parse_env("BATCH_LINGER_MS")?.unwrap_or(DEFAULT_BATCH_LINGER_MS);
        let max_inflight_batches =
            parse_env("MAX_INFLIGHT_BATCHES")?.unwrap_or(DEFAULT_MAX_INFLIGHT_BATCHES);
        let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

        Ok(Self {
            token: args.token,
            account_id,
            secret_keys,
            rpc_url,
            batch_size,
            batch_linger_ms,
            max_inflight_batches,
            bind_addr,
        })
    }
}

/// Main server logic - can be called from tests or main()
pub async fn run(config: RelayConfig) -> Result<()> {
    let account_id = config.account_id.clone();
    let secret_keys = config.secret_keys.clone();

    // Create network config
    let network = NetworkConfig {
        rpc_endpoints: vec![near_api::RPCEndpoint::new(config.rpc_url.parse()?)],
        ..NetworkConfig::testnet()
    };

    // Create base signer from first key
    let first_key = secret_keys.first().unwrap();
    let signer = Signer::new(Signer::from_secret_key(first_key.parse()?))?;

    // Add remaining keys to the pool
    for key_str in secret_keys.iter().skip(1) {
        let key_signer = Signer::from_secret_key(key_str.parse()?);
        signer.add_signer_to_pool(key_signer).await?;
    }

    info!("Initialized signer with {} keys in pool", secret_keys.len());

    // shared queue for logical transfers
    let (tx, mut rx) = mpsc::channel::<TransferReq>(200_000);

    // server state
    let state = AppState {
        token: config.token.clone(),
        signer_account: account_id.clone(),
        signer,
        network: network.clone(),
        tx: tx.clone(),
    };

    // HTTP: POST /v1/transfer => enqueue
    let app = Router::new()
        .route(
            "/v1/transfer",
            post(
                |State(state): State<AppState>, Json(b): Json<TransferBody>| async move {
                    state
                        .tx
                        .send(TransferReq {
                            receiver_id: b.receiver_id,
                            amount: b.amount,
                            memo: b.memo,
                        })
                        .await
                        .map_err(|_| axum::http::StatusCode::SERVICE_UNAVAILABLE)?;
                    Ok::<_, axum::http::StatusCode>(Json(serde_json::json!({"status":"accepted"})))
                },
            ),
        )
        .with_state(state.clone());

    // spawn server
    let bind_addr = config.bind_addr.clone();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
        info!("listening on http://{}", listener.local_addr().unwrap());
        axum::serve(listener, app).await.unwrap();
    });

    // batcher loop: collect up to BATCH_SIZE or linger L ms, then submit
    let batch_size = config.batch_size;
    let linger_ms = config.batch_linger_ms;
    let max_inflight_batches = config.max_inflight_batches;

    let semaphore = Arc::new(Semaphore::new(max_inflight_batches));

    loop {
        // await first item, then quickly drain to fill batch
        let mut batch = Vec::with_capacity(batch_size);
        if let Some(first) = rx.recv().await {
            batch.push(first);
        }
        let start = tokio::time::Instant::now();
        while batch.len() < batch_size && start.elapsed() < Duration::from_millis(linger_ms) {
            match rx.try_recv() {
                Ok(m) => batch.push(m),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    tokio::task::yield_now().await
                }
                Err(_) => break,
            }
        }
        if batch.is_empty() {
            continue;
        }

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let signer = state.signer.clone();
        let token = state.token.clone();
        let signer_account = state.signer_account.clone();
        let network = state.network.clone();

        tokio::spawn(async move {
            if let Err(e) = transfer_batch(signer, signer_account, token, &batch, &network).await {
                warn!("batch send failed: {e:?}");
            }
            drop(permit);
        });
    }
}

/// Build & submit one transaction containing multiple ft_transfer actions.
async fn transfer_batch(
    signer: Arc<Signer>,
    signer_account: AccountId,
    token: AccountId,
    batch: &[TransferReq],
    network: &NetworkConfig,
) -> Result<()> {
    if FT_TRANSFER_GAS_PER_ACTION > MAX_TOTAL_PREPAID_GAS {
        return Err(anyhow!(
            "configured gas per action ({}) exceeds transaction prepaid gas limit ({})",
            FT_TRANSFER_GAS_PER_ACTION,
            MAX_TOTAL_PREPAID_GAS
        ));
    }

    let max_actions_per_tx = std::cmp::max(
        1,
        (MAX_TOTAL_PREPAID_GAS / FT_TRANSFER_GAS_PER_ACTION) as usize,
    );

    for (_chunk_idx, chunk) in batch.chunks(max_actions_per_tx).enumerate() {
        let mut actions = Vec::with_capacity(chunk.len());
        for r in chunk {
            let args = json!({
                "receiver_id": r.receiver_id,
                "amount": r.amount,
                "memo": r.memo
            })
            .to_string()
            .into_bytes();

            actions.push(Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ft_transfer".to_string(),
                args,
                gas: FT_TRANSFER_GAS_PER_ACTION,
                deposit: FT_TRANSFER_DEPOSIT,
            })));
        }

        let tx = Transaction::construct(signer_account.clone(), token.clone())
            .add_actions(actions)
            .with_signer(signer.clone())
            .send_to(network)
            .await?;

        info!(
            "tx submitted: {} (actions={})",
            tx.transaction_outcome.id,
            chunk.len()
        );
    }

    Ok(())
}
