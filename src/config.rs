use anyhow::{anyhow, Result};
use clap::Parser;
use config::{Config, Environment};
use near_api_types::AccountId;
use serde::Deserialize;

pub const FT_TRANSFER_DEPOSIT: u128 = 1; // yoctoNEAR
pub const STORAGE_DEPOSIT_AMOUNT: u128 = 1_250_000_000_000_000_000_000; // 0.00125 NEAR
pub const FT_TRANSFER_GAS_PER_ACTION: u64 = 3_000_000_000_000; // 3 Tgas (~0.22Tgas actual, 10x safety)
pub const STORAGE_DEPOSIT_GAS_PER_ACTION: u64 = 5_000_000_000_000; // 5 Tgas (allows ~60 per tx)
pub const MAX_GAS_PER_TX: u64 = 300_000_000_000_000; // 300 TGas
pub const DEFAULT_BATCH_LINGER_MS: u64 = 20;
pub const DEFAULT_MAX_INFLIGHT_BATCHES: usize = 200;
pub const DEFAULT_MAX_TRANSFER_WORKERS: usize = 3;
pub const DEFAULT_MAX_REGISTRATION_WORKERS: usize = 1;

pub const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
pub const DEFAULT_REDIS_STREAM_KEY: &str = "ftrelay:pending";
pub const DEFAULT_REDIS_CONSUMER_GROUP: &str = "ftrelay:batcher";
pub const DEFAULT_REDIS_REGISTRATION_STREAM_KEY: &str = "ftrelay:registrations";
pub const DEFAULT_REDIS_REGISTRATION_CONSUMER_GROUP: &str = "ftrelay:registration_worker";

#[derive(Debug, Clone)]
pub struct RedisSettings {
    pub url: String,
    pub stream_key: String,
    pub consumer_group: String,
    pub registration_stream_key: String,
    pub registration_consumer_group: String,
}

impl RedisSettings {
    pub fn new(
        url: impl Into<String>,
        stream_key: impl Into<String>,
        consumer_group: impl Into<String>,
        registration_stream_key: impl Into<String>,
        registration_consumer_group: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            stream_key: stream_key.into(),
            consumer_group: consumer_group.into(),
            registration_stream_key: registration_stream_key.into(),
            registration_consumer_group: registration_consumer_group.into(),
        }
    }
}

impl Default for RedisSettings {
    fn default() -> Self {
        Self {
            url: DEFAULT_REDIS_URL.to_string(),
            stream_key: DEFAULT_REDIS_STREAM_KEY.to_string(),
            consumer_group: DEFAULT_REDIS_CONSUMER_GROUP.to_string(),
            registration_stream_key: DEFAULT_REDIS_REGISTRATION_STREAM_KEY.to_string(),
            registration_consumer_group: DEFAULT_REDIS_REGISTRATION_CONSUMER_GROUP.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub token: AccountId,
    pub account_id: AccountId,
    pub secret_keys: Vec<String>,
    pub rpc_url: String,
    pub batch_linger_ms: u64,
    pub max_inflight_batches: usize,
    pub max_workers: usize,
    pub bind_addr: String,
    pub redis: RedisSettings,
}

impl RelayConfig {
    pub fn load() -> Result<Self> {
        RelayConfigBuilder::from_env()?.build()
    }

    pub fn load_with_cli(args: &CliArgs) -> Result<Self> {
        RelayConfigBuilder::from_env()?.with_cli_args(args).build()
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Fungible Token Relay", long_about = None)]
pub struct CliArgs {
    /// Token (FT contract) account ID
    #[arg(long)]
    pub token: AccountId,

    /// RPC URL override (optional; defaults to environment)
    #[arg(long)]
    pub rpc_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfigBuilder {
    #[serde(default)]
    token: Option<AccountId>,
    account_id: AccountId,
    private_keys: String,
    rpc_url: String,
    #[serde(default)]
    batch_linger_ms: Option<u64>,
    #[serde(default)]
    max_inflight_batches: Option<usize>,
    #[serde(default)]
    max_workers: Option<usize>,
    #[serde(default)]
    bind_addr: Option<String>,
    #[serde(default)]
    redis_url: Option<String>,
    #[serde(default)]
    redis_stream_key: Option<String>,
    #[serde(default)]
    redis_consumer_group: Option<String>,
    #[serde(default)]
    redis_registration_stream_key: Option<String>,
    #[serde(default)]
    redis_registration_consumer_group: Option<String>,
}

impl RelayConfigBuilder {
    pub fn from_env() -> Result<Self> {
        let config = Config::builder()
            .add_source(Environment::default().list_separator(","))
            .build()?;

        Ok(config.try_deserialize::<Self>()?)
    }

    pub fn with_cli_args(mut self, args: &CliArgs) -> Self {
        self.token = Some(args.token.clone());
        if let Some(rpc) = &args.rpc_url {
            self.rpc_url = rpc.clone();
        }
        self
    }

    pub fn build(self) -> Result<RelayConfig> {
        let RelayConfigBuilder {
            token,
            account_id,
            private_keys,
            rpc_url,
            batch_linger_ms,
            max_inflight_batches,
            max_workers,
            bind_addr,
            redis_url,
            redis_stream_key,
            redis_consumer_group,
            redis_registration_stream_key,
            redis_registration_consumer_group,
        } = self;

        let token = token
            .ok_or_else(|| anyhow!("FT token must be provided via --token or TOKEN env var"))?;
        let secret_keys = parse_secret_keys(&private_keys)?;

        let redis = RedisSettings::new(
            redis_url.unwrap_or_else(|| DEFAULT_REDIS_URL.to_string()),
            redis_stream_key.unwrap_or_else(|| DEFAULT_REDIS_STREAM_KEY.to_string()),
            redis_consumer_group.unwrap_or_else(|| DEFAULT_REDIS_CONSUMER_GROUP.to_string()),
            redis_registration_stream_key
                .unwrap_or_else(|| DEFAULT_REDIS_REGISTRATION_STREAM_KEY.to_string()),
            redis_registration_consumer_group
                .unwrap_or_else(|| DEFAULT_REDIS_REGISTRATION_CONSUMER_GROUP.to_string()),
        );

        Ok(RelayConfig {
            token,
            account_id,
            secret_keys,
            rpc_url,
            batch_linger_ms: batch_linger_ms.unwrap_or(DEFAULT_BATCH_LINGER_MS),
            max_inflight_batches: max_inflight_batches.unwrap_or(DEFAULT_MAX_INFLIGHT_BATCHES),
            max_workers: max_workers.unwrap_or(DEFAULT_MAX_TRANSFER_WORKERS),
            bind_addr: bind_addr.unwrap_or_else(|| "0.0.0.0:8080".to_string()),
            redis,
        })
    }
}

fn parse_secret_keys(keys: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for key in keys.split(',') {
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        out.push(key.to_string());
    }
    if out.is_empty() {
        Err(anyhow!("No PRIVATE_KEYS provided"))
    } else {
        Ok(out)
    }
}

impl From<&RelayConfig> for RedisSettings {
    fn from(config: &RelayConfig) -> Self {
        config.redis.clone()
    }
}
