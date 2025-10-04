use anyhow::{anyhow, Result};
use clap::Parser;
use config::{Config, Environment};
use near_api_types::AccountId;
use serde::Deserialize;

pub const FT_TRANSFER_DEPOSIT: u128 = 1; // yoctoNEAR
pub const FT_TRANSFER_GAS_PER_ACTION: u64 = 3_000_000_000_000; // 3 Tgas (~0.22Tgas actual, 10x safety)
pub const MAX_TOTAL_PREPAID_GAS: u64 = 300_000_000_000_000; // protocol hard limit per transaction
pub const DEFAULT_BATCH_SIZE: usize = 90; // Fits safely under 300 TGas
pub const DEFAULT_BATCH_LINGER_MS: u64 = 20;
pub const DEFAULT_MAX_INFLIGHT_BATCHES: usize = 200;
pub const DEFAULT_MAX_WORKERS: usize = 3;

pub const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
pub const DEFAULT_REDIS_STREAM_KEY: &str = "ftrelay:pending";
pub const DEFAULT_REDIS_CONSUMER_GROUP: &str = "ftrelay:batcher";

#[derive(Parser, Debug, Clone)]
pub struct CliArgs {
    /// Token (FT contract) account ID
    #[clap(long)]
    pub token: AccountId,

    /// RPC URL (overrides .env)
    #[clap(long)]
    pub rpc_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub url: String,
    pub stream_key: String,
    pub consumer_group: String,
}

#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub token: AccountId,
    pub account_id: AccountId,
    pub secret_keys: Vec<String>,
    pub rpc_url: String,
    pub batch_size: usize,
    pub batch_linger_ms: u64,
    pub max_inflight_batches: usize,
    pub max_workers: usize,
    pub bind_addr: String,
    pub redis: RedisConfig,
}

#[derive(Debug, Deserialize)]
struct RawSettings {
    account_id: AccountId,
    private_keys: String,
    rpc_url: Option<String>,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    #[serde(default = "default_batch_linger_ms")]
    batch_linger_ms: u64,
    #[serde(default = "default_max_inflight_batches")]
    max_inflight_batches: usize,
    #[serde(default = "default_max_workers")]
    max_workers: usize,
    #[serde(default = "default_bind_addr")]
    bind_addr: String,
    #[serde(default = "default_redis_url")]
    redis_url: String,
    #[serde(default = "default_redis_stream_key")]
    redis_stream_key: String,
    #[serde(default = "default_redis_consumer_group")]
    redis_consumer_group: String,
}

impl RelayConfig {
    pub fn from_env_and_args(args: CliArgs) -> Result<Self> {
        let settings = load_settings()?;

        let rpc_url = args
            .rpc_url
            .or(settings.rpc_url)
            .ok_or_else(|| anyhow!("RPC_URL not provided"))?;

        let secret_keys = parse_secret_keys(&settings.private_keys)?;

        let redis = RedisConfig {
            url: settings.redis_url,
            stream_key: settings.redis_stream_key,
            consumer_group: settings.redis_consumer_group,
        };

        Ok(Self {
            token: args.token,
            account_id: settings.account_id,
            secret_keys,
            rpc_url,
            batch_size: settings.batch_size,
            batch_linger_ms: settings.batch_linger_ms,
            max_inflight_batches: settings.max_inflight_batches,
            max_workers: settings.max_workers,
            bind_addr: settings.bind_addr,
            redis,
        })
    }
}

fn load_settings() -> Result<RawSettings> {
    let config = Config::builder()
        .add_source(Environment::default().list_separator(","))
        .build()?;

    Ok(config.try_deserialize::<RawSettings>()?)
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

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_batch_linger_ms() -> u64 {
    DEFAULT_BATCH_LINGER_MS
}

fn default_max_inflight_batches() -> usize {
    DEFAULT_MAX_INFLIGHT_BATCHES
}

fn default_max_workers() -> usize {
    DEFAULT_MAX_WORKERS
}

fn default_bind_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_redis_url() -> String {
    DEFAULT_REDIS_URL.to_string()
}

fn default_redis_stream_key() -> String {
    DEFAULT_REDIS_STREAM_KEY.to_string()
}

fn default_redis_consumer_group() -> String {
    DEFAULT_REDIS_CONSUMER_GROUP.to_string()
}
