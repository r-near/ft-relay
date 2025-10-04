use anyhow::{anyhow, Result};
use config::{Config, Environment};
use near_api_types::AccountId;
use serde::Deserialize;

pub const FT_TRANSFER_DEPOSIT: u128 = 1; // yoctoNEAR
pub const FT_TRANSFER_GAS_PER_ACTION: u64 = 3_000_000_000_000; // 3 Tgas (~0.22Tgas actual, 10x safety)
pub const DEFAULT_BATCH_SIZE: usize = 90; // Fits safely under 300 TGas
pub const DEFAULT_BATCH_LINGER_MS: u64 = 20;
pub const DEFAULT_MAX_INFLIGHT_BATCHES: usize = 200;
pub const DEFAULT_MAX_WORKERS: usize = 3;

pub const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
pub const DEFAULT_REDIS_STREAM_KEY: &str = "ftrelay:pending";
pub const DEFAULT_REDIS_CONSUMER_GROUP: &str = "ftrelay:batcher";

#[derive(Debug, Clone)]
pub struct RedisSettings {
    pub url: String,
    pub stream_key: String,
    pub consumer_group: String,
}

impl RedisSettings {
    pub fn new(
        url: impl Into<String>,
        stream_key: impl Into<String>,
        consumer_group: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            stream_key: stream_key.into(),
            consumer_group: consumer_group.into(),
        }
    }
}

impl Default for RedisSettings {
    fn default() -> Self {
        Self {
            url: DEFAULT_REDIS_URL.to_string(),
            stream_key: DEFAULT_REDIS_STREAM_KEY.to_string(),
            consumer_group: DEFAULT_REDIS_CONSUMER_GROUP.to_string(),
        }
    }
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
    pub redis: RedisSettings,
}

impl RelayConfig {
    pub fn load() -> Result<Self> {
        RelayConfigBuilder::from_env()?.build()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfigBuilder {
    token: AccountId,
    account_id: AccountId,
    private_keys: String,
    rpc_url: String,
    #[serde(default)]
    batch_size: Option<usize>,
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
}

impl RelayConfigBuilder {
    pub fn from_env() -> Result<Self> {
        let config = Config::builder()
            .add_source(Environment::default().list_separator(","))
            .build()?;

        Ok(config.try_deserialize::<Self>()?)
    }

    pub fn build(self) -> Result<RelayConfig> {
        let secret_keys = parse_secret_keys(&self.private_keys)?;

        let redis = RedisSettings::new(
            self.redis_url
                .unwrap_or_else(|| DEFAULT_REDIS_URL.to_string()),
            self.redis_stream_key
                .unwrap_or_else(|| DEFAULT_REDIS_STREAM_KEY.to_string()),
            self.redis_consumer_group
                .unwrap_or_else(|| DEFAULT_REDIS_CONSUMER_GROUP.to_string()),
        );

        Ok(RelayConfig {
            token: self.token,
            account_id: self.account_id,
            secret_keys,
            rpc_url: self.rpc_url,
            batch_size: self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
            batch_linger_ms: self.batch_linger_ms.unwrap_or(DEFAULT_BATCH_LINGER_MS),
            max_inflight_batches: self
                .max_inflight_batches
                .unwrap_or(DEFAULT_MAX_INFLIGHT_BATCHES),
            max_workers: self.max_workers.unwrap_or(DEFAULT_MAX_WORKERS),
            bind_addr: self.bind_addr.unwrap_or_else(|| "0.0.0.0:8080".to_string()),
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
