use anyhow::{anyhow, Result};
use clap::Parser;
use config::{Config, Environment};
use serde::Deserialize;

use crate::types::AccountId;

pub const FT_TRANSFER_DEPOSIT: u128 = 1;
pub const STORAGE_DEPOSIT_AMOUNT: u128 = 1_250_000_000_000_000_000_000;
pub const FT_TRANSFER_GAS_PER_ACTION: u64 = 3_000_000_000_000;
pub const STORAGE_DEPOSIT_GAS_PER_ACTION: u64 = 5_000_000_000_000;
pub const DEFAULT_BATCH_LINGER_MS: u64 = 20;
pub const DEFAULT_TRANSFER_WORKERS: usize = 1;
pub const DEFAULT_REGISTRATION_WORKERS: usize = 1;
pub const DEFAULT_VERIFICATION_WORKERS: usize = 1;

pub const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";

#[derive(Debug, Clone)]
pub struct RedisSettings {
    pub url: String,
}

impl RedisSettings {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl Default for RedisSettings {
    fn default() -> Self {
        Self::new(DEFAULT_REDIS_URL)
    }
}

#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub token: AccountId,
    pub account_id: AccountId,
    pub secret_keys: Vec<String>,
    pub rpc_url: String,
    pub batch_linger_ms: u64,
    pub transfer_workers: usize,
    pub registration_workers: usize,
    pub verification_workers: usize,
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
    #[arg(long)]
    pub token: String,

    #[arg(long)]
    pub rpc_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfigBuilder {
    #[serde(default)]
    token: Option<String>,
    account_id: String,
    private_keys: String,
    rpc_url: String,
    #[serde(default)]
    batch_linger_ms: Option<u64>,
    #[serde(default)]
    transfer_workers: Option<usize>,
    #[serde(default)]
    registration_workers: Option<usize>,
    #[serde(default)]
    verification_workers: Option<usize>,
    #[serde(default)]
    bind_addr: Option<String>,
    #[serde(default)]
    redis_url: Option<String>,
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
        let token = self
            .token
            .ok_or_else(|| anyhow!("FT token must be provided via --token or TOKEN env var"))?;
        let secret_keys = parse_secret_keys(&self.private_keys)?;

        let redis = RedisSettings::new(
            self.redis_url
                .unwrap_or_else(|| DEFAULT_REDIS_URL.to_string()),
        );

        Ok(RelayConfig {
            token,
            account_id: self.account_id,
            secret_keys,
            rpc_url: self.rpc_url,
            batch_linger_ms: self.batch_linger_ms.unwrap_or(DEFAULT_BATCH_LINGER_MS),
            transfer_workers: self.transfer_workers.unwrap_or(DEFAULT_TRANSFER_WORKERS),
            registration_workers: self
                .registration_workers
                .unwrap_or(DEFAULT_REGISTRATION_WORKERS),
            verification_workers: self
                .verification_workers
                .unwrap_or(DEFAULT_VERIFICATION_WORKERS),
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
