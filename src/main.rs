use anyhow::Result;
use clap::Parser;
use dotenv::dotenv;
use ft_relay::{CliArgs, RelayConfig};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    env_logger::init_from_env(
        env_logger::Env::default().default_filter_or("info,near_api=warn,tracing::span=warn"),
    );

    let cli = CliArgs::parse();
    let config = RelayConfig::load_with_cli(&cli)?;

    ft_relay::run(config).await
}
