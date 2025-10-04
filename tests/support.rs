use std::time::Duration;

use anyhow::Result;
use near_api::{NetworkConfig, RPCEndpoint};

#[allow(dead_code)]
pub fn network_from_url(name: &str, rpc_url: &str) -> Result<NetworkConfig> {
    Ok(NetworkConfig {
        network_name: name.to_string(),
        rpc_endpoints: vec![RPCEndpoint::new(rpc_url.parse()?)],
        ..NetworkConfig::testnet()
    })
}

pub fn http_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .expect("reqwest client")
}
