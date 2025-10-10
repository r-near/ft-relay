/// Fully integrated benchmark test
///
/// This test:
/// 1. Starts near-sandbox programmatically  
/// 2. Deploys the FT contract
/// 3. Starts the relay API server (with registration workers)
/// 4. Runs benchmark with actual FT transfers
/// 5. Verifies balances changed correctly (registration happens automatically)

// NOTE: This test requires near-sandbox which has complex setup requirements
// For now, use manual testing approach documented in RUN_BENCHMARK.md

#[tokio::test]
#[ignore]
async fn test_integrated_benchmark_placeholder() {
    println!("
╔════════════════════════════════════════════════════════════╗
║  Integrated Benchmark Test                                 ║
║                                                            ║
║  This test is currently disabled due to near-sandbox      ║
║  API changes. Please use the manual testing approach:     ║
║                                                            ║
║  1. Start Redis: redis-server                             ║
║  2. Set environment variables in .env                      ║
║  3. Start relay: cargo run                                 ║
║  4. In another terminal:                                   ║
║     curl -X POST http://localhost:3030/v1/transfer \\      ║
║       -H 'X-Idempotency-Key: test-1' \\                   ║
║       -H 'Content-Type: application/json' \\              ║
║       -d '{{\"receiver_id\":\"alice.testnet\",              ║
║            \"amount\":\"1000000000000000000\"}}'            ║
║                                                            ║
║  See RUN_BENCHMARK.md for detailed instructions           ║
╚════════════════════════════════════════════════════════════╝
    ");
}

// Future: Re-enable this when near-sandbox API is stable
/*
use ft_relay::{RedisSettings, RelayConfig};
use near_sandbox::{GenesisAccount, Sandbox, SandboxConfig};
use reqwest::Client;
use serde_json::json;
use std::time::{Duration, Instant};

const FT_WASM_PATH: &str = "resources/fungible_token.wasm";
const NUM_TRANSFERS: usize = 100;

#[tokio::test]
#[ignore]
async fn test_100_transfers_integrated() -> Result<(), Box<dyn std::error::Error>> {
    // ... full sandbox test here ...
    Ok(())
}
*/
