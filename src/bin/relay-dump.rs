use anyhow::Result;
use redis::AsyncCommands;
use serde_json::Value;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let client = redis::Client::open(redis_url.as_str())?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  RELAY STATE DUMP                                          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Get all transaction status keys
    let tx_keys: Vec<String> = conn.keys("tx:status:*").await?;
    
    let mut submitted = 0;
    let mut succeeded = 0;
    let mut timeout = 0;
    let mut failed = 0;
    let mut timeout_txs = Vec::new();
    let mut succeeded_txs = Vec::new();
    
    for key in tx_keys {
        let value: String = conn.get(&key).await?;
        if let Ok(status) = serde_json::from_str::<Value>(&value) {
            match status["status"].as_str() {
                Some("submitted") => submitted += 1,
                Some("succeeded") => {
                    succeeded += 1;
                    succeeded_txs.push(status);
                }
                Some("timeout") => {
                    timeout += 1;
                    timeout_txs.push(status);
                }
                Some("failed") => failed += 1,
                _ => {}
            }
        }
    }

    println!("📊 Transaction Summary:");
    println!("  Submitted:  {}", submitted);
    println!("  Succeeded:  {}", succeeded);
    println!("  Timeout:    {}", timeout);
    println!("  Failed:     {}", failed);
    println!("  Total:      {}\n", submitted + succeeded + timeout + failed);

    // Check for retries
    if timeout > 0 {
        println!("⏱️  Timeout Transactions:");
        for tx in &timeout_txs {
            let tx_hash = tx["tx_hash"].as_str().unwrap_or("unknown");
            let batch_id = tx["batch_id"].as_str().unwrap_or("unknown");
            let count = tx["transfer_count"].as_u64().unwrap_or(0);
            
            // Check if it was retried
            let retry_key = format!("batch:retries:{}", batch_id);
            let retries: Vec<String> = conn.lrange(&retry_key, 0, -1).await.unwrap_or_default();
            
            println!("  {} (batch: {}, {} transfers)", &tx_hash[..16], &batch_id[..8], count);
            
            if !retries.is_empty() {
                println!("    └─ Retried {} time(s):", retries.len());
                for (i, retry_tx) in retries.iter().enumerate() {
                    // Check retry outcome
                    let retry_status_key = format!("tx:status:{}", retry_tx);
                    let retry_status: Option<String> = conn.get(&retry_status_key).await.ok();
                    
                    let status_str = if let Some(s) = retry_status {
                        if let Ok(v) = serde_json::from_str::<Value>(&s) {
                            v["status"].as_str().unwrap_or("unknown").to_string()
                        } else {
                            "unknown".to_string()
                        }
                    } else {
                        "unknown".to_string()
                    };
                    
                    println!("       {}. {} → {}", i + 1, &retry_tx[..16], status_str);
                }
            } else {
                println!("    └─ No retries found");
            }
            println!();
        }
    }

    // Check for duplicates - group by transfer_id to find if same transfer completed multiple times
    let completed_keys: Vec<String> = conn.keys("transfer:completed:*").await?;
    let mut transfer_completions: HashMap<String, Vec<String>> = HashMap::new();
    
    for key in &completed_keys {
        // Extract transfer_id from "transfer:completed:{id}"
        if let Some(transfer_id) = key.strip_prefix("transfer:completed:") {
            let tx_hash: String = conn.get(key).await?;
            transfer_completions
                .entry(transfer_id.to_string())
                .or_insert_with(Vec::new)
                .push(tx_hash);
        }
    }

    // Count total completed transfers (unique transfer_ids)
    let total_completed = transfer_completions.len();
    println!("✅ Completed Transfers: {}", total_completed);

    // Find duplicates (same transfer_id completed by multiple transactions)
    let duplicates: Vec<(&String, &Vec<String>)> = transfer_completions
        .iter()
        .filter(|(_, txs)| txs.len() > 1)
        .collect();

    if !duplicates.is_empty() {
        let duplicate_count: usize = duplicates.iter().map(|(_, txs)| txs.len() - 1).sum();
        println!("⚠️  WARNING: {} duplicate transfers detected!", duplicate_count);
        println!("\nDuplicate Details (showing first 10):");
        for (transfer_id, tx_hashes) in duplicates.iter().take(10) {
            println!("  Transfer {} completed by {} transactions:", 
                &transfer_id[..16.min(transfer_id.len())], tx_hashes.len());
            for (i, tx) in tx_hashes.iter().enumerate() {
                println!("    {}. {}", i + 1, &tx[..16.min(tx.len())]);
            }
        }
    }

    println!("\n✨ Dump complete!");

    Ok(())
}
