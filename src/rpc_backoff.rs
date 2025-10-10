use redis::AsyncCommands;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use log::{warn, info, debug};

const RATE_LIMIT_KEY: &str = "rpc:rate_limit_hits";
const BACKOFF_UNTIL_KEY: &str = "rpc:backoff_until";
const BACKOFF_LEVEL_KEY: &str = "rpc:backoff_level";
const ERROR_WINDOW_SECS: u64 = 10; // Track errors in 10-second windows
const MAX_BACKOFF_SECS: u64 = 120; // Max 2 minutes backoff
const MIN_BACKOFF_SECS: u64 = 1;

/// Record that we hit a rate limit error
pub async fn record_rate_limit_hit(
    conn: &mut redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    let now = now_timestamp();
    
    // Add to sorted set with timestamp as score
    conn.zadd(RATE_LIMIT_KEY, now, now).await?;
    
    // Remove old entries (older than ERROR_WINDOW_SECS)
    let cutoff = now.saturating_sub(ERROR_WINDOW_SECS);
    conn.zrembyscore(RATE_LIMIT_KEY, 0, cutoff).await?;
    
    // Count recent errors
    let error_count: u64 = conn.zcount(RATE_LIMIT_KEY, cutoff, now).await?;
    
    // Get current backoff level (accumulated hits)
    let current_level: Option<u64> = conn.get(BACKOFF_LEVEL_KEY).await?;
    let new_level = current_level.unwrap_or(0) + 1;
    
    // Calculate backoff based on BOTH error density AND accumulated level
    // This ensures we back off more aggressively as problems persist
    let backoff_secs = calculate_backoff_with_level(error_count, new_level);
    let backoff_until = now + backoff_secs;
    
    // Update backoff state with longer TTL for sustained issues
    let ttl = (backoff_secs + 30).min(300); // Max 5 min TTL
    conn.set_ex(BACKOFF_UNTIL_KEY, backoff_until, ttl).await?;
    conn.set_ex(BACKOFF_LEVEL_KEY, new_level, ttl).await?;
    
    warn!("🚦 Rate limit detected! Errors in last {}s: {}, Level: {}. Backing off for {}s", 
        ERROR_WINDOW_SECS, error_count, new_level, backoff_secs);
    
    Ok(())
}

/// Check if we should backoff, and if so, sleep until backoff expires
/// Returns true if we waited, false if no backoff needed
pub async fn check_and_wait_for_backoff(
    conn: &mut redis::aio::ConnectionManager,
) -> anyhow::Result<bool> {
    let backoff_until: Option<u64> = conn.get(BACKOFF_UNTIL_KEY).await?;
    
    if let Some(until_timestamp) = backoff_until {
        let now = now_timestamp();
        
        if now < until_timestamp {
            let wait_secs = until_timestamp - now;
            debug!("⏸️  RPC backoff active, waiting {}s", wait_secs);
            tokio::time::sleep(Duration::from_secs(wait_secs)).await;
            return Ok(true);
        }
    }
    
    Ok(false)
}

/// Record a successful RPC call to help with recovery
pub async fn record_success(
    conn: &mut redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    // Slowly decrement backoff level on success
    // We decrement slower for high levels to avoid yo-yo behavior
    let current_level: Option<i64> = conn.get(BACKOFF_LEVEL_KEY).await?;
    
    if let Some(level) = current_level {
        if level > 0 {
            // Decrement more aggressively at lower levels, slower at higher levels
            let decrement = if level < 50 {
                1
            } else if level < 150 {
                level / 50  // Decrement by ~1-2 for mid levels
            } else {
                level / 30  // Decrement by ~5-10 for high levels
            };
            
            let new_level = (level - decrement).max(0);
            
            if new_level == 0 {
                // Fully recovered
                conn.del(BACKOFF_LEVEL_KEY).await?;
                conn.del(BACKOFF_UNTIL_KEY).await?;
                info!("✅ RPC backoff recovered - resuming normal operations");
            } else {
                conn.set_ex(BACKOFF_LEVEL_KEY, new_level, 60).await?;
            }
        }
    }
    
    Ok(())
}

/// Calculate backoff duration based on error count AND accumulated level
/// The level tracks how many times we've hit rate limits total, allowing
/// us to back off much more aggressively for sustained issues
fn calculate_backoff_with_level(error_count: u64, level: u64) -> u64 {
    // Base backoff from recent error density
    let density_backoff = if error_count <= 3 {
        2
    } else if error_count <= 7 {
        5
    } else if error_count <= 15 {
        10
    } else {
        20
    };
    
    // Level-based multiplier for sustained issues
    // Level grows with each rate limit hit, so we back off more aggressively over time
    let level_multiplier = if level < 10 {
        1
    } else if level < 30 {
        2  // 10-29 hits: 2x backoff
    } else if level < 50 {
        3  // 30-49 hits: 3x backoff
    } else if level < 100 {
        5  // 50-99 hits: 5x backoff
    } else if level < 200 {
        8  // 100-199 hits: 8x backoff
    } else {
        12 // 200+ hits: 12x backoff
    };
    
    let backoff_secs = density_backoff * level_multiplier;
    
    // Cap at max, but allow at least MIN
    backoff_secs.max(MIN_BACKOFF_SECS).min(MAX_BACKOFF_SECS)
}

fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_backoff_calculation() {
        // Early levels (1-9): minimal backoff
        assert_eq!(calculate_backoff_with_level(5, 1), 2);  // 2s base, 1x = 2s
        assert_eq!(calculate_backoff_with_level(8, 5), 5);  // 5s base, 1x = 5s
        
        // Mid levels (10-49): moderate backoff
        assert_eq!(calculate_backoff_with_level(5, 20), 10); // 5s base, 2x = 10s
        assert_eq!(calculate_backoff_with_level(8, 40), 15); // 5s base, 3x = 15s
        
        // Higher levels (50-99): aggressive backoff
        assert_eq!(calculate_backoff_with_level(8, 75), 25); // 5s base, 5x = 25s
        
        // Very high levels (100-199): very aggressive
        assert_eq!(calculate_backoff_with_level(8, 150), 40); // 5s base, 8x = 40s
        
        // Extreme levels (200+): maximum aggression
        assert_eq!(calculate_backoff_with_level(8, 250), 60); // 5s base, 12x = 60s
        assert_eq!(calculate_backoff_with_level(20, 250), 120); // 20s base, 12x = 120s (capped)
    }
}
