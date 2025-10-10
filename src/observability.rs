use anyhow::Result;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Unique identifier for a batch of transfers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchId(Uuid);

impl BatchId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for BatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

/// Transaction status tracked in Redis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxStatus {
    pub tx_hash: String,
    pub batch_id: String,
    pub transfer_count: usize,
    pub status: TxStatusKind,
    pub submitted_at: i64,
    pub completed_at: Option<i64>,
    pub error: Option<String>,
    pub verified_onchain: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxStatusKind {
    Submitted,
    Succeeded,
    Timeout,
    Failed,
}

/// Audit log entry
#[derive(Debug, Serialize)]
pub struct AuditEvent {
    pub event: String,
    pub timestamp: i64,
    pub tx_hash: Option<String>,
    pub batch_id: String,
    pub transfer_count: usize,
    pub details: serde_json::Value,
}

/// Metrics aggregator
#[derive(Debug, Default, Clone)]
pub struct Metrics {
    pub submitted: u64,
    pub succeeded: u64,
    pub timeout: u64,
    pub retried: u64,
    pub duplicates: u64,
    pub failed: u64,
}

/// Observability context shared across workers
pub struct ObservabilityContext {
    pub redis_client: redis::Client,
    pub metrics: Arc<RwLock<Metrics>>,
    pub audit_log: Arc<RwLock<Vec<AuditEvent>>>, // In production, write to file
}

impl ObservabilityContext {
    pub fn new(redis_client: redis::Client) -> Self {
        Self {
            redis_client,
            metrics: Arc::new(RwLock::new(Metrics::default())),
            audit_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Log an audit event
    pub async fn audit(&self, event: AuditEvent) {
        // In production, write to a file or send to logging service
        // For now, just collect in memory
        let json = serde_json::to_string(&event).unwrap_or_default();
        log::info!("[AUDIT] {}", json);
        
        let mut log = self.audit_log.write().await;
        log.push(event);
    }

    /// Track transaction status in Redis
    pub async fn track_tx_status(&self, status: TxStatus) -> Result<()> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        
        let key = format!("tx:status:{}", status.tx_hash);
        let value = serde_json::to_string(&status)?;
        
        conn.set_ex::<_, _, ()>(&key, value, 86400).await?; // 24h TTL
        
        // Also store reverse lookup: batch_id -> tx_hash
        let batch_key = format!("tx:status:batch:{}", status.batch_id);
        conn.set_ex::<_, _, ()>(&batch_key, &status.tx_hash, 86400).await?;
        
        Ok(())
    }

    /// Track retry relationship
    pub async fn track_retry(&self, batch_id: &BatchId, tx_hash: &str) -> Result<()> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        
        let key = format!("batch:retries:{}", batch_id);
        conn.rpush::<_, _, ()>(&key, tx_hash).await?;
        conn.expire::<_, ()>(&key, 86400).await?; // 24h TTL
        
        Ok(())
    }

    /// Check if transfer already completed (duplicate detection)
    pub async fn check_duplicate(&self, transfer_id: &str) -> Result<Option<String>> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        
        let key = format!("transfer:completed:{}", transfer_id);
        let existing: Option<String> = conn.get(&key).await?;
        
        Ok(existing)
    }

    /// Mark transfer as completed
    pub async fn mark_completed(&self, transfer_id: &str, tx_hash: &str) -> Result<()> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        
        let key = format!("transfer:completed:{}", transfer_id);
        conn.set_ex::<_, _, ()>(&key, tx_hash, 86400).await?; // 24h TTL
        
        Ok(())
    }

    /// Batch check and mark transfers as completed (connection-efficient)
    /// Returns list of (transfer_id, existing_tx_hash) for duplicates found
    pub async fn check_and_mark_batch(
        &self,
        transfers: &[(String, String)], // (transfer_id, tx_hash)
    ) -> Result<Vec<(String, String)>> {
        use redis::AsyncCommands;
        
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        let mut duplicates = Vec::new();
        
        // First, check all for duplicates
        for (transfer_id, _) in transfers {
            let key = format!("transfer:completed:{}", transfer_id);
            if let Ok(Some(existing_tx)) = conn.get::<_, Option<String>>(&key).await {
                duplicates.push((transfer_id.clone(), existing_tx));
            }
        }
        
        // Then mark all as completed using pipeline for efficiency
        let mut pipe = redis::pipe();
        for (transfer_id, tx_hash) in transfers {
            let key = format!("transfer:completed:{}", transfer_id);
            pipe.set_ex(&key, tx_hash, 86400);
        }
        pipe.query_async::<()>(&mut conn).await?;
        
        Ok(duplicates)
    }

    /// Get transaction hash for a batch_id
    pub async fn get_tx_hash_for_batch(&self, batch_id: &str) -> Option<String> {
        use redis::AsyncCommands;
        
        let mut conn = self.redis_client.get_multiplexed_async_connection().await.ok()?;
        let key = format!("tx:status:batch:{}", batch_id);
        
        // Try to find the tx_status entry
        let tx_hash: Option<String> = conn.get(&key).await.ok()?;
        tx_hash
    }

    /// Get all transaction statuses (for verification)
    pub async fn get_tx_statuses(&self) -> Result<Vec<TxStatus>> {
        use redis::AsyncCommands;
        
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = conn.keys("tx:status:*").await?;
        
        let mut statuses = Vec::new();
        for key in keys {
            // Skip batch lookup keys
            if key.contains(":batch:") {
                continue;
            }
            
            if let Ok(Some(value)) = conn.get::<_, Option<String>>(&key).await {
                if let Ok(status) = serde_json::from_str::<TxStatus>(&value) {
                    statuses.push(status);
                }
            }
        }
        
        Ok(statuses)
    }

    /// Increment metric counter
    pub async fn inc_metric(&self, metric: &str) {
        let mut metrics = self.metrics.write().await;
        match metric {
            "submitted" => metrics.submitted += 1,
            "succeeded" => metrics.succeeded += 1,
            "timeout" => metrics.timeout += 1,
            "retried" => metrics.retried += 1,
            "duplicates" => metrics.duplicates += 1,
            "failed" => metrics.failed += 1,
            _ => {}
        }
    }

    /// Get current metrics snapshot
    pub async fn get_metrics(&self) -> Metrics {
        self.metrics.read().await.clone()
    }

    /// Print summary to logs
    pub async fn log_summary(&self) {
        let metrics = self.get_metrics().await;
        log::info!(
            "[SUMMARY] submitted={}, succeeded={}, timeout={}, retried={}, duplicates={}, failed={}",
            metrics.submitted,
            metrics.succeeded,
            metrics.timeout,
            metrics.retried,
            metrics.duplicates,
            metrics.failed
        );
    }
}

/// Macro for easy audit logging
#[macro_export]
macro_rules! audit_log {
    ($ctx:expr, $event:expr, $batch_id:expr, $transfer_count:expr, $details:expr) => {
        $ctx.audit(AuditEvent {
            event: $event.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            tx_hash: None,
            batch_id: $batch_id.to_string(),
            transfer_count: $transfer_count,
            details: serde_json::json!($details),
        })
        .await;
    };

    ($ctx:expr, $event:expr, $tx_hash:expr, $batch_id:expr, $transfer_count:expr, $details:expr) => {
        $ctx.audit(AuditEvent {
            event: $event.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            tx_hash: Some($tx_hash.to_string()),
            batch_id: $batch_id.to_string(),
            transfer_count: $transfer_count,
            details: serde_json::json!($details),
        })
        .await;
    };
}

/// Periodic summary logger task
pub async fn summary_logger_task(ctx: Arc<ObservabilityContext>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
    
    loop {
        interval.tick().await;
        ctx.log_summary().await;
    }
}
