use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::transfer_states::{Transfer, PendingRegistration, ReadyToSend};

/// Multi-stream queue architecture:
/// 1. pending:{token}:{account_id} - List of Transfer<PendingRegistration> per account
/// 2. needs_registration:{token} - Set of account_ids needing registration
/// 3. registered:{token} - Set of registered account_ids
/// 4. ready:{token} - Stream of Transfer<ReadyToSend>
/// 5. ready:{token}:group - Consumer group for ready stream
#[derive(Clone)]
pub struct TransferQueue {
    client: Client,
    conn: Arc<Mutex<MultiplexedConnection>>,
    token: String,
    ready_stream_key: String,
    ready_consumer_group: String,
}

impl TransferQueue {
    /// Get the underlying Redis client (for custom operations)
    pub fn get_client(&self) -> &Client {
        &self.client
    }

    pub async fn new(
        redis_url: &str,
        token: &str,
        ready_stream_key: &str,
        ready_consumer_group: &str,
    ) -> Result<Self> {
        let client = Client::open(redis_url)?;
        let conn = client.get_multiplexed_async_connection().await?;

        let queue = Self {
            client,
            conn: Arc::new(Mutex::new(conn)),
            token: token.to_string(),
            ready_stream_key: ready_stream_key.to_string(),
            ready_consumer_group: ready_consumer_group.to_string(),
        };

        // Ensure consumer group exists
        queue.ensure_consumer_group().await?;

        Ok(queue)
    }

    async fn ensure_consumer_group(&self) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let _: Result<String, _> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.ready_stream_key)
            .arg(&self.ready_consumer_group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut *conn)
            .await;
        Ok(())
    }

    // ========================================================================
    // Pending registration operations
    // ========================================================================

    /// Check if an account is already registered
    pub async fn is_registered(&self, account_id: &str) -> Result<bool> {
        let key = format!("registered:{}", self.token);
        let mut conn = self.conn.lock().await;
        let is_member: bool = conn.sismember(&key, account_id).await?;
        Ok(is_member)
    }

    /// Push a transfer to pending list and mark account as needing registration
    pub async fn push_pending(&self, transfer: &Transfer<PendingRegistration>) -> Result<()> {
        let account_id = transfer.data().receiver_id.to_string();
        let pending_key = format!("pending:{}:{}", self.token, account_id);
        let needs_reg_key = format!("needs_registration:{}", self.token);

        let serialized = transfer.serialize()?;
        let mut conn = self.conn.lock().await;

        // Use pipeline for atomicity
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.rpush(&pending_key, &serialized);  // Add to pending list
        pipe.sadd(&needs_reg_key, &account_id);  // Add to needs_registration set
        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    /// Get all accounts that need registration
    pub async fn get_accounts_needing_registration(&self) -> Result<Vec<String>> {
        let key = format!("needs_registration:{}", self.token);
        let mut conn = self.conn.lock().await;
        let accounts: Vec<String> = conn.smembers(&key).await?;
        Ok(accounts)
    }

    /// Get all pending transfers for an account (and remove from pending list)
    pub async fn pop_pending_transfers(&self, account_id: &str) -> Result<Vec<Transfer<PendingRegistration>>> {
        let pending_key = format!("pending:{}:{}", self.token, account_id);
        let mut conn = self.conn.lock().await;

        // Get all pending transfers and delete the list
        let serialized_list: Vec<String> = redis::cmd("LRANGE")
            .arg(&pending_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut *conn)
            .await?;

        // Delete the pending list
        conn.del::<_, ()>(&pending_key).await?;

        // Deserialize all transfers
        let mut transfers = Vec::new();
        for s in serialized_list {
            if let Ok(transfer) = Transfer::<PendingRegistration>::deserialize(&s) {
                transfers.push(transfer);
            }
        }

        Ok(transfers)
    }

    /// Mark account as registered and enqueue pending transfers to ready stream
    pub async fn mark_registered_and_enqueue(
        &self,
        account_id: &str,
        transfers: Vec<Transfer<ReadyToSend>>,
    ) -> Result<()> {
        let registered_key = format!("registered:{}", self.token);
        let needs_reg_key = format!("needs_registration:{}", self.token);

        let mut conn = self.conn.lock().await;

        // Use pipeline for atomicity
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.sadd(&registered_key, account_id);  // Mark as registered
        pipe.srem(&needs_reg_key, account_id);  // Remove from needs_registration

        // Enqueue all transfers to ready stream
        for transfer in &transfers {
            let serialized = transfer.serialize()?;
            pipe.xadd(
                &self.ready_stream_key,
                "*",
                &[("data", serialized.as_str())],
            );
        }

        pipe.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    // ========================================================================
    // Ready transfer operations (for already-registered accounts)
    // ========================================================================

    /// Push a transfer directly to ready stream (for already-registered accounts)
    pub async fn push_ready(&self, transfer: &Transfer<ReadyToSend>) -> Result<()> {
        let serialized = transfer.serialize()?;
        let mut conn = self.conn.lock().await;

        conn.xadd::<_, _, _, _, ()>(
            &self.ready_stream_key,
            "*",
            &[("data", &serialized)],
        ).await?;

        Ok(())
    }

    /// Pop a batch of ready transfers from the stream
    /// Simple linger batching: read now, wait, read again
    pub async fn pop_ready_batch(
        &self,
        consumer_name: &str,
        count: usize,
        linger_ms: u64,
    ) -> Result<Vec<(String, Transfer<ReadyToSend>)>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        // Read whatever is available right now (non-blocking)
        let mut collected = self.read_transfers(&mut conn, consumer_name, count, 0).await?;

        // If batch is full, return immediately
        if collected.len() >= count {
            return Ok(collected);
        }

        // If we got some messages, wait linger time then read more
        if !collected.is_empty() {
            tokio::time::sleep(Duration::from_millis(linger_ms)).await;
            let remaining = count.saturating_sub(collected.len());
            let more = self.read_transfers(&mut conn, consumer_name, remaining, 0).await?;
            collected.extend(more);
            return Ok(collected);
        }

        // Stream was empty - do a blocking wait
        collected = self.read_transfers(&mut conn, consumer_name, count, linger_ms).await?;
        Ok(collected)
    }

    /// Helper to read transfers from stream
    async fn read_transfers(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        consumer_name: &str,
        count: usize,
        block_ms: u64,
    ) -> Result<Vec<(String, Transfer<ReadyToSend>)>> {
        let result: redis::RedisResult<redis::streams::StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.ready_consumer_group)
            .arg(consumer_name)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(block_ms as usize)
            .arg("STREAMS")
            .arg(&self.ready_stream_key)
            .arg(">")
            .query_async(conn)
            .await;

        let mut transfers = Vec::new();
        if let Ok(reply) = result {
            for stream in reply.keys {
                for id_data in stream.ids {
                    if let Some(redis::Value::BulkString(bytes)) = id_data.map.get("data") {
                        if let Ok(s) = std::str::from_utf8(bytes) {
                            if let Ok(transfer) = Transfer::<ReadyToSend>::deserialize(s) {
                                transfers.push((id_data.id, transfer));
                            }
                        }
                    }
                }
            }
        }
        Ok(transfers)
    }

    /// Acknowledge processed transfers
    pub async fn ack(&self, redis_ids: &[String]) -> Result<()> {
        if redis_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.lock().await;
        let mut cmd = redis::cmd("XACK");
        cmd.arg(&self.ready_stream_key);
        cmd.arg(&self.ready_consumer_group);
        for id in redis_ids {
            cmd.arg(id);
        }
        cmd.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    /// Set transfer status (for tracking/debugging)
    pub async fn set_status(&self, transfer_ids: &[String], tx_hash: &str) -> Result<()> {
        if transfer_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.lock().await;
        for transfer_id in transfer_ids {
            let key = format!("status:{}:{}", self.token, transfer_id);
            conn.set_ex::<_, _, ()>(&key, tx_hash, 86400).await?; // 24h TTL
        }

        Ok(())
    }
}
