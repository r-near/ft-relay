use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::transfer_states::{ReadyToSend, Transfer};

/// Trait for types that can be serialized/deserialized to/from Redis Streams
pub trait StreamMessage: Sized {
    /// The field name used in Redis Stream entries
    fn field_name() -> &'static str;

    /// Serialize the message to a string
    fn serialize(&self) -> Result<String>;

    /// Deserialize the message from a string
    fn deserialize(s: &str) -> Result<Self>;
}

/// Generic Redis Stream queue with consumer group support
/// Handles XREADGROUP, XAUTOCLAIM, and XACK operations
#[derive(Clone)]
pub struct StreamQueue<T: StreamMessage> {
    client: Client,
    conn: Arc<Mutex<MultiplexedConnection>>,
    stream_key: String,
    consumer_group: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: StreamMessage> StreamQueue<T> {
    pub async fn new(
        redis_url: &str,
        stream_key: String,
        consumer_group: String,
    ) -> Result<Self> {
        let client = Client::open(redis_url)?;
        let conn = client.get_multiplexed_async_connection().await?;

        let queue = Self {
            client,
            conn: Arc::new(Mutex::new(conn)),
            stream_key,
            consumer_group,
            _phantom: std::marker::PhantomData,
        };

        // Ensure consumer group exists
        queue.ensure_consumer_group().await?;

        Ok(queue)
    }

    async fn ensure_consumer_group(&self) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let _: Result<String, _> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream_key)
            .arg(&self.consumer_group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut *conn)
            .await;
        Ok(())
    }

    /// Push a message to the stream
    pub async fn push(&self, message: &T) -> Result<()> {
        let serialized = message.serialize()?;
        let mut conn = self.conn.lock().await;
        conn.xadd::<_, _, _, _, ()>(
            &self.stream_key,
            "*",
            &[(T::field_name(), serialized.as_str())],
        )
        .await?;
        Ok(())
    }

    /// Pop a batch of messages from the stream
    /// Implements autoclaim from PEL + read new messages with optional lingering
    pub async fn pop_batch(
        &self,
        consumer_name: &str,
        count: usize,
        linger_ms: u64,
    ) -> Result<Vec<(String, T)>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        // First, try to autoclaim idle messages from PEL (idle > 30s)
        let mut collected = self.autoclaim_from_pel(&mut conn, consumer_name, count).await?;

        // If we got enough from PEL, return early
        if collected.len() >= count {
            return Ok(collected);
        }

        // Read whatever is available right now (minimal block time)
        let remaining = count.saturating_sub(collected.len());
        let mut more = self
            .read_new_messages(&mut conn, consumer_name, remaining, 1)
            .await?;
        collected.append(&mut more);

        // If batch is full, return immediately
        if collected.len() >= count {
            return Ok(collected);
        }

        // If we got some messages, wait linger time then read more
        if !collected.is_empty() {
            tokio::time::sleep(Duration::from_millis(linger_ms)).await;
            let remaining = count.saturating_sub(collected.len());
            let mut more = self
                .read_new_messages(&mut conn, consumer_name, remaining, 1)
                .await?;
            collected.append(&mut more);
        }

        // Return what we have (might be empty)
        // If empty, worker loop will retry and we'll batch properly on next iteration
        Ok(collected)
    }

    /// Autoclaim idle messages from PEL
    async fn autoclaim_from_pel(
        &self,
        conn: &mut MultiplexedConnection,
        consumer_name: &str,
        count: usize,
    ) -> Result<Vec<(String, T)>> {
        let mut messages = Vec::new();

        let autoclaim_result: redis::RedisResult<redis::Value> = redis::cmd("XAUTOCLAIM")
            .arg(&self.stream_key)
            .arg(&self.consumer_group)
            .arg(consumer_name)
            .arg(30000) // Min idle time: 30s
            .arg("0-0")
            .arg("COUNT")
            .arg(count)
            .query_async(conn)
            .await;

        match &autoclaim_result {
            Ok(redis::Value::Array(ref parts)) => {
                if let Some(redis::Value::Array(entries)) = parts.get(1) {
                    if !entries.is_empty() {
                        log::info!(
                            "XAUTOCLAIM found {} idle messages in {}",
                            entries.len(),
                            self.stream_key
                        );
                    }
                }
            }
            Err(e) => {
                log::warn!("XAUTOCLAIM failed for {}: {:?}", self.stream_key, e);
            }
            _ => {}
        }

        if let Ok(redis::Value::Array(ref parts)) = autoclaim_result {
            if let Some(redis::Value::Array(entries)) = parts.get(1) {
                for entry in entries {
                    if let redis::Value::Array(ref pair) = entry {
                        let id = pair.first().and_then(|v| {
                            if let redis::Value::BulkString(b) = v {
                                std::str::from_utf8(b).ok()
                            } else {
                                None
                            }
                        });
                        let fields = pair.get(1).and_then(|v| {
                            if let redis::Value::Array(f) = v {
                                Some(f)
                            } else {
                                None
                            }
                        });

                        if let (Some(id), Some(fields)) = (id, fields) {
                            for chunk in fields.chunks(2) {
                                if let [redis::Value::BulkString(k), redis::Value::BulkString(v)] =
                                    chunk
                                {
                                    if k == T::field_name().as_bytes() {
                                        if let Ok(s) = std::str::from_utf8(v) {
                                            if let Ok(message) = T::deserialize(s) {
                                                messages.push((id.to_string(), message));
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(messages)
    }

    /// Read new messages from stream (not from PEL)
    async fn read_new_messages(
        &self,
        conn: &mut MultiplexedConnection,
        consumer_name: &str,
        count: usize,
        block_ms: u64,
    ) -> Result<Vec<(String, T)>> {
        let mut messages = Vec::new();

        let result: redis::RedisResult<redis::streams::StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.consumer_group)
            .arg(consumer_name)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(block_ms as usize)
            .arg("STREAMS")
            .arg(&self.stream_key)
            .arg(">")
            .query_async(conn)
            .await;

        if let Ok(reply) = result {
            for stream in reply.keys {
                for id_data in stream.ids {
                    if let Some(redis::Value::BulkString(bytes)) = id_data.map.get(T::field_name())
                    {
                        if let Ok(s) = std::str::from_utf8(bytes) {
                            if let Ok(message) = T::deserialize(s) {
                                messages.push((id_data.id, message));
                            }
                        }
                    }
                }
            }
        }

        Ok(messages)
    }

    /// Acknowledge processed messages
    pub async fn ack(&self, redis_ids: &[String]) -> Result<()> {
        if redis_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.lock().await;
        let mut cmd = redis::cmd("XACK");
        cmd.arg(&self.stream_key);
        cmd.arg(&self.consumer_group);
        for id in redis_ids {
            cmd.arg(id);
        }
        cmd.query_async::<()>(&mut *conn).await?;

        Ok(())
    }

    /// Get the underlying Redis client (for custom operations)
    pub fn get_client(&self) -> &Client {
        &self.client
    }

    /// Get the stream key
    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }
}

// ========================================================================
// StreamMessage implementations
// ========================================================================

/// Implementation for Transfer<ReadyToSend> - used in ready stream
impl StreamMessage for Transfer<ReadyToSend> {
    fn field_name() -> &'static str {
        "data"
    }

    fn serialize(&self) -> Result<String> {
        Ok(Transfer::<ReadyToSend>::serialize(self)?)
    }

    fn deserialize(s: &str) -> Result<Self> {
        Ok(Transfer::<ReadyToSend>::deserialize(s)?)
    }
}

/// Wrapper for registration requests (account_id strings)
#[derive(Debug, Clone)]
pub struct RegistrationRequest {
    pub account_id: String,
}

impl StreamMessage for RegistrationRequest {
    fn field_name() -> &'static str {
        "account_id"
    }

    fn serialize(&self) -> Result<String> {
        Ok(self.account_id.clone())
    }

    fn deserialize(s: &str) -> Result<Self> {
        Ok(RegistrationRequest {
            account_id: s.to_string(),
        })
    }
}
