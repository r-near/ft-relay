use ::redis::aio::ConnectionLike;
use ::redis::AsyncCommands;
use ::redis::RedisResult;
use anyhow::Result;
use serde::de::DeserializeOwned;

pub async fn pop_batch<C, T>(
    conn: &mut C,
    stream_key: &str,
    consumer_group: &str,
    consumer_name: &str,
    max_count: usize,
    linger_ms: u64,
) -> Result<Vec<(String, T)>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
    T: DeserializeOwned,
{
    let start = std::time::Instant::now();
    let mut batch: Vec<(String, T)> = Vec::new();
    let mut total_read_time_ms = 0u128;
    let mut read_count = 0;

    while batch.len() < max_count && start.elapsed().as_millis() < linger_ms as u128 {
        let remaining = max_count - batch.len();
        let time_left = linger_ms.saturating_sub(start.elapsed().as_millis() as u64);
        let block_ms = time_left.min(10);

        let read_start = std::time::Instant::now();
        let mut additional = read_stream_batch(
            conn,
            stream_key,
            consumer_group,
            consumer_name,
            remaining,
            block_ms,
        )
        .await?;
        let read_duration = read_start.elapsed();
        total_read_time_ms += read_duration.as_millis();
        read_count += 1;

        if !additional.is_empty() {
            batch.append(&mut additional);
        }
    }

    if !batch.is_empty() {
        log::debug!(
            "[REDIS_TIMING] pop_batch: {} msgs in {}ms total ({} reads, {}ms in XREADGROUP)",
            batch.len(),
            start.elapsed().as_millis(),
            read_count,
            total_read_time_ms
        );
    }

    Ok(batch)
}

async fn read_stream_batch<C, T>(
    conn: &mut C,
    stream_key: &str,
    consumer_group: &str,
    consumer_name: &str,
    max_count: usize,
    block_ms: u64,
) -> Result<Vec<(String, T)>>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
    T: DeserializeOwned,
{
    let result: RedisResult<::redis::streams::StreamReadReply> = ::redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(consumer_group)
        .arg(consumer_name)
        .arg("COUNT")
        .arg(max_count)
        .arg("BLOCK")
        .arg(block_ms as usize)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(conn)
        .await;

    let reply = match result {
        Ok(r) => r,
        Err(_) => return Ok(Vec::new()),
    };

    let mut batch = Vec::new();

    for stream in reply.keys {
        for id_data in stream.ids {
            let stream_id = id_data.id.clone();

            let data = match id_data.map.get("data") {
                Some(::redis::Value::BulkString(bytes)) => match std::str::from_utf8(bytes) {
                    Ok(s) => s,
                    Err(e) => {
                        log::warn!("Failed to parse stream data as UTF-8: {:?}", e);
                        ack(conn, stream_key, consumer_group, &stream_id).await?;
                        continue;
                    }
                },
                _ => {
                    log::warn!("Stream message missing 'data' field");
                    ack(conn, stream_key, consumer_group, &stream_id).await?;
                    continue;
                }
            };

            let msg: T = match serde_json::from_str(data) {
                Ok(m) => m,
                Err(e) => {
                    log::warn!("Failed to deserialize message: {:?}", e);
                    ack(conn, stream_key, consumer_group, &stream_id).await?;
                    continue;
                }
            };

            batch.push((stream_id, msg));
        }
    }

    Ok(batch)
}

async fn ack<C>(conn: &mut C, stream_key: &str, consumer_group: &str, stream_id: &str) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    let _: u64 = ::redis::cmd("XACK")
        .arg(stream_key)
        .arg(consumer_group)
        .arg(stream_id)
        .query_async(conn)
        .await?;
    Ok(())
}

pub async fn ack_message<C>(
    conn: &mut C,
    stream_key: &str,
    consumer_group: &str,
    stream_id: &str,
) -> Result<()>
where
    C: ConnectionLike + AsyncCommands + Send + Sync,
{
    ack(conn, stream_key, consumer_group, stream_id).await
}
