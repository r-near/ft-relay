use ::redis::aio::ConnectionManager;
use anyhow::Result;
use uuid::Uuid;

pub fn consumer_name(prefix: &str) -> String {
    format!("{}-{}", prefix, Uuid::new_v4())
}

pub async fn ensure_consumer_group(
    conn: &mut ConnectionManager,
    stream_key: &str,
    consumer_group: &str,
) -> Result<()> {
    let _: Result<String, _> = ::redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream_key)
        .arg(consumer_group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    Ok(())
}
