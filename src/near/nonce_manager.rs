use ::redis::aio::ConnectionManager;
use ::redis::AsyncCommands;
use anyhow::Result;
use log::debug;

#[derive(Clone)]
pub struct NonceManager {
    redis: ConnectionManager,
}

impl NonceManager {
    pub fn new(redis: ConnectionManager) -> Self {
        Self { redis }
    }

    pub async fn get_next_nonce(&mut self, key_id: &str) -> Result<u64> {
        let nonce_key = format!("nonce:{}", key_id);
        let nonce: u64 = self.redis.incr(&nonce_key, 1).await?;
        Ok(nonce)
    }

    pub async fn initialize_nonce(&mut self, key_id: &str, initial_nonce: u64) -> Result<()> {
        let nonce_key = format!("nonce:{}", key_id);

        let set: bool = ::redis::cmd("SET")
            .arg(&nonce_key)
            .arg(initial_nonce)
            .arg("NX")
            .query_async(&mut self.redis)
            .await?;

        if set {
            debug!("Initialized nonce for key {} to {}", key_id, initial_nonce);
        }

        Ok(())
    }

    pub async fn is_initialized(&mut self, key_id: &str) -> Result<bool> {
        let nonce_key = format!("nonce:{}", key_id);
        let exists: bool = self.redis.exists(&nonce_key).await?;
        Ok(exists)
    }
}
