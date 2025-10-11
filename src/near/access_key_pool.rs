use ::redis::aio::ConnectionManager;
use anyhow::Result;
use log::{debug, warn};
use uuid::Uuid;

use crate::types::AccessKey;

pub struct AccessKeyPool {
    keys: Vec<AccessKey>,
    redis: ConnectionManager,
}

impl AccessKeyPool {
    pub fn new(keys: Vec<AccessKey>, redis: ConnectionManager) -> Self {
        Self { keys, redis }
    }

    pub async fn lease(&self) -> Result<LeasedKey> {
        use rand::Rng;

        loop {
            let idx = rand::rng().random_range(0..self.keys.len());
            let key = &self.keys[idx];

            let lease_key = format!("access_key:lease:{}", key.key_id);
            let token = Uuid::new_v4().to_string();

            let mut conn = self.redis.clone();
            let acquired: bool = ::redis::cmd("SET")
                .arg(&lease_key)
                .arg(&token)
                .arg("NX")
                .arg("EX")
                .arg(30)
                .query_async(&mut conn)
                .await?;

            if acquired {
                debug!("Leased access key: {}", key.key_id);
                return Ok(LeasedKey {
                    key_id: key.key_id.clone(),
                    secret_key: key.secret_key.clone(),
                    public_key: key.public_key.clone(),
                    lease_key,
                    token,
                    redis: self.redis.clone(),
                });
            }
        }
    }
}

pub struct LeasedKey {
    pub key_id: String,
    pub secret_key: near_crypto::SecretKey,
    pub public_key: near_crypto::PublicKey,
    lease_key: String,
    token: String,
    redis: ConnectionManager,
}

impl Drop for LeasedKey {
    fn drop(&mut self) {
        let lease_key = self.lease_key.clone();
        let token = self.token.clone();
        let mut redis = self.redis.clone();

        tokio::spawn(async move {
            let script = r#"
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    return redis.call("DEL", KEYS[1])
                else
                    return 0
                end
            "#;

            let result: Result<i32, _> = ::redis::Script::new(script)
                .key(&lease_key)
                .arg(&token)
                .invoke_async(&mut redis)
                .await;

            match result {
                Ok(1) => debug!("Released access key lease: {}", lease_key),
                Ok(_) => warn!("Access key lease already expired: {}", lease_key),
                Err(e) => warn!("Failed to release access key lease {}: {:?}", lease_key, e),
            }
        });
    }
}
