use anyhow::{anyhow, Result};
use log::{debug, info};
use redis::aio::ConnectionManager;
use near_crypto::{PublicKey, SecretKey};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_jsonrpc_primitives::types::transactions::RpcSendTransactionRequest;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction, Transaction};
use near_primitives::types::{BlockReference, Finality};
use near_primitives::views::{
    AccessKeyView, FinalExecutionOutcomeView, FinalExecutionStatus, TxExecutionStatus,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::config::*;
use crate::types::{AccountId, RPC_CALLS};

#[derive(Debug, Clone)]
pub enum TxStatus {
    Pending,
    Success(Box<FinalExecutionOutcomeView>),
    Failed(String),
}

pub struct NearRpcClient {
    client: JsonRpcClient,
    block_hash_cache: Arc<RwLock<Option<(CryptoHash, Instant)>>>,
    redis_conn: ConnectionManager,
    env: String,
}

impl NearRpcClient {
    pub fn new(rpc_url: &str, redis_conn: ConnectionManager, env: String) -> Self {
        let client = JsonRpcClient::connect(rpc_url);
        Self {
            client,
            block_hash_cache: Arc::new(RwLock::new(None)),
            redis_conn,
            env,
        }
    }

    async fn incr_stat(&self, field: &str) {
        let key = format!("ftrelay:{}:rpc_stats", self.env);
        let mut conn = self.redis_conn.clone();
        let _ = redis::cmd("HINCRBY")
            .arg(key)
            .arg(field)
            .arg(1)
            .query_async::<()>(&mut conn)
            .await;
    }

    pub async fn get_block_hash(&self) -> Result<CryptoHash> {
        {
            let cache = self.block_hash_cache.read().await;
            if let Some((hash, cached_at)) = *cache {
                // Reduced from 3600s to 1s to prevent expiration during high load
                // Sandbox creates new block per tx, so stale hash = expired tx after ~100 blocks
                if cached_at.elapsed() < Duration::from_millis(500) {
                    debug!("Using cached block hash");
                    return Ok(hash);
                }
            }
        }

        debug!("Fetching fresh block hash from RPC");
        self.incr_stat("get_block_hash").await;
        RPC_CALLS.fetch_add(1, Ordering::Relaxed);
        let request = methods::block::RpcBlockRequest {
            block_reference: BlockReference::Finality(Finality::Final),
        };

        let response = self
            .client
            .call(request)
            .await
            .map_err(|e| anyhow!("Failed to fetch block: {:?}", e))?;

        let block_hash = response.header.hash;

        {
            let mut cache = self.block_hash_cache.write().await;
            *cache = Some((block_hash, Instant::now()));
        }

        Ok(block_hash)
    }

    pub async fn broadcast_tx(
        &self,
        signed_tx: SignedTransaction,
    ) -> Result<(CryptoHash, FinalExecutionOutcomeView)> {
        let tx_hash = signed_tx.get_hash();
        debug!("Broadcasting transaction: {}", tx_hash);
        self.incr_stat("broadcast_tx").await;

        RPC_CALLS.fetch_add(1, Ordering::Relaxed);
        let request = RpcSendTransactionRequest {
            signed_transaction: signed_tx,
            wait_until: TxExecutionStatus::Final,
        };

        let response = self
            .client
            .call(request)
            .await
            .map_err(|e| anyhow!("Failed to broadcast transaction: {:?}", e))?;

        let outcome = response
            .final_execution_outcome
            .ok_or_else(|| anyhow!("No execution outcome returned"))?
            .into_outcome();

        Ok((tx_hash, outcome))
    }

    pub async fn check_tx_status(
        &self,
        tx_hash: &CryptoHash,
        sender: &AccountId,
    ) -> Result<TxStatus> {
        self.incr_stat("check_tx_status").await;
        RPC_CALLS.fetch_add(1, Ordering::Relaxed);
        let request = methods::tx::RpcTransactionStatusRequest {
            transaction_info: methods::tx::TransactionInfo::TransactionId {
                tx_hash: *tx_hash,
                sender_account_id: sender
                    .parse()
                    .map_err(|e| anyhow!("Invalid account ID: {:?}", e))?,
            },
            wait_until: TxExecutionStatus::Final,
        };

        match self.client.call(request).await {
            Ok(response) => {
                let outcome = response
                    .final_execution_outcome
                    .ok_or_else(|| anyhow!("No execution outcome"))?;
                let outcome_view = outcome.into_outcome();
                match outcome_view.status {
                    FinalExecutionStatus::SuccessValue(_) => {
                        Ok(TxStatus::Success(Box::new(outcome_view)))
                    }
                    FinalExecutionStatus::Failure(err) => {
                        Ok(TxStatus::Failed(format!("{:?}", err)))
                    }
                    FinalExecutionStatus::NotStarted | FinalExecutionStatus::Started => {
                        Ok(TxStatus::Pending)
                    }
                }
            }
            Err(e) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("UNKNOWN_TRANSACTION") || err_str.contains("does not exist") {
                    // Transaction not found yet - still pending
                    Ok(TxStatus::Pending)
                } else if err_str.contains("TimeoutError") || err_str.contains("TIMEOUT_ERROR") {
                    // RPC timeout while waiting for finality - transaction state unknown
                    // Per NEAR docs: transaction was routed but not recorded on chain in 10 seconds
                    // Solution: resubmit the transaction (NEAR's idempotency will handle duplicates)
                    // We treat this as Failed so the verification worker will re-enqueue transfers
                    debug!("RPC timeout checking tx status - will resubmit transaction");
                    Ok(TxStatus::Failed("RPC timeout - resubmitting transaction".to_string()))
                } else {
                    Err(anyhow!("Failed to check transaction status: {:?}", e))
                }
            }
        }
    }

    pub async fn get_access_key(
        &self,
        account: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKeyView> {
        self.incr_stat("get_access_key").await;
        RPC_CALLS.fetch_add(1, Ordering::Relaxed);
        let request = methods::query::RpcQueryRequest {
            block_reference: BlockReference::Finality(Finality::Final),
            request: near_primitives::views::QueryRequest::ViewAccessKey {
                account_id: account
                    .parse()
                    .map_err(|e| anyhow!("Invalid account ID: {:?}", e))?,
                public_key: public_key.clone(),
            },
        };

        let response = self
            .client
            .call(request)
            .await
            .map_err(|e| anyhow!("Failed to fetch access key for {} / {}: {:?}", account, public_key, e))?;

        match response.kind {
            QueryResponseKind::AccessKey(access_key) => Ok(access_key),
            _ => Err(anyhow!("Unexpected response type")),
        }
    }

    /// Fetch all access keys for an account at once (much faster than individual queries)
    pub async fn get_access_key_list(
        &self,
        account: &AccountId,
    ) -> Result<Vec<near_primitives::views::AccessKeyInfoView>> {
        self.incr_stat("get_access_key_list").await;
        RPC_CALLS.fetch_add(1, Ordering::Relaxed);
        let request = methods::query::RpcQueryRequest {
            block_reference: BlockReference::Finality(Finality::Final),
            request: near_primitives::views::QueryRequest::ViewAccessKeyList {
                account_id: account
                    .parse()
                    .map_err(|e| anyhow!("Invalid account ID: {:?}", e))?,
            },
        };

        let response = self
            .client
            .call(request)
            .await
            .map_err(|e| anyhow!("Failed to fetch access key list for {}: {:?}", account, e))?;

        match response.kind {
            QueryResponseKind::AccessKeyList(access_keys) => Ok(access_keys.keys),
            _ => Err(anyhow!("Unexpected response type")),
        }
    }

    pub async fn register_account(
        &self,
        signer_account: &AccountId,
        token: &AccountId,
        account_to_register: &AccountId,
        secret_key: &SecretKey,
        nonce: u64,
    ) -> Result<(CryptoHash, FinalExecutionOutcomeView)> {
        let block_hash = self.get_block_hash().await?;

        let args = serde_json::json!({
            "account_id": account_to_register,
            "registration_only": true,
        });

        let action = Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "storage_deposit".to_string(),
            args: args.to_string().into_bytes(),
            gas: STORAGE_DEPOSIT_GAS_PER_ACTION,
            deposit: STORAGE_DEPOSIT_AMOUNT,
        }));

        let transaction = Transaction::V0(near_primitives::transaction::TransactionV0 {
            signer_id: signer_account
                .parse()
                .map_err(|e| anyhow!("Invalid signer: {:?}", e))?,
            public_key: secret_key.public_key(),
            nonce,
            receiver_id: token
                .parse()
                .map_err(|e| anyhow!("Invalid receiver: {:?}", e))?,
            block_hash,
            actions: vec![action],
        });

        let signature = secret_key.sign(transaction.get_hash_and_size().0.as_ref());
        let signed_tx = SignedTransaction::new(signature, transaction);

        self.broadcast_tx(signed_tx).await
    }

    /// Register multiple accounts in a single transaction (batch storage_deposit)
    pub async fn register_accounts_batch(
        &self,
        signer_account: &AccountId,
        token: &AccountId,
        accounts_to_register: Vec<AccountId>,
        secret_key: &SecretKey,
        nonce: u64,
    ) -> Result<(CryptoHash, FinalExecutionOutcomeView)> {
        let block_hash = self.get_block_hash().await?;

        // Create one storage_deposit action per account
        let mut actions = Vec::new();
        for account in accounts_to_register {
            let args = serde_json::json!({
                "account_id": account,
                "registration_only": true,
            });

            let action = Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "storage_deposit".to_string(),
                args: args.to_string().into_bytes(),
                gas: STORAGE_DEPOSIT_GAS_PER_ACTION,
                deposit: STORAGE_DEPOSIT_AMOUNT,
            }));

            actions.push(action);
        }

        let transaction = Transaction::V0(near_primitives::transaction::TransactionV0 {
            signer_id: signer_account
                .parse()
                .map_err(|e| anyhow!("Invalid signer: {:?}", e))?,
            public_key: secret_key.public_key(),
            nonce,
            receiver_id: token
                .parse()
                .map_err(|e| anyhow!("Invalid receiver: {:?}", e))?,
            block_hash,
            actions,
        });

        let signature = secret_key.sign(transaction.get_hash_and_size().0.as_ref());
        let signed_tx = SignedTransaction::new(signature, transaction);

        self.broadcast_tx(signed_tx).await
    }

    pub async fn submit_batch_transfer(
        &self,
        signer_account: &AccountId,
        token: &AccountId,
        receivers: Vec<(AccountId, String)>,
        secret_key: &SecretKey,
        nonce: u64,
    ) -> Result<(CryptoHash, FinalExecutionOutcomeView)> {
        let block_hash = self.get_block_hash().await?;

        let actions: Vec<Action> = receivers
            .into_iter()
            .map(|(receiver_id, amount)| {
                let args = serde_json::json!({
                    "receiver_id": receiver_id,
                    "amount": amount,
                });

                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "ft_transfer".to_string(),
                    args: args.to_string().into_bytes(),
                    gas: FT_TRANSFER_GAS_PER_ACTION,
                    deposit: FT_TRANSFER_DEPOSIT,
                }))
            })
            .collect();

        let transaction = Transaction::V0(near_primitives::transaction::TransactionV0 {
            signer_id: signer_account
                .parse()
                .map_err(|e| anyhow!("Invalid signer: {:?}", e))?,
            public_key: secret_key.public_key(),
            nonce,
            receiver_id: token
                .parse()
                .map_err(|e| anyhow!("Invalid receiver: {:?}", e))?,
            block_hash,
            actions,
        });

        let signature = secret_key.sign(transaction.get_hash_and_size().0.as_ref());
        let signed_tx = SignedTransaction::new(signature, transaction);

        self.broadcast_tx(signed_tx).await
    }

    /// Submit batch transfer and return tx_hash even on broadcast errors
    /// Returns: (tx_hash, Result<outcome>)
    /// The tx_hash is always available, even if broadcast times out
    pub async fn submit_batch_transfer_with_hash(
        &self,
        signer_account: &AccountId,
        token: &AccountId,
        receivers: Vec<(AccountId, String)>,
        secret_key: &SecretKey,
        nonce: u64,
    ) -> Result<(CryptoHash, std::result::Result<FinalExecutionOutcomeView, String>)> {
        let block_hash = self.get_block_hash().await?;

        let actions: Vec<Action> = receivers
            .into_iter()
            .map(|(receiver_id, amount)| {
                let args = serde_json::json!({
                    "receiver_id": receiver_id,
                    "amount": amount,
                });

                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "ft_transfer".to_string(),
                    args: args.to_string().into_bytes(),
                    gas: FT_TRANSFER_GAS_PER_ACTION,
                    deposit: FT_TRANSFER_DEPOSIT,
                }))
            })
            .collect();

        let transaction = Transaction::V0(near_primitives::transaction::TransactionV0 {
            signer_id: signer_account
                .parse()
                .map_err(|e| anyhow!("Invalid signer: {:?}", e))?,
            public_key: secret_key.public_key(),
            nonce,
            receiver_id: token
                .parse()
                .map_err(|e| anyhow!("Invalid receiver: {:?}", e))?,
            block_hash,
            actions,
        });

        let signature = secret_key.sign(transaction.get_hash_and_size().0.as_ref());
        let signed_tx = SignedTransaction::new(signature, transaction);
        
        let tx_hash = signed_tx.get_hash();

        // Broadcast and capture result separately
        match self.broadcast_tx(signed_tx).await {
            Ok((_hash, outcome)) => Ok((tx_hash, Ok(outcome))),
            Err(e) => Ok((tx_hash, Err(format!("{:?}", e)))),
        }
    }

    /// Broadcast a batch transfer without waiting for finality; returns the tx hash immediately.
    pub async fn submit_batch_transfer_async(
        &self,
        signer_account: &AccountId,
        token: &AccountId,
        receivers: Vec<(AccountId, String)>,
        secret_key: &SecretKey,
        nonce: u64,
    ) -> Result<CryptoHash> {
        let block_hash = self.get_block_hash().await?;

        let actions: Vec<Action> = receivers
            .into_iter()
            .map(|(receiver_id, amount)| {
                let args = serde_json::json!({
                    "receiver_id": receiver_id,
                    "amount": amount,
                });

                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "ft_transfer".to_string(),
                    args: args.to_string().into_bytes(),
                    gas: FT_TRANSFER_GAS_PER_ACTION,
                    deposit: FT_TRANSFER_DEPOSIT,
                }))
            })
            .collect();

        let transaction = Transaction::V0(near_primitives::transaction::TransactionV0 {
            signer_id: signer_account
                .parse()
                .map_err(|e| anyhow!("Invalid signer: {:?}", e))?,
            public_key: secret_key.public_key(),
            nonce,
            receiver_id: token
                .parse()
                .map_err(|e| anyhow!("Invalid receiver: {:?}", e))?,
            block_hash,
            actions,
        });

        let signature = secret_key.sign(transaction.get_hash_and_size().0.as_ref());
        let signed_tx = SignedTransaction::new(signature, transaction);

        self.broadcast_tx_async_hash(signed_tx).await
    }

    /// Low-level: call broadcast_tx_async; returns only the tx hash without waiting
    pub async fn broadcast_tx_async_hash(&self, signed_tx: SignedTransaction) -> Result<CryptoHash> {
        self.incr_stat("broadcast_tx_async_hash").await;
        RPC_CALLS.fetch_add(1, Ordering::Relaxed);
        let request = methods::broadcast_tx_async::RpcBroadcastTxAsyncRequest { signed_transaction: signed_tx };
        let tx_hash: CryptoHash = self
            .client
            .call(request)
            .await
            .map_err(|e| anyhow!("Failed to broadcast (async) transaction: {:?}", e))?;
        Ok(tx_hash)
    }
}
