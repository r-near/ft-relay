use anyhow::{anyhow, Result};
use log::{debug, info};
use near_crypto::{PublicKey, SecretKey};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction, Transaction};
use near_primitives::types::{BlockReference, Finality};
use near_primitives::views::{AccessKeyView, FinalExecutionOutcomeView, FinalExecutionStatus};
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
}

impl NearRpcClient {
    pub fn new(rpc_url: &str) -> Self {
        let client = JsonRpcClient::connect(rpc_url);
        Self {
            client,
            block_hash_cache: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn get_block_hash(&self) -> Result<CryptoHash> {
        {
            let cache = self.block_hash_cache.read().await;
            if let Some((hash, cached_at)) = *cache {
                if cached_at.elapsed() < Duration::from_secs(3600) {
                    debug!("Using cached block hash");
                    return Ok(hash);
                }
            }
        }

        debug!("Fetching fresh block hash from RPC");
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

    pub async fn broadcast_tx(&self, signed_tx: SignedTransaction) -> Result<CryptoHash> {
        let tx_hash = signed_tx.get_hash();
        info!("Broadcasting transaction: {}", tx_hash);

        RPC_CALLS.fetch_add(1, Ordering::Relaxed);
        let request = methods::broadcast_tx_async::RpcBroadcastTxAsyncRequest {
            signed_transaction: signed_tx,
        };

        self.client
            .call(request)
            .await
            .map_err(|e| anyhow!("Failed to broadcast transaction: {:?}", e))?;

        Ok(tx_hash)
    }

    pub async fn check_tx_status(
        &self,
        tx_hash: &CryptoHash,
        sender: &AccountId,
    ) -> Result<TxStatus> {
        RPC_CALLS.fetch_add(1, Ordering::Relaxed);
        let request = methods::tx::RpcTransactionStatusRequest {
            transaction_info: methods::tx::TransactionInfo::TransactionId {
                tx_hash: *tx_hash,
                sender_account_id: sender.parse().map_err(|e| anyhow!("Invalid account ID: {:?}", e))?,
            },
            wait_until: near_primitives::views::TxExecutionStatus::Final,
        };

        match self.client.call(request).await {
            Ok(response) => {
                let outcome = response.final_execution_outcome
                    .ok_or_else(|| anyhow!("No execution outcome"))?;
                let outcome_view = outcome.into_outcome();
                match outcome_view.status {
                    FinalExecutionStatus::SuccessValue(_) => {
                        Ok(TxStatus::Success(Box::new(outcome_view)))
                    }
                    FinalExecutionStatus::Failure(err) => Ok(TxStatus::Failed(format!("{:?}", err))),
                    FinalExecutionStatus::NotStarted | FinalExecutionStatus::Started => Ok(TxStatus::Pending),
                }
            }
            Err(e) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("UNKNOWN_TRANSACTION") || err_str.contains("does not exist") {
                    Ok(TxStatus::Pending)
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
        RPC_CALLS.fetch_add(1, Ordering::Relaxed);
        let request = methods::query::RpcQueryRequest {
            block_reference: BlockReference::Finality(Finality::Final),
            request: near_primitives::views::QueryRequest::ViewAccessKey {
                account_id: account.parse().map_err(|e| anyhow!("Invalid account ID: {:?}", e))?,
                public_key: public_key.clone(),
            },
        };

        let response = self
            .client
            .call(request)
            .await
            .map_err(|e| anyhow!("Failed to fetch access key: {:?}", e))?;

        match response.kind {
            QueryResponseKind::AccessKey(access_key) => Ok(access_key),
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
    ) -> Result<CryptoHash> {
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
            signer_id: signer_account.parse().map_err(|e| anyhow!("Invalid signer: {:?}", e))?,
            public_key: secret_key.public_key(),
            nonce,
            receiver_id: token.parse().map_err(|e| anyhow!("Invalid receiver: {:?}", e))?,
            block_hash,
            actions: vec![action],
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
            signer_id: signer_account.parse().map_err(|e| anyhow!("Invalid signer: {:?}", e))?,
            public_key: secret_key.public_key(),
            nonce,
            receiver_id: token.parse().map_err(|e| anyhow!("Invalid receiver: {:?}", e))?,
            block_hash,
            actions,
        });

        let signature = secret_key.sign(transaction.get_hash_and_size().0.as_ref());
        let signed_tx = SignedTransaction::new(signature, transaction);
        
        self.broadcast_tx(signed_tx).await
    }
}
