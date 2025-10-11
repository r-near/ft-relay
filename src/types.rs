use chrono::{DateTime, Utc};
use near_crypto::{PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU64;

pub type AccountId = String;

// Global RPC call counter for metrics
pub static RPC_CALLS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Status {
    Received,
    QueuedRegistration,
    Registered,
    QueuedTransfer,
    Submitted,
    QueuedVerification,
    Completed,
    Failed,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Received => "RECEIVED",
            Status::QueuedRegistration => "QUEUED_REGISTRATION",
            Status::Registered => "REGISTERED",
            Status::QueuedTransfer => "QUEUED_TRANSFER",
            Status::Submitted => "SUBMITTED",
            Status::QueuedVerification => "QUEUED_VERIFICATION",
            Status::Completed => "COMPLETED",
            Status::Failed => "FAILED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferState {
    pub transfer_id: String,
    pub status: Status,
    pub receiver_id: AccountId,
    pub amount: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    pub retry_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl TransferState {
    pub fn new(transfer_id: String, receiver_id: AccountId, amount: String) -> Self {
        let now = Utc::now();
        Self {
            transfer_id,
            status: Status::Received,
            receiver_id,
            amount,
            tx_hash: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            retry_count: 0,
            error_message: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub time: DateTime<Utc>,
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl Event {
    pub fn new(event: impl Into<String>) -> Self {
        Self {
            time: Utc::now(),
            event: event.into(),
            tx_hash: None,
            reason: None,
        }
    }

    pub fn with_tx_hash(mut self, tx_hash: String) -> Self {
        self.tx_hash = Some(tx_hash);
        self
    }

    pub fn with_reason(mut self, reason: String) -> Self {
        self.reason = Some(reason);
        self
    }
}

#[derive(Debug, Clone)]
pub struct AccessKey {
    pub key_id: String,
    pub secret_key: SecretKey,
    pub public_key: PublicKey,
}

impl AccessKey {
    pub fn from_secret_key(secret_key: SecretKey) -> Self {
        let public_key = secret_key.public_key();
        let key_id = format!("{}", public_key);
        Self {
            key_id,
            secret_key,
            public_key,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationMessage {
    pub account: String,  // Account to register (not transfer_id!)
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferMessage {
    pub transfer_id: String,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationMessage {
    pub transfer_id: String,
    pub tx_hash: String,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationTxMessage {
    pub tx_hash: String,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferRequest {
    pub receiver_id: AccountId,
    pub amount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferResponse {
    pub transfer_id: String,
    pub status: Status,
    pub receiver_id: AccountId,
    pub amount: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events: Option<Vec<Event>>,
}

impl From<TransferState> for TransferResponse {
    fn from(state: TransferState) -> Self {
        Self {
            transfer_id: state.transfer_id,
            status: state.status,
            receiver_id: state.receiver_id,
            amount: state.amount,
            tx_hash: state.tx_hash,
            created_at: state.created_at,
            completed_at: state.completed_at,
            retry_count: Some(state.retry_count),
            events: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}
