use near_api_types::AccountId;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// Core transfer data, shared across all states
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferData {
    pub transfer_id: String,
    pub receiver_id: AccountId,
    pub amount: String,
    pub memo: Option<String>,
    pub attempts: u32,
    pub enqueued_at: u64,
}

// State markers (zero-cost, compile-time only)
#[derive(Debug, Clone)]
pub struct PendingRegistration;

#[derive(Debug, Clone)]
pub struct ReadyToSend;

#[derive(Debug, Clone)]
pub struct Failed;

/// Transfer in a specific state
/// The state parameter S ensures only valid transitions are possible
#[derive(Debug, Clone)]
pub struct Transfer<S> {
    pub data: TransferData,
    _state: PhantomData<S>,
}

impl<S> Transfer<S> {
    /// Access the underlying data (available in all states)
    pub fn data(&self) -> &TransferData {
        &self.data
    }
}

// ============================================================================
// State transitions (only valid transitions are implemented)
// ============================================================================

impl Transfer<PendingRegistration> {
    /// Create a new transfer in pending registration state
    pub fn new(
        transfer_id: String,
        receiver_id: AccountId,
        amount: String,
        memo: Option<String>,
        enqueued_at: u64,
    ) -> Self {
        Self {
            data: TransferData {
                transfer_id,
                receiver_id,
                amount,
                memo,
                attempts: 0,
                enqueued_at,
            },
            _state: PhantomData,
        }
    }

    /// Transition to ReadyToSend after registration completes
    pub fn mark_registered(self) -> Transfer<ReadyToSend> {
        Transfer {
            data: self.data,
            _state: PhantomData,
        }
    }
}

impl Transfer<ReadyToSend> {
    /// Create from deserialized data
    pub fn from_data(data: TransferData) -> Self {
        Self {
            data,
            _state: PhantomData,
        }
    }

    /// Transition to Failed state (e.g., after tx submission failure)
    pub fn mark_failed(mut self) -> Transfer<Failed> {
        self.data.attempts += 1;
        Transfer {
            data: self.data,
            _state: PhantomData,
        }
    }

    /// Mark as completed (terminal state, returns just the data)
    pub fn mark_completed(self) -> TransferData {
        self.data
    }

    /// Convert back to PendingRegistration (for re-registration cases)
    /// This happens when a transfer fails due to unregistered account
    pub fn into_pending_registration(self) -> Transfer<PendingRegistration> {
        Transfer {
            data: self.data,
            _state: PhantomData,
        }
    }
}

impl Transfer<Failed> {
    /// Create from deserialized data
    pub fn from_data(data: TransferData) -> Self {
        Self {
            data,
            _state: PhantomData,
        }
    }

    /// Attempt to retry (returns None if max attempts reached)
    pub fn retry(self, max_attempts: u32) -> Option<Transfer<ReadyToSend>> {
        if self.data.attempts >= max_attempts {
            None // Terminal - too many retries
        } else {
            Some(Transfer {
                data: self.data,
                _state: PhantomData,
            })
        }
    }

    /// Check if this transfer should be terminated
    pub fn should_terminate(&self, max_attempts: u32) -> bool {
        self.data.attempts >= max_attempts
    }
}

// ============================================================================
// Serialization helpers (for Redis storage)
// ============================================================================

impl Transfer<PendingRegistration> {
    pub fn serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.data)
    }

    pub fn deserialize(s: &str) -> Result<Self, serde_json::Error> {
        let data: TransferData = serde_json::from_str(s)?;
        Ok(Self {
            data,
            _state: PhantomData,
        })
    }
}

impl Transfer<ReadyToSend> {
    pub fn serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.data)
    }

    pub fn deserialize(s: &str) -> Result<Self, serde_json::Error> {
        let data: TransferData = serde_json::from_str(s)?;
        Ok(Self::from_data(data))
    }
}

impl Transfer<Failed> {
    pub fn serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.data)
    }

    pub fn deserialize(s: &str) -> Result<Self, serde_json::Error> {
        let data: TransferData = serde_json::from_str(s)?;
        Ok(Self::from_data(data))
    }
}
