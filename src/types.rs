use near_api_types::AccountId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransferBody {
    pub receiver_id: AccountId,
    pub amount: String,
    #[serde(default)]
    pub memo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferReq {
    pub transfer_id: String,
    pub receiver_id: AccountId,
    pub amount: String,
    pub memo: Option<String>,
    pub attempts: u32,
    pub enqueued_at: u64,
}
