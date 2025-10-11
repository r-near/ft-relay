pub fn transfer_stream(env: &str) -> String {
    format!("ftrelay:{}:xfer", env)
}

pub fn transfer_consumer_group(env: &str) -> String {
    format!("ftrelay:{}:xfer_workers", env)
}

pub fn registration_stream(env: &str) -> String {
    format!("ftrelay:{}:reg", env)
}

pub fn registration_consumer_group(env: &str) -> String {
    format!("ftrelay:{}:reg_workers", env)
}

pub fn verification_stream(env: &str) -> String {
    format!("ftrelay:{}:verify", env)
}

pub fn verification_consumer_group(env: &str) -> String {
    format!("ftrelay:{}:verify_workers", env)
}

pub fn pending_verification_txs(env: &str) -> String {
    format!("ftrelay:{}:pending_verification_txs", env)
}

pub fn rpc_stats_hash(env: &str) -> String {
    format!("ftrelay:{}:rpc_stats", env)
}

pub fn transfer_state(transfer_id: &str) -> String {
    format!("transfer:{}", transfer_id)
}

pub fn transfer_events(transfer_id: &str) -> String {
    format!("transfer:{}:ev", transfer_id)
}

pub fn tx_transfers(tx_hash: &str) -> String {
    format!("tx:{}:transfers", tx_hash)
}

pub fn tx_status(tx_hash: &str) -> String {
    format!("tx:{}:status", tx_hash)
}

pub fn transfer_waiting_list(account: &str) -> String {
    format!("waiting_transfers:{}", account)
}

pub fn pending_registration_accounts() -> &'static str {
    "pending_registration_accounts"
}

pub fn registered_accounts() -> &'static str {
    "registered_accounts"
}

pub fn pending_registrations() -> &'static str {
    "pending_registrations"
}
