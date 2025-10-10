use std::{
    sync::{atomic::{AtomicU64, Ordering}, Arc},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use log::{info, warn};
use near_api_types::AccountId;
use redis::AsyncCommands;
use serde_json::json;
use uuid::Uuid;

use crate::{
    redis_helpers,
    stream_queue::{RegistrationRequest, StreamQueue},
    transfer_states::{PendingRegistration, ReadyToSend, Transfer},
};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TransferBody {
    pub receiver_id: AccountId,
    pub amount: String,
    #[serde(default)]
    pub memo: Option<String>,
}

#[derive(Clone)]
struct AppState {
    redis_conn: redis::aio::ConnectionManager,
    ready_queue: Arc<StreamQueue<Transfer<ReadyToSend>>>,
    registration_queue: Arc<StreamQueue<RegistrationRequest>>,
    token: AccountId,
    // Diagnostic counters
    requests_received: Arc<AtomicU64>,
    ready_xadd_attempts: Arc<AtomicU64>,
    ready_xadd_succeeded: Arc<AtomicU64>,
    pending_rpush_attempts: Arc<AtomicU64>,
    pending_rpush_succeeded: Arc<AtomicU64>,
    reg_xadd_attempts: Arc<AtomicU64>,
    reg_xadd_succeeded: Arc<AtomicU64>,
}

pub fn build_router(
    redis_conn: redis::aio::ConnectionManager,
    ready_queue: Arc<StreamQueue<Transfer<ReadyToSend>>>,
    registration_queue: Arc<StreamQueue<RegistrationRequest>>,
    token: AccountId,
) -> Router {
    let requests_received = Arc::new(AtomicU64::new(0));
    let ready_xadd_attempts = Arc::new(AtomicU64::new(0));
    let ready_xadd_succeeded = Arc::new(AtomicU64::new(0));
    let pending_rpush_attempts = Arc::new(AtomicU64::new(0));
    let pending_rpush_succeeded = Arc::new(AtomicU64::new(0));
    let reg_xadd_attempts = Arc::new(AtomicU64::new(0));
    let reg_xadd_succeeded = Arc::new(AtomicU64::new(0));
    
    let state = AppState {
        redis_conn,
        ready_queue,
        registration_queue,
        token,
        requests_received: requests_received.clone(),
        ready_xadd_attempts: ready_xadd_attempts.clone(),
        ready_xadd_succeeded: ready_xadd_succeeded.clone(),
        pending_rpush_attempts: pending_rpush_attempts.clone(),
        pending_rpush_succeeded: pending_rpush_succeeded.clone(),
        reg_xadd_attempts: reg_xadd_attempts.clone(),
        reg_xadd_succeeded: reg_xadd_succeeded.clone(),
    };
    
    // Spawn diagnostic logger
    let diag_requests = requests_received.clone();
    let diag_ready_att = ready_xadd_attempts.clone();
    let diag_ready_succ = ready_xadd_succeeded.clone();
    let diag_pend_att = pending_rpush_attempts.clone();
    let diag_pend_succ = pending_rpush_succeeded.clone();
    let diag_reg_att = reg_xadd_attempts.clone();
    let diag_reg_succ = reg_xadd_succeeded.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let req = diag_requests.load(Ordering::Relaxed);
            let r_att = diag_ready_att.load(Ordering::Relaxed);
            let r_succ = diag_ready_succ.load(Ordering::Relaxed);
            let p_att = diag_pend_att.load(Ordering::Relaxed);
            let p_succ = diag_pend_succ.load(Ordering::Relaxed);
            let reg_att = diag_reg_att.load(Ordering::Relaxed);
            let reg_succ = diag_reg_succ.load(Ordering::Relaxed);
            if req > 0 {
                info!("📊 HTTP: req={}, ready_xadd={}/{}, pending_rpush={}/{}, reg_xadd={}/{}", 
                    req, r_succ, r_att, p_succ, p_att, reg_succ, reg_att);
            }
        }
    });

    Router::new()
        .route("/v1/transfer", post(create_transfer))
        .route("/v1/transfer/{id}", get(get_transfer_status))
        .with_state(state)
}

async fn create_transfer(
    State(state): State<AppState>,
    Json(body): Json<TransferBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state.requests_received.fetch_add(1, Ordering::Relaxed);
    
    let transfer_id = Uuid::new_v4().to_string();
    let enqueued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let receiver_str = body.receiver_id.to_string();
    let token_str = state.token.to_string();

    let mut conn = state.redis_conn.clone();
    
    // Log entry
    log::trace!("Processing transfer {} for {}", transfer_id, receiver_str);

    // Check if account is already registered
    let is_registered = redis_helpers::is_registered(&mut conn, &token_str, &receiver_str)
        .await
        .map_err(|err| {
            warn!("failed to check registration: {err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if is_registered {
        // Account already registered - enqueue directly to ready stream
        let transfer = Transfer::<ReadyToSend>::from_data(crate::transfer_states::TransferData {
            transfer_id: transfer_id.clone(),
            receiver_id: body.receiver_id,
            amount: body.amount,
            memo: body.memo,
            attempts: 0,
            enqueued_at,
        });

        // Use the HTTP handler's own connection to avoid lock contention
        state.ready_xadd_attempts.fetch_add(1, Ordering::Relaxed);
        match state.ready_queue.push_with_conn(&mut conn, &transfer).await {
            Ok(_) => {
                state.ready_xadd_succeeded.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                warn!("❌ CRITICAL: Failed to enqueue transfer {}: {err:?}", transfer_id);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }

        // Set status
        redis_helpers::set_transfer_status(&mut conn, &token_str, &transfer_id, "ready")
            .await
            .map_err(|err| {
                warn!("failed to set transfer status: {err:?}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    } else {
        // Account needs registration - enqueue to pending + registration stream
        let transfer = Transfer::<PendingRegistration>::new(
            transfer_id.clone(),
            body.receiver_id.clone(),
            body.amount,
            body.memo,
            enqueued_at,
        );

        // Push to pending list (this uses RPUSH to a list, not XADD to a stream)
        state.pending_rpush_attempts.fetch_add(1, Ordering::Relaxed);
        match redis_helpers::push_to_pending_list(&mut conn, &token_str, &transfer).await {
            Ok(_) => {
                state.pending_rpush_succeeded.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                warn!("❌ CRITICAL: Failed to enqueue pending transfer {}: {err:?}", transfer_id);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }

        // Check if registration is already pending using SADD
        // Only push to registration stream if this is the first request for this account
        let pending_reg_key = format!("registration_pending:{}", token_str);
        let newly_added: i32 = conn
            .sadd(&pending_reg_key, &receiver_str)
            .await
            .map_err(|err| {
                warn!("failed to check registration pending: {err:?}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        if newly_added == 1 {
            // First time seeing this account - push to registration stream
            let reg_request = RegistrationRequest {
                account_id: body.receiver_id.to_string(),
            };
            state.reg_xadd_attempts.fetch_add(1, Ordering::Relaxed);
            match state.registration_queue.push_with_conn(&mut conn, &reg_request).await {
                Ok(_) => {
                    state.reg_xadd_succeeded.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    warn!("❌ CRITICAL: Failed to enqueue registration request for {}: {err:?}", receiver_str);
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        } else {
            // Registration already pending - but check if it completed while we were processing!
            // This handles the race condition where registration completes between our initial
            // is_registered() check and now.
            let is_now_registered = redis_helpers::is_registered(&mut conn, &token_str, &receiver_str)
                .await
                .map_err(|err| {
                    warn!("failed to re-check registration: {err:?}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            
            if is_now_registered {
                // Registration completed! Remove from pending list and add to ready stream
                log::info!("🔄 Race condition detected: {} registered while request was processing, moving to ready", receiver_str);
                
                // Pop this transfer from pending list
                let popped = redis_helpers::pop_pending_transfers(&mut conn, &token_str, &receiver_str).await
                    .map_err(|err| {
                        warn!("failed to pop pending transfer: {err:?}");
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?;
                
                // Add to ready stream (including the current transfer)
                let mut ready_transfers = vec![];
                for pending_transfer in popped {
                    ready_transfers.push(pending_transfer.mark_registered());
                }
                
                // Current transfer was already added to pending list above,
                // and we just popped it along with any others, so we're good to add them all
                
                // Add all to ready stream
                for transfer in ready_transfers {
                    state.ready_xadd_attempts.fetch_add(1, Ordering::Relaxed);
                    match state.ready_queue.push_with_conn(&mut conn, &transfer).await {
                        Ok(_) => {
                            state.ready_xadd_succeeded.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(err) => {
                            warn!("❌ CRITICAL: Failed to enqueue ready transfer: {err:?}");
                            return Err(StatusCode::INTERNAL_SERVER_ERROR);
                        }
                    }
                    
                    // Set status
                    let tid = &transfer.data().transfer_id;
                    redis_helpers::set_transfer_status(&mut conn, &token_str, tid, "ready")
                        .await
                        .map_err(|err| {
                            warn!("failed to set transfer status: {err:?}");
                            StatusCode::INTERNAL_SERVER_ERROR
                        })?;
                }
                
                // Return early since we handled it
                return Ok(Json(json!({
                    "status": "queued",
                    "transfer_id": transfer_id,
                })));
            }
            // else: registration still pending, transfer is in pending list and will be processed when registration completes
        }
    }

    Ok(Json(json!({
        "status": "queued",
        "transfer_id": transfer_id,
    })))
}

async fn get_transfer_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut conn = state.redis_conn.clone();

    // Check completion status (tx_hash in status key)
    let status_key = format!("status:{}:{}", state.token, id);
    let tx_hash: Option<String> = conn.get(&status_key).await.map_err(|err| {
        warn!("failed to get transfer status: {err:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if let Some(hash) = tx_hash {
        return Ok(Json(json!({
            "transfer_id": id,
            "status": "completed",
            "tx_hash": hash,
        })));
    }

    // Check lifecycle status (O(1) lookup)
    let transfer_status_key = format!("transfer:{}:{}", state.token, id);
    let status: Option<String> = conn.get(&transfer_status_key).await.map_err(|err| {
        warn!("failed to get transfer lifecycle status: {err:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    match status.as_deref() {
        Some("ready") => Ok(Json(json!({
            "transfer_id": id,
            "status": "ready",
            "message": "Waiting for worker to process transfer"
        }))),
        Some("pending_registration") => Ok(Json(json!({
            "transfer_id": id,
            "status": "pending_registration",
            "message": "Waiting for account registration to complete"
        }))),
        _ => {
            // Transfer not found or status expired
            Ok(Json(json!({
                "transfer_id": id,
                "status": "unknown",
                "message": "Transfer not found or status expired (1h TTL)"
            })))
        }
    }
}
