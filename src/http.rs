use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use log::warn;
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
    redis_client: redis::Client,
    ready_queue: Arc<StreamQueue<Transfer<ReadyToSend>>>,
    registration_queue: Arc<StreamQueue<RegistrationRequest>>,
    token: AccountId,
}

pub fn build_router(
    redis_client: redis::Client,
    ready_queue: Arc<StreamQueue<Transfer<ReadyToSend>>>,
    registration_queue: Arc<StreamQueue<RegistrationRequest>>,
    token: AccountId,
) -> Router {
    let state = AppState {
        redis_client,
        ready_queue,
        registration_queue,
        token,
    };

    Router::new()
        .route("/v1/transfer", post(create_transfer))
        .route("/v1/transfer/{id}", get(get_transfer_status))
        .with_state(state)
}

async fn create_transfer(
    State(state): State<AppState>,
    Json(body): Json<TransferBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let transfer_id = Uuid::new_v4().to_string();
    let enqueued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let receiver_str = body.receiver_id.to_string();
    let token_str = state.token.to_string();

    let mut conn = state
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .map_err(|err| {
            warn!("failed to connect to redis: {err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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

        state.ready_queue.push(&transfer).await.map_err(|err| {
            warn!("failed to enqueue transfer: {err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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

        // Push to pending list
        redis_helpers::push_to_pending_list(&mut conn, &token_str, &transfer)
            .await
            .map_err(|err| {
                warn!("failed to enqueue pending transfer: {err:?}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

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
            state
                .registration_queue
                .push(&reg_request)
                .await
                .map_err(|err| {
                    warn!("failed to enqueue registration request: {err:?}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
        }
        // else: registration already pending, transfer is in pending list and will be processed when registration completes
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
    let mut conn = state
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .map_err(|err| {
            warn!("failed to connect to redis: {err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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
