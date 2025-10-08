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
    queue::TransferQueue,
    transfer_states::{Transfer, PendingRegistration, ReadyToSend},
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
    queue: Arc<TransferQueue>,
    token: AccountId,
}

pub fn build_router(queue: Arc<TransferQueue>, token: AccountId) -> Router {
    let state = AppState { queue, token };

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

    // Check if account is already registered
    let is_registered = state
        .queue
        .is_registered(&receiver_str)
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

        state.queue.push_ready(&transfer).await.map_err(|err| {
            warn!("failed to enqueue transfer: {err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    } else {
        // Account needs registration - enqueue to pending
        let transfer = Transfer::<PendingRegistration>::new(
            transfer_id.clone(),
            body.receiver_id,
            body.amount,
            body.memo,
            enqueued_at,
        );

        state.queue.push_pending(&transfer).await.map_err(|err| {
            warn!("failed to enqueue pending transfer: {err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
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
        .queue
        .get_client()
        .get_multiplexed_async_connection()
        .await
        .map_err(|err| {
            warn!("failed to connect to redis: {err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Check if completed (has tx_hash in status key)
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

    // Check if in ready stream (search stream - expensive, but accurate)
    // For production, consider caching or using a separate status tracking system
    let ready_stream_key = format!("ready:{}", state.token);
    let stream_result: Result<redis::streams::StreamReadReply, _> = redis::cmd("XRANGE")
        .arg(&ready_stream_key)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await;

    if let Ok(reply) = stream_result {
        for stream in reply.keys {
            for entry in stream.ids {
                if let Some(redis::Value::BulkString(bytes)) = entry.map.get("data") {
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        if s.contains(&format!("\"transfer_id\":\"{}\"", id)) {
                            return Ok(Json(json!({
                                "transfer_id": id,
                                "status": "ready",
                                "message": "Waiting for worker to process transfer"
                            })));
                        }
                    }
                }
            }
        }
    }

    // Check if pending registration (check all pending lists - also expensive)
    // For now, just return pending_registration if not in ready or completed
    Ok(Json(json!({
        "transfer_id": id,
        "status": "pending_registration",
        "message": "Waiting for account registration to complete"
    })))
}
