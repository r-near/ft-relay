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
use serde_json::json;
use uuid::Uuid;

use crate::{
    redis_queue::RedisQueue,
    types::{TransferBody, TransferReq},
};

#[derive(Clone)]
struct AppState {
    queue: Arc<RedisQueue>,
}

pub fn build_router(queue: Arc<RedisQueue>) -> Router {
    let state = AppState { queue };

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

    let request = TransferReq {
        transfer_id: transfer_id.clone(),
        receiver_id: body.receiver_id,
        amount: body.amount,
        memo: body.memo,
        attempts: 0,
        enqueued_at,
    };

    state.queue.push(&request).await.map_err(|err| {
        warn!("failed to enqueue transfer: {err:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(json!({
        "status": "queued",
        "transfer_id": transfer_id,
    })))
}

async fn get_transfer_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let tx_hash = state.queue.get_status(&id).await.map_err(|err| {
        warn!("failed to get transfer status: {err:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    match tx_hash {
        Some(hash) => Ok(Json(json!({
            "transfer_id": id,
            "status": "completed",
            "tx_hash": hash,
        }))),
        None => Ok(Json(json!({
            "transfer_id": id,
            "status": "pending",
        }))),
    }
}
