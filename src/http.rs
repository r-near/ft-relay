use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use log::{info, warn};
use redis::AsyncCommands;
use serde_json::json;

use crate::redis_helpers as rh;
use crate::types::{
    AccountId, ErrorResponse, Event, Status, TransferRequest, TransferResponse, TransferState,
};

#[derive(Clone)]
struct AppState {
    redis_conn: redis::aio::ConnectionManager,
    env: String,
}

pub fn build_router(
    redis_conn: redis::aio::ConnectionManager,
    env: String,
    _token: AccountId,
) -> Router {
    let state = AppState { redis_conn, env };

    Router::new()
        .route("/v1/transfer", post(create_transfer))
        .route("/v1/transfer/{id}", get(get_transfer_status))
        .route("/health", get(health_check))
        .with_state(state)
}

async fn create_transfer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<TransferRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorResponse>)> {

    // Extract idempotency key
    let idempotency_key = headers
        .get("X-Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let transfer_id = match idempotency_key {
        Some(key) => key,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Missing X-Idempotency-Key header".to_string(),
                }),
            ));
        }
    };

    if body.receiver_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid receiver_id".to_string(),
            }),
        ));
    }

    if body.amount.parse::<u128>().is_err() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid amount".to_string(),
            }),
        ));
    }

    // Clone connection (ConnectionManager is designed for concurrent use)
    let mut conn = state.redis_conn.clone();

    // FAST PATH: Just enqueue directly - no Redis state storage in HTTP handler
    // Workers will handle all the state management
    rh::enqueue_registration(&mut conn, &state.env, &transfer_id, 0)
        .await
        .map_err(|e| {
            warn!("Redis XADD error: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;

    // Return minimal response - worker will handle the rest
    let response = json!({
        "transfer_id": transfer_id,
        "receiver_id": body.receiver_id,
        "amount": body.amount,
        "status": "QUEUED"
    });

    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_transfer_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TransferResponse>, (StatusCode, Json<ErrorResponse>)> {
    let mut conn = state.redis_conn.clone();

    let transfer = match rh::get_transfer_state(&mut conn, &id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "Transfer not found".to_string(),
                }),
            ));
        }
        Err(e) => {
            warn!("Redis error: {:?}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            ));
        }
    };

    let events = rh::get_events(&mut conn, &id).await.ok();

    let mut response: TransferResponse = transfer.into();
    response.events = events;

    Ok(Json(response))
}

async fn health_check(State(state): State<AppState>) -> Json<serde_json::Value> {
    let mut conn = state.redis_conn.clone();

    let redis_status = match conn.ping::<()>().await {
        Ok(_) => "connected",
        Err(_) => "disconnected",
    };

    let rpc_calls = crate::types::RPC_CALLS.load(std::sync::atomic::Ordering::Relaxed);

    Json(json!({
        "status": "healthy",
        "redis": redis_status,
        "metrics": {
            "rpc_calls_total": rpc_calls
        }
    }))
}
