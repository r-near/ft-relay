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
    redis_conn: std::sync::Arc<tokio::sync::Mutex<redis::aio::ConnectionManager>>,
    env: String,
}

pub fn build_router(
    redis_conn: std::sync::Arc<tokio::sync::Mutex<redis::aio::ConnectionManager>>,
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    static REQUEST_COUNT: AtomicUsize = AtomicUsize::new(0);
    let count = REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);
    info!("[REQUEST_TRACE] #{} - ENTRY", count);

    // Extract idempotency key
    let idempotency_key = headers
        .get("X-Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let transfer_id = match idempotency_key {
        Some(key) => key,
        None => {
            warn!("[REQUEST_TRACE] #{} - Missing idempotency key", count);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Missing X-Idempotency-Key header".to_string(),
                }),
            ));
        }
    };

    if body.receiver_id.is_empty() {
        warn!("[REQUEST_TRACE] #{} - Invalid receiver_id", count);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid receiver_id".to_string(),
            }),
        ));
    }

    if body.amount.parse::<u128>().is_err() {
        warn!("[REQUEST_TRACE] #{} - Invalid amount", count);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid amount".to_string(),
            }),
        ));
    }

    info!("[REQUEST_TRACE] #{} - Validation passed", count);

    // Lock the mutex to get exclusive access to the ConnectionManager
    // This serializes Redis operations across all concurrent requests
    let mut conn = state.redis_conn.lock().await;

    let transfer = TransferState::new(
        transfer_id.clone(),
        body.receiver_id.clone(),
        body.amount.clone(),
    );

    // Check for existing transfer (idempotency)
    if let Some(existing) = rh::get_transfer_state(&mut *conn, &transfer_id)
        .await
        .map_err(|e| {
            warn!("[REQUEST_TRACE] #{} - Redis GET error: {:?}", count, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?
    {
        if existing.receiver_id != body.receiver_id || existing.amount != body.amount {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorResponse {
                    error: "Idempotency key already used with different parameters".to_string(),
                }),
            ));
        }
        
        // Return existing transfer (idempotency hit)
        info!("Idempotency hit for transfer {}", transfer_id);
        let mut response: TransferResponse = existing.into();
        
        // Include audit trail in response
        if let Ok(events) = rh::get_events(&mut *conn, &transfer_id).await {
            response.events = Some(events);
        }
        
        // Return 201 if still in RECEIVED state, 200 otherwise
        let status = if response.status == Status::Received {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        };
        
        return Ok((status, Json(serde_json::to_value(response).unwrap())));
    }

    // Store new transfer
    rh::store_transfer_state(&mut *conn, &transfer)
        .await
        .map_err(|e| {
            warn!("[REQUEST_TRACE] #{} - Redis SET error: {:?}", count, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;

    // Log RECEIVED event
    rh::log_event(&mut *conn, &transfer_id, Event::new("RECEIVED"))
        .await
        .map_err(|e| {
            warn!("Failed to log RECEIVED event: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;

    // Enqueue for registration
    rh::enqueue_registration(&mut *conn, &state.env, &transfer_id, 0)
        .await
        .map_err(|e| {
            warn!("[REQUEST_TRACE] #{} - Redis ENQUEUE error: {:?}", count, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;

    // Update status to QUEUED_REGISTRATION
    rh::update_transfer_status(&mut *conn, &transfer_id, Status::QueuedRegistration)
        .await
        .map_err(|e| {
            warn!("[REQUEST_TRACE] #{} - Redis UPDATE error: {:?}", count, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;

    // Log QUEUED_REGISTRATION event
    rh::log_event(&mut *conn, &transfer_id, Event::new("QUEUED_REGISTRATION"))
        .await
        .map_err(|e| {
            warn!("Failed to log QUEUED_REGISTRATION event: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;
    
    info!("Created transfer {}", transfer_id);

    let response = TransferResponse {
        transfer_id: transfer.transfer_id,
        status: Status::QueuedRegistration,
        receiver_id: transfer.receiver_id,
        amount: transfer.amount,
        tx_hash: None,
        created_at: transfer.created_at,
        completed_at: None,
        retry_count: Some(0),
        events: None,
    };

    Ok((
        StatusCode::CREATED,
        Json(serde_json::to_value(response).unwrap()),
    ))
}

async fn get_transfer_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TransferResponse>, (StatusCode, Json<ErrorResponse>)> {
    let mut conn = state.redis_conn.lock().await;

    let transfer = match rh::get_transfer_state(&mut *conn, &id).await {
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

    let events = rh::get_events(&mut *conn, &id).await.ok();

    let mut response: TransferResponse = transfer.into();
    response.events = events;

    Ok(Json(response))
}

async fn health_check(State(state): State<AppState>) -> Json<serde_json::Value> {
    let mut conn = state.redis_conn.lock().await;

    let redis_status = match (*conn).ping::<()>().await {
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
