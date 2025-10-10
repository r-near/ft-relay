use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use log::warn;
use redis::AsyncCommands;
use serde_json::json;

use crate::redis_helpers as rh;
use crate::types::{
    AccountId, ErrorResponse, Event, RegistrationMessage, Status, TransferRequest,
    TransferResponse, TransferState,
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

    // Create transfer state
    let transfer = TransferState::new(
        transfer_id.clone(),
        body.receiver_id.clone(),
        body.amount.clone(),
    );

    // Store transfer state (needed by workers)
    rh::store_transfer_state(&mut conn, &transfer)
        .await
        .map_err(|e| {
            warn!("Failed to store transfer state: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;

    // Log RECEIVED event
    rh::log_event(&mut conn, &transfer_id, Event::new("RECEIVED"))
        .await
        .map_err(|e| {
            warn!("Failed to log event: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;

    // Check if receiver is already registered (fast path)
    let is_registered = rh::is_account_registered(&mut conn, &body.receiver_id)
        .await
        .map_err(|e| {
            warn!("Failed to check registration: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;

    let status = if is_registered {
        // Already registered - skip registration queue, go directly to transfer
        rh::enqueue_transfer(&mut conn, &state.env, &transfer_id, 0)
            .await
            .map_err(|e| {
                warn!("Failed to enqueue transfer: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Internal server error".to_string(),
                    }),
                )
            })?;
        rh::log_event(&mut conn, &transfer_id, Event::new("QUEUED_TRANSFER"))
            .await
            .map_err(|e| {
                warn!("Failed to log event: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Internal server error".to_string(),
                    }),
                )
            })?;
        Status::Registered
    } else {
        // Not registered - atomically add transfer to waiting list and check if first
        // Lua script ensures no race conditions!
        let is_first_request = rh::add_transfer_waiting_for_registration(
            &mut conn,
            &body.receiver_id,
            &transfer_id,
        )
        .await
        .map_err(|e| {
            warn!("Failed to add to waiting list: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            )
        })?;

        if is_first_request {
            // First transfer for this account - enqueue ACCOUNT registration job
            let registration_msg = RegistrationMessage {
                account: body.receiver_id.clone(),
                retry_count: 0,
            };
            let serialized = serde_json::to_string(&registration_msg).map_err(|e| {
                warn!("Failed to serialize registration message: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Internal server error".to_string(),
                    }),
                )
            })?;

            let stream_key = format!("ftrelay:{}:reg", state.env);
            let _: String = conn
                .xadd(&stream_key, "*", &[("data", serialized)])
                .await
                .map_err(|e| {
                    warn!("Failed to enqueue registration: {:?}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "Internal server error".to_string(),
                        }),
                    )
                })?;

            rh::log_event(&mut conn, &transfer_id, Event::new("QUEUED_REGISTRATION"))
                .await
                .map_err(|e| {
                    warn!("Failed to log event: {:?}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "Internal server error".to_string(),
                        }),
                    )
                })?;
        } else {
            // Already pending - transfer added to waiting list, will be processed when registration completes
            rh::log_event(&mut conn, &transfer_id, Event::new("WAITING_REGISTRATION"))
                .await
                .map_err(|e| {
                    warn!("Failed to log event: {:?}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "Internal server error".to_string(),
                        }),
                    )
                })?;
        }
        Status::QueuedRegistration
    };

    let response = TransferResponse {
        transfer_id: transfer.transfer_id,
        status,
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
