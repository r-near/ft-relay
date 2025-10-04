use std::sync::Arc;

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
    redis_queue::RedisContext,
    types::{now_millis, TransferBody, TransferReq},
};

#[derive(Clone)]
struct AppState {
    redis: Arc<RedisContext>,
}

pub fn build_router(redis: Arc<RedisContext>) -> Router {
    let state = AppState { redis };

    Router::new()
        .route(
            "/v1/transfer",
            post(
                |State(state): State<AppState>, Json(body): Json<TransferBody>| async move {
                    let transfer_id = Uuid::new_v4().to_string();
                    let request = TransferReq {
                        transfer_id: transfer_id.clone(),
                        receiver_id: body.receiver_id,
                        amount: body.amount,
                        memo: body.memo,
                        attempts: 0,
                        enqueued_at: now_millis(),
                    };

                    state.redis.enqueue(&request).await.map_err(|err| {
                        warn!("failed to enqueue transfer: {err:?}");
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?;

                    Ok::<_, StatusCode>(Json(json!({
                        "status": "queued",
                        "transfer_id": transfer_id,
                    })))
                },
            ),
        )
        .route(
            "/v1/transfer/{id}",
            get(
                |State(state): State<AppState>, Path(id): Path<String>| async move {
                    let tx_hash = state.redis.get_transfer_status(&id).await.map_err(|err| {
                        warn!("failed to get transfer status: {err:?}");
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?;

                    match tx_hash {
                        Some(hash) => Ok::<_, StatusCode>(Json(json!({
                            "transfer_id": id,
                            "status": "completed",
                            "tx_hash": hash,
                        }))),
                        None => Ok::<_, StatusCode>(Json(json!({
                            "transfer_id": id,
                            "status": "pending",
                        }))),
                    }
                },
            ),
        )
        .with_state(state)
}
