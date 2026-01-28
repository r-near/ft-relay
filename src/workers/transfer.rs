use ::redis::aio::ConnectionManager;
use anyhow::{self, Result};
use log::{debug, info, warn};
use near_kit::{Gas, Near, NearToken, TxExecutionStatus};
use std::sync::Arc;

use crate::redis::{events, keys, state, streams, transfers, verification};
use crate::types::{AccountId, Event, Status, TransferMessage, TransferState};

use super::shared;

const MAX_RETRIES: u32 = 10;
const MAX_BATCH_SIZE: usize = 100;

// Gas per ft_transfer action
const FT_TRANSFER_GAS: Gas = Gas::tgas(3);
// 1 yocto deposit required for ft_transfer
const FT_TRANSFER_DEPOSIT: NearToken = NearToken::yocto(1);

struct TransferJob {
    stream_id: String,
    message: TransferMessage,
    state: TransferState,
}

impl TransferJob {
    fn id(&self) -> &str {
        &self.message.transfer_id
    }
}

pub struct TransferWorkerRuntime {
    pub redis_conn: ConnectionManager,
    pub near: Arc<Near>,
    pub relay_account: AccountId,
    pub token: AccountId,
    pub env: String,
}

pub struct TransferWorkerContext {
    pub runtime: Arc<TransferWorkerRuntime>,
    pub linger_ms: u64,
}

pub async fn transfer_worker_loop(ctx: TransferWorkerContext) -> Result<()> {
    let consumer_name = shared::consumer_name("transfer");
    let stream_key = keys::transfer_stream(&ctx.runtime.env);
    let consumer_group = keys::transfer_consumer_group(&ctx.runtime.env);

    let mut conn = ctx.runtime.redis_conn.clone();
    shared::ensure_consumer_group(&mut conn, &stream_key, &consumer_group).await?;

    info!("Transfer worker {} started", consumer_name);

    loop {
        let loop_start = std::time::Instant::now();
        let mut conn = ctx.runtime.redis_conn.clone();

        let pop_start = std::time::Instant::now();
        let batch: Vec<(String, TransferMessage)> = match streams::pop_batch(
            &mut conn,
            &stream_key,
            &consumer_group,
            &consumer_name,
            MAX_BATCH_SIZE,
            ctx.linger_ms,
        )
        .await
        {
            Ok(b) => {
                if !b.is_empty() {
                    let pop_duration = pop_start.elapsed();
                    debug!(
                        "[METRIC] op=pop_batch duration_ms={} msg_count={}",
                        pop_duration.as_millis(),
                        b.len()
                    );
                }
                b
            }
            Err(e) => {
                warn!("Error popping batch: {:?}", e);
                continue;
            }
        };

        if batch.is_empty() {
            continue;
        }

        let batch_process_start = std::time::Instant::now();
        process_batch(&ctx, &stream_key, &consumer_group, batch).await?;
        let batch_process_duration = batch_process_start.elapsed();
        let loop_duration = loop_start.elapsed();
        debug!(
            "[METRIC] op=batch_process duration_ms={}",
            batch_process_duration.as_millis()
        );
        debug!(
            "[METRIC] op=loop_total duration_ms={}",
            loop_duration.as_millis()
        );
    }
}

async fn process_batch(
    ctx: &TransferWorkerContext,
    stream_key: &str,
    consumer_group: &str,
    batch: Vec<(String, TransferMessage)>,
) -> Result<()> {
    let batch_start = std::time::Instant::now();
    debug!("[METRIC] op=process_batch_start batch_size={}", batch.len());
    let mut conn = ctx.runtime.redis_conn.clone();

    let fetch_start = std::time::Instant::now();
    let transfer_ids: Vec<String> = batch
        .iter()
        .map(|(_, msg)| msg.transfer_id.clone())
        .collect();
    let states = state::get_transfer_states_batch(&mut conn, &transfer_ids).await?;

    let mut transfer_jobs: Vec<TransferJob> = Vec::new();
    for ((stream_id, msg), state) in batch.into_iter().zip(states.into_iter()) {
        match state {
            Some(transfer) => transfer_jobs.push(TransferJob {
                stream_id,
                message: msg,
                state: transfer,
            }),
            None => {
                warn!("Transfer {} not found", msg.transfer_id);
                streams::ack_message(&mut conn, stream_key, consumer_group, &stream_id).await?;
            }
        }
    }

    let fetch_duration = fetch_start.elapsed();
    debug!(
        "[METRIC] op=fetch_states duration_ms={} count={}",
        fetch_duration.as_millis(),
        transfer_jobs.len()
    );

    if transfer_jobs.is_empty() {
        return Ok(());
    }

    debug!("Processing batch of {} transfers", transfer_jobs.len());
    let build_transfers_duration = batch_start.elapsed();
    debug!(
        "[METRIC] op=build_transfers duration_ms={} count={}",
        build_transfers_duration.as_millis(),
        transfer_jobs.len()
    );

    // Build a multi-action transaction with all ft_transfer calls
    let rpc_start = std::time::Instant::now();

    let mut call_builder = ctx
        .runtime
        .near
        .transaction(&ctx.runtime.token)
        .call("ft_transfer")
        .args(serde_json::json!({
            "receiver_id": transfer_jobs[0].state.receiver_id,
            "amount": transfer_jobs[0].state.amount,
        }))
        .gas(FT_TRANSFER_GAS)
        .deposit(FT_TRANSFER_DEPOSIT);

    for job in transfer_jobs.iter().skip(1) {
        let args = serde_json::json!({
            "receiver_id": job.state.receiver_id,
            "amount": job.state.amount,
        });

        call_builder = call_builder
            .call("ft_transfer")
            .args(args)
            .gas(FT_TRANSFER_GAS)
            .deposit(FT_TRANSFER_DEPOSIT);
    }

    // Send with ExecutedOptimistic for faster response, verification worker handles confirmation
    let result = call_builder
        .wait_until(TxExecutionStatus::ExecutedOptimistic)
        .send()
        .await;

    let rpc_duration = rpc_start.elapsed();
    debug!(
        "[METRIC] op=rpc_broadcast duration_ms={} batch_size={}",
        rpc_duration.as_millis(),
        transfer_jobs.len()
    );

    match result {
        Ok(outcome) => {
            let tx_hash = outcome
                .transaction_hash()
                .map(|h| h.to_string())
                .unwrap_or_else(|| "unknown".to_string());

            if outcome.is_success() {
                info!(
                    "Batch of {} transfers completed successfully, tx: {}",
                    transfer_jobs.len(),
                    tx_hash
                );

                finalize_completed_batch(
                    &mut conn,
                    stream_key,
                    consumer_group,
                    &transfer_jobs,
                    &tx_hash,
                )
                .await
            } else {
                warn!(
                    "Batch tx {} finished with non-success status - enqueuing for verification",
                    tx_hash
                );

                finalize_submitted_batch(
                    &mut conn,
                    stream_key,
                    consumer_group,
                    &transfer_jobs,
                    &tx_hash,
                    outcome.failure_message().as_deref(),
                )
                .await?;
                verification::enqueue_tx_verification_once(
                    &mut conn,
                    &ctx.runtime.env,
                    &tx_hash,
                    0,
                )
                .await?;
                Ok(())
            }
        }
        Err(e) => {
            let err_str = format!("{:?}", e);
            warn!("Failed to submit batch: {}", err_str);

            // Check if we got a tx hash from the error (for timeout cases)
            // In that case, enqueue for verification rather than retrying
            handle_submission_error(
                &mut conn,
                ctx,
                stream_key,
                consumer_group,
                &transfer_jobs,
                &e,
            )
            .await
        }
    }
}

async fn handle_submission_error(
    conn: &mut ConnectionManager,
    ctx: &TransferWorkerContext,
    stream_key: &str,
    consumer_group: &str,
    transfer_jobs: &[TransferJob],
    err: &near_kit::Error,
) -> Result<()> {
    let err_debug = format!("{:?}", err);

    for job in transfer_jobs {
        streams::ack_message(conn, stream_key, consumer_group, &job.stream_id).await?;

        let retry_count = job.message.retry_count + 1;
        if retry_count < MAX_RETRIES {
            state::increment_retry_count(conn, job.id()).await?;
            transfers::enqueue_transfer(conn, &ctx.runtime.env, job.id(), retry_count).await?;
        } else {
            state::update_transfer_status(conn, job.id(), Status::Failed).await?;
            events::log_event(
                conn,
                job.id(),
                Event::new("FAILED").with_reason(format!(
                    "Submission failed after {} retries: {}",
                    retry_count, err_debug
                )),
            )
            .await?;
        }
    }

    Ok(())
}

async fn finalize_completed_batch(
    conn: &mut ConnectionManager,
    stream_key: &str,
    consumer_group: &str,
    transfer_jobs: &[TransferJob],
    tx_hash: &str,
) -> Result<()> {
    let mut pipe = ::redis::pipe();
    pipe.atomic();
    let now = chrono::Utc::now().to_rfc3339();

    for job in transfer_jobs {
        let transfer_key = keys::transfer_state(job.id());
        let event_key = keys::transfer_events(job.id());

        pipe.hset(&transfer_key, "status", Status::Completed.as_str());
        pipe.hset(&transfer_key, "updated_at", &now);
        pipe.hset(&transfer_key, "completed_at", &now);
        pipe.hset(&transfer_key, "tx_hash", tx_hash);

        let submitted_event =
            serde_json::to_string(&Event::new("SUBMITTED").with_tx_hash(tx_hash.to_string()))?;
        pipe.lpush(&event_key, submitted_event);

        let completed_event = serde_json::to_string(&Event::new("COMPLETED"))?;
        pipe.lpush(&event_key, completed_event);
        pipe.expire(&event_key, 86400);

        pipe.cmd("XACK")
            .arg(stream_key)
            .arg(consumer_group)
            .arg(&job.stream_id);
    }

    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}

async fn finalize_submitted_batch(
    conn: &mut ConnectionManager,
    stream_key: &str,
    consumer_group: &str,
    transfer_jobs: &[TransferJob],
    tx_hash: &str,
    reason: Option<&str>,
) -> Result<()> {
    let mut pipe = ::redis::pipe();
    pipe.atomic();
    let now = chrono::Utc::now().to_rfc3339();
    let tx_transfers_key = keys::tx_transfers(tx_hash);

    for job in transfer_jobs {
        let transfer_key = keys::transfer_state(job.id());
        let event_key = keys::transfer_events(job.id());

        pipe.hset(&transfer_key, "status", Status::Submitted.as_str());
        pipe.hset(&transfer_key, "updated_at", &now);
        pipe.hset(&transfer_key, "tx_hash", tx_hash);

        pipe.sadd(&tx_transfers_key, job.id());
        pipe.expire(&tx_transfers_key, 86400);

        let submitted_event =
            serde_json::to_string(&Event::new("SUBMITTED").with_tx_hash(tx_hash.to_string()))?;
        pipe.lpush(&event_key, submitted_event);

        let mut queued_verification = Event::new("QUEUED_VERIFICATION");
        if let Some(reason_text) = reason {
            queued_verification = queued_verification.with_reason(reason_text.to_string());
        }
        let queued_event = serde_json::to_string(&queued_verification)?;
        pipe.lpush(&event_key, queued_event);
        pipe.expire(&event_key, 86400);

        pipe.cmd("XACK")
            .arg(stream_key)
            .arg(consumer_group)
            .arg(&job.stream_id);
    }

    pipe.query_async::<()>(&mut *conn).await?;
    Ok(())
}
