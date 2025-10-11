use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Result};
use ft_relay::{RedisSettings, RelayConfig};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::types::{AccountId, BlockReference, Finality};
use near_primitives::views::QueryRequest;
use redis::aio::ConnectionManager;
use redis::Client;
use reqwest::Client as HttpClient;
use serde_json::json;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub async fn flush_redis(url: &str) -> Result<()> {
    let client = Client::open(url)?;
    let mut conn = ConnectionManager::new(client).await?;
    redis::cmd("FLUSHALL").query_async::<()>(&mut conn).await?;
    println!("Redis flushed at {}", url);
    Ok(())
}

pub fn redis_settings(url: impl Into<String>) -> RedisSettings {
    RedisSettings::new(url)
}

pub fn redis_settings_from_env() -> RedisSettings {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    redis_settings(url)
}

pub struct RelayHandle {
    join: JoinHandle<()>,
}

pub async fn spawn_relay(config: RelayConfig) -> RelayHandle {
    let join = tokio::spawn(async move {
        if let Err(err) = ft_relay::run(config).await {
            eprintln!("Relay server error: {err:?}");
            panic!("Relay server terminated: {:?}", err);
        }
    });
    RelayHandle { join }
}

impl RelayHandle {
    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    pub async fn shutdown(self) {
        self.join.abort();
        let _ = self.join.await;
    }
}

pub async fn wait_for_health(base_url: &str, attempts: usize, delay: Duration) -> Result<()> {
    let client = HttpClient::new();
    let health_url = format!("{}/health", base_url.trim_end_matches('/'));
    for attempt in 1..=attempts {
        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                println!("Server is ready (attempt {})", attempt);
                return Ok(());
            }
            _ if attempt < attempts => tokio::time::sleep(delay).await,
            _ => {
                return Err(anyhow!(
                    "Server health check failed after {} attempts",
                    attempts
                ))
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct BenchmarkPlan {
    pub name: &'static str,
    pub relay_config: RelayConfig,
    pub redis_url: String,
    pub redis_namespace: String,
    pub total_requests: usize,
    pub concurrency: usize,
    pub transfer_amount: String,
    pub receivers: Vec<AccountId>,
    pub token_account: AccountId,
    pub rpc_url: String,
    pub yocto_per_transfer: u128,
    pub min_throughput: f64,
    pub max_duration: Duration,
    pub poll_interval: Duration,
    pub max_polls: usize,
    pub queue_drain_interval: Duration,
    pub max_queue_checks: usize,
}

impl BenchmarkPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &'static str,
        relay_config: RelayConfig,
        redis_url: impl Into<String>,
        redis_namespace: impl Into<String>,
        total_requests: usize,
        receivers: Vec<AccountId>,
        token_account: AccountId,
        rpc_url: impl Into<String>,
        transfer_amount: impl Into<String>,
        yocto_per_transfer: u128,
    ) -> Self {
        Self {
            name,
            relay_config,
            redis_url: redis_url.into(),
            redis_namespace: redis_namespace.into(),
            total_requests,
            concurrency: receivers.len().max(1),
            transfer_amount: transfer_amount.into(),
            receivers,
            token_account,
            rpc_url: rpc_url.into(),
            yocto_per_transfer,
            min_throughput: 0.0,
            max_duration: Duration::from_secs(600),
            poll_interval: Duration::from_secs(5),
            max_polls: 300,
            queue_drain_interval: Duration::from_millis(500),
            max_queue_checks: 240,
        }
    }

    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    pub fn with_min_throughput(mut self, value: f64) -> Self {
        self.min_throughput = value;
        self
    }

    pub fn with_max_duration(mut self, duration: Duration) -> Self {
        self.max_duration = duration;
        self
    }

    pub fn with_polling(mut self, interval: Duration, max_polls: usize) -> Self {
        self.poll_interval = interval;
        self.max_polls = max_polls;
        self
    }

    #[allow(dead_code)]
    pub fn with_queue_drain(mut self, interval: Duration, max_checks: usize) -> Self {
        self.queue_drain_interval = interval;
        self.max_queue_checks = max_checks;
        self
    }
}

pub struct HttpMetrics {
    pub accepted: usize,
    pub failed: usize,
    pub duration: Duration,
    pub throughput: f64,
    pub success_rate: f64,
    pub start: Instant,
    pub end: Instant,
}

pub struct OnchainMetrics {
    pub final_total: u128,
    pub success_rate: f64,
    pub onchain_duration: Duration,
    pub settlement_duration: Duration,
    pub post_api_duration: Duration,
    pub blockchain_throughput: f64,
    pub post_api_throughput: f64,
    pub receiver_balances: Vec<(AccountId, u128)>,
}

pub struct BenchmarkOutcome {
    pub http: HttpMetrics,
    pub onchain: OnchainMetrics,
}

pub fn print_benchmark_summary(
    name: &str,
    outcome: &BenchmarkOutcome,
    expected_per_receiver: Option<u128>,
    yocto_per_transfer: u128,
) {
    println!();
    println!("Benchmark: {}", name);
    println!(
        "  HTTP phase   : {:.2}s, throughput {:.1} req/s, success {:.2}%",
        outcome.http.duration.as_secs_f64(),
        outcome.http.throughput,
        outcome.http.success_rate
    );
    println!(
        "  Settlement   : loop {:.2}s, total {:.2}s (lag {:.2}s), throughput {:.1} tx/s overall / {:.1} tx/s post-HTTP, success {:.2}%",
        outcome.onchain.settlement_duration.as_secs_f64(),
        outcome.onchain.onchain_duration.as_secs_f64(),
        outcome.onchain.post_api_duration.as_secs_f64(),
        outcome.onchain.blockchain_throughput,
        outcome.onchain.post_api_throughput,
        outcome.onchain.success_rate
    );
    let env_base = name
        .split_once('_')
        .map(|(prefix, _)| prefix)
        .unwrap_or(name);
    let env_key = env_base.to_ascii_lowercase();
    let env_title = {
        let mut chars = env_base.chars();
        match chars.next() {
            Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            None => String::new(),
        }
    };

    let metrics: Vec<(&str, String)> = vec![
        ("Benchmark", name.to_string()),
        (
            "HTTP duration (s)",
            format!("{:.2}", outcome.http.duration.as_secs_f64()),
        ),
        (
            "HTTP throughput (req/s)",
            format!("{:.1}", outcome.http.throughput),
        ),
        (
            "HTTP success (%)",
            format!("{:.2}", outcome.http.success_rate),
        ),
        (
            "Settlement loop (s)",
            format!("{:.2}", outcome.onchain.settlement_duration.as_secs_f64()),
        ),
        (
            "Settlement total (s)",
            format!("{:.2}", outcome.onchain.onchain_duration.as_secs_f64()),
        ),
        (
            "Settlement lag (s)",
            format!("{:.2}", outcome.onchain.post_api_duration.as_secs_f64()),
        ),
        (
            "Chain throughput (tx/s)",
            format!("{:.1}", outcome.onchain.blockchain_throughput),
        ),
        (
            "Post-HTTP throughput (tx/s)",
            format!("{:.1}", outcome.onchain.post_api_throughput),
        ),
        (
            "On-chain success (%)",
            format!("{:.2}", outcome.onchain.success_rate),
        ),
    ];

    println!("| Metric | {} |", env_title);
    println!("| --- | --- |");
    for (metric, value) in &metrics {
        let display_value = if *metric == "Chain throughput (tx/s)" {
            format!("**{}**", value)
        } else {
            value.clone()
        };
        println!("| {} | {} |", metric, display_value);
    }

    for (metric, value) in &metrics {
        println!("BENCHMARK_METRIC|{}|{}|{}", env_key, metric, value);
    }

    if let Some(expected) = expected_per_receiver {
        println!("Receiver balances:");
        for (account, balance) in &outcome.onchain.receiver_balances {
            println!(
                "  {} => {} (expected: {})",
                account,
                balance / yocto_per_transfer,
                expected / yocto_per_transfer
            );
        }
    }
}

pub async fn run_benchmark(plan: BenchmarkPlan) -> Result<BenchmarkOutcome> {
    let bind_addr = plan.relay_config.bind_addr.clone();
    let base_url = format!("http://{}", bind_addr);

    let relay_handle = spawn_relay(plan.relay_config.clone()).await;
    let outcome = run_benchmark_inner(&plan, &base_url, &relay_handle).await;
    relay_handle.shutdown().await;
    outcome
}

async fn run_benchmark_inner(
    plan: &BenchmarkPlan,
    base_url: &str,
    relay_handle: &RelayHandle,
) -> Result<BenchmarkOutcome> {
    println!("Waiting for relay health check at {}...", base_url);
    wait_for_health(base_url, 20, Duration::from_millis(500)).await?;
    println!(
        "Starting benchmark '{}' with {} requests (concurrency {}).",
        plan.name, plan.total_requests, plan.concurrency
    );

    let http_metrics = execute_http_phase(plan).await?;

    ensure!(
        http_metrics.accepted == plan.total_requests,
        "Accepted {} of {} HTTP requests",
        http_metrics.accepted,
        plan.total_requests
    );
    ensure!(
        http_metrics.throughput >= plan.min_throughput,
        "HTTP throughput {:.2} req/sec below target {:.2}",
        http_metrics.throughput,
        plan.min_throughput
    );
    ensure!(
        http_metrics.duration <= plan.max_duration,
        "HTTP phase took {:.2?}, exceeds {:?}",
        http_metrics.duration,
        plan.max_duration
    );
    ensure!(
        !relay_handle.is_finished(),
        "Relay server task ended during HTTP load"
    );

    println!(
        "HTTP phase complete: accepted {} / failed {} / throughput {:.1} req/s",
        http_metrics.accepted, http_metrics.failed, http_metrics.throughput
    );

    let mut redis_conn = ConnectionManager::new(Client::open(plan.redis_url.as_str())?).await?;
    let final_count = verify_transfer_state(&mut redis_conn, http_metrics.accepted as u64).await?;
    ensure!(
        final_count == http_metrics.accepted as u64,
        "Redis recorded {} transfer states but expected {}",
        final_count,
        http_metrics.accepted
    );

    wait_for_transfer_queue(plan, &mut redis_conn).await?;

    let expected_total = (http_metrics.accepted as u128) * plan.yocto_per_transfer;
    let onchain_metrics = verify_onchain(plan, &http_metrics, expected_total).await?;

    ensure!(
        onchain_metrics.final_total == expected_total,
        "On-chain total {} mismatches expected {}",
        onchain_metrics.final_total,
        expected_total
    );
    ensure!(
        onchain_metrics.success_rate >= 100.0,
        "On-chain success {:.2}% below 100%",
        onchain_metrics.success_rate
    );
    println!(
        "Benchmark '{}' succeeded: on-chain success {:.2}% in {:.1}s",
        plan.name,
        onchain_metrics.success_rate,
        onchain_metrics.onchain_duration.as_secs_f64()
    );

    Ok(BenchmarkOutcome {
        http: http_metrics,
        onchain: onchain_metrics,
    })
}

async fn execute_http_phase(plan: &BenchmarkPlan) -> Result<HttpMetrics> {
    let endpoint = format!("http://{}/v1/transfer", plan.relay_config.bind_addr);
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let start = Instant::now();
    let phase = PhaseTimer::with_start("HTTP load", start);
    let receiver_count = plan.receivers.len();
    let requests_per_worker = plan.total_requests / plan.concurrency;

    let mut tasks = Vec::with_capacity(plan.concurrency);
    for worker_id in 0..plan.concurrency {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let receivers = plan.receivers.clone();
        let amount = plan.transfer_amount.clone();
        let worker_start = start;
        let start_idx = worker_id * requests_per_worker;
        let end_idx = if worker_id == plan.concurrency - 1 {
            plan.total_requests
        } else {
            start_idx + requests_per_worker
        };

        let task = tokio::spawn(async move {
            let mut success = 0usize;
            let mut errors = 0usize;
            for i in start_idx..end_idx {
                let receiver = &receivers[i % receiver_count];
                let payload = json!({
                    "receiver_id": receiver,
                    "amount": amount,
                });
                let idempotency_key = Uuid::new_v4().to_string();

                match client
                    .post(&endpoint)
                    .header("X-Idempotency-Key", &idempotency_key)
                    .json(&payload)
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => success += 1,
                    Ok(resp) => {
                        if worker_id == 0 && success == 0 && errors == 0 {
                            eprintln!(
                                "Worker {} first failure: status {}",
                                worker_id,
                                resp.status()
                            );
                        }
                        errors += 1;
                    }
                    Err(err) => {
                        if worker_id == 0 && success == 0 && errors == 0 {
                            eprintln!("Worker {} request error: {:?}", worker_id, err);
                        }
                        errors += 1;
                    }
                }

                if worker_id == 0 && (i - start_idx + 1) % 10_000 == 0 {
                    let elapsed = worker_start.elapsed();
                    let rate = (i - start_idx + 1) as f64 / elapsed.as_secs_f64().max(1e-6);
                    println!(
                        "  Progress: {} requests handled ({:.1} req/sec)",
                        i - start_idx + 1,
                        rate
                    );
                }
            }

            (success, errors)
        });
        tasks.push(task);
    }

    let mut accepted = 0usize;
    let mut failed = 0usize;
    for task in tasks {
        let (success, errors) = task.await.unwrap_or((0, 0));
        accepted += success;
        failed += errors;
    }

    let duration = phase.finish();
    let success_rate = (accepted as f64 / plan.total_requests as f64) * 100.0;
    let throughput = if duration.as_secs_f64() > 0.0 {
        accepted as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    Ok(HttpMetrics {
        accepted,
        failed,
        duration,
        throughput,
        success_rate,
        start,
        end: start + duration,
    })
}

async fn count_transfer_states(redis_conn: &mut ConnectionManager) -> Result<u64> {
    let mut cursor: u64 = 0;
    let mut count: u64 = 0;

    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("transfer:*")
            .arg("COUNT")
            .arg(2000)
            .query_async(redis_conn)
            .await?;

        count += keys.iter().filter(|k| !k.ends_with(":ev")).count() as u64;
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(count)
}

async fn verify_transfer_state(redis_conn: &mut ConnectionManager, expected: u64) -> Result<u64> {
    let mut last_count = 0u64;
    for attempt in 1..=60 {
        let count = count_transfer_states(redis_conn).await?;
        if count != last_count {
            println!(
                "Redis transfer states attempt {}: {} (expected {})",
                attempt, count, expected
            );
        }
        if count >= expected {
            return Ok(count);
        }
        last_count = count;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Err(anyhow!(
        "Timed out waiting for {} transfer state entries in Redis",
        expected
    ))
}

async fn pending_in_group(
    redis_conn: &mut ConnectionManager,
    stream_key: &str,
    group_name: &str,
) -> Result<u64> {
    if let redis::Value::Array(items) = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(stream_key)
        .query_async::<redis::Value>(redis_conn)
        .await?
    {
        for group in items {
            if let redis::Value::Array(kv) = group {
                let mut name: Option<String> = None;
                let mut pending: Option<u64> = None;
                let mut i = 0;
                while i + 1 < kv.len() {
                    if let redis::Value::BulkString(key) = &kv[i] {
                        if key == b"name" {
                            if let redis::Value::BulkString(value) = &kv[i + 1] {
                                name = std::str::from_utf8(value).ok().map(|s| s.to_string());
                            }
                        } else if key == b"pending" {
                            if let redis::Value::Int(value) = &kv[i + 1] {
                                pending = Some(*value as u64);
                            }
                        }
                    }
                    i += 2;
                }

                if let (Some(n), Some(p)) = (name, pending) {
                    if n == group_name {
                        return Ok(p);
                    }
                }
            }
        }
    }

    Ok(0)
}

async fn wait_for_transfer_queue(
    plan: &BenchmarkPlan,
    redis_conn: &mut ConnectionManager,
) -> Result<()> {
    let stream_key = format!("{}:xfer", plan.redis_namespace);
    let group_name = format!("{}:xfer_workers", plan.redis_namespace);

    for attempt in 1..=plan.max_queue_checks {
        let pending = pending_in_group(redis_conn, &stream_key, &group_name).await?;
        if pending == 0 {
            println!("Transfer worker queue drained after {} checks", attempt);
            return Ok(());
        }
        tokio::time::sleep(plan.queue_drain_interval).await;
    }

    Err(anyhow!("Transfer queue did not drain within timeout"))
}

async fn verify_onchain(
    plan: &BenchmarkPlan,
    http: &HttpMetrics,
    expected_total: u128,
) -> Result<OnchainMetrics> {
    let settlement_phase = PhaseTimer::start("On-chain settlement wait");

    let client = JsonRpcClient::connect(&plan.rpc_url);
    let mut final_balances = Vec::new();
    let mut final_total: u128 = 0;
    let mut final_success_rate = 0.0;
    let mut completion_instant: Option<Instant> = None;

    for poll in 1..=plan.max_polls {
        let mut balances = Vec::with_capacity(plan.receivers.len());
        let mut total_balance: u128 = 0;

        for receiver in &plan.receivers {
            let balance_args = json!({
                "account_id": receiver,
            })
            .to_string()
            .into_bytes();

            let request = methods::query::RpcQueryRequest {
                block_reference: BlockReference::Finality(Finality::Final),
                request: QueryRequest::CallFunction {
                    account_id: plan.token_account.clone(),
                    method_name: "ft_balance_of".to_string(),
                    args: near_primitives::types::FunctionArgs::from(balance_args),
                },
            };

            let response = client.call(request).await?;
            let balance: u128 = match response.kind {
                QueryResponseKind::CallResult(result) => {
                    let balance_str: String = serde_json::from_slice(&result.result)?;
                    balance_str.parse().unwrap_or(0)
                }
                _ => 0,
            };

            total_balance += balance;
            balances.push(balance);
        }

        final_success_rate = (total_balance as f64 / expected_total as f64) * 100.0;
        println!(
            "On-chain poll {}: {} tokens ({:.2}% complete)",
            poll,
            total_balance / plan.yocto_per_transfer,
            final_success_rate
        );

        if total_balance >= expected_total {
            let elapsed_secs = (poll as u64) * plan.poll_interval.as_secs();
            println!(
                "Reached expected total after {} polls (~{}s)",
                poll, elapsed_secs
            );
            final_balances = balances;
            final_total = total_balance;
            completion_instant = Some(Instant::now());
            break;
        }

        if poll == plan.max_polls {
            let elapsed_secs = (poll as u64) * plan.poll_interval.as_secs();
            println!("Polling cap reached (~{}s); proceeding", elapsed_secs);
            final_balances = balances;
            final_total = total_balance;
            completion_instant = Some(Instant::now());
            break;
        }

        tokio::time::sleep(plan.poll_interval).await;
    }

    let settlement_duration = settlement_phase.finish();
    let completion_instant = completion_instant.unwrap_or_else(Instant::now);
    let onchain_elapsed = completion_instant
        .checked_duration_since(http.start)
        .unwrap_or_else(|| Duration::from_secs(0));
    let post_api_elapsed = completion_instant
        .checked_duration_since(http.end)
        .unwrap_or_else(|| Duration::from_secs(0));
    let completed_transfers = (final_total / plan.yocto_per_transfer) as usize;
    let blockchain_throughput = if onchain_elapsed.as_secs_f64() > 0.0 {
        completed_transfers as f64 / onchain_elapsed.as_secs_f64()
    } else {
        0.0
    };
    let post_api_throughput = if post_api_elapsed.as_secs_f64() > 0.0 {
        completed_transfers as f64 / post_api_elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!(
        "On-chain totals: expected {} / actual {} (success {:.2}% in {:.1}s)",
        expected_total / plan.yocto_per_transfer,
        final_total / plan.yocto_per_transfer,
        final_success_rate,
        onchain_elapsed.as_secs_f64()
    );

    Ok(OnchainMetrics {
        final_total,
        success_rate: final_success_rate,
        onchain_duration: onchain_elapsed,
        settlement_duration,
        post_api_duration: post_api_elapsed,
        blockchain_throughput,
        post_api_throughput,
        receiver_balances: plan
            .receivers
            .iter()
            .cloned()
            .zip(final_balances.into_iter())
            .collect(),
    })
}

pub struct PhaseTimer {
    label: String,
    start: Instant,
}

impl PhaseTimer {
    pub fn start(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            start: Instant::now(),
        }
    }

    pub fn with_start(label: impl Into<String>, start: Instant) -> Self {
        Self {
            label: label.into(),
            start,
        }
    }

    pub fn finish(self) -> Duration {
        let elapsed = self.start.elapsed();
        println!("{} completed in {:.2?}", self.label, elapsed);
        elapsed
    }
}
