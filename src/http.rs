use crate::{
    application::{ActionCommand, Error, Repository},
    config::Config,
    domain::{self, Action},
    normalize, search,
    signals::Signals,
    store::PgRepository,
    wire,
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::{sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

pub struct App {
    pub config: Arc<Config>,
    pub repository: PgRepository,
    pub signals: Arc<Signals>,
    requests: Semaphore,
    search: reqwest::Client,
}
impl App {
    pub async fn new(config: Arc<Config>, signals: Arc<Signals>) -> Result<Arc<Self>, Error> {
        let repository = PgRepository::open(
            &config.database,
            config.tenant.clone(),
            config.expected_sources.clone(),
        )
        .await?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let search = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(8))
            .connect_timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(2)
            .build()
            .map_err(|_| Error::Unavailable)?;
        Ok(Arc::new(Self {
            config,
            repository,
            signals,
            requests: Semaphore::new(8),
            search,
        }))
    }
}
fn reject(error: Error) -> Response {
    let (status, kind) = match error {
        Error::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            wire::Error::DependencyUnavailable,
        ),
        Error::Capacity => (
            StatusCode::SERVICE_UNAVAILABLE,
            wire::Error::CapacityExhausted,
        ),
        Error::Invalid => (StatusCode::BAD_REQUEST, wire::Error::InvalidRequest),
        Error::NotFound => (StatusCode::NOT_FOUND, wire::Error::NotFound),
        Error::Revision => (StatusCode::CONFLICT, wire::Error::RevisionConflict),
        Error::Idempotency => (StatusCode::CONFLICT, wire::Error::IdempotencyConflict),
        Error::Transition => (StatusCode::CONFLICT, wire::Error::InvalidTransition),
    };
    (status, Json(wire::ObservabilityError { error: kind })).into_response()
}
fn token_matches(headers: &HeaderMap, expected: &str) -> bool {
    let Some(value) = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
    else {
        return false;
    };
    if value.len() > 128 {
        return false;
    }
    bool::from(Sha256::digest(value.as_bytes()).ct_eq(&Sha256::digest(expected.as_bytes())))
}
async fn boundary(State(app): State<Arc<App>>, request: Request<Body>, next: Next) -> Response {
    let public = matches!(request.uri().path(), "/v1/health" | "/v1/ready" | "/source");
    let expected = if request.uri().path() == "/v2/observability/events" {
        app.config.ingest_token.expose()
    } else {
        app.config.admin_token.expose()
    };
    let mut response = if !public && !token_matches(request.headers(), expected) {
        app.signals.rejected("unauthorized");
        (
            StatusCode::UNAUTHORIZED,
            Json(wire::ObservabilityError {
                error: wire::Error::Unauthorized,
            }),
        )
            .into_response()
    } else if let Ok(_permit) = app.requests.try_acquire() {
        match tokio::time::timeout(Duration::from_secs(10), next.run(request)).await {
            Ok(response) => response,
            Err(_) => reject(Error::Unavailable),
        }
    } else {
        app.signals.rejected("capacity");
        reject(Error::Capacity)
    };
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    response
}

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route(
            "/source",
            get(|| async {
                Json(crate::source_wire::SourceResponse {
                    source_url:
                        "https://github.com/NDDev-OpenNetwork/nddev-device-sync-observability"
                            .into(),
                    license: crate::source_wire::License::Agpl30Only,
                })
            }),
        )
        .route("/v1/health", get(|| async { "ok" }))
        .route("/v1/ready", get(ready))
        .route("/v2/observability/events", post(ingest))
        .route("/v2/observability/status", get(status))
        .route("/v2/observability/incidents", get(incidents))
        .route(
            "/v2/observability/incidents/{id}/acknowledge",
            post(acknowledge),
        )
        .route("/v2/observability/incidents/{id}/silence", post(silence))
        .route("/v2/observability/incidents/{id}/resolve", post(resolve))
        .route("/v2/observability/search", post(search))
        .layer(DefaultBodyLimit::max(
            100 * (nddev_device_sync_telemetry::MAX_EVENT_BYTES + 1),
        ))
        .with_state(app.clone())
        .layer(middleware::from_fn_with_state(app, boundary))
}
/// Scoped diagnostic search. Only the server calls this route (admin token); it states the
/// scope it authorized in `x-nds-scope-kind` and `x-nds-scope-subject`, and the body can only
/// narrow to that scope.
async fn search(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
    let refuse = |status, error| (status, Json(wire::ObservabilityError { error })).into_response();
    let Some(config) = app.config.search.as_ref() else {
        return reject(Error::Unavailable);
    };
    if body.len() > 8192 {
        return reject(Error::Invalid);
    }
    let Ok(request) = serde_json::from_slice::<wire::DiagnosticSearchRequest>(&body) else {
        return reject(Error::Invalid);
    };
    let header = |name| {
        headers
            .get(name)
            .and_then(|v: &HeaderValue| v.to_str().ok())
    };
    let scope = match search::scope(
        header("x-nds-scope-kind"),
        header("x-nds-scope-subject"),
        &request,
    ) {
        Ok(scope) => scope,
        Err(_) => return refuse(StatusCode::FORBIDDEN, wire::Error::ForbiddenScope),
    };
    match search::search(&app.search, config, &scope, &request, Utc::now()).await {
        Ok(page) => Json(page).into_response(),
        Err(search::SearchError::Invalid) => reject(Error::Invalid),
        Err(search::SearchError::Forbidden) => {
            refuse(StatusCode::FORBIDDEN, wire::Error::ForbiddenScope)
        }
        Err(search::SearchError::Unavailable) => reject(Error::Unavailable),
    }
}
async fn ready(State(app): State<Arc<App>>) -> Response {
    match app.repository.ready().await {
        Ok(()) => (StatusCode::OK, "ready").into_response(),
        Err(error) => reject(error),
    }
}

async fn ingest(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
    if headers.get("content-type").and_then(|v| v.to_str().ok()) != Some("application/x-ndjson") {
        return reject(Error::Invalid);
    }
    let mut events = Vec::new();
    let mut spans = Vec::new();
    for line in body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        if line.len() > nddev_device_sync_telemetry::MAX_EVENT_BYTES || events.len() >= 100 {
            app.signals.rejected("invalid");
            return reject(Error::Invalid);
        }
        let value = match serde_json::from_slice(line) {
            Ok(value) => value,
            Err(_) => {
                app.signals.rejected("invalid");
                return reject(Error::Invalid);
            }
        };
        match normalize::accept(value, Utc::now()) {
            Ok((event, accepted)) => {
                spans.push(app.signals.processing(&event));
                events.push(accepted);
            }
            Err(error) => {
                app.signals.rejected("invalid");
                return reject(error);
            }
        }
    }
    if events.is_empty() {
        return reject(Error::Invalid);
    }
    match app
        .repository
        .ingest(
            &events,
            app.config.export_enabled,
            app.config.outbox_ttl.as_secs(),
        )
        .await
    {
        Ok(receipt) => {
            app.signals.accepted(receipt.accepted);
            Json(wire::IngressReceipt {
                accepted: receipt.accepted as i32,
                duplicates: receipt.duplicates as i32,
                export_mode: export_mode(&app),
            })
            .into_response()
        }
        Err(error) => {
            app.signals.rejected("storage");
            reject(error)
        }
    }
}
fn export_mode(app: &App) -> wire::ExportMode {
    if app.config.export_enabled {
        wire::ExportMode::Enabled
    } else {
        wire::ExportMode::Disabled
    }
}

fn formatted(ms: i64) -> String {
    DateTime::from_timestamp_millis(ms)
        .expect("validated database time")
        .to_rfc3339()
}
fn incident(value: domain::Incident) -> Result<wire::Incident, Error> {
    let kind = match value.kind.as_str() {
        "admission" => wire::Kind::Admission,
        "database" => wire::Kind::Database,
        "tls" => wire::Kind::Tls,
        "process" => wire::Kind::Process,
        "delivery" => wire::Kind::Delivery,
        "source_stale" => wire::Kind::SourceStale,
        "source_gap" => wire::Kind::SourceGap,
        _ => return Err(Error::Unavailable),
    };
    Ok(wire::Incident {
        incident_id: value.id,
        owner: crate::wire::Owner::Operator,
        severity: if value.severity == "critical" {
            crate::wire::Severity::Critical
        } else {
            crate::wire::Severity::Warning
        },
        dedup_key: value.dedup_key,
        service_name: value.service,
        kind,
        state: match value.head.state {
            domain::State::Open => wire::IncidentState::Open,
            domain::State::Acknowledged => wire::IncidentState::Acknowledged,
            domain::State::Resolved => wire::IncidentState::Resolved,
        },
        revision: value.head.revision,
        occurrences: value.occurrences,
        first_seen: formatted(value.first_seen_ms),
        last_seen: formatted(value.last_seen_ms),
        silenced_until: value.head.silenced_until_ms.map(formatted),
        resolved_at: value.resolved_at_ms.map(formatted),
    })
}
#[derive(Deserialize)]
struct Page {
    #[serde(default)]
    after: String,
    #[serde(default = "page_limit")]
    limit: usize,
}
fn page_limit() -> usize {
    50
}
async fn incidents(State(app): State<Arc<App>>, Query(page): Query<Page>) -> Response {
    if !(1..=100).contains(&page.limit) || page.after.len() > 128 {
        return reject(Error::Invalid);
    }
    match app.repository.incidents(&page.after, page.limit + 1).await {
        Ok(mut values) => {
            let has_more = values.len() > page.limit;
            values.truncate(page.limit);
            let cursor = has_more.then(|| values.last().expect("nonempty page").id.clone());
            let converted: Result<Vec<_>, _> = values.into_iter().map(incident).collect();
            match converted {
                Ok(incidents) => Json(wire::IncidentPage {
                    incidents,
                    next_cursor: cursor,
                    generated_at: Utc::now().to_rfc3339(),
                })
                .into_response(),
                Err(error) => reject(error),
            }
        }
        Err(error) => reject(error),
    }
}
async fn apply(
    app: &App,
    id: String,
    headers: HeaderMap,
    body: &[u8],
    action: Action,
    operation: String,
    expected: i64,
) -> Response {
    let Some(actor) = headers
        .get("x-nds-actor")
        .and_then(|v| v.to_str().ok())
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-_:".contains(&b))
        })
    else {
        return reject(Error::Invalid);
    };
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_:".contains(&b))
        || operation.len() > 128
        || body.len() > 8192
    {
        return reject(Error::Invalid);
    }
    let mut fingerprint = Sha256::new();
    // All components are either bounded identifier fields or fixed numeric/enum
    // encodings; length prefixes preserve the exact actor and semantic request.
    let action_name = match action {
        Action::Acknowledge => "acknowledge",
        Action::Silence { .. } => "silence",
        Action::Resolve => "resolve",
    };
    for value in [id.as_bytes(), actor.as_bytes(), action_name.as_bytes()] {
        fingerprint.update((value.len() as u64).to_be_bytes());
        fingerprint.update(value);
    }
    fingerprint.update(expected.to_be_bytes());
    if let Action::Silence { until_ms } = action {
        fingerprint.update(until_ms.to_be_bytes());
    }
    let command = ActionCommand {
        incident: id,
        operation,
        actor: actor.to_owned(),
        expected,
        action,
        fingerprint: fingerprint.finalize().into(),
    };
    match app.repository.action(command).await.and_then(incident) {
        Ok(value) => Json(value).into_response(),
        Err(error) => reject(error),
    }
}
async fn acknowledge(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok(value) = serde_json::from_slice::<wire::IncidentAction>(&body) else {
        return reject(Error::Invalid);
    };
    apply(
        &app,
        id,
        headers,
        &body,
        Action::Acknowledge,
        value.operation_id,
        value.expected_revision,
    )
    .await
}
async fn resolve(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok(value) = serde_json::from_slice::<wire::IncidentAction>(&body) else {
        return reject(Error::Invalid);
    };
    apply(
        &app,
        id,
        headers,
        &body,
        Action::Resolve,
        value.operation_id,
        value.expected_revision,
    )
    .await
}
async fn silence(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok(value) = serde_json::from_slice::<wire::SilenceAction>(&body) else {
        return reject(Error::Invalid);
    };
    let Ok(until) = DateTime::parse_from_rfc3339(&value.until) else {
        return reject(Error::Invalid);
    };
    apply(
        &app,
        id,
        headers,
        &body,
        Action::Silence {
            until_ms: until.timestamp_millis(),
        },
        value.operation_id,
        value.expected_revision,
    )
    .await
}
async fn status(State(app): State<Arc<App>>) -> Response {
    match snapshot(&app).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => reject(error),
    }
}
async fn snapshot(app: &App) -> Result<wire::ObservabilityStatus, Error> {
    let budget = sqlx::query("SELECT * FROM delivery_state WHERE tenant_id=$1")
        .bind(&*app.repository.tenant)
        .fetch_one(&app.repository.pool)
        .await
        .map_err(|_| Error::Unavailable)?;
    let rows = sqlx::query(
        "SELECT *,service_name=ANY($2) AS configured FROM sources WHERE tenant_id=$1 AND required ORDER BY service_name LIMIT 16",
    )
    .bind(&*app.repository.tenant)
    .bind(app.repository.expected.as_ref())
    .fetch_all(&app.repository.pool)
    .await
    .map_err(|_| Error::Unavailable)?;
    let mut sources = Vec::new();
    let cutoff = Utc::now() - chrono::Duration::seconds(app.config.stale_after.as_secs() as i64);
    for row in rows {
        let received: Option<DateTime<Utc>> = row
            .try_get("last_received_at")
            .map_err(|_| Error::Unavailable)?;
        let event: Option<DateTime<Utc>> = row
            .try_get("last_event_at")
            .map_err(|_| Error::Unavailable)?;
        let sequence: Option<i64> = row
            .try_get("last_sequence")
            .map_err(|_| Error::Unavailable)?;
        let gap: bool = row.try_get("gap_active").map_err(|_| Error::Unavailable)?;
        let configured: bool = row.try_get("configured").map_err(|_| Error::Unavailable)?;
        let state = if !configured {
            wire::SourceState::Inactive
        } else if received.is_none() {
            wire::SourceState::Missing
        } else if received.is_some_and(|v| v < cutoff) || event.is_none_or(|v| v < cutoff) {
            wire::SourceState::Stale
        } else if sequence.is_none() {
            wire::SourceState::Unsequenced
        } else if gap {
            wire::SourceState::GapObserved
        } else {
            wire::SourceState::Fresh
        };
        sources.push(wire::SourceHealth {
            service_name: row
                .try_get("service_name")
                .map_err(|_| Error::Unavailable)?,
            state,
            last_received_at: received.map(|v| v.to_rfc3339()),
            last_event_at: event.map(|v| v.to_rfc3339()),
            last_sequence: sequence,
            gap_observations: row
                .try_get("gap_observations")
                .map_err(|_| Error::Unavailable)?,
        });
    }
    let success: Option<DateTime<Utc>> = budget
        .try_get("last_success_at")
        .map_err(|_| Error::Unavailable)?;
    let last_outcome: String = budget
        .try_get("last_outcome")
        .map_err(|_| Error::Unavailable)?;
    let delivered: i64 = budget
        .try_get("delivered_events")
        .map_err(|_| Error::Unavailable)?;
    Ok(wire::ObservabilityStatus {
        export_mode: export_mode(app),
        queued_events: i32::try_from(
            budget
                .try_get::<i64, _>("queued_events")
                .map_err(|_| Error::Unavailable)?,
        )
        .map_err(|_| Error::Unavailable)?,
        queued_bytes: i32::try_from(
            budget
                .try_get::<i64, _>("queued_bytes")
                .map_err(|_| Error::Unavailable)?,
        )
        .map_err(|_| Error::Unavailable)?,
        delivered_events: delivered,
        expired_events: budget
            .try_get("expired_events")
            .map_err(|_| Error::Unavailable)?,
        sources,
        logs: wire::SignalDelivery {
            state: if !app.config.export_enabled {
                wire::State::Disabled
            } else if last_outcome == "failure" {
                wire::State::Degraded
            } else if last_outcome == "success" {
                wire::State::Healthy
            } else {
                wire::State::Unknown
            },
            successes: delivered,
            failures: budget.try_get("failures").map_err(|_| Error::Unavailable)?,
            last_success_at: success.map(|v| v.to_rfc3339()),
        },
        metrics: app
            .signals
            .metrics_delivery
            .snapshot(app.config.export_enabled),
        traces: app
            .signals
            .traces_delivery
            .snapshot(app.config.export_enabled),
    })
}
