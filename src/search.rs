//! Scoped diagnostic search over the stored OpenObserve records (P2).
//!
//! The signed NDS server is the only caller: it authenticates the user, decides which scope the
//! request may read, and passes that decision in fixed headers next to its admin token. This
//! service never trusts the request body for identity. SQL is assembled only from validated
//! identifiers and fixed column names, so no request value can carry query syntax, and every hit
//! is projected onto the fixed, redacted `DiagnosticRecord` fields; raw stored rows never leave.
use crate::config::SearchConfig;
use crate::wire::{
    DiagnosticFilters, DiagnosticRecord, DiagnosticScope, DiagnosticSearchPage,
    DiagnosticSearchRequest, DiagnosticSignal,
};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

/// Stored diagnostics are kept one week; a search never reaches further.
pub const MAX_WINDOW: Duration = Duration::days(7);
const MAX_RESPONSE_BYTES: usize = 1_048_576;

/// The scope the server authorized, independent of what the body asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    Member { user: String },
    Organization { tenant: String },
    Global,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SearchError {
    Invalid,
    Forbidden,
    Unavailable,
}

/// Identifiers are the only values that reach SQL: they cannot contain quotes, spaces,
/// operators or comment markers.
pub fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}

/// Resolves the server's scope headers against the request body: the body may only narrow to
/// what the server authorized.
pub fn scope(
    kind: Option<&str>,
    subject: Option<&str>,
    request: &DiagnosticSearchRequest,
) -> Result<Scope, SearchError> {
    let subject = subject.filter(|s| identifier(s));
    let authorized = match (kind, subject) {
        (Some("member"), Some(user)) => Scope::Member { user: user.into() },
        (Some("organization"), Some(tenant)) => Scope::Organization {
            tenant: tenant.into(),
        },
        (Some("global"), None) => Scope::Global,
        _ => return Err(SearchError::Forbidden),
    };
    match (&request.scope, &authorized) {
        (DiagnosticScope::Member, Scope::Member { .. })
        | (DiagnosticScope::Global, Scope::Global) => Ok(authorized),
        (DiagnosticScope::Organization, Scope::Organization { tenant })
            if request.organization_id.as_deref() == Some(tenant.as_str()) =>
        {
            Ok(authorized)
        }
        _ => Err(SearchError::Forbidden),
    }
}

/// Where one signal is stored and how its columns are named. OpenObserve flattens dotted keys
/// with `_`; for traces it prefixes resource attributes with `service_` (the server attaches
/// `user.id`, `tenant.id` and `device.id` as resource attributes), while span attributes keep
/// their own names.
pub struct Layout {
    pub stream: String,
    /// OpenObserve stream type, sent as `?type=` on every call.
    kind: &'static str,
    user: &'static str,
    tenant: &'static str,
    time: &'static str,
    /// (record field, stored column)
    projection: &'static [(&'static str, &'static str)],
    filters: &'static [(&'static str, &'static str)],
}

const LOG_PROJECTION: &[(&str, &str)] = &[
    ("service_name", "service_name"),
    ("event_name", "event_name"),
    ("module", "module"),
    ("outcome", "outcome"),
    ("error_type", "error_type"),
    ("severity", "severity"),
    ("operation_id", "operation_id"),
    ("device_id", "device_id"),
    ("user_id", "user_id"),
    ("tenant_id", "tenant_id"),
    ("trace_id", "trace_id"),
    ("span_id", "span_id"),
    ("duration_ms", "duration_ms"),
];
const LOG_FILTERS: &[(&str, &str)] = &[
    ("operation_id", "operation_id"),
    ("event_name", "event_name"),
    ("module", "module"),
    ("device_id", "device_id"),
    ("outcome", "outcome"),
    ("trace_id", "trace_id"),
];
const TRACE_PROJECTION: &[(&str, &str)] = &[
    ("service_name", "service_name"),
    ("name", "operation_name"),
    ("module", "module"),
    ("operation_id", "operation_id"),
    ("device_id", "service_device_id"),
    ("user_id", "service_user_id"),
    ("tenant_id", "service_tenant_id"),
    ("trace_id", "trace_id"),
    ("span_id", "span_id"),
    ("duration_ms", "duration"),
];
const TRACE_FILTERS: &[(&str, &str)] = &[
    ("operation_id", "operation_id"),
    ("module", "module"),
    ("device_id", "service_device_id"),
    ("trace_id", "trace_id"),
];

pub fn layout(signal: &DiagnosticSignal, logs: &str, traces: &str) -> Option<Layout> {
    match signal {
        DiagnosticSignal::Logs => Some(Layout {
            stream: logs.into(),
            kind: "logs",
            user: "user_id",
            tenant: "tenant_id",
            time: "_timestamp",
            projection: LOG_PROJECTION,
            filters: LOG_FILTERS,
        }),
        DiagnosticSignal::Traces => Some(Layout {
            stream: traces.into(),
            kind: "traces",
            user: "service_user_id",
            tenant: "service_tenant_id",
            time: "_timestamp",
            projection: TRACE_PROJECTION,
            filters: TRACE_FILTERS,
        }),
        // Metric streams are named per metric; they join this search with their own layout.
        DiagnosticSignal::Metrics => None,
    }
}

const FILTER_ORDER: [&str; 6] = [
    "operation_id",
    "event_name",
    "module",
    "device_id",
    "outcome",
    "trace_id",
];

fn filter_value<'a>(filters: &'a DiagnosticFilters, field: &str) -> Option<&'a str> {
    match field {
        "operation_id" => filters.operation_id.as_deref(),
        "event_name" => filters.event_name.as_deref(),
        "module" => filters.module.as_deref(),
        "device_id" => filters.device_id.as_deref(),
        "outcome" => filters.outcome.as_deref(),
        "trace_id" => filters.trace_id.as_deref(),
        _ => None,
    }
}

/// The time window in microseconds, bounded to the retention and to the present.
pub fn window(
    request: &DiagnosticSearchRequest,
    now: DateTime<Utc>,
) -> Result<(i64, i64), SearchError> {
    let parse = |value: &str| {
        DateTime::parse_from_rfc3339(value)
            .map(|time| time.with_timezone(&Utc))
            .map_err(|_| SearchError::Invalid)
    };
    let (start, end) = (parse(&request.start)?, parse(&request.end)?);
    if end <= start || end - start > MAX_WINDOW || end > now + Duration::minutes(1) {
        return Err(SearchError::Invalid);
    }
    Ok((start.timestamp_micros(), end.timestamp_micros()))
}

/// The one SQL statement for this request and scope, or `None` when a condition names a column
/// the stream has never stored, so no record can match. OpenObserve refuses any reference to an
/// unknown column (`SearchFieldNotFound`), so the projection is also limited to stored columns.
pub fn sql(
    layout: &Layout,
    scope: &Scope,
    request: &DiagnosticSearchRequest,
    stored: &BTreeSet<String>,
) -> Result<Option<String>, SearchError> {
    if !(1..=200).contains(&request.limit) || !identifier(&layout.stream) {
        return Err(SearchError::Invalid);
    }
    let mut conditions = Vec::new();
    match scope {
        Scope::Member { user } => conditions.push((layout.user, user.as_str())),
        Scope::Organization { tenant } => conditions.push((layout.tenant, tenant.as_str())),
        Scope::Global => {}
    }
    if let Some(filters) = &request.filters {
        for field in FILTER_ORDER {
            let Some(value) = filter_value(filters, field) else {
                continue;
            };
            if !identifier(value) {
                return Err(SearchError::Invalid);
            }
            let column = layout
                .filters
                .iter()
                .find(|(name, _)| *name == field)
                .map(|(_, column)| *column)
                .ok_or(SearchError::Invalid)?;
            conditions.push((column, value));
        }
    }
    if !stored.contains(layout.time)
        || conditions
            .iter()
            .any(|(column, _)| !stored.contains(*column))
    {
        return Ok(None);
    }
    let columns = std::iter::once(layout.time)
        .chain(
            layout
                .projection
                .iter()
                .map(|(_, column)| *column)
                .filter(|column| stored.contains(*column)),
        )
        .collect::<Vec<_>>()
        .join(", ");
    let filter = if conditions.is_empty() {
        String::new()
    } else {
        let terms = conditions
            .iter()
            .map(|(column, value)| format!("{column} = '{value}'"))
            .collect::<Vec<_>>();
        format!(" WHERE {}", terms.join(" AND "))
    };
    Ok(Some(format!(
        "SELECT {columns} FROM \"{}\"{filter} ORDER BY {} DESC",
        layout.stream, layout.time
    )))
}

fn text(hit: &Map<String, Value>, column: &str) -> Option<String> {
    hit.get(column)
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
        })
        .map(str::to_owned)
}

/// Projects one stored hit onto the record; anything outside the fixed fields is dropped, and
/// a hit without a valid time or service is skipped rather than invented.
pub fn project(
    layout: &Layout,
    signal: &DiagnosticSignal,
    hit: &Map<String, Value>,
) -> Option<DiagnosticRecord> {
    let micros = hit.get(layout.time)?.as_i64()?;
    let timestamp = DateTime::<Utc>::from_timestamp_micros(micros)?
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let column = |field: &str| {
        layout
            .projection
            .iter()
            .find(|(name, _)| *name == field)
            .map(|(_, column)| *column)
    };
    let field = |name: &str| column(name).and_then(|c| text(hit, c));
    let duration_ms = column("duration_ms")
        .and_then(|c| hit.get(c))
        .and_then(Value::as_f64)
        .map(|value| match signal {
            // OpenObserve records span duration in microseconds.
            DiagnosticSignal::Traces => value / 1000.0,
            _ => value,
        })
        .filter(|value| (0.0..=86_400_000.0).contains(value));
    Some(DiagnosticRecord {
        timestamp,
        signal: signal.clone(),
        service_name: field("service_name")?,
        event_name: field("event_name"),
        module: field("module"),
        outcome: field("outcome"),
        error_type: field("error_type"),
        severity: field("severity"),
        operation_id: field("operation_id"),
        device_id: field("device_id"),
        user_id: field("user_id"),
        tenant_id: field("tenant_id"),
        trace_id: field("trace_id"),
        span_id: field("span_id"),
        name: field("name"),
        duration_ms,
        value: None,
    })
}

async fn fetch(
    request: reqwest::RequestBuilder,
    config: &SearchConfig,
) -> Result<Option<Value>, SearchError> {
    let mut response = request
        .basic_auth(&config.user, Some(config.password.expose()))
        .send()
        .await
        .map_err(|_| SearchError::Unavailable)?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(SearchError::Unavailable);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| SearchError::Unavailable)?
    {
        if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(SearchError::Unavailable);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| SearchError::Unavailable)
}

/// Reads one page from OpenObserve with the service's own read-only account: first the stream's
/// stored columns, then one bounded SQL page.
pub async fn search(
    http: &reqwest::Client,
    config: &SearchConfig,
    scope: &Scope,
    request: &DiagnosticSearchRequest,
    now: DateTime<Utc>,
) -> Result<DiagnosticSearchPage, SearchError> {
    let started = std::time::Instant::now();
    let layout =
        layout(&request.signal, &config.logs, &config.traces).ok_or(SearchError::Invalid)?;
    let (start, end) = window(request, now)?;
    let limit = usize::try_from(request.limit).map_err(|_| SearchError::Invalid)?;
    let endpoint = |path: String| {
        let mut url = config
            .url
            .join(&path)
            .map_err(|_| SearchError::Unavailable)?;
        url.query_pairs_mut().append_pair("type", layout.kind);
        Ok::<_, SearchError>(url)
    };
    let schema = fetch(
        http.get(endpoint(format!(
            "api/{}/streams/{}/schema",
            config.organization, layout.stream
        ))?),
        config,
    )
    .await?;
    let stored = schema
        .as_ref()
        .and_then(|document| document.get("schema"))
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(|field| field.get("name").and_then(Value::as_str))
                .map(str::to_owned)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let page = |records, truncated| DiagnosticSearchPage {
        records,
        truncated,
        took_ms: i32::try_from(started.elapsed().as_millis().min(600_000)).unwrap_or(600_000),
    };
    let Some(statement) = sql(&layout, scope, request, &stored)? else {
        return Ok(page(Vec::new(), false));
    };
    let body = serde_json::json!({
        "query": {
            "sql": statement,
            "start_time": start,
            "end_time": end,
            "from": 0,
            "size": i64::from(request.limit) + 1,
        }
    });
    let document = fetch(
        http.post(endpoint(format!("api/{}/_search", config.organization))?)
            .json(&body),
        config,
    )
    .await?
    .ok_or(SearchError::Unavailable)?;
    let hits = document
        .get("hits")
        .and_then(Value::as_array)
        .ok_or(SearchError::Unavailable)?;
    let records = hits
        .iter()
        .take(limit)
        .filter_map(Value::as_object)
        .filter_map(|hit| project(&layout, &request.signal, hit))
        .collect();
    Ok(page(records, hits.len() > limit))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> DiagnosticSearchRequest {
        DiagnosticSearchRequest {
            scope: DiagnosticScope::Member,
            organization_id: None,
            signal: DiagnosticSignal::Logs,
            start: "2026-11-01T00:00:00Z".into(),
            end: "2026-11-01T01:00:00Z".into(),
            filters: Some(DiagnosticFilters {
                operation_id: Some("op-1".into()),
                event_name: Some("module.admission.fullauto".into()),
                module: None,
                device_id: None,
                outcome: None,
                trace_id: None,
            }),
            limit: 50,
        }
    }

    #[test]
    fn the_server_decides_the_scope_and_the_body_can_only_narrow_to_it() {
        let member = request();
        assert_eq!(
            scope(Some("member"), Some("user-1"), &member),
            Ok(Scope::Member {
                user: "user-1".into()
            })
        );
        // A body that asks for more than the server authorized is refused.
        let mut global = request();
        global.scope = DiagnosticScope::Global;
        assert_eq!(
            scope(Some("member"), Some("user-1"), &global),
            Err(SearchError::Forbidden)
        );
        let mut org = request();
        org.scope = DiagnosticScope::Organization;
        org.organization_id = Some("org-2".into());
        assert_eq!(
            scope(Some("organization"), Some("org-1"), &org),
            Err(SearchError::Forbidden)
        );
        org.organization_id = Some("org-1".into());
        assert!(scope(Some("organization"), Some("org-1"), &org).is_ok());
        assert_eq!(scope(None, None, &member), Err(SearchError::Forbidden));
        assert_eq!(
            scope(Some("member"), Some("x' OR '1'='1"), &member),
            Err(SearchError::Forbidden)
        );
        assert_eq!(scope(Some("global"), None, &global), Ok(Scope::Global));
    }

    fn every_column() -> BTreeSet<String> {
        LOG_PROJECTION
            .iter()
            .chain(TRACE_PROJECTION)
            .map(|(_, column)| column.to_string())
            .chain(["_timestamp", "service_tenant_id"].map(String::from))
            .collect()
    }

    #[test]
    fn sql_is_built_from_identifiers_and_fixed_columns_only() {
        let all = every_column();
        let member = Scope::Member {
            user: "user-1".into(),
        };
        let unfiltered = DiagnosticSearchRequest {
            filters: None,
            ..request()
        };
        let logs = layout(&DiagnosticSignal::Logs, "nds", "default").unwrap();
        assert_eq!(
            sql(&logs, &member, &request(), &all).unwrap().unwrap(),
            "SELECT _timestamp, service_name, event_name, module, outcome, error_type, severity, \
             operation_id, device_id, user_id, tenant_id, trace_id, span_id, duration_ms FROM \"nds\" \
             WHERE user_id = 'user-1' AND operation_id = 'op-1' AND event_name = 'module.admission.fullauto' \
             ORDER BY _timestamp DESC"
        );
        let traces = layout(&DiagnosticSignal::Traces, "nds", "default").unwrap();
        let organization = Scope::Organization {
            tenant: "org-1".into(),
        };
        let statement = sql(&traces, &organization, &unfiltered, &all)
            .unwrap()
            .unwrap();
        assert!(
            statement.contains("FROM \"default\" WHERE service_tenant_id = 'org-1'"),
            "{statement}"
        );
        let global = sql(&logs, &Scope::Global, &unfiltered, &all)
            .unwrap()
            .unwrap();
        assert!(!global.contains("WHERE"), "{global}");
        // Anything that is not an identifier never reaches SQL.
        let mut hostile = request();
        hostile.filters.as_mut().unwrap().module = Some("x' OR 1=1 --".into());
        assert_eq!(
            sql(&logs, &Scope::Global, &hostile, &all),
            Err(SearchError::Invalid)
        );
        // A filter the signal does not record is refused, not silently dropped.
        let mut event_on_traces = request();
        event_on_traces.signal = DiagnosticSignal::Traces;
        assert_eq!(
            sql(&traces, &Scope::Global, &event_on_traces, &all),
            Err(SearchError::Invalid)
        );
        for limit in [0, 201] {
            assert_eq!(
                sql(
                    &logs,
                    &Scope::Global,
                    &DiagnosticSearchRequest { limit, ..request() },
                    &all
                ),
                Err(SearchError::Invalid)
            );
        }
    }

    #[test]
    fn only_stored_columns_are_named_and_an_unstored_condition_matches_nothing() {
        let logs = layout(&DiagnosticSignal::Logs, "nds", "default").unwrap();
        let young: BTreeSet<String> = ["_timestamp", "service_name", "user_id", "event_name"]
            .map(String::from)
            .into();
        let member = Scope::Member {
            user: "user-1".into(),
        };
        let unfiltered = DiagnosticSearchRequest {
            filters: None,
            ..request()
        };
        assert_eq!(
            sql(&logs, &member, &unfiltered, &young).unwrap().unwrap(),
            "SELECT _timestamp, service_name, event_name, user_id FROM \"nds\" \
             WHERE user_id = 'user-1' ORDER BY _timestamp DESC"
        );
        // `operation_id` was never stored, so the filtered search has no match to find.
        assert_eq!(sql(&logs, &member, &request(), &young), Ok(None));
        let organization = Scope::Organization {
            tenant: "org-1".into(),
        };
        assert_eq!(sql(&logs, &organization, &unfiltered, &young), Ok(None));
        assert_eq!(
            sql(&logs, &Scope::Global, &unfiltered, &BTreeSet::new()),
            Ok(None)
        );
    }

    #[test]
    fn the_window_stays_inside_the_retention_and_the_present() {
        let now = DateTime::parse_from_rfc3339("2026-11-01T02:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(window(&request(), now).is_ok());
        let mut long = request();
        long.start = "2026-10-24T00:00:00Z".into();
        assert_eq!(window(&long, now), Err(SearchError::Invalid));
        let mut reversed = request();
        reversed.end = reversed.start.clone();
        assert_eq!(window(&reversed, now), Err(SearchError::Invalid));
        let mut future = request();
        future.end = "2026-11-01T03:00:00Z".into();
        assert_eq!(window(&future, now), Err(SearchError::Invalid));
    }

    #[test]
    fn hits_are_projected_onto_fixed_fields_only() {
        let logs = layout(&DiagnosticSignal::Logs, "nds", "default").unwrap();
        let hit = serde_json::json!({
            "_timestamp": 1_793_500_000_000_000_i64,
            "service_name": "nddev-device-sync-agent",
            "event_name": "module.admission.fullauto",
            "user_id": "user-1",
            "message": "never returned",
            "secret_canary": "never returned",
            "outcome": "line\nbreak"
        });
        let record = project(&logs, &DiagnosticSignal::Logs, hit.as_object().unwrap()).unwrap();
        let json = serde_json::to_value(&record).unwrap();
        assert_eq!(json["event_name"], "module.admission.fullauto");
        assert!(json.get("message").is_none() && json.get("secret_canary").is_none());
        assert!(
            json.get("outcome").is_none(),
            "control characters are not projected"
        );
        let traces = layout(&DiagnosticSignal::Traces, "nds", "default").unwrap();
        let span = serde_json::json!({"_timestamp": 1_793_500_000_000_000_i64, "service_name": "agent",
            "operation_name": "local_use", "duration": 2500, "service_user_id": "user-1"});
        let record = project(
            &traces,
            &DiagnosticSignal::Traces,
            span.as_object().unwrap(),
        )
        .unwrap();
        assert_eq!(
            (
                record.name.as_deref(),
                record.duration_ms,
                record.user_id.as_deref()
            ),
            (Some("local_use"), Some(2.5), Some("user-1"))
        );
        assert!(
            project(
                &logs,
                &DiagnosticSignal::Logs,
                serde_json::json!({"service_name": "x"})
                    .as_object()
                    .unwrap()
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn one_page_is_read_with_the_read_account_and_truncation_is_reported() {
        use axum::{Json, Router, http::HeaderMap, routing::post};
        use std::sync::{Arc, Mutex};
        let _ = rustls::crypto::ring::default_provider().install_default();
        let seen = Arc::new(Mutex::new(None::<(Option<String>, Value)>));
        let captured = seen.clone();
        let fake = Router::new()
            .route(
                "/oo/api/default/streams/nds/schema",
                axum::routing::get(|| async {
                    Json(serde_json::json!({"name": "nds", "schema": [
                        {"name": "_timestamp", "type": "Int64"},
                        {"name": "service_name", "type": "Utf8"},
                        {"name": "event_name", "type": "Utf8"},
                        {"name": "user_id", "type": "Utf8"},
                        {"name": "raw", "type": "Utf8"}]}))
                }),
            )
            .route(
                "/oo/api/default/_search",
                post(
                    move |headers: HeaderMap,
                          axum::extract::RawQuery(query): axum::extract::RawQuery,
                          Json(body): Json<Value>| {
                        let captured = captured.clone();
                        async move {
                            let auth = headers
                                .get("authorization")
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_owned);
                            assert_eq!(query.as_deref(), Some("type=logs"));
                            *captured.lock().unwrap() = Some((auth, body));
                            let hit = |n: i64| {
                                serde_json::json!({"_timestamp": 1_793_500_000_000_000_i64 + n,
                            "service_name": "nddev-device-sync-server", "user_id": "user-1",
                            "event_name": "operation.completed", "raw": "dropped"})
                            };
                            Json(serde_json::json!({"hits": [hit(3), hit(2), hit(1)], "total": 3}))
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, fake).await });
        let config = SearchConfig {
            url: reqwest::Url::parse(&format!("http://{address}/oo/")).unwrap(),
            organization: "default".into(),
            logs: "nds".into(),
            traces: "default".into(),
            user: "nds-search@nddev.local".into(),
            password: crate::config::Secret::for_tests("read-only-password"),
        };
        let now = DateTime::parse_from_rfc3339("2026-11-01T02:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let page = search(
            &reqwest::Client::new(),
            &config,
            &Scope::Member {
                user: "user-1".into(),
            },
            &DiagnosticSearchRequest {
                limit: 2,
                filters: None,
                ..request()
            },
            now,
        )
        .await
        .unwrap();
        assert_eq!(page.records.len(), 2);
        assert!(page.truncated);
        let json = serde_json::to_value(&page.records[0]).unwrap();
        assert_eq!(json["timestamp"], "2026-11-01T02:26:40.000Z");
        assert!(json.get("raw").is_none());
        let (auth, body) = seen.lock().unwrap().take().unwrap();
        assert!(auth.unwrap().starts_with("Basic "));
        assert_eq!(body["query"]["size"], 3);
        assert_eq!(body["query"]["start_time"], 1_793_491_200_000_000_i64);
        assert!(
            body["query"]["sql"]
                .as_str()
                .unwrap()
                .contains("WHERE user_id = 'user-1'")
        );
        // An unreachable or failing store is reported as unavailable, never as an empty page.
        let gone = SearchConfig {
            url: reqwest::Url::parse("http://127.0.0.1:9/").unwrap(),
            ..config
        };
        assert_eq!(
            search(
                &reqwest::Client::new(),
                &gone,
                &Scope::Global,
                &DiagnosticSearchRequest {
                    scope: DiagnosticScope::Global,
                    ..request()
                },
                now
            )
            .await
            .err(),
            Some(SearchError::Unavailable)
        );
    }
}
