//! Process-owned, bounded telemetry. No dependency events, bodies or message dumps.
use opentelemetry::{
    Context,
    trace::{TraceContextExt, TracerProvider as _},
};
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer};
use serde_json::{Map, Value};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
use std::{
    fmt,
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_opentelemetry::OpenTelemetrySpanExt;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

#[cfg(feature = "otlp")]
pub mod otlp;
use tracing_subscriber::{
    Layer,
    layer::{Context as LayerContext, SubscriberExt},
    registry::LookupSpan,
};

#[cfg(any(target_arch = "wasm32", test))]
mod browser_queue_policy;
#[cfg(feature = "client-traces")]
pub mod client_traces;
mod feed;
#[cfg(any(feature = "otlp", feature = "client-traces"))]
mod span;
pub use feed::{ExportBatch, ExportFeed, FeedHealth};
mod output;
pub use output::{LocalOutput, LocalWriter, OutputHealth};
mod http;
pub use http::{finish_http_server_span, http_server_span};
mod contract;
pub mod event;
pub mod view;
pub use contract::{MAX_EVENT_BYTES, decode_event};

const MAX_COUNTER: u64 = 9_007_199_254_740_991;

#[derive(Debug, thiserror::Error)]
#[error("invalid telemetry configuration or unavailable subscriber")]
pub struct Error;

/// Immutable public build identity. Never pass operator identities or credentials.
#[derive(Clone)]
pub struct Config {
    pub service: String,
    pub version: String,
    pub repository: String,
    pub commit: String,
    pub channel: String,
    pub standards: String,
    pub environment: String,
    pub target_prefixes: Vec<String>,
    pub default_module: String,
    pub debug: Option<DebugWindow>,
}
#[derive(Clone)]
pub struct DebugWindow {
    pub scope: String,
    pub duration: Duration,
    pub events: u64,
}
impl DebugWindow {
    fn validate(&self) -> Result<(), Error> {
        if valid_scope(&self.scope)
            && (1..=900).contains(&self.duration.as_secs())
            && self.duration.subsec_nanos() == 0
            && (1..=10000).contains(&self.events)
        {
            Ok(())
        } else {
            Err(Error)
        }
    }

    /// One server/service configuration contract; adapters supply environment I/O.
    pub fn from_settings(get: impl Fn(&str) -> Option<String>) -> Result<Option<Self>, Error> {
        match get("NDS_LOG_MODE").as_deref().unwrap_or("normal") {
            "normal"
                if [
                    "NDS_DEBUG_SCOPE",
                    "NDS_DEBUG_SECONDS",
                    "NDS_DEBUG_EVENT_LIMIT",
                ]
                .iter()
                .all(|name| get(name).is_none()) =>
            {
                Ok(None)
            }
            "debug" => {
                let value = Self {
                    scope: get("NDS_DEBUG_SCOPE").ok_or(Error)?,
                    duration: Duration::from_secs(
                        get("NDS_DEBUG_SECONDS")
                            .ok_or(Error)?
                            .parse()
                            .map_err(|_| Error)?,
                    ),
                    events: get("NDS_DEBUG_EVENT_LIMIT")
                        .unwrap_or_else(|| "1000".into())
                        .parse()
                        .map_err(|_| Error)?,
                };
                value.validate()?;
                Ok(Some(value))
            }
            _ => Err(Error),
        }
    }
}

impl view::DebugScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Transport => "transport",
            Self::Database => "database",
            Self::Io => "io",
            Self::Process => "process",
        }
    }
    pub(crate) fn parse(value: &str) -> Option<Self> {
        [
            Self::Http,
            Self::Transport,
            Self::Database,
            Self::Io,
            Self::Process,
        ]
        .into_iter()
        .find(|scope| scope.as_str() == value)
    }
}

impl view::LocalDiagnostics {
    pub fn unavailable() -> Self {
        Self {
            sink_initialized: false,
            debug_enabled: false,
            debug_scope: None,
            rejected_fields_or_events: None,
            dropped_events: None,
            pending_events: None,
            write_failures: None,
            terminating: None,
            shutdown_incomplete: None,
        }
    }
}

struct Policy {
    properties: Map<String, Value>,
    base: Map<String, Value>,
}
impl Policy {
    fn new(config: &Config) -> Result<Self, Error> {
        if config.target_prefixes.is_empty()
            || config.target_prefixes.len() > 16
            || config.target_prefixes.iter().any(|v| !identifier(v))
        {
            return Err(Error);
        }
        if let Some(debug) = &config.debug {
            debug.validate()?;
        }
        let source = &*contract::DOCUMENT;
        let base: Map<_, _> = [
            ("service.name", &config.service),
            ("service.version", &config.version),
            ("release.version", &config.version),
            ("source.repository", &config.repository),
            ("source.commit", &config.commit),
            ("release.channel", &config.channel),
            ("standards.release", &config.standards),
            ("deployment.environment", &config.environment),
            ("module", &config.default_module),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), Value::String(v.clone())))
        .collect();
        let mut check = base.clone();
        check.insert("timestamp".into(), chrono::Utc::now().to_rfc3339().into());
        check.insert("severity".into(), "info".into());
        check.insert("event.name".into(), "logging.initialized".into());
        if !contract::VALIDATOR.is_valid(&Value::Object(check)) {
            return Err(Error);
        }
        Ok(Self {
            properties: source["properties"].as_object().ok_or(Error)?.clone(),
            base,
        })
    }
    fn writable(&self, key: &str) -> bool {
        self.properties.contains_key(key)
            && !self.base.contains_key(key)
            && !matches!(
                key,
                "timestamp"
                    | "severity"
                    | "producer.instance_id"
                    | "producer.sequence"
                    | "trace_id"
                    | "span_id"
            )
            || key == "module"
    }
    fn max_string(&self, key: &str) -> usize {
        self.properties
            .get(key)
            .and_then(|v| v.get("maxLength"))
            .and_then(Value::as_u64)
            .unwrap_or(128) as usize
    }
}
fn valid_scope(scope: &str) -> bool {
    view::DebugScope::parse(scope).is_some()
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}

#[derive(Default, Clone)]
struct Fields {
    export_suppressed: bool,
    values: Map<String, Value>,
    rejected: u64,
}
struct Visitor<'a> {
    fields: &'a mut Fields,
    policy: &'a Policy,
    sdk_http_span: bool,
}
impl Visitor<'_> {
    fn insert(&mut self, field: &Field, value: Value) {
        if self.policy.writable(field.name()) {
            self.fields.values.insert(field.name().into(), value);
        } else {
            self.fields.rejected += 1;
        }
    }
    fn text(&mut self, field: &Field, text: &str) {
        // The SDK's HTTP constructor owns this constant OTel transport field.
        // It is not an event field and grants no other attribute an exemption.
        if self.sdk_http_span && field.name() == "otel.kind" && text == "server" {
            return;
        }
        if !self.policy.writable(field.name()) {
            self.fields.rejected += 1;
            return;
        }
        if text.len() > self.policy.max_string(field.name()) {
            self.fields.rejected += 1;
            return;
        }
        let text = if field.name() == "error.type" {
            contract::error_class(text)
        } else {
            text
        };
        self.insert(field, text.into());
    }
}
struct BoundedText {
    value: String,
    limit: usize,
}
impl fmt::Write for BoundedText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.value.len() + value.len() > self.limit {
            return Err(fmt::Error);
        }
        self.value.push_str(value);
        Ok(())
    }
}
impl Visit for Visitor<'_> {
    fn record_str(&mut self, f: &Field, v: &str) {
        self.text(f, v)
    }
    fn record_bool(&mut self, f: &Field, v: bool) {
        self.insert(f, v.into())
    }
    fn record_i64(&mut self, f: &Field, v: i64) {
        self.insert(f, v.into())
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        self.insert(f, v.into())
    }
    fn record_f64(&mut self, f: &Field, v: f64) {
        self.insert(f, Value::from(v))
    }
    fn record_debug(&mut self, f: &Field, v: &dyn fmt::Debug) {
        // Crucially do not even format unknown fields: Debug may expose secrets
        // or allocate an arbitrarily large body before redaction can run.
        if !self.policy.writable(f.name()) {
            self.fields.rejected += 1;
            return;
        }
        let mut text = BoundedText {
            value: String::new(),
            limit: self.policy.max_string(f.name()),
        };
        if fmt::write(&mut text, format_args!("{v:?}")).is_err() {
            self.fields.rejected += 1;
        } else {
            self.text(f, &text.value);
        }
    }
}

struct Sink<W> {
    writer: W,
    sequence: u64,
    instance: String,
}
struct ActiveDebug {
    window: DebugWindow,
    expires: Instant,
    remaining: u64,
}
struct Shared<W> {
    feed: feed::Router,
    policy: Policy,
    sink: Mutex<Sink<W>>,
    rejected: AtomicU64,
    write_failures: AtomicU64,
    debug: Mutex<Option<ActiveDebug>>,
    config: Config,
}
impl<W: Write> Shared<W> {
    fn owns_target(&self, target: &str) -> bool {
        self.config.target_prefixes.iter().any(|prefix| {
            target == prefix
                || target
                    .strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with("::"))
        })
    }
    fn prepare(&self, mut fields: Map<String, Value>, severity: &str) -> Map<String, Value> {
        for (key, value) in &self.policy.base {
            if key != "module" || !fields.contains_key(key) {
                fields.insert(key.clone(), value.clone());
            }
        }
        fields.insert(
            "timestamp".into(),
            chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                .into(),
        );
        fields.insert("severity".into(), severity.into());
        if !contract::VALIDATOR.is_valid(&Value::Object(fields.clone())) {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            fields.retain(|k, _| {
                self.policy.base.contains_key(k) || matches!(k.as_str(), "timestamp" | "severity")
            });
            fields.insert("event.name".into(), "logging.event.rejected".into());
            fields.insert("module".into(), "process".into());
            fields.insert("severity".into(), "warn".into());
            fields.insert("outcome".into(), "rejected".into());
            fields.insert("rejected_count".into(), 1.into());
        }
        fields
    }
    fn emit(&self, fields: Map<String, Value>, severity: &str) {
        self.emit_export(fields, severity, true);
    }
    fn emit_export(&self, fields: Map<String, Value>, severity: &str, export: bool) {
        let fields = self.prepare(fields, severity);
        let Ok(mut sink) = self.sink.lock() else {
            self.write_failures.fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.write(&mut sink, fields, export);
    }
    fn write(&self, sink: &mut Sink<W>, mut fields: Map<String, Value>, export: bool) {
        if sink.sequence < MAX_COUNTER {
            sink.sequence += 1;
            fields.insert("producer.instance_id".into(), sink.instance.clone().into());
            fields.insert("producer.sequence".into(), sink.sequence.into());
        }
        let Ok(mut bytes) = serde_json::to_vec(&fields) else {
            return;
        };
        if bytes.len() > MAX_EVENT_BYTES {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        bytes.push(b'\n');
        // Delivery operations keep local diagnostics but never feed themselves.
        if export
            && !fields
                .get("route")
                .and_then(Value::as_str)
                .is_some_and(|route| route.starts_with("/v2/observability/"))
            && fields.get("module").and_then(Value::as_str) != Some("telemetry")
        {
            self.feed.enqueue(&fields);
        }
        if sink.writer.write_all(&bytes).is_err() {
            self.write_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn panic_notice(&self) {
        // Never format PanicHookInfo, invoke a prior raw hook, or wait for a
        // logger mutex while unwinding (including a panic inside formatting).
        let Ok(mut sink) = self.sink.try_lock() else {
            self.write_failures.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let fields = self.prepare(
            Map::from_iter([
                ("event.name".into(), "process.failed".into()),
                ("error.type".into(), "rust_panic".into()),
                ("module".into(), "process".into()),
                ("outcome".into(), "error".into()),
            ]),
            "fatal",
        );
        self.write(&mut sink, fields, true);
    }
    fn reject_fields(&self, count: u64, export: bool) {
        if count > 0 {
            self.rejected.fetch_add(count, Ordering::Relaxed);
            self.emit_export(
                Map::from_iter([
                    ("event.name".into(), "logging.fields.rejected".into()),
                    ("module".into(), "process".into()),
                    ("rejected_count".into(), count.min(MAX_COUNTER).into()),
                    ("outcome".into(), "rejected".into()),
                ]),
                "warn",
                export,
            );
        }
    }
    fn debug_notice(&self, debug: &DebugWindow, reason: Option<&str>) {
        let mut fields = Map::from_iter([
            (
                "event.name".into(),
                if reason.is_some() {
                    "logging.debug.disabled"
                } else {
                    "logging.debug.enabled"
                }
                .into(),
            ),
            ("scope".into(), debug.scope.clone().into()),
            ("outcome".into(), "ok".into()),
        ]);
        if let Some(reason) = reason {
            fields.insert("reason".into(), reason.into());
        } else {
            fields.insert("duration_seconds".into(), debug.duration.as_secs().into());
            fields.insert("event_budget".into(), debug.events.into());
        }
        self.emit(fields, "info");
    }
    fn disable_debug(&self, reason: &str) {
        if let Ok(mut active) = self.debug.lock()
            && let Some(debug) = active.take()
        {
            self.debug_notice(&debug.window, Some(reason));
        }
    }
    fn debug_tick(&self) {
        if let Ok(mut active) = self.debug.lock()
            && let Some(debug) = active.as_ref()
        {
            let reason = if Instant::now() >= debug.expires {
                Some("expired")
            } else if debug.remaining == 0 {
                Some("event_budget")
            } else {
                None
            };
            if let Some(reason) = reason {
                let debug = active.take().expect("present window");
                self.debug_notice(&debug.window, Some(reason));
            }
        }
    }
    fn admit_debug(&self, module: &str) -> bool {
        self.debug_tick();
        if let Ok(mut active) = self.debug.lock()
            && let Some(debug) = active.as_mut()
            && debug.window.scope == module
            && debug.remaining > 0
        {
            debug.remaining -= 1;
            return true;
        }
        false
    }
}
struct EventLayer<W> {
    shared: Arc<Shared<W>>,
    dispatch: std::sync::OnceLock<tracing::dispatcher::WeakDispatch>,
}
impl<S, W> Layer<S> for EventLayer<W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: Write + Send + Sync + 'static,
{
    fn on_register_dispatch(&self, dispatch: &tracing::Dispatch) {
        let _ = self.dispatch.set(dispatch.downgrade());
    }
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let parent = attrs.parent().and_then(|id| ctx.span(id)).or_else(|| {
                attrs
                    .is_contextual()
                    .then(|| ctx.lookup_current())
                    .flatten()
            });
            let mut fields = Fields {
                export_suppressed: attrs.metadata().target() == "nds_telemetry"
                    && attrs.metadata().name() == "telemetry.delivery"
                    || parent.is_some_and(|parent| {
                        parent
                            .extensions()
                            .get::<Fields>()
                            .is_some_and(|fields| fields.export_suppressed)
                    }),
                ..Fields::default()
            };
            // Dependency spans are not NDS producers. Keep only suppression
            // ancestry through them; neither their fields nor rejection counts
            // may relabel or multiply events from an owned nested operation.
            if attrs.metadata().target() == "nds_telemetry"
                || self.shared.owns_target(attrs.metadata().target())
            {
                attrs.record(&mut Visitor {
                    fields: &mut fields,
                    policy: &self.shared.policy,
                    sdk_http_span: attrs.metadata().target() == "nds_telemetry"
                        && attrs.metadata().name() == "http.request",
                });
            }
            span.extensions_mut().insert(fields);
        }
    }
    fn on_record(&self, id: &Id, record: &Record<'_>, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(id)
            && (span.metadata().target() == "nds_telemetry"
                || self.shared.owns_target(span.metadata().target()))
            && let Some(fields) = span.extensions_mut().get_mut::<Fields>()
        {
            record.record(&mut Visitor {
                fields,
                policy: &self.shared.policy,
                sdk_http_span: false,
            });
        }
    }
    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        let metadata = event.metadata();
        if !self.shared.owns_target(metadata.target()) {
            return;
        }
        let mut fields = Fields::default();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(value) = span.extensions().get::<Fields>() {
                    fields.export_suppressed |= value.export_suppressed;
                    fields.values.extend(value.values.clone());
                    fields.rejected += value.rejected;
                }
            }
        }
        event.record(&mut Visitor {
            fields: &mut fields,
            policy: &self.shared.policy,
            sdk_http_span: false,
        });
        let scope = fields
            .values
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or("process");
        if *metadata.level() > Level::INFO
            && (*metadata.level() != Level::DEBUG || !self.shared.admit_debug(scope))
        {
            return;
        }
        // Only SDK spans enter the trace exporter. A dependency span may sit
        // between an owned event and its canonical operation/request context.
        if let Some(span) = ctx
            .event_scope(event)
            .and_then(|mut scope| scope.find(|span| span.metadata().target() == "nds_telemetry"))
        {
            let context = self
                .dispatch
                .get()
                .and_then(|weak| weak.upgrade())
                .and_then(|dispatch| {
                    tracing_opentelemetry::get_otel_context(&span.id(), &dispatch)
                });
            if let Some(context) = context {
                let span = context.span();
                let id = span.span_context();
                if id.is_valid() {
                    fields
                        .values
                        .insert("trace_id".into(), id.trace_id().to_string().into());
                    fields
                        .values
                        .insert("span_id".into(), id.span_id().to_string().into());
                }
            }
        }
        self.shared
            .reject_fields(fields.rejected, !fields.export_suppressed);
        self.shared.emit_export(
            fields.values,
            &metadata.level().as_str().to_ascii_lowercase(),
            !fields.export_suppressed,
        );
        self.shared.debug_tick();
    }
}

/// Handle reports local write/rejection failures independently of external delivery.
pub struct Control<W: Write> {
    shared: Arc<Shared<W>>,
    output: Option<Arc<output::State>>,
}
impl<W: Write> Clone for Control<W> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            output: self.output.clone(),
        }
    }
}
impl<W: Write> Control<W> {
    pub fn subscribe(&self) -> Result<ExportFeed, Error> {
        self.shared.feed.subscribe()
    }
    pub fn report_client_lifecycle(&self, state: view::ClientLifecycle) {
        self.shared.emit(
            Map::from_iter([
                ("event.name".into(), state.event().into()),
                ("module".into(), "ui".into()),
                ("scope".into(), "process".into()),
                (
                    "outcome".into(),
                    if state == view::ClientLifecycle::Ready {
                        "ready"
                    } else {
                        "checked"
                    }
                    .into(),
                ),
            ]),
            "info",
        );
    }
    pub fn diagnostics(&self) -> view::LocalDiagnostics {
        let health = self.output_health();
        self.tick();
        let debug_scope = self.shared.debug.lock().ok().and_then(|active| {
            active
                .as_ref()
                .and_then(|window| view::DebugScope::parse(&window.window.scope))
        });
        view::LocalDiagnostics {
            sink_initialized: true,
            debug_enabled: debug_scope.is_some(),
            debug_scope,
            rejected_fields_or_events: Some(self.rejected_fields_or_events()),
            dropped_events: Some(health.dropped_events),
            pending_events: Some(health.pending_events),
            write_failures: Some(health.write_failures),
            terminating: Some(health.terminating),
            shutdown_incomplete: Some(health.shutdown_incomplete),
        }
    }

    /// Presentation supplies a closed class, never an exception or stack.
    /// This records an actual reporting operation; it does not reconstruct a
    /// missing application span or promise collection/delivery.
    pub fn report_failure(&self, kind: view::FailureKind) {
        let span = operation_span("ui", "process").expect("static reporting scope");
        let context = trace_context(&span);
        let context_span = context.span();
        let ids = context_span.span_context();
        let mut fields = Map::from_iter([
            ("event.name".into(), "client.failure".into()),
            ("module".into(), "ui".into()),
            ("scope".into(), "process".into()),
            ("error.type".into(), kind.class().into()),
            ("outcome".into(), "error".into()),
        ]);
        if ids.is_valid() {
            fields.insert("trace_id".into(), ids.trace_id().to_string().into());
            fields.insert("span_id".into(), ids.span_id().to_string().into());
        }
        span.set_status(opentelemetry::trace::Status::error(kind.class()));
        self.shared.emit(fields, "error");
    }
    pub fn rejected_fields_or_events(&self) -> u64 {
        self.shared.rejected.load(Ordering::Relaxed)
    }
    pub fn write_failures(&self) -> u64 {
        self.output_health().write_failures
    }
    pub fn output_health(&self) -> OutputHealth {
        let mut status = self
            .output
            .as_ref()
            .map_or_else(OutputHealth::default, |state| state.snapshot());
        status.write_failures = status
            .write_failures
            .saturating_add(self.shared.write_failures.load(Ordering::Relaxed));
        status
    }
    pub fn tick(&self) {
        self.shared.debug_tick();
    }
    pub fn debug_active(&self) -> bool {
        self.tick();
        self.shared
            .debug
            .lock()
            .is_ok_and(|window| window.is_some())
    }
    /// Replace the diagnostic window atomically; None returns to normal mode.
    pub fn set_debug(&self, window: Option<DebugWindow>) -> Result<(), Error> {
        if self.output_health().terminating {
            return Err(Error);
        }
        if let Some(debug) = &window {
            debug.validate()?;
        }
        let mut active = self.shared.debug.lock().map_err(|_| Error)?;
        if let Some(previous) = active.take() {
            self.shared.debug_notice(&previous.window, Some("operator"));
        }
        if let Some(window) = window {
            self.shared.debug_notice(&window, None);
            *active = Some(ActiveDebug {
                expires: Instant::now() + window.duration,
                remaining: window.events,
                window,
            });
        }
        Ok(())
    }
}
/// Caller-owned writer and provider: this embedded/test API has the writer's own
/// blocking/lifecycle contract. Process entry points use bounded `install`.
pub fn subscriber<W: Write + Send + Sync + 'static>(
    config: Config,
    writer: W,
    tracer: Tracer,
) -> Result<(impl Subscriber + Send + Sync, Control<W>), Error> {
    subscriber_configured(config, writer, tracer, false)
}
fn subscriber_configured<W: Write + Send + Sync + 'static>(
    config: Config,
    writer: W,
    tracer: Tracer,
    buffer_startup: bool,
) -> Result<(impl Subscriber + Send + Sync, Control<W>), Error> {
    let policy = Policy::new(&config)?;
    let feed = feed::Router::default();
    if buffer_startup {
        feed.buffer_startup()?;
    }
    let shared = Arc::new(Shared {
        feed,
        policy,
        sink: Mutex::new(Sink {
            writer,
            sequence: 0,
            instance: uuid::Uuid::new_v4().to_string(),
        }),
        rejected: AtomicU64::new(0),
        write_failures: AtomicU64::new(0),
        debug: Mutex::new(config.debug.clone().map(|window| ActiveDebug {
            expires: Instant::now() + window.duration,
            remaining: window.events,
            window,
        })),
        config,
    });
    let otel = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.is_span() && metadata.target() == "nds_telemetry"
        }));
    let subscriber = tracing_subscriber::registry().with(otel).with(EventLayer {
        dispatch: std::sync::OnceLock::new(),
        shared: shared.clone(),
    });
    if let Some(debug) = &shared.config.debug {
        shared.emit(
            Map::from_iter([
                ("event.name".into(), "logging.debug.enabled".into()),
                ("scope".into(), debug.scope.clone().into()),
                ("duration_seconds".into(), debug.duration.as_secs().into()),
                ("event_budget".into(), debug.events.into()),
                ("outcome".into(), "ok".into()),
            ]),
            "info",
        );
    }
    Ok((
        subscriber,
        Control {
            shared,
            output: None,
        },
    ))
}

pub struct Logger {
    pub control: Control<LocalWriter>,
    worker: Option<output::Worker>,
    provider: Option<SdkTracerProvider>,
    #[cfg(not(target_arch = "wasm32"))]
    expiry: Option<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
    #[cfg(target_arch = "wasm32")]
    expiry: Option<futures::future::AbortHandle>,
}
impl Drop for Logger {
    fn drop(&mut self) {
        self.shutdown();
    }
}
impl Logger {
    /// Terminal and idempotent. Platforms call this only on an actual controlled
    /// exit, never on mobile background/detach notifications.
    pub fn shutdown(&mut self) {
        if self.worker.is_none() {
            return;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((cancel, thread)) = self.expiry.take() {
            let _ = cancel.send(());
            let _ = thread.join();
        }
        #[cfg(target_arch = "wasm32")]
        if let Some(cancel) = self.expiry.take() {
            cancel.abort();
        }
        self.control.shared.disable_debug("process_exit");
        self.control.shared.emit(
            Map::from_iter([
                ("event.name".into(), "process.stopped".into()),
                ("outcome".into(), "stopped".into()),
            ]),
            "info",
        );
        if let Some(provider) = self.provider.take() {
            let _ = provider.shutdown_with_timeout(Duration::from_secs(2));
        }
        if let Some(mut worker) = self.worker.take() {
            worker.close();
        }
    }
}
/// Local structured output and real spans; no OTLP exporter is configured here.
pub fn install(config: Config) -> Result<Logger, Error> {
    let provider = SdkTracerProvider::builder().build();
    let tracer = provider.tracer("nds-telemetry");
    let mut logger = install_with_tracer(config, tracer)?;
    logger.provider = Some(provider);
    Ok(logger)
}
pub fn install_with_tracer(config: Config, tracer: Tracer) -> Result<Logger, Error> {
    install_with_tracer_to(config, tracer, LocalOutput::Stdout)
}
/// Same bounded native sink and lifecycle, with an explicit CLI output stream.
pub fn install_with_tracer_to(
    config: Config,
    tracer: Tracer,
    output: LocalOutput,
) -> Result<Logger, Error> {
    install_configured(config, tracer, output, false)
}
/// A bounded startup feed awaits the first authenticated client subscription.
/// Pre-authentication capture is RAM-only until a verified scope claims it.
/// Replacing/closing that subscription never rebinds its records to a new owner.
pub fn install_client_with_tracer_to(
    config: Config,
    tracer: Tracer,
    output: LocalOutput,
) -> Result<Logger, Error> {
    install_configured(config, tracer, output, true)
}
fn install_configured(
    config: Config,
    tracer: Tracer,
    output: LocalOutput,
    buffer_startup: bool,
) -> Result<Logger, Error> {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.set(()).map_err(|_| Error)?;
    // Privacy does not depend on a working OS sink. If initialization fails,
    // retain a nonblocking hook that cannot print a raw panic payload/stack.
    // The structured hook replaces it once the bounded sink is available.
    std::panic::set_hook(Box::new(|_| {}));
    let (writer, worker) = output::start(output).map_err(|_| Error)?;
    let output = writer.state.clone();
    let (subscriber, mut control) = subscriber_configured(config, writer, tracer, buffer_startup)?;
    control.output = Some(output.clone());
    tracing::subscriber::set_global_default(subscriber).map_err(|_| Error)?;
    let panic_control = control.clone();
    std::panic::set_hook(Box::new(move |_| panic_control.shared.panic_notice()));
    control.shared.emit(
        Map::from_iter([
            ("event.name".into(), "process.started".into()),
            ("outcome".into(), "started".into()),
        ]),
        "info",
    );
    let shared = control.shared.clone();
    #[cfg(not(target_arch = "wasm32"))]
    let expiry = {
        let (cancel, receiver) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let mut heartbeat = Instant::now();
            while matches!(
                receiver.recv_timeout(Duration::from_secs(1)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ) {
                lifecycle_tick(&shared, &output, &mut heartbeat);
            }
        });
        Some((cancel, thread))
    };
    #[cfg(target_arch = "wasm32")]
    let expiry = {
        let (cancel, registration) = futures::future::AbortHandle::new_pair();
        wasm_bindgen_futures::spawn_local(async move {
            let work = async move {
                let mut heartbeat = Instant::now();
                loop {
                    gloo_timers::future::TimeoutFuture::new(1000).await;
                    lifecycle_tick(&shared, &output, &mut heartbeat);
                }
            };
            let _ = futures::future::Abortable::new(work, registration).await;
        });
        Some(cancel)
    };
    Ok(Logger {
        control,
        provider: None,
        worker: Some(worker),
        expiry,
    })
}

fn lifecycle_tick(shared: &Shared<LocalWriter>, output: &output::State, heartbeat: &mut Instant) {
    shared.debug_tick();
    if heartbeat.elapsed() >= Duration::from_secs(30) {
        shared.emit(
            Map::from_iter([
                ("event.name".into(), "process.heartbeat".into()),
                ("module".into(), "process".into()),
                (
                    "outcome".into(),
                    if output.snapshot().dropped_events > 0 {
                        "error"
                    } else {
                        "ok"
                    }
                    .into(),
                ),
                (
                    "dropped_count".into(),
                    output.snapshot().dropped_events.into(),
                ),
            ]),
            if output.snapshot().dropped_events > 0 {
                "warn"
            } else {
                "info"
            },
        );
        *heartbeat = Instant::now();
    }
}

/// A real operation span with a validated, bounded module label. Instrument the
/// actual future with this span; do not create spans from previously emitted logs.
pub fn operation_span(module: &str, scope: &str) -> Result<tracing::Span, Error> {
    if !identifier(module) || !valid_scope(scope) {
        return Err(Error);
    }
    let suppressed = current_export_suppressed();
    let span = tracing::info_span!(target:"nds_telemetry","module.operation",module,scope);
    if suppressed {
        span.set_attribute("nds.export.suppressed", true);
    }
    Ok(span)
}
/// Finish an operation using the caller's classification of its terminal result.
/// The core/caller decides whether cancellation, conflict or partial completion
/// is a failure. This helper never classifies events or accepts error text.
/// A non-failure leaves status unchanged; it neither claims success nor clears
/// an error already recorded on the span.
pub fn finish_operation_span(span: &tracing::Span, terminal_failure: bool) {
    if terminal_failure {
        span.set_status(opentelemetry::trace::Status::error(""));
    }
}
/// Instrument the entire delivery operation, including native credential reads.
/// Descendant events stay local; descendant native spans must not export either.
/// The marker follows tracing parents across async and blocking workers.
pub fn delivery_span() -> tracing::Span {
    let span = tracing::info_span!(target:"nds_telemetry","telemetry.delivery");
    span.set_attribute("nds.export.suppressed", true);
    span
}
fn current_export_suppressed() -> bool {
    tracing::Span::current()
        .with_subscriber(|(id, dispatch)| {
            dispatch
                .downcast_ref::<tracing_subscriber::Registry>()
                .and_then(|registry| registry.span(id))
                .is_some_and(|span| {
                    span.extensions()
                        .get::<Fields>()
                        .is_some_and(|fields| fields.export_suppressed)
                })
        })
        .unwrap_or(false)
}
/// Used by transport owners with a W3C TextMapPropagator; excludes all secrets.
pub fn trace_context(span: &tracing::Span) -> Context {
    span.context()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{SpanContext, SpanKind, Status, TraceFlags};
    use opentelemetry_sdk::{
        error::OTelSdkResult,
        trace::{Span as NativeSpan, SpanData, SpanProcessor},
    };
    use std::io;
    #[derive(Clone, Debug, Default)]
    struct CompletedSpans(Arc<Mutex<Vec<SpanData>>>);
    impl SpanProcessor for CompletedSpans {
        fn on_start(&self, _: &mut NativeSpan, _: &Context) {}
        fn on_end(&self, span: SpanData) {
            let mut spans = self.0.lock().unwrap();
            assert!(spans.len() < 16);
            spans.push(span);
        }
        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }
        fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
            Ok(())
        }
    }
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl Write for Capture {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn private_fields_and_delivery_feedback_stay_out_of_bounded_export_feed() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) = subscriber(
            config(),
            Capture(output.clone()),
            provider.tracer("export-feed"),
        )
        .unwrap();
        let feed = control.subscribe().unwrap();
        assert!(control.subscribe().is_err());
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                event.name = "client.acceptance",
                authorization = "PRIVATE_AUTHORIZATION_MUST_NEVER_EXPORT",
                outcome = "ok"
            );
            tracing::info!(
                event.name = "identity.request_completed",
                route = "/v2/observability/events",
                outcome = "ok"
            );
            delivery_span().in_scope(|| {
                operation_span("accounts", "io").unwrap().in_scope(|| {
                    assert!(current_export_suppressed());
                    tracing::info!(event.name = "identity.credential.read", outcome = "ok");
                });
            });
        });
        for _ in 0..300 {
            control.report_client_lifecycle(view::ClientLifecycle::Ready);
        }
        assert!(feed.health().pending <= 128 && feed.health().dropped > 0);
        let mut events = 0;
        while let Some(batch) = feed.take_batch() {
            let count = batch.count();
            assert!(batch.bytes().len() <= 65_536 && count <= 32);
            let text = std::str::from_utf8(batch.bytes()).unwrap();
            assert!(
                !text.contains("PRIVATE_AUTHORIZATION")
                    && !text.contains("/v2/observability/")
                    && !text.contains("identity.credential.read")
            );
            for line in text.lines() {
                decode_event(serde_json::from_str(line).unwrap()).unwrap();
            }
            batch.acknowledge();
            events += count;
        }
        assert!(
            String::from_utf8(output.lock().unwrap().clone())
                .unwrap()
                .contains("identity.credential.read")
        );
        assert_eq!(feed.health().delivered, u64::from(events));
        drop(feed);
        assert!(control.subscribe().is_ok());
    }

    #[test]
    fn delivery_rejections_stay_local_through_async_completion_and_cancellation() {
        use std::{future::Future, task::Poll};
        use tracing::Instrument;

        struct Completion;
        impl Drop for Completion {
            fn drop(&mut self) {
                tracing::warn!(
                    event.name = "delivery.worker.stopped",
                    private_detail = "PRIVATE_DELIVERY_DETAIL",
                    outcome = "stopped"
                );
            }
        }
        let output = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) = subscriber(
            config(),
            Capture(output.clone()),
            provider.tracer("delivery-rejection"),
        )
        .unwrap();
        let feed = control.subscribe().unwrap();
        tracing::subscriber::with_default(subscriber, || {
            for cancel in [false, true] {
                let delivery = delivery_span();
                let child = delivery.in_scope(|| operation_span("accounts", "io").unwrap());
                let future = async {
                    let _completion = Completion;
                    let mut suspended = false;
                    std::future::poll_fn(move |_| {
                        if suspended {
                            Poll::Ready(())
                        } else {
                            suspended = true;
                            Poll::Pending
                        }
                    })
                    .await;
                    tracing::warn!(
                        event.name = "delivery.worker.completed",
                        private_detail = "PRIVATE_DELIVERY_DETAIL",
                        outcome = "ok"
                    );
                }
                .instrument(child);
                // The waiting parent can end before its detached I/O worker.
                drop(delivery);
                let dispatch = tracing::dispatcher::get_default(Clone::clone);
                std::thread::spawn(move || {
                    tracing::dispatcher::with_default(&dispatch, || {
                        let mut future = Box::pin(future);
                        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                        assert!(future.as_mut().poll(&mut context).is_pending());
                        if !cancel {
                            assert!(future.as_mut().poll(&mut context).is_ready());
                        }
                        drop(future);
                    });
                })
                .join()
                .unwrap();
            }
            assert_eq!(feed.health().pending, 0);
            assert_eq!(control.rejected_fields_or_events(), 3);
            // Rejection outside delivery still reports to the export feed.
            tracing::warn!(
                event.name = "module.query.completed",
                private_detail = "PRIVATE_DELIVERY_DETAIL",
                outcome = "ok"
            );
        });
        let batch = feed.take_batch().unwrap();
        assert_eq!(batch.count(), 2);
        let exported = std::str::from_utf8(batch.bytes()).unwrap();
        assert!(exported.contains("logging.fields.rejected"));
        assert!(exported.contains("module.query.completed"));
        assert!(!exported.contains("delivery.worker") && !exported.contains("PRIVATE_DELIVERY"));
        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert_eq!(output.matches("logging.fields.rejected").count(), 4);
        assert_eq!(output.matches("delivery.worker.stopped").count(), 2);
        assert!(
            output.contains("delivery.worker.completed") && !output.contains("PRIVATE_DELIVERY")
        );
        assert_eq!(control.rejected_fields_or_events(), 4);
    }

    #[test]
    fn foreign_spans_cannot_inject_fields_or_rejections_into_owned_events() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        let feed = control.subscribe().unwrap();
        tracing::subscriber::with_default(subscriber, || {
            let owner = operation_span("accounts", "io").unwrap();
            owner.in_scope(|| {
                let foreign = tracing::info_span!(target: "foreign_transport", "connect",
                    module = "remote", op = "PRIVATE_TRANSPORT_DETAIL");
                foreign.in_scope(|| {
                    tracing::info!(event.name = "identity.credential.read", outcome = "ok");
                    foreign.record("module", "vault");
                    tracing::info!(event.name = "identity.offline.fence", outcome = "ok");
                });
            });
            assert_eq!(feed.health().pending, 2);
            // The SDK's delivery suppression still crosses foreign ancestors.
            delivery_span().in_scope(|| {
                tracing::info_span!(target: "foreign_transport", "delivery", op = "PRIVATE_TRANSPORT_DETAIL")
                    .in_scope(|| operation_span("accounts", "io").unwrap().in_scope(|| {
                        tracing::info!(event.name = "identity.credential.read", outcome = "ok");
                    }));
            });
            assert_eq!(feed.health().pending, 2);
        });
        let values = records(&bytes);
        assert_eq!(values.len(), 3);
        for value in values {
            assert_eq!(value["module"], "accounts");
            assert_eq!(value["scope"], "io");
            assert_eq!(value["trace_id"].as_str().unwrap().len(), 32);
            assert_eq!(value["span_id"].as_str().unwrap().len(), 16);
        }
        assert_eq!(control.rejected_fields_or_events(), 0);
        assert!(
            !String::from_utf8(bytes.lock().unwrap().clone())
                .unwrap()
                .contains("PRIVATE_TRANSPORT")
        );
    }

    fn config() -> Config {
        Config {
            service: "generated-sdk-acceptance".into(),
            version: "0.0.1-alpha.1".into(),
            repository: "NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-observability".into(),
            commit: "unknown".into(),
            channel: "alpha".into(),
            standards: "v0.0.1-alpha.9".into(),
            environment: "test".into(),
            target_prefixes: vec!["nddev_device_sync_telemetry".into()],
            default_module: "process".into(),
            debug: None,
        }
    }
    fn records(bytes: &Arc<Mutex<Vec<u8>>>) -> Vec<Value> {
        let schema = contract::DOCUMENT.clone();
        let validator = jsonschema::options()
            .should_validate_formats(true)
            .build(&schema)
            .unwrap();
        let result: Vec<Value> = bytes
            .lock()
            .unwrap()
            .split(|b| *b == b'\n')
            .filter(|b| !b.is_empty())
            .map(|line| {
                assert!(line.len() <= MAX_EVENT_BYTES);
                serde_json::from_slice(line).unwrap()
            })
            .collect();
        for value in &result {
            assert!(validator.is_valid(value), "schema rejected emitted event");
        }
        result
    }

    #[test]
    fn producers_and_ingress_share_validation_and_redaction() {
        let provider = SdkTracerProvider::builder().build();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let (subscriber, control) = subscriber(
            config(),
            Capture(bytes.clone()),
            provider.tracer("contract"),
        )
        .unwrap();
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(event.name="operation.failed",error.type="private:class",outcome="error");
            control.report_failure(view::FailureKind::FlutterFramework);
            control.report_failure(view::FailureKind::ReactRender);
            control.report_failure(view::FailureKind::BrowserUnhandled);
        });
        let emitted = records(&bytes);
        assert_eq!(emitted[0]["error.type"], "redacted_error");
        assert_eq!(emitted[2]["error.type"], "react_render_error");
        assert_eq!(emitted[3]["error.type"], "browser_unhandled_error");
        assert_eq!(emitted.len(), 4);
        for value in &emitted {
            let decoded = decode_event(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), *value);
        }
        let mut external = emitted[0].clone();
        external["error.type"] = "private:class".into();
        assert_eq!(
            decode_event(external.clone())
                .unwrap()
                .error_type
                .as_deref(),
            Some("redacted_error")
        );
        external["authorization"] = "synthetic-secret".into();
        assert!(decode_event(external).is_err());
        let mut oversized = emitted[0].clone();
        oversized["event.name"] = "x".repeat(MAX_EVENT_BYTES + 1).into();
        assert!(decode_event(oversized).is_err());
        let mut invalid_time = emitted[0].clone();
        invalid_time["timestamp"] = "not-a-date".into();
        assert!(decode_event(invalid_time).is_err());
    }

    #[test]
    fn debug_settings_have_one_explicit_bounded_contract() {
        assert!(DebugWindow::from_settings(|_| None).unwrap().is_none());
        assert!(
            DebugWindow::from_settings(|name| (name == "NDS_DEBUG_SCOPE").then(|| "io".into()))
                .is_err()
        );
        for (name, value) in [
            ("NDS_DEBUG_SCOPE", "all"),
            ("NDS_DEBUG_SECONDS", "0"),
            ("NDS_DEBUG_SECONDS", "901"),
            ("NDS_DEBUG_EVENT_LIMIT", "10001"),
            ("NDS_LOG_MODE", "verbose"),
        ] {
            assert!(
                DebugWindow::from_settings(|key| {
                    Some(
                        if key == name {
                            value
                        } else {
                            match key {
                                "NDS_LOG_MODE" => "debug",
                                "NDS_DEBUG_SCOPE" => "io",
                                "NDS_DEBUG_SECONDS" => "30",
                                "NDS_DEBUG_EVENT_LIMIT" => "10",
                                _ => return None,
                            }
                        }
                        .into(),
                    )
                })
                .is_err()
            );
        }
        let valid = DebugWindow::from_settings(|key| match key {
            "NDS_LOG_MODE" => Some("debug".into()),
            "NDS_DEBUG_SCOPE" => Some("io".into()),
            "NDS_DEBUG_SECONDS" => Some("30".into()),
            _ => None,
        })
        .unwrap()
        .unwrap();
        assert_eq!(valid.events, 1000);
        assert_eq!(valid.duration, Duration::from_secs(30));
    }
    #[test]
    fn unknown_debug_is_not_formatted_and_schema_violations_are_observable() {
        struct MustNotFormat;
        impl fmt::Debug for MustNotFormat {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("unknown secret-bearing field was formatted")
            }
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(event.name="operation.completed",outcome="ok",body=?MustNotFormat);
            tracing::info!(event.name = "invalid.bound", status = 600);
            tracing::info!(event.name="operation.failed",error.type="private free prose must not leave the process",outcome="error");
        });
        let values = records(&bytes);
        assert_eq!(values.len(), 4);
        assert_eq!(values[0]["event.name"], "logging.fields.rejected");
        assert_eq!(values[1]["event.name"], "operation.completed");
        assert_eq!(values[2]["event.name"], "logging.event.rejected");
        assert_eq!(values[3]["error.type"], "redacted_error");
        assert_eq!(control.rejected_fields_or_events(), 2);
    }
    #[test]
    fn actual_nested_sdk_spans_correlate_events_without_export_claim() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, _control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            let parent = operation_span("agent", "io").unwrap();
            let _parent = parent.enter();
            tracing::info!(event.name = "operation.started", outcome = "started");
            let child = operation_span("sysinfo", "io").unwrap();
            assert!(trace_context(&child).span().span_context().is_valid());
            let _child = child.enter();
            tracing::info!(event.name = "operation.completed", outcome = "ok");
        });
        let values = records(&bytes);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0]["trace_id"], values[1]["trace_id"]);
        assert_ne!(values[0]["span_id"], values[1]["span_id"]);
        assert_eq!(values[1]["module"], "sysinfo");
    }
    #[test]
    fn http_spans_preserve_native_parent_kind_status_and_closed_events() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let completed = CompletedSpans::default();
        let provider = SdkTracerProvider::builder()
            .with_span_processor(completed.clone())
            .build();
        let (subscriber, control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            let parent = operation_span("accounts", "http").unwrap();
            let id = trace_context(&parent).span().span_context().clone();
            let context = SpanContext::new(
                id.trace_id(),
                id.span_id(),
                id.trace_flags(),
                true,
                id.trace_state().clone(),
            );
            drop(parent);
            // Only an immutable parent identity survives; request duration is
            // not extended by a live parent span in a later operation.
            assert_eq!(completed.0.lock().unwrap().len(), 1);
            for status in [400, 500] {
                let request = http_server_span("POST", "/v2/devices/{device_id}").unwrap();
                request
                    .set_parent(Context::new().with_remote_span_context(context.clone()))
                    .unwrap();
                request.in_scope(|| {
                    finish_http_server_span(&request, status).unwrap();
                    tracing::error!(
                        event.name = "http.request.completed",
                        error.type = "request_rejected",
                        outcome = "error"
                    );
                });
            }
        });
        provider.force_flush().unwrap();
        let spans = completed.0.lock().unwrap();
        assert_eq!(spans.len(), 3);
        let parent = &spans[0];
        for (span, status) in spans[1..].iter().zip([400, 500]) {
            assert_eq!(span.span_kind, SpanKind::Server);
            assert_eq!(span.parent_span_id, parent.span_context.span_id());
            assert_eq!(span.span_context.trace_id(), parent.span_context.trace_id());
            assert!(span.parent_span_is_remote);
            assert_eq!(
                span.status,
                if status == 500 {
                    Status::error("")
                } else {
                    Status::Unset
                }
            );
            for (key, value) in [
                ("http.request.method", opentelemetry::Value::from("POST")),
                (
                    "http.route",
                    opentelemetry::Value::from("/v2/devices/{device_id}"),
                ),
                (
                    "http.response.status_code",
                    opentelemetry::Value::from(status),
                ),
            ] {
                assert!(
                    span.attributes
                        .iter()
                        .any(|v| v.key.as_str() == key && v.value == value)
                );
            }
        }
        let values = records(&bytes);
        assert_eq!(values.len(), 2);
        for (value, span) in values.iter().zip(&spans[1..]) {
            assert_eq!(value["trace_id"], span.span_context.trace_id().to_string());
            assert_eq!(value["span_id"], span.span_context.span_id().to_string());
            assert_eq!(value["module"], "http");
            assert_eq!(value["scope"], "http");
            assert_eq!(value["method"], "POST");
            assert_eq!(value["route"], "/v2/devices/{device_id}");
        }
        assert_eq!(values[0]["status"], 400);
        assert_eq!(values[1]["status"], 500);
        assert_eq!(control.rejected_fields_or_events(), 0);
    }
    #[test]
    fn unsampled_http_parent_never_suppresses_local_security_or_error_events() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let completed = CompletedSpans::default();
        let provider = SdkTracerProvider::builder()
            .with_span_processor(completed.clone())
            .build();
        let (subscriber, control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            let parent = operation_span("accounts", "http").unwrap();
            let id = trace_context(&parent).span().span_context().clone();
            drop(parent);
            let unsampled = SpanContext::new(
                id.trace_id(),
                id.span_id(),
                TraceFlags::NOT_SAMPLED,
                true,
                id.trace_state().clone(),
            );
            let request = http_server_span("OTHER", "unmatched").unwrap();
            request
                .set_parent(Context::new().with_remote_span_context(unsampled))
                .unwrap();
            assert!(!trace_context(&request).span().span_context().is_sampled());
            request.in_scope(|| {
                tracing::warn!(event.name = "security.request.rejected", outcome = "rejected");
                finish_http_server_span(&request, 503).unwrap();
                tracing::error!(event.name = "http.request.completed", error.type = "server_busy", outcome = "error");
            });
        });
        provider.force_flush().unwrap();
        assert_eq!(completed.0.lock().unwrap().len(), 1);
        let values = records(&bytes);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0]["event.name"], "security.request.rejected");
        assert_eq!(values[1]["status"], 503);
        assert_eq!(values[0]["trace_id"], values[1]["trace_id"]);
        assert_eq!(values[0]["span_id"], values[1]["span_id"]);
        assert_eq!(values[0]["method"], "OTHER");
        assert_eq!(values[0]["route"], "unmatched");
        assert_eq!(control.rejected_fields_or_events(), 0);
    }
    #[test]
    fn http_boundaries_and_transport_metadata_do_not_bypass_event_projection() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            for route in [
                "",
                "/v2/auth?token=generated",
                "/v2/\n",
                "https://example.invalid/",
            ] {
                assert!(http_server_span("POST", route).is_err());
            }
            assert!(http_server_span("GET", &format!("/{}", "a".repeat(256))).is_err());
            assert!(http_server_span("get", "/source").is_err());
            let request = http_server_span("GET", "/source").unwrap();
            assert!(finish_http_server_span(&request, 99).is_err());
            assert!(finish_http_server_span(&request, 600).is_err());
            request.in_scope(|| {
                tracing::warn!(
                    event.name = "security.rejected",
                    outcome = "rejected",
                    otel.kind = "server",
                    otel.status_description = "generated forbidden prose"
                );
            });
        });
        let values = records(&bytes);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0]["event.name"], "logging.fields.rejected");
        assert_eq!(values[0]["rejected_count"], 2);
        assert_eq!(values[1]["event.name"], "security.rejected");
        assert!(
            !values[1]
                .as_object()
                .unwrap()
                .keys()
                .any(|key| key.starts_with("otel."))
        );
        assert_eq!(control.rejected_fields_or_events(), 2);
    }
    #[test]
    fn concurrent_writes_are_complete_ordered_and_debug_budget_is_finite() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let mut config = config();
        config.debug = Some(DebugWindow {
            scope: "io".into(),
            duration: Duration::from_secs(30),
            events: 2,
        });
        let (subscriber, control) =
            subscriber(config, Capture(bytes.clone()), provider.tracer("test")).unwrap();
        let dispatch = tracing::Dispatch::new(subscriber);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        for _ in 0..8 {
                            tracing::info!(event.name = "parallel.completed", outcome = "ok");
                            tracing::debug!(
                                event.name = "diagnostic.completed",
                                module = "sysinfo",
                                scope = "io",
                                outcome = "ok"
                            );
                        }
                    })
                });
            }
        });
        let values = records(&bytes);
        assert_eq!(
            values
                .iter()
                .filter(|v| v["event.name"] == "parallel.completed")
                .count(),
            64
        );
        assert_eq!(
            values
                .iter()
                .filter(|v| v["event.name"] == "diagnostic.completed")
                .count(),
            2
        );
        assert_eq!(
            values
                .iter()
                .filter(|v| v["event.name"] == "logging.debug.disabled")
                .count(),
            1
        );
        for (index, value) in values.iter().enumerate() {
            assert_eq!(value["producer.sequence"], (index + 1) as u64);
        }
        assert_eq!(control.write_failures(), 0);
    }
    #[test]
    fn runtime_debug_toggle_uses_one_window_and_shared_budget() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            assert!(!control.debug_active());
            control
                .set_debug(Some(DebugWindow {
                    scope: "http".into(),
                    duration: Duration::from_secs(10),
                    events: 1,
                }))
                .unwrap();
            assert!(control.debug_active());
            tracing::debug!(event.name = "wrong.scope", module = "process");
            tracing::debug!(
                event.name = "request.completed",
                module = "accounts",
                scope = "http",
                outcome = "ok"
            );
            assert!(!control.debug_active());
            control
                .set_debug(Some(DebugWindow {
                    scope: "http".into(),
                    duration: Duration::from_secs(10),
                    events: 5,
                }))
                .unwrap();
            control.set_debug(None).unwrap();
            tracing::debug!(
                event.name = "after.disable",
                module = "accounts",
                scope = "http"
            );
        });
        let values = records(&bytes);
        assert_eq!(values.len(), 5);
        assert_eq!(values[2]["reason"], "event_budget");
        assert_eq!(values[4]["reason"], "operator");
    }
    #[test]
    fn io_failure_and_invalid_configuration_are_reported() {
        struct Fails;
        impl Write for Fails {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("generated I/O failure"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) = subscriber(config(), Fails, provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(event.name = "process.failed", outcome = "error")
        });
        assert_eq!(control.write_failures(), 1);
        assert!(operation_span("bad module", "io").is_err());
        let mut invalid = config();
        invalid.debug = Some(DebugWindow {
            scope: "all".into(),
            duration: Duration::from_secs(30),
            events: 1,
        });
        assert!(Policy::new(&invalid).is_err());
    }
}

/// Durable browser transport buffer; callers supply an authenticated scope.
#[cfg(target_arch = "wasm32")]
pub mod browser_queue;
