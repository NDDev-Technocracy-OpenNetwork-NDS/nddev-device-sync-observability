//! Optional native OTLP/HTTP egress for trusted process adapters. No collector
//! account is embedded. Mobile ingress authentication belongs to its transport.
use crate::{Config, Error};
use opentelemetry::{Context, trace::TracerProvider as _};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{
        BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider, Span, SpanData,
        SpanExporter, SpanProcessor, Tracer,
    },
};
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

const CAPACITY: u64 = 128;

#[derive(Default, Debug)]
pub struct Delivery {
    pub successes: AtomicU64,
    pub failures: AtomicU64,
    pub pending: AtomicU64,
    pub dropped: AtomicU64,
    pub last_success_ms: AtomicU64,
    closing: AtomicBool,
}

struct ObservedExporter {
    inner: opentelemetry_otlp::SpanExporter,
    delivery: Arc<Delivery>,
}
impl fmt::Debug for ObservedExporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ObservedExporter")
    }
}
impl SpanExporter for ObservedExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let count = batch.len() as u64;
        // The maintained exporter owns bounded retry/backoff inside its 2s
        // budget. Never layer another independent retry loop on top of it.
        let outcome = self.inner.export(batch).await;
        if outcome.is_ok() {
            self.delivery.successes.fetch_add(1, Ordering::Relaxed);
            self.delivery
                .last_success_ms
                .store(crate::unix_millis_now().max(0) as u64, Ordering::Relaxed);
        } else {
            self.delivery.failures.fetch_add(1, Ordering::Relaxed);
            self.delivery.dropped.fetch_add(count, Ordering::Relaxed);
        }
        self.delivery.pending.fetch_sub(count, Ordering::AcqRel);
        outcome
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
}

#[derive(Debug)]
struct BoundedProcessor {
    inner: BatchSpanProcessor,
    delivery: Arc<Delivery>,
    admission: Mutex<()>,
}
impl SpanProcessor for BoundedProcessor {
    fn on_start(&self, span: &mut Span, context: &Context) {
        self.inner.on_start(span, context);
    }
    fn on_end(&self, mut span: SpanData) {
        if !crate::span::project(&mut span) {
            return;
        }
        let _admission = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        if self.delivery.closing.load(Ordering::Acquire)
            || self
                .delivery
                .pending
                .try_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                    (pending < CAPACITY).then_some(pending + 1)
                })
                .is_err()
        {
            self.delivery.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.inner.on_end(span);
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        let admission = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        self.delivery.closing.store(true, Ordering::Release);
        drop(admission);
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

pub struct NativeTraces {
    provider: SdkTracerProvider,
    pub delivery: Arc<Delivery>,
}
impl NativeTraces {
    /// Call outside an async runtime. HTTP is for an operator-owned private
    /// network/loopback collector; remote clients must use authenticated HTTPS.
    /// No environment-provided endpoint can override this explicit origin.
    pub fn new(config: &Config, origin: &str) -> Result<Self, Error> {
        crate::Policy::new(config)?;
        let endpoint = url::Url::parse(origin).map_err(|_| Error)?;
        if origin.len() > 2048
            || !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.path() != "/"
        {
            return Err(Error);
        }
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|_| Error)?;
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_http_client(client)
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(format!(
                "{}/v1/traces",
                endpoint.as_str().trim_end_matches('/')
            ))
            .with_timeout(Duration::from_secs(2))
            .with_retry_policy(
                opentelemetry_otlp::RetryPolicy::recommended()
                    .with_max_retries(2)
                    .with_initial_delay(Duration::from_millis(50))
                    .with_max_delay(Duration::from_millis(200)),
            )
            .build()
            .map_err(|_| Error)?;
        let delivery = Arc::new(Delivery::default());
        let processor = BoundedProcessor {
            admission: Mutex::new(()),
            inner: BatchSpanProcessor::builder(ObservedExporter {
                inner: exporter,
                delivery: delivery.clone(),
            })
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(256)
                    .with_max_export_batch_size(64)
                    .with_scheduled_delay(Duration::from_millis(200))
                    .build(),
            )
            .build(),
            delivery: delivery.clone(),
        };
        let resource = crate::span::resource(config);
        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
            .with_span_processor(processor)
            .with_max_events_per_span(0)
            .with_max_links_per_span(0)
            .with_max_attributes_per_span(16)
            .build();
        Ok(Self { provider, delivery })
    }
    pub fn tracer(&self) -> Tracer {
        self.provider.tracer("nds-telemetry")
    }
    pub fn flush(&self) -> OTelSdkResult {
        self.provider.force_flush()
    }
    pub fn shutdown(&self) {
        let _ = self.provider.shutdown_with_timeout(Duration::from_secs(3));
    }
}
impl Drop for NativeTraces {
    fn drop(&mut self) {
        self.shutdown();
    }
}
