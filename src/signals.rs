//! Native OTLP signals for actual observability work, never reconstructed server spans.
use crate::{application::Error, config::Config, event_wire::TelemetryEvent, timestamp, wire};
use opentelemetry::{
    Context, KeyValue,
    metrics::{Counter, Histogram, MeterProvider as _},
    trace::{
        Span as _, SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
        Tracer as _, TracerProvider as _,
    },
};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    metrics::{
        PeriodicReader, SdkMeterProvider, Temporality, data::ResourceMetrics,
        exporter::PushMetricExporter,
    },
    trace::{
        BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider, SpanData, SpanExporter,
    },
};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Default, Debug)]
pub struct Delivery {
    outcome: AtomicU8,
    pub successes: AtomicU64,
    pub failures: AtomicU64,
    pub last_success_ms: AtomicU64,
}
fn now_ms() -> u64 {
    timestamp::millis(time::OffsetDateTime::now_utc()).max(0) as u64
}
impl Delivery {
    fn record(&self, success: bool) {
        self.record_at(success, now_ms());
    }
    fn record_at(&self, success: bool, millis: u64) {
        if success {
            self.successes.fetch_add(1, Ordering::Relaxed);
            self.last_success_ms.store(millis, Ordering::Relaxed);
        } else {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        self.outcome
            .store(if success { 1 } else { 2 }, Ordering::Release);
    }
    pub fn snapshot(&self, enabled: bool) -> wire::SignalDelivery {
        let outcome = self.outcome.load(Ordering::Acquire);
        let success = self.last_success_ms.load(Ordering::Relaxed);
        wire::SignalDelivery {
            successes: self
                .successes
                .load(Ordering::Relaxed)
                .min(crate::domain::MAX_COUNTER as u64) as i64,
            failures: self
                .failures
                .load(Ordering::Relaxed)
                .min(crate::domain::MAX_COUNTER as u64) as i64,
            last_success_at: (success > 0).then(|| {
                timestamp::format_utc(timestamp::from_millis(success as i64).expect("current time"))
            }),
            state: if !enabled {
                wire::State::Disabled
            } else if outcome == 2 {
                wire::State::Degraded
            } else if outcome == 1 {
                wire::State::Healthy
            } else {
                wire::State::Unknown
            },
        }
    }
}

struct ObservedSpans {
    inner: opentelemetry_otlp::SpanExporter,
    delivery: Arc<Delivery>,
    pending: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}
impl fmt::Debug for ObservedSpans {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ObservedSpans")
    }
}
impl SpanExporter for ObservedSpans {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let count = batch.len() as u64;
        let mut result = self.inner.export(batch.clone()).await;
        for delay in [50, 200] {
            if result.is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(delay));
            result = self.inner.export(batch.clone()).await;
        }
        self.delivery.record(result.is_ok());
        if result.is_err() {
            self.dropped.fetch_add(count, Ordering::Relaxed);
        }
        self.pending.fetch_sub(count, Ordering::Relaxed);
        result
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
}
struct ObservedMetrics {
    inner: opentelemetry_otlp::MetricExporter,
    delivery: Arc<Delivery>,
}
impl PushMetricExporter for ObservedMetrics {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut result = self.inner.export(metrics).await;
        for delay in [50, 200] {
            if result.is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(delay));
            result = self.inner.export(metrics).await;
        }
        self.delivery.record(result.is_ok());
        result
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

pub struct ProcessingSpan {
    span: opentelemetry_sdk::trace::Span,
    start: Instant,
    duration: Histogram<f64>,
}
impl Drop for ProcessingSpan {
    fn drop(&mut self) {
        self.duration
            .record(self.start.elapsed().as_secs_f64() * 1000.0, &[]);
        self.span.end();
    }
}

pub struct Signals {
    enabled: bool,
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    tracer: opentelemetry_sdk::trace::Tracer,
    pub metrics_delivery: Arc<Delivery>,
    pub traces_delivery: Arc<Delivery>,
    pending: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    accepted: Counter<u64>,
    rejected: Counter<u64>,
    duration: Histogram<f64>,
}

impl Signals {
    /// Construct outside Tokio: the OTLP SDK owns bounded blocking-export threads.
    pub fn new(config: &Config) -> Result<Self, Error> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let metrics_delivery = Arc::new(Delivery::default());
        let traces_delivery = Arc::new(Delivery::default());
        let pending = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let resource = Resource::builder_empty()
            .with_attributes([
                KeyValue::new("service.name", "nddev-device-sync-observability"),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ])
            .build();
        let mut traces = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_sampler(Sampler::AlwaysOn);
        let mut metrics = SdkMeterProvider::builder().with_resource(resource);
        if config.export_enabled {
            let base = config.otlp_url.as_str().trim_end_matches('/');
            let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(format!("{base}/v1/traces"))
                .with_timeout(Duration::from_secs(2))
                .build()
                .map_err(|_| Error::Unavailable)?;
            let exporter = ObservedSpans {
                inner: trace_exporter,
                delivery: traces_delivery.clone(),
                pending: pending.clone(),
                dropped: dropped.clone(),
            };
            let batch = BatchConfigBuilder::default()
                .with_max_queue_size(256)
                .with_max_export_batch_size(64)
                .with_scheduled_delay(Duration::from_millis(200))
                .build();
            traces = traces.with_span_processor(
                BatchSpanProcessor::builder(exporter)
                    .with_batch_config(batch)
                    .build(),
            );
            let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(format!("{base}/v1/metrics"))
                .with_timeout(Duration::from_secs(2))
                .with_temporality(Temporality::Cumulative)
                .build()
                .map_err(|_| Error::Unavailable)?;
            metrics = metrics.with_reader(
                PeriodicReader::builder(ObservedMetrics {
                    inner: metric_exporter,
                    delivery: metrics_delivery.clone(),
                })
                .with_interval(config.signal_interval)
                .build(),
            );
        }
        let tracer_provider = traces.build();
        let tracer = tracer_provider.tracer("nds-observability");
        let meter_provider = metrics.build();
        let meter = meter_provider.meter("nds-observability");
        let dropped_observer = dropped.clone();
        meter
            .u64_observable_counter("nds_observability_spans_dropped")
            .with_callback(move |observer| {
                observer.observe(dropped_observer.load(Ordering::Relaxed), &[])
            })
            .build();
        Ok(Self {
            enabled: config.export_enabled,
            tracer_provider,
            meter_provider,
            tracer,
            metrics_delivery,
            traces_delivery,
            pending,
            dropped,
            accepted: meter
                .u64_counter("nds_observability_events_accepted")
                .build(),
            rejected: meter
                .u64_counter("nds_observability_requests_rejected")
                .build(),
            duration: meter
                .f64_histogram("nds_observability_processing_duration_ms")
                .with_unit("ms")
                .with_boundaries(vec![0.1, 1.0, 10.0, 100.0, 1000.0, 10000.0])
                .build(),
        })
    }
    pub fn accepted(&self, count: usize) {
        self.accepted.add(count as u64, &[]);
    }
    pub fn rejected(&self, reason: &'static str) {
        if matches!(reason, "unauthorized" | "invalid") {
            tracing::warn!(event.name="ingress.rejected",module="observability",scope="http",error.type=reason,outcome="rejected");
        }
        self.rejected.add(1, &[KeyValue::new("reason", reason)]);
    }
    pub fn processing(&self, event: &TelemetryEvent) -> Option<ProcessingSpan> {
        if !self.enabled {
            return None;
        }
        // Reserve through exporter completion. At most128 spans can reach the
        // SDK queue (capacity256), so SDK queue overflow cannot silently lose them.
        if self
            .pending
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                (value < 128).then_some(value + 1)
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            self.traces_delivery.record(false);
            return None;
        }
        let parent = event
            .trace_id
            .as_deref()
            .zip(event.span_id.as_deref())
            .and_then(|(trace, span)| {
                Some((TraceId::from_hex(trace).ok()?, SpanId::from_hex(span).ok()?))
            })
            .map(|(trace, span)| {
                Context::new().with_remote_span_context(SpanContext::new(
                    trace,
                    span,
                    TraceFlags::SAMPLED,
                    true,
                    TraceState::default(),
                ))
            })
            .unwrap_or_default();
        let mut span = self
            .tracer
            .start_with_context("telemetry.normalize", &parent);
        span.set_attribute(KeyValue::new("nds.event.name", event.event_name.clone()));
        Some(ProcessingSpan {
            span,
            start: Instant::now(),
            duration: self.duration.clone(),
        })
    }
    pub fn shutdown(&self) {
        if self
            .tracer_provider
            .shutdown_with_timeout(Duration::from_secs(8))
            .is_err()
        {
            self.traces_delivery.record(false);
        }
        if self
            .meter_provider
            .shutdown_with_timeout(Duration::from_secs(8))
            .is_err()
        {
            self.metrics_delivery.record(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delivery_outcome_is_independent_of_wall_clock_order() {
        let delivery = Delivery::default();
        delivery.record_at(true, 2000);
        delivery.record_at(false, 1000);
        assert!(matches!(
            delivery.snapshot(true).state,
            wire::State::Degraded
        ));
        delivery.record_at(true, 1000);
        assert!(matches!(
            delivery.snapshot(true).state,
            wire::State::Healthy
        ));
        delivery.record_at(false, 1000);
        assert!(matches!(
            delivery.snapshot(true).state,
            wire::State::Degraded
        ));
        assert!(matches!(
            delivery.snapshot(false).state,
            wire::State::Disabled
        ));
    }
}
