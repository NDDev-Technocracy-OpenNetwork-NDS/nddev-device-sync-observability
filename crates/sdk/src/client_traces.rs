//! Real native spans for the authenticated client transport. The SDK owns the
//! projection and bounded queue; accounts owns signing, retries and cancellation.
use crate::{Config, Error, ExportFeed, feed::Router};
use opentelemetry::{
    Context, KeyValue,
    trace::{Link, SpanKind, TraceId, TracerProvider as _},
};
use opentelemetry_proto::{
    tonic::{collector::trace::v1::ExportTraceServiceRequest, trace::v1::ResourceSpans},
    transform::common::tonic::ResourceAttributesWithSchema,
};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{
        Sampler, SamplingDecision, SamplingResult, SdkTracerProvider, ShouldSample, Span, SpanData,
        SpanProcessor, Tracer,
    },
};
use prost::Message;
use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

static RESOURCE_FIELDS: LazyLock<std::collections::BTreeMap<&'static str, jsonschema::Validator>> =
    LazyLock::new(|| {
        [
            "service.name",
            "service.version",
            "source.repository",
            "source.commit",
            "deployment.environment",
        ]
        .into_iter()
        .map(|key| {
            (
                key,
                jsonschema::validator_for(&crate::contract::DOCUMENT["properties"][key])
                    .expect("embedded resource field schema"),
            )
        })
        .collect()
    });

const COLLECTION_EPOCH: &str = "nds.collection.epoch";

#[derive(Clone)]
struct CollectionSampler {
    collection: crate::collection::Collection,
    standard: Sampler,
}
impl fmt::Debug for CollectionSampler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientCollectionSampler")
    }
}
impl ShouldSample for CollectionSampler {
    fn should_sample(
        &self,
        parent: Option<&Context>,
        trace_id: TraceId,
        name: &str,
        kind: &SpanKind,
        attributes: &[KeyValue],
        links: &[Link],
    ) -> SamplingResult {
        let epoch = self.collection.current();
        let sampler = if epoch.is_some() {
            &self.standard
        } else {
            &Sampler::AlwaysOff
        };
        let mut result = sampler.should_sample(parent, trace_id, name, kind, attributes, links);
        if let Some(epoch) = epoch
            && result.decision == SamplingDecision::RecordAndSample
        {
            result
                .attributes
                .push(KeyValue::new(COLLECTION_EPOCH, epoch.to_string()));
        }
        result
    }
}

struct Processor {
    collection: crate::collection::Collection,
    router: Arc<Router>,
    resource: Mutex<ResourceAttributesWithSchema>,
}
impl fmt::Debug for Processor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientTraceProcessor")
    }
}
impl SpanProcessor for Processor {
    fn on_start(&self, _: &mut Span, _: &Context) {}
    fn on_end(&self, mut span: SpanData) {
        let epoch = span
            .attributes
            .iter()
            .find(|field| field.key.as_str() == COLLECTION_EPOCH)
            .and_then(|field| match &field.value {
                opentelemetry::Value::String(value) => value.as_str().parse::<u64>().ok(),
                _ => None,
            });
        let Some(epoch) = epoch.filter(|epoch| self.collection.current() == Some(*epoch)) else {
            return;
        };
        span.attributes
            .retain(|field| field.key.as_str() != COLLECTION_EPOCH);
        if !crate::span::project(&mut span) {
            return;
        }
        let resource = self
            .resource
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans::new(span, &resource)],
        };
        // OTLP's top-level field is repeated ResourceSpans; concatenating its
        // encoded records produces one valid protobuf request without JSON spans.
        self.router.enqueue_span(request.encode_to_vec(), || {
            self.collection.current() == Some(epoch)
        });
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        Ok(())
    }
    fn set_resource(&mut self, resource: &Resource) {
        *self
            .resource
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = resource.into();
    }
}
pub struct ClientTraces {
    pub(crate) collection: crate::collection::Collection,
    provider: SdkTracerProvider,
    router: Arc<Router>,
}
impl ClientTraces {
    pub fn new(config: &Config) -> Result<Self, Error> {
        crate::Policy::new(config)?;
        let router = Arc::new(Router::default());
        router.buffer_startup()?;
        let collection = crate::collection::Collection::client(router.clone());
        let provider = SdkTracerProvider::builder()
            .with_resource(crate::span::resource(config))
            .with_sampler(CollectionSampler {
                collection: collection.clone(),
                standard: Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
            })
            .with_span_processor(Processor {
                collection: collection.clone(),
                router: router.clone(),
                resource: Mutex::default(),
            })
            .with_max_attributes_per_span(16)
            .with_max_events_per_span(0)
            .with_max_links_per_span(0)
            .build();
        Ok(Self {
            collection,
            provider,
            router,
        })
    }
    pub fn tracer(&self) -> Tracer {
        self.provider.tracer("nds-telemetry")
    }
    pub fn subscribe(&self) -> Result<ExportFeed, Error> {
        self.router.subscribe()
    }
    pub fn shutdown(&self) {
        let _ = self.provider.shutdown_with_timeout(Duration::from_secs(1));
    }
}
impl Drop for ClientTraces {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Verified by the calling server's session/device boundary, never by payload.
pub struct AuthenticatedSource<'a> {
    pub tenant: &'a str,
    pub user: &'a str,
    pub device: &'a str,
}
/// Decode and validate the closed native projection before forwarding. Unknown
/// protobuf fields are discarded by the maintained decoder; unknown attributes,
/// arbitrary event/link contents and client-supplied identity are rejected.
/// Returns the normalized binary body and the number of actual spans.
pub fn authenticated_batch(
    bytes: &[u8],
    source: AuthenticatedSource<'_>,
    now_ns: u64,
) -> Result<(Vec<u8>, u32), Error> {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    if bytes.is_empty()
        || bytes.len() > 65_536
        || [source.tenant, source.user, source.device]
            .iter()
            .any(|value| !crate::identifier(value))
    {
        return Err(Error);
    }
    let mut request = ExportTraceServiceRequest::decode(bytes).map_err(|_| Error)?;
    if request.resource_spans.is_empty() || request.resource_spans.len() > 32 {
        return Err(Error);
    }
    let mut count = 0u32;
    for resource_spans in &mut request.resource_spans {
        if !resource_spans.schema_url.is_empty()
            || resource_spans.scope_spans.is_empty()
            || resource_spans.scope_spans.len() > 32
        {
            return Err(Error);
        }
        let resource = resource_spans.resource.as_mut().ok_or(Error)?;
        if resource.attributes.len() != 5
            || resource.dropped_attributes_count != 0
            || !resource.entity_refs.is_empty()
        {
            return Err(Error);
        }
        let mut names = BTreeSet::new();
        for field in &resource.attributes {
            if field.key_strindex != 0 || !names.insert(field.key.as_str()) {
                return Err(Error);
            }
            let Some(Value::StringValue(value)) =
                field.value.as_ref().and_then(|value| value.value.as_ref())
            else {
                return Err(Error);
            };
            let canonical = match field.key.as_str() {
                "deployment.environment.name" => "deployment.environment",
                "service.name" | "service.version" | "source.repository" | "source.commit" => {
                    field.key.as_str()
                }
                _ => return Err(Error),
            };
            if value.len() > 256 || !RESOURCE_FIELDS[canonical].is_valid(&serde_json::json!(value))
            {
                return Err(Error);
            }
            if canonical == "service.name"
                && !matches!(
                    value.as_str(),
                    "nddev-device-sync-client" | "nddev-device-sync-agent"
                )
            {
                return Err(Error);
            }
            if canonical == "source.repository"
                && !matches!(
                    value.as_str(),
                    "NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-desktop"
                        | "NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-agent"
                        // Installed pre-transfer clients and durable batches retain
                        // their original source identity until they are upgraded.
                        | "NDDev-OpenNetwork/nddev-device-sync-desktop"
                        | "NDDev-OpenNetwork/nddev-device-sync-agent"
                )
            {
                return Err(Error);
            }
        }
        for (key, value) in [
            ("tenant.id", source.tenant),
            ("user.id", source.user),
            ("device.id", source.device),
        ] {
            resource.attributes.push(KeyValue {
                key: key.into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(value.into())),
                }),
                ..KeyValue::default()
            });
        }
        for scope_spans in &resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref().ok_or(Error)?;
            if scope.name != "nds-telemetry"
                || !scope.version.is_empty()
                || !scope.attributes.is_empty()
                || scope.dropped_attributes_count != 0
                || !scope_spans.schema_url.is_empty()
                || scope_spans.spans.is_empty()
            {
                return Err(Error);
            }
            for span in &scope_spans.spans {
                count += 1;
                let valid_id =
                    |bytes: &[u8], size| bytes.len() == size && bytes.iter().any(|byte| *byte != 0);
                if count > 32
                    || !valid_id(&span.trace_id, 16)
                    || !valid_id(&span.span_id, 8)
                    || !span.parent_span_id.is_empty() && !valid_id(&span.parent_span_id, 8)
                    || !matches!(span.name.as_str(), "http.request" | "module.operation")
                    || !(1..=5).contains(&span.kind)
                    || !span.trace_state.is_empty()
                    || !span.events.is_empty()
                    || !span.links.is_empty()
                    || span.attributes.len() > 5
                    || span.flags & !0x3ff != 0
                    || span.start_time_unix_nano == 0
                    || span.end_time_unix_nano < span.start_time_unix_nano
                    || span.start_time_unix_nano.saturating_add(86_400_000_000_000) < now_ns
                    || span.end_time_unix_nano > now_ns.saturating_add(300_000_000_000)
                    || span.status.as_ref().is_some_and(|status| {
                        !status.message.is_empty() || !(0..=2).contains(&status.code)
                    })
                {
                    return Err(Error);
                }
                let mut names = BTreeSet::new();
                for field in &span.attributes {
                    if field.key_strindex != 0 || !names.insert(&field.key) {
                        return Err(Error);
                    }
                    let value = match field.value.as_ref().and_then(|value| value.value.as_ref()) {
                        Some(Value::StringValue(value)) if value.len() <= 128 => {
                            serde_json::json!(value)
                        }
                        Some(Value::IntValue(value)) => serde_json::json!(value),
                        _ => return Err(Error),
                    };
                    if !crate::span::valid_attribute(&field.key, &value) {
                        return Err(Error);
                    }
                }
            }
        }
    }
    Ok((request.encode_to_vec(), count))
}
