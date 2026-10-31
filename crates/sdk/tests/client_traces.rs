#![cfg(feature = "client-traces")]
use nddev_device_sync_telemetry::{
    Config,
    client_traces::{AuthenticatedSource, ClientTraces, authenticated_batch},
    delivery_span, finish_operation_span, operation_span, subscriber,
};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
};
use prost::Message;
use tracing_opentelemetry::OpenTelemetrySpanExt;

fn source() -> AuthenticatedSource<'static> {
    AuthenticatedSource {
        tenant: "tenant-test",
        user: "owner-test",
        device: "device-test",
    }
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
fn config() -> Config {
    Config {
        service: "nddev-device-sync-client".into(),
        version: "0.0.1-alpha.1".into(),
        repository: "NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-desktop".into(),
        commit: "unknown".into(),
        channel: "alpha".into(),
        standards: "unreleased".into(),
        environment: "development".into(),
        target_prefixes: vec!["nds_acceptance".into()],
        default_module: "process".into(),
        debug: None,
    }
}
#[test]
fn actual_native_spans_are_bounded_redacted_and_bound_to_authenticated_source() {
    let config = config();
    let traces = ClientTraces::new(&config).unwrap();
    let feed = traces.subscribe().unwrap();
    let (subscriber, _) = subscriber(config, std::io::sink(), traces.tracer()).unwrap();
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..2 {
            let span = operation_span("vault", "io").unwrap();
            span.set_attribute("authorization", "PRIVATE_VALUE_MUST_NOT_LEAVE");
            span.set_attribute("http.route", "/private?token=PRIVATE_VALUE_MUST_NOT_LEAVE");
            span.set_status(opentelemetry::trace::Status::error(
                "PRIVATE_VALUE_MUST_NOT_LEAVE",
            ));
            span.in_scope(|| {
                std::thread::yield_now();
            });
        }
        delivery_span().in_scope(|| {
            let child = operation_span("accounts", "io").unwrap();
            let dispatch = tracing::dispatcher::get_default(Clone::clone);
            // This is how the native credential adapter preserves context into
            // its OS worker. Delivery instrumentation must stay suppressed there.
            std::thread::spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    child.in_scope(|| {
                        let _nested = operation_span("accounts", "io").unwrap();
                    })
                })
            })
            .join()
            .unwrap();
        });
    });
    let batch = feed.take_batch().unwrap();
    assert_eq!(batch.count(), 2);
    assert!(
        !batch
            .bytes()
            .windows(b"PRIVATE_VALUE".len())
            .any(|part| part == b"PRIVATE_VALUE")
    );
    // Exercise actual exported OTLP, including retained pre-transfer source IDs.
    for (repository, accepted) in [
        (
            "NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-desktop",
            true,
        ),
        (
            "NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-agent",
            true,
        ),
        ("NDDev-OpenNetwork/nddev-device-sync-desktop", true),
        ("NDDev-OpenNetwork/nddev-device-sync-agent", true),
        ("untrusted/nddev-device-sync-desktop", false),
        (
            "NDDev-Technocracy-OpenNetwork-NDS/unrecognized-producer",
            false,
        ),
    ] {
        let mut candidate = ExportTraceServiceRequest::decode(batch.bytes()).unwrap();
        for resource in &mut candidate.resource_spans {
            for field in &mut resource.resource.as_mut().unwrap().attributes {
                if field.key == "source.repository" {
                    field.value.as_mut().unwrap().value =
                        Some(Value::StringValue(repository.into()));
                }
            }
        }
        assert_eq!(
            authenticated_batch(&candidate.encode_to_vec(), source(), now()).is_ok(),
            accepted,
            "{repository}"
        );
    }
    let (normalized, count) = authenticated_batch(batch.bytes(), source(), now()).unwrap();
    assert_eq!(count, 2);
    let decoded = ExportTraceServiceRequest::decode(normalized.as_slice()).unwrap();
    assert_eq!(decoded.resource_spans.len(), 2);
    for resource in &decoded.resource_spans {
        let attrs = &resource.resource.as_ref().unwrap().attributes;
        assert_eq!(attrs.len(), 8);
        assert!(attrs.iter().any(|field| field.key == "device.id"
            && field.value.as_ref().unwrap().value
                == Some(Value::StringValue("device-test".into()))));
        let span = &resource.scope_spans[0].spans[0];
        assert_eq!(span.name, "module.operation");
        assert_eq!(span.trace_id.len(), 16);
        assert_eq!(span.span_id.len(), 8);
        assert!(span.end_time_unix_nano >= span.start_time_unix_nano);
        assert_eq!(span.status.as_ref().unwrap().message, "");
    }
    // A previously authenticated body cannot inject that identity in another
    // request. Only the server may add resource ownership.
    assert!(authenticated_batch(&normalized, source(), now()).is_err());
    let mut forged = ExportTraceServiceRequest::decode(batch.bytes()).unwrap();
    forged.resource_spans[0].scope_spans[0].spans[0]
        .attributes
        .push(KeyValue {
            key: "authorization".into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("PRIVATE".into())),
            }),
            ..Default::default()
        });
    assert!(authenticated_batch(&forged.encode_to_vec(), source(), now()).is_err());
    let mut malformed = ExportTraceServiceRequest::decode(batch.bytes()).unwrap();
    malformed.resource_spans[0].scope_spans[0].spans[0].trace_id = vec![0; 16];
    assert!(authenticated_batch(&malformed.encode_to_vec(), source(), now()).is_err());
    assert!(authenticated_batch(batch.bytes(), source(), now() + 86_401_000_000_000).is_err());
    batch.acknowledge();
    assert_eq!(feed.health().delivered, 2);
    assert_eq!(feed.health().pending, 0);
}

#[test]
fn caller_classified_failure_survives_authenticated_otlp_without_new_fields() {
    let config = config();
    let traces = ClientTraces::new(&config).unwrap();
    let feed = traces.subscribe().unwrap();
    let (subscriber, _) = subscriber(config, std::io::sink(), traces.tracer()).unwrap();
    tracing::subscriber::with_default(subscriber, || {
        let failed = operation_span("remote", "io").unwrap();
        // Failure is explicit even when there was no warning/error event.
        finish_operation_span(&failed, true);
        drop(failed);

        let handled = operation_span("github", "io").unwrap();
        handled.in_scope(|| {
            tracing::event!(target:"nds_acceptance",tracing::Level::WARN, {
                event.name="module.query.checked",outcome="checked"
            });
        });
        // A warning does not determine the operation's terminal result.
        finish_operation_span(&handled, false);
        drop(handled);

        let retained = operation_span("accounts", "io").unwrap();
        finish_operation_span(&retained, true);
        finish_operation_span(&retained, false);
        drop(retained);

        delivery_span().in_scope(|| {
            let local = operation_span("accounts", "io").unwrap();
            finish_operation_span(&local, true);
        });
    });
    let batch = feed.take_batch().unwrap();
    assert_eq!(batch.count(), 3);
    let (normalized, count) = authenticated_batch(batch.bytes(), source(), now()).unwrap();
    assert_eq!(count, 3);
    let decoded = ExportTraceServiceRequest::decode(normalized.as_slice()).unwrap();
    let spans: Vec<_> = decoded
        .resource_spans
        .iter()
        .flat_map(|resource| &resource.scope_spans)
        .flat_map(|scope| &scope.spans)
        .collect();
    let statuses: Vec<_> = spans
        .iter()
        .map(|span| span.status.as_ref().unwrap().code)
        .collect();
    assert_eq!(statuses, [2, 0, 2]);
    assert!(spans.iter().all(|span| {
        span.status.as_ref().unwrap().message.is_empty()
            && span.events.is_empty()
            && span
                .attributes
                .iter()
                .all(|field| matches!(field.key.as_str(), "module" | "scope"))
    }));
}
