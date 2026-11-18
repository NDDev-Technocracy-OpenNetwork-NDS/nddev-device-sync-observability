use crate::timestamp;
use crate::{
    application::{AcceptedEvent, Error},
    event_wire::{Resource, TelemetryEvent},
};
use nddev_device_sync_telemetry::decode_event;
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};

pub fn accept(
    value: serde_json::Value,
    now: OffsetDateTime,
) -> Result<(TelemetryEvent, AcceptedEvent), Error> {
    let event = decode_event(value).map_err(|_| Error::Invalid)?;
    let time = timestamp::parse(&event.timestamp).map_err(|_| Error::Invalid)?;
    if time > now + Duration::seconds(60) || time < now - Duration::hours(24) {
        return Err(Error::Invalid);
    }
    let payload = serde_json::to_vec(&event).map_err(|_| Error::Invalid)?;
    let result = AcceptedEvent {
        fingerprint: Sha256::digest(&payload).into(),
        payload,
        service: event.service_name.clone(),
        event_name: event.event_name.clone(),
        resource: event.resource.as_ref().map(|r| {
            match r {
                Resource::Connections => "connections",
                Resource::Requests => "requests",
            }
            .to_owned()
        }),
        route: event.route.clone(),
        status: event.status,
        timestamp_ms: timestamp::millis(time),
        device: event.device_id.clone(),
        instance: event.producer_instance_id.clone(),
        sequence: event.producer_sequence,
    };
    Ok((event, result))
}
