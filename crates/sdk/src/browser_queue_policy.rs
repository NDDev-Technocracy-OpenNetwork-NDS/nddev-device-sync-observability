//! Durable-spool policy. Browser adapters apply this plan in the transaction
//! that supplied the snapshot; payloads remain opaque outside the SDK boundary.
use crate::{Error, FeedHealth};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const MAX_RECORDS: u32 = 4096;
pub(crate) const MAX_SCOPES: usize = 16;
pub(crate) const MAX_COUNTER: u64 = crate::MAX_COUNTER;
pub(crate) const MAX_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAX_BATCH_BYTES: usize = 65536;
pub(crate) const MAX_ATTEMPTS: u8 = 8;
pub(crate) const SENDER_MS: u32 = 35000;
pub(crate) const IO_MS: u32 = 5000;
#[cfg(target_arch = "wasm32")]
pub(crate) const MAX_OPERATIONS: u8 = 4;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Signal {
    Events,
    Traces,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Health {
    pub events: FeedHealth,
    pub traces: FeedHealth,
    pub enabled: bool,
    pub last_success_at: u64,
    pub revision: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub id: String,
    pub signal: Signal,
    pub count: u32,
    pub expires_at_ms: u64,
    pub attempts: u8,
}
impl Metadata {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if uuid::Uuid::parse_str(&self.id).is_err()
            || !(1..=32).contains(&self.count)
            || self.expires_at_ms > MAX_COUNTER
            || self.attempts > MAX_ATTEMPTS
        {
            return Err(Error);
        }
        Ok(())
    }
}
pub(crate) fn valid_scope(scope: &str) -> bool {
    scope.len() == 64
        && scope
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
#[derive(Clone)]
pub(crate) enum Command {
    Append(Metadata),
    Health,
    Next(String),
    Enabled(bool),
    Finish {
        sender: String,
        batch: String,
        disposition: Disposition,
    },
    Losses {
        events: FeedHealth,
        traces: FeedHealth,
    },
}
#[derive(Clone, Copy)]
pub(crate) enum Disposition {
    Delivered,
    Count { original: u32, accepted: i64 },
    Dropped,
    Failed,
}
#[derive(Serialize)]
pub(crate) struct Reads<'a> {
    pub scope: &'a str,
    pub lookup: Option<&'a str>,
    pub scan: bool,
    pub ordered: bool,
    pub next_limit: u32,
    pub sender: Option<&'a str>,
    pub row_limit: u32,
    pub health_limit: usize,
    pub counter_limit: u64,
    pub timeout_ms: u32,
}
impl Command {
    pub(crate) fn reads<'a>(&'a self, scope: &'a str, bytes: usize) -> Result<Reads<'a>, Error> {
        if !valid_scope(scope) || bytes > MAX_BATCH_BYTES {
            return Err(Error);
        }
        let mut reads = Reads {
            scope,
            lookup: None,
            scan: false,
            ordered: false,
            next_limit: 32,
            sender: None,
            row_limit: MAX_RECORDS + 1,
            health_limit: MAX_SCOPES + 2,
            counter_limit: MAX_COUNTER,
            timeout_ms: IO_MS,
        };
        match self {
            Self::Append(metadata) => {
                metadata.validate()?;
                if metadata.attempts != 0 || bytes == 0 {
                    return Err(Error);
                }
                reads.lookup = Some(&metadata.id);
            }
            Self::Next(sender) => {
                reads.sender = Some(sender);
                reads.scan = true;
                reads.ordered = true;
            }
            Self::Enabled(false) => reads.scan = true,
            Self::Finish { sender, batch, .. } => {
                if uuid::Uuid::parse_str(batch).is_err() {
                    return Err(Error);
                }
                reads.lookup = Some(batch);
                reads.sender = Some(sender);
            }
            Self::Losses { events, traces } => {
                validate_health(events)?;
                validate_health(traces)?;
            }
            _ => {}
        }
        if let Self::Finish {
            disposition: Disposition::Count { original, accepted },
            ..
        } = self
        {
            crate::feed::settlement(*original, *accepted)?;
        }
        if !matches!(self, Self::Append(_)) && bytes != 0 {
            return Err(Error);
        }
        if reads
            .sender
            .is_some_and(|id| uuid::Uuid::parse_str(id).is_err())
        {
            return Err(Error);
        }
        Ok(reads)
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Record {
    #[serde(flatten)]
    pub metadata: Metadata,
    pub scope: String,
    pub size: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inflight_until: Option<u64>,
    #[serde(default, skip_serializing)]
    pub byte_count: Option<u32>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Scope {
    key: String,
    events: FeedHealth,
    traces: FeedHealth,
    enabled: bool,
    last_success_at: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Global {
    key: String,
    bytes: u64,
    records: u32,
    batches: u32,
    revision: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Snapshot {
    pub now: u64,
    pub health: Vec<serde_json::Value>,
    pub record_count: u32,
    pub expired: Vec<Record>,
    pub records: Vec<Record>,
    pub lookup: Option<Record>,
    pub held_scope: Option<String>,
}
#[derive(Serialize)]
pub(crate) struct Plan {
    pub remove: Vec<String>,
    pub combine: Vec<String>,
    pub put: Vec<Record>,
    pub append: Option<Record>,
    health: Vec<Scope>,
    pub global: Option<Global>,
    pub result: Health,
    pub next: Option<Metadata>,
}
fn validate_health(value: &FeedHealth) -> Result<(), Error> {
    if value.dropped > MAX_COUNTER || value.delivered > MAX_COUNTER || value.failures > MAX_COUNTER
    {
        Err(Error)
    } else {
        Ok(())
    }
}
fn increment(value: &mut u64, amount: u64) {
    *value = value.saturating_add(amount).min(MAX_COUNTER);
}
fn signal(scope: &mut Scope, kind: Signal) -> &mut FeedHealth {
    match kind {
        Signal::Events => &mut scope.events,
        Signal::Traces => &mut scope.traces,
    }
}
fn remove(
    record: &Record,
    disposition: Disposition,
    scopes: &mut BTreeMap<String, Scope>,
    global: &mut Global,
    changed: &mut BTreeSet<String>,
) -> Result<(), Error> {
    record.metadata.validate()?;
    if !(1..=MAX_BATCH_BYTES as u32).contains(&record.size) {
        return Err(Error);
    }
    let owner = scopes.get_mut(&record.scope).ok_or(Error)?;
    let health = signal(owner, record.metadata.signal);
    let count = record.metadata.count;
    if health.pending < count
        || global.records < count
        || global.batches < 1
        || global.bytes < u64::from(record.size)
    {
        return Err(Error);
    }
    let partial = match disposition {
        Disposition::Count { original, accepted } => {
            if original != count {
                return Err(Error);
            }
            Some(crate::feed::settlement(original, accepted)?)
        }
        Disposition::Delivered => Some(crate::feed::settlement(count, i64::from(count))?),
        _ => None,
    };
    health.pending -= count;
    match disposition {
        Disposition::Delivered => increment(&mut health.delivered, u64::from(count)),
        Disposition::Dropped => increment(&mut health.dropped, u64::from(count)),
        Disposition::Count { .. } => {
            let (delivered, dropped) = partial.ok_or(Error)?;
            increment(&mut health.delivered, u64::from(delivered));
            increment(&mut health.dropped, u64::from(dropped));
        }
        Disposition::Failed => return Err(Error),
    }
    global.records -= count;
    global.batches -= 1;
    global.bytes -= u64::from(record.size);
    changed.insert(record.scope.clone());
    Ok(())
}

// Same ordering as scope_expiry: expiry, then the object store's primary ID.
fn order(left: &Record, right: &Record) -> std::cmp::Ordering {
    (left.metadata.expires_at_ms, &left.metadata.id)
        .cmp(&(right.metadata.expires_at_ms, &right.metadata.id))
}

pub(crate) fn decide(
    scope: &str,
    command: &Command,
    bytes: usize,
    snapshot: Snapshot,
) -> Result<Plan, Error> {
    let reads = command.reads(scope, bytes)?;
    if snapshot.now > MAX_COUNTER
        || snapshot.health.len() > MAX_SCOPES + 1
        || snapshot.expired.len() > MAX_RECORDS as usize
        || snapshot.records.len() > MAX_RECORDS as usize
        || (reads.ordered && snapshot.records.len() > reads.next_limit as usize)
    {
        return Err(Error);
    }
    if reads.sender.is_some() && snapshot.held_scope.as_deref() != Some(scope) {
        return Err(Error);
    }
    let mut global = None;
    let mut scopes = BTreeMap::new();
    for value in snapshot.health {
        let key = value
            .get("key")
            .and_then(|v| v.as_str())
            .ok_or(Error)?
            .to_owned();
        if key == "global" {
            if global.is_some() {
                return Err(Error);
            }
            global = Some(serde_json::from_value::<Global>(value).map_err(|_| Error)?);
        } else {
            if !valid_scope(&key) {
                return Err(Error);
            }
            let row: Scope = serde_json::from_value(value).map_err(|_| Error)?;
            validate_health(&row.events)?;
            validate_health(&row.traces)?;
            if row.last_success_at > MAX_COUNTER || scopes.insert(key, row).is_some() {
                return Err(Error);
            }
        }
    }
    let mut global_changed = global.is_none();
    let mut global = match global {
        Some(value) => value,
        None if scopes.is_empty() && snapshot.record_count == 0 => Global {
            key: "global".into(),
            bytes: 0,
            records: 0,
            batches: 0,
            revision: 0,
        },
        None => return Err(Error),
    };
    if global.bytes > MAX_COUNTER || global.revision > MAX_COUNTER {
        return Err(Error);
    }
    let mut changed = BTreeSet::new();
    if !scopes.contains_key(scope) {
        if scopes.len() >= MAX_SCOPES {
            return Err(Error);
        }
        scopes.insert(
            scope.into(),
            Scope {
                key: scope.into(),
                events: FeedHealth::default(),
                traces: FeedHealth::default(),
                enabled: true,
                last_success_at: 0,
            },
        );
        changed.insert(scope.into());
    }
    let mut deleted = BTreeSet::new();
    let mut put = Vec::new();
    let mut append = None;
    let mut next = None;
    let mut combine = Vec::new();
    let mut protected: Option<Record> = None;
    for record in snapshot.expired {
        if record
            .inflight_until
            .is_none_or(|until| until <= snapshot.now)
        {
            remove(
                &record,
                Disposition::Dropped,
                &mut scopes,
                &mut global,
                &mut changed,
            )?;
            deleted.insert(record.metadata.id);
            global_changed = true;
        } else if matches!(command, Command::Next(_))
            && record.scope == scope
            && protected
                .as_ref()
                .is_none_or(|previous| order(&record, previous).is_lt())
        {
            protected = Some(record);
        }
    }
    match command {
        Command::Append(metadata) => {
            if snapshot.lookup.is_some() && !deleted.contains(&metadata.id) {
                return Err(Error);
            }
            let current = scopes.get_mut(scope).ok_or(Error)?;
            if !current.enabled
                || metadata.expires_at_ms <= snapshot.now
                || global.records.saturating_add(metadata.count) > MAX_RECORDS
                || global.bytes.saturating_add(bytes as u64) > MAX_BYTES
            {
                increment(
                    &mut signal(current, metadata.signal).dropped,
                    u64::from(metadata.count),
                );
            } else {
                signal(current, metadata.signal).pending = signal(current, metadata.signal)
                    .pending
                    .checked_add(metadata.count)
                    .ok_or(Error)?;
                global.batches = global.batches.checked_add(1).ok_or(Error)?;
                global.records += metadata.count;
                global.bytes += bytes as u64;
                global_changed = true;
                append = Some(Record {
                    metadata: metadata.clone(),
                    scope: scope.into(),
                    size: bytes as u32,
                    inflight_until: None,
                    byte_count: None,
                });
            }
            changed.insert(scope.into());
        }
        Command::Losses { events, traces } => {
            let current = scopes.get_mut(scope).ok_or(Error)?;
            for (signal, loss) in [(&mut current.events, events), (&mut current.traces, traces)] {
                increment(&mut signal.dropped, loss.dropped);
                increment(&mut signal.failures, loss.failures);
            }
            changed.insert(scope.into());
        }
        Command::Enabled(enabled) => {
            let current = scopes.get_mut(scope).ok_or(Error)?;
            if current.enabled != *enabled {
                current.enabled = *enabled;
                changed.insert(scope.into());
            }
            if !enabled {
                for record in snapshot.records {
                    if !deleted.contains(&record.metadata.id) {
                        remove(
                            &record,
                            Disposition::Dropped,
                            &mut scopes,
                            &mut global,
                            &mut changed,
                        )?;
                        deleted.insert(record.metadata.id);
                        global_changed = true;
                    }
                }
            }
        }
        Command::Next(_) => {
            let mut candidates: Vec<_> = protected
                .into_iter()
                .chain(snapshot.records)
                .filter(|record| !deleted.contains(&record.metadata.id))
                .collect();
            candidates.sort_by(order);
            let mut candidates = candidates.into_iter();
            if scopes.get(scope).ok_or(Error)?.enabled
                && let Some(mut record) = candidates.next()
            {
                record.metadata.validate()?;
                if record.scope != scope
                    || record.byte_count != Some(record.size)
                    || !(1..=MAX_BATCH_BYTES as u32).contains(&record.size)
                {
                    return Err(Error);
                }
                // Combine only before the first claim. Once a sender could have
                // submitted bytes, every retry must keep that exact payload.
                // Like the RAM feed, a batch keeps its oldest member's expiry.
                if record.inflight_until.is_none() && record.metadata.attempts == 0 {
                    for other in
                        candidates.filter(|other| other.metadata.signal == record.metadata.signal)
                    {
                        other.metadata.validate()?;
                        if other.scope != scope
                            || other.metadata.id == record.metadata.id
                            || other.byte_count != Some(other.size)
                            || !(1..=MAX_BATCH_BYTES as u32).contains(&other.size)
                        {
                            return Err(Error);
                        }
                        if other.inflight_until.is_some()
                            || other.metadata.attempts != 0
                            || record.metadata.count + other.metadata.count > 32
                            || record.size + other.size > MAX_BATCH_BYTES as u32
                        {
                            break;
                        }
                        if combine.is_empty() {
                            combine.push(record.metadata.id.clone());
                        }
                        if !deleted.insert(other.metadata.id.clone()) {
                            return Err(Error);
                        }
                        combine.push(other.metadata.id);
                        record.metadata.count += other.metadata.count;
                        record.size += other.size;
                        global.batches = global.batches.checked_sub(1).ok_or(Error)?;
                        if global.batches == 0 {
                            return Err(Error);
                        }
                        global_changed = true;
                    }
                }
                record.inflight_until = Some(snapshot.now + u64::from(SENDER_MS));
                next = Some(record.metadata.clone());
                put.push(record);
            }
        }
        Command::Finish { disposition, .. } => {
            if let Some(mut record) = snapshot
                .lookup
                .filter(|record| !deleted.contains(&record.metadata.id))
            {
                if record.scope != scope {
                    return Err(Error);
                }
                if matches!(disposition, Disposition::Failed) {
                    increment(
                        &mut signal(scopes.get_mut(scope).ok_or(Error)?, record.metadata.signal)
                            .failures,
                        1,
                    );
                    record.metadata.attempts =
                        record.metadata.attempts.saturating_add(1).min(MAX_ATTEMPTS);
                    record.inflight_until = Some(0);
                    put.push(record);
                    changed.insert(scope.into());
                } else {
                    remove(
                        &record,
                        *disposition,
                        &mut scopes,
                        &mut global,
                        &mut changed,
                    )?;
                    if matches!(disposition, Disposition::Delivered)
                        || matches!(disposition, Disposition::Count { original, accepted } if i64::from(*original) == *accepted)
                    {
                        scopes.get_mut(scope).ok_or(Error)?.last_success_at = snapshot.now / 1000;
                    }
                    deleted.insert(record.metadata.id);
                    global_changed = true;
                }
            }
        }
        Command::Health => {}
    }
    if global_changed || !changed.is_empty() {
        if global.revision == MAX_COUNTER {
            return Err(Error);
        }
        global.revision += 1;
        global_changed = true;
    }
    let current = scopes.get(scope).ok_or(Error)?;
    let result = Health {
        events: current.events,
        traces: current.traces,
        enabled: current.enabled,
        last_success_at: current.last_success_at,
        revision: global.revision,
    };
    let health = changed
        .into_iter()
        .map(|key| scopes.remove(&key).ok_or(Error))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Plan {
        remove: deleted.into_iter().collect(),
        combine,
        put,
        append,
        health,
        global: global_changed.then_some(global),
        result,
        next,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scope() -> String {
        "a".repeat(64)
    }
    fn snapshot(pending: u32) -> Snapshot {
        let scope = scope();
        serde_json::from_value(serde_json::json!({"now":1000,"record_count":u32::from(pending>0),"health":[
            {"key":"global","bytes":if pending>0{4}else{0},"records":pending,"batches":u32::from(pending>0),"revision":1},
            {"key":scope,"events":{"pending":pending,"dropped":0,"delivered":0,"failures":0},"traces":{"pending":0,"dropped":0,"delivered":0,"failures":0},"enabled":true,"last_success_at":0}],
            "expired":[],"records":[],"lookup":null,"held_scope":scope})).unwrap()
    }
    fn record() -> Record {
        Record {
            metadata: Metadata {
                id: "00000000-0000-4000-8000-000000000001".into(),
                signal: Signal::Events,
                count: 2,
                expires_at_ms: 5000,
                attempts: 0,
            },
            scope: scope(),
            size: 4,
            inflight_until: None,
            byte_count: Some(4),
        }
    }
    fn sender() -> String {
        "00000000-0000-4000-8000-000000000002".into()
    }
    fn pending(id: u64, signal: Signal, count: u32, size: u32) -> Record {
        let mut item = record();
        item.metadata.id = format!("00000000-0000-4000-8000-{id:012x}");
        item.metadata.expires_at_ms += id;
        item.metadata.signal = signal;
        item.metadata.count = count;
        item.size = size;
        item.byte_count = Some(size);
        item
    }
    fn with_records(records: Vec<Record>) -> Snapshot {
        let mut state = snapshot(0);
        state.record_count = records.len() as u32;
        state.health[0]["records"] = records.iter().map(|r| r.metadata.count).sum::<u32>().into();
        state.health[0]["bytes"] = records
            .iter()
            .map(|r| u64::from(r.size))
            .sum::<u64>()
            .into();
        state.health[0]["batches"] = state.record_count.into();
        for (name, kind) in [("events", Signal::Events), ("traces", Signal::Traces)] {
            state.health[1][name]["pending"] = records
                .iter()
                .filter(|r| r.metadata.signal == kind)
                .map(|r| r.metadata.count)
                .sum::<u32>()
                .into();
        }
        state.records = records;
        state
    }
    #[test]
    fn pending_batches_coalesce_per_signal_without_changing_health_or_extending_expiry() {
        let first = pending(1, Signal::Events, 2, 4);
        let trace = pending(2, Signal::Traces, 3, 6);
        let last = pending(3, Signal::Events, 4, 8);
        let done = decide(
            &scope(),
            &Command::Next(sender()),
            0,
            with_records(vec![last.clone(), trace, first.clone()]),
        )
        .unwrap();
        assert_eq!(
            done.combine,
            vec![first.metadata.id.clone(), last.metadata.id.clone()]
        );
        assert_eq!(done.remove, vec![last.metadata.id]);
        let selected = done.next.unwrap();
        assert_eq!(
            (selected.count, selected.expires_at_ms),
            (6, first.metadata.expires_at_ms)
        );
        assert_eq!(done.put[0].size, 12);
        assert_eq!(
            (done.result.events.pending, done.result.traces.pending),
            (6, 3)
        );
        assert_eq!(done.result.events.delivered + done.result.events.dropped, 0);
        let global = done.global.unwrap();
        assert_eq!((global.records, global.bytes, global.batches), (9, 18, 2));
    }
    #[test]
    fn coalescing_obeys_transport_bounds_and_stops_at_an_existing_claim() {
        for (count, size, claimed) in [(31, 4, false), (2, 65533, false), (2, 4, true)] {
            let first = pending(1, Signal::Events, count, size);
            let mut second = pending(2, Signal::Events, 2, 4);
            if claimed {
                second.inflight_until = Some(0);
            }
            let later = pending(3, Signal::Events, 1, 1);
            let done = decide(
                &scope(),
                &Command::Next(sender()),
                0,
                with_records(vec![first.clone(), second, later]),
            )
            .unwrap();
            assert!(done.combine.is_empty() && done.remove.is_empty());
            assert_eq!(done.next.unwrap().count, first.metadata.count);
            assert_eq!(done.put[0].size, first.size);
        }
        let first = pending(1, Signal::Traces, 30, 65532);
        let second = pending(2, Signal::Traces, 2, 4);
        let done = decide(
            &scope(),
            &Command::Next(sender()),
            0,
            with_records(vec![first, second]),
        )
        .unwrap();
        assert_eq!(done.next.unwrap().count, 32);
        assert_eq!(done.put[0].size, 65536);
    }
    #[test]
    fn a_claimed_combination_keeps_its_original_receipt_and_cannot_absorb_new_records() {
        let selected = decide(
            &scope(),
            &Command::Next(sender()),
            0,
            with_records(vec![
                pending(1, Signal::Events, 2, 4),
                pending(2, Signal::Events, 2, 4),
            ]),
        )
        .unwrap();
        let mut batch = selected.put[0].clone();
        // The adapter physically joined bytes in the same committed transaction.
        batch.byte_count = Some(batch.size);
        for (attempts, inflight) in [(0, batch.inflight_until), (1, Some(0))] {
            batch.metadata.attempts = attempts;
            batch.inflight_until = inflight;
            let replay = decide(
                &scope(),
                &Command::Next(sender()),
                0,
                with_records(vec![batch.clone(), pending(3, Signal::Events, 1, 1)]),
            )
            .unwrap();
            assert!(replay.combine.is_empty() && replay.remove.is_empty());
            let next = replay.next.unwrap();
            assert_eq!(
                (next.id, next.count, next.expires_at_ms),
                (batch.metadata.id.clone(), 4, batch.metadata.expires_at_ms)
            );
            assert_eq!(replay.put[0].size, 8);
        }
        let mut state = with_records(vec![batch.clone()]);
        state.lookup = Some(batch.clone());
        let done = decide(
            &scope(),
            &Command::Finish {
                sender: sender(),
                batch: batch.metadata.id,
                disposition: Disposition::Count {
                    original: 4,
                    accepted: 3,
                },
            },
            0,
            state,
        )
        .unwrap();
        assert_eq!(
            (
                done.result.events.pending,
                done.result.events.delivered,
                done.result.events.dropped
            ),
            (0, 3, 1)
        );
        let global = done.global.unwrap();
        assert_eq!((global.records, global.bytes, global.batches), (0, 0, 0));
    }
    #[test]
    fn coalescing_rejects_corrupt_payload_sizes_and_duplicate_ids() {
        for duplicate in [false, true] {
            let first = pending(1, Signal::Events, 1, 4);
            let mut other = pending(2, Signal::Events, 1, 4);
            if duplicate {
                other.metadata.id = first.metadata.id.clone();
            } else {
                other.byte_count = Some(3);
            }
            assert!(
                decide(
                    &scope(),
                    &Command::Next(sender()),
                    0,
                    with_records(vec![first, other])
                )
                .is_err()
            );
        }
    }
    #[test]
    fn count_settlement_validates_before_mutation_and_partial_keeps_full_success_time() {
        for accepted in [0, 1, 2] {
            let mut before = snapshot(2);
            before.lookup = Some(record());
            before.health[1]["last_success_at"] = 5.into();
            let request = Command::Finish {
                sender: sender(),
                batch: record().metadata.id,
                disposition: Disposition::Count {
                    original: 2,
                    accepted,
                },
            };
            let done = decide(&scope(), &request, 0, before).unwrap();
            assert_eq!(
                (
                    done.result.events.pending,
                    done.result.events.delivered,
                    done.result.events.dropped
                ),
                (0, accepted as u64, 2 - accepted as u64)
            );
            assert_eq!(
                done.result.last_success_at,
                if accepted == 2 { 1 } else { 5 }
            );
            let mut absent = snapshot(0);
            absent.health[1]["events"]["delivered"] = accepted.into();
            absent.health[1]["events"]["dropped"] = (2 - accepted).into();
            let late = decide(&scope(), &request, 0, absent).unwrap();
            assert!(late.remove.is_empty());
            assert_eq!(late.result.events.delivered, accepted as u64);
        }
        for accepted in [-1, 3] {
            let request = Command::Finish {
                sender: sender(),
                batch: record().metadata.id,
                disposition: Disposition::Count {
                    original: 2,
                    accepted,
                },
            };
            assert!(decide(&scope(), &request, 0, snapshot(0)).is_err());
        }
    }
    #[test]
    fn original_store_receipt_is_counted_once_and_removed_bytes_are_exact() {
        let mut before = snapshot(2);
        before.lookup = Some(record());
        let request = Command::Finish {
            sender: sender(),
            batch: record().metadata.id,
            disposition: Disposition::Delivered,
        };
        let done = decide(&scope(), &request, 0, before).unwrap();
        assert_eq!(done.result.events.pending, 0);
        assert_eq!(done.result.events.delivered, 2);
        let global = done.global.unwrap();
        assert_eq!((global.bytes, global.records, global.batches), (0, 0, 0));
        let mut after = snapshot(0);
        after.health[1]["events"]["delivered"] = 2.into();
        let duplicate = decide(&scope(), &request, 0, after).unwrap();
        assert_eq!(duplicate.result.events.delivered, 2);
        assert!(duplicate.remove.is_empty());
    }
    #[test]
    fn expiry_and_opt_out_count_the_same_record_once() {
        let mut before = snapshot(2);
        let mut entry = record();
        entry.metadata.expires_at_ms = 900;
        before.expired = vec![entry.clone()];
        before.records = vec![entry];
        let done = decide(&scope(), &Command::Enabled(false), 0, before).unwrap();
        assert_eq!(done.result.events.dropped, 2);
        assert_eq!(done.remove.len(), 1);
        assert_eq!(done.result.events.pending, 0);
        assert!(!done.result.enabled);
    }
    #[test]
    fn expiry_cannot_discard_an_active_receipt_and_claim_keeps_original_metadata() {
        let mut before = snapshot(2);
        let mut entry = record();
        entry.metadata.expires_at_ms = 900;
        entry.inflight_until = Some(2000);
        before.expired = vec![entry.clone()];
        before.records = vec![];
        let done = decide(&scope(), &Command::Next(sender()), 0, before).unwrap();
        assert_eq!(done.result.events.pending, 2);
        assert!(done.remove.is_empty());
        let next = done.next.unwrap();
        assert_eq!(next.id, entry.metadata.id);
        assert_eq!(next.expires_at_ms, 900);
        assert_eq!(done.put[0].size, 4);
    }
    #[test]
    fn protected_expired_candidate_precedes_live_candidate_after_cleanup() {
        let mut gone = record();
        gone.metadata.expires_at_ms = 800;
        let mut held = record();
        held.metadata.id = "00000000-0000-4000-8000-000000000002".into();
        held.metadata.expires_at_ms = 900;
        held.inflight_until = Some(2000);
        let mut live = record();
        live.metadata.id = "00000000-0000-4000-8000-000000000003".into();
        for grace in [Some(2000), Some(1000)] {
            let mut before = snapshot(6);
            before.record_count = 3;
            before.health[0]["bytes"] = 12.into();
            before.health[0]["batches"] = 3.into();
            held.inflight_until = grace;
            before.expired = vec![gone.clone(), held.clone()];
            before.records = vec![live.clone()];
            let done = decide(&scope(), &Command::Next(sender()), 0, before).unwrap();
            let (selected, dropped) = if grace == Some(2000) {
                (&held, 2)
            } else {
                (&live, 4)
            };
            assert_eq!(done.next.unwrap().id, selected.metadata.id);
            assert_eq!(
                done.put[0].metadata.expires_at_ms,
                selected.metadata.expires_at_ms
            );
            assert_eq!(done.result.events.dropped, dropped);
            assert_eq!(done.result.events.pending, 6 - dropped as u32);
        }
    }
    #[test]
    fn corruption_scope_loss_and_counter_exhaustion_do_not_produce_a_mutation_plan() {
        let mut before = snapshot(2);
        before.lookup = Some(record());
        before.held_scope = None;
        let finish = Command::Finish {
            sender: sender(),
            batch: record().metadata.id,
            disposition: Disposition::Failed,
        };
        assert!(decide(&scope(), &finish, 0, before).is_err());
        let mut before = snapshot(2);
        before.lookup = Some(record());
        before.health[0]["revision"] = MAX_COUNTER.into();
        assert!(decide(&scope(), &finish, 0, before).is_err());
        let mut before = snapshot(0);
        before.health.clear();
        before.record_count = 1;
        assert!(decide(&scope(), &Command::Health, 0, before).is_err());
    }
    #[test]
    fn full_budget_rejects_payload_but_preserves_existing_records() {
        let mut before = snapshot(MAX_RECORDS);
        before.record_count = MAX_RECORDS / 32;
        before.health[0]["batches"] = (MAX_RECORDS / 32).into();
        before.health[0]["bytes"] = MAX_BYTES.into();
        let command = Command::Append(record().metadata);
        let done = decide(&scope(), &command, 4, before).unwrap();
        assert!(done.append.is_none());
        assert!(done.remove.is_empty());
        assert_eq!(done.result.events.pending, MAX_RECORDS);
        assert_eq!(done.result.events.dropped, 2);
    }
    #[test]
    fn retry_accounting_is_bounded_without_altering_the_batch_identity() {
        let mut before = snapshot(2);
        let mut entry = record();
        entry.metadata.attempts = MAX_ATTEMPTS;
        before.lookup = Some(entry.clone());
        let request = Command::Finish {
            sender: sender(),
            batch: entry.metadata.id.clone(),
            disposition: Disposition::Failed,
        };
        let done = decide(&scope(), &request, 0, before).unwrap();
        assert_eq!(done.put[0].metadata.attempts, MAX_ATTEMPTS);
        assert_eq!(done.put[0].metadata.id, entry.metadata.id);
        assert_eq!(done.result.events.failures, 1);
        assert_eq!(done.result.events.pending, 2);
        let loss = Command::Losses {
            events: FeedHealth {
                dropped: MAX_COUNTER,
                failures: MAX_COUNTER,
                ..Default::default()
            },
            traces: FeedHealth::default(),
        };
        let done = decide(&scope(), &loss, 0, snapshot(0)).unwrap();
        assert_eq!(done.result.events.dropped, MAX_COUNTER);
        assert!(Command::Append(entry.metadata).reads(&scope(), 4).is_err());
    }
}
