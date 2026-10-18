//! Bounded, caller-owned export queues. A removed batch owns its receipt until
//! acknowledgement or drop, including when an async transport is cancelled.
use crate::Error;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Poll, Waker},
    time::Duration,
};
#[cfg(target_arch = "wasm32")]
use web_time::Instant;
const CAPACITY: usize = 128;
const MAX_COUNTER: u64 = 9_007_199_254_740_991;
const RETENTION: Duration = Duration::from_secs(1800);

#[derive(Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedHealth {
    pub pending: u32,
    pub dropped: u64,
    pub delivered: u64,
    pub failures: u64,
}
struct Item {
    bytes: Vec<u8>,
    created: Instant,
}
struct State {
    items: VecDeque<Item>,
    inflight: u32,
    dropped: u64,
    delivered: u64,
    failures: u64,
    instance: String,
    sequence: u64,
    enabled: bool,
    closed: bool,
    waiter: Option<Waker>,
}
impl State {
    fn new() -> Self {
        Self {
            items: VecDeque::new(),
            inflight: 0,
            dropped: 0,
            delivered: 0,
            failures: 0,
            instance: uuid::Uuid::new_v4().to_string(),
            sequence: 0,
            enabled: true,
            closed: false,
            waiter: None,
        }
    }
    fn discard(&mut self, count: u32) {
        self.dropped = self
            .dropped
            .saturating_add(u64::from(count))
            .min(MAX_COUNTER);
    }
    fn clear(&mut self) {
        self.discard(self.items.len() as u32);
        self.items.clear();
    }
    fn expire(&mut self) {
        while self
            .items
            .front()
            .is_some_and(|item| item.created.elapsed() >= RETENTION)
        {
            self.items.pop_front();
            self.discard(1);
        }
    }
    fn push(&mut self, bytes: Vec<u8>) {
        self.expire();
        if self.items.len() + self.inflight as usize >= CAPACITY || bytes.len() > 16_385 {
            self.discard(1);
        } else {
            self.items.push_back(Item {
                bytes,
                created: Instant::now(),
            });
        }
    }
}

// Wake outside the state lock: a consumer may immediately try to drain it.
fn wake_ready(mut state: MutexGuard<'_, State>) {
    let waiter = if state.closed || state.enabled && state.inflight == 0 && !state.items.is_empty()
    {
        state.waiter.take()
    } else {
        None
    };
    drop(state);
    if let Some(waiter) = waiter {
        waiter.wake();
    }
}
/// Validate an opaque batch's transport count before any health mutation.
pub(crate) fn settlement(count: u32, accepted: i64) -> Result<(u32, u32), Error> {
    let accepted = u32::try_from(accepted).map_err(|_| Error)?;
    if accepted > count {
        return Err(Error);
    }
    Ok((accepted, count - accepted))
}

pub struct ExportFeed(Arc<Mutex<State>>);
pub struct ExportBatch {
    state: Arc<Mutex<State>>,
    bytes: Vec<u8>,
    count: u32,
    created: Instant,
    settled: bool,
}
impl ExportBatch {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn count(&self) -> u32 {
        self.count
    }
    pub fn expired(&self) -> bool {
        self.created.elapsed() >= RETENTION
    }
    pub fn failed(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.failures = state.failures.saturating_add(1).min(MAX_COUNTER);
    }
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn expires_at_ms(&self) -> Result<u64, Error> {
        let remaining = RETENTION.checked_sub(self.created.elapsed()).ok_or(Error)?;
        u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| Error)?
            .checked_add(remaining.as_millis() as u64)
            .ok_or(Error)
    }
    /// Ownership moved to committed browser storage; this is not delivery.
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn persisted(mut self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.inflight -= self.count;
        self.settled = true;
        wake_ready(state);
    }
    pub fn acknowledge(mut self) {
        self.acknowledge_count(i64::from(self.count))
            .expect("full batch count");
    }
    /// A transport accepted count settles the original batch once. Rejected
    /// records are counted as dropped; this never reserializes or retries bytes.
    /// Invalid counts leave ownership and health unchanged for caller handling.
    pub fn acknowledge_count(&mut self, accepted: i64) -> Result<(), Error> {
        let (delivered, dropped) = settlement(self.count, accepted)?;
        if self.settled {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.inflight -= self.count;
        state.delivered = state
            .delivered
            .saturating_add(u64::from(delivered))
            .min(MAX_COUNTER);
        state.discard(dropped);
        self.settled = true;
        wake_ready(state);
        Ok(())
    }
}
impl Drop for ExportBatch {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !self.settled {
            state.inflight -= self.count;
            state.discard(self.count);
            wake_ready(state);
        }
    }
}

#[derive(Default)]
struct Route {
    active: Weak<Mutex<State>>,
    startup: Option<Arc<Mutex<State>>>,
}
#[derive(Default)]
pub(crate) struct Router(Mutex<Route>);
impl Router {
    /// Only client compositions opt in. Claim startup once; events between
    /// later owners are never carried into the replacement subscription.
    pub fn buffer_startup(&self) -> Result<(), Error> {
        let mut slot = self.0.lock().map_err(|_| Error)?;
        if slot.active.strong_count() != 0 {
            return Err(Error);
        }
        let state = Arc::new(Mutex::new(State::new()));
        slot.active = Arc::downgrade(&state);
        slot.startup = Some(state);
        Ok(())
    }
    pub fn subscribe(&self) -> Result<ExportFeed, Error> {
        let mut slot = self.0.lock().map_err(|_| Error)?;
        if let Some(startup) = slot.startup.take() {
            return Ok(ExportFeed(startup));
        }
        if let Some(previous) = slot.active.upgrade()
            && !previous.lock().map_err(|_| Error)?.closed
        {
            return Err(Error);
        }
        let state = Arc::new(Mutex::new(State::new()));
        slot.active = Arc::downgrade(&state);
        Ok(ExportFeed(state))
    }
    #[cfg(feature = "client-traces")]
    pub fn enqueue_span(&self, bytes: Vec<u8>) {
        if let Some(queue) = self.0.lock().ok().and_then(|slot| slot.active.upgrade()) {
            let mut queue = queue.lock().unwrap_or_else(|error| error.into_inner());
            if queue.enabled && !queue.closed {
                queue.push(bytes);
                wake_ready(queue);
            }
        }
    }
    pub fn enqueue(&self, fields: &serde_json::Map<String, serde_json::Value>) {
        let queue = self.0.lock().ok().and_then(|slot| slot.active.upgrade());
        if let Some(queue) = queue {
            let mut queue = queue.lock().unwrap_or_else(|error| error.into_inner());
            if !queue.enabled || queue.closed {
                return;
            }
            if queue.sequence == MAX_COUNTER {
                queue.discard(1);
                return;
            }
            queue.sequence += 1;
            let mut fields = fields.clone();
            fields.insert("producer.instance_id".into(), queue.instance.clone().into());
            fields.insert("producer.sequence".into(), queue.sequence.into());
            match serde_json::to_vec(&fields) {
                Ok(mut bytes) => {
                    bytes.push(b'\n');
                    queue.push(bytes);
                }
                Err(_) => queue.discard(1),
            }
            wake_ready(queue);
        }
    }
}
impl ExportFeed {
    /// Wake the single asynchronous drainer when a batch can be taken or the
    /// feed closes. This does not take ownership, change bounds or settle data.
    pub async fn ready(&self) {
        std::future::poll_fn(|context| {
            let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
            state.expire();
            if state.closed || state.enabled && state.inflight == 0 && !state.items.is_empty() {
                return Poll::Ready(());
            }
            let previous = if state
                .waiter
                .as_ref()
                .is_none_or(|w| !w.will_wake(context.waker()))
            {
                state.waiter.replace(context.waker().clone())
            } else {
                None
            };
            drop(state);
            drop(previous);
            Poll::Pending
        })
        .await
    }
    /// One batch, at most 32 complete records / 64 KiB. Concurrent drains are
    /// rejected so retries cannot reorder this producer's sequence.
    pub fn take_batch(&self) -> Option<ExportBatch> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.expire();
        if state.closed || !state.enabled || state.inflight != 0 {
            return None;
        }
        let created = state.items.front()?.created;
        let mut bytes = Vec::new();
        let mut count = 0;
        while count < 32
            && state
                .items
                .front()
                .is_some_and(|item| bytes.len() + item.bytes.len() <= 65_536)
        {
            bytes.extend(state.items.pop_front()?.bytes);
            count += 1;
        }
        state.inflight += count;
        Some(ExportBatch {
            state: self.0.clone(),
            bytes,
            count,
            created,
            settled: false,
        })
    }
    /// Opt-out discards queued data; the transport still owns its in-flight
    /// batch and must cancel/drop it. New events remain local only.
    pub fn set_enabled(&self, enabled: bool) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.enabled = enabled;
        if !enabled {
            state.clear();
        }
        wake_ready(state);
    }
    pub fn close(&self) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.closed = true;
        state.clear();
        wake_ready(state);
    }
    pub fn health(&self) -> FeedHealth {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.expire();
        FeedHealth {
            pending: state.items.len() as u32 + state.inflight,
            dropped: state.dropped,
            delivered: state.delivered,
            failures: state.failures,
        }
    }
}
impl Drop for ExportFeed {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Wake},
    };

    struct Consumer {
        state: Weak<Mutex<State>>,
        wakes: AtomicUsize,
    }
    impl Consumer {
        fn new(feed: &ExportFeed) -> Arc<Self> {
            Arc::new(Self {
                state: Arc::downgrade(&feed.0),
                wakes: AtomicUsize::new(0),
            })
        }
        fn count(&self) -> usize {
            self.wakes.load(Ordering::SeqCst)
        }
    }
    impl Wake for Consumer {
        fn wake(self: Arc<Self>) {
            // A runtime can immediately re-enter the feed from its callback.
            assert!(self.state.upgrade().unwrap().try_lock().is_ok());
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn event(router: &Router) {
        router.enqueue(&serde_json::Map::new());
    }
    #[test]
    fn arrival_wakes_idle_consumer_without_taking_the_batch() {
        let router = Router::default();
        let feed = router.subscribe().unwrap();
        let consumer = Consumer::new(&feed);
        let waker = Waker::from(consumer.clone());
        let mut context = Context::from_waker(&waker);
        let mut ready = Box::pin(feed.ready());
        assert!(ready.as_mut().poll(&mut context).is_pending());
        event(&router);
        event(&router);
        assert_eq!(consumer.count(), 1);
        assert!(ready.as_mut().poll(&mut context).is_ready());
        assert_eq!(feed.health().pending, 2);
        feed.take_batch().unwrap().acknowledge();
        assert_eq!(feed.health().delivered, 2);
        assert_eq!(feed.health().dropped, 0);
    }
    #[test]
    fn queued_records_wait_for_the_original_batch_to_settle() {
        for acknowledge in [true, false] {
            let router = Router::default();
            let feed = router.subscribe().unwrap();
            event(&router);
            let first = feed.take_batch().unwrap();
            let consumer = Consumer::new(&feed);
            let waker = Waker::from(consumer.clone());
            let mut context = Context::from_waker(&waker);
            let mut ready = Box::pin(feed.ready());
            assert!(ready.as_mut().poll(&mut context).is_pending());
            event(&router);
            assert_eq!(consumer.count(), 0);
            assert!(feed.take_batch().is_none());
            if acknowledge {
                first.acknowledge();
            } else {
                drop(first);
            }
            assert_eq!(consumer.count(), 1);
            assert!(ready.as_mut().poll(&mut context).is_ready());
            let second = feed.take_batch().unwrap();
            let record: serde_json::Value = serde_json::from_slice(second.bytes()).unwrap();
            assert_eq!(record["producer.sequence"], 2);
            second.acknowledge();
            assert_eq!(feed.health().delivered, if acknowledge { 2 } else { 1 });
            assert_eq!(feed.health().dropped, if acknowledge { 0 } else { 1 });
        }
    }
    #[test]
    fn cancelled_wait_can_be_replaced_and_close_wakes_the_current_consumer() {
        let router = Router::default();
        let feed = router.subscribe().unwrap();
        let old = Consumer::new(&feed);
        let old_waker = Waker::from(old.clone());
        let mut cancelled = Box::pin(feed.ready());
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&old_waker))
                .is_pending()
        );
        drop(cancelled);
        let current = Consumer::new(&feed);
        let waker = Waker::from(current.clone());
        let mut context = Context::from_waker(&waker);
        let mut ready = Box::pin(feed.ready());
        assert!(ready.as_mut().poll(&mut context).is_pending());
        feed.set_enabled(false);
        event(&router);
        assert!(ready.as_mut().poll(&mut context).is_pending());
        assert_eq!(current.count(), 0);
        feed.close();
        assert_eq!((old.count(), current.count()), (0, 1));
        assert!(ready.as_mut().poll(&mut context).is_ready());
        assert!(feed.take_batch().is_none());
    }
    #[cfg(feature = "client-traces")]
    #[test]
    fn trace_arrival_before_and_after_registration_are_both_ready() {
        let router = Router::default();
        let feed = router.subscribe().unwrap();
        router.enqueue_span(vec![1]);
        let consumer = Consumer::new(&feed);
        let waker = Waker::from(consumer.clone());
        let mut context = Context::from_waker(&waker);
        assert!(
            Box::pin(feed.ready())
                .as_mut()
                .poll(&mut context)
                .is_ready()
        );
        feed.take_batch().unwrap().acknowledge();
        let mut ready = Box::pin(feed.ready());
        assert!(ready.as_mut().poll(&mut context).is_pending());
        router.enqueue_span(vec![2]);
        assert_eq!(consumer.count(), 1);
        assert!(ready.as_mut().poll(&mut context).is_ready());
        assert_eq!(feed.take_batch().unwrap().bytes(), &[2]);
    }
    #[test]
    fn accepted_count_settles_owned_native_batch_once_without_changing_bytes() {
        for accepted in [0, 1, 2] {
            let router = Router::default();
            let feed = router.subscribe().unwrap();
            event(&router);
            event(&router);
            let mut batch = feed.take_batch().unwrap();
            let bytes = batch.bytes().to_vec();
            assert_eq!(feed.health().pending, 2);
            for invalid in [-1, 3] {
                assert!(batch.acknowledge_count(invalid).is_err());
                let health = feed.health();
                assert_eq!(
                    (health.pending, health.delivered, health.dropped),
                    (2, 0, 0)
                );
                assert_eq!(batch.bytes(), bytes);
            }
            batch.acknowledge_count(accepted).unwrap();
            batch.acknowledge_count(accepted).unwrap();
            assert_eq!(batch.bytes(), bytes);
            drop(batch);
            let health = feed.health();
            assert_eq!(
                (health.pending, health.delivered, health.dropped),
                (0, accepted as u64, 2 - accepted as u64)
            );
        }
    }
    #[test]
    fn native_optout_expiry_and_unknown_drop_keep_original_ownership_totals() {
        let router = Router::default();
        let feed = router.subscribe().unwrap();
        event(&router);
        event(&router);
        let mut batch = feed.take_batch().unwrap();
        event(&router);
        feed.set_enabled(false);
        assert_eq!((feed.health().pending, feed.health().dropped), (2, 1));
        batch.created = Instant::now() - RETENTION;
        assert!(batch.expired());
        batch.acknowledge_count(1).unwrap();
        drop(batch);
        assert_eq!(
            (
                feed.health().pending,
                feed.health().delivered,
                feed.health().dropped
            ),
            (0, 1, 2)
        );
        feed.set_enabled(true);
        event(&router);
        drop(feed.take_batch().unwrap());
        assert_eq!(
            (
                feed.health().pending,
                feed.health().delivered,
                feed.health().dropped
            ),
            (0, 1, 3)
        );
    }
    #[test]
    fn startup_is_claimed_once_and_cannot_cross_a_replacement_owner() {
        let router = Router::default();
        router.buffer_startup().unwrap();
        event(&router);
        let first = router.subscribe().unwrap();
        assert_eq!(first.health().pending, 1);
        first.take_batch().unwrap().acknowledge();
        first.close();
        event(&router);
        let replacement = router.subscribe().unwrap();
        assert_eq!(replacement.health().pending, 0);
        event(&router);
        assert_eq!(replacement.health().pending, 1);
    }
    #[test]
    fn cancellation_opt_out_and_replacement_account_for_every_admitted_record() {
        let router = Router::default();
        let feed = router.subscribe().unwrap();
        for _ in 0..40 {
            event(&router);
        }
        let batch = feed.take_batch().unwrap();
        assert_eq!(batch.count(), 32);
        assert_eq!(feed.health().pending, 40);
        assert!(feed.take_batch().is_none());
        batch.failed();
        feed.set_enabled(false);
        event(&router);
        assert_eq!(feed.health().pending, 32);
        assert_eq!(feed.health().dropped, 8);
        drop(batch);
        assert_eq!(feed.health().pending, 0);
        assert_eq!(feed.health().dropped, 40);
        feed.set_enabled(true);
        event(&router);
        let old = feed.take_batch().unwrap();
        feed.close();
        let next = router.subscribe().unwrap();
        event(&router);
        drop(old);
        assert_eq!(feed.health().dropped, 41);
        assert_eq!(next.health().pending, 1);
        next.take_batch().unwrap().acknowledge();
        assert_eq!(next.health().delivered, 1);
        assert_eq!(next.health().pending, 0);
        assert_eq!(next.health().dropped, 0);
    }
    #[test]
    fn paused_records_expire_and_inflight_records_share_the_capacity_bound() {
        let router = Router::default();
        let feed = router.subscribe().unwrap();
        for _ in 0..128 {
            event(&router);
        }
        let batch = feed.take_batch().unwrap();
        event(&router);
        assert_eq!(feed.health().pending, 128);
        assert_eq!(feed.health().dropped, 1);
        batch.acknowledge();
        for item in &mut feed.0.lock().unwrap().items {
            item.created -= RETENTION;
        }
        assert!(feed.take_batch().is_none());
        assert_eq!(feed.health().dropped, 97);
        assert_eq!(feed.health().delivered, 32);
    }
}
