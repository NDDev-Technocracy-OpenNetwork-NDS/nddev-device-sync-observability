//! Authenticated export spool. Rust owns policy; the browser owns the strict
//! transaction, opaque byte storage and physical sender lock. Cancelled callers
//! cannot drop a callback while its IndexedDB operation is still using it.
use crate::{Error, ExportBatch, FeedHealth, browser_queue_policy as policy};
use policy::{Command, Disposition, Metadata, decide};
pub use policy::{Health, Signal};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
    time::Duration,
};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;
use web_time::Instant;

pub struct Batch {
    metadata: Metadata,
    bytes: Vec<u8>,
}
impl Batch {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn count(&self) -> u32 {
        self.metadata.count
    }
    pub fn attempts(&self) -> u8 {
        self.metadata.attempts
    }
    pub fn signal(&self) -> Signal {
        self.metadata.signal
    }
    pub fn expired(&self) -> bool {
        crate::unix_millis_now().max(0) as u64 >= self.metadata.expires_at_ms
    }
}
#[wasm_bindgen(module = "/src/browser_queue.js")]
extern "C" {
    #[wasm_bindgen(catch,js_name=queueOperation)]
    fn operation(
        reads: &str,
        bytes: &[u8],
        policy: &js_sys::Function,
    ) -> Result<js_sys::Promise, JsValue>;
    #[wasm_bindgen(catch,js_name=acquireSender)]
    fn acquire(scope: &str, id: &str, timeout: u32) -> Result<js_sys::Promise, JsValue>;
    #[wasm_bindgen(js_name=renewSender)]
    fn renew(scope: &str, id: &str, timeout: u32) -> bool;
    #[wasm_bindgen(js_name=releaseSender)]
    fn release(id: &str);
}
thread_local! {
    static OPERATIONS:Cell<u8>=const{Cell::new(0)};
    static SENDERS:RefCell<BTreeMap<String,Instant>>=const{RefCell::new(BTreeMap::new())};
}
struct Operation;
impl Operation {
    fn admit() -> Result<Self, Error> {
        OPERATIONS.with(|count| {
            if count.get() >= policy::MAX_OPERATIONS {
                Err(Error)
            } else {
                count.set(count.get() + 1);
                Ok(Self)
            }
        })
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        OPERATIONS.with(|count| count.set(count.get() - 1));
    }
}
struct Lease {
    id: String,
    scope: String,
}
impl Lease {
    fn new(scope: String) -> Result<Rc<Self>, Error> {
        let id = uuid::Uuid::new_v4().to_string();
        SENDERS.with(|senders| {
            let mut senders = senders.borrow_mut();
            let now = Instant::now();
            senders.retain(|_, deadline| *deadline > now);
            if senders.len() >= policy::MAX_SCOPES {
                return Err(Error);
            }
            senders.insert(
                id.clone(),
                now + Duration::from_millis(u64::from(policy::SENDER_MS)),
            );
            Ok(())
        })?;
        Ok(Rc::new(Self { id, scope }))
    }
    fn renew(&self, timeout: u32) -> Result<(), Error> {
        SENDERS.with(|senders| {
            let mut senders = senders.borrow_mut();
            let deadline = senders.get_mut(&self.id).ok_or(Error)?;
            let now = Instant::now();
            if *deadline <= now {
                return Err(Error);
            }
            let extended = (*deadline).max(now + Duration::from_millis(u64::from(timeout)));
            let timeout =
                u32::try_from(extended.duration_since(now).as_nanos().div_ceil(1_000_000))
                    .map_err(|_| Error)?;
            if !renew(&self.scope, &self.id, timeout) {
                return Err(Error);
            }
            *deadline = extended;
            Ok(())
        })
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        release(&self.id);
        SENDERS.with(|senders| {
            senders.borrow_mut().remove(&self.id);
        });
    }
}
#[derive(Clone)]
pub struct Queue {
    scope: String,
}
impl Queue {
    pub fn new(scope: String) -> Result<Self, Error> {
        if !policy::valid_scope(&scope) {
            return Err(Error);
        }
        Ok(Self { scope })
    }
    async fn call(
        &self,
        command: Command,
        bytes: &[u8],
        lease: Option<Rc<Lease>>,
    ) -> Result<JsValue, Error> {
        let reads =
            serde_json::to_string(&command.reads(&self.scope, bytes.len())?).map_err(|_| Error)?;
        let permit = Operation::admit()?;
        let scope = self.scope.clone();
        let bytes = bytes.to_vec();
        let size = bytes.len();
        let (send, receive) = futures::channel::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let callback = Closure::<dyn FnMut(String) -> Result<String, JsValue>>::new(
                move |snapshot: String| {
                    let result = (|| {
                        if snapshot.len() > 8 * 1024 * 1024 {
                            return Err(Error);
                        }
                        let snapshot = serde_json::from_str(&snapshot).map_err(|_| Error)?;
                        serde_json::to_string(&decide(&scope, &command, size, snapshot)?)
                            .map_err(|_| Error)
                    })();
                    result.map_err(|_| js_sys::Error::new("telemetry_store_unavailable").into())
                },
            );
            // Neither callback nor admission is tied to the caller's future.
            // The adapter resolves only after commit/abort and has no later call.
            let result = match operation(&reads, &bytes, callback.as_ref().unchecked_ref()) {
                Ok(promise) => JsFuture::from(promise).await.map_err(|_| Error),
                Err(_) => Err(Error),
            };
            drop(callback);
            drop(lease);
            drop(permit);
            let _ = send.send(result);
        });
        receive.await.map_err(|_| Error)?
    }
    fn health_result(value: JsValue) -> Result<Health, Error> {
        let text = value.as_string().filter(|v| v.len() <= 4096).ok_or(Error)?;
        serde_json::from_str(&text).map_err(|_| Error)
    }
    pub async fn health(&self) -> Result<Health, Error> {
        Self::health_result(self.call(Command::Health, &[], None).await?)
    }
    pub async fn record_losses(
        &self,
        events: FeedHealth,
        traces: FeedHealth,
    ) -> Result<Health, Error> {
        Self::health_result(
            self.call(Command::Losses { events, traces }, &[], None)
                .await?,
        )
    }
    pub async fn set_enabled(&self, enabled: bool) -> Result<Health, Error> {
        Self::health_result(self.call(Command::Enabled(enabled), &[], None).await?)
    }
    pub async fn persist(&self, signal: Signal, batch: ExportBatch) -> Result<Health, Error> {
        let metadata = Metadata {
            id: uuid::Uuid::new_v4().to_string(),
            signal,
            count: batch.count(),
            expires_at_ms: batch.expires_at_ms()?,
            attempts: 0,
        };
        let queue = self.clone();
        let (send, receive) = futures::channel::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = queue
                .call(Command::Append(metadata), batch.bytes(), None)
                .await
                .and_then(Self::health_result);
            if result.is_ok() {
                batch.persisted();
            } else {
                batch.failed();
            }
            let _ = send.send(result);
        });
        receive.await.map_err(|_| Error)?
    }
    pub async fn sender(&self) -> Result<Option<Sender>, Error> {
        let lease = Lease::new(self.scope.clone())?;
        let (send, receive) = futures::channel::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = match acquire(&lease.scope, &lease.id, policy::SENDER_MS) {
                Ok(promise) => JsFuture::from(promise)
                    .await
                    .map_err(|_| Error)
                    .and_then(|value| value.as_bool().ok_or(Error)),
                Err(_) => Err(Error),
            }
            .map(|acquired| acquired.then_some(Sender { lease }));
            let _ = send.send(result);
        });
        receive.await.map_err(|_| Error)?
    }
}
pub struct Sender {
    lease: Rc<Lease>,
}
impl Sender {
    async fn call(&self, command: Command) -> Result<JsValue, Error> {
        Queue {
            scope: self.lease.scope.clone(),
        }
        .call(command, &[], Some(self.lease.clone()))
        .await
    }
    pub async fn next(&self) -> Result<Option<Batch>, Error> {
        let value = self.call(Command::Next(self.lease.id.clone())).await?;
        // A health result means no eligible record; the adapter returns opaque
        // bytes only for the exact record the Rust plan selected.
        if value.is_string() {
            Queue::health_result(value)?;
            return Ok(None);
        }
        let parts = value.dyn_into::<js_sys::Array>().map_err(|_| Error)?;
        if parts.length() != 2 {
            return Err(Error);
        }
        let metadata: Metadata = serde_json::from_str(
            &parts
                .get(0)
                .as_string()
                .filter(|v| v.len() <= 1024)
                .ok_or(Error)?,
        )
        .map_err(|_| Error)?;
        metadata.validate()?;
        let bytes = parts
            .get(1)
            .dyn_into::<js_sys::Uint8Array>()
            .map_err(|_| Error)?
            .to_vec();
        if bytes.is_empty() || bytes.len() > policy::MAX_BATCH_BYTES {
            return Err(Error);
        }
        Ok(Some(Batch { metadata, bytes }))
    }
    pub async fn acknowledge(&self, batch: &Batch) -> Result<Health, Error> {
        self.finish(Disposition::Delivered, batch).await
    }
    /// Partial transport acceptance removes the opaque batch once and reports
    /// rejected records as dropped, without asserting full-success freshness.
    pub async fn acknowledge_count(&self, batch: &Batch, accepted: i64) -> Result<Health, Error> {
        crate::feed::settlement(batch.count(), accepted)?;
        // Only a known, valid receipt may extend current ownership. Never
        // shorten its granted tail or start an extension after batch expiry;
        // an already-held sender can still settle the protected late receipt.
        let now = crate::unix_millis_now().max(0) as u64;
        let remaining = batch
            .metadata
            .expires_at_ms
            .saturating_sub(now)
            .min(u64::from(policy::SENDER_MS)) as u32;
        if remaining != 0 {
            self.lease.renew(remaining)?;
        }
        self.finish(
            Disposition::Count {
                original: batch.count(),
                accepted,
            },
            batch,
        )
        .await
    }
    pub async fn failed(&self, batch: &Batch) -> Result<Health, Error> {
        self.finish(Disposition::Failed, batch).await
    }
    pub async fn discard(&self, batch: &Batch) -> Result<Health, Error> {
        self.finish(Disposition::Dropped, batch).await
    }
    async fn finish(&self, disposition: Disposition, batch: &Batch) -> Result<Health, Error> {
        Self::health(
            self.call(Command::Finish {
                sender: self.lease.id.clone(),
                batch: batch.metadata.id.clone(),
                disposition,
            })
            .await?,
        )
    }
    fn health(value: JsValue) -> Result<Health, Error> {
        Queue::health_result(value)
    }
}
