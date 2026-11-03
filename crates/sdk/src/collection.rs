//! A client collection lifetime is shared by local events and real spans.
//! Export opt-out remains a separate transport decision.
use crate::{Error, feed::Router};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone)]
pub(crate) struct Collection {
    epoch: Arc<AtomicU64>,
    traces: Option<Arc<Router>>,
}
impl Default for Collection {
    fn default() -> Self {
        Self {
            epoch: Arc::new(AtomicU64::new(1)),
            traces: None,
        }
    }
}
impl Collection {
    #[cfg(feature = "client-traces")]
    pub(crate) fn client(traces: Arc<Router>) -> Self {
        Self {
            traces: Some(traces),
            ..Self::default()
        }
    }
    pub(crate) fn current(&self) -> Option<u64> {
        let epoch = self.epoch.load(Ordering::Acquire);
        (epoch & 1 == 1).then_some(epoch)
    }
    pub(crate) fn set_enabled(&self, enabled: bool) -> Result<bool, Error> {
        let previous = self
            .epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                if (epoch & 1 == 1) == enabled {
                    Some(epoch)
                } else {
                    epoch.checked_add(1)
                }
            })
            .map_err(|_| Error)?;
        let changed = (previous & 1 == 1) != enabled;
        if changed && let Some(traces) = &self.traces {
            traces.set_enabled(enabled);
        }
        Ok(changed)
    }
}
