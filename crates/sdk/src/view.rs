//! Typed local diagnostics shared with native and browser presentation. No raw error input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "browser-bindings"),
    derive(serde::Serialize, tsify::Tsify, serde::Deserialize)
)]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "browser-bindings"),
    tsify(large_number_types_as_bigints, missing_as_null)
)]
pub enum DebugScope {
    Http,
    Transport,
    Database,
    Io,
    Process,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DiagnosticsError {
    #[error("invalid_diagnostics_input")]
    InvalidInput,
    #[error("diagnostics_unavailable")]
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "browser-bindings"),
    derive(serde::Serialize, tsify::Tsify, serde::Deserialize)
)]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "browser-bindings"),
    tsify(large_number_types_as_bigints, missing_as_null)
)]
pub enum FailureKind {
    FlutterFramework,
    DartUnhandled,
    NativeBridge,
    ReactRender,
    BrowserUnhandled,
}
impl FailureKind {
    pub(crate) fn class(self) -> &'static str {
        match self {
            Self::FlutterFramework => "flutter_framework_error",
            Self::DartUnhandled => "dart_unhandled_error",
            Self::NativeBridge => "native_bridge_error",
            Self::ReactRender => "react_render_error",
            Self::BrowserUnhandled => "browser_unhandled_error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "browser-bindings"),
    derive(serde::Serialize, tsify::Tsify, serde::Deserialize)
)]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "browser-bindings"),
    tsify(large_number_types_as_bigints, missing_as_null)
)]
pub enum ClientLifecycle {
    Ready,
    Resumed,
    Inactive,
    Hidden,
    Paused,
    Detached,
}
impl ClientLifecycle {
    pub(crate) fn event(self) -> &'static str {
        match self {
            Self::Ready => "client.ready",
            Self::Resumed => "client.resumed",
            Self::Inactive => "client.inactive",
            Self::Hidden => "client.hidden",
            Self::Paused => "client.paused",
            Self::Detached => "client.detached",
        }
    }
}

/// A local sink is never evidence of remote collection or searchable delivery.
#[derive(Clone)]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "browser-bindings"),
    derive(serde::Serialize, tsify::Tsify)
)]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "browser-bindings"),
    tsify(large_number_types_as_bigints, missing_as_null)
)]
pub struct LocalDiagnostics {
    pub sink_initialized: bool,
    /// Actual local collection state; None means the SDK is unavailable.
    pub collection_enabled: Option<bool>,
    pub debug_enabled: bool,
    pub debug_scope: Option<DebugScope>,
    pub rejected_fields_or_events: Option<u64>,
    pub dropped_events: Option<u64>,
    pub pending_events: Option<u64>,
    pub write_failures: Option<u64>,
    pub terminating: Option<bool>,
    pub shutdown_incomplete: Option<bool>,
}
