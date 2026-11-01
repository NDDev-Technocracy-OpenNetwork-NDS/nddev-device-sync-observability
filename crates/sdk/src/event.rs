// Generated; do not edit. AGPL-3.0-only. NDDev OpenNetwork — https://nddev.ai
// Source: NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-protocol/contracts/v2/telemetry-event.schema.json
// Schema SHA-256: 30b2e4e5efde87165d9da30e4e29ddca468423f246d4b98a94daf9318182d01f
// Generator script SHA-256: ba71ce353a691d59227fbe948f45edf4b0667be7e5a105741f551d3186f6a8b8
// Tool: quicktype-core 26.0.0; dependency lock SHA-256: ec1d8541bcae0771e2fe0e95e572f60898fd62e6e31fbfbea80451d244aff5b9
// DTO generation is not schema, authorization or cryptographic validation.

use serde::{Deserialize, Serialize};

/// Closed flat process envelope matching the server producer. The ingress also enforces a
/// maximum encoded size of 16384 UTF-8 bytes; JSON Schema validation alone does not enforce
/// a byte budget. Context uses matched routes and stable error classes, never raw request
/// data.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<AuthMethod>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub available: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff_seconds: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub challenge_invalidated: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_verified: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_seconds: Option<i32>,

    #[serde(rename = "deployment.environment")]
    pub deployment_environment: String,

    #[serde(rename = "device.id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub dropped_count: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub email_available: Option<bool>,

    /// A fixed redacted producer error class. The current server also emits fixed human-readable
    /// error classes; never driver text, URLs or dynamic messages.
    #[serde(rename = "error.type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,

    #[serde(rename = "event.name")]
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_budget: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_available: Option<bool>,

    /// Public invitation identity only. Invitation token, verifier and recipient email remain
    /// excluded.
    #[serde(rename = "invitation.id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invitation_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mailbox_delivery: Option<MailboxDelivery>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<Method>,

    #[serde(rename = "migration.version")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migration_version: Option<i64>,

    pub module: String,

    /// Public operation identity for correlated committed/replayed control actions; never a
    /// credential.
    #[serde(rename = "operation.id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,

    #[serde(rename = "producer.instance_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_instance_id: Option<String>,

    #[serde(rename = "producer.sequence")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_sequence: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejected_count: Option<i64>,

    #[serde(rename = "release.channel")]
    pub release_channel: String,

    #[serde(rename = "release.version")]
    pub release_version: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<Resource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<Scope>,

    #[serde(rename = "service.name")]
    pub service_name: String,

    #[serde(rename = "service.version")]
    pub service_version: String,

    pub severity: Severity,

    #[serde(rename = "source.commit")]
    pub source_commit: String,

    #[serde(rename = "source.repository")]
    pub source_repository: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,

    #[serde(rename = "standards.release")]
    pub standards_release: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<i32>,

    /// Explicit target of an administrative action; user.id remains the authenticated actor.
    #[serde(rename = "target.user.id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_user_id: Option<String>,

    #[serde(rename = "tenant.id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,

    pub timestamp: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<Transport>,

    #[serde(rename = "user.id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    #[serde(rename = "email_otp")]
    EmailOtp,

    Github,

    Passkey,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MailboxDelivery {
    Unverified,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Method {
    #[serde(rename = "CONNECT")]
    Connect,

    #[serde(rename = "DELETE")]
    Delete,

    #[serde(rename = "GET")]
    Get,

    #[serde(rename = "HEAD")]
    Head,

    #[serde(rename = "OPTIONS")]
    Options,

    #[serde(rename = "OTHER")]
    Other,

    #[serde(rename = "PATCH")]
    Patch,

    #[serde(rename = "POST")]
    Post,

    #[serde(rename = "PUT")]
    Put,

    #[serde(rename = "TRACE")]
    Trace,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Accepted,

    Approved,

    Checked,

    Denied,

    Error,

    #[serde(rename = "ok")]
    OutcomeOk,

    Pending,

    #[serde(rename = "provider_accepted")]
    ProviderAccepted,

    Ready,

    Rejected,

    Started,

    Stopped,

    Unavailable,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    #[serde(rename = "event_budget")]
    EventBudget,

    Expired,

    Operator,

    #[serde(rename = "process_exit")]
    ProcessExit,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Resource {
    Connections,

    Requests,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Database,

    Http,

    Io,

    Process,

    Transport,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Debug,

    Error,

    Fatal,

    Info,

    Trace,

    Warn,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Http,

    Https,
}
