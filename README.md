# NDDev OpenNetwork — Observability

[NDDev OpenNetwork](https://nddev.ai) · AGPL-3.0-only · alpha

Rust owns the closed telemetry boundary, PostgreSQL incident state and bounded
log outbox. Docker Fluent transport delivers process events to Vector, the Rust
normalizer checks/redacts them, and a separate Vector input forwards normalized
logs to OpenObserve. Only this internal pipeline holds ingestion credentials;
this API is not a public client gateway. Source freshness is independent of the
outbox: stale/unsequenced streams and observed sequence gaps cannot prove an
exact number of lost events. At most 16 source names (including this service)
are registered per tenant; removed configuration entries remain visible as
inactive and continue to occupy their durable slot.

The service exports native OTLP counters, durations and spans for its actual
processing work. A server JSON event may provide the parent trace context; it
is never reconstructed as a server span. Delivery counters describe transport
acknowledgements; acceptance separately verifies searchable logs, metrics and
traces in OpenObserve. Incidents have an operator owner, severity, deduplication,
acknowledgement, finite silence and resolution. No notification channel is
configured or claimed delivered.

`NDS_TELEMETRY_ENABLED=false` stops log/metric/trace export and requires no
exporter credential. Local lifecycle, security and health signals continue.
Previously queued payloads remain bounded until expiry or later re-enablement;
new events received while disabled are not queued for export. Event fingerprint
deduplication retains receipts for one hour, while valid event timestamps may
be up to 24 hours old: replay after that horizon can be accepted again. This is
not the business sync operation log. Incident action receipts also retain one
hour; timestamped operation IDs and a persisted monotonic observed-time floor
prevent an expired operation from becoming new again after clock rollback.

Client batches may arrive out of producer order. Unique delayed records remain
searchable and repeated records use the same fingerprint receipts.
Retained producer progress and source freshness never move backwards. Newly
observed failures enter the ordinary incident lifecycle; duplicate receipts do
not reapply transitions. An existing producer cannot be rebound to a different
device or service. A sequence-gap incident retains the observed discontinuity
for operator review; later contiguous arrivals alone do not prove which missing
records were recovered.

Clients can subscribe to SDK-owned event and actual OTLP span feeds.
Each feed bounds queued plus in-flight records to 128, batches to 32 records /
64 KiB and retention to 30 minutes. Batch ownership accounts for cancellation,
expiry and opt-out without duplicate loss counts. A single asynchronous drainer
can await feed readiness; notification does not claim or settle a batch.
Delivery spans retain local
credential diagnostics but cannot recursively enter either export feed. The
`client-traces` feature validates a closed native protobuf projection and lets
the authenticated server attach tenant/user/device ownership. Feed acceptance
is a transport receipt, not proof of searchable OpenObserve delivery. Accounts
and server adapters own that end-to-end transport and its qualification.

The shared `crates/sdk` package owns schema projection, redaction, real operation
span context and bounded debug windows. The collector consumes the same generated
event DTO and validator; it adds ingress freshness/admission rules. The SDK also
owns typed Flutter failure/lifecycle inputs and local diagnostic status. No
exception text or stack crosses that boundary. Process installation replaces the
raw Rust panic hook with a fixed redacted fatal event; a contended logger records
failure rather than waiting while unwinding. Startup and controlled shutdown are
emitted once by the SDK. Forced process termination cannot promise a final event.

Servers and the collector use the same `NDS_LOG_MODE=normal|debug`,
`NDS_DEBUG_SCOPE=http|transport|database|io|process`, `NDS_DEBUG_SECONDS=1..900`
and optional `NDS_DEBUG_EVENT_LIMIT=1..10000` (default 1000). Normal mode rejects
stray debug settings; expiry returns to normal through the same audited window.
`install` uses one writer with 128 event
slots, nonblocking admission, loss counters and at most one second of writer
shutdown waiting. A stuck OS writer is detached until process exit; neither
flushing nor delivery is promised for an undrained pipe. `subscriber<W>` is a
caller-owned embedded/test API with the supplied writer's lifecycle contract.
Android uses native liblog (Logcat tag `NDS`) through the same queue and policy,
because application stdout can be `/dev/null`. Complete JSON records are capped
at 3,900 bytes; larger records or native rejections count as write failures and
loss, with no truncation. Admitted debug records use the native INFO transport
priority while retaining JSON severity; process-global log filters stay intact.
Native acceptance does not promise durable storage or later retrieval.
The WebAssembly SDK uses the same envelope, redaction, native span projection
and feed bounds. Its local sink writes complete immutable JSON to the browser
console and counts write failures; an abortable browser timer owns debug expiry
and heartbeat. It does not create an OS thread or block browser shutdown.
Browser clients stage those feeds into a strict IndexedDB transaction before
network delivery. The spool binds to the caller's verified origin/owner/device
scope and stores exact redacted JSON or OTLP bytes. Limits are 8 MiB of encoded payloads, 4096 records
(and therefore at most 4096 batches) and 16 scopes per origin; batches retain their original 30-minute
eligibility and durable retry counts. Before the first claim, Rust can combine
records of one signal within the same 32-record / 64 KiB transport limits. The
transaction joins their exact bytes and keeps the oldest expiry; a previously
claimed or attempted payload is never changed by batching.
A short in-flight grace permits a bounded
request to settle. A browser-owned sender lock serializes tabs. After a known
receipt, callers retain the batch/count and retry only local `acknowledge_count`
settlement; browser freshness comes from returned `Health.last_success_at`.
Unknown outcomes may repeat spans with their original IDs;
this is not an exactly-once delivery claim. Opt-out discards the scoped queue.
Counters distinguish queued, durably acknowledged, discarded and failed work.
The SDK owns durable-spool admission, expiry and receipt accounting in Rust.
Its synchronous policy callback returns mutations inside the strict IndexedDB
transaction; generated JavaScript owns storage/lock I/O only. Detached operation ownership
keeps the callback alive through commit or abort after caller cancellation.
Author that I/O in `browser_queue.ts` and the console sink in `output/console.ts`.
Storage metadata stays unknown until the Rust policy validates it; TypeScript
checks the concrete browser API inputs, without a parallel spool model.
`just browser-check` checks strict types, Biome and generated-output drift with
the frozen Bun lockfile.

`install_client_with_tracer_to` captures bounded startup events for the first
subscription. Client spans have the same initial buffer. Pre-authentication
capture is RAM-only until a verified scope claims it; closing/replacing a feed
does not transfer its records to another owner. Browser staging continues
independently of a slow HTTP request and retains ownership until actual IDB
completion. A tab can still disappear before a write commits, and browser site
data can be evicted or explicitly cleared. Console output, a successful staging
receipt and a compiled WASM target do not prove remote searchable delivery;
browser applications own that separate acceptance.
For the real Android sink check, select an owned running device explicitly:
`python3 scripts/check-android-sdk.py --serial SERIAL --ndk ANDROID_NDK_PATH`.
It checks the native SDK process; production Flutter APK acceptance is separate.

Run `just check`, `just protocol-check`, `just sdk-check` and `just integration`.
`just sdk-wasm-check` checks the stable Rust browser target; it is a compilation
gate, separate from real browser execution and pipeline qualification.
Integration requires Docker Engine >=28.2.0 for finite Fluent write timeouts;
CI pins 29.9.1. Integration requires a clean, committed collector checkout. It
builds immutable collector and locked server snapshots, passes each exact commit
into its build and verifies their searchable source identities. It starts only temporary generated
accounts/data in an isolated Compose project, then removes its own resources.
The Compose pipeline exposes only the Fluent collector on loopback; its data
services remain internal. Runtime memory caps fit an 8 GiB single-node plan.
Persistent deployment additionally needs approved filesystem quotas for data
volumes. There is no deployment, backup or recovery command in this module.

Vector 0.59.0 (MPL-2.0), OpenObserve 1.1.0 (AGPL-3.0) and PostgreSQL 18.6 images
are pinned by manifest digest in `deploy/compose.yaml`. OpenObserve comes from
the [publisher's registry](https://openobserve.ai/downloads/) with the same OSS
manifest digest previously qualified from ECR. Vector uses its native
directory secret backend, without environment interpolation. OpenObserve vendor
telemetry, model pricing synchronization and GeoIP downloads are disabled.
OpenObserve's default diagnostic retention is seven days; the Rust outbox is capped at 4,096 events
and 64 MiB with finite retries and expiry. Rust dependencies are in `Cargo.lock`.
Local delivery expiry and functional history retain their separate policies.
The storage default does not override existing per-stream retention settings;
qualify the selected deployment and its overrides before claiming a one-week
retention result. Source configuration alone does not change a running service.

Release journals live in [GitHub Releases](https://github.com/NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-observability/releases).
