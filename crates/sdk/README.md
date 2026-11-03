# nddev-device-sync-telemetry

Bounded telemetry client SDK for nddev-device-sync: typed events, OTLP and
Fluent transport, client traces and the browser queue. Domain transitions
stay pure; adapters own HTTP, PostgreSQL and process I/O.

Consumers pin this crate by immutable Git revision through the lockfile; see
the root README for the release journal.

Native storage owners can opt into `native-spool` to read
`ExportBatch::expires_at_ms()` and call `persisted()` after committing the exact
batch to their durable queue. This transfers ownership without reporting remote
delivery or loss; the caller owns the subsequent expiry, receipt and discard
accounting. The feature alone creates no queue and gives no restart guarantee.
