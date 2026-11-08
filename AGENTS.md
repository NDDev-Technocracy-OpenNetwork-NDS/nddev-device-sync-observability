# nddev-device-sync-observability working contract

Read README.md, module.yaml and contracts.lock. Domain transitions are pure; adapters
own HTTP, PostgreSQL, OTLP and process I/O. Protocol owns schemas; generate consumer
DTOs from the recorded immutable source instead of maintaining copies by hand.

Keep all work bounded and cancellable. Preserve private estates and unrelated work.
Never commit operator accounts, credentials, deployment addresses, real events or
runtime evidence. Secret values never enter logs, responses or Debug. No backups,
recovery, live deployment or provider notifications are implied.

Use `just check` and the isolated real Compose acceptance before claiming a working
pipeline. JSON logs are not OTLP traces; report the actual signal types, delivery state
and source freshness. Clean only test-owned ephemeral resources.
