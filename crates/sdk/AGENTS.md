# Telemetry SDK working contract

Secret values never enter events, logs, responses or Debug output; keep
batches bounded and cancellable. Browser bindings stay behind their feature
and expose no credentials. Regenerate consumer-facing schemas through the
pinned protocol source, never by hand. Run the workspace `just check` and
`sdk-check` before review.
