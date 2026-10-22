use nddev_device_sync_observability::{config, runtime, signals::Signals, store};
use nddev_device_sync_telemetry::{Config, DebugWindow, install};
use std::sync::Arc;

fn main() -> std::process::ExitCode {
    let debug = DebugWindow::from_settings(|name| std::env::var(name).ok());
    let logging_valid = debug.is_ok();
    let logging = install(Config {
        service: "nddev-device-sync-observability".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        repository: "NDDev-Technocracy-OpenNetwork-NDS/nddev-device-sync-observability".into(),
        commit: option_env!("NDS_SOURCE_COMMIT").unwrap_or("unknown").into(),
        channel: "alpha".into(),
        standards: env!("NDS_STANDARDS_RELEASE").into(),
        environment: "self-hosted".into(),
        target_prefixes: vec!["nddev_device_sync_observability".into()],
        default_module: "process".into(),
        debug: debug.unwrap_or(None),
    });
    let Ok(_logging) = logging else {
        eprintln!("telemetry initialization failed");
        return std::process::ExitCode::FAILURE;
    };
    if !logging_valid {
        tracing::error!(event.name="process.failed",error.type="invalid_logging_configuration",outcome="error");
        return std::process::ExitCode::FAILURE;
    }
    let result = execute();
    if result.is_err() {
        tracing::error!(event.name="process.failed",error.type="process_unavailable",outcome="error");
        std::process::ExitCode::FAILURE
    } else {
        std::process::ExitCode::SUCCESS
    }
}
fn execute() -> Result<(), ()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .map_err(|_| ())?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["migrate"] {
        let database = config::migration_url().map_err(|_| ())?;
        tracing::info!(event.name = "migration.started", outcome = "started");
        let result = runtime.block_on(store::migrate(&database));
        runtime.shutdown_timeout(std::time::Duration::from_secs(2));
        result.map_err(|_| ())?;
        tracing::info!(
            event.name = "migration.completed",
            migration.version = store::REQUIRED_SCHEMA_VERSION,
            outcome = "ok"
        );
        return Ok(());
    }
    if !args.is_empty() {
        return Err(());
    }
    let config = Arc::new(config::Config::read().map_err(|_| ())?);
    // Construct and close the blocking OTLP clients outside the Tokio context.
    let signals = Arc::new(Signals::new(&config).map_err(|_| ())?);
    let result = runtime.block_on(runtime::run(config, signals.clone()));
    signals.shutdown();
    drop(signals);
    runtime.shutdown_timeout(std::time::Duration::from_secs(2));
    result.map_err(|_| ())
}
