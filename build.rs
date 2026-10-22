use std::fs;
use std::path::Path;

// Telemetry must report the consumed standards release, so the value is baked
// from the immutable lock at compile time rather than repeated by hand.
fn main() {
    println!("cargo:rerun-if-changed=standarts.lock");
    let release = fs::read_to_string(Path::new("standarts.lock"))
        .ok()
        .and_then(|body| {
            body.lines().map(str::trim).find_map(|line| {
                let rest = line.strip_prefix("source_release")?;
                let rest = rest.trim_start().strip_prefix('=')?.trim();
                rest.strip_prefix('"')?.strip_suffix('"').map(str::to_owned)
            })
        })
        .unwrap_or_else(|| "unreleased".into());
    println!("cargo:rustc-env=NDS_STANDARDS_RELEASE={release}");
}
