use std::{fmt, fs::File, io::Read, net::SocketAddr, path::Path, time::Duration};

#[derive(Clone)]
pub struct Secret(String);
impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
    #[cfg(test)]
    pub(crate) fn for_tests(value: &str) -> Self {
        Self(value.into())
    }
}
impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid observability configuration")]
pub struct ConfigError;

fn secret(name: &str) -> Result<Secret, ConfigError> {
    let path = std::env::var(format!("{name}_FILE")).map_err(|_| ConfigError)?;
    if std::env::var_os(name).is_some() {
        return Err(ConfigError);
    }
    let mut file = File::open(Path::new(&path)).map_err(|_| ConfigError)?;
    if !file.metadata().map_err(|_| ConfigError)?.is_file() {
        return Err(ConfigError);
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(16_385)
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigError)?;
    if bytes.len() > 16_384 {
        return Err(ConfigError);
    }
    let value = String::from_utf8(bytes)
        .map_err(|_| ConfigError)?
        .trim_end_matches(['\r', '\n'])
        .to_owned();
    if value.is_empty() {
        return Err(ConfigError);
    }
    Ok(Secret(value))
}

fn count(name: &str, default: u64, maximum: u64) -> Result<u64, ConfigError> {
    let value = std::env::var(name)
        .ok()
        .map(|v| v.parse::<u64>())
        .transpose()
        .map_err(|_| ConfigError)?
        .unwrap_or(default);
    (value > 0 && value <= maximum)
        .then_some(value)
        .ok_or(ConfigError)
}

pub struct Config {
    pub address: SocketAddr,
    pub tenant: String,
    pub database: Secret,
    pub ingest_token: Secret,
    pub admin_token: Secret,
    pub vector_password: Option<Secret>,
    pub vector_url: reqwest::Url,
    pub otlp_url: reqwest::Url,
    pub export_enabled: bool,
    pub expected_sources: Vec<String>,
    pub stale_after: Duration,
    pub delivery_interval: Duration,
    pub outbox_ttl: Duration,
    pub signal_interval: Duration,
    pub search: Option<SearchConfig>,
}

/// Read access to the stored diagnostics (OpenObserve `_search`). Absent when
/// `NDS_OO_SEARCH_URL` is unset; search then answers `dependency_unavailable`.
pub struct SearchConfig {
    pub url: reqwest::Url,
    pub organization: String,
    pub logs: String,
    pub traces: String,
    pub user: String,
    pub password: Secret,
}

fn name(variable: &str, default: &str) -> Result<String, ConfigError> {
    let value = std::env::var(variable).unwrap_or_else(|_| default.into());
    crate::search::identifier(&value)
        .then_some(value)
        .ok_or(ConfigError)
}

fn search() -> Result<Option<SearchConfig>, ConfigError> {
    let Ok(url) = std::env::var("NDS_OO_SEARCH_URL") else {
        return Ok(None);
    };
    let mut url = reqwest::Url::parse(&url).map_err(|_| ConfigError)?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError);
    }
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(Some(SearchConfig {
        url,
        organization: name("NDS_OO_ORG", "default")?,
        logs: name("NDS_OO_LOGS_STREAM", "nds")?,
        traces: name("NDS_OO_TRACES_STREAM", "default")?,
        user: std::env::var("NDS_OO_SEARCH_USER")
            .ok()
            .filter(|user| !user.is_empty() && user.len() <= 254 && !user.contains(':'))
            .ok_or(ConfigError)?,
        password: secret("NDS_OO_SEARCH_PASSWORD")?,
    }))
}

impl Config {
    pub fn read() -> Result<Self, ConfigError> {
        let address = std::env::var("NDS_OBS_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".into())
            .parse()
            .map_err(|_| ConfigError)?;
        let vector_url = reqwest::Url::parse(
            &std::env::var("NDS_VECTOR_URL").unwrap_or_else(|_| "http://vector:8081/events".into()),
        )
        .map_err(|_| ConfigError)?;
        let otlp_url = reqwest::Url::parse(
            &std::env::var("NDS_OTLP_URL").unwrap_or_else(|_| "http://vector:4318".into()),
        )
        .map_err(|_| ConfigError)?;
        for url in [&vector_url, &otlp_url] {
            if !matches!(url.scheme(), "http" | "https")
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(ConfigError);
            }
        }
        let expected_sources: Vec<_> = std::env::var("NDS_EXPECTED_SOURCES")
            .unwrap_or_else(|_| "nddev-device-sync-server".into())
            .split(',')
            .map(str::to_owned)
            .collect();
        if expected_sources.is_empty()
            || expected_sources.len() > 16
            || expected_sources.iter().any(|s| {
                s.is_empty()
                    || s.len() > 128
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b))
            })
        {
            return Err(ConfigError);
        }
        let expected_sources = source_set(expected_sources)?;
        let tenant = std::env::var("NDS_OBS_TENANT_ID").unwrap_or_else(|_| "local".into());
        if tenant.is_empty()
            || tenant.len() > 128
            || !tenant
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".-_:".contains(&b))
        {
            return Err(ConfigError);
        }
        let export_enabled = match std::env::var("NDS_TELEMETRY_ENABLED")
            .as_deref()
            .unwrap_or("true")
        {
            "true" => true,
            "false" => false,
            _ => return Err(ConfigError),
        };
        let ingest_token = secret("NDS_INGEST_TOKEN")?;
        let admin_token = secret("NDS_ADMIN_TOKEN")?;
        let vector_password = export_enabled
            .then(|| secret("NDS_VECTOR_PASSWORD"))
            .transpose()?;
        for token in [&ingest_token, &admin_token]
            .into_iter()
            .chain(vector_password.as_ref())
        {
            if token.expose().len() < 32
                || token.expose().len() > 128
                || !token
                    .expose()
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
            {
                return Err(ConfigError);
            }
        }
        if ingest_token.expose() == admin_token.expose() {
            return Err(ConfigError);
        }
        Ok(Self {
            address,
            tenant,
            database: secret("NDS_OBS_DATABASE_URL")?,
            ingest_token,
            admin_token,
            vector_password,
            vector_url,
            otlp_url,
            export_enabled,
            expected_sources,
            stale_after: Duration::from_secs(count("NDS_SOURCE_STALE_SECONDS", 90, 900)?),
            delivery_interval: Duration::from_millis(count(
                "NDS_DELIVERY_INTERVAL_MS",
                1000,
                10000,
            )?),
            outbox_ttl: Duration::from_secs(count("NDS_OUTBOX_TTL_SECONDS", 3600, 86400)?),
            signal_interval: Duration::from_secs(count("NDS_SIGNAL_INTERVAL_SECONDS", 5, 60)?),
            search: search()?,
        })
    }
}

pub fn migration_url() -> Result<Secret, ConfigError> {
    secret("NDS_OBS_MIGRATION_DATABASE_URL")
}

pub(crate) const OWN_SERVICE: &str = "nddev-device-sync-observability";
pub(crate) fn source_set(values: Vec<String>) -> Result<Vec<String>, ConfigError> {
    let mut unique: std::collections::BTreeSet<_> = values.into_iter().collect();
    unique.insert(OWN_SERVICE.into());
    if unique.len() > 16 {
        return Err(ConfigError);
    }
    Ok(unique.into_iter().collect())
}
