use crate::timestamp;
use crate::{
    application::{AcceptedEvent, ActionCommand, Error, Receipt, Repository},
    config::Secret,
    domain::{self, Action, Condition, Head, Incident, State},
};
use sqlx::{
    ConnectOptions, PgPool, Postgres, Row, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions, PgRow},
};
use std::{str::FromStr, sync::Arc, time::Duration};
use time::OffsetDateTime;

pub const MAX_RECEIPTS: i64 = 65_536;
pub const MAX_QUEUE: i64 = 4_096;
pub const MAX_QUEUE_BYTES: i64 = 67_108_864;
pub const REQUIRED_SCHEMA_VERSION: u32 = 2;
use crate::config::OWN_SERVICE;

#[derive(Clone)]
pub struct PgRepository {
    pub pool: PgPool,
    pub tenant: Arc<str>,
    pub expected: Arc<[String]>,
}

fn unavailable(_: sqlx::Error) -> Error {
    Error::Unavailable
}
fn instant(ms: i64) -> Result<OffsetDateTime, Error> {
    timestamp::from_millis(ms).ok_or(Error::Invalid)
}

pub async fn connect(url: &Secret, maximum: u32) -> Result<PgPool, Error> {
    let options = PgConnectOptions::from_str(url.expose())
        .map_err(|_| Error::Unavailable)?
        .disable_statement_logging();
    PgPoolOptions::new()
        .max_connections(maximum)
        .acquire_timeout(Duration::from_secs(3))
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='3s'")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET lock_timeout='1s'")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(unavailable)
}

pub async fn migrate(url: &Secret) -> Result<(), Error> {
    let pool = connect(url, 1).await?;
    let result = tokio::time::timeout(Duration::from_secs(60), sqlx::migrate!().run(&pool)).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), pool.close()).await;
    result
        .map_err(|_| Error::Unavailable)?
        .map_err(|_| Error::Unavailable)
}

impl PgRepository {
    pub async fn open(url: &Secret, tenant: String, expected: Vec<String>) -> Result<Self, Error> {
        let expected = crate::config::source_set(expected).map_err(|_| Error::Invalid)?;
        let pool = connect(url, 4).await?;
        let privileged: bool = sqlx::query_scalar("SELECT rolsuper OR rolcreatedb OR rolcreaterole OR has_schema_privilege(current_user,'public','CREATE') FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&pool).await.map_err(unavailable)?;
        if privileged {
            return Err(Error::Unavailable);
        }
        let repo = Self {
            pool,
            tenant: tenant.into(),
            expected: expected.into(),
        };
        repo.ready().await?;
        let mut tx = repo.pool.begin().await.map_err(unavailable)?;
        sqlx::query("INSERT INTO delivery_state(tenant_id) VALUES($1) ON CONFLICT DO NOTHING")
            .bind(&*repo.tenant)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        repo.budget(&mut tx).await?;
        let retained: Vec<String> = sqlx::query_scalar(
            "SELECT service_name FROM sources WHERE tenant_id=$1 AND required LIMIT 17",
        )
        .bind(&*repo.tenant)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        let total: std::collections::BTreeSet<_> =
            retained.iter().chain(repo.expected.iter()).collect();
        if total.len() > 16 {
            return Err(Error::Capacity);
        }
        for service in repo.expected.iter() {
            sqlx::query(
                "INSERT INTO sources(tenant_id,service_name) VALUES($1,$2) ON CONFLICT DO NOTHING",
            )
            .bind(&*repo.tenant)
            .bind(service)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(repo)
    }

    pub async fn ready(&self) -> Result<(), Error> {
        let current: bool =
            sqlx::query_scalar("SELECT value=$1 FROM nds_observability_meta WHERE key='schema'")
                .bind(REQUIRED_SCHEMA_VERSION.to_string())
                .fetch_one(&self.pool)
                .await
                .map_err(unavailable)?;
        if current {
            Ok(())
        } else {
            Err(Error::Unavailable)
        }
    }

    async fn budget(&self, tx: &mut Transaction<'_, Postgres>) -> Result<PgRow, Error> {
        sqlx::query("UPDATE delivery_state SET time_floor=GREATEST(time_floor,clock_timestamp()) WHERE tenant_id=$1 RETURNING *")
            .bind(&*self.tenant)
            .fetch_one(&mut **tx)
            .await
            .map_err(unavailable)
    }

    async fn condition(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        service: &str,
        key: &str,
        condition: Condition,
        now: OffsetDateTime,
    ) -> Result<(), Error> {
        let dedup = format!("{service}:{key}");
        if condition == Condition::Recovered {
            sqlx::query("UPDATE incidents SET state='resolved',revision=revision+1,resolved_at=$3,silenced_until=NULL WHERE tenant_id=$1 AND dedup_key=$2 AND state<>'resolved' AND revision<9007199254740991")
                .bind(&*self.tenant).bind(&dedup).bind(now).execute(&mut **tx).await.map_err(unavailable)?;
            return Ok(());
        }
        let kind = key.split(':').next().ok_or(Error::Invalid)?;
        sqlx::query("INSERT INTO incidents(tenant_id,incident_id,dedup_key,service_name,kind,severity,state,revision,occurrences,first_seen,last_seen) VALUES($1,$2,$3,$4,$5,$7,'open',1,1,$6,$6) ON CONFLICT(tenant_id,dedup_key) DO UPDATE SET state=CASE WHEN incidents.state='resolved' THEN 'open' ELSE incidents.state END, revision=LEAST(incidents.revision+1,9007199254740991), occurrences=LEAST(incidents.occurrences+1,9007199254740991),last_seen=$6,resolved_at=NULL")
            .bind(&*self.tenant).bind(uuid::Uuid::new_v4().to_string()).bind(dedup).bind(service).bind(kind).bind(now).bind(if matches!(kind,"database"|"delivery"|"process"){"critical"}else{"warning"}).execute(&mut **tx).await.map_err(unavailable)?;
        Ok(())
    }

    pub async fn source_watch(&self, stale_seconds: u64) -> Result<(), Error> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let clock = self.budget(&mut tx).await?;
        let now: OffsetDateTime = clock.try_get("time_floor").map_err(unavailable)?;
        let cutoff = now - time::Duration::seconds(stale_seconds as i64);
        let rows = sqlx::query("SELECT * FROM sources WHERE tenant_id=$1 AND service_name=ANY($2)")
            .bind(&*self.tenant)
            .bind(self.expected.as_ref())
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
        for row in rows {
            let created: OffsetDateTime = row.try_get("created_at").map_err(unavailable)?;
            let received: Option<OffsetDateTime> =
                row.try_get("last_received_at").map_err(unavailable)?;
            let event: Option<OffsetDateTime> =
                row.try_get("last_event_at").map_err(unavailable)?;
            if created > cutoff && received.is_none() {
                continue;
            }
            let stale = received.is_none_or(|v| v < cutoff) || event.is_none_or(|v| v < cutoff);
            let service: String = row.try_get("service_name").map_err(unavailable)?;
            // A watchdog tick is not another producer occurrence: only transition once.
            let open:bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM incidents WHERE tenant_id=$1 AND dedup_key=$2 AND state<>'resolved')")
                .bind(&*self.tenant).bind(format!("{service}:source_stale")).fetch_one(&mut *tx).await.map_err(unavailable)?;
            if stale != open {
                self.condition(
                    &mut tx,
                    &service,
                    "source_stale",
                    if stale {
                        Condition::Failed
                    } else {
                        Condition::Recovered
                    },
                    now,
                )
                .await?;
            }
        }
        tx.commit().await.map_err(unavailable)
    }

    pub async fn next_delivery(&self) -> Result<Vec<(Vec<u8>, Vec<u8>, i32)>, Error> {
        let rows = sqlx::query("SELECT event_id,payload,attempts FROM event_receipts WHERE tenant_id=$1 AND payload IS NOT NULL AND next_attempt_at<=now() AND expires_at>now() ORDER BY received_at,event_id LIMIT 100")
            .bind(&*self.tenant).fetch_all(&self.pool).await.map_err(unavailable)?;
        let mut bytes = 0;
        let mut result = Vec::new();
        for row in rows {
            let payload: Vec<u8> = row.try_get("payload").map_err(unavailable)?;
            if bytes + payload.len() > 262_144 && !result.is_empty() {
                break;
            }
            bytes += payload.len();
            result.push((
                row.try_get("event_id").map_err(unavailable)?,
                payload,
                row.try_get("attempts").map_err(unavailable)?,
            ));
        }
        Ok(result)
    }

    pub async fn delivery_result(
        &self,
        entries: &[(Vec<u8>, Vec<u8>, i32)],
        successful: bool,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let clock = self.budget(&mut tx).await?;
        let now: OffsetDateTime = clock.try_get("time_floor").map_err(unavailable)?;
        let mut removed = 0_i64;
        let mut bytes = 0_i64;
        let mut expired = 0_i64;
        for (id, payload, attempts) in entries {
            if successful || *attempts >= 7 {
                let changed=sqlx::query("UPDATE event_receipts SET payload=NULL WHERE tenant_id=$1 AND event_id=$2 AND payload IS NOT NULL").bind(&*self.tenant).bind(id).execute(&mut *tx).await.map_err(unavailable)?.rows_affected();
                if changed > 0 {
                    removed += 1;
                    bytes += payload.len() as i64;
                    if !successful {
                        expired += 1;
                    }
                }
            } else {
                let backoff = 1_i64 << (*attempts as u32).min(6);
                sqlx::query("UPDATE event_receipts SET attempts=attempts+1,next_attempt_at=$3 WHERE tenant_id=$1 AND event_id=$2 AND payload IS NOT NULL")
                    .bind(&*self.tenant).bind(id).bind(now+time::Duration::seconds(backoff)).execute(&mut *tx).await.map_err(unavailable)?;
            }
        }
        sqlx::query("UPDATE delivery_state SET queued_events=queued_events-$2,queued_bytes=queued_bytes-$3,delivered_events=LEAST(delivered_events+$4,9007199254740991),expired_events=LEAST(expired_events+$5,9007199254740991),failures=LEAST(failures+$6,9007199254740991),last_outcome=CASE WHEN $7 THEN 'success' ELSE 'failure' END,last_success_at=CASE WHEN $7 THEN $8 ELSE last_success_at END,last_failure_at=CASE WHEN $7 THEN last_failure_at ELSE $8 END WHERE tenant_id=$1")
            .bind(&*self.tenant).bind(removed).bind(bytes).bind(if successful{removed}else{0}).bind(expired).bind(if successful{0_i64}else{1_i64}).bind(successful).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
        self.condition(
            &mut tx,
            OWN_SERVICE,
            "delivery",
            if successful {
                Condition::Recovered
            } else {
                Condition::Failed
            },
            now,
        )
        .await?;
        tx.commit().await.map_err(unavailable)
    }

    pub async fn cleanup(&self) -> Result<(), Error> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let clock = self.budget(&mut tx).await?;
        let now: OffsetDateTime = clock.try_get("time_floor").map_err(unavailable)?;
        let rows=sqlx::query("SELECT event_id,octet_length(payload) AS bytes FROM event_receipts WHERE tenant_id=$1 AND payload IS NOT NULL AND expires_at<=$2 LIMIT 512")
            .bind(&*self.tenant).bind(now).fetch_all(&mut *tx).await.map_err(unavailable)?;
        let mut bytes = 0_i64;
        for row in &rows {
            let id: Vec<u8> = row.try_get("event_id").map_err(unavailable)?;
            bytes += i64::from(row.try_get::<i32, _>("bytes").map_err(unavailable)?);
            sqlx::query(
                "UPDATE event_receipts SET payload=NULL WHERE tenant_id=$1 AND event_id=$2",
            )
            .bind(&*self.tenant)
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        sqlx::query("UPDATE delivery_state SET queued_events=queued_events-$2,queued_bytes=queued_bytes-$3,expired_events=LEAST(expired_events+$2,9007199254740991) WHERE tenant_id=$1")
            .bind(&*self.tenant).bind(rows.len() as i64).bind(bytes).execute(&mut *tx).await.map_err(unavailable)?;
        sqlx::query("DELETE FROM event_receipts WHERE (tenant_id,event_id) IN (SELECT tenant_id,event_id FROM event_receipts WHERE tenant_id=$1 AND payload IS NULL AND received_at<$2-interval '1 hour' LIMIT 512)")
            .bind(&*self.tenant).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
        sqlx::query("DELETE FROM incident_actions WHERE (tenant_id,operation_id) IN (SELECT tenant_id,operation_id FROM incident_actions WHERE tenant_id=$1 AND created_at<$2-interval '1 hour' LIMIT 512)")
            .bind(&*self.tenant).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }
}

impl Repository for PgRepository {
    async fn ingest(
        &self,
        events: &[AcceptedEvent],
        export: bool,
        ttl_seconds: u64,
    ) -> Result<Receipt, Error> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let budget = self.budget(&mut tx).await?;
        let mut queued: i64 = budget.try_get("queued_events").map_err(unavailable)?;
        let mut bytes: i64 = budget.try_get("queued_bytes").map_err(unavailable)?;
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM event_receipts WHERE tenant_id=$1")
                .bind(&*self.tenant)
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
        let now: OffsetDateTime = budget.try_get("time_floor").map_err(unavailable)?;
        let mut accepted = 0;
        let mut duplicates = 0;
        for event in events {
            let client = matches!(
                event.service.as_str(),
                "nddev-device-sync-client" | "nddev-device-sync-agent"
            );
            if !client
                && event.service != OWN_SERVICE
                && !self.expected.iter().any(|s| s == &event.service)
            {
                return Err(Error::Invalid);
            }
            let inserted=sqlx::query("INSERT INTO event_receipts(tenant_id,event_id,expires_at,payload,received_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
                .bind(&*self.tenant).bind(event.fingerprint.as_slice()).bind(now+time::Duration::seconds(ttl_seconds as i64)).bind(export.then_some(&event.payload)).bind(now).execute(&mut *tx).await.map_err(unavailable)?.rows_affected();
            if inserted == 0 {
                duplicates += 1;
                continue;
            }
            if count + accepted as i64 >= MAX_RECEIPTS {
                return Err(Error::Capacity);
            }
            if export {
                queued += 1;
                bytes += event.payload.len() as i64;
                if queued > MAX_QUEUE || bytes > MAX_QUEUE_BYTES {
                    return Err(Error::Capacity);
                }
            }
            let (service, gap) = if client {
                let device = event.device.as_deref().ok_or(Error::Invalid)?;
                let instance = event.instance.as_deref().ok_or(Error::Invalid)?;
                let sequence = event
                    .sequence
                    .filter(|sequence| *sequence > 0)
                    .ok_or(Error::Invalid)?;
                // Device-bound client sources have no always-on liveness promise.
                // Producer rows separate concurrent app/CLI streams on one device.
                use sha2::{Digest, Sha256};
                let digest = base16ct::lower::encode_string(&Sha256::digest(device.as_bytes()));
                let service = format!("{}.{}", event.service, &digest[..24]);
                let known: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM sources WHERE tenant_id=$1 AND service_name=$2)",
                )
                .bind(&*self.tenant)
                .bind(&service)
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
                if !known {
                    let count: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM sources WHERE tenant_id=$1 AND NOT required",
                    )
                    .bind(&*self.tenant)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(unavailable)?;
                    if count >= 64 {
                        return Err(Error::Capacity);
                    }
                    sqlx::query(
                        "INSERT INTO sources(tenant_id,service_name,required) VALUES($1,$2,false)",
                    )
                    .bind(&*self.tenant)
                    .bind(&service)
                    .execute(&mut *tx)
                    .await
                    .map_err(unavailable)?;
                }
                sqlx::query("DELETE FROM client_producers WHERE tenant_id=$1 AND last_received_at<$2-interval '1 hour'").bind(&*self.tenant).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
                let previous:Option<(String,i64)>=sqlx::query_as("SELECT service_name,last_sequence FROM client_producers WHERE tenant_id=$1 AND instance_id=$2::uuid FOR UPDATE").bind(&*self.tenant).bind(instance).fetch_optional(&mut *tx).await.map_err(unavailable)?;
                if previous
                    .as_ref()
                    .is_some_and(|(stored, _)| stored != &service)
                {
                    return Err(Error::Invalid);
                }
                if previous.is_none() {
                    let count: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM client_producers WHERE tenant_id=$1",
                    )
                    .bind(&*self.tenant)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(unavailable)?;
                    if count >= 4096 {
                        return Err(Error::Capacity);
                    }
                }
                // Offline batches and retries can arrive out of producer order.
                // Fingerprint receipts deduplicate records; the sequence is a
                // high-water mark, not an admission rule for historical logs.
                let advances = previous.as_ref().is_none_or(|(_, last)| sequence > *last);
                let gap = previous.is_some_and(|(_, last)| sequence > last.saturating_add(1));
                sqlx::query("INSERT INTO client_producers(tenant_id,instance_id,service_name,last_sequence,last_received_at) VALUES($1,$2::uuid,$3,$4,$5) ON CONFLICT(tenant_id,instance_id) DO UPDATE SET last_sequence=GREATEST(client_producers.last_sequence,EXCLUDED.last_sequence),last_received_at=EXCLUDED.last_received_at").bind(&*self.tenant).bind(instance).bind(&service).bind(sequence).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
                sqlx::query("UPDATE sources SET last_received_at=$3,last_event_at=GREATEST(last_event_at,$4),gap_observations=LEAST(gap_observations+$5,9007199254740991),gap_active=CASE WHEN $7 THEN $6 ELSE gap_active END WHERE tenant_id=$1 AND service_name=$2").bind(&*self.tenant).bind(&service).bind(now).bind(instant(event.timestamp_ms)?).bind(if gap{1_i64}else{0_i64}).bind(gap).bind(advances).execute(&mut *tx).await.map_err(unavailable)?;
                (service, gap)
            } else {
                let source=sqlx::query("SELECT instance_id::text,last_sequence FROM sources WHERE tenant_id=$1 AND service_name=$2 FOR UPDATE").bind(&*self.tenant).bind(&event.service).fetch_one(&mut *tx).await.map_err(unavailable)?;
                let old_instance: Option<String> =
                    source.try_get("instance_id").map_err(unavailable)?;
                let old_sequence: Option<i64> =
                    source.try_get("last_sequence").map_err(unavailable)?;
                let gap = event.instance.is_some()
                    && old_instance == event.instance
                    && event
                        .sequence
                        .zip(old_sequence)
                        .is_some_and(|(a, b)| a != b.saturating_add(1));
                sqlx::query("UPDATE sources SET last_received_at=$3,last_event_at=$4,instance_id=$5::uuid,last_sequence=$6,gap_observations=LEAST(gap_observations+$7,9007199254740991),gap_active=$8 WHERE tenant_id=$1 AND service_name=$2").bind(&*self.tenant).bind(&event.service).bind(now).bind(instant(event.timestamp_ms)?).bind(&event.instance).bind(event.sequence).bind(if gap{1_i64}else{0_i64}).bind(gap).execute(&mut *tx).await.map_err(unavailable)?;
                (event.service.clone(), gap)
            };
            if gap {
                self.condition(&mut tx, &service, "source_gap", Condition::Failed, now)
                    .await?;
            }
            if let Some((kind, condition)) = domain::condition(
                &event.event_name,
                event.resource.as_deref(),
                event.route.as_deref(),
                event.status,
            ) {
                self.condition(&mut tx, &service, kind, condition, now)
                    .await?;
            }
            accepted += 1;
        }
        sqlx::query(
            "UPDATE delivery_state SET queued_events=$2,queued_bytes=$3 WHERE tenant_id=$1",
        )
        .bind(&*self.tenant)
        .bind(queued)
        .bind(bytes)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Receipt {
            accepted,
            duplicates,
        })
    }

    async fn action(&self, command: ActionCommand) -> Result<Incident, Error> {
        let operation = uuid::Uuid::parse_str(&command.operation).map_err(|_| Error::Invalid)?;
        if operation.get_version_num() != 7 || operation.to_string() != command.operation {
            return Err(Error::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let clock = self.budget(&mut tx).await?;
        if let Some(row) = sqlx::query("SELECT fingerprint,result FROM incident_actions WHERE tenant_id=$1 AND operation_id=$2")
            .bind(&*self.tenant).bind(&command.operation).fetch_optional(&mut *tx).await.map_err(unavailable)? {
            let fingerprint:Vec<u8> = row.try_get("fingerprint").map_err(unavailable)?;
            if fingerprint != command.fingerprint {return Err(Error::Idempotency);}
            let result:sqlx::types::Json<Result<Incident,Error>> = row.try_get("result").map_err(unavailable)?;
            return result.0;
        }
        let (seconds, nanos) = operation.get_timestamp().ok_or(Error::Invalid)?.to_unix();
        let millis = seconds
            .saturating_mul(1000)
            .saturating_add(u64::from(nanos) / 1_000_000);
        let now: OffsetDateTime = clock.try_get("time_floor").map_err(unavailable)?;
        // The time-bearing ID cannot become new again after finite receipt retention.
        if timestamp::millis(now).abs_diff(millis as i64) > 600_000 {
            return Err(Error::Invalid);
        }
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM incident_actions WHERE tenant_id=$1")
                .bind(&*self.tenant)
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
        if count >= 8192 {
            return Err(Error::Capacity);
        }
        let row =
            sqlx::query("SELECT * FROM incidents WHERE tenant_id=$1 AND incident_id=$2 FOR UPDATE")
                .bind(&*self.tenant)
                .bind(&command.incident)
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?;
        let result = match row {
            None => Err(Error::NotFound),
            Some(row) => {
                let mut incident = from_row(&row)?;
                match incident
                    .head
                    .apply(command.expected, command.action, timestamp::millis(now))
                {
                    Err(domain::RuleError::Revision) => Err(Error::Revision),
                    Err(domain::RuleError::Transition) => Err(Error::Transition),
                    Ok(head) => {
                        incident.head = head;
                        if incident.head.state == State::Resolved {
                            incident.resolved_at_ms = Some(timestamp::millis(now));
                        }
                        sqlx::query("UPDATE incidents SET state=$3,revision=$4,silenced_until=$5,resolved_at=$6 WHERE tenant_id=$1 AND incident_id=$2")
                            .bind(&*self.tenant).bind(&incident.id).bind(incident.head.state.as_str()).bind(incident.head.revision).bind(incident.head.silenced_until_ms.map(instant).transpose()?).bind(incident.resolved_at_ms.map(instant).transpose()?).execute(&mut *tx).await.map_err(unavailable)?;
                        Ok(incident)
                    }
                }
            }
        };
        // A rejected authoritative operation is also a stable idempotent result.
        sqlx::query("INSERT INTO incident_actions(tenant_id,operation_id,fingerprint,incident_id,actor_id,action,result,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(&*self.tenant).bind(&command.operation).bind(command.fingerprint.as_slice()).bind(&command.incident).bind(&command.actor).bind(match command.action{Action::Acknowledge=>"acknowledge",Action::Silence{..}=>"silence",Action::Resolve=>"resolve"}).bind(sqlx::types::Json(&result)).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        result
    }

    async fn incidents(&self, after: &str, limit: usize) -> Result<Vec<Incident>, Error> {
        let rows=sqlx::query("SELECT * FROM incidents WHERE tenant_id=$1 AND incident_id>$2 ORDER BY incident_id LIMIT $3")
            .bind(&*self.tenant).bind(after).bind(limit.min(101) as i64).fetch_all(&self.pool).await.map_err(unavailable)?;
        rows.iter().map(from_row).collect()
    }
}

fn from_row(row: &PgRow) -> Result<Incident, Error> {
    let state: String = row.try_get("state").map_err(unavailable)?;
    let state = match state.as_str() {
        "open" => State::Open,
        "acknowledged" => State::Acknowledged,
        "resolved" => State::Resolved,
        _ => return Err(Error::Unavailable),
    };
    let optional_time = |name| {
        row.try_get::<Option<OffsetDateTime>, _>(name)
            .map(|v| v.map(timestamp::millis))
            .map_err(unavailable)
    };
    Ok(Incident {
        id: row.try_get("incident_id").map_err(unavailable)?,
        dedup_key: row.try_get("dedup_key").map_err(unavailable)?,
        service: row.try_get("service_name").map_err(unavailable)?,
        kind: row.try_get("kind").map_err(unavailable)?,
        owner: row.try_get("owner").map_err(unavailable)?,
        severity: row.try_get("severity").map_err(unavailable)?,
        head: Head {
            state,
            revision: row.try_get("revision").map_err(unavailable)?,
            silenced_until_ms: optional_time("silenced_until")?,
        },
        occurrences: row.try_get("occurrences").map_err(unavailable)?,
        first_seen_ms: timestamp::millis(
            row.try_get::<OffsetDateTime, _>("first_seen")
                .map_err(unavailable)?,
        ),
        last_seen_ms: timestamp::millis(
            row.try_get::<OffsetDateTime, _>("last_seen")
                .map_err(unavailable)?,
        ),
        resolved_at_ms: optional_time("resolved_at")?,
    })
}
