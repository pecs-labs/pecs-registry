//! Lease-backed coordination, independent of the Registry provider contract.
use crate::{RegistryConfig, RegistryError, RegistryResult};
use etcd_client::{Client, Compare, GetOptions, LockOptions, PutOptions, Txn, TxnOp};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LockConfig {
    pub enabled: bool,
    pub prefix: String,
    pub lease_ttl_secs: i64,
    pub acquire_timeout_ms: u64,
    pub rpc_timeout_ms: u64,
}
impl Default for LockConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            prefix: "/pecs/dev/locks".into(),
            lease_ttl_secs: 30,
            acquire_timeout_ms: 3000,
            rpc_timeout_ms: 3000,
        }
    }
}

#[derive(Clone)]
pub struct LockService {
    client: std::sync::Arc<tokio::sync::Mutex<Option<Client>>>,
    connection: RegistryConfig,
    config: LockConfig,
}
impl LockService {
    pub fn new(connection: RegistryConfig, config: LockConfig) -> RegistryResult<Self> {
        if config.lease_ttl_secs < 3
            || config.rpc_timeout_ms == 0
            || config.acquire_timeout_ms == 0
            || config.rpc_timeout_ms >= (config.lease_ttl_secs as u64).saturating_mul(1000) / 3
            || config.prefix.trim_matches('/').is_empty()
        {
            return Err(RegistryError::config(
                "invalid distributed lock configuration",
            ));
        }
        Ok(Self {
            client: Default::default(),
            connection,
            config,
        })
    }
    /// Atomically claim a named occurrence for a bounded retention period.
    /// Returns false when already claimed. This records an attempt, not success:
    /// cancellation/crash after the claim does not permit another attempt until expiry.
    /// Use a separate resource namespace from locks. Ambiguous RPC failures must
    /// skip execution; the claim may have committed and will expire naturally.
    pub async fn claim_once(&self, resource: &str, retention_secs: i64) -> RegistryResult<bool> {
        if retention_secs <= 0 {
            return Err(RegistryError::config("claim retention must be positive"));
        }
        self.validate_resource(resource)?;
        let rpc = Duration::from_millis(self.config.rpc_timeout_ms);
        let mut client = self.connected_client().await?;
        let lease = tokio::time::timeout(rpc, client.lease_grant(retention_secs, None))
            .await
            .map_err(|_| RegistryError::timeout("claim lease grant"))?
            .map_err(|e| RegistryError::driver(e.to_string()))?;
        let key = format!("{}/{}", self.config.prefix.trim_end_matches('/'), resource);
        let txn = Txn::new()
            .when(vec![Compare::version(
                key.clone(),
                etcd_client::CompareOp::Equal,
                0,
            )])
            .and_then(vec![TxnOp::put(
                key,
                "claimed",
                Some(PutOptions::new().with_lease(lease.id())),
            )]);
        let response = tokio::time::timeout(rpc, client.txn(txn))
            .await
            .map_err(|_| RegistryError::timeout("claim transaction"))?
            .map_err(|e| RegistryError::driver(e.to_string()))?;
        if !response.succeeded() {
            // A definite rejected transaction never attached this lease to a key.
            let _ = tokio::time::timeout(rpc, client.lease_revoke(lease.id())).await;
        }
        Ok(response.succeeded())
    }

    fn validate_resource(&self, resource: &str) -> RegistryResult<()> {
        if !self.config.enabled {
            return Err(RegistryError::config("distributed lock disabled"));
        }
        if resource.is_empty()
            || resource
                .split('/')
                .any(|s| s.is_empty() || s == "." || s == "..")
        {
            return Err(RegistryError::config("invalid lock resource"));
        }
        Ok(())
    }

    async fn connected_client(&self) -> RegistryResult<Client> {
        let mut connection = self.connection.clone();
        connection.timeout = (self.config.rpc_timeout_ms / 1000).max(1);
        let mut cached = self.client.lock().await;
        if cached.is_none() {
            *cached = Some(crate::config::connect_etcd(&connection).await?);
        }
        Ok(cached.as_ref().unwrap().clone())
    }

    /// Acquire using the configured wait budget. Connection and lease allocation
    /// have separate RPC deadlines; the wait budget covers Lock and ownership Get.
    pub async fn acquire(&self, resource: &str) -> RegistryResult<LockGuard> {
        self.acquire_with_timeout(
            resource,
            Duration::from_millis(self.config.acquire_timeout_ms),
        )
        .await
    }

    /// Override the wait budget for this call. A short timeout is bounded waiting,
    /// not an atomic try-lock. Cancellation drops the pending guard and revokes its lease.
    pub async fn acquire_with_timeout(
        &self,
        resource: &str,
        wait_timeout: Duration,
    ) -> RegistryResult<LockGuard> {
        if wait_timeout.is_zero() {
            return Err(RegistryError::config("lock wait timeout must be positive"));
        }
        self.validate_resource(resource)?;
        let rpc = Duration::from_millis(self.config.rpc_timeout_ms);
        let mut client = self.connected_client().await?;
        let grant_started = tokio::time::Instant::now();
        let lease = tokio::time::timeout(rpc, client.lease_grant(self.config.lease_ttl_secs, None))
            .await
            .map_err(|_| RegistryError::timeout("lock lease grant"))?
            .map_err(|e| RegistryError::driver(e.to_string()))?;
        let stop = CancellationToken::new();
        let validity = std::sync::Arc::new(LeaseValidity::new(grant_started, lease.ttl())?);
        let task = spawn_lease(
            client.clone(),
            lease.id(),
            lease.ttl(),
            rpc,
            stop.clone(),
            validity.clone(),
        );
        // Guard exists before the first cancellable await following lease allocation.
        let mut guard = LockGuard {
            stop,
            validity,
            task: Some(task),
            token: 0,
        };
        let name = format!("{}/{}", self.config.prefix.trim_end_matches('/'), resource);
        let acquire = async {
            let response = client
                .lock(name, Some(LockOptions::new().with_lease(lease.id())))
                .await
                .map_err(|e| RegistryError::driver(e.to_string()))?;
            let key = response.key().to_vec();
            let result = tokio::time::timeout(rpc, client.get(key, Some(GetOptions::new())))
                .await
                .map_err(|_| RegistryError::timeout("lock ownership read"))?
                .map_err(|e| RegistryError::driver(e.to_string()))?;
            let kv = result
                .kvs()
                .first()
                .ok_or_else(|| RegistryError::driver("lock ownership lost"))?;
            if kv.lease() != lease.id() {
                return Err(RegistryError::driver("lock lease mismatch"));
            }
            Ok(kv.create_revision())
        };
        guard.token = tokio::select! {
            biased;
            _ = guard.lost() => return Err(RegistryError::driver("lock lease lost")),
            result = tokio::time::timeout(wait_timeout, acquire) =>
                result.map_err(|_| RegistryError::timeout("lock acquisition"))??,
        };
        if guard.is_lost() {
            return Err(RegistryError::driver("lock lease lost"));
        }
        Ok(guard)
    }
}

/// Not Clone: one acquisition owns exactly one lease. Drop schedules cleanup;
/// release awaits server confirmation. Lost guards must never resume protected work.
#[must_use = "dropping the guard releases the distributed lock"]
pub struct LockGuard {
    stop: CancellationToken,
    validity: std::sync::Arc<LeaseValidity>,
    task: Option<JoinHandle<RegistryResult<()>>>,
    token: i64,
}
impl LockGuard {
    pub fn fencing_token(&self) -> i64 {
        self.token
    }
    /// Resolves on renewal failure or local confirmation expiry, including after a pause.
    pub async fn lost(&self) {
        loop {
            if self.is_lost() {
                return;
            }
            let deadline = self.validity.deadline();
            tokio::select! {
                _ = self.validity.lost.cancelled() => return,
                _ = tokio::time::sleep_until(deadline) => {},
            }
        }
    }
    pub fn is_lost(&self) -> bool {
        self.validity.is_lost()
    }
    pub async fn release(mut self) -> RegistryResult<()> {
        self.stop.cancel();
        self.task
            .take()
            .unwrap()
            .await
            .map_err(|e| RegistryError::other(e.to_string()))?
    }
}
impl Drop for LockGuard {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

// A confirmation cannot revive a locally expired owner. Use the request send time,
// rather than the response arrival time, so buffered replies do not extend validity.
struct LeaseValidity {
    deadline: std::sync::Mutex<tokio::time::Instant>,
    lost: CancellationToken,
}
impl LeaseValidity {
    fn new(sent: tokio::time::Instant, ttl: i64) -> RegistryResult<Self> {
        Ok(Self {
            deadline: std::sync::Mutex::new(Self::expires_at(sent, ttl)?),
            lost: CancellationToken::new(),
        })
    }
    fn expires_at(sent: tokio::time::Instant, ttl: i64) -> RegistryResult<tokio::time::Instant> {
        if ttl <= 0 {
            return Err(RegistryError::driver("lock lease expired"));
        }
        sent.checked_add(Duration::from_secs(
            (ttl.saturating_mul(2) / 3).max(1) as u64
        ))
        .ok_or_else(|| RegistryError::config("lock TTL exceeds supported clock range"))
    }
    fn deadline(&self) -> tokio::time::Instant {
        *self.deadline.lock().unwrap()
    }
    fn is_lost(&self) -> bool {
        if tokio::time::Instant::now() >= self.deadline() {
            self.lost.cancel();
        }
        self.lost.is_cancelled()
    }
    fn confirm(&self, sent: tokio::time::Instant, ttl: i64) -> RegistryResult<()> {
        let next = Self::expires_at(sent, ttl)?;
        if self.is_lost() || tokio::time::Instant::now() >= next {
            self.lost.cancel();
            return Err(RegistryError::driver("late lock renewal acknowledgement"));
        }
        *self.deadline.lock().unwrap() = next;
        Ok(())
    }
}

fn spawn_lease(
    mut client: Client,
    id: i64,
    ttl: i64,
    rpc: Duration,
    stop: CancellationToken,
    validity: std::sync::Arc<LeaseValidity>,
) -> JoinHandle<RegistryResult<()>> {
    tokio::spawn(async move {
        let renew = async {
            let setup_sent = tokio::time::Instant::now();
            let setup_deadline = validity.deadline().min(setup_sent + rpc);
            let (mut keeper, mut stream) =
                tokio::time::timeout_at(setup_deadline, client.lease_keep_alive(id))
                    .await
                    .map_err(|_| RegistryError::timeout("lock keepalive setup"))?
                    .map_err(|e| RegistryError::driver(e.to_string()))?;
            // etcd-client consumes the first KeepAlive confirmation during setup.
            validity.confirm(setup_sent, ttl)?;
            let mut interval = tokio::time::interval(Duration::from_secs((ttl / 3).max(1) as u64));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut pending = None;
            loop {
                if validity.is_lost() {
                    return Err(RegistryError::driver("lock confirmation expired"));
                }
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(validity.deadline()) => return Err::<(), _>(RegistryError::timeout("lock renewal acknowledgement")),
                    reply = stream.message() => {
                        let reply = reply.map_err(|e| RegistryError::driver(e.to_string()))?
                            .filter(|r| r.id() == id && r.ttl() > 0)
                            .ok_or_else(|| RegistryError::driver("lock lease expired"))?;
                        let sent = pending.take().ok_or_else(|| RegistryError::driver("unexpected lock renewal acknowledgement"))?;
                        validity.confirm(sent, reply.ttl())?;
                    }
                    _ = interval.tick(), if pending.is_none() => {
                        let sent = tokio::time::Instant::now();
                        tokio::time::timeout_at(validity.deadline().min(sent + rpc), keeper.keep_alive()).await
                            .map_err(|_| RegistryError::timeout("lock renewal"))?
                            .map_err(|e| RegistryError::driver(e.to_string()))?;
                        pending = Some(sent);
                    }
                }
            }
        };
        tokio::select! {
            biased;
            _ = stop.cancelled() => {},
            _ = validity.lost.cancelled() => {},
            result = renew => { if let Err(error) = result { tracing::warn!(%error, "Distributed lock lost"); } },
        }
        validity.lost.cancel();
        // Revoke removes the ownership key AND any pending Lock waiter for this lease.
        let result = tokio::time::timeout(rpc, client.lease_revoke(id))
            .await
            .map_err(|_| RegistryError::timeout("lock release"))
            .and_then(|result| result.map_err(|e| RegistryError::driver(e.to_string())));
        if let Err(error) = &result {
            tracing::warn!(%error, lease_id = id, "Lock cleanup failed; waiting for lease expiry");
        }
        result.map(|_| ())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn delayed_confirmation_cannot_revive_expired_owner() {
        let sent = tokio::time::Instant::now();
        let validity = LeaseValidity::new(sent, 30).unwrap();
        tokio::time::advance(Duration::from_secs(21)).await;
        assert!(validity.confirm(sent, 30).is_err());
        assert!(validity.is_lost());
        assert!(validity.confirm(tokio::time::Instant::now(), 30).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn confirmation_validity_starts_when_request_was_sent() {
        let start = tokio::time::Instant::now();
        let validity = LeaseValidity::new(start, 30).unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        let sent = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_secs(5)).await;
        validity.confirm(sent, 30).unwrap();
        assert_eq!(validity.deadline(), start + Duration::from_secs(30));
        tokio::time::advance(Duration::from_secs(16)).await;
        // The guard detects expiry even before the renewal task gets scheduled.
        assert!(validity.is_lost());
    }
}
