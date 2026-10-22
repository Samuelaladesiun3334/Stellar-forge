// controller/src/leader.rs
//
// K8s-lease based leader election for the Stellar-forge operator.
//
// Design goals
// ────────────
// 1. Never block the main controller thread – all lease I/O runs in a
//    dedicated Tokio task and communicates via a watch channel.
// 2. Strict timeouts on every kube-apiserver call to prevent deadlocks.
// 3. Jittered renewal intervals to avoid thundering-herd after cluster events.
// 4. Clear, structured log events at every state transition so chaos-test
//    log scrapers can assert on exact failover timings.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::MicroTime,
};
use kube::{
    api::{Api, ObjectMeta, Patch, PatchParams, PostParams},
    Client,
};
use rand::Rng;
use tokio::{
    sync::watch,
    time::{sleep, timeout},
};
use tracing::{debug, error, info, warn};

use crate::{
    config::Config,
    error::{LeaderError, ControllerError},
};

// ─── Public types ────────────────────────────────────────────────────────────

/// The current leadership state broadcast to all watchers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderState {
    /// This pod holds the active lease and may write to the archive / SCP.
    Leader,
    /// Another pod holds the lease (or the lease is unclaimed and being contested).
    Follower,
    /// The election task has exited – controller should shut down gracefully.
    Stopped,
}

/// Handle returned to the main controller.
/// Dropping this handle signals the background election task to stop.
pub struct LeaderHandle {
    /// Subscribe to leadership changes without any allocation.
    pub rx: watch::Receiver<LeaderState>,
    /// Abort handle – dropped when the controller shuts down.
    _task: tokio::task::JoinHandle<()>,
}

impl LeaderHandle {
    /// Returns `true` if this pod is currently the active leader.
    #[inline]
    pub fn is_leader(&self) -> bool {
        *self.rx.borrow() == LeaderState::Leader
    }
}

// ─── Core election loop ───────────────────────────────────────────────────────

/// Spawn the background leader-election task and return a `LeaderHandle`.
///
/// The task runs for the lifetime of the process.  It will:
///   - Try to create or steal an expired lease.
///   - Once leader, renew the lease every `config.renew_interval` ± jitter.
///   - If renewal fails (pod restarted, apiserver unreachable), transition to
///     `Follower` immediately so the main loop stops guarded operations.
pub async fn start_leader_election(
    client: Client,
    config: Arc<Config>,
) -> Result<LeaderHandle, ControllerError> {
    let (tx, rx) = watch::channel(LeaderState::Follower);

    let task = tokio::spawn(election_loop(client, config, tx));

    Ok(LeaderHandle { rx, _task: task })
}

// ─── Internal implementation ──────────────────────────────────────────────────

async fn election_loop(client: Client, config: Arc<Config>, tx: watch::Sender<LeaderState>) {
    let api: Api<Lease> = Api::namespaced(client.clone(), &config.lease_namespace);

    info!(
        identity = %config.pod_identity,
        lease = %config.lease_name,
        namespace = %config.lease_namespace,
        lease_duration_secs = config.lease_duration.as_secs(),
        renew_interval_secs = config.renew_interval.as_secs(),
        "Leader election loop starting"
    );

    loop {
        // ── Phase 1: acquire ─────────────────────────────────────────────────
        match acquire_lease(&api, &config).await {
            Ok(()) => {
                info!(
                    identity = %config.pod_identity,
                    lease = %config.lease_name,
                    "Lease acquired – transitioning to LEADER"
                );
                let _ = tx.send(LeaderState::Leader);
            }
            Err(e) => {
                warn!(error = %e, "Failed to acquire lease, retrying");
                sleep(backoff_duration(&config)).await;
                continue;
            }
        }

        // ── Phase 2: hold (renew loop) ────────────────────────────────────────
        loop {
            let jitter = rand::thread_rng()
                .gen_range(Duration::ZERO..config.renew_jitter_max);
            sleep(config.renew_interval + jitter).await;

            match renew_lease(&api, &config).await {
                Ok(true) => {
                    debug!(
                        identity = %config.pod_identity,
                        lease = %config.lease_name,
                        "Lease renewed successfully"
                    );
                }
                Ok(false) => {
                    // Another pod has stolen the lease – we lost leadership.
                    error!(
                        identity = %config.pod_identity,
                        lease = %config.lease_name,
                        "Lease lost to another holder – transitioning to FOLLOWER"
                    );
                    let _ = tx.send(LeaderState::Follower);
                    break; // fall back to acquire loop
                }
                Err(e) => {
                    error!(
                        error = %e,
                        identity = %config.pod_identity,
                        "Lease renewal error – transitioning to FOLLOWER"
                    );
                    let _ = tx.send(LeaderState::Follower);
                    break; // fall back to acquire loop
                }
            }
        }

        // Small cooldown before retrying acquisition to avoid a hot spin.
        sleep(Duration::from_secs(2)).await;
    }
}

// ─── Lease helpers ────────────────────────────────────────────────────────────

/// Attempt to create a brand-new lease or steal one whose duration has expired.
///
/// Returns `Ok(())` if this pod now owns the lease.
async fn acquire_lease(api: &Api<Lease>, config: &Config) -> Result<(), LeaderError> {
    let now_micro = micro_time_now();

    // First, see whether a lease already exists.
    let existing = timeout(config.api_timeout, api.get_opt(&config.lease_name))
        .await
        .map_err(|_| LeaderError::AcquireTimeout {
            ttl_secs: config.api_timeout.as_secs(),
        })?
        .map_err(|e| LeaderError::AcquireFailed {
            lease_name: config.lease_name.clone(),
            source: e,
        })?;

    match existing {
        None => {
            // No lease exists – create one and immediately become leader.
            create_lease(api, config, &now_micro).await
        }
        Some(lease) => {
            // Check whether the existing lease is expired or held by us already.
            if lease_is_expired(&lease, config) || held_by_us(&lease, config) {
                steal_lease(api, config, &lease, &now_micro).await
            } else {
                // Lease is valid and held by someone else.
                Err(LeaderError::LeaseLost(config.lease_name.clone()))
            }
        }
    }
}

async fn create_lease(
    api: &Api<Lease>,
    config: &Config,
    now: &MicroTime,
) -> Result<(), LeaderError> {
    let lease = build_lease_object(config, now);
    timeout(
        config.api_timeout,
        api.create(&PostParams::default(), &lease),
    )
    .await
    .map_err(|_| LeaderError::AcquireTimeout {
        ttl_secs: config.api_timeout.as_secs(),
    })?
    .map_err(|e| LeaderError::AcquireFailed {
        lease_name: config.lease_name.clone(),
        source: e,
    })?;

    info!(
        identity = %config.pod_identity,
        lease = %config.lease_name,
        "Created new lease"
    );
    Ok(())
}

async fn steal_lease(
    api: &Api<Lease>,
    config: &Config,
    existing: &Lease,
    now: &MicroTime,
) -> Result<(), LeaderError> {
    let previous_holder = existing
        .spec
        .as_ref()
        .and_then(|s| s.holder_identity.as_deref())
        .unwrap_or("<none>");

    info!(
        identity = %config.pod_identity,
        previous_holder = previous_holder,
        lease = %config.lease_name,
        "Stealing expired lease"
    );

    let patch = build_lease_patch(config, now);
    let pp = PatchParams::apply("stellar-forge-controller").force();

    timeout(
        config.api_timeout,
        api.patch(&config.lease_name, &pp, &Patch::Apply(&patch)),
    )
    .await
    .map_err(|_| LeaderError::AcquireTimeout {
        ttl_secs: config.api_timeout.as_secs(),
    })?
    .map_err(|e| LeaderError::AcquireFailed {
        lease_name: config.lease_name.clone(),
        source: e,
    })?;

    Ok(())
}

/// Renew the lease held by this pod.
///
/// Returns `Ok(true)`  – renewed successfully.
/// Returns `Ok(false)` – another pod now holds the lease (we lost it).
/// Returns `Err`       – API error preventing renewal.
async fn renew_lease(api: &Api<Lease>, config: &Config) -> Result<bool, LeaderError> {
    let current = timeout(config.api_timeout, api.get(&config.lease_name))
        .await
        .map_err(|_| LeaderError::AcquireTimeout {
            ttl_secs: config.api_timeout.as_secs(),
        })?
        .map_err(|e| LeaderError::RenewFailed {
            lease_name: config.lease_name.clone(),
            source: e,
        })?;

    if !held_by_us(&current, config) {
        return Ok(false);
    }

    let now = micro_time_now();
    let patch = build_lease_patch(config, &now);
    let pp = PatchParams::apply("stellar-forge-controller").force();

    timeout(
        config.api_timeout,
        api.patch(&config.lease_name, &pp, &Patch::Apply(&patch)),
    )
    .await
    .map_err(|_| LeaderError::AcquireTimeout {
        ttl_secs: config.api_timeout.as_secs(),
    })?
    .map_err(|e| LeaderError::RenewFailed {
        lease_name: config.lease_name.clone(),
        source: e,
    })?;

    Ok(true)
}

// ─── Lease object builders ────────────────────────────────────────────────────

fn build_lease_object(config: &Config, now: &MicroTime) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(config.lease_name.clone()),
            namespace: Some(config.lease_namespace.clone()),
            ..Default::default()
        },
        spec: Some(build_lease_spec(config, now)),
    }
}

fn build_lease_patch(config: &Config, now: &MicroTime) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(config.lease_name.clone()),
            namespace: Some(config.lease_namespace.clone()),
            ..Default::default()
        },
        spec: Some(build_lease_spec(config, now)),
    }
}

fn build_lease_spec(config: &Config, now: &MicroTime) -> LeaseSpec {
    LeaseSpec {
        holder_identity: Some(config.pod_identity.clone()),
        lease_duration_seconds: Some(config.lease_duration.as_secs() as i32),
        acquire_time: Some(now.clone()),
        renew_time: Some(now.clone()),
        lease_transitions: Some(1),
    }
}

// ─── Utility helpers (also re-exported for unit tests) ───────────────────────

/// Public sub-module so `chaos_test.rs` can import helpers without making them
/// fully `pub` on the crate root.
pub mod leader_helpers {
    pub use super::{held_by_us, lease_is_expired};
}

/// Returns `true` when the lease's `renewTime + leaseDuration` is in the past.
pub fn lease_is_expired(lease: &Lease, config: &Config) -> bool {
    let spec = match lease.spec.as_ref() {
        Some(s) => s,
        None => return true, // malformed lease – treat as expired
    };

    let renew_time = match spec.renew_time.as_ref() {
        Some(t) => t.0,
        None => return true,
    };

    let duration_secs = spec
        .lease_duration_seconds
        .unwrap_or(config.lease_duration.as_secs() as i32) as i64;

    let expires_at = renew_time + chrono::Duration::seconds(duration_secs);
    Utc::now() >= expires_at
}

/// Returns `true` if the lease is already held by this pod.
pub fn held_by_us(lease: &Lease, config: &Config) -> bool {
    lease
        .spec
        .as_ref()
        .and_then(|s| s.holder_identity.as_deref())
        .map(|h| h == config.pod_identity)
        .unwrap_or(false)
}

/// Returns the current wall-clock time as a Kubernetes `MicroTime`.
fn micro_time_now() -> MicroTime {
    MicroTime(Utc::now())
}

/// Exponential back-off with cap for acquire retries.
fn backoff_duration(config: &Config) -> Duration {
    let base = Duration::from_secs(1);
    let jitter = rand::thread_rng().gen_range(Duration::ZERO..Duration::from_millis(500));
    (base + jitter).min(config.acquire_retry_cap)
}
