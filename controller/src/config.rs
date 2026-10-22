// controller/src/config.rs
// Runtime configuration sourced from environment variables.

use std::time::Duration;

/// All tunable parameters for the leader-election subsystem and controller lifecycle.
#[derive(Debug, Clone)]
pub struct Config {
    /// Kubernetes namespace in which the Lease object is created.
    pub lease_namespace: String,

    /// Name of the Lease object (must be unique per operator deployment).
    pub lease_name: String,

    /// Identity of this pod (typically the pod name injected via Downward API).
    pub pod_identity: String,

    /// How long the lease is valid before another candidate may steal it.
    /// Maps directly to `spec.leaseDurationSeconds`.
    pub lease_duration: Duration,

    /// How often the current leader renews the lease.
    /// Should be significantly shorter than `lease_duration` to absorb transient API errors.
    pub renew_interval: Duration,

    /// Maximum time to wait for a single lease acquire/renew API call.
    /// Prevents a hung kube-apiserver from blocking the election loop.
    pub api_timeout: Duration,

    /// Maximum jitter added to `renew_interval` to avoid thundering-herd renewal
    /// storms when many replicas come up simultaneously after a cluster event.
    pub renew_jitter_max: Duration,

    /// Retry back-off cap when attempting to acquire the lease from a failed state.
    pub acquire_retry_cap: Duration,
}

impl Config {
    /// Build a `Config` from environment variables, falling back to safe defaults.
    ///
    /// | Variable                    | Default                   |
    /// |-----------------------------|---------------------------|
    /// | `LEASE_NAMESPACE`           | `default`                 |
    /// | `LEASE_NAME`                | `stellar-forge-leader`    |
    /// | `POD_NAME`                  | `stellar-forge-0`         |
    /// | `LEASE_DURATION_SECS`       | `30`                      |
    /// | `RENEW_INTERVAL_SECS`       | `10`                      |
    /// | `API_TIMEOUT_SECS`          | `5`                       |
    /// | `RENEW_JITTER_MAX_MS`       | `2000`                    |
    /// | `ACQUIRE_RETRY_CAP_SECS`    | `15`                      |
    pub fn from_env() -> Self {
        let env_u64 = |key: &str, default: u64| -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };

        Self {
            lease_namespace: std::env::var("LEASE_NAMESPACE")
                .unwrap_or_else(|_| "default".into()),
            lease_name: std::env::var("LEASE_NAME")
                .unwrap_or_else(|_| "stellar-forge-leader".into()),
            pod_identity: std::env::var("POD_NAME")
                .unwrap_or_else(|_| "stellar-forge-0".into()),
            lease_duration: Duration::from_secs(env_u64("LEASE_DURATION_SECS", 30)),
            renew_interval: Duration::from_secs(env_u64("RENEW_INTERVAL_SECS", 10)),
            api_timeout: Duration::from_secs(env_u64("API_TIMEOUT_SECS", 5)),
            renew_jitter_max: Duration::from_millis(env_u64("RENEW_JITTER_MAX_MS", 2_000)),
            acquire_retry_cap: Duration::from_secs(env_u64("ACQUIRE_RETRY_CAP_SECS", 15)),
        }
    }
}
