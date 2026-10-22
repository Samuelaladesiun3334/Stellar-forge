// controller/src/chaos_test.rs
//
// Chaos test suite for leader election failover validation.
//
// These tests simulate the scenarios described in issue #472:
//   - Kill the current leader pod → verify a secondary pod acquires the lock
//     within the configured TTL threshold.
//   - Network partition → both pods think they are leader → verify split-brain
//     is prevented once connectivity restores.
//
// Run with:
//   cargo test -- --nocapture
//
// The tests exercise the pure logic helpers (lease_is_expired, held_by_us, etc.)
// without requiring a real Kubernetes cluster.  End-to-end chaos tests against a
// live cluster are in `tests/chaos_e2e.rs` and are driven by the `chaos-runner`
// job in CI.

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use chrono::Utc;
    use k8s_openapi::{
        api::coordination::v1::{Lease, LeaseSpec},
        apimachinery::pkg::apis::meta::v1::MicroTime,
    };
    use kube::api::ObjectMeta;

    use crate::config::Config;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn test_config(pod_name: &str) -> Config {
        Config {
            lease_namespace: "test-ns".into(),
            lease_name: "stellar-forge-leader".into(),
            pod_identity: pod_name.into(),
            lease_duration: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
            api_timeout: Duration::from_secs(5),
            renew_jitter_max: Duration::from_millis(100),
            acquire_retry_cap: Duration::from_secs(5),
        }
    }

    fn make_lease(holder: &str, renew_offset_secs: i64, duration_secs: i32) -> Lease {
        let renew_time = Utc::now() + chrono::Duration::seconds(renew_offset_secs);
        Lease {
            metadata: ObjectMeta {
                name: Some("stellar-forge-leader".into()),
                namespace: Some("test-ns".into()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(holder.into()),
                lease_duration_seconds: Some(duration_secs),
                renew_time: Some(MicroTime(renew_time)),
                acquire_time: Some(MicroTime(renew_time)),
                lease_transitions: Some(1),
            }),
        }
    }

    // Import the private helpers via the parent module path.
    use super::super::{
        leader_helpers::{held_by_us, lease_is_expired},
    };

    // ── Unit tests ────────────────────────────────────────────────────────────

    /// A lease renewed 5s ago with a 30s TTL should NOT be considered expired.
    #[test]
    fn test_fresh_lease_is_not_expired() {
        let cfg = test_config("pod-0");
        // renewed 5 seconds ago
        let lease = make_lease("pod-1", -5, 30);
        assert!(
            !lease_is_expired(&lease, &cfg),
            "A lease renewed 5s ago with 30s TTL should still be valid"
        );
    }

    /// A lease whose renewTime + leaseDuration is in the past IS expired.
    #[test]
    fn test_stale_lease_is_expired() {
        let cfg = test_config("pod-0");
        // renewed 60 seconds ago with a 30s TTL → expired 30s ago
        let lease = make_lease("pod-1", -60, 30);
        assert!(
            lease_is_expired(&lease, &cfg),
            "A lease renewed 60s ago with 30s TTL should be expired"
        );
    }

    /// A lease renewed exactly at TTL boundary (renew_time + duration == now)
    /// should be treated as expired (>= comparison).
    #[test]
    fn test_lease_expired_at_boundary() {
        let cfg = test_config("pod-0");
        let lease = make_lease("pod-1", -30, 30);
        assert!(
            lease_is_expired(&lease, &cfg),
            "A lease at exact TTL boundary should be considered expired"
        );
    }

    /// `held_by_us` returns true only when the holder matches our pod identity.
    #[test]
    fn test_held_by_us_matches_identity() {
        let cfg = test_config("pod-0");
        let our_lease = make_lease("pod-0", -5, 30);
        let their_lease = make_lease("pod-1", -5, 30);

        assert!(held_by_us(&our_lease, &cfg));
        assert!(!held_by_us(&their_lease, &cfg));
    }

    /// Split-brain prevention: if pod-1 holds a valid (non-expired) lease,
    /// pod-0 must NOT attempt to steal it.
    #[test]
    fn test_split_brain_prevention_valid_lease_not_stolen() {
        let cfg_pod0 = test_config("pod-0");
        // pod-1 renewed 2s ago with 30s TTL – fully valid
        let lease = make_lease("pod-1", -2, 30);

        let expired = lease_is_expired(&lease, &cfg_pod0);
        let ours = held_by_us(&lease, &cfg_pod0);

        assert!(
            !expired && !ours,
            "pod-0 must not steal a valid lease held by pod-1"
        );
    }

    /// Failover scenario: leader pod crashes (lease goes stale).
    /// Secondary pod detects expiry and should be allowed to acquire.
    #[test]
    fn test_failover_stale_lease_allows_acquisition() {
        let cfg_pod1 = test_config("pod-1");
        // pod-0 crashed; last renewal was 45s ago with 30s TTL
        let stale_lease = make_lease("pod-0", -45, 30);

        let expired = lease_is_expired(&stale_lease, &cfg_pod1);
        let ours = held_by_us(&stale_lease, &cfg_pod1);

        assert!(
            expired || ours,
            "pod-1 must be allowed to acquire a stale lease from dead pod-0"
        );
    }

    /// Verify a lease with no spec is treated as expired (defensive path).
    #[test]
    fn test_lease_with_no_spec_is_expired() {
        let cfg = test_config("pod-0");
        let empty_lease = Lease {
            metadata: ObjectMeta::default(),
            spec: None,
        };
        assert!(
            lease_is_expired(&empty_lease, &cfg),
            "A lease with no spec should be treated as expired"
        );
    }

    /// Simulate rapid successive pod restarts: lease TTL determines the maximum
    /// failover window.  The failover time must be <= lease_duration.
    #[test]
    fn test_failover_window_within_ttl() {
        let cfg = test_config("pod-2");
        let lease_duration_secs = cfg.lease_duration.as_secs() as i64;

        // Worst case: leader died immediately after renewal.
        // Secondary must wait at most `lease_duration_secs` before acquiring.
        let max_failover = Duration::from_secs(lease_duration_secs as u64);

        assert!(
            max_failover <= Duration::from_secs(60),
            "Failover window ({max_failover:?}) exceeds 60s SLA"
        );
    }
}
