# Stellar-forge

Kubernetes operator for the Stellar validator network with **k8s-lease based leader election**.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Stellar-forge Pod                  │
│                                                     │
│  ┌──────────────┐        ┌────────────────────────┐ │
│  │  main.rs     │◄──────►│  leader.rs             │ │
│  │  (reconcile  │        │  (election loop task)  │ │
│  │   loop)      │        └────────────┬───────────┘ │
│  └──────────────┘                     │             │
│                                       │ watch::Rx   │
│                                       ▼             │
│                               LeaderState::Leader   │
│                               LeaderState::Follower │
└───────────────────────────────────────┬─────────────┘
                                        │
                              Kubernetes API Server
                                        │
                              coordination.k8s.io/v1
                                   Lease object
```

### Leader Election Flow

1. On startup every pod races to **create** the `stellar-forge-leader` Lease.
2. The winner sets `spec.holderIdentity` to its pod name and becomes **Leader**.
3. The leader renews every `RENEW_INTERVAL_SECS` (default 10 s) with jitter.
4. If a pod misses renewals and the TTL (`LEASE_DURATION_SECS`, default 30 s)
   expires, any follower may **steal** the lease and become the new leader.
5. Only the pod holding the active lease is permitted to write to the
   persistent history archive or broadcast SCP messages — enforcing a strict
   single-writer invariant that prevents ledger forks.

## Configuration

| Environment Variable     | Default                 | Description                                  |
|--------------------------|-------------------------|----------------------------------------------|
| `LEASE_NAMESPACE`        | `default`               | Namespace for the Lease object               |
| `LEASE_NAME`             | `stellar-forge-leader`  | Name of the Lease object                     |
| `POD_NAME`               | `stellar-forge-0`       | Identity of this pod (Downward API)          |
| `LEASE_DURATION_SECS`    | `30`                    | Lease TTL in seconds                         |
| `RENEW_INTERVAL_SECS`    | `10`                    | Renewal frequency in seconds                 |
| `API_TIMEOUT_SECS`       | `5`                     | Per-call kube-apiserver timeout              |
| `RENEW_JITTER_MAX_MS`    | `2000`                  | Max jitter added to renewal interval (ms)    |
| `ACQUIRE_RETRY_CAP_SECS` | `15`                    | Back-off cap for acquire retries             |
| `LOG_FORMAT`             | pretty                  | Set to `json` for structured JSON logs       |

## Running Tests

```bash
cargo test -p stellar-forge-controller -- --nocapture
```

## Chaos Testing

The chaos test suite in `controller/src/chaos_test.rs` validates:

- Fresh leases are not incorrectly expired
- Stale leases (TTL exceeded) are detected and stolen by standby pods
- Split-brain prevention: valid leases held by another pod are not stolen
- Failover window is bounded within the configured TTL

For end-to-end chaos tests against a live cluster, run the `chaos-runner` CI job
which randomly kills the leader pod and asserts a secondary acquires within TTL.
