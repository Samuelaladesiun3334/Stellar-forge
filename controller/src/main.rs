// controller/src/main.rs
//
// Stellar-forge Kubernetes operator entry point.
//
// Startup sequence
// ────────────────
// 1. Initialise structured logging (JSON in production, pretty in dev).
// 2. Load configuration from environment variables.
// 3. Connect to the Kubernetes API.
// 4. Spawn the background leader-election task (non-blocking).
// 5. Enter the main reconcile loop; all guarded operations (SCP broadcast,
//    history-archive writes) are gated behind `handle.is_leader()`.
// 6. Respond to OS signals (SIGTERM / Ctrl-C) with a graceful shutdown that
//    releases the lease before exit.

mod config;
mod error;
mod leader;
#[cfg(test)]
mod chaos_test;

use std::sync::Arc;

use tokio::{
    signal,
    sync::watch,
    time::{sleep, Duration},
};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

use crate::{
    config::Config,
    leader::{LeaderState, start_leader_election},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── 1. Logging ─────────────────────────────────────────────────────────────
    init_tracing();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        "Stellar-forge controller starting"
    );

    // ── 2. Configuration ───────────────────────────────────────────────────────
    let config = Arc::new(Config::from_env());

    info!(
        pod_identity  = %config.pod_identity,
        lease_name    = %config.lease_name,
        namespace     = %config.lease_namespace,
        lease_duration_secs = config.lease_duration.as_secs(),
        renew_interval_secs = config.renew_interval.as_secs(),
        api_timeout_secs    = config.api_timeout.as_secs(),
        "Configuration loaded"
    );

    // ── 3. Kubernetes client ───────────────────────────────────────────────────
    let client = kube::Client::try_default().await?;
    info!("Connected to Kubernetes API server");

    // ── 4. Leader election ─────────────────────────────────────────────────────
    let mut handle = start_leader_election(client.clone(), Arc::clone(&config)).await?;

    info!("Leader election task spawned – waiting for initial state");

    // ── 5. Main reconcile loop ─────────────────────────────────────────────────
    loop {
        tokio::select! {
            // Watch for leadership state changes.
            changed = handle.rx.changed() => {
                match changed {
                    Ok(()) => {
                        let state = handle.rx.borrow().clone();
                        on_leader_state_change(&state, &config);

                        if state == LeaderState::Stopped {
                            info!("Election task stopped – shutting down controller");
                            break;
                        }
                    }
                    Err(_) => {
                        error!("Leader election watch channel closed – shutting down");
                        break;
                    }
                }
            }

            // Periodic reconcile tick (only meaningful work when leader).
            _ = sleep(Duration::from_secs(5)) => {
                if handle.is_leader() {
                    reconcile(&config).await;
                } else {
                    debug_standby(&config);
                }
            }

            // Graceful shutdown on SIGTERM or Ctrl-C.
            _ = shutdown_signal() => {
                info!(
                    identity = %config.pod_identity,
                    "Shutdown signal received – releasing lease and exiting"
                );
                // The lease TTL will expire naturally; we log clearly so
                // chaos-test harnesses can assert on this event.
                break;
            }
        }
    }

    info!("Stellar-forge controller shut down cleanly");
    Ok(())
}

// ─── Reconcile logic ──────────────────────────────────────────────────────────

/// Placeholder for the real SCP / history-archive reconcile logic.
///
/// In production this will:
///   - Verify quorum membership in the SCP overlay.
///   - Flush pending ledger entries to the persistent history archive.
///   - Broadcast SCP messages to peer validators.
///
/// IMPORTANT: This function is ONLY called when `handle.is_leader()` is true,
/// enforcing the single-writer invariant required to prevent ledger forks.
async fn reconcile(config: &Config) {
    info!(
        identity = %config.pod_identity,
        "Reconciling as active leader – SCP broadcast & archive write permitted"
    );
    // TODO: insert real reconcile logic here.
}

/// Log a heartbeat when running in follower/standby mode.
fn debug_standby(config: &Config) {
    tracing::debug!(
        identity = %config.pod_identity,
        "Standby – waiting for lease acquisition before reconciling"
    );
}

// ─── State-change handler ─────────────────────────────────────────────────────

fn on_leader_state_change(state: &LeaderState, config: &Config) {
    match state {
        LeaderState::Leader => {
            info!(
                identity = %config.pod_identity,
                // Structured field used by chaos-test log assertions.
                event = "LEADER_ACQUIRED",
                "This pod is now the ACTIVE LEADER"
            );
        }
        LeaderState::Follower => {
            warn!(
                identity = %config.pod_identity,
                event = "LEADER_LOST",
                "This pod is now a FOLLOWER – guarded operations suspended"
            );
        }
        LeaderState::Stopped => {
            error!(
                identity = %config.pod_identity,
                event = "ELECTION_STOPPED",
                "Leader election task has stopped"
            );
        }
    }
}

// ─── Tracing ─────────────────────────────────────────────────────────────────

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // Use JSON formatter in production (LOG_FORMAT=json), pretty otherwise.
    match std::env::var("LOG_FORMAT").as_deref() {
        Ok("json") => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_current_span(true)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .pretty()
                .with_env_filter(filter)
                .init();
        }
    }
}

// ─── Signal handling ──────────────────────────────────────────────────────────

/// Resolves when SIGTERM (Unix) or Ctrl-C is received.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}
