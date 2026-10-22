// controller/src/error.rs
// Centralised error types for the Stellar-forge controller.

use thiserror::Error;

/// Top-level controller errors.
#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Leader-election error: {0}")]
    LeaderElection(#[from] LeaderError),

    #[error("Serialisation error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Timeout while {operation}")]
    Timeout { operation: &'static str },
}

/// Errors that originate specifically inside the lease / leader-election subsystem.
#[derive(Debug, Error)]
pub enum LeaderError {
    #[error("Failed to acquire Kubernetes lease '{lease_name}': {source}")]
    AcquireFailed {
        lease_name: String,
        #[source]
        source: kube::Error,
    },

    #[error("Failed to renew Kubernetes lease '{lease_name}': {source}")]
    RenewFailed {
        lease_name: String,
        #[source]
        source: kube::Error,
    },

    #[error("Lease '{0}' was lost to another holder")]
    LeaseLost(String),

    #[error("Lease channel closed unexpectedly")]
    ChannelClosed,

    #[error("Timed out waiting for lease acquisition (ttl={ttl_secs}s)")]
    AcquireTimeout { ttl_secs: u64 },
}
