//! Cluster integration fixtures grouped by the boundary they exercise.

#[cfg(all(test, feature = "diagnostics"))]
mod group_isolation;
#[path = "../../../../rockbee-raft-gateway/tests/support/history.rs"]
// Shared by node fixtures; the gateway suite exercises the full model.
#[allow(dead_code)]
mod history_model;
mod public_reply_loss;
