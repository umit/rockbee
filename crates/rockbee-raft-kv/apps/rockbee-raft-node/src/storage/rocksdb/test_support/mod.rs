//! Instance-owned storage fault and scheduling hooks, compiled only for tests.
#[cfg(feature = "diagnostics")]
pub(super) mod append_failure;
pub(super) mod joint_apply_pause;
