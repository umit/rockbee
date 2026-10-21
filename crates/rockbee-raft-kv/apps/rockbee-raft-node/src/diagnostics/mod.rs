//! Bounded, opt-in incident evidence. This is not the product metrics API.
//!
//! Compile with `--features diagnostics` and set `ROCKBEE_RAFT_DIAGNOSTICS=1`.
//! Hot paths only try to append to a preallocated buffer; they never write logs.
//! The CLI emits retained events after owned node shutdown. Missing events are
//! counted. A dropped span means caller cancellation, not necessarily a timeout.

#[cfg(feature = "diagnostics")]
mod recording;
#[cfg(feature = "diagnostics")]
pub use recording::Session;
#[cfg(feature = "diagnostics")]
pub(crate) use recording::{Span, enabled};

#[cfg(not(feature = "diagnostics"))]
pub(crate) struct Span;

#[cfg(not(feature = "diagnostics"))]
impl Span {
    pub(crate) fn start(_: u64, _: u64, _: Option<u64>, _: &'static str, _: bool) -> Self {
        Self
    }
    pub(crate) fn phase(&mut self, _: &'static str) {}
    pub(crate) fn success(self) {}
    pub(crate) fn failure(self, _: &impl std::fmt::Display) {}
}
