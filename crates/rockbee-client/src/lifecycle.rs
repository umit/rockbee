//! Shared admission gate and complete public-operation lifetimes.
use crate::Result;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Default)]
struct State {
    closing: bool,
    operations: usize,
}

pub(crate) struct Lifecycle {
    state: watch::Sender<State>,
}

impl Lifecycle {
    pub(crate) fn new() -> Self {
        Self {
            state: watch::channel(State::default()).0,
        }
    }

    pub(crate) fn start(self: &Arc<Self>) -> Result<Operation> {
        // Updating under watch's lock makes accepting an operation atomic with
        // closing the gate. No lock is held over admission or network awaits.
        let accepted = self.state.send_if_modified(|state| {
            if state.closing {
                return false;
            }
            state.operations += 1;
            true
        });
        if !accepted {
            return Err("client is shut down".into());
        }
        Ok(Operation(Arc::clone(self)))
    }

    pub(crate) async fn drain(&self) {
        let mut state = self.state.subscribe();
        self.state.send_modify(|state| state.closing = true);
        // This sender lives at least as long as the drain call. Unlike a
        // pending-map count, this includes queued PUTs and result delivery.
        state
            .wait_for(|state| state.operations == 0)
            .await
            .expect("client lifecycle sender outlives its drain receiver");
    }
}

pub(crate) struct Operation(Arc<Lifecycle>);

impl Drop for Operation {
    fn drop(&mut self) {
        self.0.state.send_modify(|state| state.operations -= 1);
    }
}
