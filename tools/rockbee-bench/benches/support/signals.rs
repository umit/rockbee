//! Signal handling stays outside the measured request pipeline.

use super::{OwnedProcesses, Result, owned_processes};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::oneshot;

pub(super) struct ShutdownSignals {
    stop: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl ShutdownSignals {
    pub(super) fn install(processes: Arc<Mutex<OwnedProcesses>>) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let terminate_kind = SignalKind::terminate();
        let interrupt_kind = SignalKind::interrupt();
        // Registration is synchronous on the owner thread. A signal arriving before
        // the listener thread starts polling is queued; no service exists yet.
        let (mut terminate, mut interrupt) = {
            let _entered = runtime.enter();
            (signal(terminate_kind)?, signal(interrupt_kind)?)
        };
        let (stop, stopped) = oneshot::channel();
        let thread = thread::Builder::new()
            .name("benchmark-shutdown".into())
            .spawn(move || {
                let received = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = terminate.recv() => Some(terminate_kind.as_raw_value()),
                        _ = interrupt.recv() => Some(interrupt_kind.as_raw_value()),
                        _ = stopped => None,
                    }
                });
                if let Some(signal) = received {
                    // Keep the lock through reaping and writing cleanup.json. If Drop
                    // is already cleaning up, wait for it before exiting the process.
                    {
                        let mut processes = owned_processes(&processes);
                        // Record cancellation before closing connections. In-flight
                        // request errors then wait for cleanup instead of panicking.
                        processes.interrupted_by = Some(signal);
                        eprintln!(
                            "Benchmark interrupted by signal {signal}; stopping owned services"
                        );
                        processes.cleanup();
                    }
                    std::process::exit(128 + signal);
                }
            })?;
        Ok(Self {
            stop: Some(stop),
            thread: Some(thread),
        })
    }
}

impl Drop for ShutdownSignals {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
