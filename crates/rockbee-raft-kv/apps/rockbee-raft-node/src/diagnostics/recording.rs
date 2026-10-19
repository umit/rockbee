use std::{
    fmt,
    io::Write,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const EVENT_LIMIT: usize = 8192;
const SLOW: Duration = Duration::from_millis(25);
static RECORDER: OnceLock<Recorder> = OnceLock::new();

struct Recorder {
    started: Instant,
    unix_us: u128,
    events: Mutex<Vec<Event>>,
    omitted: AtomicUsize,
}

impl Recorder {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            unix_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros(),
            events: Mutex::new(Vec::with_capacity(EVENT_LIMIT)),
            omitted: AtomicUsize::new(0),
        }
    }

    fn record(&self, event: Event) {
        // Diagnostic contention must not stall peer RPCs or a held storage gate.
        if let Ok(mut events) = self.events.try_lock()
            && events.len() < EVENT_LIMIT
        {
            events.push(event);
            return;
        }
        self.omitted.fetch_add(1, Ordering::Relaxed);
    }
}

struct Event {
    start_us: u128,
    elapsed_us: u128,
    phase_us: u128,
    node: u64,
    group: u64,
    target: Option<u64>,
    operation: &'static str,
    phase: &'static str,
    outcome: &'static str,
    detail: String,
}

/// CLI-owned recorder lifetime; its drop runs after node resources are closed.
pub struct Session;

impl Session {
    pub fn from_environment() -> Self {
        if std::env::var("ROCKBEE_RAFT_DIAGNOSTICS").as_deref() == Ok("1") {
            RECORDER.get_or_init(Recorder::new);
        }
        Self
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let Some(recorder) = RECORDER.get() else {
            return;
        };
        let Ok(events) = recorder.events.lock() else {
            return;
        };
        let mut output = std::io::stderr().lock();
        let _ = writeln!(
            output,
            "RB_DIAG_HEADER\tstart_unix_us\telapsed_us\tphase_us\tnode\tgroup\ttarget\toperation\tphase\toutcome\tdetail"
        );
        for event in events.iter() {
            let _ = writeln!(
                output,
                "RB_DIAG\t{}\t{}\t{}\t{}\t{}\t{:?}\t{}\t{}\t{}\t{}",
                event.start_us,
                event.elapsed_us,
                event.phase_us,
                event.node,
                event.group,
                event.target,
                event.operation,
                event.phase,
                event.outcome,
                event.detail
            );
        }
        let _ = writeln!(
            output,
            "RB_DIAG_END\tretained={}\tomitted={}",
            events.len(),
            recorder.omitted.load(Ordering::Relaxed)
        );
    }
}

pub(crate) fn enabled() -> bool {
    RECORDER.get().is_some()
}

struct ActiveSpan {
    recorder: &'static Recorder,
    started: Instant,
    phase_started: Instant,
    event: Event,
    record_slow: bool,
}

/// A diagnostic span follows real ownership, including native work after cancellation.
pub(crate) struct Span(Option<ActiveSpan>);

impl Span {
    pub(crate) fn start(
        node: u64,
        group: u64,
        target: Option<u64>,
        operation: &'static str,
        record_slow: bool,
    ) -> Self {
        Self(RECORDER.get().map(|recorder| {
            let started = Instant::now();
            ActiveSpan {
                recorder,
                started,
                phase_started: started,
                record_slow,
                event: Event {
                    start_us: recorder.unix_us
                        + started.duration_since(recorder.started).as_micros(),
                    elapsed_us: 0,
                    phase_us: 0,
                    node,
                    group,
                    target,
                    operation,
                    phase: "start",
                    outcome: "caller_cancelled",
                    detail: String::new(),
                },
            }
        }))
    }

    pub(crate) fn phase(&mut self, phase: &'static str) {
        if let Some(active) = &mut self.0 {
            active.event.phase = phase;
            active.phase_started = Instant::now();
        }
    }

    pub(crate) fn success(mut self) {
        if let Some(active) = self.0.take()
            && active.record_slow
            && active.started.elapsed() >= SLOW
        {
            Self::finish(active, "slow_success", &"");
        }
    }

    pub(crate) fn failure(mut self, error: &impl fmt::Display) {
        if let Some(active) = self.0.take() {
            Self::finish(active, "error", error);
        }
    }

    pub(crate) fn note(mut self, detail: &impl fmt::Display) {
        if let Some(active) = self.0.take() {
            Self::finish(active, "observed", detail);
        }
    }

    fn finish(mut active: ActiveSpan, outcome: &'static str, detail: &impl fmt::Display) {
        active.event.elapsed_us = active.started.elapsed().as_micros();
        active.event.phase_us = active.phase_started.elapsed().as_micros();
        active.event.outcome = outcome;
        // Bound retained diagnostic strings independently of transport payloads.
        active.event.detail = detail
            .to_string()
            .chars()
            .take(512)
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect();
        active.recorder.record(active.event);
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if let Some(active) = self.0.take() {
            Self::finish(active, "caller_cancelled", &"");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> Event {
        Event {
            start_us: 0,
            elapsed_us: 0,
            phase_us: 0,
            node: 1,
            group: 1,
            target: None,
            operation: "test",
            phase: "wait",
            outcome: "caller_cancelled",
            detail: String::new(),
        }
    }

    #[test]
    fn diagnostic_pressure_drops_events_without_waiting_or_growing() {
        let recorder = Recorder::new();
        let guard = recorder.events.lock().unwrap();
        recorder.record(event());
        assert_eq!(recorder.omitted.load(Ordering::Relaxed), 1);
        drop(guard);
        for _ in 0..=EVENT_LIMIT {
            recorder.record(event());
        }
        assert_eq!(recorder.events.lock().unwrap().len(), EVENT_LIMIT);
        assert_eq!(recorder.omitted.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn aborted_caller_preserves_last_phase_without_inventing_timeout_cause() {
        let recorder: &'static Recorder = Box::leak(Box::new(Recorder::new()));
        let (entered, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let now = Instant::now();
            let mut span = Span(Some(ActiveSpan {
                recorder,
                started: now,
                phase_started: now,
                event: event(),
                record_slow: false,
            }));
            span.phase("connect");
            entered.send(()).unwrap();
            std::future::pending::<()>().await;
            span.success();
        });
        ready.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].phase, "connect");
        assert_eq!(events[0].outcome, "caller_cancelled");
        assert!(events[0].detail.is_empty());
    }
}
