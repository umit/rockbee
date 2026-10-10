use rockbee_protocol::Result;

pub async fn wait_for_shutdown(ready: impl FnOnce()) -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        // Both registrations must precede readiness, even if the supervisor
        // signals us before the receive futures below are first polled.
        ready();
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {}
        }
    }
    #[cfg(windows)]
    {
        let mut interrupt = tokio::signal::windows::ctrl_c()?;
        ready();
        interrupt.recv().await;
    }
    Ok(())
}

#[cfg(all(test, unix))]
#[path = "tests/shutdown.rs"]
mod tests;
