//! Gateway-only backend client. Routing policy stays outside the shared TCP link.
use super::config::UpstreamConfig;
use rockbee_protocol::Result;
use rockbee_protocol::transport::Link;
use rockbee_protocol::{Request, Response};
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub(super) struct Upstream {
    link: Arc<Link>,
    owner: watch::Sender<()>,
}
impl Upstream {
    pub(crate) fn new(address: String, config: UpstreamConfig) -> Self {
        let (owner, _) = watch::channel(());
        Self {
            link: Arc::new(Link::new(
                address,
                config.max_in_flight(),
                config.max_pending(),
            )),
            owner,
        }
    }
    pub(crate) async fn exchange(&self, request: &Request, deadline: Instant) -> Result<Response> {
        self.link.exchange(request, deadline).await
    }

    pub(crate) fn start_get(
        &self,
        key: &[u8],
        deadline: Instant,
    ) -> Result<JoinHandle<Result<Response>>> {
        let admission = self.link.try_admit()?;
        let request = Request::Get { key: key.to_vec() };
        let link = Arc::clone(&self.link);
        let mut owner = self.owner.subscribe();
        Ok(tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = owner.changed() => Err("upstream owner dropped".into()),
                result = link.exchange_admitted(&request, deadline, admission) => result,
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockbee_protocol::RequestEnvelope;
    use rockbee_protocol::transport::{FrameReader, read_frame};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    #[tokio::test]
    async fn owner_drop_cancels_admitted_and_waiting_gets_without_sending_a_third() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let (first_seen, first_ready) = tokio::sync::oneshot::channel();
        let (no_second, no_second_ready) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut input = FrameReader::new(stream);
            let first =
                RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
            assert!(matches!(first.request, Request::Get { ref key } if key == b"active"));
            first_seen.send(()).unwrap();
            assert!(
                timeout(Duration::from_millis(100), read_frame(&mut input))
                    .await
                    .is_err()
            );
            no_second.send(()).unwrap();
            assert!(
                timeout(Duration::from_secs(1), read_frame(&mut input))
                    .await
                    .unwrap()
                    .unwrap()
                    .is_none()
            );
        });

        let upstream = Upstream::new(address, UpstreamConfig::new(1, 1)?);
        let first = upstream.start_get(b"active", Instant::now() + Duration::from_secs(5))?;
        let second = upstream.start_get(b"waiting", Instant::now() + Duration::from_secs(5))?;
        timeout(Duration::from_secs(1), first_ready).await??;
        assert!(
            upstream
                .start_get(b"rejected", Instant::now() + Duration::from_secs(5))
                .is_err()
        );
        timeout(Duration::from_secs(1), no_second_ready).await??;
        drop(upstream);

        assert!(timeout(Duration::from_secs(1), first).await??.is_err());
        assert!(timeout(Duration::from_secs(1), second).await??.is_err());
        timeout(Duration::from_secs(1), peer).await??;
        Ok(())
    }
}
