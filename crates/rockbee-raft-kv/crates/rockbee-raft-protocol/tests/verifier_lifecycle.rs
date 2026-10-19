use std::{
    future::{Future, poll_fn},
    sync::{Arc, Condvar, Mutex, OnceLock},
    task::Poll,
    time::Duration,
};

use rockbee_raft_protocol::{
    ProtocolError, RequestEnvelope, ResponseEnvelope, VERSION, WireRequest, decode_frame,
    encode_frame,
    transport::{Delivery, Link, LinkConfig},
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    time::{Instant, timeout},
};

#[derive(Serialize, Deserialize)]
struct PausedRequest;

#[derive(Serialize, Deserialize)]
struct PausedReply(u8);

struct ValidationGate {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

impl ValidationGate {
    fn release(&self) {
        *self.released.lock().expect("gate lock") = true;
        self.wake.notify_all();
    }
}

// A failing assertion must also release the intentionally blocked reader.
struct ReleaseOnDrop(Arc<ValidationGate>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

static VALIDATION_GATE: OnceLock<Arc<ValidationGate>> = OnceLock::new();

impl WireRequest<PausedReply> for PausedRequest {
    type Expected = ();

    fn validate(&self) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn retained_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(size_of::<Self>())
    }
    fn expected_response(&self) -> Self::Expected {}
    fn validate_response(_: (), response: &PausedReply) -> Result<(), ProtocolError> {
        assert_eq!(response.0, 7);
        let gate = VALIDATION_GATE.get().expect("installed gate");
        gate.entered.notify_one();
        let mut released = gate.released.lock().expect("gate lock");
        while !*released {
            released = gate.wake.wait(released).expect("gate wait");
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_then_reclose_retains_all_worker_join_ownership() {
    timeout(Duration::from_secs(10), async {
        let gate = Arc::new(ValidationGate {
            entered: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
        });
        let _release_on_failure = ReleaseOnDrop(Arc::clone(&gate));
        assert!(VALIDATION_GATE.set(Arc::clone(&gate)).is_ok());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("owned port");
        let address = listener.local_addr().expect("address");
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let length = stream.read_u32().await.expect("header");
            let mut frame = length.to_be_bytes().to_vec();
            frame.resize(usize::try_from(length).expect("fixture length") + 4, 0);
            stream.read_exact(&mut frame[4..]).await.expect("body");
            let request: RequestEnvelope<PausedRequest> = decode_frame(&frame).expect("request");
            let reply = encode_frame(&ResponseEnvelope {
                version: VERSION,
                request_id: request.request_id,
                response: PausedReply(7),
            })
            .expect("reply");
            stream.write_all(&reply).await.expect("write reply");
            let mut rest = Vec::new();
            stream.read_to_end(&mut rest).await.expect("closed socket");
            assert!(rest.is_empty());
        });
        let link = Link::<PausedRequest, PausedReply>::connect(
            address,
            LinkConfig::default(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("connect");
        let pending = link
            .try_submit(PausedRequest, Instant::now() + Duration::from_secs(60))
            .expect("submit");
        gate.entered.notified().await;
        let mut first_close = Box::pin(link.close());
        poll_fn(|cx| match first_close.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(()) => panic!("close returned while reader was held"),
        })
        .await;
        drop(first_close);
        let failure = pending.await.err().expect("close retires handed request");
        assert_eq!(failure.delivery, Delivery::MayHaveBeenSent);

        let mut second_close = Box::pin(link.close());
        poll_fn(|cx| match second_close.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(()) => {
                panic!("cancelled close detached a worker instead of retaining ownership")
            }
        })
        .await;
        gate.release();
        second_close.await;
        peer.await.expect("peer joins");
    })
    .await
    .expect("bounded cancel-close test");
}
