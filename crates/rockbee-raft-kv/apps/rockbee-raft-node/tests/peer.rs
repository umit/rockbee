//! Public-contract tests for the private peer module.
//!
//! Message-shape tests live beside private wire DTOs. This file checks the API a
//! node runtime imports: one shared resource owner validates limits up front.
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use rockbee_raft_node::rpc::{PeerError, PeerIdentity, PeerLimits, PeerResources, PeerServer};

#[test]
fn public_resource_constructor_rejects_an_invalid_snapshot_budget() {
    let limits = PeerLimits {
        max_snapshot_chunk_bytes: 0,
        ..PeerLimits::default()
    };
    assert!(matches!(
        PeerResources::new(limits),
        Err(PeerError::InvalidLimit {
            name: "max_snapshot_chunk_bytes"
        })
    ));
}

#[tokio::test]
async fn closing_server_releases_an_idle_peer_before_it_sends_a_header() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind owned test listener");
    let address = listener.local_addr().expect("read owned listener address");
    let resources = Arc::new(PeerResources::new(PeerLimits::default()).expect("valid test limits"));
    let mut server = PeerServer::start(
        listener,
        PeerIdentity {
            cluster_id: rockbee_raft_protocol::ClusterId::new([1; 16]).expect("test cluster"),
            node_id: rockbee_raft_protocol::NodeId::new(1).expect("test node"),
        },
        BTreeMap::new(),
        resources,
    );
    let _idle_peer = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect idle peer");
    tokio::time::timeout(Duration::from_secs(1), server.close())
        .await
        .expect("close must not wait for an idle peer forever");
}
