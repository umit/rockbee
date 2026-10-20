#![allow(clippy::unwrap_used)]

use super::*;
use rockbee_raft_protocol::{ShardConfig, routing::NodeEndpoint, transport::LinkConfig};

fn directory() -> ClusterDirectory {
    ClusterDirectory::initial(
        ClusterId::new([22; 16]).unwrap(),
        ShardConfig::new(2, 91).unwrap(),
        (1..=3)
            .map(|id| NodeEndpoint {
                node_id: NodeId::new(id).unwrap(),
                peer_address: format!("127.0.0.1:{}", 2000 + id * 2).parse().unwrap(),
                data_address: format!("127.0.0.1:{}", 2001 + id * 2).parse().unwrap(),
            })
            .collect(),
    )
    .unwrap()
}

fn moved(current: &ClusterDirectory) -> ClusterDirectory {
    let mut next = current.clone();
    next.revision += 1;
    next.nodes.push(NodeEndpoint {
        node_id: NodeId::new(4).unwrap(),
        peer_address: "127.0.0.1:2010".parse().unwrap(),
        data_address: "127.0.0.1:2011".parse().unwrap(),
    });
    next.placements[1].voters[2] = NodeId::new(4).unwrap();
    next
}

fn gateway(directory: ClusterDirectory) -> Gateway {
    Gateway {
        cluster_id: directory.cluster_id,
        directory: RwLock::new(Arc::new(directory)),
        refresh_generation: AtomicU64::new(0),
        refresh: Mutex::new(Ok(())),
        backend_config: LinkConfig::default(),
        links: Mutex::new(BTreeMap::new()),
        metrics: GatewayMetrics::default(),
    }
}

#[tokio::test]
async fn publication_is_monotonic_coherent_and_keeps_c_for_other_groups() {
    let initial = directory();
    let next = moved(&initial);
    let gateway = gateway(initial.clone());
    let old_snapshot = gateway.directory_snapshot().await;
    assert_eq!(
        gateway
            .publish_directory(next.clone())
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        &next
    );
    // An in-flight operation retains the complete old snapshot after the swap.
    assert_eq!(old_snapshot.as_ref(), &initial);
    assert!(gateway.publish_directory(initial).await.unwrap().is_none());
    let current = gateway.directory_snapshot().await;
    assert_eq!(current.revision, 2);
    assert_eq!(
        current.voters(GroupId::new(0)),
        old_snapshot.voters(GroupId::new(0))
    );
    assert_eq!(
        current.voters(GroupId::new(2)),
        old_snapshot.voters(GroupId::new(2))
    );
    assert!(current.node(NodeId::new(3).unwrap()).is_some());
    let same = gateway
        .publish_directory(next.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&same, &current));
    let mut conflicting = next;
    conflicting.placements[1].voters.reverse();
    assert_eq!(
        gateway.publish_directory(conflicting).await,
        Err(RefreshFailure::EqualRevisionConflict)
    );
    assert!(Arc::ptr_eq(&gateway.directory_snapshot().await, &current));
}

#[test]
fn updates_reject_cluster_hash_metadata_endpoint_and_catalog_conflicts() {
    let initial = directory();
    let next = moved(&initial);
    assert_eq!(
        validate_directory_update(&initial, &next),
        Ok(RevisionOrder::Greater)
    );
    let mut invalid = Vec::new();
    let mut changed = next.clone();
    changed.cluster_id = ClusterId::new([23; 16]).unwrap();
    invalid.push(changed);
    let mut changed = next.clone();
    changed.shard_config = ShardConfig::new(2, 92).unwrap();
    invalid.push(changed);
    let mut changed = next.clone();
    changed.placements[0].voters[2] = NodeId::new(4).unwrap();
    invalid.push(changed);
    let mut changed = next.clone();
    changed.nodes[0].data_address = "127.0.0.1:9000".parse().unwrap();
    invalid.push(changed);
    let mut changed = next.clone();
    changed.nodes[0].peer_address = "127.0.0.1:9001".parse().unwrap();
    invalid.push(changed);
    let mut changed = next.clone();
    changed.placements[0].voters.retain(|node| node.get() != 3);
    changed.placements[2].voters.retain(|node| node.get() != 3);
    changed.nodes.retain(|node| node.node_id.get() != 3);
    invalid.push(changed);
    let mut changed = next;
    for id in 5..=17 {
        changed.nodes.push(NodeEndpoint {
            node_id: NodeId::new(id).unwrap(),
            peer_address: format!("127.0.0.1:{}", 2000 + id * 2).parse().unwrap(),
            data_address: format!("127.0.0.1:{}", 2001 + id * 2).parse().unwrap(),
        });
    }
    invalid.push(changed);
    for invalid in invalid {
        assert_eq!(
            validate_directory_update(&initial, &invalid),
            Err(RefreshFailure::InvalidDirectory)
        );
    }
}

#[tokio::test]
async fn unknown_destinations_cannot_allocate_slots_and_catalog_cannot_exceed_sixteen() {
    let initial = directory();
    let gateway = gateway(initial.clone());
    let deadline = Instant::now() + std::time::Duration::from_secs(1);
    for id in 4..=100 {
        assert!(
            gateway
                .backend_link(&initial, NodeId::new(id).unwrap(), deadline)
                .await
                .is_err()
        );
    }
    assert!(gateway.links.lock().await.is_empty());
    let mut largest = initial;
    largest.revision += 1;
    for id in 4..=16 {
        largest.nodes.push(NodeEndpoint {
            node_id: NodeId::new(id).unwrap(),
            peer_address: format!("127.0.0.1:{}", 2000 + id * 2).parse().unwrap(),
            data_address: format!("127.0.0.1:{}", 2001 + id * 2).parse().unwrap(),
        });
    }
    gateway.publish_directory(largest).await.unwrap();
    let snapshot = gateway.directory_snapshot().await;
    // Zero connection budgets exercise validated slot creation without dialing.
    for node in &snapshot.nodes {
        assert!(
            gateway
                .backend_link(&snapshot, node.node_id, Instant::now())
                .await
                .is_err()
        );
    }
    assert_eq!(gateway.links.lock().await.len(), 16);
    assert!(
        gateway
            .backend_link(&snapshot, NodeId::new(17).unwrap(), deadline)
            .await
            .is_err()
    );
    assert_eq!(gateway.links.lock().await.len(), 16);
}

struct DirectoryOnly(ClusterDirectory);

impl RequestHandler for DirectoryOnly {
    type Request = BackendRequest;
    type Response = BackendResponse;

    fn handle(
        &self,
        request: BackendRequest,
        _deadline: Instant,
    ) -> impl Future<Output = BackendResponse> + Send {
        let directory = self.0.clone();
        async move {
            BackendResponse {
                cluster_id: request.cluster_id,
                node_id: request.target_node,
                reply: BackendReply::Directory(Ok(directory)),
            }
        }
    }
}

#[tokio::test]
async fn discovery_seed_outside_catalog_or_at_wrong_address_never_creates_a_slot() {
    let initial = directory();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut server = Server::start(
        listener,
        Default::default(),
        Arc::new(DirectoryOnly(initial.clone())),
    )
    .unwrap();
    for id in [1, 99] {
        let config = GatewayConfig {
            cluster_id: initial.cluster_id,
            expected_shard_config: initial.shard_config,
            seeds: vec![crate::SeedNode {
                node_id: NodeId::new(id).unwrap(),
                address,
            }],
            listen_address: "127.0.0.1:9020".parse().unwrap(),
            backend_link: LinkConfig::default(),
            server: Default::default(),
            startup_timeout: std::time::Duration::from_secs(1),
        };
        let gateway = Gateway::connect(&config, Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        assert!(gateway.links.lock().await.is_empty());
        gateway.close().await;
    }
    server.close().await;
}
