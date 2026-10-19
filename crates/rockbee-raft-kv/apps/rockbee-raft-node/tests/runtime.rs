//! Real in-process nodes with separate RocksDB directories and owned TCP ports.
#![allow(clippy::unwrap_used, clippy::panic)]

use bytes::Bytes;
use rockbee_raft_client::{ClientConfig, RockBeeRaftClient};
use rockbee_raft_gateway::{GatewayConfig, GatewayRuntime, SeedNode};
use rockbee_raft_node::{cluster::NodeRuntime, config::NodeConfig, storage::OpenMode};
use rockbee_raft_protocol::{
    ClusterId, GroupId, KvRequest, KvResponse, MutationOutcome, NodeId, ShardConfig,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    routing::{ClusterDirectory, NodeEndpoint},
    transport::server::ServerConfig,
    transport::{Link, LinkConfig},
};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    net::TcpListener,
    time::{Instant, sleep, timeout},
};

struct Cluster {
    directory: ClusterDirectory,
    nodes: Vec<Option<NodeRuntime>>,
    roots: Vec<tempfile::TempDir>,
    links: Vec<Link<BackendRequest, BackendResponse>>,
}

impl Cluster {
    async fn start() -> Self {
        let mut listeners = Vec::new();
        let mut endpoints = Vec::new();
        for id in 1..=3 {
            let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let data = TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.push(NodeEndpoint {
                node_id: NodeId::new(id).unwrap(),
                peer_address: peer.local_addr().unwrap(),
                data_address: data.local_addr().unwrap(),
            });
            listeners.push((peer, data));
        }
        let directory = ClusterDirectory::initial(
            ClusterId::new([7; 16]).unwrap(),
            ShardConfig::new(4, 0).unwrap(),
            endpoints,
        )
        .unwrap();
        let mut nodes = Vec::new();
        let mut roots = Vec::new();
        for (index, (peer, data)) in listeners.into_iter().enumerate() {
            let root = tempfile::tempdir().unwrap();
            let config = NodeConfig::new(
                directory.clone(),
                directory.nodes[index].node_id,
                root.path().join("db"),
                OpenMode::CreateNew,
            )
            .unwrap();
            nodes.push(Some(
                NodeRuntime::start_with_listeners(config, peer, data)
                    .await
                    .unwrap(),
            ));
            roots.push(root);
        }
        nodes[0]
            .as_ref()
            .unwrap()
            .initialize_groups()
            .await
            .unwrap();
        let mut links = Vec::new();
        for endpoint in &directory.nodes {
            links.push(
                Link::connect(endpoint.data_address, LinkConfig::default(), deadline())
                    .await
                    .unwrap(),
            );
        }
        Self {
            directory,
            nodes,
            roots,
            links,
        }
    }

    async fn leader(&self, group: GroupId) -> usize {
        timeout(Duration::from_secs(15), async {
            loop {
                for node in self.nodes.iter().flatten() {
                    if let Some(id) = node.leader_hint(group) {
                        let index = usize::try_from(id.get() - 1).unwrap();
                        if self.nodes.get(index).is_some_and(Option::is_some)
                            && self.nodes[index].as_ref().unwrap().leader_hint(group) == Some(id)
                        {
                            return index;
                        }
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a surviving quorum must elect a leader")
    }

    async fn request(&self, node: usize, operation: BackendOperation) -> BackendReply {
        self.links[node]
            .try_submit(
                BackendRequest {
                    cluster_id: self.directory.cluster_id,
                    target_node: self.directory.nodes[node].node_id,
                    operation,
                },
                deadline(),
            )
            .unwrap()
            .await
            .unwrap()
            .reply
    }

    async fn kv(&self, group: GroupId, request: KvRequest) -> KvResponse {
        let leader = self.leader(group).await;
        match self
            .request(
                leader,
                BackendOperation::Kv {
                    group_id: group,
                    request,
                },
            )
            .await
        {
            BackendReply::Kv { response, .. } => response,
            other => panic!("unexpected backend response: {other:?}"),
        }
    }

    async fn close(mut self) {
        for link in &self.links {
            link.close().await;
        }
        for node in self.nodes.iter_mut().flatten() {
            node.close().await;
        }
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

fn keys(directory: &ClusterDirectory) -> BTreeMap<GroupId, Bytes> {
    let mut keys = BTreeMap::new();
    for id in 0..100 {
        let key = Bytes::from(format!("product/{id}"));
        keys.entry(directory.shard_config.group_for_key(&key).unwrap())
            .or_insert(key);
        if keys.len() == 4 {
            break;
        }
    }
    assert_eq!(keys.len(), 4);
    keys
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_groups_replicate_and_committed_values_survive_leader_stop_and_restart() {
    let mut cluster = Cluster::start().await;
    let keys = keys(&cluster.directory);
    for (group, key) in &keys {
        assert_eq!(
            cluster
                .kv(
                    *group,
                    KvRequest::Put {
                        key: key.clone(),
                        value: Bytes::from_static(b"in-stock")
                    }
                )
                .await,
            KvResponse::Mutation(MutationOutcome::Committed)
        );
        assert_eq!(
            cluster
                .kv(*group, KvRequest::Get { key: key.clone() })
                .await,
            KvResponse::Value(Some(Bytes::from_static(b"in-stock")))
        );
    }
    let stopped = cluster.leader(GroupId::new(1)).await;
    cluster.links[stopped].close().await;
    let mut stopped_node = cluster.nodes[stopped].take().unwrap();
    stopped_node.close().await;
    drop(stopped_node);
    for (group, key) in &keys {
        assert_eq!(
            cluster
                .kv(*group, KvRequest::Get { key: key.clone() })
                .await,
            KvResponse::Value(Some(Bytes::from_static(b"in-stock")))
        );
    }
    let key = keys.get(&GroupId::new(1)).unwrap();
    assert_eq!(
        cluster
            .kv(GroupId::new(1), KvRequest::Delete { key: key.clone() })
            .await,
        KvResponse::Mutation(MutationOutcome::Committed)
    );
    assert_eq!(
        cluster
            .kv(GroupId::new(1), KvRequest::Get { key: key.clone() })
            .await,
        KvResponse::Value(None)
    );

    let config = NodeConfig::new(
        cluster.directory.clone(),
        cluster.directory.nodes[stopped].node_id,
        cluster.roots[stopped].path().join("db"),
        OpenMode::OpenExisting,
    )
    .unwrap();
    cluster.nodes[stopped] = Some(NodeRuntime::start(config).await.unwrap());
    let endpoint = cluster.directory.nodes[stopped].data_address;
    cluster.links[stopped] = Link::connect(endpoint, LinkConfig::default(), deadline())
        .await
        .unwrap();
    let leader = cluster.leader(GroupId::new(1)).await;
    let applied = cluster.nodes[leader]
        .as_ref()
        .unwrap()
        .applied_index(GroupId::new(1))
        .unwrap();
    timeout(Duration::from_secs(15), async {
        loop {
            if cluster.nodes[stopped]
                .as_ref()
                .unwrap()
                .applied_index(GroupId::new(1))
                .is_some_and(|index| index >= applied)
            {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("restarted replica must catch up");
    assert_eq!(
        cluster
            .kv(GroupId::new(1), KvRequest::Get { key: key.clone() })
            .await,
        KvResponse::Value(None)
    );
    cluster.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immutable_directory_is_durably_bootstrapped_and_equal_retry_does_not_write() {
    let cluster = Cluster::start().await;
    cluster.nodes[0]
        .as_ref()
        .unwrap()
        .bootstrap_directory(deadline())
        .await
        .unwrap();
    let leader = cluster.leader(GroupId::new(0)).await;
    assert_eq!(
        cluster.request(leader, BackendOperation::Directory).await,
        BackendReply::Directory(Ok(cluster.directory.clone())),
    );
    let applied_before = cluster.nodes[leader]
        .as_ref()
        .unwrap()
        .applied_index(GroupId::new(0));
    assert_eq!(
        cluster
            .request(leader, BackendOperation::BootstrapDirectory)
            .await,
        BackendReply::Bootstrap(MutationOutcome::Committed),
    );
    assert_eq!(
        cluster.nodes[leader]
            .as_ref()
            .unwrap()
            .applied_index(GroupId::new(0)),
        applied_before
    );
    cluster.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_client_uses_gateway_and_real_raft_groups_for_put_get_and_delete() {
    let cluster = Cluster::start().await;
    cluster.nodes[0]
        .as_ref()
        .unwrap()
        .bootstrap_directory(deadline())
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = GatewayConfig {
        cluster_id: cluster.directory.cluster_id,
        expected_shard_config: cluster.directory.shard_config,
        seeds: cluster
            .directory
            .nodes
            .iter()
            .map(|node| SeedNode {
                node_id: node.node_id,
                address: node.data_address,
            })
            .collect(),
        listen_address: address,
        backend_link: LinkConfig::default(),
        server: ServerConfig::default(),
        startup_timeout: Duration::from_secs(10),
    };
    let mut gateway = GatewayRuntime::start_with_listener(config, listener)
        .await
        .unwrap();
    let client = RockBeeRaftClient::connect(address, ClientConfig::default())
        .await
        .unwrap();
    for key in keys(&cluster.directory).values() {
        assert_eq!(
            client
                .put(key.clone(), Bytes::from_static(b"available"))
                .unwrap()
                .await,
            MutationOutcome::Committed
        );
        assert_eq!(
            client.get(key.clone()).await.unwrap(),
            Some(Bytes::from_static(b"available"))
        );
        assert_eq!(
            client.delete(key.clone()).unwrap().await,
            MutationOutcome::Committed
        );
        assert_eq!(client.get(key.clone()).await.unwrap(), None);
    }
    client.close().await;
    gateway.close().await;
    cluster.close().await;
}
