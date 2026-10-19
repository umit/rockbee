#![allow(clippy::unwrap_used)]

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use rockbee_raft_client::{ClientConfig, RockBeeRaftClient};
use rockbee_raft_gateway::{GatewayConfig, GatewayRuntime, SeedNode};
use rockbee_raft_protocol::{
    ClusterId, GroupId, KvResponse, MutationOutcome, NodeId, ShardConfig,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    routing::{ClusterDirectory, NodeEndpoint},
    transport::{
        LinkConfig,
        server::{RequestHandler, Server, ServerConfig},
    },
};
use tokio::{net::TcpListener, time::Instant};

struct BackendFixture {
    directory: ClusterDirectory,
    read_mode: ReadMode,
    mutation_count: Option<Arc<AtomicUsize>>,
}

#[derive(Clone)]
enum ReadMode {
    MutationOnly,
    NotLeader,
    EmptyValue,
    DelayedMutation(Duration),
}

impl RequestHandler for BackendFixture {
    type Request = BackendRequest;
    type Response = BackendResponse;

    fn handle(
        &self,
        request: BackendRequest,
        _deadline: Instant,
    ) -> impl Future<Output = BackendResponse> + Send {
        let directory = self.directory.clone();
        let node_id = request.target_node;
        let read_mode = self.read_mode.clone();
        let mutation_count = self.mutation_count.clone();
        async move {
            // Root adds an internal metadata-bootstrap variant; gateway never
            // accepts it. Keep this controlled fixture fail-closed for that
            // future wire case while current protocol has only three variants.
            #[allow(unreachable_patterns)]
            let reply = match request.operation {
                BackendOperation::Directory => BackendReply::Directory(Ok(directory)),
                BackendOperation::Leader { group_id } => BackendReply::Leader {
                    group_id,
                    leader: Some(request.target_node),
                },
                BackendOperation::Kv { request, .. } => {
                    if matches!(
                        request,
                        rockbee_raft_protocol::KvRequest::Put { .. }
                            | rockbee_raft_protocol::KvRequest::Delete { .. }
                    ) {
                        if let Some(mutation_count) = mutation_count {
                            mutation_count.fetch_add(1, Ordering::SeqCst);
                        }
                        if let ReadMode::DelayedMutation(delay) = &read_mode {
                            tokio::time::sleep(*delay).await;
                        }
                    }
                    BackendReply::Kv {
                        response: match (&read_mode, request) {
                            (ReadMode::NotLeader, rockbee_raft_protocol::KvRequest::Get { .. }) => {
                                KvResponse::ReadError(rockbee_raft_protocol::ReadFailure::new(
                                    rockbee_raft_protocol::Failure {
                                        reason: rockbee_raft_protocol::FailureReason::NotLeader,
                                        message: "fixture follower".to_owned(),
                                    },
                                ))
                            }
                            (
                                ReadMode::EmptyValue,
                                rockbee_raft_protocol::KvRequest::Get { .. },
                            ) => KvResponse::Value(Some(Bytes::new())),
                            (
                                ReadMode::DelayedMutation(_),
                                rockbee_raft_protocol::KvRequest::Get { .. },
                            ) => KvResponse::Value(None),
                            _ => KvResponse::Mutation(MutationOutcome::Committed),
                        },
                        leader_hint: Some(node_id),
                    }
                }
                _ => BackendReply::Leader {
                    group_id: GroupId::new(0),
                    leader: None,
                },
            };
            BackendResponse {
                cluster_id: request.cluster_id,
                node_id: request.target_node,
                reply,
            }
        }
    }
}

#[tokio::test]
async fn gateway_requires_directory_then_routes_public_mutation_to_backend() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_address = backend_listener.local_addr().unwrap();
    let cluster_id = ClusterId::new([7; 16]).unwrap();
    let shard_config = ShardConfig::new(4, 0).unwrap();
    // The directory validates every advertised endpoint is unique. Retain
    // placeholder listeners during fixture construction so the OS cannot reuse
    // an address between allocations. Only node one is contacted in this test.
    let mut placeholders = Vec::new();
    for _ in 0..5 {
        placeholders.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses: Vec<_> = placeholders
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect();
    let nodes = vec![
        NodeEndpoint {
            node_id: NodeId::new(1).unwrap(),
            peer_address: addresses[0],
            data_address: backend_address,
        },
        NodeEndpoint {
            node_id: NodeId::new(2).unwrap(),
            peer_address: addresses[1],
            data_address: addresses[2],
        },
        NodeEndpoint {
            node_id: NodeId::new(3).unwrap(),
            peer_address: addresses[3],
            data_address: addresses[4],
        },
    ];
    let directory = ClusterDirectory::initial(cluster_id, shard_config, nodes).unwrap();
    let backend = Arc::new(BackendFixture {
        directory,
        read_mode: ReadMode::MutationOnly,
        mutation_count: None,
    });
    let mut backend_server =
        Server::start(backend_listener, ServerConfig::default(), backend).unwrap();

    let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_address = public_listener.local_addr().unwrap();
    let config = GatewayConfig {
        cluster_id,
        expected_shard_config: shard_config,
        seeds: vec![SeedNode {
            node_id: NodeId::new(1).unwrap(),
            address: backend_address,
        }],
        listen_address: public_address,
        backend_link: LinkConfig::default(),
        server: ServerConfig::default(),
        startup_timeout: Duration::from_secs(5),
    };
    let mut gateway = GatewayRuntime::start_with_listener(config, public_listener)
        .await
        .unwrap();
    let client = RockBeeRaftClient::connect(public_address, ClientConfig::default())
        .await
        .unwrap();
    let outcome = client
        .put(Bytes::from_static(b"order/42"), Bytes::from_static(b"paid"))
        .unwrap()
        .await;
    assert_eq!(outcome, MutationOutcome::Committed);
    client.close().await;
    gateway.close().await;
    backend_server.close().await;
}

#[tokio::test]
async fn gateway_retries_get_on_explicit_not_leader_and_keeps_empty_value() {
    let node_listeners = [
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    let node_addresses: Vec<_> = node_listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect();
    let peer_listeners = [
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    let cluster_id = ClusterId::new([8; 16]).unwrap();
    let shard_config = ShardConfig::new(4, 0).unwrap();
    let directory = ClusterDirectory::initial(
        cluster_id,
        shard_config,
        (0..3)
            .map(|index| NodeEndpoint {
                node_id: NodeId::new(u64::try_from(index + 1).unwrap()).unwrap(),
                peer_address: peer_listeners[index].local_addr().unwrap(),
                data_address: node_addresses[index],
            })
            .collect(),
    )
    .unwrap();
    let mut servers = Vec::new();
    for (index, listener) in node_listeners.into_iter().enumerate() {
        let read_mode = if index == 0 {
            ReadMode::NotLeader
        } else {
            ReadMode::EmptyValue
        };
        servers.push(
            Server::start(
                listener,
                ServerConfig::default(),
                Arc::new(BackendFixture {
                    directory: directory.clone(),
                    read_mode,
                    mutation_count: None,
                }),
            )
            .unwrap(),
        );
    }
    let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_address = public_listener.local_addr().unwrap();
    let mut gateway = GatewayRuntime::start_with_listener(
        GatewayConfig {
            cluster_id,
            expected_shard_config: shard_config,
            seeds: vec![SeedNode {
                node_id: NodeId::new(1).unwrap(),
                address: node_addresses[0],
            }],
            listen_address: public_address,
            backend_link: LinkConfig::default(),
            server: ServerConfig::default(),
            startup_timeout: Duration::from_secs(5),
        },
        public_listener,
    )
    .await
    .unwrap();
    let client = RockBeeRaftClient::connect(public_address, ClientConfig::default())
        .await
        .unwrap();
    assert_eq!(
        client.get(Bytes::from_static(b"product/42")).await.unwrap(),
        Some(Bytes::new())
    );
    client.close().await;
    gateway.close().await;
    for server in &mut servers {
        server.close().await;
    }
}

#[tokio::test]
async fn gateway_safely_tries_the_next_voter_when_first_mutation_connection_is_down() {
    // Keep a concrete address for voter one, then release it. A refused TCP
    // connect proves this PUT was not handed to that voter.
    let unavailable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable_address = unavailable_listener.local_addr().unwrap();
    drop(unavailable_listener);

    let live_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_address = live_listener.local_addr().unwrap();
    let placeholders = [
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    let cluster_id = ClusterId::new([9; 16]).unwrap();
    let shard_config = ShardConfig::new(1, 0).unwrap();
    let directory = ClusterDirectory::initial(
        cluster_id,
        shard_config,
        vec![
            NodeEndpoint {
                node_id: NodeId::new(1).unwrap(),
                peer_address: placeholders[0].local_addr().unwrap(),
                data_address: unavailable_address,
            },
            NodeEndpoint {
                node_id: NodeId::new(2).unwrap(),
                peer_address: placeholders[1].local_addr().unwrap(),
                data_address: live_address,
            },
            NodeEndpoint {
                node_id: NodeId::new(3).unwrap(),
                peer_address: placeholders[2].local_addr().unwrap(),
                data_address: placeholders[3].local_addr().unwrap(),
            },
        ],
    )
    .unwrap();
    let mutation_count = Arc::new(AtomicUsize::new(0));
    let mut live_server = Server::start(
        live_listener,
        ServerConfig::default(),
        Arc::new(BackendFixture {
            directory,
            read_mode: ReadMode::MutationOnly,
            mutation_count: Some(Arc::clone(&mutation_count)),
        }),
    )
    .unwrap();
    let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_address = public_listener.local_addr().unwrap();
    let mut gateway = GatewayRuntime::start_with_listener(
        GatewayConfig {
            cluster_id,
            expected_shard_config: shard_config,
            seeds: vec![SeedNode {
                node_id: NodeId::new(2).unwrap(),
                address: live_address,
            }],
            listen_address: public_address,
            backend_link: LinkConfig::default(),
            server: ServerConfig::default(),
            startup_timeout: Duration::from_secs(5),
        },
        public_listener,
    )
    .await
    .unwrap();
    let client = RockBeeRaftClient::connect(public_address, ClientConfig::default())
        .await
        .unwrap();
    assert_eq!(
        client
            .put(Bytes::from_static(b"order/43"), Bytes::from_static(b"paid"))
            .unwrap()
            .await,
        MutationOutcome::Committed
    );
    assert_eq!(mutation_count.load(Ordering::SeqCst), 1);
    client.close().await;
    gateway.close().await;
    live_server.close().await;
}

#[tokio::test]
async fn gateway_never_replays_unknown_mutation_and_reconnects_for_later_read() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_address = backend_listener.local_addr().unwrap();
    let placeholders = [
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    let cluster_id = ClusterId::new([10; 16]).unwrap();
    let shard_config = ShardConfig::new(1, 0).unwrap();
    let directory = ClusterDirectory::initial(
        cluster_id,
        shard_config,
        vec![
            NodeEndpoint {
                node_id: NodeId::new(1).unwrap(),
                peer_address: placeholders[0].local_addr().unwrap(),
                data_address: backend_address,
            },
            NodeEndpoint {
                node_id: NodeId::new(2).unwrap(),
                peer_address: placeholders[1].local_addr().unwrap(),
                data_address: placeholders[2].local_addr().unwrap(),
            },
            NodeEndpoint {
                node_id: NodeId::new(3).unwrap(),
                peer_address: placeholders[4].local_addr().unwrap(),
                data_address: placeholders[5].local_addr().unwrap(),
            },
        ],
    )
    .unwrap();
    let mutation_count = Arc::new(AtomicUsize::new(0));
    let mut backend_server = Server::start(
        backend_listener,
        ServerConfig::default(),
        Arc::new(BackendFixture {
            directory,
            read_mode: ReadMode::DelayedMutation(Duration::from_millis(100)),
            mutation_count: Some(Arc::clone(&mutation_count)),
        }),
    )
    .unwrap();
    let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_address = public_listener.local_addr().unwrap();
    let mut gateway = GatewayRuntime::start_with_listener(
        GatewayConfig {
            cluster_id,
            expected_shard_config: shard_config,
            seeds: vec![SeedNode {
                node_id: NodeId::new(1).unwrap(),
                address: backend_address,
            }],
            listen_address: public_address,
            backend_link: LinkConfig::default(),
            server: ServerConfig::default(),
            startup_timeout: Duration::from_secs(5),
        },
        public_listener,
    )
    .await
    .unwrap();
    let short_timeout = ClientConfig {
        request_timeout: Duration::from_millis(30),
        ..ClientConfig::default()
    };
    let client = RockBeeRaftClient::connect(public_address, short_timeout)
        .await
        .unwrap();
    assert!(matches!(
        client
            .put(Bytes::from_static(b"order/44"), Bytes::from_static(b"paid"))
            .unwrap()
            .await,
        MutationOutcome::Unknown(_)
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(mutation_count.load(Ordering::SeqCst), 1);
    client.close().await;

    let later_client = RockBeeRaftClient::connect(public_address, ClientConfig::default())
        .await
        .unwrap();
    assert_eq!(
        later_client
            .get(Bytes::from_static(b"order/44"))
            .await
            .unwrap(),
        None
    );
    later_client.close().await;
    gateway.close().await;
    backend_server.close().await;
}
