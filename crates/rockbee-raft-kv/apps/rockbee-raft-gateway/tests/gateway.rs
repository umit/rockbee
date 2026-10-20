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

struct RefreshState {
    directory: std::sync::RwLock<ClusterDirectory>,
    directories: AtomicUsize,
    mutations: AtomicUsize,
    first_metadata_delay_ms: AtomicUsize,
    hold_mutation: std::sync::atomic::AtomicBool,
    mutation_started: tokio::sync::Notify,
    release_mutation: tokio::sync::Notify,
    metadata_unavailable: std::sync::atomic::AtomicBool,
    uncertain_mutation: std::sync::atomic::AtomicBool,
    d_unavailable: std::sync::atomic::AtomicBool,
    missing_read: std::sync::atomic::AtomicBool,
    after_safe_rejection: std::sync::RwLock<Option<AfterSafeRejection>>,
    kv_attempts: [AtomicUsize; 4],
}

#[derive(Clone, Copy)]
enum AfterSafeRejection {
    Committed,
    Unknown,
}

struct RefreshBackend(Arc<RefreshState>);

impl RequestHandler for RefreshBackend {
    type Request = BackendRequest;
    type Response = BackendResponse;

    fn handle(
        &self,
        request: BackendRequest,
        _deadline: Instant,
    ) -> impl Future<Output = BackendResponse> + Send {
        let state = Arc::clone(&self.0);
        async move {
            let failure = || rockbee_raft_protocol::Failure {
                reason: rockbee_raft_protocol::FailureReason::NotLeader,
                message: "moved group".to_owned(),
            };
            let reply = match request.operation {
                BackendOperation::Directory => {
                    state.directories.fetch_add(1, Ordering::SeqCst);
                    if request.target_node.get() == 1 {
                        let millis = state.first_metadata_delay_ms.load(Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(u64::try_from(millis).unwrap()))
                            .await;
                    }
                    if state.metadata_unavailable.load(Ordering::SeqCst) {
                        BackendReply::Directory(Err(rockbee_raft_protocol::ReadFailure::new(
                            failure(),
                        )))
                    } else {
                        BackendReply::Directory(Ok(state.directory.read().unwrap().clone()))
                    }
                }
                BackendOperation::Kv {
                    request: operation, ..
                } => {
                    let index = usize::try_from(request.target_node.get() - 1).unwrap();
                    state.kv_attempts[index].fetch_add(1, Ordering::SeqCst);
                    let after_safe_rejection = *state.after_safe_rejection.read().unwrap();
                    let response = if let Some(mode) = after_safe_rejection {
                        assert!(matches!(
                            operation,
                            rockbee_raft_protocol::KvRequest::Put { .. }
                        ));
                        KvResponse::Mutation(match request.target_node.get() {
                            1 => MutationOutcome::Rejected(rockbee_raft_protocol::Failure {
                                reason: rockbee_raft_protocol::FailureReason::Unavailable,
                                message: "A safely refused before submission".to_owned(),
                            }),
                            2 if matches!(mode, AfterSafeRejection::Unknown) => {
                                MutationOutcome::Unknown(rockbee_raft_protocol::Failure {
                                    reason: rockbee_raft_protocol::FailureReason::Unavailable,
                                    message: "B result is uncertain".to_owned(),
                                })
                            }
                            // C/D would expose a replay by returning Committed.
                            _ => MutationOutcome::Committed,
                        })
                    } else if state.missing_read.load(Ordering::SeqCst)
                        && matches!(operation, rockbee_raft_protocol::KvRequest::Get { .. })
                    {
                        KvResponse::Value(None)
                    } else if request.target_node.get() == 1
                        && state.hold_mutation.load(Ordering::SeqCst)
                        && !matches!(operation, rockbee_raft_protocol::KvRequest::Get { .. })
                    {
                        state.mutations.fetch_add(1, Ordering::SeqCst);
                        state.mutation_started.notify_one();
                        state.release_mutation.notified().await;
                        KvResponse::Mutation(MutationOutcome::Committed)
                    } else if state.uncertain_mutation.load(Ordering::SeqCst)
                        && !matches!(operation, rockbee_raft_protocol::KvRequest::Get { .. })
                    {
                        state.mutations.fetch_add(1, Ordering::SeqCst);
                        KvResponse::Mutation(MutationOutcome::Unknown(
                            rockbee_raft_protocol::Failure {
                                reason: rockbee_raft_protocol::FailureReason::Unavailable,
                                message: "fixture lost mutation result".to_owned(),
                            },
                        ))
                    } else if request.target_node.get() == 4
                        && !state.d_unavailable.load(Ordering::SeqCst)
                    {
                        match operation {
                            rockbee_raft_protocol::KvRequest::Get { .. } => {
                                KvResponse::Value(Some(Bytes::from_static(b"from D")))
                            }
                            _ => {
                                state.mutations.fetch_add(1, Ordering::SeqCst);
                                KvResponse::Mutation(MutationOutcome::Committed)
                            }
                        }
                    } else {
                        match operation {
                            rockbee_raft_protocol::KvRequest::Get { .. } => KvResponse::ReadError(
                                rockbee_raft_protocol::ReadFailure::new(failure()),
                            ),
                            _ => KvResponse::Mutation(MutationOutcome::Rejected(failure())),
                        }
                    };
                    // Deliberately advertises D before it is in the catalog.
                    BackendReply::Kv {
                        response,
                        leader_hint: Some(NodeId::new(4).unwrap()),
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

struct RefreshFixture {
    gateway: GatewayRuntime,
    client: RockBeeRaftClient,
    state: Arc<RefreshState>,
    initial: ClusterDirectory,
    updated: ClusterDirectory,
    servers: Vec<Server>,
    _peers: Vec<TcpListener>,
}

impl RefreshFixture {
    async fn start() -> Self {
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..4 {
            listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
            peers.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let nodes: Vec<_> = (0..4)
            .map(|index| NodeEndpoint {
                node_id: NodeId::new(u64::try_from(index + 1).unwrap()).unwrap(),
                peer_address: peers[index].local_addr().unwrap(),
                data_address: listeners[index].local_addr().unwrap(),
            })
            .collect();
        let cluster_id = ClusterId::new([11; 16]).unwrap();
        let shard_config = ShardConfig::new(1, 0).unwrap();
        let initial =
            ClusterDirectory::initial(cluster_id, shard_config, nodes[..3].to_vec()).unwrap();
        let state = Arc::new(RefreshState {
            directory: std::sync::RwLock::new(initial.clone()),
            directories: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
            first_metadata_delay_ms: AtomicUsize::new(0),
            hold_mutation: std::sync::atomic::AtomicBool::new(false),
            mutation_started: tokio::sync::Notify::new(),
            release_mutation: tokio::sync::Notify::new(),
            metadata_unavailable: std::sync::atomic::AtomicBool::new(false),
            uncertain_mutation: std::sync::atomic::AtomicBool::new(false),
            d_unavailable: std::sync::atomic::AtomicBool::new(false),
            missing_read: std::sync::atomic::AtomicBool::new(false),
            after_safe_rejection: std::sync::RwLock::new(None),
            kv_attempts: std::array::from_fn(|_| AtomicUsize::new(0)),
        });
        let mut servers = Vec::new();
        for listener in listeners {
            servers.push(
                Server::start(
                    listener,
                    ServerConfig::default(),
                    Arc::new(RefreshBackend(Arc::clone(&state))),
                )
                .unwrap(),
            );
        }
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let gateway = GatewayRuntime::start_with_listener(
            GatewayConfig {
                cluster_id,
                expected_shard_config: shard_config,
                seeds: vec![SeedNode {
                    node_id: nodes[0].node_id,
                    address: nodes[0].data_address,
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
        let mut updated = initial.clone();
        updated.revision = 2;
        updated.nodes.push(nodes[3].clone());
        updated.placements[1].voters = vec![nodes[0].node_id, nodes[1].node_id, nodes[3].node_id];
        updated.validate().unwrap();
        let client = RockBeeRaftClient::connect(public_address, ClientConfig::default())
            .await
            .unwrap();
        Self {
            gateway,
            client,
            state,
            initial,
            updated,
            servers,
            _peers: peers,
        }
    }

    fn publish(&self, directory: ClusterDirectory) {
        *self.state.directory.write().unwrap() = directory;
    }

    async fn close(&mut self) {
        self.client.close().await;
        self.gateway.close().await;
        for server in &mut self.servers {
            server.close().await;
        }
    }
}

async fn mutation_after_safe_rejection(
    mode: AfterSafeRejection,
) -> (MutationOutcome, [usize; 4], usize) {
    let mut fixture = RefreshFixture::start().await;
    *fixture.state.after_safe_rejection.write().unwrap() = Some(mode);
    let directories_before = fixture.state.directories.load(Ordering::SeqCst);
    let outcome = fixture
        .client
        .put(
            Bytes::from_static(b"safe-prefix"),
            Bytes::from_static(b"one attempt"),
        )
        .unwrap()
        .await;
    fixture.close().await;
    let attempts =
        std::array::from_fn(|index| fixture.state.kv_attempts[index].load(Ordering::SeqCst));
    let refreshes = fixture.state.directories.load(Ordering::SeqCst) - directories_before;
    eprintln!(
        "safe_rejection_prefix outcome={outcome:?} kv_attempts={attempts:?} refreshes={refreshes} cleanup=joined"
    );
    (outcome, attempts, refreshes)
}

#[tokio::test]
async fn gateway_safe_rejection_then_unknown_is_terminal_without_replay() {
    let (outcome, attempts, refreshes) =
        mutation_after_safe_rejection(AfterSafeRejection::Unknown).await;
    assert_eq!(
        outcome,
        MutationOutcome::Unknown(rockbee_raft_protocol::Failure {
            reason: rockbee_raft_protocol::FailureReason::Unavailable,
            message: "B result is uncertain".to_owned(),
        })
    );
    assert_eq!(attempts, [1, 1, 0, 0]);
    assert_eq!(refreshes, 0);
}

#[tokio::test]
async fn gateway_safe_rejection_then_committed_uses_the_next_candidate() {
    let (outcome, attempts, refreshes) =
        mutation_after_safe_rejection(AfterSafeRejection::Committed).await;
    assert_eq!(outcome, MutationOutcome::Committed);
    assert_eq!(attempts, [1, 1, 0, 0]);
    assert_eq!(refreshes, 0);
}

#[tokio::test]
async fn running_gateway_refreshes_published_placement_and_routes_to_d() {
    let mut fixture = RefreshFixture::start().await;
    fixture.publish(fixture.updated.clone());
    assert_eq!(
        fixture
            .client
            .get(Bytes::from_static(b"moved"))
            .await
            .unwrap(),
        Some(Bytes::from_static(b"from D"))
    );
    assert_eq!(
        fixture
            .client
            .put(Bytes::from_static(b"moved"), Bytes::new())
            .unwrap()
            .await,
        MutationOutcome::Committed
    );
    assert_eq!(
        fixture
            .client
            .delete(Bytes::from_static(b"moved"))
            .unwrap()
            .await,
        MutationOutcome::Committed
    );
    assert_eq!(fixture.state.mutations.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.state.directories.load(Ordering::SeqCst), 2);
    fixture.close().await;
}

#[tokio::test]
async fn gateway_refresh_coalesces_unchanged_and_failed_metadata_attempts() {
    let mut fixture = RefreshFixture::start().await;
    fixture
        .state
        .first_metadata_delay_ms
        .store(40, Ordering::SeqCst);
    for unavailable in [false, true] {
        fixture
            .state
            .metadata_unavailable
            .store(unavailable, Ordering::SeqCst);
        let before = fixture.state.directories.load(Ordering::SeqCst);
        let gateway = fixture.gateway.gateway();
        let request = rockbee_raft_protocol::KvRequest::Get {
            key: Bytes::from_static(b"moved"),
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        let results = tokio::join!(
            gateway.handle(request.clone(), deadline),
            gateway.handle(request.clone(), deadline),
            gateway.handle(request.clone(), deadline),
            gateway.handle(request, deadline)
        );
        for result in [results.0, results.1, results.2, results.3] {
            assert!(matches!(result, KvResponse::ReadError(_)));
        }
        let expected = if unavailable { 3 } else { 1 };
        assert_eq!(
            fixture.state.directories.load(Ordering::SeqCst) - before,
            expected
        );
    }
    fixture.close().await;
}

#[tokio::test]
async fn gateway_metadata_candidates_share_the_original_deadline() {
    let mut fixture = RefreshFixture::start().await;
    fixture.publish(fixture.updated.clone());
    fixture
        .state
        .first_metadata_delay_ms
        .store(600, Ordering::SeqCst);
    let started = Instant::now();
    let result = fixture
        .gateway
        .gateway()
        .handle(
            rockbee_raft_protocol::KvRequest::Get {
                key: Bytes::from_static(b"moved"),
            },
            started + Duration::from_millis(400),
        )
        .await;
    assert_eq!(
        result,
        KvResponse::Value(Some(Bytes::from_static(b"from D")))
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(fixture.state.directories.load(Ordering::SeqCst) >= 3);
    fixture.close().await;
}

#[tokio::test]
async fn gateway_equal_revision_conflict_fails_closed_and_older_directory_cannot_regress() {
    let mut fixture = RefreshFixture::start().await;
    let mut conflicting = fixture.updated.clone();
    conflicting.revision = 1;
    fixture.publish(conflicting);
    assert!(
        fixture
            .client
            .get(Bytes::from_static(b"moved"))
            .await
            .is_err()
    );
    assert_eq!(fixture.state.mutations.load(Ordering::SeqCst), 0);
    fixture.publish(fixture.updated.clone());
    assert_eq!(
        fixture
            .client
            .get(Bytes::from_static(b"moved"))
            .await
            .unwrap(),
        Some(Bytes::from_static(b"from D"))
    );
    fixture.publish(fixture.initial.clone());
    fixture.state.d_unavailable.store(true, Ordering::SeqCst);
    assert!(
        fixture
            .client
            .get(Bytes::from_static(b"moved"))
            .await
            .is_err()
    );
    fixture.state.d_unavailable.store(false, Ordering::SeqCst);
    assert_eq!(
        fixture
            .client
            .get(Bytes::from_static(b"moved"))
            .await
            .unwrap(),
        Some(Bytes::from_static(b"from D"))
    );
    fixture.close().await;
}

#[tokio::test]
async fn gateway_published_directory_never_replays_an_uncertain_mutation_or_refreshes_final_absence()
 {
    let mut fixture = RefreshFixture::start().await;
    fixture.publish(fixture.updated.clone());
    fixture
        .state
        .uncertain_mutation
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        fixture
            .client
            .put(Bytes::from_static(b"moved"), Bytes::new())
            .unwrap()
            .await,
        MutationOutcome::Unknown(_)
    ));
    assert_eq!(fixture.state.mutations.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.directories.load(Ordering::SeqCst), 1);
    fixture.state.missing_read.store(true, Ordering::SeqCst);
    assert_eq!(
        fixture
            .client
            .get(Bytes::from_static(b"moved"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(fixture.state.directories.load(Ordering::SeqCst), 1);
    fixture.close().await;
}

#[tokio::test]
async fn gateway_refresh_coalesces_new_revision_and_preserves_original_deadline_when_unavailable() {
    let mut fixture = RefreshFixture::start().await;
    fixture.publish(fixture.updated.clone());
    fixture
        .state
        .first_metadata_delay_ms
        .store(40, Ordering::SeqCst);
    let gateway = fixture.gateway.gateway();
    let request = rockbee_raft_protocol::KvRequest::Get {
        key: Bytes::from_static(b"moved"),
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    let results = tokio::join!(
        gateway.handle(request.clone(), deadline),
        gateway.handle(request.clone(), deadline),
        gateway.handle(request.clone(), deadline),
        gateway.handle(request.clone(), deadline)
    );
    for result in [results.0, results.1, results.2, results.3] {
        assert_eq!(
            result,
            KvResponse::Value(Some(Bytes::from_static(b"from D")))
        );
    }
    assert_eq!(fixture.state.directories.load(Ordering::SeqCst), 2);
    fixture.state.d_unavailable.store(true, Ordering::SeqCst);
    fixture
        .state
        .metadata_unavailable
        .store(true, Ordering::SeqCst);
    fixture
        .state
        .first_metadata_delay_ms
        .store(600, Ordering::SeqCst);
    let started = Instant::now();
    assert!(matches!(
        fixture
            .gateway
            .gateway()
            .handle(request, started + Duration::from_millis(120))
            .await,
        KvResponse::ReadError(_)
    ));
    assert!(started.elapsed() < Duration::from_millis(250));
    assert!(fixture.state.directories.load(Ordering::SeqCst) >= 5);
    fixture.close().await;
}

#[tokio::test]
async fn short_metadata_refresh_does_not_retire_an_unrelated_pending_mutation() {
    let mut fixture = RefreshFixture::start().await;
    fixture.state.hold_mutation.store(true, Ordering::SeqCst);
    fixture
        .state
        .first_metadata_delay_ms
        .store(600, Ordering::SeqCst);
    fixture
        .state
        .metadata_unavailable
        .store(true, Ordering::SeqCst);
    let gateway = fixture.gateway.gateway();
    let now = Instant::now();
    let (mutation, read) = tokio::join!(
        gateway.handle(
            rockbee_raft_protocol::KvRequest::Put {
                key: Bytes::from_static(b"pending"),
                value: Bytes::new(),
            },
            now + Duration::from_secs(2)
        ),
        async {
            // Positively observe the handed PUT and hold its response until
            // metadata refresh has timed out and tried another voter.
            fixture.state.mutation_started.notified().await;
            let read = gateway
                .handle(
                    rockbee_raft_protocol::KvRequest::Get {
                        key: Bytes::from_static(b"moved"),
                    },
                    Instant::now() + Duration::from_millis(120),
                )
                .await;
            fixture.state.release_mutation.notify_one();
            read
        }
    );
    assert_eq!(mutation, KvResponse::Mutation(MutationOutcome::Committed));
    assert!(matches!(read, KvResponse::ReadError(_)));
    assert_eq!(fixture.state.mutations.load(Ordering::SeqCst), 1);
    assert!(fixture.state.directories.load(Ordering::SeqCst) >= 4);
    fixture.state.missing_read.store(true, Ordering::SeqCst);
    assert_eq!(
        fixture
            .client
            .get(Bytes::from_static(b"after refresh"))
            .await
            .unwrap(),
        None
    );
    fixture.close().await;
}
