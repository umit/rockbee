//! A real node-to-gateway reply loss leaves the public connection usable.

use super::{
    Events, TEST_TIMEOUT,
    backend_proxy::BackendReplyLossProxy,
    capture::{BackendCapturedHistory, check_persisted_backend_history},
    history::{KeyWitness, Verdict},
    resources::Resources,
    scenario::{MutationObservation, ReadObservation, observe_lost_reply, observe_read},
};
use crate::{
    cluster::{NodeError, NodeRuntime},
    config::{NodeConfig, ResolvedStartup},
    storage::OpenMode,
};
use bytes::Bytes;
use rockbee_raft_client::{ClientConfig, RockBeeRaftClient};
use rockbee_raft_gateway::{GatewayConfig, GatewayRuntime, SeedNode};
use rockbee_raft_protocol::{
    ClusterId, FailureReason, NodeId, ShardConfig, WireRequest,
    backend::{BackendOperation, BackendReply, BackendRequest},
    routing::{ClusterDirectory, NodeEndpoint},
    transport::server::{RequestHandler, ServerConfig},
};
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, time::Instant};

/// Immutable addresses captured from listeners owned by this one fixture.
struct RelayMapping {
    endpoint: NodeEndpoint,
    private_data_address: SocketAddr,
}

struct Listeners {
    peer: TcpListener,
    data: TcpListener,
    relay: TcpListener,
    mapping: RelayMapping,
}

impl Listeners {
    async fn bind(node_id: NodeId) -> Result<Self, NodeError> {
        let peer = TcpListener::bind("127.0.0.1:0").await?;
        let data = TcpListener::bind("127.0.0.1:0").await?;
        let relay = TcpListener::bind("127.0.0.1:0").await?;
        let mapping = RelayMapping {
            endpoint: NodeEndpoint {
                node_id,
                peer_address: peer.local_addr()?,
                data_address: relay.local_addr()?,
            },
            private_data_address: data.local_addr()?,
        };
        Ok(Self {
            peer,
            data,
            relay,
            mapping,
        })
    }

    /// This fixture uses private data listeners behind advertised TCP relays.
    /// Validate both sides before bypassing the normal exact-listener check.
    fn resolve(&self, config: NodeConfig) -> Result<ResolvedStartup, NodeError> {
        let resolved = ResolvedStartup::bootstrap(config)?;
        if resolved.node_id != self.mapping.endpoint.node_id
            || resolved.endpoint != self.mapping.endpoint
            || self.peer.local_addr()? != self.mapping.endpoint.peer_address
            || self.data.local_addr()? != self.mapping.private_data_address
            || self.relay.local_addr()? != self.mapping.endpoint.data_address
        {
            return Err(NodeError::Config(
                "backend reply-loss fixture listener mapping mismatch".into(),
            ));
        }
        Ok(resolved)
    }
}

struct Observation {
    key: Bytes,
    value: Bytes,
    mutation: MutationObservation,
    read: ReadObservation,
}

async fn start_resources(
    resources: &mut Resources,
    key: Bytes,
    events: Arc<Events>,
) -> Result<(SocketAddr, ClusterDirectory), String> {
    let cluster = ClusterId::new([109; 16]).map_err(|error| error.to_string())?;
    let mut listeners = Vec::new();
    for raw_id in 1..=3 {
        listeners.push(
            Listeners::bind(NodeId::new(raw_id).map_err(|error| error.to_string())?)
                .await
                .map_err(|error| error.to_string())?,
        );
    }
    let directory = ClusterDirectory::initial(
        cluster,
        ShardConfig::new(2, 0).map_err(|error| error.to_string())?,
        listeners
            .iter()
            .map(|listeners| listeners.mapping.endpoint.clone())
            .collect(),
    )
    .map_err(|error| error.to_string())?;
    for listeners in listeners {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        let config = NodeConfig::new(
            directory.clone(),
            listeners.mapping.endpoint.node_id,
            root.path().join("db"),
            OpenMode::CreateNew,
        )
        .map_err(|error| error.to_string())?;
        let resolved = listeners
            .resolve(config)
            .map_err(|error| error.to_string())?;
        resources.roots.push(root);
        let proxy = BackendReplyLossProxy::start(
            listeners.relay,
            listeners.mapping.private_data_address,
            cluster,
            resolved.node_id,
            key.clone(),
            Arc::clone(&events),
        )
        .map_err(|error| error.to_string())?;
        assert_eq!(proxy.address, resolved.endpoint.data_address);
        resources.backend_proxies.push(proxy);
        resources.begin_node(NodeRuntime::start_validated(
            resolved,
            listeners.peer,
            listeners.data,
            None,
        ));
        resources.finish_node().await?;
    }
    resources.nodes[0]
        .initialize_groups()
        .await
        .map_err(|error| error.to_string())?;
    resources.nodes[0]
        .bootstrap_directory(Instant::now() + TEST_TIMEOUT)
        .await
        .map_err(|error| error.to_string())?;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    resources.begin_gateway(GatewayRuntime::start_with_listener(
        GatewayConfig {
            cluster_id: cluster,
            expected_shard_config: directory.shard_config,
            seeds: directory
                .nodes
                .iter()
                .map(|node| SeedNode {
                    node_id: node.node_id,
                    address: node.data_address,
                })
                .collect(),
            listen_address: address,
            backend_link: Default::default(),
            server: ServerConfig::default(),
            startup_timeout: TEST_TIMEOUT,
        },
        listener,
    ));
    resources.finish_gateway().await?;
    Ok((address, directory))
}

async fn observe(resources: &mut Resources, events: Arc<Events>) -> Result<Observation, String> {
    let key = Bytes::from_static(b"backend-reply-loss/one-real-put");
    let value = Bytes::from_static(b"durable-before-backend-reply-loss");
    let (address, directory) = start_resources(resources, key.clone(), Arc::clone(&events)).await?;
    resources.original = Some(
        RockBeeRaftClient::connect(address, ClientConfig::default())
            .await
            .map_err(|error| error.to_string())?,
    );
    let group = directory
        .shard_config
        .group_for_key(&key)
        .map_err(|error| error.to_string())?;
    let first_node = directory.nodes[0].node_id;
    if directory.voters(group).and_then(|voters| voters.first()) != Some(&first_node) {
        return Err("fixture requires A as the first gateway candidate".into());
    }
    // This setup operation runs the real authority barrier, not a leader hint.
    // The captured public PUT/GET below still traverse every real TCP hop.
    let request = BackendRequest {
        cluster_id: directory.cluster_id,
        target_node: first_node,
        operation: BackendOperation::ObserveMembership { data_group: group },
    };
    let expected = request.expected_response();
    let response = resources.nodes[0]
        .service()
        .handle(request, Instant::now() + TEST_TIMEOUT)
        .await;
    BackendRequest::validate_response(expected, &response).map_err(|error| error.to_string())?;
    if !matches!(
        response.reply,
        BackendReply::Membership { result: Ok(_), .. }
    ) {
        return Err(format!(
            "first candidate lacks actual authority: {response:?}"
        ));
    }
    let client = resources.original.as_ref().expect("owned public client");
    // The gateway maps a backend disconnect to public Unavailable, preserving
    // Unknown certainty. The public TCP connection itself is still healthy.
    let mutation = observe_lost_reply(
        client,
        key.clone(),
        value.clone(),
        Arc::clone(&events),
        FailureReason::Unavailable,
    )
    .await?;
    // No close/reconnect/new client: Unknown is a valid public response, so
    // the same client generation must still serve the next request.
    let read = observe_read(client, key.clone(), value.clone(), events).await?;
    Ok(Observation {
        key,
        value,
        mutation,
        read,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_backend_response_loss_keeps_client_usable_without_replaying_put() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install test SIGTERM handler");
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("install test SIGINT handler");
    let interrupted = async {
        #[cfg(unix)]
        tokio::select! {
            _ = terminate.recv() => {},
            _ = interrupt.recv() => {},
        }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };
    let report = tempfile::Builder::new()
        .prefix("rockbee-backend-reply-loss-")
        .tempdir()
        .expect("create capture directory")
        .keep();
    let capture_path = report.join("history.bin");
    let events = Arc::new(Events::default());
    let owner = Arc::new(tokio::sync::Mutex::new(Resources::new()));
    let observer_owner = Arc::clone(&owner);
    let observer_events = Arc::clone(&events);
    let mut observer =
        tokio::spawn(
            async move { observe(&mut *observer_owner.lock().await, observer_events).await },
        );
    let observed = tokio::select! {
        result = &mut observer => Ok(result),
        _ = tokio::time::sleep(TEST_TIMEOUT) => Err("backend observation deadline expired"),
        _ = interrupted => Err("backend observation interrupted by signal"),
    };
    let observation = match observed {
        Ok(result) => result
            .map_err(|error| format!("backend observation task failed: {error}"))
            .and_then(|result| result),
        Err(reason) => {
            observer.abort();
            let _ = observer.await;
            Err(reason.to_owned())
        }
    };
    let mutex = match Arc::try_unwrap(owner) {
        Ok(mutex) => mutex,
        Err(_) => panic!("observer must release outer resources before cleanup"),
    };
    let mut resources = mutex.into_inner();
    let cleanup = resources.cleanup().await;
    let observation = observation.expect("backend reply-loss observation after cleanup");
    assert!(cleanup.pending_node_joined && cleanup.pending_gateway_joined);
    assert!(cleanup.node_startup_failure.is_none() && cleanup.gateway_startup_failure.is_none());
    let receipts = cleanup
        .backend_proxies
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("all backend relays joined successfully");
    assert_eq!(receipts.len(), 3);
    let forwarded_puts: usize = receipts.iter().map(|r| r.forwarded_put_frames).sum();
    let connections: usize = receipts.iter().map(|r| r.accepted_connections).sum();
    let suppressed: usize = receipts.iter().map(|r| r.suppressed_response_bytes).sum();
    assert_eq!(forwarded_puts, 1, "exactly one target PUT across all nodes");
    assert_eq!(
        cleanup.client_write_calls, 1,
        "exactly one native KV submission"
    );
    let committed: Vec<_> = receipts.iter().filter_map(|r| r.committed_at).collect();
    assert_eq!(committed.len(), 1, "exactly one suppressed Committed");
    assert_eq!(
        receipts
            .iter()
            .filter(|r| r.lost_generation_eof_observed)
            .count(),
        1
    );
    assert!(suppressed > 0);
    let committed_at = committed[0];
    assert!(committed_at < observation.mutation.unknown_at);
    let capture = BackendCapturedHistory {
        key: observation.key.clone(),
        value: observation.value,
        put_invoked_at: observation.mutation.put_invoked_at,
        committed_at,
        unknown_at: observation.mutation.unknown_at,
        get_invoked_at: observation.read.get_invoked_at,
        get_value_at: observation.read.get_value_at,
        horizon: events.next(),
    };
    match check_persisted_backend_history(&capture_path, &capture) {
        Verdict::Legal { witnesses, .. } => assert_eq!(
            witnesses,
            vec![KeyWitness {
                key: observation.key.to_vec(),
                linearized: vec![1, 2],
                omitted: vec![],
            }],
        ),
        other => panic!("captured backend reply-loss history must be legal: {other:?}"),
    }
    log::info!(
        "raft_backend_reply_loss_complete accepted_connections={connections} forwarded_puts={forwarded_puts} suppressed_response_bytes={suppressed} client_write_calls={} lost_generation_eof=true same_client_get=found history=legal history_path={} event_ordinals=put:{} committed:{} unknown:{} get:{}:{} horizon={}",
        cleanup.client_write_calls,
        capture_path.display(),
        capture.put_invoked_at,
        capture.committed_at,
        capture.unknown_at,
        capture.get_invoked_at,
        capture.get_value_at,
        capture.horizon,
    );
}

#[tokio::test]
async fn mismatched_backend_relay_mapping_is_rejected_before_database_creation() {
    let cluster = ClusterId::new([110; 16]).unwrap();
    let mut listeners = Vec::new();
    for raw_id in 1..=3 {
        listeners.push(Listeners::bind(NodeId::new(raw_id).unwrap()).await.unwrap());
    }
    let directory = ClusterDirectory::initial(
        cluster,
        ShardConfig::new(2, 0).unwrap(),
        listeners
            .iter()
            .map(|l| l.mapping.endpoint.clone())
            .collect(),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let config = NodeConfig::new(
        directory,
        listeners[1].mapping.endpoint.node_id,
        path.clone(),
        OpenMode::CreateNew,
    )
    .unwrap();
    // Both configs and listeners are valid individually, but belong to
    // different nodes. The fixture must reject the mismatched ownership.
    let result = listeners[0].resolve(config);
    assert!(
        matches!(result, Err(NodeError::Config(message)) if message.contains("listener mapping mismatch"))
    );
    assert!(!path.exists(), "mapping validation precedes Storage::open");
}
