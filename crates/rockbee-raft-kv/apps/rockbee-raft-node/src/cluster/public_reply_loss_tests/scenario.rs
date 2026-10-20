//! Real runtime startup and the bounded client observation.

use super::{Events, TEST_TIMEOUT};
use crate::{cluster::NodeRuntime, config::NodeConfig, storage::OpenMode};
use bytes::Bytes;
use rockbee_raft_client::{AdmitError, ClientConfig, ReadError, RockBeeRaftClient};
use rockbee_raft_gateway::{GatewayConfig, GatewayRuntime, SeedNode};
use rockbee_raft_protocol::{
    ClusterId, FailureReason, MutationOutcome, ShardConfig,
    routing::{ClusterDirectory, NodeEndpoint},
    transport::server::ServerConfig,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, time::Instant};

use super::{proxy::ReplyLossProxy, resources::Resources};

pub(super) struct MutationObservation {
    pub(super) put_invoked_at: u64,
    pub(super) unknown_at: u64,
}

pub(super) struct ReadObservation {
    pub(super) get_invoked_at: u64,
    pub(super) get_value_at: u64,
}

pub(super) struct Observation {
    pub(super) key: Bytes,
    pub(super) value: Bytes,
    pub(super) mutation: MutationObservation,
    pub(super) retired_get_invoked_at: u64,
    pub(super) retired_get_error_at: u64,
    pub(super) fresh_read: ReadObservation,
}

pub(super) async fn start_resources(resources: &mut Resources) -> Result<SocketAddr, String> {
    let cluster = ClusterId::new([108; 16]).map_err(|error| error.to_string())?;
    let mut listeners = Vec::new();
    let mut endpoints = Vec::new();
    for raw_id in 1..=3 {
        let peer = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| error.to_string())?;
        let data = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| error.to_string())?;
        endpoints.push(NodeEndpoint {
            node_id: rockbee_raft_protocol::NodeId::new(raw_id)
                .map_err(|error| error.to_string())?,
            peer_address: peer.local_addr().map_err(|error| error.to_string())?,
            data_address: data.local_addr().map_err(|error| error.to_string())?,
        });
        listeners.push((peer, data));
    }
    let directory = ClusterDirectory::initial(
        cluster,
        ShardConfig::new(2, 0).map_err(|error| error.to_string())?,
        endpoints,
    )
    .map_err(|error| error.to_string())?;
    for (endpoint, (peer, data)) in directory.nodes.iter().zip(listeners) {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        let config = NodeConfig::new(
            directory.clone(),
            endpoint.node_id,
            root.path().join("db"),
            OpenMode::CreateNew,
        )
        .map_err(|error| error.to_string())?;
        resources.roots.push(root);
        resources.begin_node(NodeRuntime::start_with_listeners(config, peer, data));
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
    let gateway_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let gateway_address = gateway_listener
        .local_addr()
        .map_err(|error| error.to_string())?;
    resources.begin_gateway(GatewayRuntime::start_with_listener(
        GatewayConfig {
            cluster_id: directory.cluster_id,
            expected_shard_config: directory.shard_config,
            seeds: directory
                .nodes
                .iter()
                .map(|endpoint| SeedNode {
                    node_id: endpoint.node_id,
                    address: endpoint.data_address,
                })
                .collect(),
            listen_address: gateway_address,
            backend_link: Default::default(),
            server: ServerConfig::default(),
            startup_timeout: TEST_TIMEOUT,
        },
        gateway_listener,
    ));
    resources.finish_gateway().await?;
    Ok(gateway_address)
}

pub(super) async fn observe_lost_reply(
    original: &RockBeeRaftClient,
    key: Bytes,
    value: Bytes,
    events: Arc<Events>,
    expected_reason: FailureReason,
) -> Result<MutationObservation, String> {
    let put_invoked_at = events.next();
    let handle = original
        .put(key, value)
        .map_err(|error| format!("eager PUT admission: {error}"))?;
    let outcome = handle.await;
    let unknown_at = events.next();
    if !matches!(outcome, MutationOutcome::Unknown(ref failure) if failure.reason == expected_reason)
    {
        return Err(format!(
            "lost reply must be Unknown({expected_reason:?}), got {outcome:?}"
        ));
    }
    Ok(MutationObservation {
        put_invoked_at,
        unknown_at,
    })
}

pub(super) async fn observe_read(
    client: &RockBeeRaftClient,
    key: Bytes,
    value: Bytes,
    events: Arc<Events>,
) -> Result<ReadObservation, String> {
    let get_invoked_at = events.next();
    let observed = client
        .get(key)
        .await
        .map_err(|error| format!("strong GET after reply loss: {error}"))?;
    let get_value_at = events.next();
    if observed != Some(value) {
        return Err(format!(
            "committed value must survive reply loss, got {observed:?}"
        ));
    }
    Ok(ReadObservation {
        get_invoked_at,
        get_value_at,
    })
}

pub(super) async fn run_observation(
    resources: &mut Resources,
    events: Arc<Events>,
) -> Result<Observation, String> {
    let gateway_address = start_resources(resources).await?;
    resources.proxy = Some(
        ReplyLossProxy::start(gateway_address, Arc::clone(&events))
            .await
            .map_err(|error| error.to_string())?,
    );
    let key = Bytes::from_static(b"reply-loss/one-real-put");
    let value = Bytes::from_static(b"durable-before-public-reply-loss");
    let proxy_address = resources.proxy.as_ref().expect("owned proxy").address;
    let original = RockBeeRaftClient::connect(proxy_address, ClientConfig::default())
        .await
        .map_err(|error| error.to_string())?;
    resources.original = Some(original);
    let mutation = observe_lost_reply(
        resources.original.as_ref().expect("owned original client"),
        key.clone(),
        value.clone(),
        Arc::clone(&events),
        FailureReason::Disconnected,
    )
    .await?;
    let retired_get_invoked_at = events.next();
    let retired_get = resources
        .original
        .as_ref()
        .expect("owned original client")
        .get(key.clone())
        .await;
    let retired_get_error_at = events.next();
    if !matches!(&retired_get, Err(ReadError::Admission(AdmitError::Closed))) {
        return Err(format!(
            "retired original client must reject GET without reconnecting, got {retired_get:?}"
        ));
    }
    resources
        .original
        .as_ref()
        .expect("owned original client")
        .close()
        .await;

    let fresh = RockBeeRaftClient::connect(gateway_address, ClientConfig::default())
        .await
        .map_err(|error| error.to_string())?;
    resources.fresh = Some(fresh);
    let fresh_read = observe_read(
        resources.fresh.as_ref().expect("owned fresh client"),
        key.clone(),
        value.clone(),
        Arc::clone(&events),
    )
    .await?;
    resources
        .fresh
        .as_ref()
        .expect("owned fresh client")
        .close()
        .await;

    Ok(Observation {
        key,
        value,
        mutation,
        retired_get_invoked_at,
        retired_get_error_at,
        fresh_read,
    })
}
