//! Gateway request handler and public listener lifecycle.

use std::{collections::BTreeMap, future::Future, sync::Arc};

use rockbee_raft_protocol::{
    ClusterId, Failure, FailureReason, GroupId, KvRequest, KvResponse, MutationOutcome, NodeId,
    ReadFailure,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    routing::ClusterDirectory,
    transport::server::{RequestHandler, Server},
    transport::{AdmitError, Delivery, Link, TransportFailure},
};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::Mutex,
    time::{Instant, timeout_at},
};

use crate::{
    admission::mutation_transport_failure,
    config::{GatewayConfig, GatewayConfigError},
    metrics::GatewayMetrics,
    routing::group_for_request,
};

type BackendLink = Link<BackendRequest, BackendResponse>;
type BackendSlot = Arc<Mutex<BackendSlotState>>;

#[derive(Default)]
struct BackendSlotState {
    generation: u64,
    link: Option<BackendLink>,
}

struct BackendConnection {
    generation: u64,
    link: BackendLink,
}

/// Validated gateway routing state plus bounded links to backend nodes.
pub struct Gateway {
    cluster_id: ClusterId,
    directory: ClusterDirectory,
    backend_config: rockbee_raft_protocol::transport::LinkConfig,
    /// One creation lock per node. The directory map lock is never held while
    /// dialing a backend, so one unavailable node cannot stall healthy nodes.
    links: Mutex<BTreeMap<NodeId, BackendSlot>>,
    metrics: GatewayMetrics,
}

impl Gateway {
    /// Discovers and validates the replicated directory before serving requests.
    ///
    /// Seed addresses are only bootstrap candidates. A usable directory must be
    /// returned by a backend that proves the configured cluster and node ID.
    pub async fn connect(config: &GatewayConfig, deadline: Instant) -> Result<Self, GatewayError> {
        config.validate()?;
        if deadline <= Instant::now() {
            return Err(GatewayError::StartupDeadline);
        }
        let mut links = BTreeMap::new();
        for seed in &config.seeds {
            log::debug!(
                "directory discovery node={} address={}",
                seed.node_id.get(),
                seed.address
            );
            let Ok(link) = BackendLink::connect(seed.address, config.backend_link, deadline).await
            else {
                continue;
            };
            let directory = read_directory(&link, config.cluster_id, seed.node_id, deadline).await;
            let directory = match directory {
                Some(directory) => Some(directory),
                None => {
                    let leader =
                        read_metadata_leader(&link, config.cluster_id, seed.node_id, deadline)
                            .await;
                    let Some(leader) = leader else {
                        continue;
                    };
                    let Some(leader_seed) = config
                        .seeds
                        .iter()
                        .find(|candidate| candidate.node_id == leader)
                    else {
                        continue;
                    };
                    let Ok(leader_link) =
                        BackendLink::connect(leader_seed.address, config.backend_link, deadline)
                            .await
                    else {
                        continue;
                    };
                    let result =
                        read_directory(&leader_link, config.cluster_id, leader, deadline).await;
                    if result.is_some() {
                        links.insert(
                            leader,
                            Arc::new(Mutex::new(BackendSlotState {
                                generation: 1,
                                link: Some(leader_link),
                            })),
                        );
                    }
                    result
                }
            };
            let Some(directory) = directory else {
                continue;
            };
            if directory.cluster_id != config.cluster_id
                || directory.shard_config != config.expected_shard_config
                || directory.validate().is_err()
            {
                continue;
            }
            links
                .entry(seed.node_id)
                .or_insert_with(|| Arc::new(Mutex::new(BackendSlotState::default())))
                .lock()
                .await
                .link
                .replace(link);
            log::info!(
                "gateway directory validated seed_node={}",
                seed.node_id.get()
            );
            return Ok(Self {
                cluster_id: config.cluster_id,
                directory,
                backend_config: config.backend_link,
                links: Mutex::new(links),
                metrics: GatewayMetrics::default(),
            });
        }
        log::warn!("gateway directory unavailable seeds={}", config.seeds.len());
        Err(GatewayError::DirectoryUnavailable)
    }

    /// Returns a diagnostic snapshot without exposing mutable routing state.
    pub fn metrics(&self) -> &GatewayMetrics {
        &self.metrics
    }

    /// Retires and joins every backend connection generation owned by gateway.
    pub async fn close(&self) {
        log::debug!("gateway closing backend links");
        let slots: Vec<_> = self.links.lock().await.values().cloned().collect();
        for slot in slots {
            if let Some(link) = slot.lock().await.link.clone() {
                link.close().await;
            }
        }
    }

    async fn backend_link(
        &self,
        node_id: NodeId,
        deadline: Instant,
    ) -> Result<BackendConnection, GatewayError> {
        let endpoint = self
            .directory
            .node(node_id)
            .ok_or(GatewayError::DirectoryUnavailable)?;
        let slot = self.backend_slot(node_id).await;
        let mut state = timeout_at(deadline, slot.lock())
            .await
            .map_err(|_| GatewayError::BackendDeadline)?;
        if let Some(link) = state.link.as_ref() {
            return Ok(BackendConnection {
                generation: state.generation,
                link: link.clone(),
            });
        }
        let connected = BackendLink::connect(endpoint.data_address, self.backend_config, deadline)
            .await
            .map_err(GatewayError::BackendConnect)?;
        state.generation = state.generation.wrapping_add(1);
        state.link.replace(connected.clone());
        let generation = state.generation;
        drop(state);
        log::debug!(
            "backend connected node={} address={} generation={generation}",
            node_id.get(),
            endpoint.data_address
        );
        Ok(BackendConnection {
            generation,
            link: connected,
        })
    }

    async fn replace_backend_link(
        &self,
        node_id: NodeId,
        observed_generation: u64,
        deadline: Instant,
    ) -> Result<BackendConnection, GatewayError> {
        let endpoint = self
            .directory
            .node(node_id)
            .ok_or(GatewayError::DirectoryUnavailable)?;
        let slot = self.backend_slot(node_id).await;
        let mut state = timeout_at(deadline, slot.lock())
            .await
            .map_err(|_| GatewayError::BackendDeadline)?;
        if state.generation != observed_generation
            && let Some(link) = state.link.as_ref()
        {
            return Ok(BackendConnection {
                generation: state.generation,
                link: link.clone(),
            });
        }
        let connected = BackendLink::connect(endpoint.data_address, self.backend_config, deadline)
            .await
            .map_err(GatewayError::BackendConnect)?;
        state.generation = state.generation.wrapping_add(1);
        let old = state.link.replace(connected.clone());
        let generation = state.generation;
        drop(state);
        log::debug!(
            "backend replaced node={} generation={generation} previous_generation={observed_generation}",
            node_id.get()
        );
        if let Some(old) = old {
            old.close().await;
        }
        Ok(BackendConnection {
            generation,
            link: connected,
        })
    }

    async fn backend_slot(&self, node_id: NodeId) -> BackendSlot {
        let mut slots = self.links.lock().await;
        slots
            .entry(node_id)
            .or_insert_with(|| Arc::new(Mutex::new(BackendSlotState::default())))
            .clone()
    }

    async fn route(&self, request: KvRequest, deadline: Instant) -> KvResponse {
        let Ok(group_id) = group_for_request(&self.directory, &request) else {
            self.metrics.rejected();
            return rejected(FailureReason::InvalidRequest, "key cannot be routed");
        };
        let Some(candidates) = self.directory.voters(group_id) else {
            self.metrics.rejected();
            return rejected(FailureReason::Unavailable, "directory has no group voters");
        };
        let candidates = candidates.to_vec();
        let is_mutation = !matches!(request, KvRequest::Get { .. });
        let operation = operation_name(&request);
        let mut last_read_failure = None;
        let mut last_unsent_mutation = None;
        for node_id in candidates {
            if deadline <= Instant::now() {
                break;
            }
            log::trace!(
                "route attempt operation={operation} group={} node={} remaining_us={}",
                group_id.get(),
                node_id.get(),
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_micros()
            );
            let response = self
                .route_to_node(node_id, group_id, request.clone(), deadline)
                .await;
            match response {
                Ok(KvResponse::Mutation(MutationOutcome::Rejected(failure)))
                    if failure.reason == FailureReason::NotLeader =>
                {
                    log::trace!(
                        "route redirect operation={operation} group={} node={}",
                        group_id.get(),
                        node_id.get()
                    );
                    continue;
                }
                Ok(KvResponse::ReadError(ReadFailure(failure)))
                    if failure.reason == FailureReason::NotLeader =>
                {
                    log::trace!(
                        "route redirect operation={operation} group={} node={}",
                        group_id.get(),
                        node_id.get()
                    );
                    continue;
                }
                Ok(response) => {
                    match &response {
                        KvResponse::ReadError(error) => log::debug!(
                            "backend read failed group={} node={} reason={:?}",
                            group_id.get(),
                            node_id.get(),
                            error.0.reason
                        ),
                        KvResponse::Mutation(MutationOutcome::Unknown(failure)) => log::debug!(
                            "mutation outcome unknown operation={operation} group={} node={} reason={:?} replay=false",
                            group_id.get(),
                            node_id.get(),
                            failure.reason
                        ),
                        _ => {}
                    }
                    return response;
                }
                Err(error) if is_mutation => {
                    log::debug!(
                        "backend mutation transport failed operation={operation} group={} node={} delivery={:?} reason={:?}",
                        group_id.get(),
                        node_id.get(),
                        error.delivery,
                        error.reason
                    );
                    self.metrics.backend_failure();
                    match error.delivery {
                        // This request was never handed to this candidate. It
                        // is safe to try another configured voter; this does
                        // not replay a prior attempt.
                        Delivery::NotSent => {
                            last_unsent_mutation =
                                Some(KvResponse::Mutation(mutation_transport_failure(error)));
                        }
                        // Handoff may have reached Raft. Preserve uncertainty
                        // and stop before any other node can see a replay.
                        Delivery::MayHaveBeenSent => {
                            return KvResponse::Mutation(mutation_transport_failure(error));
                        }
                    }
                }
                Err(error) => {
                    log::debug!(
                        "backend read transport failed group={} node={} delivery={:?} reason={:?}",
                        group_id.get(),
                        node_id.get(),
                        error.delivery,
                        error.reason
                    );
                    self.metrics.backend_failure();
                    last_read_failure = Some(ReadFailure::new(Failure {
                        reason: FailureReason::Unavailable,
                        message: "backend read did not return a final result".to_owned(),
                    }));
                }
            }
        }
        if is_mutation {
            last_unsent_mutation.unwrap_or_else(|| {
                rejected(
                    FailureReason::NotLeader,
                    "no candidate accepted the mutation before its deadline",
                )
            })
        } else {
            KvResponse::ReadError(last_read_failure.unwrap_or_else(|| {
                ReadFailure::new(Failure {
                    reason: FailureReason::Deadline,
                    message: "read deadline expired before a backend responded".to_owned(),
                })
            }))
        }
    }

    async fn route_to_node(
        &self,
        node_id: NodeId,
        group_id: GroupId,
        request: KvRequest,
        deadline: Instant,
    ) -> Result<KvResponse, TransportFailure> {
        let connection = self
            .backend_link(node_id, deadline)
            .await
            .map_err(|error| {
                log::debug!(
                    "backend connection unavailable group={} node={} error={error}",
                    group_id.get(),
                    node_id.get()
                );
                TransportFailure {
                    delivery: Delivery::NotSent,
                    reason: rockbee_raft_protocol::transport::TransportFailureKind::Disconnected,
                }
            })?;
        let backend_request = BackendRequest {
            cluster_id: self.cluster_id,
            target_node: node_id,
            operation: BackendOperation::Kv { group_id, request },
        };
        let pending = match connection
            .link
            .try_submit(backend_request.clone(), deadline)
        {
            Ok(pending) => pending,
            // Admission reports Closed before this request is queued or handed
            // off. Replacing the retired generation is safe for this *new*
            // request; a prior handed mutation is never submitted again.
            Err(AdmitError::Closed) => self
                .replace_backend_link(node_id, connection.generation, deadline)
                .await
                .map_err(connect_failure)?
                .link
                .try_submit(backend_request, deadline)
                .map_err(admission_failure)?,
            Err(error) => return Err(admission_failure(error)),
        };
        let response = pending.await?;
        match response.reply {
            BackendReply::Kv { response, .. } => Ok(response),
            _ => Err(TransportFailure {
                delivery: Delivery::MayHaveBeenSent,
                reason: rockbee_raft_protocol::transport::TransportFailureKind::Protocol,
            }),
        }
    }
}

async fn read_directory(
    link: &BackendLink,
    cluster_id: ClusterId,
    node_id: NodeId,
    deadline: Instant,
) -> Option<ClusterDirectory> {
    let request = BackendRequest {
        cluster_id,
        target_node: node_id,
        operation: BackendOperation::Directory,
    };
    let pending = link.try_submit(request, deadline).ok()?;
    let response = pending.await.ok()?;
    let BackendReply::Directory(Ok(directory)) = response.reply else {
        return None;
    };
    Some(directory)
}

async fn read_metadata_leader(
    link: &BackendLink,
    cluster_id: ClusterId,
    node_id: NodeId,
    deadline: Instant,
) -> Option<NodeId> {
    let request = BackendRequest {
        cluster_id,
        target_node: node_id,
        operation: BackendOperation::Leader {
            group_id: GroupId::new(0),
        },
    };
    let pending = link.try_submit(request, deadline).ok()?;
    let response = pending.await.ok()?;
    let BackendReply::Leader { group_id, leader } = response.reply else {
        return None;
    };
    if group_id.is_metadata() { leader } else { None }
}

impl RequestHandler for Gateway {
    type Request = KvRequest;
    type Response = KvResponse;

    fn handle(
        &self,
        request: KvRequest,
        deadline: Instant,
    ) -> impl Future<Output = KvResponse> + Send {
        self.metrics.accepted();
        async move {
            let operation = operation_name(&request);
            let started = log::log_enabled!(log::Level::Trace).then(Instant::now);
            let response = self.route(request, deadline).await;
            if let Some(started) = started {
                log::trace!(
                    "gateway request completed operation={operation} outcome={} elapsed_us={}",
                    response_name(&response),
                    started.elapsed().as_micros()
                );
            }
            response
        }
    }
}

/// Owns a gateway plus its public TCP server.
pub struct GatewayRuntime {
    gateway: Arc<Gateway>,
    server: Server,
}

impl GatewayRuntime {
    /// Binds the configured public address after metadata discovery succeeds.
    pub async fn start(config: GatewayConfig) -> Result<Self, GatewayError> {
        let listener = TcpListener::bind(config.listen_address)
            .await
            .map_err(GatewayError::Listen)?;
        Self::start_with_listener(config, listener).await
    }

    /// Starts with an already-bound listener, useful for owned ephemeral tests.
    pub async fn start_with_listener(
        config: GatewayConfig,
        listener: TcpListener,
    ) -> Result<Self, GatewayError> {
        let deadline = Instant::now() + config.startup_timeout;
        let gateway = Arc::new(Gateway::connect(&config, deadline).await?);
        let server = Server::start(listener, config.server, Arc::clone(&gateway))
            .map_err(GatewayError::Server)?;
        log::info!("gateway listening address={}", config.listen_address);
        Ok(Self { gateway, server })
    }

    /// Stops public acceptance, waits accepted work, then retires backend links.
    pub async fn close(&mut self) {
        self.server.close().await;
        self.gateway.close().await;
        log::info!("gateway stopped");
    }

    /// Gives diagnostics access without allowing routing mutation.
    pub fn gateway(&self) -> &Gateway {
        &self.gateway
    }
}

fn operation_name(request: &KvRequest) -> &'static str {
    match request {
        KvRequest::Put { .. } => "put",
        KvRequest::Get { .. } => "get",
        KvRequest::Delete { .. } => "delete",
    }
}

fn response_name(response: &KvResponse) -> &'static str {
    match response {
        KvResponse::Value(Some(_)) => "found",
        KvResponse::Value(None) => "absent",
        KvResponse::ReadError(_) => "read_error",
        KvResponse::Mutation(MutationOutcome::Committed) => "committed",
        KvResponse::Mutation(MutationOutcome::NotSent(_)) => "not_sent",
        KvResponse::Mutation(MutationOutcome::Rejected(_)) => "rejected",
        KvResponse::Mutation(MutationOutcome::Unknown(_)) => "unknown",
    }
}

/// Gateway startup failures; no public client request was accepted yet.
#[derive(Debug, Error)]
pub enum GatewayError {
    /// Configuration was invalid before binding or connecting.
    #[error(transparent)]
    Config(#[from] GatewayConfigError),
    /// The absolute bootstrap deadline elapsed.
    #[error("gateway startup deadline expired")]
    StartupDeadline,
    /// No seed yielded a validated directory.
    #[error("no seed returned a validated cluster directory")]
    DirectoryUnavailable,
    /// A backend connection could not be established for future work.
    #[error("backend connection failed: {0}")]
    BackendConnect(rockbee_raft_protocol::transport::ConnectError),
    /// The request budget elapsed while waiting for a per-node connection slot.
    #[error("backend connection slot deadline expired")]
    BackendDeadline,
    /// The public listener failed to bind.
    #[error("gateway listener failed: {0}")]
    Listen(std::io::Error),
    /// The typed public server rejected configuration.
    #[error("gateway server failed: {0}")]
    Server(rockbee_raft_protocol::transport::server::ServerError),
}

fn rejected(reason: FailureReason, message: &str) -> KvResponse {
    KvResponse::Mutation(MutationOutcome::Rejected(Failure {
        reason,
        message: message.to_owned(),
    }))
}

fn connect_failure(_: GatewayError) -> TransportFailure {
    TransportFailure {
        delivery: Delivery::NotSent,
        reason: rockbee_raft_protocol::transport::TransportFailureKind::Disconnected,
    }
}

fn admission_failure(error: AdmitError) -> TransportFailure {
    let reason = match error {
        AdmitError::Deadline => rockbee_raft_protocol::transport::TransportFailureKind::Deadline,
        _ => rockbee_raft_protocol::transport::TransportFailureKind::Disconnected,
    };
    TransportFailure {
        delivery: Delivery::NotSent,
        reason,
    }
}
