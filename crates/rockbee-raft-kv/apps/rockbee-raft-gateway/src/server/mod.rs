//! Gateway request handler and public listener lifecycle.

use std::{
    cmp::Ordering as RevisionOrder,
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

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
    sync::{Mutex, RwLock},
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
    directory: RwLock<Arc<ClusterDirectory>>,
    /// Completion generation, including failed and unchanged refresh attempts.
    refresh_generation: AtomicU64,
    refresh: Mutex<Result<(), RefreshFailure>>,
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
        for (index, seed) in config.seeds.iter().enumerate() {
            let candidate_deadline = shared_deadline(deadline, config.seeds.len() - index);
            log::debug!(
                "directory discovery node={} address={}",
                seed.node_id.get(),
                seed.address
            );
            let Ok(mut link) =
                BackendLink::connect(seed.address, config.backend_link, candidate_deadline).await
            else {
                continue;
            };
            let mut source = *seed;
            let mut directory =
                read_directory(&link, config.cluster_id, source.node_id, candidate_deadline).await;
            if directory.is_none() {
                let leader = read_metadata_leader(
                    &link,
                    config.cluster_id,
                    source.node_id,
                    candidate_deadline,
                )
                .await;
                // Every discarded startup generation is explicitly retired and joined.
                link.close().await;
                let Some(leader_seed) = leader.and_then(|leader| {
                    config
                        .seeds
                        .iter()
                        .find(|candidate| candidate.node_id == leader)
                }) else {
                    continue;
                };
                let Ok(leader_link) = BackendLink::connect(
                    leader_seed.address,
                    config.backend_link,
                    candidate_deadline,
                )
                .await
                else {
                    continue;
                };
                link = leader_link;
                source = *leader_seed;
                directory =
                    read_directory(&link, config.cluster_id, source.node_id, candidate_deadline)
                        .await;
            }
            let Some(directory) = directory else {
                link.close().await;
                continue;
            };
            if directory.cluster_id != config.cluster_id
                || directory.shard_config != config.expected_shard_config
                || directory.validate().is_err()
            {
                link.close().await;
                continue;
            }
            let mut links = BTreeMap::new();
            // Seeds are discovery addresses, never permission for an extra slot.
            if directory
                .node(source.node_id)
                .is_some_and(|node| node.data_address == source.address)
            {
                links.insert(
                    source.node_id,
                    Arc::new(Mutex::new(BackendSlotState {
                        generation: 1,
                        link: Some(link),
                    })),
                );
            } else {
                link.close().await;
            }
            log::info!(
                "gateway directory validated seed_node={}",
                source.node_id.get()
            );
            return Ok(Self {
                cluster_id: config.cluster_id,
                directory: RwLock::new(Arc::new(directory)),
                refresh_generation: AtomicU64::new(0),
                refresh: Mutex::new(Ok(())),
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
        directory: &ClusterDirectory,
        node_id: NodeId,
        deadline: Instant,
    ) -> Result<BackendConnection, GatewayError> {
        let endpoint = directory
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
        directory: &ClusterDirectory,
        node_id: NodeId,
        observed_generation: u64,
        deadline: Instant,
    ) -> Result<BackendConnection, GatewayError> {
        let endpoint = directory
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

    async fn directory_snapshot(&self) -> Arc<ClusterDirectory> {
        Arc::clone(&*self.directory.read().await)
    }

    async fn publish_directory(
        &self,
        next: ClusterDirectory,
    ) -> Result<Option<Arc<ClusterDirectory>>, RefreshFailure> {
        let mut current = self.directory.write().await;
        match validate_directory_update(&current, &next)? {
            RevisionOrder::Less => Ok(None),
            RevisionOrder::Equal => Ok(Some(Arc::clone(&current))),
            RevisionOrder::Greater => {
                log::info!(
                    "gateway directory refreshed old_revision={} revision={}",
                    current.revision,
                    next.revision
                );
                *current = Arc::new(next);
                Ok(Some(Arc::clone(&current)))
            }
        }
    }

    async fn refresh_directory(
        &self,
        observed_generation: u64,
        deadline: Instant,
    ) -> Result<Arc<ClusterDirectory>, RefreshFailure> {
        let mut completed = timeout_at(deadline, self.refresh.lock())
            .await
            .map_err(|_| RefreshFailure::Deadline)?;
        if self.refresh_generation.load(Ordering::Acquire) != observed_generation {
            // Joiners reuse the completed result even when no revision changed.
            (*completed)?;
            return Ok(self.directory_snapshot().await);
        }
        let result = self.fetch_directory(deadline).await;
        *completed = result.as_ref().map(|_| ()).map_err(|error| *error);
        self.refresh_generation.fetch_add(1, Ordering::Release);
        result
    }

    async fn fetch_directory(
        &self,
        deadline: Instant,
    ) -> Result<Arc<ClusterDirectory>, RefreshFailure> {
        let snapshot = self.directory_snapshot().await;
        let candidates = snapshot
            .voters(GroupId::new(0))
            .ok_or(RefreshFailure::InvalidDirectory)?;
        for (index, node_id) in candidates.iter().copied().enumerate() {
            if Instant::now() >= deadline {
                return Err(RefreshFailure::Deadline);
            }
            // Reserve a finite share for every remaining metadata voter and
            // for the request's routing pass after successful publication.
            let candidate_deadline = shared_deadline(deadline, candidates.len() - index + 1);
            let endpoint = snapshot
                .node(node_id)
                .ok_or(RefreshFailure::InvalidDirectory)?;
            // A handed timeout retires the entire Link generation. Metadata
            // subdeadlines therefore use a refresh-owned link, never the pool
            // that can carry unrelated durable mutations or reads.
            let Ok(link) = BackendLink::connect(
                endpoint.data_address,
                self.backend_config,
                candidate_deadline,
            )
            .await
            else {
                continue;
            };
            let request = BackendRequest {
                cluster_id: self.cluster_id,
                target_node: node_id,
                operation: BackendOperation::Directory,
            };
            let response = match link.try_submit(request, candidate_deadline) {
                Ok(pending) => pending.await,
                Err(error) => Err(admission_failure(error)),
            };
            // Singleflight owns at most one temporary metadata generation and
            // retires/joins it before trying another candidate or publishing.
            link.close().await;
            match response {
                Ok(BackendResponse {
                    reply: BackendReply::Directory(Ok(next)),
                    ..
                }) => {
                    if let Some(published) = self.publish_directory(next).await? {
                        return Ok(published);
                    }
                }
                Err(TransportFailure {
                    reason: rockbee_raft_protocol::transport::TransportFailureKind::Protocol,
                    ..
                }) => {
                    return Err(RefreshFailure::InvalidDirectory);
                }
                _ => {}
            }
        }
        Err(RefreshFailure::Unavailable)
    }

    async fn route(&self, request: KvRequest, deadline: Instant) -> KvResponse {
        let refresh_generation = self.refresh_generation.load(Ordering::Acquire);
        let mut snapshot = self.directory_snapshot().await;
        let Ok(group_id) = group_for_request(&snapshot, &request) else {
            self.metrics.rejected();
            return rejected(FailureReason::InvalidRequest, "key cannot be routed");
        };
        let is_mutation = !matches!(request, KvRequest::Get { .. });
        let operation = operation_name(&request);
        let mut last_read_failure = None;
        let mut last_unsent_mutation = None;
        // A request refreshes at most once and keeps its original deadline.
        for pass in 0..2 {
            let Some(candidates) = snapshot.voters(group_id) else {
                return routing_failure(
                    is_mutation,
                    FailureReason::Unavailable,
                    "directory has no group voters",
                );
            };
            for (index, node_id) in candidates.iter().copied().enumerate() {
                if deadline <= Instant::now() {
                    break;
                }
                let candidate_deadline =
                    shared_deadline(deadline, candidates.len() - index + usize::from(pass == 0));
                // Both handed reads and mutations retain the original budget:
                // a shortened shared-link deadline would retire unrelated work.
                // Only pre-handoff connection waits are shared among voters.
                log::trace!(
                    "route attempt operation={operation} group={} node={} revision={}",
                    group_id.get(),
                    node_id.get(),
                    snapshot.revision
                );
                let response = self
                    .route_to_node(
                        &snapshot,
                        node_id,
                        group_id,
                        request.clone(),
                        candidate_deadline,
                        deadline,
                    )
                    .await;
                match response {
                    Ok(KvResponse::Mutation(MutationOutcome::Rejected(failure)))
                        if failure.reason == FailureReason::NotLeader => {}
                    Ok(KvResponse::ReadError(ReadFailure(failure)))
                        if matches!(
                            failure.reason,
                            FailureReason::NotLeader
                                | FailureReason::Unavailable
                                | FailureReason::Disconnected
                                | FailureReason::Deadline
                        ) =>
                    {
                        last_read_failure = Some(ReadFailure(failure));
                    }
                    Ok(response) => {
                        // Includes Unknown and successful/missing reads. These
                        // are final; metadata discovery cannot authorize replay.
                        if let KvResponse::Mutation(MutationOutcome::Unknown(failure)) = &response {
                            log::debug!(
                                "mutation outcome unknown operation={operation} group={} node={} reason={:?} replay=false",
                                group_id.get(),
                                node_id.get(),
                                failure.reason
                            );
                        }
                        return response;
                    }
                    Err(error) if is_mutation => {
                        self.metrics.backend_failure();
                        match error.delivery {
                            Delivery::NotSent => {
                                last_unsent_mutation =
                                    Some(KvResponse::Mutation(mutation_transport_failure(error)));
                            }
                            Delivery::MayHaveBeenSent => {
                                log::debug!(
                                    "mutation transport outcome unknown operation={operation} group={} node={} replay=false",
                                    group_id.get(),
                                    node_id.get()
                                );
                                return KvResponse::Mutation(mutation_transport_failure(error));
                            }
                        }
                    }
                    Err(_) => {
                        self.metrics.backend_failure();
                        last_read_failure = Some(ReadFailure::new(Failure {
                            reason: FailureReason::Unavailable,
                            message: "backend read did not return a final result".to_owned(),
                        }));
                    }
                }
            }
            if pass == 1 || Instant::now() >= deadline {
                break;
            }
            match self.refresh_directory(refresh_generation, deadline).await {
                Ok(refreshed) if refreshed.revision > snapshot.revision => snapshot = refreshed,
                Ok(_) => break,
                Err(error) => return routing_failure(is_mutation, error.reason(), error.message()),
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
        directory: &ClusterDirectory,
        node_id: NodeId,
        group_id: GroupId,
        request: KvRequest,
        connect_deadline: Instant,
        deadline: Instant,
    ) -> Result<KvResponse, TransportFailure> {
        let response = self
            .send_backend(
                directory,
                BackendRequest {
                    cluster_id: self.cluster_id,
                    target_node: node_id,
                    operation: BackendOperation::Kv { group_id, request },
                },
                connect_deadline,
                deadline,
            )
            .await?;
        match response.reply {
            BackendReply::Kv { response, .. } => Ok(response),
            _ => Err(TransportFailure {
                delivery: Delivery::MayHaveBeenSent,
                reason: rockbee_raft_protocol::transport::TransportFailureKind::Protocol,
            }),
        }
    }

    async fn send_backend(
        &self,
        directory: &ClusterDirectory,
        request: BackendRequest,
        connect_deadline: Instant,
        deadline: Instant,
    ) -> Result<BackendResponse, TransportFailure> {
        let node_id = request.target_node;
        let connection = self
            .backend_link(directory, node_id, connect_deadline)
            .await
            .map_err(connect_failure)?;
        let pending = match connection.link.try_submit(request.clone(), deadline) {
            Ok(pending) => pending,
            // Closed admission proves this request was never handed off. Keep
            // the generation-safe replacement used by ordinary later requests.
            Err(AdmitError::Closed) => self
                .replace_backend_link(directory, node_id, connection.generation, connect_deadline)
                .await
                .map_err(connect_failure)?
                .link
                .try_submit(request, deadline)
                .map_err(admission_failure)?,
            Err(error) => return Err(admission_failure(error)),
        };
        pending.await
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
        let server = match Server::start(listener, config.server, Arc::clone(&gateway)) {
            Ok(server) => server,
            Err(error) => {
                gateway.close().await;
                return Err(GatewayError::Server(error));
            }
        };
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

    /// Returns the public server admission state for bounded test diagnostics.
    #[cfg(feature = "diagnostics")]
    pub fn admission_snapshot(
        &self,
    ) -> rockbee_raft_protocol::transport::server::ServerAdmissionSnapshot {
        self.server.admission_snapshot()
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RefreshFailure {
    Deadline,
    Unavailable,
    InvalidDirectory,
    EqualRevisionConflict,
}

impl RefreshFailure {
    fn reason(self) -> FailureReason {
        match self {
            Self::Deadline => FailureReason::Deadline,
            Self::Unavailable => FailureReason::Unavailable,
            Self::InvalidDirectory | Self::EqualRevisionConflict => FailureReason::Protocol,
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::Deadline => "directory refresh deadline expired",
            Self::Unavailable => "metadata voters returned no current directory",
            Self::InvalidDirectory => "directory refresh returned incompatible routing state",
            Self::EqualRevisionConflict => "directory revision has conflicting routing state",
        }
    }
}

fn validate_directory_update(
    current: &ClusterDirectory,
    next: &ClusterDirectory,
) -> Result<RevisionOrder, RefreshFailure> {
    if next.validate().is_err()
        || next.cluster_id != current.cluster_id
        || next.shard_config != current.shard_config
    {
        return Err(RefreshFailure::InvalidDirectory);
    }
    let order = next.revision.cmp(&current.revision);
    match order {
        RevisionOrder::Less => return Ok(order),
        RevisionOrder::Equal => {
            return if next == current {
                Ok(order)
            } else {
                Err(RefreshFailure::EqualRevisionConflict)
            };
        }
        RevisionOrder::Greater => {}
    }
    // Retain every known node, including C for metadata and sibling groups.
    if current
        .nodes
        .iter()
        .any(|node| next.node(node.node_id) != Some(node))
    {
        return Err(RefreshFailure::InvalidDirectory);
    }
    let metadata = GroupId::new(0);
    let old_voters = current
        .voters(metadata)
        .ok_or(RefreshFailure::InvalidDirectory)?;
    let new_voters = next
        .voters(metadata)
        .ok_or(RefreshFailure::InvalidDirectory)?;
    if old_voters.len() != new_voters.len()
        || old_voters.iter().any(|node| !new_voters.contains(node))
    {
        return Err(RefreshFailure::InvalidDirectory);
    }
    Ok(order)
}

fn shared_deadline(deadline: Instant, remaining_shares: usize) -> Instant {
    let now = Instant::now();
    // Directory validation bounds candidates by sixteen; this is also used for
    // at most sixteen startup seeds, with one optional refresh/routing share.
    let shares = u32::try_from(remaining_shares).unwrap_or(u32::MAX).max(1);
    now + deadline.saturating_duration_since(now) / shares
}

fn routing_failure(is_mutation: bool, reason: FailureReason, message: &str) -> KvResponse {
    if is_mutation {
        rejected(reason, message)
    } else {
        KvResponse::ReadError(ReadFailure::new(Failure {
            reason,
            message: message.to_owned(),
        }))
    }
}

#[cfg(test)]
mod tests;
