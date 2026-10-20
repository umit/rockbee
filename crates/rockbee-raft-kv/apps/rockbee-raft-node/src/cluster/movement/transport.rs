//! Bounded backend requests and retained metadata seed discovery.
use super::*;

impl NodeService {
    pub(super) async fn fetch_move(
        &self,
        data_group: GroupId,
        deadline: Instant,
    ) -> Result<MoveState, ReadFailure> {
        loop {
            self.movement.check_running(deadline)?;
            if let Some(BackendReply::MoveState {
                result: Ok(state), ..
            }) = self
                .metadata_request(BackendOperation::GetMove { data_group }, deadline)
                .await
            {
                if state.directory.cluster_id != self.identity.cluster_id
                    || state.directory.shard_config != self.shard_config
                {
                    return Err(read_error(
                        FailureReason::InvalidRequest,
                        "metadata cluster/hash identity mismatch",
                    ));
                }
                return Ok(*state);
            }
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(25))).await;
        }
    }

    pub(super) async fn metadata_request(
        &self,
        operation: BackendOperation,
        deadline: Instant,
    ) -> Option<BackendReply> {
        for seed in &self.metadata_seeds {
            if self.movement.check_running(deadline).is_err() {
                return None;
            }
            let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
            if let Some(reply) = self
                .backend_request(seed.node_id, seed.address, operation.clone(), attempt)
                .await
            {
                let success = matches!(
                    &reply,
                    BackendReply::MoveState { result: Ok(_), .. }
                        | BackendReply::Move {
                            result: AdminResult::Completed { .. },
                            ..
                        }
                        | BackendReply::Move {
                            result: AdminResult::Conflict { .. },
                            ..
                        }
                );
                if success {
                    return Some(reply);
                }
            }
        }
        None
    }

    pub(super) async fn backend_request(
        &self,
        node_id: NodeId,
        address: std::net::SocketAddr,
        operation: BackendOperation,
        deadline: Instant,
    ) -> Option<BackendReply> {
        let link = BackendLink::connect(address, LinkConfig::default(), deadline)
            .await
            .ok()?;
        let response = match link.try_submit(
            BackendRequest {
                cluster_id: self.identity.cluster_id,
                target_node: node_id,
                operation,
            },
            deadline,
        ) {
            Ok(pending) => pending.await.ok(),
            Err(_) => None,
        };
        link.close().await;
        response.map(|response| response.reply)
    }
}
