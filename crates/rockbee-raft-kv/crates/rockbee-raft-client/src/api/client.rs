use super::MutationHandle;
use crate::{
    AdmitError, ClientConfig, ClientConnectError, ClientStatsHandle, ClientStatsSnapshot, ReadError,
};
use bytes::Bytes;
use rockbee_raft_protocol::{
    KvRequest, KvResponse,
    transport::{Delivery, Link, TransportFailure, TransportFailureKind},
};
use std::{net::SocketAddr, time::Duration};
use tokio::time::Instant;

/// A clone shares one connection generation and its bounded admission.
#[derive(Clone)]
pub struct RockBeeRaftClient {
    link: Link<KvRequest, KvResponse>,
    request_timeout: Duration,
    stats: ClientStatsHandle,
}

impl RockBeeRaftClient {
    pub async fn connect(
        address: SocketAddr,
        config: ClientConfig,
    ) -> Result<Self, ClientConnectError> {
        log::info!("raft client connecting remote={address}");
        if config.request_timeout < Duration::from_millis(1)
            || config.request_timeout > Duration::from_secs(60)
        {
            log::debug!("raft client rejected invalid request timeout remote={address}");
            return Err(ClientConnectError::InvalidTimeout);
        }
        let link = match Link::connect(
            address,
            config.link,
            Instant::now() + config.request_timeout,
        )
        .await
        {
            Ok(link) => link,
            Err(error) => {
                log::debug!("raft client connection failed remote={address}");
                return Err(error.into());
            }
        };
        log::info!("raft client connected remote={address}");
        Ok(Self {
            link,
            request_timeout: config.request_timeout,
            stats: ClientStatsHandle::default(),
        })
    }

    /// Reads this connection's PUT, GET and DELETE lifecycle counters.
    ///
    /// Client clones share counters. Separately connected clients have separate
    /// counters. PUT/DELETE attempts start at the call; GET attempts start when
    /// its future is first polled. Dropping an admitted operation's observer does
    /// not suppress its eventual transport outcome.
    ///
    /// `pending` counts requests without a terminal transport decision. A
    /// completed but unpolled result may still hold admission capacity with
    /// `pending == 0`. `unknown` preserves uncertainty about a write's effect.
    /// Fields are sampled independently; accounting identities apply after work
    /// has quiesced and while `overflowed` is false. Cumulative counters saturate
    /// and set `overflowed` instead of wrapping.
    pub fn stats(&self) -> ClientStatsSnapshot {
        self.stats.snapshot()
    }

    /// Returns a shared statistics handle that can outlive every client clone.
    ///
    /// The handle retains counters only. It does not keep the connection,
    /// workers, request values or result payloads alive.
    pub fn stats_handle(&self) -> ClientStatsHandle {
        self.stats.clone()
    }

    /// Admits a PUT now. Polling or dropping its handle does not start/cancel it.
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<MutationHandle, AdmitError> {
        let key_len = key.len();
        let value_len = value.len();
        log::trace!(
            "raft client operation admitted attempt operation=put key_bytes={key_len} value_bytes={value_len}"
        );
        self.submit_mutation(KvRequest::Put { key, value })
    }

    /// Admits a DELETE now, preserving the same certainty semantics as PUT.
    pub fn delete(&self, key: Bytes) -> Result<MutationHandle, AdmitError> {
        let key_len = key.len();
        log::trace!("raft client operation admitted attempt operation=delete key_bytes={key_len}");
        self.submit_mutation(KvRequest::Delete { key })
    }

    fn submit_mutation(&self, request: KvRequest) -> Result<MutationHandle, AdmitError> {
        let (operation, key_len, value_len, stats) = match &request {
            KvRequest::Put { key, value } => {
                ("put", key.len(), Some(value.len()), self.stats.for_put())
            }
            KvRequest::Delete { key } => ("delete", key.len(), None, self.stats.for_delete()),
            KvRequest::Get { .. } => unreachable!("only mutations are submitted here"),
        };
        match self
            .link
            .try_submit_with_stats(request, Instant::now() + self.request_timeout, stats)
        {
            Ok(pending) => Ok(MutationHandle { pending }),
            Err(error) => {
                log::debug!(
                    "raft client operation admission failed operation={operation} key_bytes={key_len} value_bytes={value_len:?} kind={error:?}"
                );
                Err(error)
            }
        }
    }

    pub async fn get(&self, key: Bytes) -> Result<Option<Bytes>, ReadError> {
        let key_len = key.len();
        log::trace!("raft client operation admitted attempt operation=get key_bytes={key_len}");
        let pending = match self.link.try_submit_with_stats(
            KvRequest::Get { key },
            Instant::now() + self.request_timeout,
            self.stats.for_get(),
        ) {
            Ok(pending) => pending,
            Err(error) => {
                log::debug!(
                    "raft client operation admission failed operation=get key_bytes={key_len} kind={error:?}"
                );
                return Err(error.into());
            }
        };
        let response = match pending.await {
            Ok(response) => response,
            Err(error) => {
                log::debug!(
                    "raft client operation transport failure operation=get key_bytes={key_len} delivery={:?} kind={:?}",
                    error.delivery,
                    error.reason
                );
                return Err(error.into());
            }
        };
        match response {
            KvResponse::Value(value) => {
                log::trace!(
                    "raft client operation completed operation=get key_bytes={key_len} value_bytes={:?}",
                    value.as_ref().map(Bytes::len)
                );
                Ok(value)
            }
            KvResponse::ReadError(error) => {
                log::debug!(
                    "raft client operation remote failure operation=get key_bytes={key_len} kind={:?}",
                    error.0.reason
                );
                Err(ReadError::Remote(error))
            }
            KvResponse::Mutation(_) => {
                log::debug!(
                    "raft client operation protocol mismatch operation=get key_bytes={key_len}"
                );
                Err(ReadError::Transport(TransportFailure {
                    delivery: Delivery::MayHaveBeenSent,
                    reason: TransportFailureKind::Protocol,
                }))
            }
        }
    }

    pub async fn close(&self) {
        log::info!("raft client closing connection");
        self.link.close().await;
        log::info!("raft client connection closed");
    }
}
