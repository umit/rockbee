use rockbee_raft_protocol::{
    ReadFailure,
    transport::{AdmitError, ConnectError, TransportFailure},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientConnectError {
    #[error("request timeout must be between one millisecond and sixty seconds")]
    InvalidTimeout,
    #[error(transparent)]
    Transport(#[from] ConnectError),
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("read admission failed: {0}")]
    Admission(#[from] AdmitError),
    #[error("read transport failed: {0}")]
    Transport(#[from] TransportFailure),
    #[error("remote read failed: {0:?}")]
    Remote(ReadFailure),
}
