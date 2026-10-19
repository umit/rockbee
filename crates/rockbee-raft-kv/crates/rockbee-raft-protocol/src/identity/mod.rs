//! Typed identifiers whose private fields preserve validity through serde.
//! `TryFrom` performs the same checked construction as `new`; `From` unwraps
//! valid IDs without a fallible cast. Serde uses these standard conversions.

use crate::ProtocolError;
use serde::{Deserialize, Serialize};

/// A nonzero response-correlation ID, not a deduplication token.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct RequestId(u64);

impl RequestId {
    /// Creates an identifier.
    ///
    /// # Errors
    /// Returns an error if `value` is zero.
    pub fn new(value: u64) -> Result<Self, ProtocolError> {
        if value == 0 {
            return Err(ProtocolError::InvalidIdentifier("RequestId"));
        }
        Ok(Self(value))
    }

    /// Returns the underlying integer.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for RequestId {
    type Error = ProtocolError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<RequestId> for u64 {
    fn from(value: RequestId) -> Self {
        value.get()
    }
}

/// A nonzero node identity; membership does not participate in key sharding.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct NodeId(u64);

impl NodeId {
    /// Creates an identifier.
    ///
    /// # Errors
    /// Returns an error if `value` is zero.
    pub fn new(value: u64) -> Result<Self, ProtocolError> {
        if value == 0 {
            return Err(ProtocolError::InvalidIdentifier("NodeId"));
        }
        Ok(Self(value))
    }

    /// Returns the underlying integer.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for NodeId {
    type Error = ProtocolError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<NodeId> for u64 {
    fn from(value: NodeId) -> Self {
        value.get()
    }
}

/// A cluster identity containing at least one nonzero byte.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct ClusterId([u8; 16]);
impl ClusterId {
    /// Creates a cluster identity.
    ///
    /// # Errors
    /// Returns an error for the all-zero identity.
    pub fn new(value: [u8; 16]) -> Result<Self, ProtocolError> {
        if value == [0; 16] {
            return Err(ProtocolError::InvalidIdentifier("ClusterId"));
        }
        Ok(Self(value))
    }

    /// Returns the identity bytes.
    pub const fn get(self) -> [u8; 16] {
        self.0
    }
}

impl TryFrom<[u8; 16]> for ClusterId {
    type Error = ProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ClusterId> for [u8; 16] {
    fn from(value: ClusterId) -> Self {
        value.get()
    }
}

/// A Raft group identity. Zero is reserved for metadata, not key routing.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GroupId(u64);
impl GroupId {
    /// Wraps a group number, including the reserved metadata group zero.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the group number.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Reports whether this is the metadata group.
    pub const fn is_metadata(self) -> bool {
        self.0 == 0
    }
}
