//! Stable FNV-1a64 key routing, independent of node membership.
mod directory;
use std::hash::Hasher;

use crate::{GroupId, ProtocolError, validation::validate_key};
pub use directory::{ClusterDirectory, GroupPlacement, MAX_DIRECTORY_NODES, NodeEndpoint};
use fnv::FnvHasher;
use serde::{Deserialize, Serialize};

/// Version of the seeded FNV-1a64 mapping; changing it changes key placement.
pub const HASH_VERSION: u8 = 1;

const MAX_SHARD_COUNT: u16 = 256;

/// Validated routing settings. Persist these with the cluster configuration.
///
/// FNV is a deterministic non-security hash, not a defense against adversarial
/// keys. Membership changes never affect mapping; count or seed changes do.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawConfig", into = "RawConfig")]
pub struct ShardConfig {
    count: u16,
    seed: u64,
}
#[derive(Serialize, Deserialize)]
struct RawConfig {
    version: u8,
    count: u16,
    seed: u64,
}

impl ShardConfig {
    /// Creates version-one routing settings with an explicit seed.
    ///
    /// # Errors
    /// Returns an error unless `count` is in `1..=256`.
    pub fn new(count: u16, seed: u64) -> Result<Self, ProtocolError> {
        if !(1..=MAX_SHARD_COUNT).contains(&count) {
            return Err(ProtocolError::InvalidShardCount(count));
        }
        Ok(Self { count, seed })
    }

    /// Returns the number of data groups (metadata is excluded).
    pub const fn count(self) -> u16 {
        self.count
    }

    /// Returns the persisted seed.
    pub const fn seed(self) -> u64 {
        self.seed
    }

    /// Returns the supported mapping version.
    pub const fn version(self) -> u8 {
        HASH_VERSION
    }

    /// Maps an opaque key into data groups `1..=count`.
    ///
    /// # Errors
    /// Returns an error for an empty key or a key larger than 64 KiB.
    pub fn group_for_key(self, key: &[u8]) -> Result<GroupId, ProtocolError> {
        validate_key(key)?;
        let hash = seeded_fnv1a_hash(key, self.seed);
        Ok(GroupId::new(hash % u64::from(self.count) + 1))
    }
}

/// Calculates the persisted v1 hash before mapping it into a data group.
fn seeded_fnv1a_hash(key: &[u8], seed: u64) -> u64 {
    // v1 starts FNV-1a at its offset basis XOR the persisted seed. `write`
    // consumes exactly the opaque key bytes; `Hash::hash` would add type
    // framing and change existing group placement.
    let mut hasher = FnvHasher::with_key(0xcbf2_9ce4_8422_2325 ^ seed);
    hasher.write(key);
    hasher.finish()
}

impl TryFrom<RawConfig> for ShardConfig {
    type Error = ProtocolError;
    fn try_from(value: RawConfig) -> Result<Self, Self::Error> {
        if value.version != HASH_VERSION {
            return Err(ProtocolError::UnsupportedHashVersion(value.version));
        }
        Self::new(value.count, value.seed)
    }
}

impl From<ShardConfig> for RawConfig {
    fn from(value: ShardConfig) -> Self {
        Self {
            version: HASH_VERSION,
            count: value.count,
            seed: value.seed,
        }
    }
}
