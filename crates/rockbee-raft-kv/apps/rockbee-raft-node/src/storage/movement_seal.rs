//! Local native-membership ownership and persistent predecessor seals.
//! This is not a distributed fence or permission to accept a successor job.

use std::sync::{Arc, atomic::Ordering};

use openraft::{ErrorSubject, ErrorVerb};
use rockbee_raft_protocol::control::MoveJobId;
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::OwnedSemaphorePermit;

use super::{Group, Lane, decode, encode, error, keys};
use crate::types::StoreError;

const SEAL_VERSION: u8 = 1;
const MAX_SEAL_BYTES: usize = 64;

/// Existing acceptance revision plus job identity; revisions are not job-ID order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct MovementIdentity {
    pub(crate) revision: u64,
    pub(crate) job_id: MoveJobId,
}

#[derive(Serialize, Deserialize)]
struct SealRecord {
    version: u8,
    identity: MovementIdentity,
}

/// Shares the existing group and a gate independent from its RocksDB write gate.
#[derive(Clone)]
pub(crate) struct MembershipControl {
    group: Arc<Group>,
}

/// Must remain owned throughout the entire actual native membership future.
pub(crate) struct MembershipPermit {
    _gate: OwnedSemaphorePermit,
    _group: Arc<Group>,
}

impl MembershipControl {
    pub(super) fn new(group: Arc<Group>) -> Self {
        Self { group }
    }

    #[cfg(test)]
    pub(crate) fn native_permit_available_for_test(&self) -> bool {
        self.group.membership_gate.available_permits() == 1
    }

    pub(crate) async fn begin(
        &self,
        identity: MovementIdentity,
    ) -> Result<MembershipPermit, StoreError> {
        self.validate(identity)?;
        let gate = Arc::clone(&self.group.membership_gate)
            .acquire_owned()
            .await
            .map_err(failure)?;
        let sealed = self.group.read(ErrorSubject::Store, load).await?;
        if sealed.is_some_and(|sealed| identity.revision <= sealed.revision) {
            return Err(failure("native membership identity is durably sealed"));
        }
        Ok(MembershipPermit {
            _gate: gate,
            _group: Arc::clone(&self.group),
        })
    }

    pub(crate) async fn seal(&self, identity: MovementIdentity) -> Result<(), StoreError> {
        self.validate(identity)?;
        // Declaration order matters on cancellation: session (below) marks
        // retired before this caller's final native-gate owner is dropped.
        let native_gate = Arc::new(
            Arc::clone(&self.group.membership_gate)
                .acquire_owned()
                .await
                .map_err(failure)?,
        );
        let sealed = self.group.read(ErrorSubject::Store, load).await?;
        if let Some(sealed) = sealed {
            if identity.revision < sealed.revision {
                return Ok(());
            }
            if identity.revision == sealed.revision {
                return if identity == sealed {
                    Ok(())
                } else {
                    Err(failure("same movement revision has a different sealed job"))
                };
            }
        }
        // Ordinary identity conflicts above do not retire the storage writer.
        let session = self.group.reserve(ErrorSubject::Store).await?;
        let native_owner = Arc::clone(&native_gate);
        self.group
            .write(&session, Lane::Log, ErrorSubject::Store, move |group| {
                // The blocking closure retains this owner if its caller disappears.
                let _native_owner = native_owner;
                let bytes = encode(
                    &SealRecord {
                        version: SEAL_VERSION,
                        identity,
                    },
                    ErrorSubject::Store,
                )?;
                if bytes.len() > MAX_SEAL_BYTES {
                    return Err(failure("movement seal exceeds fixed bound"));
                }
                let mut batch = WriteBatch::default();
                batch.put_cf(
                    group.cf("raft_meta")?,
                    keys::meta(group.id, keys::MOVEMENT_SEAL),
                    bytes,
                );
                group.persist(batch, ErrorSubject::Store)
            })
            .await?;
        session.complete();
        Ok(())
    }

    fn validate(&self, identity: MovementIdentity) -> Result<(), StoreError> {
        if identity.revision == 0
            || self.group.id == 0
            || self.group.id > u64::from(self.group.shared.identity.shard_config.count())
        {
            return Err(failure(
                "movement identity requires a configured data group and nonzero revision",
            ));
        }
        Ok(())
    }
}

fn load(group: &Group) -> Result<Option<MovementIdentity>, StoreError> {
    if group.retired.load(Ordering::Acquire) {
        return Err(failure(
            "native membership admission unavailable: storage group is retired",
        ));
    }
    let value = group
        .shared
        .db
        .get_pinned_cf(
            group.cf("raft_meta")?,
            keys::meta(group.id, keys::MOVEMENT_SEAL),
        )
        .map_err(failure)?;
    let Some(value) = value else {
        return Ok(None);
    };
    if value.len() > MAX_SEAL_BYTES {
        return Err(failure("stored movement seal exceeds fixed bound"));
    }
    let record: SealRecord = decode(&value, ErrorSubject::Store)?;
    if record.version != SEAL_VERSION || record.identity.revision == 0 {
        return Err(failure("invalid movement seal version or revision"));
    }
    Ok(Some(record.identity))
}

fn failure(message: impl std::fmt::Display) -> StoreError {
    error(ErrorSubject::Store, ErrorVerb::Read, message)
}

#[cfg(test)]
mod tests;
