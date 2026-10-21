use std::collections::BTreeMap;

use rockbee_raft_protocol::{
    GroupId,
    control::{MoveIntent, MoveJobId, MoveProgress, MoveRecord},
    routing::ClusterDirectory,
};
use serde::{Deserialize, Serialize};

use super::{
    command::{
        ConflictReason, HandoffProof, MembershipObservation, MetadataCommand, MetadataResult,
        PreparingSuccessor,
    },
    encoding, validation,
};

/// Existing directory key; its framed value format remains unchanged.
pub const DIRECTORY_KEY: &[u8] = b"rockbee/internal/initial-directory/v1";
/// Immutable bootstrap identity stored as an ordinary generation state value.
pub const ORIGIN_KEY: &[u8] = b"rockbee/internal/bootstrap-origin/v1";
/// Prefix for one bounded movement lineage per data group; followed by a u64 BE ID.
pub const JOB_KEY_PREFIX: &[u8] = b"rockbee/internal/move-job/v1/";
/// Maximum framed bytes for a current directory or immutable origin.
pub const MAX_DIRECTORY_BYTES: usize = 64 * 1024;
/// Maximum framed bytes for one immutable move and its progress.
pub const MAX_JOB_BYTES: usize = 4 * 1024;
/// Maximum framed bytes for a first move and its bounded successor preparation.
pub const MAX_STORED_MOVES_BYTES: usize = 16 * 1024;
/// Reservation for at most three effects, keys and native overhead included.
/// Applied-state record and batch header are additional integration charges.
pub const METADATA_EFFECT_RESERVATION: usize = 192 * 1024;

/// One replacement of a reserved generation-prefixed state value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataEffect {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// All effects must be persisted atomically with the Raft applied identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTransition {
    pub result: MetadataResult,
    pub effects: Vec<MetadataEffect>,
}

/// Persisted corruption is fatal to apply, unlike a conditional conflict.
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("corrupt metadata: {0}")]
    Corrupt(&'static str),
    #[error("metadata codec failure: {0}")]
    Codec(#[from] rockbee_raft_protocol::ProtocolError),
    #[error("metadata value exceeds its encoded bound")]
    EncodedLimit,
}
/// The sole runnable successor retains its original handoff evidence forever.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AcceptedSuccessor {
    pub record: MoveRecord,
    pub handoff: HandoffProof,
}

/// Mutually exclusive in-memory states; the durable encoding has no enum tag.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuccessorState {
    Preparing(Box<PreparingSuccessor>),
    Accepted(Box<AcceptedSuccessor>),
}

/// Complete first job plus at most one successor at the same durable group key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredMoves {
    pub first: MoveRecord,
    pub successor: Option<SuccessorState>,
}

impl StoredMoves {
    pub fn preparing(&self) -> Option<&PreparingSuccessor> {
        match &self.successor {
            Some(SuccessorState::Preparing(preparing)) => Some(preparing),
            _ => None,
        }
    }
}

/// Encodes a bounded group record, preserving legacy bytes without preparation.
///
/// # Errors
/// Returns an error for invalid or oversized serialized state.
pub fn encode_stored_moves(moves: &StoredMoves) -> Result<Vec<u8>, MetadataError> {
    encoding::encode_stored_moves(moves)
}

/// Decodes legacy or marked version-two group state without format fallback.
///
/// # Errors
/// Returns an error for malformed, unknown-version, or oversized state.
pub fn decode_stored_moves(bytes: &[u8]) -> Result<StoredMoves, MetadataError> {
    encoding::decode_stored_moves(bytes)
}

/// One bounded metadata snapshot, loaded only for a metadata command.
///
/// Each data group retains its first job and at most one preparing or accepted
/// successor. Apply decides a complete transition before replacing any field.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataView {
    pub(super) current: Option<ClusterDirectory>,
    pub(super) origin: Option<ClusterDirectory>,
    pub(super) jobs: BTreeMap<GroupId, MoveRecord>,
    pub(super) successors: BTreeMap<GroupId, SuccessorState>,
}

impl MetadataView {
    /// Builds a view from already decoded storage values.
    ///
    /// # Errors
    /// Returns an error for corrupt or oversized cross-record state.
    pub fn from_parts(
        current: Option<ClusterDirectory>,
        origin: Option<ClusterDirectory>,
        jobs: BTreeMap<GroupId, MoveRecord>,
    ) -> Result<Self, MetadataError> {
        let view = Self {
            current,
            origin,
            jobs,
            successors: BTreeMap::new(),
        };
        validation::validate_view(&view)?;
        Ok(view)
    }

    /// Builds a checked view from bounded legacy or version-two group records.
    ///
    /// # Errors
    /// Returns an error for corrupt or inconsistent cross-record state.
    pub fn from_stored_parts(
        current: Option<ClusterDirectory>,
        origin: Option<ClusterDirectory>,
        records: BTreeMap<GroupId, StoredMoves>,
    ) -> Result<Self, MetadataError> {
        let limit = current
            .as_ref()
            .map_or(0, |directory| usize::from(directory.shard_config.count()));
        if records.len() > limit {
            return Err(MetadataError::Corrupt("too many move records"));
        }
        let mut jobs = BTreeMap::new();
        let mut successors = BTreeMap::new();
        for (group, stored) in records {
            jobs.insert(group, stored.first);
            if let Some(candidate) = stored.successor {
                successors.insert(group, candidate);
            }
        }
        let view = Self {
            current,
            origin,
            jobs,
            successors,
        };
        validation::validate_view(&view)?;
        Ok(view)
    }

    /// Decodes capped framed state values without accepting a collection hint.
    ///
    /// The storage adapter scans only the reserved job prefix, strips each
    /// generation prefix, and supplies the group encoded in its logical key.
    /// Duplicate groups or more jobs than configured groups fail before insert.
    ///
    /// # Errors
    /// Returns an error for oversized, malformed, or inconsistent stored state.
    pub fn from_encoded<'a>(
        current: Option<&[u8]>,
        origin: Option<&[u8]>,
        jobs: impl IntoIterator<Item = (GroupId, &'a [u8])>,
    ) -> Result<Self, MetadataError> {
        let current: Option<ClusterDirectory> = current
            .map(|bytes| encoding::decode_bounded(bytes, MAX_DIRECTORY_BYTES))
            .transpose()?;
        let origin = origin
            .map(|bytes| encoding::decode_bounded(bytes, MAX_DIRECTORY_BYTES))
            .transpose()?;
        let limit = current
            .as_ref()
            .map_or(0, |directory| usize::from(directory.shard_config.count()));
        let mut records = BTreeMap::new();
        for (group, bytes) in jobs {
            if records.len() >= limit || records.contains_key(&group) {
                return Err(MetadataError::Corrupt("duplicate or excess move records"));
            }
            records.insert(group, decode_stored_moves(bytes)?);
        }
        Self::from_stored_parts(current, origin, records)
    }

    pub fn directory(&self) -> Option<&ClusterDirectory> {
        self.current.as_ref()
    }
    pub(crate) fn into_directory(self) -> Option<ClusterDirectory> {
        self.current
    }
    pub fn origin(&self) -> Option<&ClusterDirectory> {
        self.origin.as_ref()
    }
    pub fn job(&self, group: GroupId) -> Option<&MoveRecord> {
        self.accepted_successor(group)
            .map(|accepted| &accepted.record)
            .or_else(|| self.first_job(group))
    }

    /// Immutable first job, including its retained completion identity.
    pub fn first_job(&self, group: GroupId) -> Option<&MoveRecord> {
        self.jobs.get(&group)
    }

    pub fn accepted_successor(&self, group: GroupId) -> Option<&AcceptedSuccessor> {
        match self.successors.get(&group) {
            Some(SuccessorState::Accepted(accepted)) => Some(accepted),
            _ => None,
        }
    }

    /// Returns a frozen preparation without exposing it as an accepted job.
    pub fn prepared_successor(&self, group: GroupId) -> Option<&PreparingSuccessor> {
        match self.successors.get(&group) {
            Some(SuccessorState::Preparing(preparing)) => Some(preparing),
            _ => None,
        }
    }

    /// Computes effects before updating this in-memory view.
    ///
    /// # Errors
    /// Returns an error for corrupt persisted state or an encoding limit.
    pub fn apply(&mut self, command: MetadataCommand) -> Result<MetadataTransition, MetadataError> {
        validation::validate_view(self)?;
        match command {
            MetadataCommand::InitializeDirectoryIfAbsent { initial_directory } => {
                self.initialize(initial_directory)
            }
            MetadataCommand::BeginMove { intent } => self.begin(intent),
            MetadataCommand::CompleteMove {
                expected_directory_revision,
                data_group,
                job_id,
                observation,
            } => self.complete(expected_directory_revision, data_group, job_id, observation),
            MetadataCommand::PrepareSuccessor {
                predecessor_job_id,
                intent,
            } => self.prepare(predecessor_job_id, intent),
            MetadataCommand::ActivateSuccessor {
                expected_directory_revision,
                data_group,
                predecessor_job_id,
                successor_job_id,
                proof,
            } => self.activate(
                expected_directory_revision,
                data_group,
                predecessor_job_id,
                successor_job_id,
                *proof,
            ),
        }
    }

    fn initialize(
        &mut self,
        initial: ClusterDirectory,
    ) -> Result<MetadataTransition, MetadataError> {
        if initial.validate().is_err() || validation::validate_bootstrap_shape(&initial).is_err() {
            return Ok(conflict(ConflictReason::BootstrapMismatch));
        }
        encoding::check_size(&initial, MAX_DIRECTORY_BYTES)?;
        if let Some(origin) = &self.origin {
            if *origin != initial {
                return Ok(conflict(ConflictReason::BootstrapMismatch));
            }
            let revision = self
                .current
                .as_ref()
                .ok_or(MetadataError::Corrupt("origin without directory"))?
                .revision;
            return Ok(no_effects(MetadataResult::AlreadyInitialized { revision }));
        }
        if let Some(current) = &self.current {
            if *current != initial {
                return Ok(conflict(ConflictReason::BootstrapMismatch));
            }
            let effects = encoding::origin_effect(&initial)?;
            self.origin = Some(initial);
            return Ok(MetadataTransition {
                result: MetadataResult::AlreadyInitialized { revision: 1 },
                effects,
            });
        }
        let effects = encoding::initialize_effects(&initial)?;
        self.current = Some(initial.clone());
        self.origin = Some(initial);
        Ok(MetadataTransition {
            result: MetadataResult::Initialized { revision: 1 },
            effects,
        })
    }

    fn begin(&mut self, intent: MoveIntent) -> Result<MetadataTransition, MetadataError> {
        // Check durable identity before revision: a lost response may be retried
        // after this or a sibling group has already finished its move.
        if let Some(existing) = self.jobs.get(&intent.data_group) {
            return Ok(if existing.intent == intent {
                no_effects(MetadataResult::AlreadyAccepted {
                    data_group: intent.data_group,
                    job_id: intent.job_id,
                })
            } else {
                conflict(ConflictReason::ExistingMove)
            });
        }
        if intent.validate().is_err() {
            return Ok(conflict(ConflictReason::InvalidIntent));
        }
        let (Some(current), Some(_origin)) = (&self.current, &self.origin) else {
            return Ok(conflict(ConflictReason::BootstrapMismatch));
        };
        if current.revision != intent.expected_directory_revision {
            return Ok(conflict(ConflictReason::RevisionMismatch));
        }
        if !validation::same_voters(current.voters(intent.data_group), &intent.old_voters) {
            return Ok(conflict(ConflictReason::PlacementMismatch));
        }
        if validation::validate_union(
            current,
            &self.jobs,
            &self.successors,
            Some(&intent.add_node),
        )
        .is_err()
        {
            return Ok(conflict(ConflictReason::EndpointUnion));
        }
        let group = intent.data_group;
        let job_id = intent.job_id;
        let record = MoveRecord {
            intent,
            progress: MoveProgress::Pending,
        };
        let effects = encoding::job_effect(group, &record)?;
        self.jobs.insert(group, record);
        Ok(MetadataTransition {
            result: MetadataResult::Accepted {
                data_group: group,
                job_id,
            },
            effects,
        })
    }

    fn prepare(
        &mut self,
        predecessor_job_id: MoveJobId,
        intent: MoveIntent,
    ) -> Result<MetadataTransition, MetadataError> {
        let Some(first) = self.jobs.get(&intent.data_group) else {
            return Ok(conflict(ConflictReason::UnknownMove));
        };
        if first.intent.job_id != predecessor_job_id
            || first.intent.job_id == intent.job_id
            || !matches!(first.progress, MoveProgress::Completed { .. })
        {
            return Ok(conflict(ConflictReason::ExistingMove));
        }
        // An unrelated group's publication may advance the global revision.
        // An exact retry still names the original frozen preparation.
        if let Some(existing) = self.successors.get(&intent.data_group) {
            // Once accepted, preparation cannot downgrade the slot. A runtime
            // retry must first read the accepted job and resume that identity.
            return Ok(
                if matches!(existing, SuccessorState::Preparing(preparing) if preparing.intent == intent)
                {
                    no_effects(MetadataResult::AlreadyPrepared {
                        data_group: intent.data_group,
                        job_id: intent.job_id,
                    })
                } else {
                    conflict(ConflictReason::ExistingMove)
                },
            );
        }
        if intent.validate().is_err() {
            return Ok(conflict(ConflictReason::InvalidIntent));
        }
        let current = self
            .current
            .as_ref()
            .ok_or(MetadataError::Corrupt("job without directory"))?;
        if intent.expected_directory_revision != current.revision {
            return Ok(conflict(ConflictReason::RevisionMismatch));
        }
        if intent.old_voters != first.intent.target_voters
            || !validation::same_voters(current.voters(intent.data_group), &intent.old_voters)
        {
            return Ok(conflict(ConflictReason::PlacementMismatch));
        }
        if validation::validate_union(
            current,
            &self.jobs,
            &self.successors,
            Some(&intent.add_node),
        )
        .is_err()
        {
            return Ok(conflict(ConflictReason::EndpointUnion));
        }
        let group = intent.data_group;
        let job_id = intent.job_id;
        let preparation = PreparingSuccessor {
            intent,
            executors: validation::predecessor_executors(current, first)?,
        };
        let moves = StoredMoves {
            first: first.clone(),
            successor: Some(SuccessorState::Preparing(Box::new(preparation.clone()))),
        };
        let effects = encoding::stored_effect(group, &moves)?;
        self.successors
            .insert(group, SuccessorState::Preparing(Box::new(preparation)));
        Ok(MetadataTransition {
            result: MetadataResult::Prepared {
                data_group: group,
                job_id,
            },
            effects,
        })
    }

    fn activate(
        &mut self,
        expected_revision: u64,
        group: GroupId,
        predecessor_job_id: MoveJobId,
        successor_job_id: MoveJobId,
        proof: HandoffProof,
    ) -> Result<MetadataTransition, MetadataError> {
        let Some(first) = self.first_job(group) else {
            return Ok(conflict(ConflictReason::UnknownMove));
        };
        if first.intent.job_id != predecessor_job_id {
            return Ok(conflict(ConflictReason::ExistingMove));
        }
        let Some(successor) = self.successors.get(&group) else {
            return Ok(conflict(ConflictReason::UnknownMove));
        };
        if let SuccessorState::Accepted(accepted) = successor {
            return Ok(if accepted.record.intent.job_id == successor_job_id {
                no_effects(MetadataResult::AlreadyActivated {
                    data_group: group,
                    job_id: successor_job_id,
                })
            } else {
                conflict(ConflictReason::ExistingMove)
            });
        }
        let SuccessorState::Preparing(preparing) = successor else {
            return Err(MetadataError::Corrupt("unknown successor state"));
        };
        if preparing.intent.job_id != successor_job_id {
            return Ok(conflict(ConflictReason::ExistingMove));
        }
        let current = self
            .current
            .as_ref()
            .ok_or(MetadataError::Corrupt("job without directory"))?;
        if current.revision != expected_revision {
            return Ok(conflict(ConflictReason::RevisionMismatch));
        }
        if validation::validate_handoff(current, first, &preparing.intent, &proof).is_err() {
            return Ok(conflict(ConflictReason::MembershipMismatch));
        }
        let accepted = AcceptedSuccessor {
            record: MoveRecord {
                intent: preparing.intent.clone(),
                progress: MoveProgress::Pending,
            },
            handoff: proof,
        };
        let moves = StoredMoves {
            first: first.clone(),
            successor: Some(SuccessorState::Accepted(Box::new(accepted.clone()))),
        };
        let effects = encoding::stored_effect(group, &moves)?;
        self.successors
            .insert(group, SuccessorState::Accepted(Box::new(accepted)));
        Ok(MetadataTransition {
            result: MetadataResult::Activated {
                data_group: group,
                job_id: successor_job_id,
            },
            effects,
        })
    }

    fn complete(
        &mut self,
        expected_revision: u64,
        group: GroupId,
        job_id: MoveJobId,
        observation: MembershipObservation,
    ) -> Result<MetadataTransition, MetadataError> {
        let Some(first) = self.first_job(group) else {
            return Ok(conflict(ConflictReason::UnknownMove));
        };
        let accepted = self.accepted_successor(group);
        let (record, completing_successor) = if first.intent.job_id == job_id {
            (first, None)
        } else if let Some(accepted) =
            accepted.filter(|accepted| accepted.record.intent.job_id == job_id)
        {
            (&accepted.record, Some(accepted))
        } else {
            return Ok(conflict(ConflictReason::ExistingMove));
        };
        let current = self
            .current
            .as_ref()
            .ok_or(MetadataError::Corrupt("job without directory"))?;
        // Completion retries keep the first durable proof, even if a stale
        // reconciler supplies another revision or observation on its retry.
        if matches!(record.progress, MoveProgress::Completed { .. }) {
            return Ok(no_effects(MetadataResult::AlreadyCompleted {
                data_group: group,
                job_id,
                revision: current.revision,
            }));
        }
        if current.revision != expected_revision {
            return Ok(conflict(ConflictReason::RevisionMismatch));
        }
        if !observation.uniform
            || observation.voters != record.intent.target_voters
            || observation.replacement_endpoint != record.intent.add_node
        {
            return Ok(conflict(ConflictReason::MembershipMismatch));
        }
        if completing_successor.is_some_and(|accepted| {
            !validation::strictly_after(observation.membership, accepted.handoff.fence.committed)
        }) {
            return Ok(conflict(ConflictReason::MembershipMismatch));
        }
        let Some(revision) = current.revision.checked_add(1) else {
            return Ok(conflict(ConflictReason::RevisionExhausted));
        };
        let mut directory = current.clone();
        directory.revision = revision;
        if directory.node(record.intent.add_node.node_id).is_none() {
            directory.nodes.push(record.intent.add_node.clone());
            directory.nodes.sort_by_key(|node| node.node_id);
        }
        let placement = directory
            .placements
            .iter_mut()
            .find(|placement| placement.group_id == group)
            .ok_or(MetadataError::Corrupt("move group has no placement"))?;
        placement.voters = record.intent.target_voters.to_vec();
        let completed = MoveRecord {
            intent: record.intent.clone(),
            progress: MoveProgress::Completed {
                membership: observation.membership,
            },
        };
        let updated_successor = completing_successor.map(|accepted| AcceptedSuccessor {
            record: completed.clone(),
            handoff: accepted.handoff.clone(),
        });
        let effects = if let Some(accepted) = &updated_successor {
            let moves = StoredMoves {
                first: first.clone(),
                successor: Some(SuccessorState::Accepted(Box::new(accepted.clone()))),
            };
            encoding::successor_completion_effects(&directory, group, &moves)?
        } else {
            encoding::completion_effects(&directory, group, &completed)?
        };
        // Count and encode every effect before changing either slot/directory.
        self.current = Some(directory);
        if let Some(accepted) = updated_successor {
            self.successors
                .insert(group, SuccessorState::Accepted(Box::new(accepted)));
        } else {
            self.jobs.insert(group, completed);
        }
        Ok(MetadataTransition {
            result: MetadataResult::Completed {
                data_group: group,
                job_id,
                revision,
            },
            effects,
        })
    }
}

fn no_effects(result: MetadataResult) -> MetadataTransition {
    MetadataTransition {
        result,
        effects: Vec::new(),
    }
}

fn conflict(reason: ConflictReason) -> MetadataTransition {
    no_effects(MetadataResult::Conflict { reason })
}
