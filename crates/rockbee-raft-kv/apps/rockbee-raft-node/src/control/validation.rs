//! Cross-record invariants for the first move and one frozen preparation.
use std::collections::BTreeMap;

use rockbee_raft_protocol::{
    GroupId, NodeId,
    control::{MoveProgress, MoveRecord},
    routing::{ClusterDirectory, MAX_DIRECTORY_NODES, NodeEndpoint},
};

use super::{
    encoding::check_size,
    model::{MAX_DIRECTORY_BYTES, MAX_JOB_BYTES, MetadataError, MetadataView, PreparingSuccessor},
};

pub(super) fn validate_view(view: &MetadataView) -> Result<(), MetadataError> {
    let Some(current) = &view.current else {
        return if view.origin.is_none() && view.jobs.is_empty() && view.preparing.is_empty() {
            Ok(())
        } else {
            Err(MetadataError::Corrupt("records without current directory"))
        };
    };
    validate_directory(current)?;
    let Some(origin) = &view.origin else {
        // The old bootstrap wrote only a framed revision-one directory. Its
        // exact configured origin can be adopted, but no move can begin yet.
        return if validate_bootstrap_shape(current).is_ok()
            && view.jobs.is_empty()
            && view.preparing.is_empty()
        {
            Ok(())
        } else {
            Err(MetadataError::Corrupt(
                "origin missing from moved directory",
            ))
        };
    };
    validate_directory(origin)?;
    validate_bootstrap_shape(origin)?;
    if current.cluster_id != origin.cluster_id || current.shard_config != origin.shard_config {
        return Err(MetadataError::Corrupt("immutable cluster identity changed"));
    }
    if view.jobs.len() > usize::from(origin.shard_config.count()) {
        return Err(MetadataError::Corrupt("too many move records"));
    }
    validate_jobs(view, current, origin)?;
    validate_catalog(current, origin, &view.jobs)?;
    validate_placements(current, origin, &view.jobs)?;
    validate_preparations(view, current)?;
    validate_union(current, &view.jobs, &view.preparing, None)
}

fn validate_directory(directory: &ClusterDirectory) -> Result<(), MetadataError> {
    directory.validate()?;
    check_size(directory, MAX_DIRECTORY_BYTES)?;
    Ok(())
}

/// Matches `ClusterDirectory::initial` without allocating another directory.
/// Mutable directories may grow to sixteen nodes; only the immutable origin
/// and a legacy origin-less revision-one directory require this exact shape.
pub(super) fn validate_bootstrap_shape(directory: &ClusterDirectory) -> Result<(), MetadataError> {
    let [first, second, third] = directory.nodes.as_slice() else {
        return Err(MetadataError::Corrupt(
            "bootstrap requires exactly three nodes",
        ));
    };
    let voters = [first.node_id, second.node_id, third.node_id];
    if directory.revision != 1 || voters.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(MetadataError::Corrupt(
            "bootstrap nodes are not canonical revision one",
        ));
    }
    for (index, placement) in directory.placements.iter().enumerate() {
        if u64::try_from(index).ok() != Some(placement.group_id.get()) || placement.voters != voters
        {
            return Err(MetadataError::Corrupt(
                "bootstrap placements must contain the same three voters",
            ));
        }
    }
    Ok(())
}

fn validate_jobs(
    view: &MetadataView,
    current: &ClusterDirectory,
    origin: &ClusterDirectory,
) -> Result<(), MetadataError> {
    let mut completed = 0u64;
    for (group, record) in &view.jobs {
        record.validate()?;
        check_size(record, MAX_JOB_BYTES)?;
        if group.is_metadata()
            || group.get() > u64::from(origin.shard_config.count())
            || *group != record.intent.data_group
            || record.intent.expected_directory_revision > current.revision
            || !same_voters(origin.voters(*group), &record.intent.old_voters)
        {
            return Err(MetadataError::Corrupt(
                "move record does not match origin or key",
            ));
        }
        if matches!(record.progress, MoveProgress::Completed { .. }) {
            if record.intent.expected_directory_revision >= current.revision {
                return Err(MetadataError::Corrupt(
                    "completed move has impossible acceptance revision",
                ));
            }
            completed = completed
                .checked_add(1)
                .ok_or(MetadataError::Corrupt("completed move count overflow"))?;
        }
    }
    // This slice has one lifetime move per group and no other directory
    // mutation. Every increment must therefore have one retained completion.
    if completed.checked_add(1) != Some(current.revision) {
        return Err(MetadataError::Corrupt(
            "directory revision lacks matching completed moves",
        ));
    }
    Ok(())
}

fn validate_preparations(
    view: &MetadataView,
    current: &ClusterDirectory,
) -> Result<(), MetadataError> {
    for (group, preparing) in &view.preparing {
        let first = view
            .jobs
            .get(group)
            .ok_or(MetadataError::Corrupt("preparation lacks first move"))?;
        let intent = &preparing.intent;
        intent.validate()?;
        if !matches!(first.progress, MoveProgress::Completed { .. })
            || intent.data_group != *group
            || intent.job_id == first.intent.job_id
            || intent.expected_directory_revision <= first.intent.expected_directory_revision
            || intent.expected_directory_revision > current.revision
            || intent.old_voters != first.intent.target_voters
            || !same_voters(current.voters(*group), &intent.old_voters)
        {
            return Err(MetadataError::Corrupt(
                "preparation does not follow completed first move",
            ));
        }
        if preparing.executors != predecessor_executors(current, first)? {
            return Err(MetadataError::Corrupt(
                "preparation executor census differs from predecessor",
            ));
        }
        super::encoding::prepared_size(first, preparing)?;
    }
    Ok(())
}

/// For the first move only, old voters plus its sole added node exhaust every
/// possible native executor. Resolve all four from retained exact endpoints.
pub(super) fn predecessor_executors(
    current: &ClusterDirectory,
    first: &MoveRecord,
) -> Result<[NodeEndpoint; 4], MetadataError> {
    let [a, b, c] = first.intent.old_voters;
    let mut ids = [a, b, c, first.intent.add_node.node_id];
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(MetadataError::Corrupt(
            "predecessor does not have four executors",
        ));
    }
    let endpoint = |id| {
        current.node(id).cloned().ok_or(MetadataError::Corrupt(
            "predecessor executor endpoint is absent",
        ))
    };
    Ok([
        endpoint(ids[0])?,
        endpoint(ids[1])?,
        endpoint(ids[2])?,
        endpoint(ids[3])?,
    ])
}

fn validate_catalog(
    current: &ClusterDirectory,
    origin: &ClusterDirectory,
    jobs: &BTreeMap<GroupId, MoveRecord>,
) -> Result<(), MetadataError> {
    for endpoint in &origin.nodes {
        if current.node(endpoint.node_id) != Some(endpoint) {
            return Err(MetadataError::Corrupt(
                "origin node endpoints changed or disappeared",
            ));
        }
    }
    for endpoint in &current.nodes {
        if origin.node(endpoint.node_id).is_none()
            && !jobs.values().any(|record| {
                matches!(record.progress, MoveProgress::Completed { .. })
                    && record.intent.add_node == *endpoint
            })
        {
            return Err(MetadataError::Corrupt(
                "published node lacks completed move",
            ));
        }
    }
    for record in jobs.values() {
        if matches!(record.progress, MoveProgress::Completed { .. })
            && current.node(record.intent.add_node.node_id) != Some(&record.intent.add_node)
        {
            return Err(MetadataError::Corrupt("completed move endpoint is absent"));
        }
    }
    Ok(())
}

fn validate_placements(
    current: &ClusterDirectory,
    origin: &ClusterDirectory,
    jobs: &BTreeMap<GroupId, MoveRecord>,
) -> Result<(), MetadataError> {
    for placement in &origin.placements {
        let expected = match jobs.get(&placement.group_id) {
            Some(record) if matches!(record.progress, MoveProgress::Completed { .. }) => {
                record.intent.target_voters.as_slice()
            }
            Some(record) => record.intent.old_voters.as_slice(),
            None => placement.voters.as_slice(),
        };
        if !same_voters(current.voters(placement.group_id), expected) {
            return Err(MetadataError::Corrupt(
                "placement lacks matching move progress",
            ));
        }
    }
    Ok(())
}

pub(super) fn same_voters(actual: Option<&[NodeId]>, expected: &[NodeId]) -> bool {
    actual.is_some_and(|actual| {
        actual.len() == expected.len() && expected.iter().all(|voter| actual.contains(voter))
    })
}

/// Check published and prospective nodes together, including crossed address
/// roles. Two groups may share one replacement only with identical identity.
pub(super) fn validate_union(
    current: &ClusterDirectory,
    jobs: &BTreeMap<GroupId, MoveRecord>,
    preparing: &BTreeMap<GroupId, PreparingSuccessor>,
    prospective: Option<&NodeEndpoint>,
) -> Result<(), MetadataError> {
    let mut nodes = BTreeMap::new();
    for endpoint in current
        .nodes
        .iter()
        .chain(jobs.values().map(|record| &record.intent.add_node))
        .chain(
            preparing
                .values()
                .map(|candidate| &candidate.intent.add_node),
        )
        .chain(prospective)
    {
        insert_endpoint(&mut nodes, endpoint)?;
    }
    Ok(())
}

fn insert_endpoint<'a>(
    nodes: &mut BTreeMap<NodeId, &'a NodeEndpoint>,
    endpoint: &'a NodeEndpoint,
) -> Result<(), MetadataError> {
    if let Some(existing) = nodes.get(&endpoint.node_id) {
        return if **existing == *endpoint {
            Ok(())
        } else {
            Err(MetadataError::Corrupt(
                "same node ID has different endpoints",
            ))
        };
    }
    if nodes.len() >= MAX_DIRECTORY_NODES {
        return Err(MetadataError::Corrupt(
            "prospective node union exceeds limit",
        ));
    }
    for existing in nodes.values() {
        if existing.peer_address == endpoint.peer_address
            || existing.peer_address == endpoint.data_address
            || existing.data_address == endpoint.peer_address
            || existing.data_address == endpoint.data_address
        {
            return Err(MetadataError::Corrupt(
                "prospective node endpoint collision",
            ));
        }
    }
    nodes.insert(endpoint.node_id, endpoint);
    Ok(())
}
