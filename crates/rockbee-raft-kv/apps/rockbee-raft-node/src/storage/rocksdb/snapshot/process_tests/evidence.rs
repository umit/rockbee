use super::*;
use openraft::{EntryPayload, RaftLogReader, Vote, storage::RaftLogStorage};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const KEYS: [&[u8]; 3] = [b"kept", b"deleted", b"new-only"];

pub(super) fn id(term: u64, index: u64) -> LogId {
    LogId::new(openraft::CommittedLeaderId::new(term, 1), index)
}

fn membership(incoming: bool) -> StoredMembership {
    let voters = if incoming { [1, 2, 4] } else { [1, 2, 3] };
    StoredMembership::new(
        Some(if incoming { id(4, 10) } else { id(3, 0) }),
        openraft::Membership::new(
            vec![BTreeSet::from(voters)],
            voters
                .into_iter()
                .map(|node| (node, openraft::BasicNode::new(format!("node-{node}"))))
                .collect::<BTreeMap<_, _>>(),
        ),
    )
}

fn put(log_id: LogId, key: &'static [u8], value: &'static [u8]) -> Entry {
    Entry {
        log_id,
        payload: EntryPayload::Normal(Command::Put {
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
        }),
    }
}

pub(super) fn entries(incoming: bool) -> Vec<Entry> {
    let membership = membership(incoming);
    let mut entries = vec![Entry {
        log_id: membership.log_id().unwrap(),
        payload: EntryPayload::Membership(membership.membership().clone()),
    }];
    if incoming {
        entries.push(put(id(4, 11), b"kept", b"new"));
        entries.push(put(id(4, 12), b"new-only", b"incoming"));
        entries.push(put(id(4, 13), b"deleted", b"was-present"));
    } else {
        entries.push(put(id(3, 1), b"kept", b"old"));
        entries.push(put(id(3, 2), b"deleted", b"was-present"));
    }
    entries.push(Entry {
        log_id: if incoming { id(4, 14) } else { id(3, 3) },
        payload: EntryPayload::Normal(Command::Delete {
            key: Bytes::from_static(b"deleted"),
        }),
    });
    entries
}

pub(super) fn sibling_entry() -> Entry {
    put(id(6, 0), b"control", b"untouched")
}

pub(super) fn validate_meta(meta: &Meta, incoming: bool) -> Result<(), String> {
    require(
        meta.last_log_id == Some(if incoming { id(4, 14) } else { id(3, 3) }),
        "snapshot applied identity mismatch",
    )?;
    require(
        meta.last_membership == membership(incoming),
        "snapshot membership mismatch",
    )?;
    require(
        !meta.snapshot_id.is_empty() && meta.snapshot_id.len() <= 128,
        "snapshot id bound",
    )
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct View {
    pub generation: u64,
    pub applied: Option<LogId>,
    pub membership: StoredMembership,
    pub values: [Option<Bytes>; 3],
}

pub(super) async fn view(group: &Arc<Group>) -> Result<View, String> {
    group
        .read(ErrorSubject::StateMachine, |group| {
            let snapshot = group.shared.db.snapshot();
            let state = state::read_record(group, &snapshot)?;
            let mut values = [None, None, None];
            for (index, key) in KEYS.into_iter().enumerate() {
                values[index] = snapshot
                    .get_cf(
                        group.cf("state")?,
                        keys::data(group.id, state.generation, key),
                    )
                    .map_err(fail)?
                    .map(Bytes::from);
            }
            Ok(View {
                generation: state.generation,
                applied: state.applied,
                membership: state.membership,
                values,
            })
        })
        .await
        .map_err(message)
}

pub(super) fn validate_old(view: &View) -> Result<(), String> {
    require(
        view.applied == Some(id(3, 3)) && view.membership == membership(false),
        "old state identity mismatch",
    )?;
    require(
        view.values == [Some(Bytes::from_static(b"old")), None, None],
        "old value/delete isolation mismatch",
    )
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Sibling {
    pub vote: Option<Vote<u64>>,
    pub applied: Option<LogId>,
    pub committed: Option<LogId>,
    pub value: Option<Bytes>,
}

pub(super) async fn sibling(stores: &mut GroupStores) -> Result<Sibling, String> {
    let read = stores
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"control"))
        .await
        .map_err(message)?;
    require(
        stores
            .state_machine
            .applied_state()
            .await
            .map_err(message)?
            .1
            == StoredMembership::default(),
        "sibling membership changed",
    )?;
    require(
        stores
            .log_store
            .try_get_log_entries(..)
            .await
            .map_err(message)?
            == vec![sibling_entry()],
        "sibling log changed",
    )?;
    let observed = Sibling {
        vote: stores.log_store.read_vote().await.map_err(message)?,
        applied: read.applied,
        committed: stores.log_store.read_committed().await.map_err(message)?,
        value: read.value,
    };
    require(
        observed == expected_sibling(),
        "sibling durable baseline changed",
    )?;
    Ok(observed)
}

fn expected_sibling() -> Sibling {
    Sibling {
        vote: Some(Vote::new(6, 1)),
        applied: Some(id(6, 0)),
        committed: Some(id(6, 0)),
        value: Some(Bytes::from_static(b"untouched")),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Imported {
    pub image: u64,
    pub file_bytes: u64,
    pub values: [Option<Bytes>; 3],
}

pub(super) fn imported(group: &Group) -> Result<Imported, String> {
    let bytes = group
        .shared
        .db
        .get_cf(
            group.cf("snapshot_meta").map_err(message)?,
            keys::meta(group.id, SEQUENCE),
        )
        .map_err(message)?
        .ok_or("missing reserved image sequence")?;
    let image = decode::<u64>(&bytes, ErrorSubject::Snapshot(None)).map_err(message)?;
    let mut values = [None, None, None];
    for (index, key) in KEYS.into_iter().enumerate() {
        values[index] = group
            .shared
            .db
            .get_cf(
                group.cf("state").map_err(message)?,
                keys::data(group.id, image, key),
            )
            .map_err(message)?
            .map(Bytes::from);
    }
    let file_bytes = fs::metadata(final_path(group, image))
        .map_err(message)?
        .len();
    require(
        file_bytes > 0 && file_bytes < 1024 * 1024,
        "missing or oversized finalized import image",
    )?;
    require(
        values
            == [
                Some(Bytes::from_static(b"new")),
                None,
                Some(Bytes::from_static(b"incoming")),
            ],
        "imported generation bytes incomplete",
    )?;
    Ok(Imported {
        image,
        file_bytes,
        values,
    })
}

pub(super) fn validate_held(held: &wire::Held) -> Result<(), String> {
    validate_meta(&held.old_meta, false)?;
    validate_meta(&held.incoming_meta, true)?;
    validate_old(&held.old)?;
    require(held.sibling == expected_sibling(), "held sibling mismatch")?;
    require(
        held.old_image != held.imported.image && held.old.generation != held.imported.image,
        "new import already published",
    )?;
    require(
        held.imported.file_bytes > 0 && held.imported.file_bytes < 1024 * 1024,
        "finalized image bound",
    )?;
    require(
        held.imported.values
            == [
                Some(Bytes::from_static(b"new")),
                None,
                Some(Bytes::from_static(b"incoming")),
            ],
        "staging receipt mismatch",
    )
}

pub(super) fn example_held() -> wire::Held {
    wire::Held {
        phase: wire::Phase::AfterRename,
        group: 1,
        old_meta: Meta {
            last_log_id: Some(id(3, 3)),
            last_membership: membership(false),
            snapshot_id: "old".into(),
        },
        incoming_meta: Meta {
            last_log_id: Some(id(4, 14)),
            last_membership: membership(true),
            snapshot_id: "new".into(),
        },
        old: View {
            generation: 0,
            applied: Some(id(3, 3)),
            membership: membership(false),
            values: [Some(Bytes::from_static(b"old")), None, None],
        },
        sibling: expected_sibling(),
        old_image: 1,
        imported: Imported {
            image: 2,
            file_bytes: 100,
            values: [
                Some(Bytes::from_static(b"new")),
                None,
                Some(Bytes::from_static(b"incoming")),
            ],
        },
        install_pending: true,
        release_sent: false,
    }
}
