#![allow(clippy::unwrap_used)]
use openraft::{ErrorSubject, ErrorVerb, StorageError, testing::StoreBuilder};
use rockbee_raft_node::{
    storage::{LogStore, OpenMode, StateMachine, Storage, StorageOptions},
    types::TypeConfig,
};

pub(super) struct Builder;

impl StoreBuilder<TypeConfig, LogStore, StateMachine, tempfile::TempDir> for Builder {
    async fn build(
        &self,
    ) -> Result<(tempfile::TempDir, LogStore, StateMachine), StorageError<u64>> {
        let dir = tempfile::tempdir()
            .map_err(|e| StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
        let storage = Storage::open(
            dir.path().join("db"),
            super::identity(),
            StorageOptions::default(),
            OpenMode::CreateNew,
        )
        .await
        .map_err(|e| {
            StorageError::from_io_error(
                ErrorSubject::Store,
                ErrorVerb::Write,
                std::io::Error::other(e),
            )
        })?;
        let group = storage
            .open_group(rockbee_raft_protocol::GroupId::new(1))
            .map_err(|e| {
                StorageError::from_io_error(
                    ErrorSubject::Store,
                    ErrorVerb::Write,
                    std::io::Error::other(e),
                )
            })?;
        Ok((dir, group.log_store, group.state_machine))
    }
}
