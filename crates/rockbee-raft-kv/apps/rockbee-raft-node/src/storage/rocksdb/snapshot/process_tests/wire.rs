//! Bounded private files, not diagnostic logs, carry the phase handshake.
use super::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::{Read, Write};

const MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Config {
    pub nonce: [u8; 16],
    pub root: PathBuf,
    pub files: PathBuf,
}
impl Config {
    pub(super) fn path(&self, name: &str) -> PathBuf {
        self.files.join(name)
    }
    pub(super) fn database(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
    pub(super) fn validate(&self) -> Result<(), String> {
        require(self.nonce != [0; 16], "empty child nonce")?;
        require(self.root.is_absolute(), "child root must be absolute")?;
        require(self.root.as_os_str().len() <= 4096, "child root too long")?;
        require(
            self.files.is_absolute()
                && self.files.as_os_str().len() <= 4096
                && self.files != self.root,
            "invalid receipt directory",
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Phase {
    Unverified,
    AfterRename,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Held {
    pub phase: Phase,
    pub group: u64,
    pub old_meta: Meta,
    pub incoming_meta: Meta,
    pub old: evidence::View,
    pub sibling: evidence::Sibling,
    pub old_image: u64,
    pub imported: evidence::Imported,
    pub install_pending: bool,
    pub release_sent: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Envelope<T> {
    pub nonce: [u8; 16],
    pub pid: u32,
    pub sequence: u8,
    pub body: T,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) enum Command {
    ConfirmHeld,
    Stop,
}

pub(super) fn write<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let mut buffer = vec![0; MAX_BYTES];
    let bytes = postcard::to_slice(value, &mut buffer).map_err(message)?;
    let temporary = path.with_extension("partial");
    let mut file = File::create(&temporary).map_err(message)?;
    file.write_all(bytes).map_err(message)?;
    file.sync_all().map_err(message)?;
    fs::rename(temporary, path).map_err(message)
}

pub(super) fn read<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(message(error)),
    };
    require(
        file.metadata().map_err(message)?.len() <= MAX_BYTES as u64,
        "oversized private receipt",
    )?;
    let mut bytes = Vec::with_capacity(MAX_BYTES);
    file.take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(message)?;
    require(bytes.len() <= MAX_BYTES, "oversized private receipt")?;
    let (value, trailing) = postcard::take_from_bytes(&bytes).map_err(message)?;
    require(trailing.is_empty(), "trailing private receipt bytes")?;
    Ok(Some(value))
}

pub(super) fn envelope<T>(config: &Config, sequence: u8, body: T) -> Envelope<T> {
    Envelope {
        nonce: config.nonce,
        pid: std::process::id(),
        sequence,
        body,
    }
}

pub(super) fn validate_identity<T>(
    value: &Envelope<T>,
    config: &Config,
    pid: u32,
    sequence: u8,
) -> Result<(), String> {
    require(value.nonce == config.nonce, "receipt nonce mismatch")?;
    require(value.pid == pid, "receipt PID mismatch")?;
    require(value.sequence == sequence, "receipt sequence mismatch")
}

pub(super) fn validate_phase(
    value: &Envelope<Held>,
    config: &Config,
    pid: u32,
    sequence: u8,
) -> Result<(), String> {
    validate_identity(value, config, pid, sequence)?;
    require(
        value.body.phase == Phase::AfterRename,
        "missing verified AfterRename phase receipt",
    )?;
    require(value.body.group == 1, "receipt group mismatch")?;
    require(
        value.body.install_pending && !value.body.release_sent,
        "install was not held",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrong_phase_nonce_or_pid_cannot_authorize_a_phase_cut() {
        let config = Config {
            nonce: [7; 16],
            root: PathBuf::from("/private/tmp/fixture"),
            files: PathBuf::from("/private/tmp/report"),
        };
        let good = Envelope {
            nonce: config.nonce,
            pid: 42,
            sequence: 1,
            body: evidence::example_held(),
        };
        assert!(validate_phase(&good, &config, 42, 1).is_ok());
        let mut bad = good.clone();
        bad.body.phase = Phase::Unverified;
        assert!(
            validate_phase(&bad, &config, 42, 1)
                .unwrap_err()
                .contains("AfterRename")
        );
        let mut bad = good.clone();
        bad.nonce = [8; 16];
        assert!(
            validate_phase(&bad, &config, 42, 1)
                .unwrap_err()
                .contains("nonce")
        );
        let mut bad = good;
        bad.pid = 43;
        assert!(
            validate_phase(&bad, &config, 42, 1)
                .unwrap_err()
                .contains("PID")
        );
    }
}
