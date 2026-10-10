use super::{Result, USAGE};
use std::net::SocketAddr;
use std::path::PathBuf;

const MAX_READERS: usize = 16;

pub(super) struct ProbeConfig {
    pub gateway: String,
    pub writer: String,
    pub readers: Vec<String>,
}

pub(super) struct RunConfig {
    pub bin_dir: Option<PathBuf>,
    pub writer_config: PathBuf,
    pub reader_config: PathBuf,
    pub endpoint: String,
    pub bucket: String,
}

impl ProbeConfig {
    pub(super) fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut gateway = None;
        let mut writer = None;
        let mut readers = Vec::new();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            let value = args.next().ok_or(USAGE)?;
            match flag.as_str() {
                "--gateway" if gateway.is_none() => gateway = Some(endpoint(value)?),
                "--writer" if writer.is_none() => writer = Some(endpoint(value)?),
                "--reader" if readers.len() < MAX_READERS => readers.push(endpoint(value)?),
                _ => return Err(USAGE.into()),
            }
        }
        Ok(Self {
            gateway: gateway.ok_or(USAGE)?,
            writer: writer.ok_or(USAGE)?,
            readers: (!readers.is_empty()).then_some(readers).ok_or(USAGE)?,
        })
    }
}

impl RunConfig {
    pub(super) fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut bin_dir = None;
        let mut writer_config = None;
        let mut reader_config = None;
        let mut endpoint = None;
        let mut bucket = None;
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            let value = args.next().ok_or(USAGE)?;
            match flag.as_str() {
                "--bin-dir" if bin_dir.is_none() => bin_dir = Some(PathBuf::from(value)),
                "--writer-config" if writer_config.is_none() => {
                    writer_config = Some(PathBuf::from(value))
                }
                "--reader-config" if reader_config.is_none() => {
                    reader_config = Some(PathBuf::from(value))
                }
                "--endpoint" if endpoint.is_none() => endpoint = Some(value),
                "--bucket" if bucket.is_none() => bucket = Some(value),
                _ => return Err(USAGE.into()),
            }
        }
        Ok(Self {
            bin_dir,
            writer_config: writer_config.unwrap_or_else(|| PathBuf::from("configs/writer.toml")),
            reader_config: reader_config.unwrap_or_else(|| PathBuf::from("configs/reader.toml")),
            endpoint: endpoint.unwrap_or_else(|| "http://127.0.0.1:4566".to_owned()),
            bucket: bucket.unwrap_or_else(|| "rockbee-test".to_owned()),
        })
    }
}

fn endpoint(value: String) -> Result<String> {
    value
        .parse::<SocketAddr>()
        .map(|address| address.to_string())
        .map_err(|_| {
            "probe endpoint must be a literal IP address and port (IPv6 needs brackets)".into()
        })
}
