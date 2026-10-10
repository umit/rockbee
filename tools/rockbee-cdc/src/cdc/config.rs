use rockbee_node::backend::Backend;
use rockbee_protocol::Result;
use std::path::PathBuf;

pub const CDC_USAGE: &str = "Usage: rockbee-cdc [--backend localstack|file|s3] [--prefix PREFIX] \\
    [--bucket BUCKET] [--endpoint URL] [--directory PATH] [--cursor PATH] [--max-wal-files N]";

pub struct CdcConfig {
    pub backend: Backend,
    pub prefix: String,
    pub cursor: PathBuf,
    pub max_wal_files: Option<u64>,
    pub identity: String,
}

impl CdcConfig {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Self>> {
        let mut prefix = "rockbee".to_owned();
        let mut backend = "localstack".to_owned();
        let mut bucket = "rockbee-test".to_owned();
        let mut endpoint = "http://127.0.0.1:4566".to_owned();
        let mut directory = PathBuf::from("./objects");
        let mut cursor = PathBuf::from("./rockbee-cdc.cursor.json");
        let mut max_wal_files = None;
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            if flag == "--help" || flag == "-h" {
                return Ok(None);
            }
            let value = args.next().filter(|v| !v.is_empty()).ok_or(CDC_USAGE)?;
            match flag.as_str() {
                "--prefix" => prefix = value,
                "--backend" => backend = value,
                "--bucket" => bucket = value,
                "--endpoint" => endpoint = value,
                "--directory" => directory = value.into(),
                "--cursor" => cursor = value.into(),
                "--max-wal-files" => {
                    max_wal_files = Some(
                        value
                            .parse::<u64>()
                            .ok()
                            .filter(|n| *n > 0)
                            .ok_or(CDC_USAGE)?,
                    )
                }
                _ => return Err(CDC_USAGE.into()),
            }
        }
        let (backend, identity) = match backend.as_str() {
            "localstack" => (
                Backend::LocalStack {
                    bucket: bucket.clone(),
                    endpoint: endpoint.clone(),
                },
                serde_json::json!({"backend":"localstack","bucket":bucket,"endpoint":endpoint,"prefix":prefix}).to_string(),
            ),
            "s3" => (
                Backend::S3 {
                    bucket: bucket.clone(),
                },
                serde_json::json!({"backend":"s3","bucket":bucket,"prefix":prefix}).to_string(),
            ),
            "file" => (
                Backend::File {
                    directory: directory.clone(),
                },
                serde_json::json!({"backend":"file","directory":directory,"prefix":prefix}).to_string(),
            ),
            _ => return Err(CDC_USAGE.into()),
        };
        Ok(Some(Self {
            backend,
            prefix,
            cursor,
            max_wal_files,
            identity,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn structured_identity_cannot_collide_on_colons() {
        let a = CdcConfig::parse(
            [
                "--backend",
                "file",
                "--directory",
                "/tmp/a:b",
                "--prefix",
                "c",
            ]
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        let b = CdcConfig::parse(
            [
                "--backend",
                "file",
                "--directory",
                "/tmp/a",
                "--prefix",
                "b:c",
            ]
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_ne!(a.identity, b.identity);
        assert!(CdcConfig::parse(["--max-wal-files", "0"].map(str::to_owned)).is_err());
    }
}
