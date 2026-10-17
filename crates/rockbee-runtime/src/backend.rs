use crate::Result;
use slatedb::object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem};
use std::path::PathBuf;
use std::sync::Arc;

/// Creates the object store. Database logic does not know about S3 credentials.
pub enum Backend {
    LocalStack { bucket: String, endpoint: String },
    S3 { bucket: String },
    File { directory: PathBuf },
}

impl Backend {
    pub fn connect(&self) -> Result<Arc<dyn ObjectStore>> {
        match self {
            Self::LocalStack { bucket, endpoint } => Ok(Arc::new(
                AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_endpoint(endpoint)
                    .with_region("us-east-1")
                    .with_access_key_id("test")
                    .with_secret_access_key("test")
                    .with_allow_http(true)
                    .with_virtual_hosted_style_request(false)
                    .build()?,
            )),
            Self::S3 { bucket } => Ok(Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()?,
            )),
            Self::File { directory } => {
                std::fs::create_dir_all(directory)?;
                Ok(Arc::new(LocalFileSystem::new_with_prefix(directory)?))
            }
        }
    }
}
