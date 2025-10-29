// OpenDAL-based storage implementation (SPIKE - for validation only)
//
// This is a test implementation to validate:
// 1. Binary size impact
// 2. WASM compilation compatibility
// 3. API ergonomics
//
// Philosophy: Testing whether a unified abstraction via OpenDAL is viable

#[cfg(feature = "opendal-s3")]
use opendal::{services, Operator};

#[cfg(feature = "opendal-fs")]
use opendal::{services, Operator};

#[cfg(any(feature = "opendal-s3", feature = "opendal-fs"))]
pub struct OpenDalStorage {
    operator: Operator,
}

#[cfg(any(feature = "opendal-s3", feature = "opendal-fs"))]
impl OpenDalStorage {
    /// Create storage for S3 (including R2 with custom endpoint)
    #[cfg(feature = "opendal-s3")]
    pub fn new_s3(
        bucket: &str,
        region: &str,
        endpoint: Option<&str>,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut builder = services::S3::default()
            .bucket(bucket)
            .region(region);

        if let Some(ep) = endpoint {
            builder = builder.endpoint(ep);
        }

        if let Some(key) = access_key_id {
            builder = builder.access_key_id(key);
        }

        if let Some(secret) = secret_access_key {
            builder = builder.secret_access_key(secret);
        }

        let operator = Operator::new(builder)?.finish();
        Ok(Self { operator })
    }

    /// Create storage for R2 (Cloudflare)
    #[cfg(feature = "opendal-s3")]
    pub fn new_r2(
        bucket: &str,
        account_id: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> anyhow::Result<Self> {
        let endpoint = format!("https://{}.r2.cloudflarestorage.com", account_id);
        Self::new_s3(bucket, "auto", Some(&endpoint), Some(access_key_id), Some(secret_access_key))
    }

    /// Create storage for local filesystem
    #[cfg(feature = "opendal-fs")]
    pub fn new_fs(root: &str) -> anyhow::Result<Self> {
        let builder = services::Fs::default()
            .root(root);

        let operator = Operator::new(builder)?.finish();
        Ok(Self { operator })
    }

    /// Write data to storage (async)
    pub async fn write(&self, path: &str, data: Vec<u8>) -> anyhow::Result<()> {
        self.operator.write(path, data).await?;
        Ok(())
    }

    /// Read data from storage (async)
    pub async fn read(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let data = self.operator.read(path).await?;
        Ok(data.to_vec())
    }

    /// Check if path exists
    pub async fn exists(&self, path: &str) -> anyhow::Result<bool> {
        // OpenDAL 0.54 API: use stat() and check for error
        match self.operator.stat(path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "opendal-fs")]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_opendal_fs_basic() -> anyhow::Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_root = temp_dir.join("opendal_test");

        let storage = OpenDalStorage::new_fs(test_root.to_str().unwrap())?;

        let test_data = b"Hello, OpenDAL!".to_vec();
        storage.write("test.txt", test_data.clone()).await?;

        let read_data = storage.read("test.txt").await?;
        assert_eq!(test_data, read_data);

        assert!(storage.exists("test.txt").await?);
        assert!(!storage.exists("nonexistent.txt").await?);

        Ok(())
    }
}
