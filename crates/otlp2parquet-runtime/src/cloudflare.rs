// Cloudflare Workers runtime adapter
//
// Uses R2 for storage and handles incoming requests via Worker fetch events
//
// Philosophy: Use worker crate's JS-based runtime
// No tokio - single-threaded JavaScript-style execution model
// Entry point is #[event(fetch)] macro, not main()

#[cfg(feature = "cloudflare")]
use worker::*;

#[cfg(feature = "cloudflare")]
pub struct R2Storage {
    bucket: Bucket,
}

#[cfg(feature = "cloudflare")]
impl R2Storage {
    pub fn new(bucket: Bucket) -> Self {
        Self { bucket }
    }
}

#[cfg(feature = "cloudflare")]
impl R2Storage {
    /// Write parquet data to R2 (async, uses worker runtime)
    pub async fn write(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        self.bucket
            .put(path, data.to_vec())
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!("R2 write error: {}", e))?;
        Ok(())
    }
}

// Note: The actual Worker entry point will be defined in a separate binary
// or using the #[event(fetch)] macro in the main binary
