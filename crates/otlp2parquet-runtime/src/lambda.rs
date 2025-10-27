// AWS Lambda runtime adapter
//
// Uses S3 for storage and handles Lambda function events

#[cfg(feature = "lambda")]
use aws_sdk_s3::Client;

#[cfg(feature = "lambda")]
use otlp2parquet_core::Storage;

#[cfg(feature = "lambda")]
pub struct S3Storage {
    client: Client,
    bucket: String,
}

#[cfg(feature = "lambda")]
impl S3Storage {
    pub fn new(client: Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

#[cfg(feature = "lambda")]
#[async_trait::async_trait]
impl Storage for S3Storage {
    async fn write(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(data.to_vec().into())
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("S3 write error: {}", e))?;

        Ok(())
    }
}

#[cfg(feature = "lambda")]
pub async fn run() -> anyhow::Result<()> {
    // TODO: Initialize AWS SDK
    // TODO: Set up Lambda runtime handler
    Ok(())
}
