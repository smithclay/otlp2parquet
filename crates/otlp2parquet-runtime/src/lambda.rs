// AWS Lambda runtime adapter
//
// Uses S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

#[cfg(feature = "lambda")]
use anyhow::Result;
#[cfg(feature = "lambda")]
use aws_sdk_s3::Client;

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

    /// Write parquet data to S3 (async, uses lambda_runtime's tokio)
    pub async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
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
pub async fn run() -> Result<()> {
    println!("Lambda runtime - using lambda_runtime's tokio");

    // TODO: Initialize AWS SDK
    // TODO: Set up Lambda runtime handler
    // Example:
    // let config = aws_config::load_from_env().await;
    // let s3_client = aws_sdk_s3::Client::new(&config);
    // let storage = S3Storage::new(s3_client, bucket);
    //
    // lambda_runtime::run(service_fn(|event| async {
    //     let parquet_bytes = otlp_to_parquet(&event.body)?;
    //     storage.write(generate_path(), &parquet_bytes).await?;
    //     Ok(())
    // })).await

    println!("Lambda handler not yet implemented");
    Ok(())
}
