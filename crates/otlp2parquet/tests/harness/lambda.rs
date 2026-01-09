//! Lambda + S3 smoke test harness
//!
//! Tests otlp2parquet Lambda function with AWS S3.
//!
//! ## Architecture
//! - Deploy: CloudFormation stack with Lambda function + S3 bucket
//! - Send signals: POST to Lambda Function URL with OTLP payloads
//! - Verify execution: Query CloudWatch Logs for errors
//! - Verify data: DuckDB queries S3 Parquet files directly
//! - Cleanup: Delete CloudFormation stack + temp S3 bucket
//!
//! ## Prerequisites
//! - AWS credentials configured (IAM role or environment variables)
//! - Lambda deployment package built (make build-lambda)
//!
//! ## Environment Variables
//! - `SMOKE_TEST_STACK_PREFIX`: Stack name prefix (default: "smoke-lambda")
//! - `SMOKE_TEST_AWS_REGION`: AWS region (default: "us-west-2")
//! - `SMOKE_TEST_LAMBDA_BUCKET`: (optional) S3 bucket for Lambda ZIP; auto-created if not set

use super::{
    DeploymentInfo, DuckDBVerifier, ExecutionStatus, S3Credentials, SmokeTestHarness,
    StorageBackend, StorageConfig, TestDataSet,
};
use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_cloudformation::Client as CfnClient;
use aws_sdk_cloudwatchlogs::Client as LogsClient;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;

/// Lambda smoke test harness
pub struct LambdaHarness {
    /// CloudFormation stack name
    stack_name: String,
    /// AWS region
    region: String,
    /// Prefix for test isolation
    prefix: String,
    /// S3 bucket for Lambda ZIP upload
    lambda_bucket: String,
    /// S3 key for Lambda ZIP
    lambda_key: String,
    /// AWS SDK config
    aws_config: aws_config::SdkConfig,
    /// Whether we created the lambda bucket (and should delete it on cleanup)
    owns_lambda_bucket: bool,
}

impl LambdaHarness {
    /// Create harness
    pub async fn new() -> Result<Self> {
        let stack_prefix =
            std::env::var("SMOKE_TEST_STACK_PREFIX").unwrap_or_else(|_| "smoke-lambda".to_string());
        let region =
            std::env::var("SMOKE_TEST_AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());

        // Generate unique stack name for test isolation
        let test_id = uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap()
            .to_string();
        let stack_name = format!("{}-{}", stack_prefix, test_id);
        let prefix = format!("smoke-{}/", test_id);

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.clone()))
            .load()
            .await;

        let s3_client = S3Client::new(&aws_config);

        // Use provided bucket or create a temp one
        let (lambda_bucket, owns_lambda_bucket) = if let Ok(bucket) =
            std::env::var("SMOKE_TEST_LAMBDA_BUCKET")
        {
            (bucket, false)
        } else {
            // Create a temporary bucket for the Lambda ZIP
            let temp_bucket = format!("otlp2parquet-smoke-code-{}", test_id);
            tracing::info!("Creating temporary S3 bucket: {}", temp_bucket);

            // Create bucket with location constraint for non-us-east-1 regions
            let create_result = if region == "us-east-1" {
                s3_client.create_bucket().bucket(&temp_bucket).send().await
            } else {
                s3_client
                    .create_bucket()
                    .bucket(&temp_bucket)
                    .create_bucket_configuration(
                        aws_sdk_s3::types::CreateBucketConfiguration::builder()
                            .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(
                                region.as_str(),
                            ))
                            .build(),
                    )
                    .send()
                    .await
            };

            create_result.context("Failed to create temporary S3 bucket for Lambda code")?;

            tracing::info!("Created temporary bucket: {}", temp_bucket);
            (temp_bucket, true)
        };

        let lambda_key = format!("smoke-tests/{}/bootstrap-arm64.zip", stack_name);

        let zip_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/lambda/bootstrap-arm64.zip");

        tracing::info!(
            "Uploading Lambda ZIP to s3://{}/{}",
            lambda_bucket,
            lambda_key
        );

        let body = ByteStream::from_path(&zip_path)
            .await
            .context("Failed to read Lambda ZIP file")?;

        s3_client
            .put_object()
            .bucket(&lambda_bucket)
            .key(&lambda_key)
            .body(body)
            .send()
            .await
            .context("Failed to upload Lambda ZIP to S3")?;

        Ok(Self {
            stack_name,
            region,
            prefix,
            lambda_bucket,
            lambda_key,
            aws_config,
            owns_lambda_bucket,
        })
    }
}

#[async_trait::async_trait]
impl SmokeTestHarness for LambdaHarness {
    async fn deploy(&self) -> Result<DeploymentInfo> {
        tracing::info!("Deploying Lambda smoke test stack: {}", self.stack_name);

        // Read and render the template
        let template_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("templates/cloudformation.yaml");
        let template_content = std::fs::read_to_string(&template_path)
            .context("Failed to read CloudFormation template")?;

        // Generate unique bucket name for data storage
        let data_bucket_name = format!("otlp2parquet-smoke-{}", &self.stack_name);

        // Set prefix for test isolation
        let storage_prefix = self.prefix.clone();

        // Use 's3' source for smoke tests (tests locally built binary, not GitHub release)
        let rendered = template_content
            .replace("{{STACK_NAME}}", &self.stack_name)
            .replace("{{BUCKET_NAME}}", &data_bucket_name)
            .replace("{{CATALOG_MODE}}", "none")
            .replace("{{STORAGE_PREFIX}}", &storage_prefix)
            .replace("{{LOG_RETENTION}}", "7")
            .replace("{{LAMBDA_VERSION}}", "latest");

        // Write rendered template to temp file
        let temp_template = std::env::temp_dir().join(format!("{}-template.yaml", self.stack_name));
        std::fs::write(&temp_template, &rendered).context("Failed to write rendered template")?;

        // Deploy using CloudFormation CLI
        let temp_template_str = temp_template
            .to_str()
            .context("Temp template path contains invalid UTF-8")?;

        let deploy = Command::new("aws")
            .args([
                "cloudformation",
                "deploy",
                "--template-file",
                temp_template_str,
                "--stack-name",
                &self.stack_name,
                "--region",
                &self.region,
                "--capabilities",
                "CAPABILITY_IAM",
                "--parameter-overrides",
                "AuthType=NONE",
                "LambdaSource=s3",
                &format!("LambdaS3Bucket={}", self.lambda_bucket),
                &format!("LambdaS3Key={}", self.lambda_key),
                "--no-fail-on-empty-changeset",
            ])
            .output()
            .await
            .context("Failed to execute CloudFormation deploy")?;

        if !deploy.status.success() {
            let stderr = String::from_utf8_lossy(&deploy.stderr);
            let stdout = String::from_utf8_lossy(&deploy.stdout);
            anyhow::bail!(
                "CloudFormation deploy failed:\nstderr: {}\nstdout: {}",
                stderr,
                stdout
            );
        }

        tracing::info!("CloudFormation deployment complete");

        // Clean up temp template
        let _ = std::fs::remove_file(&temp_template);

        // Get stack outputs using CloudFormation SDK
        let cfn_client = CfnClient::new(&self.aws_config);

        // Wait a bit for stack to stabilize
        tokio::time::sleep(Duration::from_secs(5)).await;

        let outputs = self.get_stack_outputs(&cfn_client).await?;

        let endpoint = outputs
            .get("FunctionUrl")
            .context("FunctionUrl output not found")?
            .clone();

        // Extract bucket name from outputs
        let bucket = outputs
            .get("DataBucketName")
            .context("DataBucketName output not found")?
            .clone();

        tracing::info!("Lambda deployed successfully: {}", endpoint);

        // Strip trailing slash from Lambda Function URL to ensure consistent URL construction
        // Lambda Function URLs include trailing slash, but URL construction assumes no trailing slash
        let endpoint = endpoint.trim_end_matches('/').to_string();

        Ok(DeploymentInfo {
            endpoint,
            bucket,
            prefix: self.prefix.clone(),
            resource_ids: HashMap::from([("stack_name".to_string(), self.stack_name.clone())]),
        })
    }

    async fn send_signals(&self, endpoint: &str) -> Result<()> {
        tracing::info!("Sending OTLP signals to Lambda");

        let testdata = TestDataSet::load();
        let client = reqwest::Client::new();

        // Send logs (protobuf)
        client
            .post(format!("{}/v1/logs", endpoint))
            .header("Content-Type", "application/x-protobuf")
            .body(testdata.logs_pb.to_vec())
            .send()
            .await
            .context("Failed to send logs protobuf")?
            .error_for_status()
            .context("Logs protobuf request failed")?;

        tracing::info!("Sent logs (protobuf)");

        // Send metrics (gauge, sum, histogram)
        client
            .post(format!("{}/v1/metrics", endpoint))
            .header("Content-Type", "application/x-protobuf")
            .body(testdata.metrics_gauge_pb.to_vec())
            .send()
            .await
            .context("Failed to send metrics gauge")?
            .error_for_status()
            .context("Metrics gauge request failed")?;

        tracing::info!("Sent metrics (gauge)");

        // Send traces (protobuf)
        client
            .post(format!("{}/v1/traces", endpoint))
            .header("Content-Type", "application/x-protobuf")
            .body(testdata.traces_pb.to_vec())
            .send()
            .await
            .context("Failed to send traces protobuf")?
            .error_for_status()
            .context("Traces protobuf request failed")?;

        tracing::info!("Sent traces (protobuf)");

        // Wait for Lambda to process and commit to S3 Tables catalog
        // Lambda is asynchronous - needs time to write Parquet and update catalog
        tracing::info!("Waiting for Lambda processing and catalog commits...");
        tokio::time::sleep(Duration::from_secs(10)).await;

        Ok(())
    }

    async fn verify_execution(&self) -> Result<ExecutionStatus> {
        tracing::info!("Verifying Lambda execution via CloudWatch Logs");

        let logs_client = LogsClient::new(&self.aws_config);
        let log_group_name = format!("/aws/lambda/{}", self.stack_name);

        // Wait a bit for logs to be available
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Query recent log events
        // Filter pattern matches actual ERROR/WARN level logs, not debug logs containing those words
        // Use "ERROR " and "WARN " with trailing space to match log levels specifically
        let events = logs_client
            .filter_log_events()
            .log_group_name(&log_group_name)
            .filter_pattern("?\"ERROR \" ?\"WARN \"")
            .start_time((chrono::Utc::now() - chrono::Duration::minutes(5)).timestamp_millis())
            .send()
            .await
            .context("Failed to query CloudWatch Logs")?;

        let error_count = events.events().len();
        let sample_errors: Vec<String> = events
            .events()
            .iter()
            .take(5)
            .filter_map(|e| e.message().map(|s| s.to_string()))
            .collect();

        // Also get all INFO logs to see what Lambda is actually doing
        let all_events = logs_client
            .filter_log_events()
            .log_group_name(&log_group_name)
            .start_time((chrono::Utc::now() - chrono::Duration::minutes(5)).timestamp_millis())
            .send()
            .await
            .context("Failed to query all CloudWatch Logs")?;

        tracing::info!(
            "Found {} total log events, {} errors/warnings in Lambda logs",
            all_events.events().len(),
            error_count
        );

        // Print sample of all logs for debugging
        if !all_events.events().is_empty() {
            tracing::info!("Sample Lambda logs:");
            for event in all_events.events().iter().take(10) {
                if let Some(msg) = event.message() {
                    tracing::info!("  {}", msg);
                }
            }
        }

        Ok(ExecutionStatus {
            error_count,
            sample_errors,
            metrics: HashMap::new(), // Could add Lambda metrics here
        })
    }

    fn duckdb_verifier(&self, info: &DeploymentInfo) -> DuckDBVerifier {
        DuckDBVerifier {
            storage_config: StorageConfig {
                backend: StorageBackend::S3 {
                    region: self.region.clone(),
                    bucket: info.bucket.clone(),
                    endpoint: None,
                    credentials: S3Credentials::FromEnvironment,
                },
                prefix: Some(info.prefix.clone()),
            },
        }
    }

    async fn cleanup(&self) -> Result<()> {
        tracing::info!("Cleaning up Lambda smoke test stack");

        let cfn_client = CfnClient::new(&self.aws_config);

        // Delete the CloudFormation stack
        cfn_client
            .delete_stack()
            .stack_name(&self.stack_name)
            .send()
            .await
            .context("Failed to delete CloudFormation stack")?;

        tracing::info!("CloudFormation stack deletion initiated");

        // Clean up the uploaded Lambda ZIP
        let s3_client = S3Client::new(&self.aws_config);
        tracing::info!(
            "Deleting Lambda ZIP from s3://{}/{}",
            self.lambda_bucket,
            self.lambda_key
        );
        if let Err(e) = s3_client
            .delete_object()
            .bucket(&self.lambda_bucket)
            .key(&self.lambda_key)
            .send()
            .await
        {
            tracing::warn!("Failed to delete Lambda ZIP: {}", e);
        }

        // If we created the lambda bucket, delete it
        if self.owns_lambda_bucket {
            tracing::info!(
                "Deleting temporary Lambda code bucket: {}",
                self.lambda_bucket
            );
            if let Err(e) = s3_client
                .delete_bucket()
                .bucket(&self.lambda_bucket)
                .send()
                .await
            {
                tracing::warn!(
                    "Failed to delete temporary bucket {}: {}",
                    self.lambda_bucket,
                    e
                );
            } else {
                tracing::info!("Deleted temporary bucket: {}", self.lambda_bucket);
            }
        }

        Ok(())
    }
}

impl LambdaHarness {
    /// Get CloudFormation stack outputs
    async fn get_stack_outputs(&self, client: &CfnClient) -> Result<HashMap<String, String>> {
        let stacks = client
            .describe_stacks()
            .stack_name(&self.stack_name)
            .send()
            .await
            .context("Failed to describe stack")?;

        let stack = stacks.stacks().first().context("Stack not found")?;

        let mut outputs = HashMap::new();
        for output in stack.outputs() {
            if let (Some(key), Some(value)) = (output.output_key(), output.output_value()) {
                outputs.insert(key.to_string(), value.to_string());
            }
        }

        Ok(outputs)
    }
}
