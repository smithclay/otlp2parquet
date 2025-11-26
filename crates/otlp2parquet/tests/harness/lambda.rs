//! Lambda + S3 Tables smoke test harness
//!
//! Tests otlp2parquet Lambda function with AWS S3 Tables Iceberg catalog.
//!
//! ## Architecture
//! - Deploy: CloudFormation stack with Lambda function + S3 Tables bucket
//! - Send signals: POST to Lambda Function URL with OTLP payloads
//! - Verify execution: Query CloudWatch Logs for errors
//! - Verify data: DuckDB queries S3 Tables catalog + S3 Parquet files
//! - Cleanup: Delete CloudFormation stack
//!
//! ## Prerequisites
//! - AWS credentials configured (IAM role or environment variables)
//! - Lambda deployment package built (make build-lambda)
//! - S3 bucket for Lambda ZIP uploads
//!
//! ## Environment Variables
//! - `SMOKE_TEST_STACK_PREFIX`: Stack name prefix (default: "smoke-lambda")
//! - `SMOKE_TEST_AWS_REGION`: AWS region (default: "us-west-2")
//! - `SMOKE_TEST_S3_TABLES_NAMESPACE`: Iceberg namespace (default: "otel_smoke")

use super::{
    CatalogMode, CatalogType, DeploymentInfo, DuckDBVerifier, ExecutionStatus, S3Credentials,
    SmokeTestHarness, StorageBackend, StorageConfig, TestDataSet,
};
use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_cloudformation::Client as CfnClient;
use aws_sdk_cloudwatchlogs::Client as LogsClient;
use aws_sdk_s3tables::Client as S3TablesClient;
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
    /// S3 Tables namespace
    namespace: String,
    /// SAM template path
    template_path: PathBuf,
    /// AWS SDK config
    aws_config: aws_config::SdkConfig,
    /// Catalog mode (S3 Tables or plain Parquet)
    catalog_mode: CatalogMode,
}

impl LambdaHarness {
    /// Create harness with specific catalog mode
    pub async fn with_catalog_mode(catalog_mode: CatalogMode) -> Result<Self> {
        let stack_prefix =
            std::env::var("SMOKE_TEST_STACK_PREFIX").unwrap_or_else(|_| "smoke-lambda".to_string());
        let region =
            std::env::var("SMOKE_TEST_AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
        let namespace = std::env::var("SMOKE_TEST_S3_TABLES_NAMESPACE")
            .unwrap_or_else(|_| "otel_smoke".to_string());

        // Generate unique stack name for test isolation
        let test_id = uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap()
            .to_string();
        let stack_name = format!("{}-{}", stack_prefix, test_id);

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.clone()))
            .load()
            .await;

        // Choose template based on catalog mode
        let template_filename = match catalog_mode {
            CatalogMode::Enabled => "lambda-template.yaml",
            CatalogMode::None => "lambda-template-plain-parquet.yaml",
        };
        let template_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/smoke")
            .join(template_filename);

        Ok(Self {
            stack_name,
            region,
            namespace,
            template_path,
            aws_config,
            catalog_mode,
        })
    }
}

#[async_trait::async_trait]
impl SmokeTestHarness for LambdaHarness {
    async fn deploy(&self) -> Result<DeploymentInfo> {
        tracing::info!("Deploying Lambda smoke test stack: {}", self.stack_name);

        // 1. Deploy using SAM CLI (automatically packages and uploads Lambda ZIP)
        let sam_deploy = Command::new("uvx")
            .args([
                "--python",
                "3.13",
                "--from",
                "aws-sam-cli",
                "sam",
                "deploy",
                "--template-file",
                self.template_path.to_str().unwrap(),
                "--stack-name",
                &self.stack_name,
                "--region",
                &self.region,
                "--capabilities",
                "CAPABILITY_IAM",
                "--parameter-overrides",
                &format!("IcebergNamespace={}", self.namespace),
                "--resolve-s3",
                "--no-confirm-changeset",
                "--no-fail-on-empty-changeset",
                "--force-upload", // Force SAM to upload template even if hash matches
            ])
            .output()
            .await
            .context("Failed to execute SAM deploy")?;

        if !sam_deploy.status.success() {
            let stderr = String::from_utf8_lossy(&sam_deploy.stderr);
            anyhow::bail!("SAM deploy failed: {}", stderr);
        }

        tracing::info!("SAM deployment complete");

        // 2. Get stack outputs using CloudFormation SDK
        let cfn_client = CfnClient::new(&self.aws_config);

        // Wait a bit for stack to stabilize
        tokio::time::sleep(Duration::from_secs(5)).await;

        let outputs = self.get_stack_outputs(&cfn_client).await?;

        let endpoint = outputs
            .get("FunctionUrl")
            .context("FunctionUrl output not found")?
            .clone();

        // Extract catalog endpoint and bucket based on catalog mode
        let (catalog_endpoint, bucket) = match self.catalog_mode {
            CatalogMode::Enabled => {
                let s3_tables_arn = outputs
                    .get("S3TablesBucketArn")
                    .context("S3TablesBucketArn output not found")?
                    .clone();
                let s3_tables_name = outputs
                    .get("S3TablesBucketName")
                    .context("S3TablesBucketName output not found")?
                    .clone();
                (s3_tables_arn, s3_tables_name)
            }
            CatalogMode::None => {
                let data_bucket = outputs
                    .get("DataBucketName")
                    .context("DataBucketName output not found")?
                    .clone();
                // No catalog endpoint for plain Parquet mode
                ("".to_string(), data_bucket)
            }
        };

        tracing::info!("Lambda deployed successfully: {}", endpoint);

        // Strip trailing slash from Lambda Function URL to ensure consistent URL construction
        // Lambda Function URLs include trailing slash, but URL construction assumes no trailing slash
        let endpoint = endpoint.trim_end_matches('/').to_string();

        Ok(DeploymentInfo {
            endpoint,
            catalog_endpoint,
            bucket,
            namespace: self.namespace.clone(),
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
        let log_group_name = format!("/aws/lambda/{}-ingest", self.stack_name);

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
        let catalog_type = match self.catalog_mode {
            CatalogMode::Enabled => CatalogType::S3Tables,
            CatalogMode::None => CatalogType::PlainParquet,
        };

        DuckDBVerifier {
            catalog_type,
            catalog_endpoint: info.catalog_endpoint.clone(),
            storage_config: StorageConfig {
                backend: StorageBackend::S3 {
                    region: self.region.clone(),
                    bucket: info.bucket.clone(),
                    endpoint: None,
                    credentials: S3Credentials::FromEnvironment,
                },
            },
            catalog_token: None, // S3 Tables uses IAM authentication
        }
    }

    async fn cleanup(&self) -> Result<()> {
        tracing::info!("Cleaning up Lambda smoke test stack");

        let cfn_client = CfnClient::new(&self.aws_config);

        // S3 Tables cleanup only needed in catalog mode
        if self.catalog_mode == CatalogMode::Enabled {
            let s3tables_client = S3TablesClient::new(&self.aws_config);

            // Get stack outputs to find S3 Tables bucket ARN
            let outputs = match self.get_stack_outputs(&cfn_client).await {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!("Failed to get stack outputs for cleanup: {}", e);
                    // Continue with stack deletion anyway
                    cfn_client
                        .delete_stack()
                        .stack_name(&self.stack_name)
                        .send()
                        .await
                        .context("Failed to delete CloudFormation stack")?;
                    return Ok(());
                }
            };

            if let Some(bucket_arn) = outputs.get("S3TablesBucketArn") {
                tracing::info!("Deleting S3 Tables resources from bucket: {}", bucket_arn);

                // Delete all tables in the namespace
                match s3tables_client
                    .list_tables()
                    .table_bucket_arn(bucket_arn)
                    .namespace(&self.namespace)
                    .send()
                    .await
                {
                    Ok(resp) => {
                        for table in resp.tables() {
                            let table_arn = table.table_arn();
                            let table_name = table.name();
                            tracing::info!("Deleting table: {}", table_arn);
                            if let Err(e) = s3tables_client
                                .delete_table()
                                .table_bucket_arn(bucket_arn)
                                .namespace(&self.namespace)
                                .name(table_name)
                                .send()
                                .await
                            {
                                tracing::warn!("Failed to delete table {}: {}", table_arn, e);
                            }
                        }

                        // Wait for all tables to be deleted (poll until empty)
                        tracing::info!("Waiting for tables to be deleted...");
                        let mut attempts = 0;
                        loop {
                            match s3tables_client
                                .list_tables()
                                .table_bucket_arn(bucket_arn)
                                .namespace(&self.namespace)
                                .send()
                                .await
                            {
                                Ok(resp) if resp.tables().is_empty() => {
                                    tracing::info!("All tables deleted successfully");
                                    break;
                                }
                                Ok(resp) => {
                                    let remaining = resp.tables().len();
                                    tracing::info!(
                                        "Still waiting for {} tables to delete...",
                                        remaining
                                    );
                                    attempts += 1;
                                    if attempts > 30 {
                                        tracing::warn!(
                                        "Timeout waiting for tables to delete, continuing anyway"
                                    );
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                }
                                Err(e) => {
                                    tracing::warn!("Error checking table deletion status: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to list tables for cleanup: {}", e);
                    }
                }

                // Delete the namespace (only if we created it)
                tracing::info!("Deleting namespace: {}", self.namespace);
                if let Err(e) = s3tables_client
                    .delete_namespace()
                    .table_bucket_arn(bucket_arn)
                    .namespace(&self.namespace)
                    .send()
                    .await
                {
                    tracing::warn!("Failed to delete namespace {}: {}", self.namespace, e);
                } else {
                    // Wait for namespace to be deleted
                    tracing::info!("Waiting for namespace to be deleted...");
                    let mut attempts = 0;
                    loop {
                        match s3tables_client
                            .list_namespaces()
                            .table_bucket_arn(bucket_arn)
                            .send()
                            .await
                        {
                            Ok(resp) => {
                                let namespace_exists = resp
                                    .namespaces()
                                    .iter()
                                    .any(|ns| ns.namespace() == &[self.namespace.as_str()]);

                                if !namespace_exists {
                                    tracing::info!("Namespace deleted successfully");
                                    break;
                                }

                                tracing::info!("Still waiting for namespace to delete...");
                                attempts += 1;
                                if attempts > 30 {
                                    tracing::warn!(
                                        "Timeout waiting for namespace to delete, continuing anyway"
                                    );
                                    break;
                                }
                                tokio::time::sleep(Duration::from_secs(2)).await;
                            }
                            Err(e) => {
                                tracing::warn!("Error checking namespace deletion status: {}", e);
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Now delete the CloudFormation stack
        cfn_client
            .delete_stack()
            .stack_name(&self.stack_name)
            .send()
            .await
            .context("Failed to delete CloudFormation stack")?;

        tracing::info!("CloudFormation stack deletion initiated");

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
