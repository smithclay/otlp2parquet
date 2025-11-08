// AWS Lambda runtime adapter
//
// Uses OpenDAL S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use otlp2parquet_batch::PassthroughBatcher;
use otlp2parquet_config::{RuntimeConfig, StorageBackend};
use otlp2parquet_storage::iceberg::{
    IcebergCatalog, IcebergCommitter, IcebergConfig, IcebergWriter,
};
use otlp2parquet_storage::{set_parquet_row_group_size, ParquetWriter};
use std::sync::Arc;

mod handlers;
mod response;

use handlers::{canonical_path, handle_http_request};
use response::{build_api_gateway_response, build_function_url_response};
pub(crate) use response::{HttpLambdaResponse, HttpRequestEvent, HttpResponseData};

/// Lambda handler for OTLP HTTP requests
async fn handle_request(
    event: LambdaEvent<HttpRequestEvent>,
    state: Arc<LambdaState>,
) -> Result<HttpLambdaResponse, Error> {
    let (request, _context) = event.into_parts();

    match request {
        HttpRequestEvent::ApiGateway(boxed_request) => {
            let request = &*boxed_request;
            let method = request.http_method.as_str();
            let path = canonical_path(request.path.as_deref());

            // Extract Content-Type header from API Gateway request
            let content_type = request
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok());

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                &state,
            )
            .await;
            Ok(build_api_gateway_response(response))
        }
        HttpRequestEvent::FunctionUrl(boxed_request) => {
            let request = &*boxed_request;
            let method = request
                .request_context
                .http
                .method
                .as_deref()
                .unwrap_or("GET");
            let path = canonical_path(
                request
                    .raw_path
                    .as_deref()
                    .or(request.request_context.http.path.as_deref()),
            );

            // Extract Content-Type header from Function URL request
            let content_type = request
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok());

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                &state,
            )
            .await;
            Ok(build_function_url_response(response))
        }
    }
}

/// Writer mode detection based on environment variables
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterMode {
    /// S3 Tables mode with integrated Iceberg catalog commits
    Iceberg,
    /// Plain S3 mode with optional post-write catalog commits
    PlainS3,
}

/// Detect writer mode from environment variables
fn detect_writer_mode() -> WriterMode {
    // Check for all required Iceberg config variables
    if std::env::var("OTLP2PARQUET_ICEBERG_REST_URI").is_ok()
        && std::env::var("OTLP2PARQUET_ICEBERG_WAREHOUSE").is_ok()
        && std::env::var("OTLP2PARQUET_ICEBERG_NAMESPACE").is_ok()
    {
        WriterMode::Iceberg
    } else {
        WriterMode::PlainS3
    }
}

/// Unified writer that routes to either IcebergWriter or PlainS3 writer
pub(crate) enum Writer {
    /// Integrated write + commit to S3 Tables with Iceberg catalog
    Iceberg {
        writer: Arc<IcebergWriter>,
        passthrough: PassthroughBatcher,
    },
    /// Plain S3 write with optional post-write catalog commit
    PlainS3 {
        parquet_writer: Arc<ParquetWriter>,
        passthrough: PassthroughBatcher,
        iceberg_committer: Option<Arc<IcebergCommitter>>,
    },
}

impl Writer {
    /// Create Writer from environment variables and configuration
    pub async fn from_env(config: &RuntimeConfig) -> Result<Self, Error> {
        let mode = detect_writer_mode();
        println!("Detected writer mode: {:?}", mode);

        match mode {
            WriterMode::Iceberg => {
                // Build Iceberg configuration
                let rest_uri = std::env::var("OTLP2PARQUET_ICEBERG_REST_URI")
                    .map_err(|_| Error::from("OTLP2PARQUET_ICEBERG_REST_URI not set"))?;
                let warehouse = std::env::var("OTLP2PARQUET_ICEBERG_WAREHOUSE")
                    .map_err(|_| Error::from("OTLP2PARQUET_ICEBERG_WAREHOUSE not set"))?;
                let namespace_str = std::env::var("OTLP2PARQUET_ICEBERG_NAMESPACE")
                    .map_err(|_| Error::from("OTLP2PARQUET_ICEBERG_NAMESPACE not set"))?;

                let iceberg_config = IcebergConfig {
                    rest_uri: rest_uri.clone(),
                    warehouse: warehouse.clone(),
                    namespace: namespace_str.clone(),
                };

                // Get S3 config for storage
                let s3 = config
                    .storage
                    .s3
                    .as_ref()
                    .ok_or_else(|| Error::from("S3 configuration required for Iceberg mode"))?;

                // Initialize OpenDAL storage pointing to warehouse location
                let storage = Arc::new(
                    otlp2parquet_storage::opendal_storage::OpenDalStorage::new_s3(
                        &s3.bucket,
                        &s3.region,
                        s3.endpoint.as_deref(),
                        None,
                        None,
                    )
                    .map_err(|e| Error::from(format!("Failed to initialize storage: {}", e)))?,
                );

                // Initialize Iceberg catalog using init module
                use otlp2parquet_storage::iceberg::http::ReqwestHttpClient;
                let http_client = ReqwestHttpClient::new()
                    .map_err(|e| Error::from(format!("Failed to create HTTP client: {}", e)))?;

                // Parse namespace
                use otlp2parquet_storage::iceberg::catalog::NamespaceIdent;
                let namespace_parts: Vec<String> = namespace_str
                    .split('.')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect();
                let namespace = NamespaceIdent::from_vec(namespace_parts)
                    .map_err(|e| Error::from(format!("Invalid namespace: {}", e)))?;

                // Build table map (for IcebergCatalog API)
                use std::collections::HashMap;
                let mut tables = HashMap::new();
                tables.insert("logs".to_string(), "logs".to_string());
                tables.insert("traces".to_string(), "traces".to_string());
                tables.insert("metrics:gauge".to_string(), "metrics_gauge".to_string());
                tables.insert("metrics:sum".to_string(), "metrics_sum".to_string());
                tables.insert(
                    "metrics:histogram".to_string(),
                    "metrics_histogram".to_string(),
                );
                tables.insert(
                    "metrics:exponential_histogram".to_string(),
                    "metrics_exponential_histogram".to_string(),
                );
                tables.insert("metrics:summary".to_string(), "metrics_summary".to_string());

                // For S3 Tables, the prefix must be the URL-encoded table bucket ARN
                // See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html
                let prefix = urlencoding::encode(&warehouse).to_string();
                println!(
                    "Using S3 Tables prefix (URL-encoded warehouse ARN): {}",
                    prefix
                );

                let catalog = Arc::new(IcebergCatalog::new(
                    http_client,
                    rest_uri,
                    prefix,
                    namespace,
                    tables,
                ));

                // Create IcebergWriter
                let writer = Arc::new(IcebergWriter::new(
                    catalog,
                    storage.operator().clone(),
                    iceberg_config,
                ));

                println!("Initialized IcebergWriter for S3 Tables mode");

                Ok(Writer::Iceberg {
                    writer,
                    passthrough: PassthroughBatcher::default(),
                })
            }
            WriterMode::PlainS3 => {
                // Initialize standard S3 writer
                let s3 = config
                    .storage
                    .s3
                    .as_ref()
                    .ok_or_else(|| Error::from("S3 configuration required for Lambda"))?;

                let storage = Arc::new(
                    otlp2parquet_storage::opendal_storage::OpenDalStorage::new_s3(
                        &s3.bucket,
                        &s3.region,
                        s3.endpoint.as_deref(),
                        None,
                        None,
                    )
                    .map_err(|e| Error::from(format!("Failed to initialize storage: {}", e)))?,
                );

                let parquet_writer = Arc::new(ParquetWriter::new(storage.operator().clone()));

                // Initialize optional Iceberg committer for post-write commits
                let iceberg_committer = {
                    use otlp2parquet_storage::iceberg::init::{initialize_committer, InitResult};
                    match initialize_committer().await {
                        InitResult::Success {
                            committer,
                            table_count,
                        } => {
                            println!(
                                "Iceberg catalog integration enabled with {} tables (post-write mode)",
                                table_count
                            );
                            Some(committer)
                        }
                        InitResult::NotConfigured(msg) => {
                            println!("Iceberg catalog not configured: {}", msg);
                            None
                        }
                        InitResult::CatalogError(msg) => {
                            println!(
                                "Failed to create Iceberg catalog: {} - continuing without Iceberg",
                                msg
                            );
                            None
                        }
                        InitResult::NamespaceError(msg) => {
                            println!(
                                "Failed to parse Iceberg namespace: {} - continuing without Iceberg",
                                msg
                            );
                            None
                        }
                    }
                };

                println!("Initialized PlainS3 writer mode");

                Ok(Writer::PlainS3 {
                    parquet_writer,
                    passthrough: PassthroughBatcher::default(),
                    iceberg_committer,
                })
            }
        }
    }

    /// Write logs batch
    pub async fn write_logs(
        &self,
        batch: &RecordBatch,
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<Vec<String>, Error> {
        match self {
            Writer::Iceberg { writer, .. } => {
                let result = writer
                    .write_and_commit("logs", None, batch)
                    .await
                    .map_err(|e| Error::from(format!("Iceberg write failed: {}", e)))?;
                Ok(vec![result.path])
            }
            Writer::PlainS3 {
                parquet_writer,
                iceberg_committer,
                ..
            } => {
                let write_result = parquet_writer
                    .write_batches_with_hash(
                        std::slice::from_ref(batch),
                        service_name,
                        timestamp_nanos,
                    )
                    .await
                    .map_err(|e| Error::from(format!("S3 write failed: {}", e)))?;

                let path = write_result.path.clone();

                // Commit to Iceberg catalog if configured (warn-and-succeed on error)
                if let Some(committer) = iceberg_committer {
                    if let Err(e) = committer
                        .commit_with_signal(
                            "logs",
                            None,
                            std::slice::from_ref(&write_result),
                            parquet_writer.operator(),
                        )
                        .await
                    {
                        eprintln!("Warning: Failed to commit logs to Iceberg catalog: {}", e);
                    }
                }

                Ok(vec![path])
            }
        }
    }

    /// Write metrics batch
    pub async fn write_metrics(
        &self,
        batch: &RecordBatch,
        service_name: &str,
        timestamp_nanos: i64,
        metric_type: &str,
    ) -> Result<Vec<String>, Error> {
        match self {
            Writer::Iceberg { writer, .. } => {
                let result = writer
                    .write_and_commit("metrics", Some(metric_type), batch)
                    .await
                    .map_err(|e| Error::from(format!("Iceberg write failed: {}", e)))?;
                Ok(vec![result.path])
            }
            Writer::PlainS3 {
                parquet_writer,
                iceberg_committer,
                ..
            } => {
                let write_result = parquet_writer
                    .write_batches_with_signal(
                        std::slice::from_ref(batch),
                        service_name,
                        timestamp_nanos,
                        "metrics",
                        Some(metric_type),
                    )
                    .await
                    .map_err(|e| Error::from(format!("S3 write failed: {}", e)))?;

                let path = write_result.path.clone();

                // Commit to Iceberg catalog if configured
                if let Some(committer) = iceberg_committer {
                    if let Err(e) = committer
                        .commit_with_signal(
                            "metrics",
                            Some(metric_type),
                            std::slice::from_ref(&write_result),
                            parquet_writer.operator(),
                        )
                        .await
                    {
                        eprintln!(
                            "Warning: Failed to commit {} metrics to Iceberg catalog: {}",
                            metric_type, e
                        );
                    }
                }

                Ok(vec![path])
            }
        }
    }

    /// Write traces batch
    pub async fn write_traces(
        &self,
        batch: &RecordBatch,
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<Vec<String>, Error> {
        match self {
            Writer::Iceberg { writer, .. } => {
                let result = writer
                    .write_and_commit("traces", None, batch)
                    .await
                    .map_err(|e| Error::from(format!("Iceberg write failed: {}", e)))?;
                Ok(vec![result.path])
            }
            Writer::PlainS3 {
                parquet_writer,
                iceberg_committer,
                ..
            } => {
                let write_result = parquet_writer
                    .write_batches_with_hash(
                        std::slice::from_ref(batch),
                        service_name,
                        timestamp_nanos,
                    )
                    .await
                    .map_err(|e| Error::from(format!("S3 write failed: {}", e)))?;

                let path = write_result.path.clone();

                // Commit to Iceberg catalog if configured (warn-and-succeed on error)
                if let Some(committer) = iceberg_committer {
                    if let Err(e) = committer
                        .commit_with_signal(
                            "traces",
                            None,
                            std::slice::from_ref(&write_result),
                            parquet_writer.operator(),
                        )
                        .await
                    {
                        eprintln!("Warning: Failed to commit traces to Iceberg catalog: {}", e);
                    }
                }

                Ok(vec![path])
            }
        }
    }

    /// Get passthrough batcher
    pub fn passthrough(&self) -> &PassthroughBatcher {
        match self {
            Writer::Iceberg { passthrough, .. } => passthrough,
            Writer::PlainS3 { passthrough, .. } => passthrough,
        }
    }
}

#[derive(Clone)]
pub(crate) struct LambdaState {
    pub writer: Arc<Writer>,
    pub max_payload_bytes: usize,
}

/// Lambda runtime entry point
pub async fn run() -> Result<(), Error> {
    // Initialize tracing subscriber for CloudWatch logs
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .without_time() // Lambda adds timestamps
        .init();

    println!("Lambda runtime - using lambda_runtime's tokio + OpenDAL S3");

    // Load configuration
    let config = RuntimeConfig::load()
        .map_err(|e| lambda_runtime::Error::from(format!("Failed to load configuration: {}", e)))?;

    // Validate storage backend is S3
    if config.storage.backend != StorageBackend::S3 {
        return Err(lambda_runtime::Error::from(format!(
            "Lambda requires S3 storage backend, got: {}",
            config.storage.backend
        )));
    }

    set_parquet_row_group_size(config.storage.parquet_row_group_size);

    // Lambda: Always use passthrough (no batching)
    // Event-driven/stateless architecture makes batching ineffective
    println!("Lambda using passthrough mode (no batching)");

    let max_payload_bytes = config.request.max_payload_bytes;
    println!("Lambda payload cap set to {} bytes", max_payload_bytes);

    // Initialize writer based on environment
    let writer = Arc::new(Writer::from_env(&config).await?);

    let state = Arc::new(LambdaState {
        writer,
        max_payload_bytes,
    });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<HttpRequestEvent>| {
        let state = state.clone();
        async move { handle_request(event, state).await }
    }))
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_mode_iceberg() {
        // Set all required Iceberg environment variables
        std::env::set_var(
            "OTLP2PARQUET_ICEBERG_REST_URI",
            "https://s3tables.us-west-2.amazonaws.com/iceberg",
        );
        std::env::set_var(
            "OTLP2PARQUET_ICEBERG_WAREHOUSE",
            "arn:aws:s3tables:us-west-2:123456789012:bucket/test",
        );
        std::env::set_var("OTLP2PARQUET_ICEBERG_NAMESPACE", "otel");

        let mode = detect_writer_mode();

        assert_eq!(mode, WriterMode::Iceberg);

        // Cleanup
        std::env::remove_var("OTLP2PARQUET_ICEBERG_REST_URI");
        std::env::remove_var("OTLP2PARQUET_ICEBERG_WAREHOUSE");
        std::env::remove_var("OTLP2PARQUET_ICEBERG_NAMESPACE");
    }

    #[test]
    fn test_detect_mode_plain_s3_no_iceberg_vars() {
        // Remove all Iceberg variables
        std::env::remove_var("OTLP2PARQUET_ICEBERG_REST_URI");
        std::env::remove_var("OTLP2PARQUET_ICEBERG_WAREHOUSE");
        std::env::remove_var("OTLP2PARQUET_ICEBERG_NAMESPACE");

        let mode = detect_writer_mode();

        assert_eq!(mode, WriterMode::PlainS3);
    }

    #[test]
    fn test_detect_mode_plain_s3_partial_iceberg_vars() {
        // Set only some Iceberg variables (incomplete config should fall back to PlainS3)
        std::env::set_var(
            "OTLP2PARQUET_ICEBERG_REST_URI",
            "https://s3tables.us-west-2.amazonaws.com/iceberg",
        );
        std::env::remove_var("OTLP2PARQUET_ICEBERG_WAREHOUSE");
        std::env::remove_var("OTLP2PARQUET_ICEBERG_NAMESPACE");

        let mode = detect_writer_mode();

        assert_eq!(mode, WriterMode::PlainS3);

        // Cleanup
        std::env::remove_var("OTLP2PARQUET_ICEBERG_REST_URI");
    }
}
