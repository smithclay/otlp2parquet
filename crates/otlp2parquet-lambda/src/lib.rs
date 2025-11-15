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
    IcebergCatalog, IcebergCommitter, IcebergConfig, IcebergRestConfig, IcebergWriter,
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

/// Unified writer that routes to either IcebergWriter or PlainS3 writer
pub(crate) enum Writer {
    /// Integrated write + commit to S3 Tables with Iceberg catalog (AWS SigV4 auth)
    Iceberg {
        writer: Arc<
            otlp2parquet_storage::iceberg::IcebergWriter<
                otlp2parquet_storage::iceberg::aws::AwsSigV4HttpClient,
            >,
        >,
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
    /// Create Writer from configuration (no direct env probing).
    pub async fn from_config(config: &RuntimeConfig) -> Result<Self, Error> {
        let integrated = config
            .lambda
            .as_ref()
            .map(|lambda| lambda.integrated_iceberg)
            .unwrap_or(false);

        if integrated {
            let iceberg_cfg = config.iceberg.as_ref().ok_or_else(|| {
                Error::from("lambda.integrated_iceberg=true requires [iceberg] configuration")
            })?;
            tracing::info!("Lambda writer mode: integrated Iceberg");
            return Self::build_integrated_writer(config, iceberg_cfg).await;
        }

        tracing::info!("Lambda writer mode: plain S3 with optional Iceberg commits");
        Self::build_plain_s3_writer(config).await
    }

    async fn build_integrated_writer(
        config: &RuntimeConfig,
        iceberg_cfg: &otlp2parquet_config::IcebergConfig,
    ) -> Result<Self, Error> {
        let rest_uri = if iceberg_cfg.rest_uri.is_empty() {
            return Err(Error::from(
                "iceberg.rest_uri must be set for integrated Iceberg mode",
            ));
        } else {
            iceberg_cfg.rest_uri.clone()
        };

        let warehouse = iceberg_cfg
            .warehouse
            .as_ref()
            .ok_or_else(|| {
                Error::from("iceberg.warehouse is required for integrated Iceberg mode")
            })?
            .clone();

        let namespace_str = iceberg_cfg
            .namespace
            .as_ref()
            .ok_or_else(|| {
                Error::from("iceberg.namespace is required for integrated Iceberg mode")
            })?
            .clone();
        let namespace_parts = iceberg_cfg.namespace_vec();
        if namespace_parts.is_empty() {
            return Err(Error::from(
                "iceberg.namespace must include at least one segment",
            ));
        }

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

        use otlp2parquet_storage::iceberg::aws::AwsSigV4HttpClient;
        use otlp2parquet_storage::iceberg::catalog::NamespaceIdent;

        let http_client = AwsSigV4HttpClient::new(&s3.region)
            .await
            .map_err(|e| Error::from(format!("Failed to create AWS SigV4 HTTP client: {}", e)))?;
        let namespace =
            NamespaceIdent::from_vec(namespace_parts).map_err(|e| Error::from(e.to_string()))?;
        let tables = iceberg_cfg.to_tables_map();
        let catalog_prefix = format!("catalogs/{}", warehouse);

        let catalog = Arc::new(IcebergCatalog::new(
            http_client,
            rest_uri.clone(),
            catalog_prefix,
            namespace,
            tables,
            iceberg_cfg.data_location.clone(),
        ));

        let iceberg_writer_config = IcebergConfig {
            rest_uri,
            warehouse,
            namespace: namespace_str,
        };

        let writer = Arc::new(IcebergWriter::new(
            catalog,
            storage.operator().clone(),
            iceberg_writer_config,
        ));

        Ok(Writer::Iceberg {
            writer,
            passthrough: PassthroughBatcher::default(),
        })
    }

    async fn build_plain_s3_writer(config: &RuntimeConfig) -> Result<Self, Error> {
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

        let iceberg_committer = {
            use otlp2parquet_storage::iceberg::init::{
                initialize_committer_with_config, InitResult,
            };

            if let Some(iceberg_cfg) = &config.iceberg {
                let rest_config = IcebergRestConfig {
                    rest_uri: iceberg_cfg.rest_uri.clone(),
                    warehouse: iceberg_cfg.warehouse.clone(),
                    namespace: iceberg_cfg.namespace_vec(),
                    tables: iceberg_cfg.to_tables_map(),
                    catalog_name: iceberg_cfg.catalog_name.clone(),
                    format_version: iceberg_cfg.format_version,
                    target_file_size_bytes: iceberg_cfg.target_file_size_bytes,
                    staging_prefix: iceberg_cfg.staging_prefix.clone(),
                    data_location: iceberg_cfg.data_location.clone(),
                };

                match initialize_committer_with_config(rest_config).await {
                    InitResult::Success {
                        committer,
                        table_count,
                    } => {
                        tracing::info!(
                            "Iceberg catalog integration enabled with {} tables (post-write mode)",
                            table_count
                        );
                        Some(committer)
                    }
                    InitResult::CatalogError(msg) => {
                        tracing::warn!(
                            "Failed to create Iceberg catalog: {} - continuing without Iceberg",
                            msg
                        );
                        None
                    }
                    InitResult::NamespaceError(msg) => {
                        tracing::warn!(
                            "Failed to parse Iceberg namespace: {} - continuing without Iceberg",
                            msg
                        );
                        None
                    }
                    InitResult::NotConfigured(msg) => {
                        tracing::warn!(
                            "Iceberg committer not configured correctly: {} - continuing without Iceberg",
                            msg
                        );
                        None
                    }
                }
            } else {
                None
            }
        };

        Ok(Writer::PlainS3 {
            parquet_writer,
            passthrough: PassthroughBatcher::default(),
            iceberg_committer,
        })
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
                        tracing::warn!("Failed to commit logs to Iceberg catalog: {}", e);
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
                        tracing::warn!(
                            "Failed to commit {} metrics to Iceberg catalog: {}",
                            metric_type,
                            e
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
                        tracing::warn!("Failed to commit traces to Iceberg catalog: {}", e);
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
        .with_max_level(tracing::Level::DEBUG)
        .without_time() // Lambda adds timestamps
        .init();

    tracing::info!("Lambda runtime - using lambda_runtime's tokio + OpenDAL S3");

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
    tracing::info!("Lambda using passthrough mode (no batching)");

    let max_payload_bytes = config.request.max_payload_bytes;
    tracing::info!("Lambda payload cap set to {} bytes", max_payload_bytes);

    // Initialize writer based on environment
    let writer = Arc::new(Writer::from_config(&config).await?);

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
    use otlp2parquet_config::Platform;

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::{Mutex, OnceLock};

        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn lambda_integrated_flag_defaults_to_false() {
        let config = RuntimeConfig::from_platform_defaults(Platform::Lambda);
        let lambda_cfg = config.lambda.expect("lambda defaults missing");
        assert!(!lambda_cfg.integrated_iceberg);
    }

    #[test]
    fn lambda_integrated_flag_respects_env_override() {
        let _guard = env_lock();
        std::env::set_var("OTLP2PARQUET_LAMBDA_INTEGRATED_ICEBERG", "true");

        let config =
            RuntimeConfig::load_for_platform(Platform::Lambda).expect("config load should succeed");
        let lambda_cfg = config.lambda.expect("lambda config missing after override");
        assert!(lambda_cfg.integrated_iceberg);

        std::env::remove_var("OTLP2PARQUET_LAMBDA_INTEGRATED_ICEBERG");
    }
}
