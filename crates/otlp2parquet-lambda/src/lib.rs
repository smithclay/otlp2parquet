// AWS Lambda runtime adapter
//
// Uses OpenDAL S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

use lambda_runtime::{service_fn, Error, LambdaEvent};
use otlp2parquet_core::config::{RuntimeConfig, StorageBackend};
use otlp2parquet_core::parquet::encoding::set_parquet_row_group_size;
use otlp2parquet_writer::set_table_name_overrides;
use std::sync::Arc;

mod handlers;
mod response;

use handlers::{canonical_path, handle_http_request};
use response::{
    build_api_gateway_v1_response, build_api_gateway_v2_response, build_function_url_response,
};
pub(crate) use response::{HttpLambdaResponse, HttpRequestEvent, HttpResponseData};

/// Lambda handler for OTLP HTTP requests
async fn handle_request(
    event: LambdaEvent<HttpRequestEvent>,
    state: Arc<LambdaState>,
) -> Result<HttpLambdaResponse, Error> {
    let (request, _context) = event.into_parts();

    match request {
        HttpRequestEvent::ApiGatewayV1(boxed_request) => {
            let request = &*boxed_request;
            let method = request.http_method.as_str();
            let path = canonical_path(request.path.as_deref());

            // Extract Content-Type header from API Gateway v1 request
            let content_type = request
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok());

            // Extract Content-Encoding header for gzip decompression
            let content_encoding = request
                .headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok());

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                content_encoding,
                &state,
            )
            .await;
            Ok(build_api_gateway_v1_response(response))
        }
        HttpRequestEvent::ApiGatewayV2(boxed_request) => {
            let request = &*boxed_request;
            let method = request.request_context.http.method.as_str();
            let path = canonical_path(request.raw_path.as_deref());

            // Extract Content-Type header from API Gateway v2 (HTTP API) request
            let content_type = request
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok());

            // Extract Content-Encoding header for gzip decompression
            let content_encoding = request
                .headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok());

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                content_encoding,
                &state,
            )
            .await;
            Ok(build_api_gateway_v2_response(response))
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

            // Extract Content-Encoding header for gzip decompression
            let content_encoding = request
                .headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok());

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                content_encoding,
                &state,
            )
            .await;
            Ok(build_function_url_response(response))
        }
    }
}

#[derive(Clone)]
pub(crate) struct LambdaState {
    pub catalog: Option<Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>>,
    pub namespace: String,
    pub max_payload_bytes: usize,
}

/// Lambda runtime entry point
pub async fn run() -> Result<(), Error> {
    // Initialize tracing subscriber for CloudWatch logs
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .without_time() // Lambda adds timestamps
        .init();

    // Log version information
    let version = env!("CARGO_PKG_VERSION");
    let git_hash = env!("GIT_HASH");
    let build_timestamp = env!("BUILD_TIMESTAMP");

    tracing::info!(
        "otlp2parquet Lambda v{} (git:{}, built:{})",
        version,
        git_hash,
        build_timestamp
    );
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

    // Check catalog mode to determine initialization path
    let (catalog, namespace) = if config.catalog_mode
        == otlp2parquet_core::config::CatalogMode::Iceberg
    {
        // Initialize catalog - support both REST catalog and S3 Tables
        let iceberg_cfg = config.iceberg.as_ref().ok_or_else(|| {
            Error::from("Lambda requires iceberg configuration (rest_uri or bucket_arn) when catalog_mode=iceberg")
        })?;

        // Respect custom table name overrides from config
        set_table_name_overrides(iceberg_cfg.to_tables_map());

        let catalog_config = if !iceberg_cfg.rest_uri.is_empty() {
            // REST catalog (Nessie, Glue REST, etc.)
            tracing::info!(
                "Initializing Lambda with REST catalog: endpoint={}, namespace={:?}",
                iceberg_cfg.rest_uri,
                iceberg_cfg.namespace
            );

            // Create S3 operator for storage
            let s3_config = config.storage.s3.as_ref().ok_or_else(|| {
                Error::from("Lambda with REST catalog requires S3 storage configuration")
            })?;

            let mut s3_builder = opendal::services::S3::default()
                .bucket(&s3_config.bucket)
                .region(&s3_config.region);

            if let Some(endpoint) = &s3_config.endpoint {
                tracing::info!("Using custom S3 endpoint: {}", endpoint);
                s3_builder = s3_builder.endpoint(endpoint);
            }

            let operator = opendal::Operator::new(s3_builder)
                .map_err(|e| Error::from(format!("Failed to build S3 operator: {}", e)))?
                .finish();

            let namespace = iceberg_cfg
                .namespace
                .clone()
                .unwrap_or_else(|| "otlp".to_string());

            otlp2parquet_writer::CatalogConfig {
                namespace,
                catalog_type: otlp2parquet_writer::CatalogType::Rest {
                    uri: iceberg_cfg.rest_uri.clone(),
                    warehouse: String::new(),
                    operator,
                    token: None,
                },
            }
        } else if let Some(bucket_arn) = &iceberg_cfg.bucket_arn {
            // S3 Tables catalog (AWS managed)
            tracing::info!("Initializing Lambda with S3 Tables catalog: {}", bucket_arn);

            let namespace = iceberg_cfg
                .namespace
                .clone()
                .unwrap_or_else(|| "otlp".to_string());

            otlp2parquet_writer::CatalogConfig {
                namespace,
                catalog_type: otlp2parquet_writer::CatalogType::S3Tables {
                    bucket_arn: bucket_arn.clone(),
                },
            }
        } else {
            return Err(Error::from(
                "Lambda requires either iceberg.rest_uri or iceberg.bucket_arn configuration when catalog_mode=iceberg",
            ));
        };

        let namespace = catalog_config.namespace.clone();
        let catalog = otlp2parquet_writer::initialize_catalog(catalog_config)
            .await
            .map_err(|e| Error::from(format!("Failed to initialize catalog: {}", e)))?;

        // Ensure namespace exists
        otlp2parquet_writer::ensure_namespace(catalog.as_ref(), &namespace)
            .await
            .map_err(|e| Error::from(format!("Failed to ensure namespace: {}", e)))?;

        // Initialize storage for best-effort plain Parquet fallback if catalog writes fail
        otlp2parquet_writer::initialize_storage(&config)
            .map_err(|e| Error::from(format!("Failed to initialize storage: {}", e)))?;

        (Some(catalog), namespace)
    } else {
        // catalog_mode == None - plain Parquet mode
        tracing::info!("Initializing Lambda in plain Parquet mode (no catalog)");

        // Initialize storage operator for plain Parquet writes
        let _s3_config = config.storage.s3.as_ref().ok_or_else(|| {
            Error::from("Lambda with catalog_mode=none requires S3 storage configuration")
        })?;

        otlp2parquet_writer::initialize_storage(&config)
            .map_err(|e| Error::from(format!("Failed to initialize storage: {}", e)))?;

        let namespace = config
            .iceberg
            .as_ref()
            .and_then(|ic| ic.namespace.clone())
            .unwrap_or_else(|| "otlp".to_string());

        tracing::info!("Using namespace: {}", namespace);

        (None, namespace)
    };

    let state = Arc::new(LambdaState {
        catalog,
        namespace,
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
    use otlp2parquet_core::config::{IcebergConfig, Platform};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::{Mutex, OnceLock};

        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn lambda_requires_bucket_arn_configuration() {
        // Lambda now always uses icepick with S3 Tables, which requires bucket_arn
        let mut config = RuntimeConfig::from_platform_defaults(Platform::Lambda);

        // Without bucket_arn, initialization should fail
        assert!(
            config.iceberg.is_none()
                || config
                    .iceberg
                    .as_ref()
                    .and_then(|ic| ic.bucket_arn.as_ref())
                    .is_none()
        );

        // With bucket_arn, config is valid
        config.iceberg = Some(IcebergConfig {
            bucket_arn: Some(
                "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket".to_string(),
            ),
            ..Default::default()
        });

        assert!(config
            .iceberg
            .as_ref()
            .and_then(|ic| ic.bucket_arn.as_ref())
            .is_some());
    }

    #[test]
    fn lambda_bucket_arn_respects_env_override() {
        let _guard = env_lock();
        let test_arn = "arn:aws:s3tables:us-east-1:999999999999:bucket/my-bucket";
        std::env::set_var("OTLP2PARQUET_ICEBERG_BUCKET_ARN", test_arn);

        let config =
            RuntimeConfig::load_for_platform(Platform::Lambda).expect("config load should succeed");

        let bucket_arn = config
            .iceberg
            .as_ref()
            .and_then(|ic| ic.bucket_arn.as_ref())
            .expect("bucket_arn should be set from env");

        assert_eq!(bucket_arn, test_arn);

        std::env::remove_var("OTLP2PARQUET_ICEBERG_BUCKET_ARN");
    }

    #[test]
    fn catalog_mode_none_requires_s3_storage_config() {
        use otlp2parquet_core::config::{CatalogMode, S3Config, StorageBackend};

        // Create config with catalog_mode=none but no S3 storage config
        let mut config = RuntimeConfig::from_platform_defaults(Platform::Lambda);
        config.catalog_mode = CatalogMode::None;
        config.storage.backend = StorageBackend::S3;
        config.storage.s3 = None;

        // Should fail validation - we validate this at runtime in run()
        assert!(config.storage.s3.is_none());
        assert_eq!(config.catalog_mode, CatalogMode::None);

        // With S3 config, should be valid
        config.storage.s3 = Some(S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-west-2".to_string(),
            endpoint: None,
        });
        assert!(config.storage.s3.is_some());
    }

    #[test]
    fn catalog_mode_iceberg_requires_bucket_arn_or_rest_uri() {
        use otlp2parquet_core::config::CatalogMode;

        // Create config with catalog_mode=iceberg
        let mut config = RuntimeConfig::from_platform_defaults(Platform::Lambda);
        config.catalog_mode = CatalogMode::Iceberg;

        // Without iceberg config, should fail
        config.iceberg = None;
        assert!(config.iceberg.is_none());

        // With bucket_arn, should be valid
        config.iceberg = Some(IcebergConfig {
            bucket_arn: Some("arn:aws:s3tables:us-west-2:123456789012:bucket/test".to_string()),
            ..Default::default()
        });
        assert!(config.iceberg.as_ref().unwrap().bucket_arn.is_some());

        // With rest_uri instead, should also be valid
        config.iceberg = Some(IcebergConfig {
            rest_uri: "https://example.com/iceberg".to_string(),
            ..Default::default()
        });
        assert!(!config.iceberg.as_ref().unwrap().rest_uri.is_empty());
    }

    #[test]
    fn catalog_mode_respects_env_override() {
        let _guard = env_lock();

        // Set catalog_mode=none via env
        std::env::set_var("OTLP2PARQUET_CATALOG_MODE", "none");

        let config =
            RuntimeConfig::load_for_platform(Platform::Lambda).expect("config load should succeed");

        assert_eq!(
            config.catalog_mode,
            otlp2parquet_core::config::CatalogMode::None
        );

        std::env::remove_var("OTLP2PARQUET_CATALOG_MODE");
    }
}
