// Cloudflare Workers runtime adapter
//
// Uses icepick with OpenDAL S3 for R2 storage and handles incoming requests via Worker fetch events
//
// Philosophy: Use otlp2parquet-writer for unified write path
// Worker crate provides the runtime, icepick provides storage abstraction
// Entry point is #[event(fetch)] macro, not main()

mod auth;
mod errors;
mod handlers;

use flate2::read::GzDecoder;
use once_cell::sync::OnceCell;
use otlp2parquet_config::{EnvSource, Platform, RuntimeConfig, ENV_PREFIX};
use std::io::Read;
use std::sync::Arc;
use worker::*;

static CATALOG: OnceCell<Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>> = OnceCell::new();
static NAMESPACE: OnceCell<String> = OnceCell::new();
static CONFIG: OnceCell<RuntimeConfig> = OnceCell::new();

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SignalKind {
    Logs,
    Traces,
    Metrics,
}

/// Catalog configuration mode
#[derive(Debug)]
enum CatalogMode {
    R2DataCatalog {
        account_id: String,
        bucket_name: String,
        api_token: String,
    },
    PlainParquet,
}

/// Validate catalog configuration with detailed error messages
fn validate_catalog_config(
    env: &Env,
) -> std::result::Result<CatalogMode, errors::ConfigValidationError> {
    let account_id = env.var("CLOUDFLARE_ACCOUNT_ID").ok().map(|v| v.to_string());
    let bucket_name = env
        .var("CLOUDFLARE_BUCKET_NAME")
        .ok()
        .map(|v| v.to_string());
    let api_token = env
        .secret("CLOUDFLARE_API_TOKEN")
        .or_else(|_| env.var("CLOUDFLARE_API_TOKEN"))
        .ok()
        .map(|v| v.to_string());

    // Collect which variables are present
    let mut present_vars = Vec::new();
    if account_id.is_some() {
        present_vars.push("CLOUDFLARE_ACCOUNT_ID");
    }
    if bucket_name.is_some() {
        present_vars.push("CLOUDFLARE_BUCKET_NAME");
    }
    if api_token.is_some() {
        present_vars.push("CLOUDFLARE_API_TOKEN");
    }

    match (account_id, bucket_name, api_token) {
        (Some(a), Some(b), Some(t)) => {
            // All catalog vars present - use R2 Data Catalog mode
            Ok(CatalogMode::R2DataCatalog {
                account_id: a,
                bucket_name: b,
                api_token: t,
            })
        }
        (None, None, None) => {
            // No catalog vars present - use plain Parquet mode
            Ok(CatalogMode::PlainParquet)
        }
        _ => {
            // Partial configuration - return descriptive error
            let all_required = vec![
                "CLOUDFLARE_ACCOUNT_ID",
                "CLOUDFLARE_BUCKET_NAME",
                "CLOUDFLARE_API_TOKEN",
            ];
            let missing: Vec<&'static str> = all_required
                .into_iter()
                .filter(|var| !present_vars.contains(var))
                .collect();

            Err(errors::ConfigValidationError::PartialCatalogConfig {
                catalog_type: "R2 Data Catalog",
                present: present_vars,
                missing,
                hint: "R2 Data Catalog requires all three variables to be set. Either provide all catalog variables for Iceberg support, or remove them entirely to use plain Parquet mode.".to_string(),
            })
        }
    }
}

/// Decompress gzip-encoded request body if Content-Encoding header indicates gzip
fn maybe_decompress(data: &[u8], content_encoding: Option<&str>) -> Result<Vec<u8>> {
    match content_encoding {
        Some(enc) if enc.eq_ignore_ascii_case("gzip") => {
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).map_err(|e| {
                worker::Error::RustError(format!("gzip decompression failed: {}", e))
            })?;
            Ok(decompressed)
        }
        _ => Ok(data.to_vec()),
    }
}

/// Cloudflare Workers entry point
#[event(fetch)]
pub async fn main(req: Request, env: Env, ctx: Context) -> Result<Response> {
    console_log!("[DEBUG] Worker fetch handler called");
    let result = handle_otlp_request(req, env, ctx).await;
    console_log!("[DEBUG] handle_otlp_request returned");
    result
}

/// Handle OTLP HTTP POST request and write to R2
async fn handle_otlp_request(mut req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_log!("[DEBUG] handle_otlp_request: start");
    // Use Cloudflare's built-in request ID (CF-Ray header)
    let request_id = req
        .headers()
        .get("cf-ray")
        .ok()
        .flatten()
        .unwrap_or_else(|| "unknown".to_string());
    console_log!("[DEBUG] request_id: {}", request_id);

    // Only accept POST requests
    if req.method() != Method::Post {
        return Response::error("Method not allowed", 405);
    }

    let path = req.path();
    console_log!("[DEBUG] path: {}", path);
    let signal = match path.as_str() {
        "/v1/logs" => SignalKind::Logs,
        "/v1/traces" => SignalKind::Traces,
        "/v1/metrics" => SignalKind::Metrics,
        _ => return Response::error("Not found", 404),
    };

    console_log!("[DEBUG] checking auth");
    // Check basic authentication if enabled
    if let Err(response) = auth::check_basic_auth(&req, &env, Some(&request_id)) {
        return Ok(response);
    }

    console_log!("[DEBUG] loading config");
    // Load config once (with platform detection and env var overrides)
    let config = CONFIG
        .get_or_try_init(|| load_worker_config(&env))
        .map_err(|e| {
            console_error!("[{}] Failed to load configuration: {:?}", request_id, e);
            e
        })?;
    console_log!("[DEBUG] config loaded successfully");

    // Initialize storage operator in writer crate if not already done
    // This is used for direct Parquet writes when no catalog is configured
    otlp2parquet_writer::initialize_storage(config).map_err(|e| {
        console_error!("[{}] Failed to initialize storage: {:?}", request_id, e);
        let error = errors::OtlpErrorKind::StorageError(format!(
            "Failed to initialize R2 storage: {}. Verify AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and OTLP2PARQUET_R2_BUCKET are correctly configured.",
            e
        ));
        let error_response = errors::ErrorResponse::from_error(error, Some(request_id.clone()));
        worker::Error::RustError(serde_json::to_string(&error_response).unwrap_or_default())
    })?;

    // Determine whether to use catalog based on catalog_mode configuration
    // - CatalogMode::Iceberg → use R2 Data Catalog (requires CLOUDFLARE_* env vars)
    // - CatalogMode::None → write directly to R2 (plain Parquet, no catalog)
    if config.catalog_mode == otlp2parquet_config::CatalogMode::Iceberg && CATALOG.get().is_none() {
        // Iceberg mode requires CLOUDFLARE_* catalog vars
        match validate_catalog_config(&env) {
            Ok(CatalogMode::R2DataCatalog {
                account_id,
                bucket_name,
                api_token,
            }) => {
                // All catalog vars present - initialize R2 Data Catalog (Iceberg)
                let namespace = env
                    .var("OTLP_NAMESPACE")
                    .ok()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "otlp".to_string());

                console_log!(
                    "[{}] Catalog mode: iceberg - Initializing R2 Data Catalog: account={}, bucket={}, namespace={}",
                    request_id,
                    account_id,
                    bucket_name,
                    namespace
                );

                // Note: R2 always uses region="auto" - no need to bridge env vars in WASM
                // icepick::R2Catalog handles the region internally via OpendDAL configuration
                // WASM cannot use std::env::set_var anyway (causes panic)

                // Get R2 credentials from config for Parquet file writes
                let r2_config = config.storage.r2.as_ref().ok_or_else(|| {
                    console_error!("[{}] R2 storage config missing", request_id);
                    worker::Error::RustError(
                        "R2 storage configuration required for catalog mode".to_string(),
                    )
                })?;

                let catalog_config = otlp2parquet_writer::CatalogConfig {
                    namespace: namespace.clone(),
                    catalog_type: otlp2parquet_writer::CatalogType::R2DataCatalog {
                        account_id,
                        bucket_name,
                        api_token,
                        access_key_id: r2_config.access_key_id.clone(),
                        secret_access_key: r2_config.secret_access_key.clone(),
                    },
                };

                let catalog = otlp2parquet_writer::initialize_catalog(catalog_config)
                    .await
                    .map_err(|e| {
                        console_error!("[{}] Failed to initialize catalog: {:?}", request_id, e);
                        let error = errors::OtlpErrorKind::CatalogError(format!(
                            "Failed to initialize R2 Data Catalog: {}. Verify CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_BUCKET_NAME, and CLOUDFLARE_API_TOKEN are correct and the API token has R2 permissions.",
                            e
                        ));
                        let error_response = errors::ErrorResponse::from_error(error, Some(request_id.clone()));
                        worker::Error::RustError(serde_json::to_string(&error_response).unwrap_or_default())
                    })?;

                // Ensure namespace exists
                otlp2parquet_writer::ensure_namespace(catalog.as_ref(), &namespace)
                    .await
                    .map_err(|e| {
                        console_error!("[{}] Failed to ensure namespace: {:?}", request_id, e);
                        let error = errors::OtlpErrorKind::CatalogError(format!(
                            "Failed to create namespace '{}': {}",
                            namespace, e
                        ));
                        let error_response =
                            errors::ErrorResponse::from_error(error, Some(request_id.clone()));
                        worker::Error::RustError(
                            serde_json::to_string(&error_response).unwrap_or_default(),
                        )
                    })?;

                let _ = CATALOG.set(catalog);
                let _ = NAMESPACE.set(namespace);
            }
            Ok(CatalogMode::PlainParquet) => {
                // Catalog vars not present but iceberg mode requested - this is a config error
                console_error!(
                    "[{}] Catalog mode is 'iceberg' but catalog configuration is incomplete",
                    request_id
                );
                let validation_error = errors::ConfigValidationError::MissingRequired {
                    component: "R2 Data Catalog",
                    missing_vars: vec!["CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_BUCKET_NAME", "CLOUDFLARE_API_TOKEN"],
                    hint: "Either set all three CLOUDFLARE_* variables for Iceberg mode, or set OTLP2PARQUET_CATALOG_MODE=none to write plain Parquet files".to_string(),
                };
                let error = errors::OtlpErrorKind::ConfigError(validation_error);
                let status_code = error.status_code();
                let error_response = errors::ErrorResponse::from_error(error, Some(request_id));
                return error_response.into_response(status_code);
            }
            Err(validation_error) => {
                console_error!(
                    "[{}] Catalog configuration validation failed: {:?}",
                    request_id,
                    validation_error
                );
                let error = errors::OtlpErrorKind::ConfigError(validation_error);
                let status_code = error.status_code();
                let error_response = errors::ErrorResponse::from_error(error, Some(request_id));
                return error_response.into_response(status_code);
            }
        }
    } else if config.catalog_mode == otlp2parquet_config::CatalogMode::None {
        // Plain Parquet mode - skip catalog entirely
        console_log!(
            "[{}] Catalog mode: none - writing plain Parquet to R2 bucket '{}' (no Iceberg catalog)",
            request_id,
            config.storage.r2.as_ref().map(|r2| r2.bucket.as_str()).unwrap_or("unknown")
        );
    }

    let catalog = CATALOG.get();
    let namespace = NAMESPACE.get();

    // Use configurable max payload size (default 10MB for CF Workers)
    // Can be overridden via OTLP2PARQUET_MAX_PAYLOAD_BYTES env var
    let max_payload_bytes = env
        .var("OTLP2PARQUET_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|val| val.to_string().parse::<usize>().ok())
        .unwrap_or(config.request.max_payload_bytes);

    console_log!(
        "Processing {} request (max payload: {} MB)",
        match signal {
            SignalKind::Logs => "logs",
            SignalKind::Traces => "traces",
            SignalKind::Metrics => "metrics",
        },
        max_payload_bytes / (1024 * 1024)
    );

    let content_type_header = req.headers().get("content-type").ok().flatten();
    let content_type = content_type_header.as_deref();
    let format = otlp2parquet_core::InputFormat::from_content_type(content_type);
    let content_encoding = req.headers().get("content-encoding").ok().flatten();

    let raw_body = req.bytes().await.map_err(|e| {
        console_error!("Failed to read request body: {:?}", e);
        e
    })?;

    // Decompress if gzip-encoded (OTel collectors typically send gzip by default)
    let body_bytes = maybe_decompress(&raw_body, content_encoding.as_deref())?;

    // Check payload size AFTER decompression (prevents zip bombs)
    if body_bytes.len() > max_payload_bytes {
        console_error!(
            "Payload too large: {} bytes (max: {} bytes / {} MB)",
            body_bytes.len(),
            max_payload_bytes,
            max_payload_bytes / (1024 * 1024)
        );
        return Response::error(
            format!(
                "Payload too large: {} bytes exceeds limit of {} MB",
                body_bytes.len(),
                max_payload_bytes / (1024 * 1024)
            ),
            413,
        );
    }

    // Determine if catalog is enabled based on config and initialization status
    let catalog_enabled = config.catalog_mode == otlp2parquet_config::CatalogMode::Iceberg;

    // Route to appropriate handler based on signal type
    match signal {
        SignalKind::Logs => {
            handlers::handle_logs_request(
                &body_bytes,
                format,
                content_type,
                catalog,
                namespace.map(|s| s.as_str()),
                catalog_enabled,
                &request_id,
            )
            .await
        }
        SignalKind::Traces => {
            handlers::handle_traces_request(
                &body_bytes,
                format,
                content_type,
                catalog,
                namespace.map(|s| s.as_str()),
                catalog_enabled,
                &request_id,
            )
            .await
        }
        SignalKind::Metrics => {
            handlers::handle_metrics_request(
                &body_bytes,
                format,
                content_type,
                catalog,
                namespace.map(|s| s.as_str()),
                catalog_enabled,
                &request_id,
            )
            .await
        }
    }
}

fn load_worker_config(env: &Env) -> Result<RuntimeConfig> {
    console_log!("[DEBUG] load_worker_config: starting");
    let provider = WorkerEnvSource { env };
    let inline = provider.get("CONFIG_CONTENT");

    console_log!("[DEBUG] Loading config for Platform::CloudflareWorkers");
    let config = RuntimeConfig::load_for_platform_with_env(
        Platform::CloudflareWorkers,
        inline.as_deref(),
        &provider,
    )
    .map_err(|e| {
        console_error!("Failed to load configuration: {:?}", e);
        worker::Error::RustError(format!("Config error: {}", e))
    })?;

    // Debug: Log loaded configuration
    console_log!("[DEBUG] Config loaded successfully!");
    console_log!("[DEBUG] Catalog mode: {:?}", config.catalog_mode);
    console_log!("[DEBUG] Storage backend: {:?}", config.storage.backend);

    if let Some(ref r2) = config.storage.r2 {
        console_log!("[DEBUG] R2 Config:");
        console_log!("[DEBUG]   bucket: {}", r2.bucket);
        console_log!("[DEBUG]   account_id: {}", r2.account_id);
        console_log!(
            "[DEBUG]   access_key_id: {}",
            if r2.access_key_id.is_empty() {
                "<empty>"
            } else {
                "<set>"
            }
        );
        console_log!(
            "[DEBUG]   secret_access_key: {}",
            if r2.secret_access_key.is_empty() {
                "<empty>"
            } else {
                "<set>"
            }
        );
        console_log!("[DEBUG]   endpoint: {:?}", r2.endpoint);
    } else {
        console_log!("[DEBUG] R2 Config: None");
    }

    Ok(config)
}

struct WorkerEnvSource<'a> {
    env: &'a Env,
}

impl<'a> EnvSource for WorkerEnvSource<'a> {
    fn get(&self, key: &str) -> Option<String> {
        let binding = format!("{}{}", ENV_PREFIX, key);

        if let Ok(val) = self.env.var(&binding) {
            console_log!("[DEBUG] EnvSource: {} = <set>", binding);
            return Some(val.to_string());
        }
        if key.contains("SECRET") {
            if let Ok(secret) = self.env.secret(&binding) {
                console_log!("[DEBUG] EnvSource: {} = <secret>", binding);
                return Some(secret.to_string());
            }
        }
        console_log!("[DEBUG] EnvSource: {} = <not set>", binding);
        None
    }

    fn get_raw(&self, key: &str) -> Option<String> {
        if let Ok(val) = self.env.var(key) {
            console_log!("[DEBUG] EnvSource (raw): {} = <set>", key);
            return Some(val.to_string());
        }
        if key.contains("SECRET") {
            if let Ok(secret) = self.env.secret(key) {
                console_log!("[DEBUG] EnvSource (raw): {} = <secret>", key);
                return Some(secret.to_string());
            }
        }
        console_log!("[DEBUG] EnvSource (raw): {} = <not set>", key);
        None
    }
}
