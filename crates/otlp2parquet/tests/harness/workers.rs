//! Cloudflare Workers + R2 smoke test harness
//!
//! Tests otlp2parquet Cloudflare Workers with both R2 plain Parquet and R2 Data Catalog (Iceberg).
//!
//! ## Architecture
//! - Setup: Create unique R2 bucket per test (e.g., "smoke-workers-{uuid}")
//! - Optional: Enable R2 Data Catalog for Iceberg tests
//! - Deploy: Wrangler deploy WASM Workers script using generated config
//! - Send signals: POST to Workers URL with OTLP payloads
//! - Verify execution: Query Workers logs via Cloudflare API
//! - Verify data: DuckDB queries R2 Parquet files (with or without catalog)
//! - Cleanup: Delete Workers script, all R2 objects, and R2 bucket
//!
//! ## Prerequisites
//! - Cloudflare API token with Workers and R2 permissions
//! - WASM binary built (make wasm-compress)
//! - R2 API credentials for S3-compatible access
//!
//! ## Environment Variables
//! - `CLOUDFLARE_API_TOKEN`: Cloudflare API token
//! - `CLOUDFLARE_ACCOUNT_ID`: Cloudflare account ID
//! - `AWS_ACCESS_KEY_ID`: R2 S3 API access key
//! - `AWS_SECRET_ACCESS_KEY`: R2 S3 API secret key
//! - `SMOKE_TEST_WORKER_PREFIX`: Worker name prefix (default: "smoke-workers")
//! - `SMOKE_TEST_LIVE_TAIL`: Set to "1" or "true" to enable live tail log capture

pub use super::CatalogMode;
use super::{
    CatalogType, DeploymentInfo, DuckDBVerifier, ExecutionStatus, R2Credentials, SmokeTestHarness,
    StorageBackend, StorageConfig, TestDataSet,
};
use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Builder as S3ConfigBuilder, Client as S3Client};
use otlp2parquet::deploy::cloudflare::process_conditional_blocks;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

/// Global mutex to serialize wrangler deploy calls.
/// This prevents race conditions when multiple tests run in parallel,
/// as wrangler's build command compiles the wasm to shared output files.
static WRANGLER_DEPLOY_LOCK: Mutex<()> = Mutex::new(());

/// Batch mode for Workers tests
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BatchMode {
    /// Enable batching via Durable Objects
    Enabled,
    /// Disable batching (direct write)
    Disabled,
}

/// Live tail session for capturing Workers logs during test execution
///
/// Spawns `wrangler tail` as a background process and collects output.
/// Use `start()` to begin tailing, perform test operations, then `stop()` to
/// retrieve all captured logs.
pub struct LiveTail {
    /// Background process handle
    process: Option<tokio::process::Child>,
    /// Receiver for log lines streamed from the tail process
    log_receiver: Option<mpsc::Receiver<String>>,
    /// Worker name being tailed
    worker_name: String,
    /// API token for wrangler
    api_token: String,
    /// Account ID for wrangler
    account_id: String,
}

impl LiveTail {
    /// Create a new LiveTail for a worker
    pub fn new(worker_name: &str, api_token: &str, account_id: &str) -> Self {
        Self {
            process: None,
            log_receiver: None,
            worker_name: worker_name.to_string(),
            api_token: api_token.to_string(),
            account_id: account_id.to_string(),
        }
    }

    /// Start tailing worker logs
    ///
    /// Spawns `wrangler tail` in the background. Logs are collected in a buffer
    /// that can be retrieved when `stop()` is called.
    pub async fn start(&mut self) -> Result<()> {
        tracing::info!("Starting live tail for worker: {}", self.worker_name);

        let mut child = tokio::process::Command::new("npx")
            .arg("wrangler")
            .arg("tail")
            .arg(&self.worker_name)
            .arg("--format")
            .arg("pretty")
            .env("CLOUDFLARE_API_TOKEN", &self.api_token)
            .env("CLOUDFLARE_ACCOUNT_ID", &self.account_id)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn wrangler tail")?;

        // Create channel for streaming logs
        let (tx, rx) = mpsc::channel::<String>(1000);

        // Take stdout and spawn reader task
        if let Some(stdout) = child.stdout.take() {
            let tx_stdout = tx.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // Log each line as it comes in for real-time visibility
                    tracing::debug!(target: "live_tail", "{}", line);
                    let _ = tx_stdout.send(line).await;
                }
            });
        }

        // Also capture stderr
        if let Some(stderr) = child.stderr.take() {
            let tx_stderr = tx;
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "live_tail_stderr", "{}", line);
                    let _ = tx_stderr.send(format!("[stderr] {}", line)).await;
                }
            });
        }

        self.process = Some(child);
        self.log_receiver = Some(rx);

        // Give wrangler tail time to connect
        tokio::time::sleep(Duration::from_secs(3)).await;

        tracing::info!("Live tail started for worker: {}", self.worker_name);
        Ok(())
    }

    /// Stop tailing and return all captured logs
    ///
    /// Kills the wrangler tail process and returns all log lines collected
    /// during the session.
    pub async fn stop(&mut self) -> Result<Vec<String>> {
        tracing::info!("Stopping live tail for worker: {}", self.worker_name);

        // Kill the process
        if let Some(mut process) = self.process.take() {
            let _ = process.kill().await;
            let _ = process.wait().await;
        }

        // Drain all remaining logs from the channel
        let mut logs = Vec::new();
        if let Some(mut rx) = self.log_receiver.take() {
            // Close the channel and drain remaining messages
            rx.close();
            while let Some(line) = rx.recv().await {
                logs.push(line);
            }
        }

        tracing::info!("Live tail stopped, captured {} log lines", logs.len());
        Ok(logs)
    }

    /// Check if the tail is currently running
    pub fn is_running(&self) -> bool {
        self.process.is_some()
    }
}

/// Cloudflare Workers smoke test harness
pub struct WorkersHarness {
    /// Workers script name (unique per test)
    worker_name: String,
    /// R2 bucket name (pre-created, shared across tests)
    bucket_name: String,
    /// Iceberg namespace unique to this test
    namespace: String,
    /// Unique test prefix within bucket (e.g., "smoke-abc123/")
    test_prefix: String,
    /// Cloudflare account ID
    account_id: String,
    /// Cloudflare API token
    api_token: String,
    /// R2 S3 API credentials (from .env)
    r2_access_key_id: String,
    r2_secret_access_key: String,
    /// Catalog mode
    catalog_mode: CatalogMode,
    /// Batch mode
    batch_mode: BatchMode,
    /// KV namespace ID (for catalog mode)
    kv_namespace_id: std::sync::RwLock<Option<String>>,
    /// KV namespace title (unique per test for isolation)
    kv_namespace_title: String,
    /// Live tail session (enabled via SMOKE_TEST_LIVE_TAIL=1)
    live_tail: std::sync::RwLock<Option<LiveTail>>,
}

const KV_NAMESPACE_BINDING: &str = "PENDING_FILES";

impl WorkersHarness {
    /// Create S3 client configured for R2's S3-compatible API
    fn create_s3_client(&self) -> S3Client {
        let credentials = Credentials::new(
            &self.r2_access_key_id,
            &self.r2_secret_access_key,
            None, // session token
            None, // expiration
            "r2-static-credentials",
        );

        let r2_endpoint = format!("https://{}.r2.cloudflarestorage.com", self.account_id);

        let s3_config = S3ConfigBuilder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("auto"))
            .endpoint_url(r2_endpoint)
            .credentials_provider(credentials)
            .force_path_style(false)
            .build();

        S3Client::from_conf(s3_config)
    }

    /// Get wrangler command with proper environment setup
    fn wrangler_command(&self) -> Command {
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .env("CLOUDFLARE_API_TOKEN", &self.api_token)
            .env("CLOUDFLARE_ACCOUNT_ID", &self.account_id);

        // Add catalog env vars if enabled
        if self.catalog_mode == CatalogMode::Enabled {
            cmd.env("CLOUDFLARE_BUCKET_NAME", &self.bucket_name);
        }

        cmd
    }

    /// Create harness from environment variables
    pub fn from_env(catalog_mode: CatalogMode) -> Result<Self> {
        Self::from_env_with_batching(catalog_mode, BatchMode::Disabled)
    }

    /// Create harness from environment variables with batching mode
    pub fn from_env_with_batching(
        catalog_mode: CatalogMode,
        batch_mode: BatchMode,
    ) -> Result<Self> {
        let worker_prefix = std::env::var("SMOKE_TEST_WORKER_PREFIX")
            .unwrap_or_else(|_| "smoke-workers".to_string());
        let account_id =
            std::env::var("CLOUDFLARE_ACCOUNT_ID").context("CLOUDFLARE_ACCOUNT_ID not set")?;
        let api_token =
            std::env::var("CLOUDFLARE_API_TOKEN").context("CLOUDFLARE_API_TOKEN not set")?;

        // R2 bucket (pre-created, shared across tests)
        let bucket_name = std::env::var("CLOUDFLARE_BUCKET_NAME")
            .unwrap_or_else(|_| "otlp2parquet-smoke".to_string());

        // R2 S3 API credentials
        let r2_access_key_id =
            std::env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID not set")?;
        let r2_secret_access_key =
            std::env::var("AWS_SECRET_ACCESS_KEY").context("AWS_SECRET_ACCESS_KEY not set")?;

        // Generate unique test ID for isolation
        let test_id = uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap()
            .to_string();

        let worker_name = format!("{}-{}", worker_prefix, test_id);
        let test_prefix = format!("smoke-{}/", test_id); // Unique prefix within shared bucket
        let namespace = format!("otlp_smoke_{}", test_id);
        // Unique KV namespace title per test to prevent cross-contamination in parallel runs
        let kv_namespace_title = format!("{}_{}", KV_NAMESPACE_BINDING, test_id);

        Ok(Self {
            worker_name,
            bucket_name,
            namespace,
            test_prefix,
            account_id,
            api_token,
            r2_access_key_id,
            r2_secret_access_key,
            catalog_mode,
            batch_mode,
            kv_namespace_id: std::sync::RwLock::new(None),
            kv_namespace_title,
            live_tail: std::sync::RwLock::new(None),
        })
    }

    /// Check if this harness has KV tracking enabled
    pub fn has_kv_tracking(&self) -> bool {
        self.catalog_mode == CatalogMode::Enabled
    }

    /// Check if live tail is enabled via SMOKE_TEST_LIVE_TAIL=1 env var
    pub fn is_live_tail_enabled() -> bool {
        std::env::var("SMOKE_TEST_LIVE_TAIL")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false)
    }

    /// Start live tail if enabled via env var
    ///
    /// Call this after deploy() to begin capturing logs.
    /// Logs are printed to stdout when stop_live_tail() is called.
    pub async fn start_live_tail(&self) -> Result<()> {
        if !Self::is_live_tail_enabled() {
            return Ok(());
        }

        let mut tail = LiveTail::new(&self.worker_name, &self.api_token, &self.account_id);
        tail.start().await?;
        *self.live_tail.write().unwrap() = Some(tail);
        Ok(())
    }

    /// Stop live tail and print captured logs
    ///
    /// Call this before cleanup() to capture and display all logs.
    pub async fn stop_live_tail(&self) -> Result<()> {
        let mut tail_guard = self.live_tail.write().unwrap();
        if let Some(mut tail) = tail_guard.take() {
            let logs = tail.stop().await?;
            if !logs.is_empty() {
                println!("\n=== Live Tail Output ({} lines) ===", logs.len());
                for line in &logs {
                    println!("{}", line);
                }
                println!("=== End Live Tail Output ===\n");
            }
        }
        Ok(())
    }

    /// Count KV receipts for pending files
    ///
    /// Uses the Cloudflare API directly instead of `wrangler kv key list` because
    /// wrangler CLI has caching/consistency issues that can return empty results
    /// even when keys exist.
    pub async fn count_kv_receipts(&self) -> Result<usize> {
        let kv_id = self
            .kv_namespace_id
            .read()
            .unwrap()
            .as_ref()
            .context("KV namespace ID not set")?
            .clone();

        tracing::info!("Counting KV receipts in namespace: {} via API", kv_id);

        // Use Cloudflare API directly for reliable key listing
        let client = reqwest::Client::new();
        let url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{}/keys?prefix=pending:",
            self.account_id, kv_id
        );

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .send()
            .await
            .context("Failed to call Cloudflare KV API")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("KV API returned {}: {}", status, body);
        }

        let body: Value = response
            .json()
            .await
            .context("Failed to parse KV API response")?;

        // API response format: { "success": true, "result": [{ "name": "..." }, ...] }
        let count = body
            .get("result")
            .and_then(|r| r.as_array())
            .map(|arr| arr.len())
            .unwrap_or(0);

        tracing::info!("Found {} KV receipts via API", count);
        Ok(count)
    }

    /// Generate wrangler config from shared template
    fn generate_config(&self) -> Result<PathBuf> {
        let wrangler_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../otlp2parquet-cloudflare");

        // Use the shared template from otlp2parquet crate
        let template = include_str!("../../templates/wrangler.toml");

        let use_iceberg = self.catalog_mode == CatalogMode::Enabled;
        let catalog_mode = if use_iceberg { "iceberg" } else { "none" };
        let use_batching = self.batch_mode == BatchMode::Enabled;

        // Replace placeholders
        let kv_id = self.kv_namespace_id.read().unwrap();
        let kv_namespace_id = kv_id.as_deref().unwrap_or("REPLACE_WITH_KV_NAMESPACE_ID");

        let mut config_content = template
            .replace("{{WORKER_NAME}}", &self.worker_name)
            .replace("{{BUCKET_NAME}}", &self.bucket_name)
            .replace("{{ACCOUNT_ID}}", &self.account_id)
            .replace("{{R2_ACCESS_KEY_ID}}", &self.r2_access_key_id)
            .replace("{{R2_SECRET_ACCESS_KEY}}", &self.r2_secret_access_key)
            .replace("{{CATALOG_MODE}}", catalog_mode)
            .replace("{{BASIC_AUTH_ENABLED}}", "false")
            .replace("{{KV_NAMESPACE_ID}}", kv_namespace_id);

        // Add batching environment variables if enabled
        if use_batching {
            config_content = config_content.replace("{{BATCH_ENABLED}}", "true");
            config_content = config_content.replace(
                "{{BATCH_MAX_ROWS}}",
                "100", // Low threshold for testing
            );
            config_content = config_content.replace(
                "{{BATCH_MAX_AGE_SECS}}",
                "5", // Low threshold for testing
            );
        } else {
            config_content = config_content.replace("{{BATCH_ENABLED}}", "false");
            config_content = config_content.replace("{{BATCH_MAX_ROWS}}", "10000");
            config_content = config_content.replace("{{BATCH_MAX_AGE_SECS}}", "300");
        }

        // Process conditional blocks - smoke tests build from source and use inline creds
        let mut config_content = process_conditional_blocks(
            &config_content,
            &[
                ("BUILD_FROM_SOURCE", true),   // Smoke tests build from source
                ("BUILD_FROM_RELEASE", false), // Not using release builds
                ("INLINE_CREDENTIALS", true),  // Smoke tests use inline credentials
                ("ICEBERG", use_iceberg),
                ("BASICAUTH", false),       // No basic auth for smoke tests
                ("LOGGING", true),          // Enable Workers observability logging
                ("BATCHING", use_batching), // Enable Durable Objects for batching
            ],
        );

        // Build test-only variables string
        let mut test_vars = format!(
            "# Test-only variables (injected by smoke test harness)\n\
             OTLP2PARQUET_R2_PREFIX = \"{}\"\n",
            self.test_prefix
        );
        if use_iceberg {
            test_vars.push_str(&format!("CLOUDFLARE_API_TOKEN = \"{}\"\n", self.api_token));
            // RuntimeConfig reads env vars with OTLP2PARQUET_ prefix via WorkerEnvSource::get()
            test_vars.push_str(&format!(
                "OTLP2PARQUET_ICEBERG_NAMESPACE = \"{}\"\n",
                self.namespace
            ));
        }

        // Insert test vars BEFORE [observability] section (to keep them in [vars])
        // The [vars] section ends when a new section header appears
        // Look for section headers at start of line (after newline)
        let insert_markers = [
            "[observability]",
            "[[analytics_engine_datasets]]",
            "[triggers]",
        ];
        if let Some(insert_pos) = insert_markers
            .iter()
            .filter_map(|marker| config_content.find(marker))
            .min()
        {
            // Insert test vars BEFORE the section header (keeping blank line if present)
            config_content.insert_str(insert_pos, &format!("{}\n", test_vars));
        } else {
            // Fallback: append at end (shouldn't happen with LOGGING=true)
            config_content.push_str(&format!("\n{}", test_vars));
        }

        // Write to wrangler directory (gitignored via wrangler-smoke-*.toml pattern)
        let config_path = wrangler_dir.join(format!("wrangler-{}.toml", self.worker_name));
        std::fs::write(&config_path, config_content)
            .context("Failed to write generated wrangler config")?;

        tracing::info!("Generated wrangler config: {}", config_path.display());
        Ok(config_path)
    }

    /// List all objects in R2 bucket with test prefix using S3 API
    async fn list_r2_objects(&self) -> Result<Vec<String>> {
        let client = self.create_s3_client();

        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        // Paginate through all objects with the test prefix
        loop {
            let mut request = client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .prefix(&self.test_prefix);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let response = match request.send().await {
                Ok(resp) => resp,
                Err(_) => {
                    // If bucket doesn't exist or is empty, return empty list
                    return Ok(Vec::new());
                }
            };

            // Iterate through contents (returns &[Object])
            for object in response.contents() {
                if let Some(key) = object.key() {
                    keys.push(key.to_string());
                }
            }

            // Check if there are more pages
            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(keys)
    }

    /// Delete a single object from R2 bucket using S3 API
    async fn delete_r2_object(&self, key: &str) -> Result<()> {
        let client = self.create_s3_client();

        let _ = client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await;

        Ok(()) // Ignore errors - best effort cleanup
    }

    /// Extract Workers URL from wrangler deploy output
    fn extract_workers_url(&self, output: &str) -> Result<String> {
        // wrangler outputs: "Published {worker_name} (1.23 sec)"
        // "  https://{worker_name}.{account_subdomain}.workers.dev"
        for line in output.lines() {
            if line.contains("https://") && line.contains(".workers.dev") {
                let url = line.trim();
                if url.starts_with("https://") {
                    return Ok(url.to_string());
                }
            }
        }

        // Fallback: construct URL from worker name
        Ok(format!("https://{}.workers.dev", self.worker_name))
    }
}

fn parse_kv_id(text: &str) -> Option<String> {
    // Try to find embedded JSON block in output (wrangler outputs JSON in human-readable text)
    // Look for { ... } containing "id" field
    if let Some(start) = text.find('{') {
        // Find the matching closing brace by counting braces
        let mut depth = 0;
        let mut end = start;
        for (i, c) in text[start..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = start + i + 1;
                        break;
                    }
                }
                _ => {}
            }
        }

        if let Ok(json) = serde_json::from_str::<Value>(&text[start..end]) {
            // Handle wrangler kv namespace create output format:
            // { "kv_namespaces": [{ "binding": "...", "id": "..." }] }
            if let Some(namespaces) = json.get("kv_namespaces").and_then(|v| v.as_array()) {
                if let Some(first) = namespaces.first() {
                    if let Some(id) = first.get("id").and_then(|v| v.as_str()) {
                        return Some(id.to_string());
                    }
                }
            }
            // Direct { "id": "..." } format
            if let Some(id) = json.get("id").and_then(|v| v.as_str()) {
                return Some(id.to_string());
            }
        }
    }

    // Try JSON object per line
    for line in text.lines() {
        if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(line) {
            if let Some(id) = map.get("id").and_then(|v| v.as_str()) {
                return Some(id.to_string());
            }
        }
    }

    // Try TOML-style 'id = "..."' format (wrangler outputs this)
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with("id = \"") && line.ends_with('"') {
            let id = &line[6..line.len() - 1]; // Strip 'id = "' prefix and '"' suffix
            if id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit()) {
                return Some(id.to_string());
            }
        }
    }

    // Try plain hex token fallback (32 char hex string)
    text.split_whitespace()
        .find(|token| token.len() == 32 && token.chars().all(|c| c.is_ascii_hexdigit()))
        .map(|s| s.to_string())
}

fn parse_kv_namespace_list(text: &str) -> Option<String> {
    if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(text) {
        for item in items {
            if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                let title_matches = item
                    .get("title")
                    .and_then(|v| v.as_str())
                    .map(|t| t.contains(KV_NAMESPACE_BINDING))
                    .unwrap_or(false);
                let binding_matches = item
                    .get("binding")
                    .and_then(|v| v.as_str())
                    .map(|b| b == KV_NAMESPACE_BINDING)
                    .unwrap_or(false);

                if title_matches || binding_matches {
                    return Some(id.to_string());
                }
            }
        }
    }

    // Try to find a line with the binding name and extract a hex token
    for line in text.lines() {
        if line.contains(KV_NAMESPACE_BINDING) {
            if let Some(id) = line
                .split_whitespace()
                .find(|token| token.len() == 32 && token.chars().all(|c| c.is_ascii_hexdigit()))
            {
                return Some(id.to_string());
            }
        }
    }

    // Fallback to generic parsing
    parse_kv_id(text)
}

#[async_trait::async_trait]
impl SmokeTestHarness for WorkersHarness {
    async fn deploy(&self) -> Result<DeploymentInfo> {
        tracing::info!(
            "Deploying Workers smoke test: {} (bucket: {}, prefix: {}, catalog: {}, batching: {})",
            self.worker_name,
            self.bucket_name,
            self.test_prefix,
            if self.catalog_mode == CatalogMode::Enabled {
                "enabled"
            } else {
                "disabled"
            },
            if self.batch_mode == BatchMode::Enabled {
                "enabled"
            } else {
                "disabled"
            }
        );

        // Ensure R2 bucket exists (idempotent - ignores "already exists" errors)
        tracing::info!("Ensuring R2 bucket exists: {}", self.bucket_name);
        let create_bucket_cmd = format!("npx wrangler r2 bucket create {}", self.bucket_name);
        let output = self
            .wrangler_command()
            .arg(&create_bucket_cmd)
            .output()
            .context("Failed to run wrangler r2 bucket create")?;

        if output.status.success() {
            tracing::info!("Created R2 bucket: {}", self.bucket_name);
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("already exists") || stderr.contains("AlreadyExists") {
                tracing::info!("R2 bucket already exists: {}", self.bucket_name);
            } else {
                tracing::warn!("Failed to create R2 bucket (may already exist): {}", stderr);
            }
        }

        // Enable R2 Data Catalog if requested
        if self.catalog_mode == CatalogMode::Enabled {
            tracing::info!("Enabling R2 Data Catalog for bucket: {}", self.bucket_name);
            let enable_catalog_cmd =
                format!("npx wrangler r2 bucket catalog enable {}", self.bucket_name);
            let output = self
                .wrangler_command()
                .arg(&enable_catalog_cmd)
                .output()
                .context("Failed to enable R2 catalog")?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::warn!("Failed to enable R2 catalog: {}", stderr);
                // Continue anyway - catalog might already be enabled
            }

            // Create KV namespace for pending files (unique per test for isolation)
            tracing::info!("Creating KV namespace: {}", self.kv_namespace_title);
            let create_kv_cmd = format!(
                "npx wrangler kv namespace create {} --binding {}",
                self.kv_namespace_title, KV_NAMESPACE_BINDING
            );
            let output = self
                .wrangler_command()
                .arg(create_kv_cmd)
                .output()
                .context("Failed to create KV namespace")?;

            if output.status.success() {
                // Parse KV namespace ID from output
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                let combined = format!("{}\n{}", stdout, stderr);
                if let Some(id) = parse_kv_id(&combined) {
                    tracing::info!("Created KV namespace with ID: {}", id);
                    *self.kv_namespace_id.write().unwrap() = Some(id);
                } else {
                    anyhow::bail!(
                        "KV namespace '{}' created but could not parse ID from output:\nSTDOUT: {}\nSTDERR: {}",
                        self.kv_namespace_title,
                        stdout,
                        stderr
                    );
                }
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // Each test should have its own unique namespace - fail if creation fails
                anyhow::bail!(
                    "Failed to create unique KV namespace '{}': {}",
                    self.kv_namespace_title,
                    stderr
                );
            }
        }

        // Generate wrangler config from template (after KV namespace creation)
        let config_path = self.generate_config()?;

        // Deploy using wrangler CLI with generated config
        // Acquire global lock to prevent concurrent builds (wasm output files are shared)
        let wrangler_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../otlp2parquet-cloudflare");
        let deploy_cmd = format!(
            "cd {} && npx wrangler deploy --config {}",
            wrangler_dir.display(),
            config_path.display()
        );

        let output = {
            let _lock = WRANGLER_DEPLOY_LOCK
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.wrangler_command()
                .arg(&deploy_cmd)
                .output()
                .context("Failed to run wrangler deploy")?
        };

        if !output.status.success() {
            anyhow::bail!(
                "Wrangler deploy failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Extract Workers URL from output
        let output_str = String::from_utf8_lossy(&output.stdout);
        let endpoint = self.extract_workers_url(&output_str)?;

        tracing::info!("Workers deployed successfully: {}", endpoint);

        // Wait for deployment to propagate globally (Workers typically take 2-5 seconds)
        tracing::info!("Waiting 5 seconds for Workers deployment to propagate...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        Ok(DeploymentInfo {
            endpoint: endpoint.clone(),
            catalog_endpoint: if self.catalog_mode == CatalogMode::Enabled {
                // R2 Data Catalog REST endpoint format
                format!(
                    "https://catalog.cloudflarestorage.com/{}/{}",
                    self.account_id, self.bucket_name
                )
            } else {
                "r2-plain-parquet".to_string()
            },
            bucket: self.bucket_name.clone(),
            namespace: self.namespace.clone(),
            resource_ids: HashMap::from([
                ("worker_name".to_string(), self.worker_name.clone()),
                ("bucket_name".to_string(), self.bucket_name.clone()),
                ("account_id".to_string(), self.account_id.clone()),
            ]),
        })
    }

    async fn send_signals(&self, endpoint: &str) -> Result<()> {
        tracing::info!("Sending OTLP signals to Workers");

        let testdata = TestDataSet::load();
        let client = reqwest::Client::new();

        if self.batch_mode == BatchMode::Enabled {
            // In batching mode, send signals multiple times to accumulate in DO
            tracing::info!("Batching mode: sending signals 3 times to accumulate data");
            for i in 1..=3 {
                tracing::info!("Batch iteration {}/3", i);

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

                // Send metrics (gauge)
                client
                    .post(format!("{}/v1/metrics", endpoint))
                    .header("Content-Type", "application/x-protobuf")
                    .body(testdata.metrics_gauge_pb.to_vec())
                    .send()
                    .await
                    .context("Failed to send metrics gauge")?
                    .error_for_status()
                    .context("Metrics gauge request failed")?;

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

                // Small delay between batches
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            // Wait for DO alarm to flush (batch_max_age_secs = 5 + buffer)
            tracing::info!("Waiting 10 seconds for DO alarm to flush batches to R2...");
            tokio::time::sleep(Duration::from_secs(10)).await;
        } else {
            // Direct write mode: send once
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

            // Send metrics (gauge)
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
        }

        // Trigger catalog sync immediately in catalog mode to avoid waiting for cron
        if self.catalog_mode == CatalogMode::Enabled {
            tracing::info!("Triggering catalog sync via internal endpoint");
            let resp = client
                .post(format!("{}/__internal/sync_catalog", endpoint))
                .send()
                .await
                .context("Failed to call /__internal/sync_catalog")?;

            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();

            if !status.is_success() {
                anyhow::bail!("Catalog sync endpoint failed (status {}): {}", status, body);
            }

            tracing::info!(
                "Catalog sync endpoint completed successfully (status {}): {}",
                status,
                body
            );
        }

        Ok(())
    }

    async fn verify_execution(&self) -> Result<ExecutionStatus> {
        tracing::info!("Verifying Workers execution via tail logs");

        // Wait for logs to be available
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Query Workers tail logs via wrangler
        let tail_cmd = format!(
            "npx wrangler tail {} --format json --once",
            self.worker_name
        );

        let output = self
            .wrangler_command()
            .arg(&tail_cmd)
            .output()
            .context("Failed to run wrangler tail")?;

        let log_output = String::from_utf8_lossy(&output.stdout);

        // Count error/warning messages (filter out CLI help text)
        let is_actual_error = |line: &&str| -> bool {
            // Skip CLI help text (contains -- flags or [choices:])
            if line.contains("--") || line.contains("[choices:") || line.contains("[array]") {
                return false;
            }
            // Match actual error/warning log messages
            line.contains("\"error\"")
                || line.contains("\"ERROR\"")
                || line.contains("\"warn\"")
                || line.contains("(error)")
                || line.contains("Error:")
        };

        let error_count = log_output.lines().filter(is_actual_error).count();

        let sample_errors: Vec<String> = log_output
            .lines()
            .filter(is_actual_error)
            .take(5)
            .map(|s| s.to_string())
            .collect();

        tracing::info!(
            "Found {} potential errors/warnings in Workers logs",
            error_count
        );

        Ok(ExecutionStatus {
            error_count,
            sample_errors,
            metrics: HashMap::new(),
        })
    }

    fn duckdb_verifier(&self, _info: &DeploymentInfo) -> DuckDBVerifier {
        DuckDBVerifier {
            catalog_type: if self.catalog_mode == CatalogMode::Enabled {
                CatalogType::R2Catalog
            } else {
                CatalogType::PlainParquet
            },
            catalog_endpoint: if self.catalog_mode == CatalogMode::Enabled {
                // R2 Data Catalog REST endpoint format
                format!(
                    "https://catalog.cloudflarestorage.com/{}/{}",
                    self.account_id, self.bucket_name
                )
            } else {
                "r2-plain-parquet".to_string()
            },
            storage_config: StorageConfig {
                backend: StorageBackend::R2 {
                    account_id: self.account_id.clone(),
                    bucket: self.bucket_name.clone(),
                    credentials: R2Credentials {
                        access_key_id: self.r2_access_key_id.clone(),
                        secret_access_key: self.r2_secret_access_key.clone(),
                    },
                },
                prefix: Some(self.test_prefix.clone()),
            },
            catalog_token: if self.catalog_mode == CatalogMode::Enabled {
                Some(self.api_token.clone())
            } else {
                None
            },
        }
    }

    async fn cleanup(&self) -> Result<()> {
        tracing::info!("Cleaning up Workers smoke test (fail-safe mode)");

        // 1. Delete Workers script using Cloudflare API (wrangler delete has permission issues)
        let client = reqwest::Client::new();
        let delete_url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/workers/scripts/{}",
            self.account_id, self.worker_name
        );

        match client
            .delete(&delete_url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!("Workers script deleted successfully");
            }
            Ok(resp) => {
                tracing::warn!("Failed to delete Workers script: HTTP {}", resp.status());
            }
            Err(e) => {
                tracing::warn!("Failed to delete Workers script: {}", e);
            }
        }

        // Cleanup generated config file (gitignored via pattern)
        let wrangler_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../otlp2parquet-cloudflare");
        let config_path = wrangler_dir.join(format!("wrangler-{}.toml", self.worker_name));
        let _ = fs::remove_file(config_path);

        // 2. Delete KV namespace if we created one
        if let Some(kv_id) = self.kv_namespace_id.read().unwrap().as_ref() {
            tracing::info!("Deleting KV namespace: {}", kv_id);
            let delete_kv_cmd =
                format!("npx wrangler kv namespace delete --namespace-id {}", kv_id);
            match self.wrangler_command().arg(&delete_kv_cmd).output() {
                Ok(output) if output.status.success() => {
                    tracing::info!("KV namespace deleted successfully");
                }
                Ok(_) => {
                    tracing::warn!("Failed to delete KV namespace (may not exist)");
                }
                Err(e) => {
                    tracing::warn!("Failed to delete KV namespace: {}", e);
                }
            }
        }

        // 3. Delete all R2 objects with test prefix using S3 API
        tracing::info!(
            "Deleting test objects from bucket {} with prefix {}",
            self.bucket_name,
            self.test_prefix
        );
        match self.list_r2_objects().await {
            Ok(objects) => {
                tracing::info!("Found {} objects to delete", objects.len());
                for key in objects {
                    let _ = self.delete_r2_object(&key).await;
                }
            }
            Err(e) => {
                tracing::warn!("Failed to list R2 objects: {}", e);
            }
        }

        // Always return Ok - cleanup failures are logged but don't fail the test
        Ok(())
    }
}

impl WorkersHarness {
    /// Count test objects written under the test prefix
    pub async fn count_r2_objects(&self) -> Result<usize> {
        Ok(self.list_r2_objects().await?.len())
    }

    /// Create a live tail session for this worker
    ///
    /// Use this to capture real-time logs during test execution:
    /// ```ignore
    /// let mut tail = harness.create_live_tail();
    /// tail.start().await?;
    ///
    /// // ... perform test operations ...
    ///
    /// let logs = tail.stop().await?;
    /// for line in &logs {
    ///     println!("{}", line);
    /// }
    /// ```
    pub fn create_live_tail(&self) -> LiveTail {
        LiveTail::new(&self.worker_name, &self.api_token, &self.account_id)
    }

    /// Get the worker name (useful for external tail commands)
    pub fn worker_name(&self) -> &str {
        &self.worker_name
    }
}
