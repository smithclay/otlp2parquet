//! Server smoke test harness
//!
//! Tests otlp2parquet server running in Docker Compose with MinIO S3.

use super::{
    DeploymentInfo, DuckDBVerifier, ExecutionStatus, S3Credentials, SmokeTestHarness,
    StorageBackend, StorageConfig, TestDataSet,
};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::time::Duration;
use tokio::process::Command;

const S3_BUCKET: &str = "otlp";

/// Server harness for Docker Compose testing
pub struct ServerHarness {
    compose_project_name: String,
    /// Prefix for test isolation
    prefix: String,
    // Dynamic port allocations (0 = let OS assign)
    minio_api_port: u16,
    minio_console_port: u16,
    http_port: u16,
    // Track if we've been cleaned up already to avoid double cleanup
    cleaned_up: std::sync::Arc<std::sync::Mutex<bool>>,
}

impl ServerHarness {
    /// Create new server harness
    pub async fn new() -> Result<Self> {
        // Generate unique project name for test isolation
        let test_id = uuid::Uuid::new_v4().simple().to_string();
        let project_name = format!("otlp2parquet-test-{}", &test_id[..8]);
        let prefix = format!("smoke-{}/", &test_id[..8]);

        tracing::info!("Creating ServerHarness with project={}", project_name);

        // Cleanup any stale Docker containers first
        Self::cleanup_stale_containers().await;

        // Use OS-assigned ports to avoid conflicts
        let minio_api_port = Self::allocate_port().await?;
        let minio_console_port = Self::allocate_port().await?;
        let http_port = Self::allocate_port().await?;

        tracing::info!(
            "Allocated ports: minio_api={}, minio_console={}, http={}",
            minio_api_port,
            minio_console_port,
            http_port
        );

        Ok(Self {
            compose_project_name: project_name,
            prefix,
            minio_api_port,
            minio_console_port,
            http_port,
            cleaned_up: std::sync::Arc::new(std::sync::Mutex::new(false)),
        })
    }

    /// Cleanup stale Docker containers from previous test runs
    async fn cleanup_stale_containers() {
        tracing::info!("Checking for stale Docker containers...");

        // List all compose projects matching our test pattern
        let output = Command::new("docker")
            .args([
                "ps",
                "-a",
                "--filter",
                "name=otlp2parquet-test-",
                "--format",
                "{{.Names}}",
            ])
            .output()
            .await;

        if let Ok(output) = output {
            let containers = String::from_utf8_lossy(&output.stdout);
            if !containers.is_empty() {
                tracing::info!("Found stale containers, cleaning up...");

                // Extract unique project names from container names
                let mut projects = std::collections::HashSet::new();
                for line in containers.lines() {
                    if line.starts_with("otlp2parquet-test-") {
                        // Extract project name: otlp2parquet-test-{uuid}
                        let parts: Vec<&str> = line.splitn(4, '-').collect();
                        if parts.len() >= 3 {
                            let project_name = format!("{}-{}-{}", parts[0], parts[1], parts[2]);
                            projects.insert(project_name);
                        }
                    }
                }

                // Clean up each stale project
                for project in projects {
                    tracing::debug!("Cleaning up stale project: {}", project);
                    let _ = Command::new("docker")
                        .args(["compose", "-p", &project, "down", "-v", "--remove-orphans"])
                        .output()
                        .await;
                }

                // Give Docker a moment to release ports
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    /// Allocate a random available port by binding to port 0
    async fn allocate_port() -> Result<u16> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        drop(listener); // Release the port
        Ok(port)
    }

    /// Wait for service to be healthy
    async fn wait_for_health(&self, url: &str, timeout_secs: u64) -> Result<()> {
        tracing::info!(
            "Waiting for {} to be healthy (timeout={}s)",
            url,
            timeout_secs
        );

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()?;

        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let mut attempts = 0;

        loop {
            attempts += 1;
            if start.elapsed() > timeout {
                anyhow::bail!(
                    "Health check timeout for {} after {} attempts",
                    url,
                    attempts
                );
            }

            match client.get(url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    tracing::info!("Service healthy: {} (after {} attempts)", url, attempts);
                    return Ok(());
                }
                Ok(resp) => {
                    tracing::debug!(
                        "Health check attempt {}: {} returned {}",
                        attempts,
                        url,
                        resp.status()
                    );
                }
                Err(e) => {
                    tracing::debug!("Health check attempt {}: {} error: {}", attempts, url, e);
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Get Docker Compose environment variables for port configuration
    fn compose_env(&self) -> Vec<(String, String)> {
        vec![
            (
                "COMPOSE_PROJECT_NAME".to_string(),
                self.compose_project_name.clone(),
            ),
            (
                "MINIO_API_PORT".to_string(),
                self.minio_api_port.to_string(),
            ),
            (
                "MINIO_CONSOLE_PORT".to_string(),
                self.minio_console_port.to_string(),
            ),
            ("HTTP_PORT".to_string(), self.http_port.to_string()),
            ("OTLP2PARQUET_PREFIX".to_string(), self.prefix.clone()),
        ]
    }
}

#[async_trait::async_trait]
impl SmokeTestHarness for ServerHarness {
    async fn deploy(&self) -> Result<DeploymentInfo> {
        tracing::info!(
            "Deploying server with Docker Compose (project: {})",
            self.compose_project_name
        );
        tracing::info!(
            "Ports: minio_api={}, http={}",
            self.minio_api_port,
            self.http_port
        );

        // Start MinIO and otlp2parquet services
        let services = vec!["minio", "otlp2parquet"];
        tracing::info!("Starting services: {:?}", services);

        // Retry logic for port binding issues
        let max_retries = 3;
        let mut last_error = None;

        for attempt in 1..=max_retries {
            if attempt > 1 {
                tracing::info!("Retry attempt {} of {}", attempt, max_retries);
                // Clean up previous attempt
                let _ = Command::new("docker")
                    .args(["compose", "-p", &self.compose_project_name, "down", "-v"])
                    .output()
                    .await;
                // Wait for ports to be released
                tokio::time::sleep(Duration::from_secs(2)).await;
            }

            // Start services with unique project name
            let mut cmd = Command::new("docker");
            cmd.args(["compose", "-p", &self.compose_project_name, "up", "-d"]);

            // Add environment variables for port configuration
            for (key, value) in self.compose_env() {
                tracing::debug!("Setting env: {}={}", key, value);
                cmd.env(key, value);
            }

            // Add services
            cmd.args(&services);

            tracing::info!(
                "Executing: docker compose -p {} up -d {:?} (attempt {})",
                self.compose_project_name,
                services,
                attempt
            );

            let output = cmd
                .output()
                .await
                .context("Failed to start docker compose")?;

            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                tracing::debug!("Docker compose output:\n{}", stdout);
                tracing::info!("Docker Compose services started successfully");
                break;
            }

            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);

            // Check if it's a port binding error
            if stderr.contains("address already in use")
                || stderr.contains("port is already allocated")
            {
                tracing::warn!(
                    "Port binding conflict on attempt {}: {}",
                    attempt,
                    stderr
                        .lines()
                        .find(|l| l.contains("address already in use")
                            || l.contains("port is already allocated"))
                        .unwrap_or("")
                );
                last_error = Some(format!("Port binding conflict: {}", stderr));
                continue;
            }

            // Other error - fail immediately
            tracing::error!(
                "Docker compose failed.\nSTDOUT:\n{}\nSTDERR:\n{}",
                stdout,
                stderr
            );
            anyhow::bail!("Docker compose up failed: {}", stderr);
        }

        // If we exhausted all retries, fail with the last error
        if let Some(error) = last_error {
            anyhow::bail!(
                "Docker compose failed after {} retries: {}",
                max_retries,
                error
            );
        }

        // Build endpoint URLs with dynamic ports
        let minio_endpoint = format!("http://localhost:{}", self.minio_api_port);
        let otlp_endpoint = format!("http://localhost:{}", self.http_port);

        // Wait for MinIO to be ready
        tracing::info!("Waiting for MinIO...");
        self.wait_for_health(&format!("{}/minio/health/live", minio_endpoint), 30)
            .await?;

        // Wait for otlp2parquet server to be ready
        tracing::info!("Waiting for otlp2parquet server...");
        self.wait_for_health(&format!("{}/health", otlp_endpoint), 30)
            .await?;

        tracing::info!("All services healthy and ready");

        Ok(DeploymentInfo {
            endpoint: otlp_endpoint,
            bucket: S3_BUCKET.to_string(),
            prefix: self.prefix.clone(),
            resource_ids: HashMap::from([
                (
                    "compose_project".to_string(),
                    self.compose_project_name.clone(),
                ),
                ("minio_endpoint".to_string(), minio_endpoint.clone()),
            ]),
        })
    }

    async fn send_signals(&self, endpoint: &str) -> Result<()> {
        tracing::info!("Sending test signals to {}", endpoint);
        let testdata = TestDataSet::load();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        // Send logs
        tracing::info!(
            "Sending logs ({} bytes) to {}/v1/logs",
            testdata.logs_pb.len(),
            endpoint
        );
        let resp = client
            .post(format!("{}/v1/logs", endpoint))
            .header("content-type", "application/x-protobuf")
            .body(testdata.logs_pb.to_vec())
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::error!("Logs request failed: {} - {}", status, body);
            anyhow::bail!("Logs request failed: {}", status);
        }
        tracing::info!("Logs sent successfully");

        // Send metrics
        tracing::info!(
            "Sending metrics gauge ({} bytes) to {}/v1/metrics",
            testdata.metrics_gauge_pb.len(),
            endpoint
        );
        let resp = client
            .post(format!("{}/v1/metrics", endpoint))
            .header("content-type", "application/x-protobuf")
            .body(testdata.metrics_gauge_pb.to_vec())
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::error!("Metrics request failed: {} - {}", status, body);
            anyhow::bail!("Metrics request failed: {}", status);
        }
        tracing::info!("Metrics sent successfully");

        // Send traces
        tracing::info!(
            "Sending traces ({} bytes) to {}/v1/traces",
            testdata.traces_pb.len(),
            endpoint
        );
        let resp = client
            .post(format!("{}/v1/traces", endpoint))
            .header("content-type", "application/x-protobuf")
            .body(testdata.traces_pb.to_vec())
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::error!("Traces request failed: {} - {}", status, body);
            anyhow::bail!("Traces request failed: {}", status);
        }
        tracing::info!("Traces sent successfully");

        // Wait for async processing
        tracing::info!("Waiting 3 seconds for processing to complete...");
        tokio::time::sleep(Duration::from_secs(3)).await;
        tracing::info!("Processing wait complete");

        Ok(())
    }

    async fn verify_execution(&self) -> Result<ExecutionStatus> {
        tracing::info!(
            "Checking Docker logs for errors (project: {})",
            self.compose_project_name
        );

        let output = Command::new("docker")
            .args([
                "compose",
                "-p",
                &self.compose_project_name,
                "logs",
                "otlp2parquet",
            ])
            .output()
            .await
            .context("Failed to get docker logs")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!("Docker logs command had issues: {}", stderr);
        }

        let logs = String::from_utf8_lossy(&output.stdout);
        tracing::debug!("Retrieved {} bytes of logs", logs.len());

        // Look for error patterns in logs
        let error_lines: Vec<String> = logs
            .lines()
            .filter(|line| {
                line.to_lowercase().contains("error")
                    || line.to_lowercase().contains("panic")
                    || line.to_lowercase().contains("fatal")
            })
            .map(String::from)
            .collect();

        let error_count = error_lines.len();
        let sample_errors: Vec<String> = error_lines.into_iter().take(5).collect();

        if error_count > 0 {
            tracing::warn!("Found {} error lines in logs", error_count);
            for (i, error) in sample_errors.iter().enumerate() {
                tracing::debug!("  Error {}: {}", i + 1, error);
            }
        } else {
            tracing::info!("No errors found in Docker logs");
        }

        Ok(ExecutionStatus {
            error_count,
            sample_errors,
            metrics: HashMap::new(),
        })
    }

    fn duckdb_verifier(&self, _info: &DeploymentInfo) -> DuckDBVerifier {
        let minio_endpoint = format!("http://localhost:{}", self.minio_api_port);

        tracing::debug!("Creating DuckDB verifier with minio={}", minio_endpoint);

        DuckDBVerifier {
            storage_config: StorageConfig {
                backend: StorageBackend::S3 {
                    region: "us-east-1".to_string(),
                    bucket: S3_BUCKET.to_string(),
                    endpoint: Some(minio_endpoint),
                    credentials: S3Credentials::Static {
                        access_key: "minioadmin".to_string(),
                        secret_key: "minioadmin".to_string(),
                    },
                },
                prefix: Some(self.prefix.clone()),
            },
        }
    }

    async fn cleanup(&self) -> Result<()> {
        // Check if already cleaned up
        {
            let mut cleaned = self.cleaned_up.lock().unwrap();
            if *cleaned {
                tracing::debug!(
                    "Docker Compose project {} already cleaned up, skipping",
                    self.compose_project_name
                );
                return Ok(());
            }
            *cleaned = true;
        }

        tracing::info!(
            "Cleaning up Docker Compose project: {}",
            self.compose_project_name
        );

        let output = Command::new("docker")
            .args(["compose", "-p", &self.compose_project_name, "down", "-v"])
            .output()
            .await;

        match output {
            Ok(out) if out.status.success() => {
                tracing::info!("Docker Compose cleanup successful");
                Ok(())
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                tracing::warn!("Docker Compose cleanup had issues: {}", stderr);
                Ok(()) // Don't fail on cleanup errors
            }
            Err(e) => {
                tracing::warn!("Failed to run docker compose down: {}", e);
                Ok(()) // Don't fail on cleanup errors
            }
        }
    }
}

impl Drop for ServerHarness {
    fn drop(&mut self) {
        // Check if cleanup has already been run
        let already_cleaned = {
            let cleaned = self.cleaned_up.lock().unwrap();
            *cleaned
        };

        if already_cleaned {
            return;
        }

        // Mark as cleaned up before running cleanup to avoid recursion
        {
            let mut cleaned = self.cleaned_up.lock().unwrap();
            *cleaned = true;
        }

        tracing::info!(
            "Drop: Cleaning up Docker Compose project: {}",
            self.compose_project_name
        );

        // Use blocking command since Drop can't be async
        let output = std::process::Command::new("docker")
            .args(["compose", "-p", &self.compose_project_name, "down", "-v"])
            .output();

        match output {
            Ok(out) if out.status.success() => {
                tracing::info!("Docker Compose cleanup successful (Drop)");
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                tracing::warn!("Docker Compose cleanup had issues (Drop): {}", stderr);
            }
            Err(e) => {
                tracing::warn!("Failed to run docker compose down (Drop): {}", e);
            }
        }
    }
}
