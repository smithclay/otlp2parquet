//! End-to-end Docker integration tests
//!
//! These tests require Docker and docker-compose to be running.
//! They validate the full pipeline from OTLP ingestion to Parquet storage
//! with real MinIO and Apache Iceberg REST catalog services.
//!
//! Enable with: cargo test --test e2e_docker --features docker-tests

#![cfg(feature = "docker-tests")]

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use reqwest::Client;
use std::fs;
use std::time::Duration;

// Service endpoints (must match docker-compose.yml)
const OTLP_ENDPOINT: &str = "http://localhost:4318";
const MINIO_ENDPOINT: &str = "http://localhost:9000";
const REST_CATALOG_ENDPOINT: &str = "http://localhost:8181";
const S3_BUCKET: &str = "otlp";

/// Send OTLP data to the otlp2parquet service
async fn send_otlp(path: &str, data: &[u8], content_type: &str) -> anyhow::Result<()> {
    eprintln!("[send_otlp] Sending {} bytes to {}", data.len(), path);

    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;

    let resp = client
        .post(format!("{}{}", OTLP_ENDPOINT, path))
        .header("content-type", content_type)
        .body(data.to_vec())
        .send()
        .await?;

    let status = resp.status();
    eprintln!("[send_otlp] Response status: {}", status);

    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        eprintln!("[send_otlp] Error response body: {}", body);
        anyhow::bail!("OTLP request failed: {} - {}", status, body);
    }

    eprintln!("[send_otlp] Successfully sent to {}", path);
    Ok(())
}

/// List Parquet files in MinIO with given prefix
async fn verify_s3_files(prefix: &str) -> anyhow::Result<Vec<String>> {
    eprintln!(
        "[verify_s3_files] Searching bucket '{}' with prefix '{}'",
        S3_BUCKET, prefix
    );

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(MINIO_ENDPOINT)
        .credentials_provider(Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "static",
        ))
        .region("us-east-1")
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();

    let client = aws_sdk_s3::Client::from_conf(s3_config);

    let resp = client
        .list_objects_v2()
        .bucket(S3_BUCKET)
        .prefix(prefix)
        .send()
        .await?;

    let all_objects: Vec<String> = resp
        .contents()
        .iter()
        .filter_map(|obj| obj.key())
        .map(String::from)
        .collect();

    eprintln!(
        "[verify_s3_files] Found {} total objects",
        all_objects.len()
    );

    let files: Vec<String> = all_objects
        .iter()
        .filter(|key| key.ends_with(".parquet"))
        .cloned()
        .collect();

    eprintln!(
        "[verify_s3_files] Found {} parquet files: {:?}",
        files.len(),
        files
    );

    Ok(files)
}

/// Verify a file in MinIO starts with Parquet magic bytes (PAR1)
async fn verify_parquet_magic(key: &str) -> anyhow::Result<()> {
    eprintln!(
        "[verify_parquet_magic] Checking magic bytes for key: {}",
        key
    );

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(MINIO_ENDPOINT)
        .credentials_provider(Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "static",
        ))
        .region("us-east-1")
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();

    let client = aws_sdk_s3::Client::from_conf(s3_config);

    let resp = client
        .get_object()
        .bucket(S3_BUCKET)
        .key(key)
        .range("bytes=0-3")
        .send()
        .await?;

    let bytes = resp.body.collect().await?.into_bytes();

    eprintln!("[verify_parquet_magic] Read {} bytes", bytes.len());

    anyhow::ensure!(
        bytes.len() >= 4 && &bytes[..4] == b"PAR1",
        "File does not start with PAR1 magic bytes"
    );

    eprintln!("[verify_parquet_magic] ✓ Valid Parquet file");

    Ok(())
}

/// Check if an Iceberg table exists in REST catalog
async fn verify_iceberg_table(namespace: &str, table: &str) -> anyhow::Result<bool> {
    let url = format!(
        "{}/v1/namespaces/{}/tables/{}",
        REST_CATALOG_ENDPOINT, namespace, table
    );
    eprintln!("[verify_iceberg_table] Checking: {}", url);

    let client = Client::new();
    let resp = client.get(&url).send().await?;

    eprintln!("[verify_iceberg_table] Response status: {}", resp.status());

    let exists = resp.status().is_success();
    if !exists {
        let body = resp.text().await.unwrap_or_default();
        eprintln!("[verify_iceberg_table] Table not found. Response: {}", body);
    } else {
        eprintln!("[verify_iceberg_table] ✓ Table exists");
    }

    Ok(exists)
}

// ============================================================================
// CORE PIPELINE TESTS
// ============================================================================

#[tokio::test]
async fn test_logs_pipeline() -> anyhow::Result<()> {
    eprintln!("\n========== test_logs_pipeline ==========");

    // Send OTLP logs
    eprintln!("[test_logs_pipeline] Reading testdata/logs.pb");
    let payload = fs::read("testdata/logs.pb")?;
    eprintln!("[test_logs_pipeline] Read {} bytes", payload.len());

    eprintln!("[test_logs_pipeline] Sending logs to /v1/logs");
    let resp_status = send_otlp("/v1/logs", &payload, "application/x-protobuf").await;
    if let Err(e) = &resp_status {
        eprintln!("[test_logs_pipeline] Failed to send logs: {}", e);
    }
    resp_status?;
    eprintln!("[test_logs_pipeline] Successfully sent logs");

    // Wait for async processing (batching disabled in tests for immediate writes)
    eprintln!("[test_logs_pipeline] Waiting 2 seconds for processing...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    eprintln!("[test_logs_pipeline] Done waiting");

    // Verify Parquet files exist in MinIO (Iceberg structure: otel/otel_logs/data/)
    eprintln!("[test_logs_pipeline] Checking for log files in S3");
    let files = verify_s3_files("otel/otel_logs/data/").await?;

    // If no files found, check all files in bucket for debugging
    if files.is_empty() {
        eprintln!("[test_logs_pipeline] No log files found, checking entire bucket");
        let all_files = verify_s3_files("").await?;
        eprintln!("[test_logs_pipeline] All files in bucket: {:?}", all_files);
    } else {
        eprintln!(
            "[test_logs_pipeline] Found {} log files: {:?}",
            files.len(),
            files
        );
    }

    assert!(!files.is_empty(), "Expected at least one log file");

    // Verify first file is valid Parquet
    eprintln!("[test_logs_pipeline] Verifying first file is valid Parquet");
    verify_parquet_magic(&files[0]).await?;

    eprintln!("[test_logs_pipeline] ✓ Test passed\n");
    Ok(())
}

#[tokio::test]
async fn test_metrics_pipeline() -> anyhow::Result<()> {
    eprintln!("\n========== test_metrics_pipeline ==========");

    // Test each metric type separately
    for (metric_type, file) in [
        ("gauge", "testdata/metrics_gauge.pb"),
        ("sum", "testdata/metrics_sum.pb"),
        ("histogram", "testdata/metrics_histogram.pb"),
        (
            "exponential_histogram",
            "testdata/metrics_exponential_histogram.pb",
        ),
        ("summary", "testdata/metrics_summary.pb"),
    ] {
        eprintln!(
            "\n[test_metrics_pipeline] Testing metric type: {}",
            metric_type
        );
        eprintln!("[test_metrics_pipeline] Reading {}", file);
        let payload = fs::read(file)?;
        eprintln!("[test_metrics_pipeline] Read {} bytes", payload.len());

        send_otlp("/v1/metrics", &payload, "application/x-protobuf").await?;

        // Wait for async processing
        eprintln!("[test_metrics_pipeline] Waiting 2 seconds for processing...");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify files exist for this metric type (Iceberg structure: otel/otel_metrics_{type}/data/)
        eprintln!("[test_metrics_pipeline] Checking for {} files", metric_type);
        let files = verify_s3_files(&format!("otel/otel_metrics_{}/data/", metric_type)).await?;
        assert!(
            !files.is_empty(),
            "Expected at least one {} file",
            metric_type
        );

        // Verify first file is valid Parquet
        eprintln!(
            "[test_metrics_pipeline] Verifying Parquet for {}",
            metric_type
        );
        verify_parquet_magic(&files[0]).await?;
        eprintln!("[test_metrics_pipeline] ✓ {} passed", metric_type);
    }

    eprintln!("[test_metrics_pipeline] ✓ All metric types passed\n");
    Ok(())
}

#[tokio::test]
async fn test_traces_pipeline() -> anyhow::Result<()> {
    eprintln!("\n========== test_traces_pipeline ==========");

    // Send OTLP traces
    eprintln!("[test_traces_pipeline] Reading testdata/traces.pb");
    let payload = fs::read("testdata/traces.pb")?;
    eprintln!("[test_traces_pipeline] Read {} bytes", payload.len());

    eprintln!("[test_traces_pipeline] Sending traces to /v1/traces");
    send_otlp("/v1/traces", &payload, "application/x-protobuf").await?;

    // Wait for async processing
    eprintln!("[test_traces_pipeline] Waiting 2 seconds for processing...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify Parquet files exist in MinIO (Iceberg structure: otel/otel_traces/data/)
    eprintln!("[test_traces_pipeline] Checking for trace files in S3");
    let files = verify_s3_files("otel/otel_traces/data/").await?;
    assert!(!files.is_empty(), "Expected at least one trace file");

    // Verify first file is valid Parquet
    eprintln!("[test_traces_pipeline] Verifying first file is valid Parquet");
    verify_parquet_magic(&files[0]).await?;

    eprintln!("[test_traces_pipeline] ✓ Test passed\n");
    Ok(())
}

// ============================================================================
// ICEBERG INTEGRATION TESTS
// ============================================================================

#[tokio::test]
async fn test_iceberg_logs_commit() -> anyhow::Result<()> {
    eprintln!("\n========== test_iceberg_logs_commit ==========");

    // Send OTLP logs
    eprintln!("[test_iceberg_logs_commit] Reading testdata/logs.pb");
    let payload = fs::read("testdata/logs.pb")?;
    eprintln!("[test_iceberg_logs_commit] Read {} bytes", payload.len());

    eprintln!("[test_iceberg_logs_commit] Sending logs to /v1/logs");
    send_otlp("/v1/logs", &payload, "application/x-protobuf").await?;

    // Wait for async processing and catalog commit
    eprintln!("[test_iceberg_logs_commit] Waiting 3 seconds for catalog commit...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify REST catalog has the table
    eprintln!("[test_iceberg_logs_commit] Verifying otel.otel_logs table exists");
    let exists = verify_iceberg_table("otel", "otel_logs").await?;
    assert!(exists, "Expected otel.otel_logs table in REST catalog");

    eprintln!("[test_iceberg_logs_commit] ✓ Test passed\n");
    Ok(())
}

#[tokio::test]
async fn test_iceberg_metrics_commit() -> anyhow::Result<()> {
    eprintln!("\n========== test_iceberg_metrics_commit ==========");

    // Send gauge metrics (one metric type as example)
    eprintln!("[test_iceberg_metrics_commit] Reading testdata/metrics_gauge.pb");
    let payload = fs::read("testdata/metrics_gauge.pb")?;
    eprintln!("[test_iceberg_metrics_commit] Read {} bytes", payload.len());

    eprintln!("[test_iceberg_metrics_commit] Sending gauge metrics to /v1/metrics");
    send_otlp("/v1/metrics", &payload, "application/x-protobuf").await?;

    // Wait for async processing and catalog commit
    eprintln!("[test_iceberg_metrics_commit] Waiting 3 seconds for catalog commit...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify REST catalog has the metrics_gauge table
    eprintln!("[test_iceberg_metrics_commit] Verifying otel.otel_metrics_gauge table exists");
    let exists = verify_iceberg_table("otel", "otel_metrics_gauge").await?;
    assert!(
        exists,
        "Expected otel.otel_metrics_gauge table in REST catalog"
    );

    eprintln!("[test_iceberg_metrics_commit] ✓ Test passed\n");
    Ok(())
}

#[tokio::test]
async fn test_iceberg_traces_commit() -> anyhow::Result<()> {
    eprintln!("\n========== test_iceberg_traces_commit ==========");

    // Send OTLP traces
    eprintln!("[test_iceberg_traces_commit] Reading testdata/traces.pb");
    let payload = fs::read("testdata/traces.pb")?;
    eprintln!("[test_iceberg_traces_commit] Read {} bytes", payload.len());

    eprintln!("[test_iceberg_traces_commit] Sending traces to /v1/traces");
    send_otlp("/v1/traces", &payload, "application/x-protobuf").await?;

    // Wait for async processing and catalog commit
    eprintln!("[test_iceberg_traces_commit] Waiting 3 seconds for catalog commit...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify REST catalog has the table
    eprintln!("[test_iceberg_traces_commit] Verifying otel.otel_traces table exists");
    let exists = verify_iceberg_table("otel", "otel_traces").await?;
    assert!(exists, "Expected otel.otel_traces table in REST catalog");

    eprintln!("[test_iceberg_traces_commit] ✓ Test passed\n");
    Ok(())
}
