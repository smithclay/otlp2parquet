//! Platform smoke tests for otlp2parquet
//!
//! Tests the full pipeline on real cloud platforms:
//! - Lambda + S3 Tables Catalog
//! - Cloudflare Workers + R2 Data Catalog
//!
//! These tests require cloud credentials and are typically run in CI, not locally.
//!
//! ## Running Tests
//!
//! Lambda smoke tests:
//! ```bash
//! export AWS_REGION=us-west-2
//! export SMOKE_TEST_DEPLOYMENT_BUCKET=my-deployment-bucket
//! make build-lambda
//! cargo test --test smoke_tests --features smoke-lambda -- lambda
//! ```
//!
//! Workers smoke tests:
//! ```bash
//! export CLOUDFLARE_API_TOKEN=...
//! export CLOUDFLARE_ACCOUNT_ID=...
//! make wasm-compress
//! cargo test --test smoke_tests --features smoke-workers -- workers
//! ```

#[path = "smoke/mod.rs"]
mod smoke;

#[cfg(feature = "smoke-lambda")]
mod lambda_tests {
    use super::smoke::{lambda::LambdaHarness, SmokeTestHarness};
    use anyhow::Result;

    #[tokio::test]
    async fn lambda_s3_tables_full_pipeline() -> Result<()> {
        let harness = LambdaHarness::from_env().await?;

        // Deploy Lambda function + S3 Tables
        let deployment = harness.deploy().await?;

        // Collect test results without panicking
        let mut test_errors = Vec::new();

        // Send OTLP signals
        if let Err(e) = harness.send_signals(&deployment.endpoint).await {
            test_errors.push(format!("Failed to send signals: {}", e));
        }

        // Verify no errors in CloudWatch Logs
        match harness.verify_execution().await {
            Ok(status) => {
                if !status.is_healthy() {
                    test_errors.push(format!("Lambda execution had errors: {:?}", status));
                }
            }
            Err(e) => test_errors.push(format!("Failed to verify execution: {}", e)),
        }

        // Verify data in S3 Tables catalog via DuckDB
        if test_errors.is_empty() {
            let mut verifier = harness.duckdb_verifier();
            verifier.catalog_endpoint = deployment.catalog_endpoint.clone();
            if let super::smoke::StorageBackend::S3 { bucket, .. } =
                &mut verifier.storage_config.backend
            {
                *bucket = deployment.bucket.clone();
            }

            match verifier.verify("otel_smoke").await {
                Ok(report) => {
                    // Validate tables exist
                    if !report.tables.contains(&"otel_logs".to_string()) {
                        test_errors.push("Missing table: otel_logs".to_string());
                    }
                    if !report.tables.contains(&"otel_traces".to_string()) {
                        test_errors.push("Missing table: otel_traces".to_string());
                    }
                    if !report.tables.contains(&"otel_metrics_gauge".to_string()) {
                        test_errors.push("Missing table: otel_metrics_gauge".to_string());
                    }

                    // Validate row counts
                    if *report.row_counts.get("otel_logs").unwrap_or(&0) == 0 {
                        test_errors.push("No rows in otel_logs".to_string());
                    }
                    if *report.row_counts.get("otel_traces").unwrap_or(&0) == 0 {
                        test_errors.push("No rows in otel_traces".to_string());
                    }
                    if *report.row_counts.get("otel_metrics_gauge").unwrap_or(&0) == 0 {
                        test_errors.push("No rows in otel_metrics_gauge".to_string());
                    }
                }
                Err(e) => test_errors.push(format!("Failed to verify catalog: {}", e)),
            }
        }

        // ALWAYS cleanup, regardless of test results
        if let Err(e) = harness.cleanup().await {
            eprintln!("Warning: Cleanup failed: {}", e);
        }

        // Now check test results after cleanup
        if !test_errors.is_empty() {
            anyhow::bail!("Test failed:\n{}", test_errors.join("\n"));
        }

        Ok(())
    }

    #[tokio::test]
    async fn lambda_logs_only() -> Result<()> {
        let harness = LambdaHarness::from_env().await?;

        let deployment = harness.deploy().await?;

        // Collect test results without panicking
        let mut test_errors = Vec::new();

        // Send only logs
        let testdata = super::smoke::TestDataSet::load();
        let client = reqwest::Client::new();

        if let Err(e) = client
            .post(format!("{}/v1/logs", deployment.endpoint))
            .header("Content-Type", "application/x-protobuf")
            .body(testdata.logs_pb.to_vec())
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            test_errors.push(format!("Failed to send logs: {}", e));
        }

        // Verify execution
        match harness.verify_execution().await {
            Ok(status) => {
                if !status.is_healthy() {
                    test_errors.push(format!("Lambda execution had errors: {:?}", status));
                }
            }
            Err(e) => test_errors.push(format!("Failed to verify execution: {}", e)),
        }

        // ALWAYS cleanup, regardless of test results
        if let Err(e) = harness.cleanup().await {
            eprintln!("Warning: Cleanup failed: {}", e);
        }

        // Now check test results after cleanup
        if !test_errors.is_empty() {
            anyhow::bail!("Test failed:\n{}", test_errors.join("\n"));
        }

        Ok(())
    }
}

#[cfg(feature = "smoke-workers")]
mod workers_tests {
    use super::smoke::{workers::WorkersHarness, SmokeTestHarness};
    use anyhow::Result;

    #[tokio::test]
    async fn workers_r2_catalog_full_pipeline() -> Result<()> {
        let harness = WorkersHarness::from_env()?;

        // Deploy Workers script
        let deployment = harness.deploy().await?;

        // Send OTLP signals
        harness.send_signals(&deployment.endpoint).await?;

        // Verify no errors in Workers logs
        let status = harness.verify_execution().await?;
        assert!(
            status.is_healthy(),
            "Workers execution had errors: {:?}",
            status
        );

        // Verify data in R2 catalog via DuckDB (if catalog configured)
        if deployment.catalog_endpoint != "r2-catalog-not-configured" {
            let mut verifier = harness.duckdb_verifier();
            verifier.catalog_endpoint = deployment.catalog_endpoint.clone();
            if let super::smoke::StorageBackend::R2 { bucket, .. } =
                &mut verifier.storage_config.backend
            {
                *bucket = deployment.bucket.clone();
            }

            let report = verifier.verify("otel").await?;

            assert!(report.tables.contains(&"otel_logs".to_string()));
            assert!(*report.row_counts.get("otel_logs").unwrap_or(&0) > 0);
        }

        // Cleanup
        harness.cleanup().await?;

        Ok(())
    }

    #[tokio::test]
    async fn workers_metrics_only() -> Result<()> {
        let harness = WorkersHarness::from_env()?;
        let deployment = harness.deploy().await?;

        // Send only metrics
        let testdata = super::smoke::TestDataSet::load();
        let client = reqwest::Client::new();

        client
            .post(format!("{}/v1/metrics", deployment.endpoint))
            .header("Content-Type", "application/x-protobuf")
            .body(testdata.metrics_gauge_pb.to_vec())
            .send()
            .await?
            .error_for_status()?;

        // Verify execution
        let status = harness.verify_execution().await?;
        assert!(status.is_healthy());

        // Cleanup
        harness.cleanup().await?;

        Ok(())
    }
}
