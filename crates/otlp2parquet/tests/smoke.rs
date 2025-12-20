//! Unified smoke tests for all platforms
//!
//! This module provides a unified test framework that runs the same test logic
//! across all platforms (Server, Lambda, Workers) with both catalog modes
//! (Iceberg catalog and plain Parquet).
//!
//! ## Test Matrix
//! - Server + Iceberg (Nessie REST catalog)
//! - Server + Plain Parquet
//! - Lambda + S3 Tables
//! - Lambda + Plain Parquet
//! - Workers + R2 Data Catalog
//! - Workers + Plain Parquet
//!
//! ## Running Tests
//! ```bash
//! # All platforms (requires Docker + cloud credentials)
//! cargo test --test smoke --features smoke-server,smoke-lambda,smoke-workers
//!
//! # Server only (local Docker)
//! cargo test --test smoke --features smoke-server
//!
//! # Cloud platforms only
//! cargo test --test smoke --features smoke-lambda,smoke-workers
//! ```

#![cfg(any(
    feature = "smoke-server",
    feature = "smoke-lambda",
    feature = "smoke-workers"
))]

mod harness;

use anyhow::Result;
use harness::SmokeTestHarness;

/// Initialize tracing subscriber for tests (call once)
fn init_tracing() {
    use std::sync::Once;
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(tracing::Level::INFO.into()),
            )
            .with_test_writer()
            .try_init()
            .ok();
    });
}

/// Macro to generate smoke tests for all platforms
///
/// Usage:
/// ```
/// smoke_test!(test_name, test_impl_fn);
/// ```
///
/// This generates:
/// - server::test_name_with_catalog
/// - server::test_name_without_catalog
/// - lambda::test_name_with_catalog
/// - lambda::test_name_without_catalog (TODO)
/// - workers::test_name_with_catalog
/// - workers::test_name_without_catalog
macro_rules! smoke_test {
    ($test_name:ident, $test_fn:expr) => {
        paste::paste! {
            #[cfg(feature = "smoke-server")]
            mod [<server_ $test_name>] {
                use super::*;

                #[tokio::test]
                async fn with_catalog() -> Result<()> {
                    init_tracing();
                    let harness = harness::server::ServerHarness::new(harness::CatalogMode::Enabled).await?;
                    $test_fn(&harness).await
                }

                #[tokio::test]
                async fn without_catalog() -> Result<()> {
                    init_tracing();
                    let harness = harness::server::ServerHarness::new(harness::CatalogMode::None).await?;
                    $test_fn(&harness).await
                }
            }

            #[cfg(feature = "smoke-lambda")]
            mod [<lambda_ $test_name>] {
                use super::*;

                #[tokio::test]
                async fn with_catalog() -> Result<()> {
                    init_tracing();
                    let harness = harness::lambda::LambdaHarness::with_catalog_mode(harness::CatalogMode::Enabled).await?;
                    $test_fn(&harness).await
                }

                #[tokio::test]
                async fn without_catalog() -> Result<()> {
                    init_tracing();
                    let harness = harness::lambda::LambdaHarness::with_catalog_mode(harness::CatalogMode::None).await?;
                    $test_fn(&harness).await
                }
            }

            #[cfg(feature = "smoke-workers")]
            mod [<workers_ $test_name>] {
                use super::*;

                #[tokio::test]
                async fn with_catalog() -> Result<()> {
                    init_tracing();
                    // Note: Workers has its own CatalogMode enum, need to use it
                    let harness = harness::workers::WorkersHarness::from_env(
                        harness::workers::CatalogMode::Enabled
                    )?;
                    $test_fn(&harness).await
                }

                #[tokio::test]
                async fn without_catalog() -> Result<()> {
                    init_tracing();
                    let harness = harness::workers::WorkersHarness::from_env(
                        harness::workers::CatalogMode::None
                    )?;
                    $test_fn(&harness).await
                }
            }
        }
    };
}

// ============================================================================
// TEST IMPLEMENTATIONS
// ============================================================================

/// Test logs pipeline end-to-end
async fn test_logs_pipeline_impl(harness: &dyn SmokeTestHarness) -> Result<()> {
    tracing::info!("Starting logs pipeline test");

    // Deploy infrastructure
    let info = harness.deploy().await?;
    tracing::info!("Deployed to endpoint: {}", info.endpoint);

    // Send OTLP signals
    harness.send_signals(&info.endpoint).await?;
    tracing::info!("Signals sent successfully");

    // Verify execution (no errors in logs)
    let status = harness.verify_execution().await?;
    assert!(
        status.is_healthy(),
        "Found {} errors in execution logs: {:?}",
        status.error_count,
        status.sample_errors
    );
    tracing::info!("Execution healthy (no errors)");

    // Verify data with DuckDB
    let verifier = harness.duckdb_verifier(&info);
    let report = verifier.verify(&info.namespace).await?;

    tracing::info!("DuckDB verification report: {:?}", report);

    // Assert we got logs data
    let logs_count = report.row_counts.get("otel_logs").unwrap_or(&0);
    assert!(
        logs_count > &0,
        "Expected logs data, got {} rows",
        logs_count
    );
    tracing::info!("Found {} log rows", logs_count);

    // Cleanup
    harness.cleanup().await?;
    tracing::info!("Cleanup complete");

    Ok(())
}

/// Test metrics pipeline end-to-end
async fn test_metrics_pipeline_impl(harness: &dyn SmokeTestHarness) -> Result<()> {
    tracing::info!("Starting metrics pipeline test");

    let info = harness.deploy().await?;
    harness.send_signals(&info.endpoint).await?;

    let status = harness.verify_execution().await?;
    assert!(
        status.is_healthy(),
        "Found {} errors: {:?}",
        status.error_count,
        status.sample_errors
    );

    let verifier = harness.duckdb_verifier(&info);
    let report = verifier.verify(&info.namespace).await?;

    // Assert we got metrics data (gauge only from test signals)
    let metrics_count = report.row_counts.get("otel_metrics_gauge").unwrap_or(&0);
    assert!(
        metrics_count > &0,
        "Expected metrics data, got {} rows",
        metrics_count
    );
    tracing::info!("Found {} metric rows", metrics_count);

    harness.cleanup().await?;
    Ok(())
}

/// Test traces pipeline end-to-end
async fn test_traces_pipeline_impl(harness: &dyn SmokeTestHarness) -> Result<()> {
    tracing::info!("Starting traces pipeline test");

    let info = harness.deploy().await?;
    harness.send_signals(&info.endpoint).await?;

    let status = harness.verify_execution().await?;
    assert!(
        status.is_healthy(),
        "Found {} errors: {:?}",
        status.error_count,
        status.sample_errors
    );

    let verifier = harness.duckdb_verifier(&info);
    let report = verifier.verify(&info.namespace).await?;

    // Assert we got traces data
    let traces_count = report.row_counts.get("otel_traces").unwrap_or(&0);
    assert!(
        traces_count > &0,
        "Expected traces data, got {} rows",
        traces_count
    );
    tracing::info!("Found {} trace rows", traces_count);

    harness.cleanup().await?;
    Ok(())
}

/// Test logs pipeline with KV receipt verification (Workers-specific)
#[cfg(feature = "smoke-workers")]
async fn test_logs_pipeline_with_kv_verification(
    harness: &harness::workers::WorkersHarness,
) -> Result<()> {
    tracing::info!("Starting logs pipeline test with KV verification");

    // Deploy infrastructure
    let info = harness.deploy().await?;
    tracing::info!("Deployed to endpoint: {}", info.endpoint);

    // Start live tail if enabled (SMOKE_TEST_LIVE_TAIL=1)
    harness.start_live_tail().await?;

    // Run the actual test logic, capturing the result so we can always stop live tail
    let test_result: Result<()> = async {
        // Send OTLP signals
        harness.send_signals(&info.endpoint).await?;
        tracing::info!("Signals sent successfully");

        if harness.has_kv_tracking() {
            // Note: In catalog mode, send_signals() triggers catalog sync which deletes
            // KV receipts on success. So KV count may be 0 if sync already processed them.
            let kv_count = harness.count_kv_receipts().await?;
            tracing::info!(
                "Found {} KV receipts (0 expected if catalog sync succeeded)",
                kv_count
            );
        }

        // Ensure we actually wrote Parquet files for this run (isolated by prefix)
        let object_count = harness.count_r2_objects().await?;
        assert!(
            object_count > 0,
            "Expected at least one Parquet object for this test run"
        );
        tracing::info!("Found {} Parquet objects under test prefix", object_count);

        // Verify execution (no errors in logs)
        let status = harness.verify_execution().await?;
        assert!(
            status.is_healthy(),
            "Found {} errors in execution logs: {:?}",
            status.error_count,
            status.sample_errors
        );
        tracing::info!("Execution healthy (no errors)");

        // Verify data with DuckDB
        let verifier = harness.duckdb_verifier(&info);
        let report = verifier.verify(&info.namespace).await?;

        tracing::info!("DuckDB verification report: {:?}", report);

        // Assert we got logs data
        let logs_count = report.row_counts.get("otel_logs").unwrap_or(&0);
        assert!(
            logs_count > &0,
            "Expected logs data, got {} rows",
            logs_count
        );
        tracing::info!("Found {} log rows", logs_count);

        Ok(())
    }
    .await;

    // Always stop live tail and print captured logs, even on test failure
    harness.stop_live_tail().await?;

    // Propagate any test failure after live tail is stopped
    test_result?;

    // Cleanup
    harness.cleanup().await?;
    tracing::info!("Cleanup complete");

    Ok(())
}

// ============================================================================
// GENERATE TESTS FOR ALL PLATFORMS
// ============================================================================

smoke_test!(logs_pipeline, test_logs_pipeline_impl);
smoke_test!(metrics_pipeline, test_metrics_pipeline_impl);
smoke_test!(traces_pipeline, test_traces_pipeline_impl);

// ============================================================================
// WORKERS BATCHING TESTS
// ============================================================================

/// Test logs pipeline with Workers batching mode (Durable Objects)
#[cfg(feature = "smoke-workers")]
mod workers_logs_batching_pipeline {
    use super::*;

    #[tokio::test]
    async fn with_catalog() -> Result<()> {
        init_tracing();
        let harness = harness::workers::WorkersHarness::from_env_with_batching(
            harness::workers::CatalogMode::Enabled,
            harness::workers::BatchMode::Enabled,
        )?;
        test_logs_pipeline_with_kv_verification(&harness).await
    }

    #[tokio::test]
    async fn without_catalog() -> Result<()> {
        init_tracing();
        let harness = harness::workers::WorkersHarness::from_env_with_batching(
            harness::workers::CatalogMode::None,
            harness::workers::BatchMode::Enabled,
        )?;
        test_logs_pipeline_impl(&harness).await
    }
}
