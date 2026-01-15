//! Unified smoke tests for server mode
//!
//! This module provides a unified test framework that runs the same test logic
//! against the server implementation writing Parquet files.
//!
//! ## Test Matrix
//! - Server + MinIO S3
//!
//! ## Running Tests
//! ```bash
//! # Server only (local Docker)
//! cargo test --test smoke --features smoke-server
//! ```

#![cfg(feature = "smoke-server")]

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

/// Macro to generate smoke tests for server
///
/// Usage:
/// ```
/// smoke_test!(test_name, test_impl_fn);
/// ```
///
/// This generates:
/// - server::test_name
macro_rules! smoke_test {
    ($test_name:ident, $test_fn:expr) => {
        paste::paste! {
            #[cfg(feature = "smoke-server")]
            mod [<server_ $test_name>] {
                use super::*;

                #[tokio::test]
                async fn test() -> Result<()> {
                    init_tracing();
                    let harness = harness::server::ServerHarness::new().await?;
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
    let report = verifier.verify(&info.prefix).await?;

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
    let report = verifier.verify(&info.prefix).await?;

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
    let report = verifier.verify(&info.prefix).await?;

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

// ============================================================================
// GENERATE TESTS FOR ALL PLATFORMS
// ============================================================================

smoke_test!(logs_pipeline, test_logs_pipeline_impl);
smoke_test!(metrics_pipeline, test_metrics_pipeline_impl);
smoke_test!(traces_pipeline, test_traces_pipeline_impl);
