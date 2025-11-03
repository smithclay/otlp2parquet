use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use arrow::array::RecordBatch;
use otlp2parquet_core::otlp::metrics;
use otlp2parquet_core::otlp::traces::{parse_otlp_trace_request, TraceArrowConverter};
use otlp2parquet_core::{parse_otlp_to_arrow, InputFormat};
use otlp2parquet_storage::parquet_writer::write_batches_with_hash;

#[tokio::main]
async fn main() -> Result<()> {
    let output_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("docs/query-demo/data"));

    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output dir {}", output_dir.display()))?;

    generate_logs(&output_dir)?;
    generate_metrics_gauge(&output_dir)?;
    generate_traces(&output_dir)?;

    println!("Demo parquet files written to {}", output_dir.display());
    Ok(())
}

fn generate_logs(output_dir: &Path) -> Result<()> {
    let payload = fs::read("testdata/logs.pb").context("reading logs.pb fixture")?;
    let (batch, _metadata) =
        parse_otlp_to_arrow(&payload, InputFormat::Protobuf).context("converting logs to arrow")?;

    let bytes = parquet_bytes(vec![batch])?;
    let target = output_dir.join("logs.parquet");
    fs::write(&target, &bytes).with_context(|| format!("writing {}", target.display()))?;
    Ok(())
}

fn generate_metrics_gauge(output_dir: &Path) -> Result<()> {
    let payload =
        fs::read("testdata/metrics_gauge.pb").context("reading metrics_gauge.pb fixture")?;
    let request =
        metrics::parse_otlp_request(&payload, InputFormat::Protobuf).context("parsing metrics")?;
    let converter = metrics::ArrowConverter::new();
    let (batches, _metadata) = converter.convert(request).context("converting metrics")?;

    let (metric_type, batch) = batches
        .into_iter()
        .find(|(metric_type, _)| metric_type == "gauge")
        .ok_or_else(|| anyhow!("gauge metrics batch not found"))?;

    let bytes = parquet_bytes(vec![batch])?;
    let target = output_dir.join(format!("metrics_{}.parquet", metric_type));
    fs::write(&target, &bytes).with_context(|| format!("writing {}", target.display()))?;
    Ok(())
}

fn generate_traces(output_dir: &Path) -> Result<()> {
    let payload = fs::read("testdata/traces.pb").context("reading traces.pb fixture")?;
    let request =
        parse_otlp_trace_request(&payload, InputFormat::Protobuf).context("parsing traces")?;
    let (batches, _metadata) =
        TraceArrowConverter::convert(&request).context("converting traces")?;

    // Trace converter currently returns a single batch
    let batch = batches
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no trace batches returned"))?;

    let bytes = parquet_bytes(vec![batch])?;
    let target = output_dir.join("traces.parquet");
    fs::write(&target, &bytes).with_context(|| format!("writing {}", target.display()))?;
    Ok(())
}

fn parquet_bytes(batches: Vec<RecordBatch>) -> Result<Vec<u8>> {
    let (bytes, _hash) = write_batches_with_hash(batches).context("writing parquet")?;
    Ok(bytes)
}
