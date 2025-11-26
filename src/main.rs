// Server entry point
//
// Platform-specific runtimes are in separate crates:
// - otlp2parquet-cli: Full-featured HTTP server with multi-backend storage (this binary)
// - otlp2parquet-lambda: Event-driven handler with S3 storage (build: cargo build -p otlp2parquet-lambda)
// - otlp2parquet-cloudflare: WASM handler with R2 storage (build: worker-build in crate dir)

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    otlp2parquet_cli::run().await
}
