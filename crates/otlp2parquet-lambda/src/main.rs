// AWS Lambda binary entry point
//
// This binary is built independently from the root workspace.
// Build with: cargo build -p otlp2parquet-lambda
//
// The lambda_runtime crate provides the tokio runtime, so we use #[tokio::main]

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    otlp2parquet_lambda::run().await
}
