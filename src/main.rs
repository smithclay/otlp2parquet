// Entry points for different platforms
//
// Platform-specific runtimes are now in separate crates:
// - otlp2parquet-server: Full-featured HTTP server with multi-backend storage
// - otlp2parquet-lambda: Event-driven handler with S3 storage
// - otlp2parquet-cloudflare: WASM handler with R2 storage
//
// This main.rs just routes to the appropriate platform crate.

// =============================================================================
// COMPILE-TIME PLATFORM CHECKS
// =============================================================================
#[cfg(all(feature = "cloudflare", not(target_arch = "wasm32")))]
compile_error!("Cloudflare Workers requires wasm32 target. Build with: cargo build --target wasm32-unknown-unknown --features cloudflare");

#[cfg(all(feature = "lambda", target_arch = "wasm32"))]
compile_error!("Lambda cannot be built for WASM. Use: cargo build --features lambda");

#[cfg(all(feature = "server", target_arch = "wasm32"))]
compile_error!("Server cannot be built for WASM. Use: cargo build --features server");

// =============================================================================
// LAMBDA ENTRY POINT
// =============================================================================
#[cfg(feature = "lambda")]
#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    otlp2parquet_lambda::run().await
}

// =============================================================================
// SERVER ENTRY POINT (DEFAULT MODE)
// =============================================================================
#[cfg(all(feature = "server", not(feature = "lambda")))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    otlp2parquet_server::run().await
}

// =============================================================================
// CLOUDFLARE WORKERS ENTRY POINT
// =============================================================================
#[cfg(all(
    feature = "cloudflare",
    not(feature = "lambda"),
    not(feature = "server")
))]
use worker::*;

#[cfg(all(
    feature = "cloudflare",
    not(feature = "lambda"),
    not(feature = "server")
))]
#[event(fetch)]
async fn worker_fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {
    console_log!("Cloudflare Workers OTLP endpoint started");
    otlp2parquet_cloudflare::handle_otlp_request(req, env, ctx).await
}

#[cfg(all(
    feature = "cloudflare",
    not(feature = "lambda"),
    not(feature = "server"),
    target_arch = "wasm32"
))]
fn main() {}

// =============================================================================
// FALLBACK (no features enabled)
// =============================================================================
#[cfg(not(any(feature = "lambda", feature = "server", feature = "cloudflare")))]
fn main() {
    eprintln!("Error: No platform feature enabled!");
    eprintln!("Build with: --features lambda|server|cloudflare");
    std::process::exit(1);
}
