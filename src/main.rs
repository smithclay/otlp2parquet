// Entry points for different platforms
//
// Philosophy: Leverage mature abstractions (OpenDAL) consistently across platforms
// - Server (default): Full-featured HTTP server with multi-backend storage
// - Lambda: Event-driven handler with S3 storage (constrained runtime)
// - Cloudflare: WASM handler with R2 storage (most constrained)

// =============================================================================
// COMPILE-TIME PLATFORM CHECKS
// =============================================================================
// Cloudflare Workers requires WASM target
#[cfg(all(feature = "cloudflare", not(target_arch = "wasm32")))]
compile_error!("Cloudflare Workers feature requires wasm32 target. Build with: cargo build --target wasm32-unknown-unknown --features cloudflare");

// Lambda and Server should NOT be built for WASM
#[cfg(all(feature = "lambda", target_arch = "wasm32"))]
compile_error!("Lambda feature cannot be built for WASM target. Use native target: cargo build --features lambda");

#[cfg(all(feature = "server", target_arch = "wasm32"))]
compile_error!("Server feature cannot be built for WASM target. Use native target: cargo build --features server");

// =============================================================================
// LAMBDA ENTRY POINT
// =============================================================================
// Lambda runtime provides tokio - lambda_runtime::run() sets it up for us
// We don't use #[tokio::main] - lambda_runtime handles the runtime
#[cfg(feature = "lambda")]
#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    println!("AWS Lambda - runtime provided by lambda_runtime crate");
    otlp2parquet_runtime::lambda::run().await
}

// =============================================================================
// SERVER ENTRY POINT (DEFAULT MODE)
// =============================================================================
// Full-featured HTTP server with multi-backend storage
#[cfg(all(feature = "server", not(feature = "lambda")))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Server mode - full-featured HTTP server with multi-backend storage");
    otlp2parquet_runtime::server::run().await
}

// =============================================================================
// CLOUDFLARE WORKERS ENTRY POINT
// =============================================================================
// Entry point is #[event(fetch)] in worker code, not main()
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
    otlp2parquet_runtime::cloudflare::handle_otlp_request(req, env, ctx).await
}

// Provide an empty main stub so cargo can build the binary target for wasm.
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
