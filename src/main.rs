// Entry points for different platforms
//
// Philosophy: Leverage mature abstractions (OpenDAL) consistently across platforms
// - Lambda: tokio + OpenDAL S3
// - Standalone: tokio + OpenDAL filesystem (async for API consistency)
// - Cloudflare: worker runtime + OpenDAL S3 → R2

// =============================================================================
// COMPILE-TIME PLATFORM CHECKS
// =============================================================================
// Cloudflare Workers requires WASM target
#[cfg(all(feature = "cloudflare", not(target_arch = "wasm32")))]
compile_error!("Cloudflare Workers feature requires wasm32 target. Build with: cargo build --target wasm32-unknown-unknown --features cloudflare");

// Lambda and Standalone should NOT be built for WASM
#[cfg(all(feature = "lambda", target_arch = "wasm32"))]
compile_error!("Lambda feature cannot be built for WASM target. Use native target: cargo build --features lambda");

#[cfg(all(feature = "standalone", target_arch = "wasm32"))]
compile_error!("Standalone feature cannot be built for WASM target. Use native target: cargo build --features standalone");

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
// STANDALONE ENTRY POINT
// =============================================================================
// Async I/O with tokio + OpenDAL filesystem
#[cfg(all(feature = "standalone", not(feature = "lambda")))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Standalone mode - async I/O with OpenDAL filesystem");
    otlp2parquet_runtime::standalone::run().await
}

// =============================================================================
// CLOUDFLARE WORKERS ENTRY POINT
// =============================================================================
// Entry point is #[event(fetch)] in worker code, not main()
#[cfg(all(
    feature = "cloudflare",
    not(feature = "lambda"),
    not(feature = "standalone")
))]
use worker::*;

#[cfg(all(
    feature = "cloudflare",
    not(feature = "lambda"),
    not(feature = "standalone")
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
    not(feature = "standalone"),
    target_arch = "wasm32"
))]
fn main() {}

// =============================================================================
// FALLBACK (no features enabled)
// =============================================================================
#[cfg(not(any(feature = "lambda", feature = "standalone", feature = "cloudflare")))]
fn main() {
    eprintln!("Error: No platform feature enabled!");
    eprintln!("Build with: --features lambda|standalone|cloudflare");
    std::process::exit(1);
}
