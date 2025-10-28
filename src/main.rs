// Entry points for different platforms
//
// Philosophy (Fred Brooks): Each platform uses its native idioms
// - Lambda: lambda_runtime provides tokio, we just use it
// - Standalone: Simple blocking I/O, no async needed
// - Cloudflare: worker::event macro, not main()

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
fn main() -> anyhow::Result<()> {
    println!("AWS Lambda - runtime provided by lambda_runtime crate");
    // The lambda_runtime::run() function will set up tokio and run our handler
    // For now, just a placeholder until we implement the actual handler
    println!("Lambda handler not yet implemented");
    println!("Use: lambda_runtime::run(service_fn(handler)).await");
    Ok(())
}

// =============================================================================
// STANDALONE ENTRY POINT
// =============================================================================
// Simple blocking I/O - no tokio needed
#[cfg(all(feature = "standalone", not(feature = "lambda")))]
fn main() -> anyhow::Result<()> {
    println!("Standalone mode - blocking I/O");
    otlp2parquet_runtime::standalone::run()
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
fn main() {
    panic!("Cloudflare Workers should use #[event(fetch)], not main()");
}

// =============================================================================
// FALLBACK (no features enabled)
// =============================================================================
#[cfg(not(any(feature = "lambda", feature = "standalone", feature = "cloudflare")))]
fn main() {
    eprintln!("Error: No platform feature enabled!");
    eprintln!("Build with: --features lambda|standalone|cloudflare");
    std::process::exit(1);
}
