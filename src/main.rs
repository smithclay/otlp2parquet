// Universal entry point for all platforms
//
// Detects the runtime platform and delegates to the appropriate handler

// Cloudflare Workers use a different entry point via the worker::event macro
// So we only compile the tokio-based main for non-cloudflare builds
#[cfg(not(feature = "cloudflare"))]
use otlp2parquet_runtime::Platform;

#[cfg(not(feature = "cloudflare"))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let platform = Platform::detect();

    println!("Detected platform: {:?}", platform);

    match platform {
        #[cfg(feature = "lambda")]
        Platform::Lambda => {
            otlp2parquet_runtime::lambda::run().await?;
        }

        #[cfg(feature = "standalone")]
        Platform::Standalone => {
            otlp2parquet_runtime::standalone::run().await?;
        }

        #[allow(unreachable_patterns)]
        _ => {
            eprintln!("Platform not supported in this build");
            std::process::exit(1);
        }
    }

    Ok(())
}

// For Cloudflare Workers, we don't use a main function
// The worker crate provides the entry point via #[event(fetch)] macro
#[cfg(feature = "cloudflare")]
fn main() {
    // Cloudflare Workers entry point is defined in the worker module
    // This is just a placeholder that won't be called
    panic!("Cloudflare Workers binary should be built as a cdylib, not a binary");
}
