// Universal entry point for all platforms
//
// Detects the runtime platform and delegates to the appropriate handler

use quill_runtime::Platform;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let platform = Platform::detect();

    println!("Detected platform: {:?}", platform);

    match platform {
        #[cfg(feature = "cloudflare")]
        Platform::CloudflareWorkers => {
            // Cloudflare Workers entry is handled by the worker macro
            // This path should not be reached in Workers environment
            eprintln!("Cloudflare Workers should use the worker entry point");
            std::process::exit(1);
        }

        #[cfg(feature = "lambda")]
        Platform::Lambda => {
            quill_runtime::lambda::run().await?;
        }

        #[cfg(feature = "standalone")]
        Platform::Standalone => {
            quill_runtime::standalone::run().await?;
        }

        #[allow(unreachable_patterns)]
        _ => {
            eprintln!("Platform not supported in this build");
            std::process::exit(1);
        }
    }

    Ok(())
}
