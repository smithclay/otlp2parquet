//! Cloudflare Workers deployment config generator

use anyhow::{bail, Context, Result};
use clap::Args;
use dialoguer::{Confirm, Input, Select};
use std::fs;
use std::path::Path;

use super::names;

const TEMPLATE: &str = include_str!("../../templates/wrangler.toml");

#[derive(Args)]
pub struct CloudflareArgs {
    /// Worker name
    #[arg(long)]
    pub worker_name: Option<String>,

    /// R2 bucket name
    #[arg(long)]
    pub bucket: Option<String>,

    /// Cloudflare Account ID
    #[arg(long)]
    pub account_id: Option<String>,

    /// Catalog mode: "iceberg" or "none"
    #[arg(long)]
    pub catalog: Option<String>,

    /// Enable HTTP Basic Authentication
    #[arg(long)]
    pub basic_auth: Option<bool>,

    /// Enable Worker logging (observability)
    #[arg(long)]
    pub logging: Option<bool>,

    /// Enable batching with Durable Objects
    #[arg(long)]
    pub batching: Option<bool>,

    /// Release version to use (default: current CLI version)
    #[arg(long)]
    pub release: Option<String>,

    /// Overwrite existing file without asking
    #[arg(long)]
    pub force: bool,
}

pub fn run(args: CloudflareArgs) -> Result<()> {
    println!();
    println!("otlp2parquet deploy - Cloudflare Workers + R2");
    println!();

    // Collect values via wizard or flags
    let default_name = names::generate();
    let worker_name = match args.worker_name {
        Some(name) => name,
        None => Input::new()
            .with_prompt("Worker name")
            .default(default_name)
            .interact_text()?,
    };

    let bucket_name = match args.bucket {
        Some(bucket) => {
            validate_bucket_name(&bucket)
                .map_err(|e| anyhow::anyhow!("Invalid bucket name: {}", e))?;
            bucket
        }
        None => {
            let default_bucket = format!("{}-data", worker_name);
            Input::new()
                .with_prompt("R2 bucket name")
                .default(default_bucket)
                .validate_with(validate_bucket_name)
                .interact_text()?
        }
    };

    let account_id = match args.account_id {
        Some(id) => {
            validate_account_id(&id).map_err(|e| anyhow::anyhow!("Invalid account ID: {}", e))?;
            id
        }
        None => Input::new()
            .with_prompt("Cloudflare Account ID")
            .validate_with(validate_account_id)
            .interact_text()?,
    };

    let catalog_mode = match args.catalog {
        Some(cat) => {
            if cat != "iceberg" && cat != "none" {
                bail!(
                    "Invalid catalog mode '{}'. Must be 'iceberg' or 'none'.",
                    cat
                );
            }
            cat
        }
        None => {
            let options = &[
                "No  - Plain Parquet files (simpler)",
                "Yes - R2 Data Catalog (queryable tables)",
            ];
            let selection = Select::new()
                .with_prompt("Enable Iceberg catalog?")
                .items(options)
                .default(0)
                .interact()?;
            if selection == 0 {
                "none".to_string()
            } else {
                "iceberg".to_string()
            }
        }
    };

    let enable_basic_auth = match args.basic_auth {
        Some(enabled) => enabled,
        None => Confirm::new()
            .with_prompt("Enable HTTP Basic Authentication?")
            .default(false)
            .interact()?,
    };

    let enable_logging = match args.logging {
        Some(enabled) => enabled,
        None => Confirm::new()
            .with_prompt("Enable Worker logging?")
            .default(false)
            .interact()?,
    };

    let enable_batching = match args.batching {
        Some(enabled) => enabled,
        None => Confirm::new()
            .with_prompt("Enable batching with Durable Objects?")
            .default(true)
            .interact()?,
    };

    let version = args
        .release
        .unwrap_or_else(|| format!("v{}", env!("CARGO_PKG_VERSION")));

    // Check if file exists
    let output_path = Path::new("wrangler.toml");
    if output_path.exists() && !args.force {
        let overwrite = Confirm::new()
            .with_prompt("wrangler.toml already exists. Overwrite?")
            .default(false)
            .interact()?;
        if !overwrite {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Render template
    let use_iceberg = catalog_mode == "iceberg";
    let content = TEMPLATE
        .replace("{{WORKER_NAME}}", &worker_name)
        .replace("{{BUCKET_NAME}}", &bucket_name)
        .replace("{{ACCOUNT_ID}}", &account_id)
        .replace("{{CATALOG_MODE}}", &catalog_mode)
        .replace("{{VERSION}}", &version)
        .replace(
            "{{BASIC_AUTH_ENABLED}}",
            if enable_basic_auth { "true" } else { "false" },
        )
        .replace(
            "{{BATCH_ENABLED}}",
            if enable_batching { "true" } else { "false" },
        )
        .replace("{{BATCH_MAX_ROWS}}", "10000")
        .replace("{{BATCH_MAX_AGE_SECS}}", "60")
        // For Iceberg mode, add placeholder for KV namespace ID
        // Users will replace this after creating the namespace
        .replace("{{KV_NAMESPACE_ID}}", "REPLACE_WITH_KV_NAMESPACE_ID");

    // Process conditional blocks
    let content = process_conditional_blocks(
        &content,
        &[
            ("BUILD_FROM_RELEASE", true),  // CLI uses release builds
            ("BUILD_FROM_SOURCE", false),  // Not building from source
            ("INLINE_CREDENTIALS", false), // CLI uses wrangler secrets
            ("ICEBERG", use_iceberg),
            ("BASICAUTH", enable_basic_auth),
            ("LOGGING", enable_logging),
            ("BATCHING", enable_batching),
        ],
    );

    // Write file
    fs::write(output_path, &content).context("Failed to write wrangler.toml")?;

    println!();
    println!("Created wrangler.toml");
    println!();
    println!("Next steps:");
    println!("  1. Create R2 bucket (if needed):");
    println!("     wrangler r2 bucket create {}", bucket_name);
    if use_iceberg {
        println!();
        println!("     IMPORTANT: Enable R2 Data Catalog on the bucket:");
        println!(
            "     - Go to Cloudflare Dashboard > R2 > {} > Settings",
            bucket_name
        );
        println!("     - Enable 'Data Catalog' under 'Iceberg catalog'");
        println!("     - This is required for Iceberg table registration to work");
    }
    println!();
    println!("  2. Set secrets (R2 S3-Compatible API credentials):");
    println!("     wrangler secret put AWS_ACCESS_KEY_ID");
    println!("     wrangler secret put AWS_SECRET_ACCESS_KEY");
    println!();
    println!("     Get credentials: https://developers.cloudflare.com/r2/api/tokens/#get-s3-api-credentials-from-an-api-token");
    if use_iceberg {
        println!("     wrangler secret put CLOUDFLARE_API_TOKEN");
    }
    if enable_basic_auth {
        println!("     These will set username/password to POST data to your worker");
        println!("     wrangler secret put OTLP2PARQUET_BASIC_AUTH_USERNAME");
        println!("     wrangler secret put OTLP2PARQUET_BASIC_AUTH_PASSWORD");
    }
    if use_iceberg {
        println!("  3. Create KV namespace for pending files:");
        println!("     wrangler kv namespace create PENDING_FILES");
        println!("     Then update wrangler.toml with the namespace ID");
        println!();
    }
    println!("  {}. Deploy:", if use_iceberg { "4" } else { "3" });
    println!("     wrangler deploy");
    println!();

    Ok(())
}

#[allow(clippy::ptr_arg)]
fn validate_bucket_name(input: &String) -> Result<(), String> {
    if input.is_empty() {
        return Err("Bucket name cannot be empty".to_string());
    }
    if input.len() < 3 || input.len() > 63 {
        return Err("Bucket name must be 3-63 characters".to_string());
    }
    if !input
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(
            "Bucket name must contain only lowercase letters, numbers, and hyphens".to_string(),
        );
    }
    if input.starts_with('-') || input.ends_with('-') {
        return Err("Bucket name cannot start or end with a hyphen".to_string());
    }
    Ok(())
}

#[allow(clippy::ptr_arg)]
fn validate_account_id(input: &String) -> Result<(), String> {
    if input.is_empty() {
        return Err("Account ID cannot be empty".to_string());
    }
    if !input.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("Account ID must be a hex string".to_string());
    }
    if input.len() != 32 {
        return Err("Account ID must be 32 characters".to_string());
    }
    Ok(())
}

/// Process conditional blocks in template content.
/// Blocks are marked with {{#NAME}} and {{/NAME}}.
/// If enabled is true, the block content is included (markers removed).
/// If enabled is false, the entire block including content is removed.
pub fn process_conditional_blocks(content: &str, conditions: &[(&str, bool)]) -> String {
    let lines: Vec<&str> = content.lines().collect();
    let mut filtered = Vec::new();
    let mut skip_stack: Vec<bool> = Vec::new();

    for line in lines {
        let mut is_marker = false;

        for &(name, enabled) in conditions {
            let start_marker = format!("{{{{#{}}}}}", name);
            let end_marker = format!("{{{{/{}}}}}", name);

            if line.contains(&start_marker) {
                skip_stack.push(!enabled);
                is_marker = true;
                break;
            }
            if line.contains(&end_marker) {
                skip_stack.pop();
                is_marker = true;
                break;
            }
        }

        if is_marker {
            continue;
        }

        // Include line only if not inside any skipped block
        if !skip_stack.iter().any(|&skip| skip) {
            filtered.push(line);
        }
    }

    filtered.join("\n")
}
