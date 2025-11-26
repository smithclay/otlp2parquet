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
        None => Input::new()
            .with_prompt("R2 bucket name")
            .validate_with(validate_bucket_name)
            .interact_text()?,
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
    let mut content = TEMPLATE
        .replace("{{WORKER_NAME}}", &worker_name)
        .replace("{{BUCKET_NAME}}", &bucket_name)
        .replace("{{ACCOUNT_ID}}", &account_id)
        .replace("{{CATALOG_MODE}}", &catalog_mode)
        .replace("{{VERSION}}", &version);

    // Handle conditional iceberg section
    if use_iceberg {
        content = content
            .replace("{{#ICEBERG}}", "")
            .replace("{{/ICEBERG}}", "");
    } else {
        // Remove iceberg-only lines
        let lines: Vec<&str> = content.lines().collect();
        let mut filtered = Vec::new();
        let mut in_iceberg_block = false;
        for line in lines {
            if line.contains("{{#ICEBERG}}") {
                in_iceberg_block = true;
                continue;
            }
            if line.contains("{{/ICEBERG}}") {
                in_iceberg_block = false;
                continue;
            }
            if !in_iceberg_block {
                filtered.push(line);
            }
        }
        content = filtered.join("\n");
    }

    // Write file
    fs::write(output_path, &content).context("Failed to write wrangler.toml")?;

    println!();
    println!("Created wrangler.toml");
    println!();
    println!("Next steps:");
    println!("  1. Create R2 bucket (if needed):");
    println!("     wrangler r2 bucket create {}", bucket_name);
    println!();
    println!("  2. Set secrets:");
    println!("     wrangler secret put AWS_ACCESS_KEY_ID");
    println!("     wrangler secret put AWS_SECRET_ACCESS_KEY");
    if use_iceberg {
        println!("     wrangler secret put CLOUDFLARE_API_TOKEN");
    }
    println!();
    println!("  3. Deploy:");
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
