//! AWS CloudFormation deployment config generator

use anyhow::{bail, Context, Result};
use clap::Args;
use dialoguer::{Confirm, Input, Select};
use std::fs;
use std::path::Path;

use super::names;

const TEMPLATE: &str = include_str!("../../templates/cloudformation.yaml");

#[derive(Args)]
pub struct AwsArgs {
    /// GitHub release version (e.g., v0.0.2 or "latest")
    #[arg(long, default_value = "latest")]
    pub version: String,

    /// CloudFormation stack name
    #[arg(long)]
    pub stack_name: Option<String>,

    /// S3 bucket name for data storage
    #[arg(long)]
    pub bucket: Option<String>,

    /// Catalog mode: "iceberg" or "none"
    #[arg(long)]
    pub catalog: Option<String>,

    /// CloudWatch log retention in days
    #[arg(long, default_value = "7")]
    pub retention: u16,

    /// Overwrite existing file without asking
    #[arg(long)]
    pub force: bool,
}

pub fn run(args: AwsArgs) -> Result<()> {
    println!();
    println!("otlp2parquet deploy - AWS Lambda + S3");
    println!();

    let version = args.version;

    let default_name = names::generate();
    let stack_name = match args.stack_name {
        Some(name) => name,
        None => Input::new()
            .with_prompt("Stack name")
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
            .with_prompt("S3 bucket name for data")
            .validate_with(validate_bucket_name)
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
            let options = &["No  - Plain Parquet to S3", "Yes - S3 Tables (Iceberg)"];
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

    let retention = args.retention;

    // Check if file exists
    let output_path = Path::new("template.yaml");
    if output_path.exists() && !args.force {
        let overwrite = Confirm::new()
            .with_prompt("template.yaml already exists. Overwrite?")
            .default(false)
            .interact()?;
        if !overwrite {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Render template
    let content = TEMPLATE
        .replace("{{STACK_NAME}}", &stack_name)
        .replace("{{BUCKET_NAME}}", &bucket_name)
        .replace("{{CATALOG_MODE}}", &catalog_mode)
        .replace("{{LOG_RETENTION}}", &retention.to_string())
        .replace("{{LAMBDA_VERSION}}", &version);

    // Write file
    fs::write(output_path, &content).context("Failed to write template.yaml")?;

    println!();
    println!("Created template.yaml (version: {})", version);
    println!();
    println!("Next steps:");
    println!("  1. Deploy:");
    println!("     aws cloudformation deploy \\");
    println!("       --template-file template.yaml \\");
    println!("       --stack-name {} \\", stack_name);
    println!("       --capabilities CAPABILITY_IAM");
    println!();
    println!("The Lambda binary will be automatically fetched from GitHub releases.");
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
