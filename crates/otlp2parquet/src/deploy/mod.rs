//! Deploy command - generates platform-specific deployment configs

mod names;

pub mod aws;
pub mod cloudflare;

use clap::Subcommand;

#[derive(Subcommand)]
pub enum DeployCommand {
    /// Generate wrangler.toml for Cloudflare Workers + R2
    Cloudflare(cloudflare::CloudflareArgs),
    /// Generate template.yaml for AWS Lambda + S3/S3 Tables
    Aws(aws::AwsArgs),
}

impl DeployCommand {
    pub fn run(self) -> anyhow::Result<()> {
        match self {
            DeployCommand::Cloudflare(args) => cloudflare::run(args),
            DeployCommand::Aws(args) => aws::run(args),
        }
    }
}
