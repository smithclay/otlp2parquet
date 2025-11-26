//! Cloudflare Workers deployment config generator

use clap::Args;

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

pub fn run(_args: CloudflareArgs) -> anyhow::Result<()> {
    todo!("Cloudflare wizard not yet implemented")
}
