//! AWS CloudFormation deployment config generator

use clap::Args;

#[derive(Args)]
pub struct AwsArgs {
    /// S3 URI of Lambda binary (e.g., s3://my-bucket/lambda.zip)
    #[arg(long)]
    pub lambda_s3_uri: Option<String>,

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

pub fn run(_args: AwsArgs) -> anyhow::Result<()> {
    todo!("AWS wizard not yet implemented")
}
