#!/usr/bin/env bash
set -euo pipefail

# deploy.sh - Deploy otlp2parquet Lambda with S3 Tables
#
# Usage:
#   ./deploy.sh [OPTIONS]
#
# Options:
#   --arch <arm64|amd64>      Lambda architecture (default: arm64)
#   --region <region>         AWS region (default: $AWS_REGION or us-west-2)
#   --stack-name <name>       CloudFormation stack name (default: otlp2parquet)
#   --version <version>       GitHub release version (default: latest)
#   --local-binary <path>     Use local Lambda zip instead of downloading from releases
#   --delete                  Delete the stack and cleanup resources
#   --help                    Show this help message

# For local binary, to rebuild zip: cd crates/otlp2parquet-lambda && cargo lambda build --release --arm64

# Default configuration
ARCH="arm64"
REGION="${AWS_REGION:-us-west-2}"
STACK_NAME="otlp2parquet"
VERSION="latest"
LOCAL_BINARY=""
DELETE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print functions
info() { echo -e "${GREEN}==>${NC} $*"; }
warn() { echo -e "${YELLOW}WARNING:${NC} $*" >&2; }
error() { echo -e "${RED}ERROR:${NC} $*" >&2; exit 1; }

# Show help
show_help() {
  grep '^#' "$0" | grep -v '#!/usr/bin/env' | sed 's/^# \?//'
  exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --arch)
      ARCH="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --stack-name)
      STACK_NAME="$2"
      shift 2
      ;;
    --version)
      VERSION="$2"
      shift 2
      ;;
    --local-binary)
      LOCAL_BINARY="$2"
      shift 2
      ;;
    --delete)
      DELETE=true
      shift
      ;;
    --help)
      show_help
      ;;
    *)
      error "Unknown option: $1. Use --help for usage."
      ;;
  esac
done

# Validate architecture
if [[ "$ARCH" != "arm64" && "$ARCH" != "amd64" ]]; then
  error "Invalid architecture: $ARCH. Must be 'arm64' or 'amd64'."
fi

# Convert amd64 to x86_64 for CloudFormation parameter
CFN_ARCH="$ARCH"
if [[ "$ARCH" == "amd64" ]]; then
  CFN_ARCH="x86_64"
fi

# Check required tools
for cmd in aws jq; do
  if ! command -v "$cmd" &> /dev/null; then
    error "$cmd is required but not installed. Please install it and try again."
  fi
done

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null) || \
  error "Failed to get AWS account ID. Is AWS CLI configured?"

info "AWS Account: $ACCOUNT_ID"
info "Region: $REGION"
info "Stack Name: $STACK_NAME"

# Deployment bucket name (unique per account)
DEPLOYMENT_BUCKET="otlp2parquet-deployment-${ACCOUNT_ID}"

# Handle stack deletion
if [[ "$DELETE" == "true" ]]; then
  info "Deleting CloudFormation stack: $STACK_NAME"

  # Get table bucket name before deletion
  TABLE_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`TableBucketName`].OutputValue' \
    --output text 2>/dev/null || echo "")

  if [[ -n "$TABLE_BUCKET" ]]; then
    warn "S3 Table Bucket '$TABLE_BUCKET' will be deleted by CloudFormation."
    warn "All Iceberg tables and data will be permanently removed."
    read -p "Continue with deletion? (yes/no): " -r
    if [[ ! "$REPLY" =~ ^[Yy][Ee][Ss]$ ]]; then
      info "Deletion cancelled."
      exit 0
    fi
  fi

  aws cloudformation delete-stack \
    --stack-name "$STACK_NAME" \
    --region "$REGION"

  info "Waiting for stack deletion to complete..."
  aws cloudformation wait stack-delete-complete \
    --stack-name "$STACK_NAME" \
    --region "$REGION" || warn "Stack deletion may have failed. Check AWS Console."

  info "Stack deleted successfully."

  # Optionally cleanup deployment bucket
  read -p "Delete deployment bucket '$DEPLOYMENT_BUCKET'? (yes/no): " -r
  if [[ "$REPLY" =~ ^[Yy][Ee][Ss]$ ]]; then
    info "Emptying and deleting deployment bucket..."
    aws s3 rm "s3://${DEPLOYMENT_BUCKET}" --recursive --region "$REGION" 2>/dev/null || true
    aws s3 rb "s3://${DEPLOYMENT_BUCKET}" --region "$REGION" 2>/dev/null || true
    info "Deployment bucket deleted."
  fi

  exit 0
fi

# Get or download Lambda binary
if [[ -n "$LOCAL_BINARY" ]]; then
  # Use local binary
  info "Using local Lambda binary: $LOCAL_BINARY"

  if [[ ! -f "$LOCAL_BINARY" ]]; then
    error "Local binary not found: $LOCAL_BINARY"
  fi

  LAMBDA_ZIP="$LOCAL_BINARY"
  ASSET_NAME="$(basename "$LOCAL_BINARY")"

  # Verify zip file
  if ! unzip -t "$LAMBDA_ZIP" &>/dev/null; then
    error "Local binary is not a valid zip archive: $LAMBDA_ZIP"
  fi

  info "Local binary verified: $ASSET_NAME"
else
  # Download from GitHub releases
  info "Downloading Lambda binary for $ARCH..."

  ASSET_NAME="otlp2parquet-lambda-linux-${ARCH}.zip"
  LAMBDA_ZIP="${SCRIPT_DIR}/${ASSET_NAME}"

  if [[ "$VERSION" == "latest" ]]; then
    DOWNLOAD_URL="https://github.com/smithclay/otlp2parquet/releases/latest/download/${ASSET_NAME}"
  else
    DOWNLOAD_URL="https://github.com/smithclay/otlp2parquet/releases/download/${VERSION}/${ASSET_NAME}"
  fi

  info "Downloading from: $DOWNLOAD_URL"

  if command -v curl &> /dev/null; then
    curl -fsSL -o "$LAMBDA_ZIP" "$DOWNLOAD_URL" || \
      error "Failed to download Lambda binary. Check that version '$VERSION' exists."
  elif command -v wget &> /dev/null; then
    wget -q -O "$LAMBDA_ZIP" "$DOWNLOAD_URL" || \
      error "Failed to download Lambda binary. Check that version '$VERSION' exists."
  else
    error "Neither curl nor wget found. Please install one and try again."
  fi

  info "Downloaded Lambda binary: $LAMBDA_ZIP"

  # Verify zip file
  if ! unzip -t "$LAMBDA_ZIP" &>/dev/null; then
    error "Downloaded file is not a valid zip archive."
  fi
fi

# Create deployment S3 bucket if it doesn't exist
info "Setting up deployment bucket: $DEPLOYMENT_BUCKET"

if ! aws s3 ls "s3://${DEPLOYMENT_BUCKET}" --region "$REGION" &>/dev/null; then
  info "Creating deployment bucket..."
  if [[ "$REGION" == "us-east-1" ]]; then
    aws s3api create-bucket \
      --bucket "$DEPLOYMENT_BUCKET" \
      --region "$REGION"
  else
    aws s3api create-bucket \
      --bucket "$DEPLOYMENT_BUCKET" \
      --region "$REGION" \
      --create-bucket-configuration "LocationConstraint=$REGION"
  fi
  info "Deployment bucket created."
else
  info "Deployment bucket already exists."
fi

# Upload Lambda zip to S3 with timestamp to force CloudFormation updates
TIMESTAMP=$(date +%s)
S3_KEY="lambda/${TIMESTAMP}/${ASSET_NAME}"
info "Uploading Lambda binary to s3://${DEPLOYMENT_BUCKET}/${S3_KEY}..."

aws s3 cp "$LAMBDA_ZIP" "s3://${DEPLOYMENT_BUCKET}/${S3_KEY}" --region "$REGION"

info "Upload complete."

# Deploy CloudFormation stack
info "Deploying CloudFormation stack..."

aws cloudformation deploy \
  --template-file "${SCRIPT_DIR}/template.yaml" \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    LambdaCodeBucket="$DEPLOYMENT_BUCKET" \
    LambdaCodeKey="$S3_KEY" \
    LambdaArchitecture="$CFN_ARCH" \
  --no-fail-on-empty-changeset

info "CloudFormation deployment complete."

# Get stack outputs
info "Retrieving stack outputs..."

OUTPUTS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs' \
  --output json)

echo "$OUTPUTS" > "${SCRIPT_DIR}/stack-outputs.json"

# Display outputs
echo ""
echo -e "${GREEN}Deployment Summary:${NC}"
echo "===================="
echo ""

echo "$OUTPUTS" | jq -r '.[] | "  \(.OutputKey): \(.OutputValue)"'

echo ""
info "Deployment complete! Stack outputs saved to: stack-outputs.json"
info "Next steps:"
echo "  1. Run ./test.sh to test the deployment"
echo "  2. Check CloudWatch logs: aws logs tail /aws/lambda/${STACK_NAME}-ingest --follow"
echo ""
