# AWS Lambda + S3 Tables Example Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create production-ready AWS Lambda deployment example with S3 Tables integration, including CloudFormation template, automation scripts, and comprehensive documentation.

**Architecture:** Event-driven OTLP ingestion using AWS Lambda writing to S3 Table Buckets (Apache Iceberg). Lambda downloads from GitHub releases, deployed via CloudFormation with helper scripts for deployment and testing.

**Tech Stack:** AWS CloudFormation, AWS Lambda (Rust binary), S3 Tables (Iceberg REST catalog), Bash automation scripts, GitHub Actions for release packaging.

---

## Task 1: Update Release Workflow to Package Lambda as Zip

**Files:**
- Modify: `.github/workflows/build-artifacts.yml:56-79`

**Step 1: Add Lambda zip packaging step**

After the existing "Package binary" step (line 56-68), add a new step to create zip files for Lambda deployment.

In `.github/workflows/build-artifacts.yml`, add this step after line 68:

```yaml
      - name: Package Lambda as zip
        run: |
          set -euo pipefail
          LAMBDA_BINARY="target/${{ matrix.target }}/release/bootstrap"
          TMP_DIR="$(mktemp -d)"
          cp "$LAMBDA_BINARY" "$TMP_DIR/bootstrap"
          cd "$TMP_DIR"
          zip -j "$GITHUB_WORKSPACE/target/${{ matrix.target }}/release/${{ matrix.asset_name }}.zip" bootstrap
          cd "$GITHUB_WORKSPACE"
          rm -rf "$TMP_DIR"
          echo "Created ${{ matrix.asset_name }}.zip"

      - name: Upload Lambda zip artifact
        uses: actions/upload-artifact@v5
        with:
          name: ${{ matrix.asset_name }}-zip
          path: target/${{ matrix.target }}/release/${{ matrix.asset_name }}.zip
```

**Step 2: Update release job to include zip files**

In the `publish` job (around line 210), update the `files:` section to include zip files:

Change:
```yaml
          files: |
            artifacts/**/*.tar.gz
            artifacts/**/*.zip
            checksums.txt
```

To:
```yaml
          files: |
            artifacts/**/*.tar.gz
            artifacts/**/*.zip
            checksums.txt
```

(It already includes `*.zip`, but verify it's there)

**Step 3: Test locally (optional verification)**

If you want to test the workflow change locally:

```bash
# Build lambda binary locally
cargo build --release -p otlp2parquet-lambda

# Verify bootstrap binary exists
ls -lh target/release/bootstrap

# Test zip creation manually
TMP_DIR=$(mktemp -d)
cp target/release/bootstrap "$TMP_DIR/bootstrap"
cd "$TMP_DIR"
zip -j /tmp/test-lambda.zip bootstrap
cd -
rm -rf "$TMP_DIR"

# Verify zip contents
unzip -l /tmp/test-lambda.zip
# Expected: Archive contains single file named 'bootstrap'
```

**Step 4: Commit workflow changes**

```bash
git add .github/workflows/build-artifacts.yml
git commit -m "ci: add Lambda zip packaging to release workflow

Package Lambda binaries as zip files in addition to tar.gz for
direct CloudFormation deployment support.

Creates:
- otlp2parquet-lambda-linux-arm64.zip
- otlp2parquet-lambda-linux-amd64.zip

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 2: Create CloudFormation Template

**Files:**
- Create: `examples/aws-lambda-s3-tables/template.yaml`
- Create: `examples/aws-lambda-s3-tables/.gitignore`

**Step 1: Create example directory**

```bash
mkdir -p examples/aws-lambda-s3-tables
```

**Step 2: Create .gitignore file**

Create `examples/aws-lambda-s3-tables/.gitignore`:

```gitignore
# Deployment artifacts
*.zip
deployment-bucket.txt

# CloudFormation outputs
stack-outputs.json

# Test results
test-results/
*.log
```

**Step 3: Write CloudFormation template**

Create `examples/aws-lambda-s3-tables/template.yaml`:

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: 'otlp2parquet Lambda with S3 Tables (Apache Iceberg)'

Parameters:
  LambdaCodeBucket:
    Type: String
    Description: S3 bucket containing the Lambda deployment package

  LambdaCodeKey:
    Type: String
    Description: S3 key for the Lambda zip file
    Default: lambda/otlp2parquet-lambda.zip

  LambdaArchitecture:
    Type: String
    Description: Lambda architecture (arm64 recommended for better price/performance)
    Default: arm64
    AllowedValues:
      - arm64
      - x86_64

Resources:
  # S3 Table Bucket - Purpose-built for Apache Iceberg tables
  # Stores both Parquet data files AND Iceberg metadata
  # Provides automatic maintenance (compaction, snapshot management)
  OtelTableBucket:
    Type: AWS::S3Tables::TableBucket
    Properties:
      TableBucketName: !Sub 'otlp2parquet-${AWS::StackName}'

  # Lambda execution role with permissions for S3 Tables and CloudWatch Logs
  LambdaExecutionRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        # Basic execution role for CloudWatch Logs
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
      Policies:
        - PolicyName: S3TablesAccess
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              # S3 Tables catalog API permissions
              - Effect: Allow
                Action:
                  - s3tables:GetTableMetadataLocation
                  - s3tables:CreateTable
                  - s3tables:UpdateTableMetadataLocation
                  - s3tables:PutTableData
                  - s3tables:GetTableData
                  - s3tables:GetTableBucket
                Resource:
                  - !GetAtt OtelTableBucket.Arn
                  - !Sub '${OtelTableBucket.Arn}/*'
              # S3 object permissions for Parquet files within the table bucket
              - Effect: Allow
                Action:
                  - s3:PutObject
                  - s3:GetObject
                  - s3:DeleteObject
                Resource:
                  - !Sub 'arn:aws:s3:::${OtelTableBucket}/*'

  # CloudWatch log group with 7-day retention for cost optimization
  LambdaLogGroup:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub '/aws/lambda/${AWS::StackName}-ingest'
      RetentionInDays: 7

  # Lambda function running otlp2parquet Rust binary
  OtlpIngestFunction:
    Type: AWS::Lambda::Function
    DependsOn: LambdaLogGroup
    Properties:
      FunctionName: !Sub '${AWS::StackName}-ingest'
      Runtime: provided.al2023
      Handler: bootstrap
      Code:
        S3Bucket: !Ref LambdaCodeBucket
        S3Key: !Ref LambdaCodeKey
      Role: !GetAtt LambdaExecutionRole.Arn
      Architectures:
        - !Ref LambdaArchitecture
      MemorySize: 512
      Timeout: 60
      Environment:
        Variables:
          # Iceberg REST catalog endpoint for S3 Tables
          OTLP2PARQUET_ICEBERG_REST_URI: !Sub 'https://s3tables.${AWS::Region}.amazonaws.com/iceberg'
          # Warehouse location (ARN of the table bucket)
          OTLP2PARQUET_ICEBERG_WAREHOUSE: !GetAtt OtelTableBucket.Arn
          # Namespace for OTLP tables (creates otel.logs, otel.traces, etc.)
          OTLP2PARQUET_ICEBERG_NAMESPACE: 'otel'
          # Rust log level for debugging
          RUST_LOG: 'info'

Outputs:
  LambdaFunctionArn:
    Description: Lambda function ARN (use for testing with aws lambda invoke)
    Value: !GetAtt OtlpIngestFunction.Arn
    Export:
      Name: !Sub '${AWS::StackName}-LambdaArn'

  LambdaFunctionName:
    Description: Lambda function name
    Value: !Ref OtlpIngestFunction
    Export:
      Name: !Sub '${AWS::StackName}-LambdaName'

  TableBucketName:
    Description: S3 Table Bucket name
    Value: !Ref OtelTableBucket
    Export:
      Name: !Sub '${AWS::StackName}-TableBucket'

  TableBucketArn:
    Description: S3 Table Bucket ARN (use as warehouse location)
    Value: !GetAtt OtelTableBucket.Arn
    Export:
      Name: !Sub '${AWS::StackName}-TableBucketArn'

  IcebergRestEndpoint:
    Description: Iceberg REST catalog endpoint (use with DuckDB, Spark, etc.)
    Value: !Sub 'https://s3tables.${AWS::Region}.amazonaws.com/iceberg'
    Export:
      Name: !Sub '${AWS::StackName}-IcebergEndpoint'

  Region:
    Description: AWS Region where resources are deployed
    Value: !Ref AWS::Region
    Export:
      Name: !Sub '${AWS::StackName}-Region'
```

**Step 4: Validate template syntax**

```bash
cd examples/aws-lambda-s3-tables
aws cloudformation validate-template --template-body file://template.yaml
# Expected: Returns TemplateDescription and Parameters without errors
```

**Step 5: Commit CloudFormation template**

```bash
git add examples/aws-lambda-s3-tables/
git commit -m "feat: add CloudFormation template for Lambda + S3 Tables

Create production-ready CloudFormation template with:
- S3 Table Bucket for Iceberg storage
- Lambda function with arm64 default architecture
- IAM role with least-privilege S3 Tables permissions
- CloudWatch log group with 7-day retention

Template supports both arm64 and x86_64 architectures via parameter.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 3: Create Deployment Script

**Files:**
- Create: `examples/aws-lambda-s3-tables/deploy.sh`

**Step 1: Write deployment script**

Create `examples/aws-lambda-s3-tables/deploy.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# deploy.sh - Deploy otlp2parquet Lambda with S3 Tables
#
# Usage:
#   ./deploy.sh [OPTIONS]
#
# Options:
#   --arch <arm64|amd64>      Lambda architecture (default: arm64)
#   --region <region>         AWS region (default: $AWS_REGION or us-east-1)
#   --stack-name <name>       CloudFormation stack name (default: otlp2parquet)
#   --version <version>       GitHub release version (default: latest)
#   --delete                  Delete the stack and cleanup resources
#   --help                    Show this help message

# Default configuration
ARCH="arm64"
REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="otlp2parquet"
VERSION="latest"
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

# Download Lambda binary from GitHub releases
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

# Create deployment S3 bucket if it doesn't exist
info "Setting up deployment bucket: $DEPLOYMENT_BUCKET"

if ! aws s3 ls "s3://${DEPLOYMENT_BUCKET}" --region "$REGION" &>/dev/null; then
  info "Creating deployment bucket..."
  if [[ "$REGION" == "us-east-1" ]]; then
    aws s3 mb "s3://${DEPLOYMENT_BUCKET}" --region "$REGION"
  else
    aws s3 mb "s3://${DEPLOYMENT_BUCKET}" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
  info "Deployment bucket created."
else
  info "Deployment bucket already exists."
fi

# Upload Lambda zip to S3
S3_KEY="lambda/${ASSET_NAME}"
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
```

**Step 2: Make script executable**

```bash
chmod +x examples/aws-lambda-s3-tables/deploy.sh
```

**Step 3: Test script help output**

```bash
cd examples/aws-lambda-s3-tables
./deploy.sh --help
# Expected: Shows usage information and options
```

**Step 4: Commit deployment script**

```bash
git add examples/aws-lambda-s3-tables/deploy.sh
git commit -m "feat: add deployment automation script

Add comprehensive deployment script that:
- Downloads Lambda binary from GitHub releases
- Creates deployment S3 bucket
- Uploads Lambda code to S3
- Deploys CloudFormation stack
- Supports cleanup with --delete flag

Includes error handling, colored output, and validation.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 4: Create Test Script

**Files:**
- Create: `examples/aws-lambda-s3-tables/test.sh`

**Step 1: Write test script**

Create `examples/aws-lambda-s3-tables/test.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# test.sh - Test otlp2parquet Lambda deployment
#
# Usage:
#   ./test.sh [OPTIONS]
#
# Options:
#   --stack-name <name>   CloudFormation stack name (default: otlp2parquet)
#   --region <region>     AWS region (default: $AWS_REGION or us-east-1)
#   --verbose             Show verbose Lambda output
#   --help                Show this help message

# Default configuration
STACK_NAME="otlp2parquet"
REGION="${AWS_REGION:-us-east-1}"
VERBOSE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTDATA_DIR="${SCRIPT_DIR}/../../testdata"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print functions
info() { echo -e "${GREEN}==>${NC} $*"; }
warn() { echo -e "${YELLOW}WARNING:${NC} $*" >&2; }
error() { echo -e "${RED}ERROR:${NC} $*" >&2; exit 1; }
section() { echo -e "\n${BLUE}### $*${NC}\n"; }

# Show help
show_help() {
  grep '^#' "$0" | grep -v '#!/usr/bin/env' | sed 's/^# \?//'
  exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --stack-name)
      STACK_NAME="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --verbose)
      VERBOSE=true
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

# Check required tools
for cmd in aws jq; do
  if ! command -v "$cmd" &> /dev/null; then
    error "$cmd is required but not installed."
  fi
done

# Check testdata directory exists
if [[ ! -d "$TESTDATA_DIR" ]]; then
  error "Testdata directory not found: $TESTDATA_DIR"
fi

# Get stack outputs
info "Retrieving stack information..."

LAMBDA_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
  --output text 2>/dev/null) || \
  error "Failed to get Lambda function name. Is stack '$STACK_NAME' deployed?"

TABLE_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`TableBucketName`].OutputValue' \
  --output text)

ICEBERG_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`IcebergRestEndpoint`].OutputValue' \
  --output text)

info "Lambda Function: $LAMBDA_NAME"
info "Table Bucket: $TABLE_BUCKET"
info "Iceberg Endpoint: $ICEBERG_ENDPOINT"

# Function to invoke Lambda with test data
invoke_lambda() {
  local test_name=$1
  local payload_file=$2
  local signal_type=$3

  section "Testing: $test_name"

  info "Payload: $payload_file"

  # Create temporary files for response
  local response_file=$(mktemp)
  local output_file=$(mktemp)

  # Invoke Lambda
  if aws lambda invoke \
    --function-name "$LAMBDA_NAME" \
    --region "$REGION" \
    --payload "file://${payload_file}" \
    --cli-binary-format raw-in-base64-out \
    "$response_file" \
    > "$output_file" 2>&1; then

    info "✓ Lambda invocation successful"

    # Show response
    if [[ "$VERBOSE" == "true" ]]; then
      echo "Response:"
      cat "$response_file" | jq '.' || cat "$response_file"
    fi

    # Check for errors in response
    if grep -q "errorMessage" "$response_file" 2>/dev/null; then
      warn "Lambda returned an error:"
      cat "$response_file" | jq '.errorMessage, .errorType' || cat "$response_file"
    else
      info "✓ No errors in response"
    fi
  else
    error "Lambda invocation failed. Output:\n$(cat "$output_file")"
  fi

  # Cleanup
  rm -f "$response_file" "$output_file"

  echo ""
}

# Test logs
if [[ -f "${TESTDATA_DIR}/log.json" ]]; then
  invoke_lambda "Logs (JSON)" "${TESTDATA_DIR}/log.json" "logs"
else
  warn "Skipping logs test: ${TESTDATA_DIR}/log.json not found"
fi

# Test traces
if [[ -f "${TESTDATA_DIR}/trace.json" ]]; then
  invoke_lambda "Traces (JSON)" "${TESTDATA_DIR}/trace.json" "traces"
else
  warn "Skipping traces test: ${TESTDATA_DIR}/trace.json not found"
fi

# Test metrics - all types
for metric_type in gauge sum histogram exponential_histogram summary; do
  metric_file="${TESTDATA_DIR}/metrics_${metric_type}.json"
  if [[ -f "$metric_file" ]]; then
    invoke_lambda "Metrics: ${metric_type}" "$metric_file" "metrics"
  else
    warn "Skipping metrics_${metric_type} test: $metric_file not found"
  fi
done

# Show recent CloudWatch logs
section "Recent CloudWatch Logs (last 5 minutes)"

LOG_GROUP="/aws/lambda/${LAMBDA_NAME}"

info "Fetching logs from: $LOG_GROUP"

aws logs tail "$LOG_GROUP" \
  --region "$REGION" \
  --since 5m \
  --format short \
  2>/dev/null || warn "No recent logs found (this is normal for first run)"

echo ""

# Summary
section "Test Summary"

info "✓ All Lambda invocations completed"
info "Check CloudWatch Logs for detailed output:"
echo "  aws logs tail $LOG_GROUP --region $REGION --follow"
echo ""
info "Next steps:"
echo "  1. Verify tables created in S3 Tables console"
echo "  2. Query data with DuckDB (see README.md for examples)"
echo "  3. Set up API Gateway for production OTLP ingestion"
echo ""
```

**Step 2: Make script executable**

```bash
chmod +x examples/aws-lambda-s3-tables/test.sh
```

**Step 3: Test script help output**

```bash
cd examples/aws-lambda-s3-tables
./test.sh --help
# Expected: Shows usage information and options
```

**Step 4: Commit test script**

```bash
git add examples/aws-lambda-s3-tables/test.sh
git commit -m "feat: add test automation script

Add comprehensive test script that:
- Invokes Lambda with all signal types (logs, traces, 5 metric types)
- Uses existing testdata/ directory
- Shows CloudWatch logs output
- Validates Lambda responses
- Provides next steps guidance

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: Create Main Documentation

**Files:**
- Create: `docs/storage/aws-lambda-s3-tables.md`

**Step 1: Write main documentation**

Create `docs/storage/aws-lambda-s3-tables.md`:

```markdown
# AWS Lambda + S3 Tables Deployment

Deploy `otlp2parquet` as an AWS Lambda function with S3 Tables (Apache Iceberg) for event-driven OTLP ingestion. This deployment pattern is ideal for cost-effective, serverless observability data pipelines.

## When to Use This Pattern

**Good fit:**
- Event-driven or batch OTLP ingestion (not real-time streaming)
- Moderate to low ingestion rates (<1000 requests/minute)
- Cost-sensitive deployments (pay only for actual usage)
- Teams already using AWS Lambda for other workloads

**Not ideal for:**
- High-throughput streaming ingestion (>1000 requests/second)
- Sub-second latency requirements
- Long-running workloads that exceed Lambda limits (15 min timeout)

For high-throughput scenarios, consider the [server deployment](../deployment/server.md) pattern instead.

## Prerequisites

- AWS account with permissions to create Lambda, S3 Tables, IAM roles, and CloudFormation stacks
- AWS CLI configured (`aws configure`)
- Basic understanding of CloudFormation and Lambda

## Quick Start

Deploy the complete stack in three commands:

```bash
# Navigate to the example directory
cd examples/aws-lambda-s3-tables

# Deploy the stack (downloads Lambda binary from GitHub releases)
./deploy.sh --region us-east-1

# Test with sample OTLP data
./test.sh
```

That's it! The deployment script automatically:
1. Downloads the latest Lambda binary from GitHub releases
2. Creates an S3 bucket for deployment artifacts
3. Uploads the Lambda code to S3
4. Deploys the CloudFormation stack with all resources

## Architecture

```
┌─────────────────┐
│  OTLP Client    │
│  (your app)     │
└────────┬────────┘
         │ HTTP POST
         │ (protobuf/JSON)
         ▼
┌─────────────────────────────────────────────────┐
│  AWS Lambda                                     │
│  ┌───────────────────────────────────────────┐ │
│  │  otlp2parquet (Rust binary)               │ │
│  │  - Parse OTLP                             │ │
│  │  - Convert to Arrow RecordBatch           │ │
│  │  - Write Parquet to S3 Table Bucket       │ │
│  │  - Commit Iceberg transaction             │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  S3 Table Bucket                                │
│  ┌───────────────┐   ┌──────────────────────┐  │
│  │ Parquet Files │   │ Iceberg Metadata     │  │
│  │ logs/...      │   │ - Manifests          │  │
│  │ traces/...    │   │ - Snapshots          │  │
│  │ metrics/...   │   │ - Schema evolution   │  │
│  └───────────────┘   └──────────────────────┘  │
│                                                 │
│  Automatic Maintenance:                         │
│  - Compaction                                   │
│  - Snapshot management                          │
│  - Unreferenced file removal                    │
└─────────────────────────────────────────────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │  Query Engines      │
         │  - DuckDB           │
         │  - Amazon Athena    │
         │  - Apache Spark     │
         └─────────────────────┘
```

### Components

**S3 Table Bucket**
- Purpose-built AWS service for Apache Iceberg tables
- Stores both Parquet data files AND Iceberg catalog metadata
- Provides automatic table maintenance (compaction, snapshot cleanup)
- No separate S3 bucket or Glue Catalog needed

**Lambda Function**
- Runtime: `provided.al2023` (custom runtime for Rust binary)
- Default: arm64 architecture (20% better price/performance vs x86_64)
- Memory: 512MB (adjustable based on batch size)
- Timeout: 60 seconds
- Triggered manually for testing (extend with API Gateway for production)

**IAM Role**
- CloudWatch Logs permissions (function logging)
- S3 Tables API permissions (create tables, commit transactions)
- S3 object permissions (read/write Parquet files)

## Configuration

### CloudFormation Parameters

When deploying the stack, you can customize these parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `LambdaCodeBucket` | S3 bucket containing Lambda zip | (set by deploy script) |
| `LambdaCodeKey` | S3 key for Lambda zip file | `lambda/otlp2parquet-lambda.zip` |
| `LambdaArchitecture` | Lambda architecture | `arm64` |

### Environment Variables

The Lambda function is configured with these environment variables (set automatically by CloudFormation):

| Variable | Value | Purpose |
|----------|-------|---------|
| `OTLP2PARQUET_ICEBERG_REST_URI` | `https://s3tables.<region>.amazonaws.com/iceberg` | S3 Tables REST catalog endpoint |
| `OTLP2PARQUET_ICEBERG_WAREHOUSE` | `arn:aws:s3tables:...` | Table bucket ARN |
| `OTLP2PARQUET_ICEBERG_NAMESPACE` | `otel` | Namespace for tables |
| `RUST_LOG` | `info` | Rust logging level |

### Tables Created

The Lambda automatically creates these Iceberg tables on first write:

- `otel.logs` - Log records
- `otel.traces` - Trace spans
- `otel.metrics_gauge` - Gauge metrics
- `otel.metrics_sum` - Sum/counter metrics
- `otel.metrics_histogram` - Histogram metrics
- `otel.metrics_exponential_histogram` - Exponential histogram metrics
- `otel.metrics_summary` - Summary metrics

## Querying Your Data

### Option 1: DuckDB (Best for Ad-Hoc Analysis)

DuckDB has excellent support for Iceberg REST catalogs and can query S3 Tables directly.

**Setup:**

```sql
-- Install and load Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Configure AWS credentials (use your preferred method)
SET s3_region='us-east-1';
SET s3_access_key_id='YOUR_ACCESS_KEY';
SET s3_secret_access_key='YOUR_SECRET_KEY';

-- Configure Iceberg REST catalog
CREATE SECRET s3tables (
    TYPE ICEBERG_REST,
    ENDPOINT 'https://s3tables.us-east-1.amazonaws.com/iceberg',
    WAREHOUSE 'arn:aws:s3tables:us-east-1:123456789012:bucket/otlp2parquet-demo',
    AWS_REGION 'us-east-1'
);
```

**Query examples:**

```sql
-- Query recent logs
SELECT
    Timestamp,
    SeverityText,
    Body,
    ServiceName
FROM iceberg_scan('s3tables', 'otel', 'logs')
WHERE Timestamp > now() - INTERVAL 1 HOUR
ORDER BY Timestamp DESC
LIMIT 100;

-- Analyze trace latencies
SELECT
    SpanName,
    COUNT(*) as count,
    AVG(Duration) / 1000000 as avg_duration_ms,
    MAX(Duration) / 1000000 as max_duration_ms
FROM iceberg_scan('s3tables', 'otel', 'traces')
WHERE Timestamp > now() - INTERVAL 1 DAY
GROUP BY SpanName
ORDER BY avg_duration_ms DESC;

-- Aggregate gauge metrics
SELECT
    MetricName,
    date_trunc('minute', TimeUnixNano / 1000000000) as minute,
    AVG(Value) as avg_value
FROM iceberg_scan('s3tables', 'otel', 'metrics_gauge')
WHERE TimeUnixNano > (SELECT extract(epoch from now() - INTERVAL 1 HOUR) * 1000000000)
GROUP BY MetricName, minute
ORDER BY minute DESC;
```

### Option 2: Amazon Athena (Best for Team Access)

Athena provides a managed query service that integrates with S3 Tables automatically.

**Setup:**

1. Open the [Amazon Athena console](https://console.aws.amazon.com/athena/)
2. Create a workgroup (if you don't have one)
3. Configure result location: `s3://your-athena-results-bucket/`
4. S3 Tables are automatically available in Athena's Data Catalog

**Query examples:**

```sql
-- Query logs (replace table bucket name)
SELECT * FROM "s3tables_catalog"."otel"."logs"
WHERE Timestamp > current_timestamp - INTERVAL '1' HOUR
LIMIT 100;

-- Trace analysis
SELECT
    SpanName,
    COUNT(*) as span_count,
    AVG(Duration) / 1000000 as avg_ms
FROM "s3tables_catalog"."otel"."traces"
WHERE Timestamp > current_timestamp - INTERVAL '1' DAY
GROUP BY SpanName
ORDER BY avg_ms DESC;
```

## Production Considerations

### API Gateway Integration

For production OTLP ingestion, add an API Gateway in front of Lambda:

```yaml
ApiGateway:
  Type: AWS::ApiGatewayV2::Api
  Properties:
    Name: otlp-ingestion
    ProtocolType: HTTP
    CorsConfiguration:
      AllowOrigins:
        - '*'
      AllowMethods:
        - POST
```

Benefits:
- HTTPS endpoint for OTLP clients
- Request throttling and quotas
- API keys for authentication
- CloudWatch metrics and logging

### VPC Placement

Lambda functions don't require VPC placement for S3 Tables access (S3 Tables is a public service). However, if your Lambda needs to access resources in a VPC:

1. Add VPC configuration to Lambda function
2. Ensure VPC has NAT Gateway or VPC endpoints for S3 and S3 Tables
3. Increase timeout to account for cold start latency

### IAM Best Practices

The provided IAM role follows least-privilege:
- Scoped to specific table bucket
- No wildcard permissions
- Separate policies for S3 Tables API vs S3 objects

For production, consider:
- Using IAM roles per environment (dev/staging/prod)
- Adding resource tags for cost allocation
- Implementing SCPs for organization-wide guardrails

### Cost Monitoring

Track costs by:
- Lambda invocations and duration (CloudWatch metrics)
- S3 Tables storage (S3 console)
- S3 Tables API requests (CloudTrail logs)
- CloudWatch log storage

Set up billing alerts:
```bash
aws cloudwatch put-metric-alarm \
  --alarm-name otlp2parquet-cost \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 86400 \
  --evaluation-periods 1 \
  --threshold 100 \
  --comparison-operator GreaterThanThreshold
```

### Batch Size Tuning

Lambda memory and timeout affect cost and performance:

| Batch Size | Memory | Timeout | Cost/Invocation |
|------------|--------|---------|-----------------|
| <100 records | 256MB | 30s | $0.000000417 |
| 100-1000 records | 512MB | 60s | $0.000000833 |
| 1000+ records | 1024MB | 120s | $0.000001667 |

Tune based on your batch characteristics:
- Logs: ~100 records per batch
- Traces: ~50 spans per batch
- Metrics: ~200 data points per batch

## Troubleshooting

### Lambda Timeout Errors

**Symptom:** Lambda times out before completing

**Solutions:**
1. Increase timeout in CloudFormation template: `Timeout: 120`
2. Increase memory (more memory = faster CPU): `MemorySize: 1024`
3. Reduce batch size in OTLP exporter configuration

### IAM Permission Errors

**Symptom:** `AccessDenied` errors in CloudWatch logs

**Solutions:**
1. Verify IAM role has S3 Tables permissions:
   ```bash
   aws iam get-role-policy --role-name <role-name> --policy-name S3TablesAccess
   ```
2. Check table bucket ARN matches IAM policy resource
3. Ensure S3 object permissions include the table bucket

### S3 Tables Quota Limits

**Symptom:** `ThrottlingException` or `ServiceQuotaExceeded`

**Solutions:**
1. Check current quotas:
   ```bash
   aws service-quotas get-service-quota \
     --service-code s3tables \
     --quota-code L-12345678
   ```
2. Request quota increase via AWS Support
3. Implement exponential backoff in OTLP exporter

### Missing Tables

**Symptom:** Tables not appearing in S3 Tables console

**Solutions:**
1. Check Lambda logs for Iceberg commit errors
2. Verify `OTLP2PARQUET_ICEBERG_*` environment variables are correct
3. Ensure at least one successful Lambda invocation per signal type
4. Tables are created lazily on first write (not pre-created)

### Cold Start Latency

**Symptom:** First invocation takes 5-10 seconds

**Solutions:**
1. This is normal for Rust Lambda cold starts
2. Enable provisioned concurrency for production (increases cost)
3. Configure OTLP exporter retry logic to handle initial latency
4. Consider keeping Lambda warm with scheduled CloudWatch Events

## Next Steps

- **Set up API Gateway** - Add HTTPS endpoint for production OTLP ingestion
- **Configure monitoring** - Set up CloudWatch dashboards and alarms
- **Implement authentication** - Add API keys or IAM authentication
- **Optimize costs** - Tune Lambda memory and batch sizes
- **Query data** - Connect DuckDB or Athena to analyze your telemetry

## Related Documentation

- [S3 Tables Overview](./iceberg.md) - Learn about Apache Iceberg integration
- [Object Storage](./object-storage.md) - Alternative: Direct Parquet file storage
- [Server Deployment](../deployment/server.md) - Alternative: Long-running server
```

**Step 2: Commit main documentation**

```bash
git add docs/storage/aws-lambda-s3-tables.md
git commit -m "docs: add comprehensive AWS Lambda + S3 Tables guide

Add production-ready documentation covering:
- When to use Lambda deployment pattern
- Quick start guide (3 commands)
- Architecture diagram and component explanation
- Configuration reference
- Query examples for DuckDB and Athena
- Production considerations (API Gateway, VPC, IAM, costs)
- Troubleshooting common issues

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 6: Create Example README

**Files:**
- Create: `examples/aws-lambda-s3-tables/README.md`

**Step 1: Write example README**

Create `examples/aws-lambda-s3-tables/README.md`:

```markdown
# AWS Lambda + S3 Tables Example

Production-ready deployment of `otlp2parquet` as an AWS Lambda function with S3 Tables (Apache Iceberg).

## Quick Start

Deploy in 3 commands:

```bash
# Deploy the stack
./deploy.sh --region us-east-1

# Test with sample data
./test.sh

# View logs
aws logs tail /aws/lambda/otlp2parquet-ingest --follow
```

## What's Included

- **`template.yaml`** - CloudFormation template creating:
  - S3 Table Bucket (Iceberg storage)
  - Lambda function (arm64)
  - IAM execution role
  - CloudWatch log group

- **`deploy.sh`** - Deployment automation:
  - Downloads Lambda binary from GitHub releases
  - Creates deployment S3 bucket
  - Uploads Lambda code
  - Deploys CloudFormation stack

- **`test.sh`** - Testing automation:
  - Invokes Lambda with sample OTLP data
  - Shows CloudWatch logs
  - Validates responses

## Prerequisites

- AWS CLI configured (`aws configure`)
- Permissions to create: Lambda, S3 Tables, IAM roles, CloudFormation stacks
- `jq` installed for JSON parsing

## Usage

### Deploy

```bash
# Deploy with defaults (arm64, us-east-1)
./deploy.sh

# Deploy with specific options
./deploy.sh --arch amd64 --region us-west-2 --stack-name my-otlp-stack

# Deploy specific version
./deploy.sh --version v0.0.2
```

### Test

```bash
# Run all tests
./test.sh

# Test specific stack
./test.sh --stack-name my-otlp-stack --region us-west-2

# Verbose output
./test.sh --verbose
```

### Cleanup

```bash
# Delete stack and resources
./deploy.sh --delete
```

## Architecture

```
OTLP Client → Lambda (otlp2parquet) → S3 Tables (Iceberg)
                                          ↓
                                    Query Engines
                                    (DuckDB, Athena)
```

The Lambda function:
1. Receives OTLP data (protobuf/JSON)
2. Converts to Arrow RecordBatch
3. Writes Parquet to S3 Table Bucket
4. Commits Iceberg transaction via REST catalog

Tables created automatically:
- `otel.logs`
- `otel.traces`
- `otel.metrics_gauge`
- `otel.metrics_sum`
- `otel.metrics_histogram`
- `otel.metrics_exponential_histogram`
- `otel.metrics_summary`

## Query Examples

### DuckDB

```sql
INSTALL iceberg;
LOAD iceberg;

-- Configure S3 Tables catalog
CREATE SECRET s3tables (
    TYPE ICEBERG_REST,
    ENDPOINT 'https://s3tables.us-east-1.amazonaws.com/iceberg',
    WAREHOUSE 'arn:aws:s3tables:us-east-1:123456789012:bucket/otlp2parquet-demo',
    AWS_REGION 'us-east-1'
);

-- Query logs
SELECT * FROM iceberg_scan('s3tables', 'otel', 'logs')
WHERE Timestamp > now() - INTERVAL 1 HOUR
LIMIT 100;
```

### Amazon Athena

```sql
SELECT * FROM "s3tables_catalog"."otel"."logs"
WHERE Timestamp > current_timestamp - INTERVAL '1' HOUR
LIMIT 100;
```

## Production Extensions

This example is for testing and development. For production, add:

- **API Gateway** - HTTPS endpoint for OTLP clients
- **Authentication** - API keys or IAM-based auth
- **Monitoring** - CloudWatch dashboards and alarms
- **VPC** - If accessing private resources
- **Provisioned Concurrency** - Reduce cold start latency

## Documentation

See [docs/storage/aws-lambda-s3-tables.md](../../docs/storage/aws-lambda-s3-tables.md) for:
- Detailed architecture explanation
- Configuration reference
- Production considerations
- Troubleshooting guide
- Cost optimization tips

## Cost Estimate

Approximate costs for 1 million OTLP requests/month:

- Lambda invocations: ~$0.20
- Lambda compute (arm64, 512MB): ~$1.00
- S3 Tables storage (10GB): ~$0.23
- S3 Tables API requests: ~$0.05
- CloudWatch Logs (1GB): ~$0.50

**Total: ~$2/month** (excluding data transfer and queries)

## Troubleshooting

**Lambda timeout errors**
- Increase `Timeout` in `template.yaml`
- Increase `MemorySize` for faster execution

**IAM permission errors**
- Check CloudWatch logs: `aws logs tail /aws/lambda/otlp2parquet-ingest`
- Verify IAM role has S3 Tables permissions

**Missing tables**
- Tables are created on first write (not pre-created)
- Check Lambda logs for Iceberg commit errors
- Verify environment variables in CloudFormation

## License

Same as otlp2parquet project.
```

**Step 2: Commit example README**

```bash
git add examples/aws-lambda-s3-tables/README.md
git commit -m "docs: add example README for Lambda deployment

Add concise task-oriented README covering:
- Quick start (3 commands)
- File descriptions
- Usage examples for deploy/test scripts
- Query examples for DuckDB and Athena
- Production extensions checklist
- Cost estimates
- Common troubleshooting

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 7: Final Integration and Testing

**Files:**
- Modify: `docs/storage/index.md` (add link to new guide)

**Step 1: Update storage index documentation**

Read the current `docs/storage/index.md` to see where to add the Lambda link:

```bash
cat docs/storage/index.md
```

Add a new row to the table or section mentioning the Lambda deployment option. The exact change depends on the current structure, but should link to `aws-lambda-s3-tables.md`.

**Step 2: Verify all files are in place**

```bash
# Check example directory structure
tree examples/aws-lambda-s3-tables/
# Expected:
# examples/aws-lambda-s3-tables/
# ├── .gitignore
# ├── README.md
# ├── deploy.sh
# ├── template.yaml
# └── test.sh

# Verify scripts are executable
ls -la examples/aws-lambda-s3-tables/*.sh
# Expected: -rwxr-xr-x for both .sh files
```

**Step 3: Validate CloudFormation template**

```bash
cd examples/aws-lambda-s3-tables
aws cloudformation validate-template --template-body file://template.yaml
# Expected: Success with parameter descriptions
```

**Step 4: Test script help outputs**

```bash
./deploy.sh --help
# Expected: Shows usage information

./test.sh --help
# Expected: Shows usage information
```

**Step 5: Commit index update**

```bash
git add docs/storage/index.md
git commit -m "docs: link to Lambda + S3 Tables deployment guide

Add reference to new AWS Lambda deployment example in storage
documentation index.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

**Step 6: Run pre-commit checks**

```bash
make pre-commit
# Expected: All checks pass (fmt, clippy, test)
```

---

## Success Criteria

✅ Release workflow packages Lambda as zip files
✅ CloudFormation template deploys cleanly with all resources
✅ Deploy script downloads from releases and deploys stack
✅ Test script invokes Lambda with all signal types
✅ Documentation explains architecture and production usage
✅ Example README provides quick-start guide
✅ All scripts have error handling and helpful output
✅ Pre-commit checks pass

## Notes for Implementation

- **Order matters**: Must implement tasks sequentially because later tasks depend on earlier files
- **Test incrementally**: Validate each script with `--help` before committing
- **AWS credentials**: Scripts assume AWS CLI is configured, no credentials in code
- **Error messages**: All scripts should fail fast with clear error messages
- **Idempotency**: Deploy script should handle existing resources gracefully
- **DRY principle**: Test script references existing `testdata/` directory
- **Conventional commits**: Use feat/docs prefixes consistently

## Future Enhancements (Not in This Plan)

- API Gateway integration (separate task)
- CI/CD pipeline for automated deployments
- Multi-region deployment templates
- VPC networking examples
- Terraform version of templates
