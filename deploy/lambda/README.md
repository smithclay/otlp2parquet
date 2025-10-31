# AWS Lambda Deployment Guide

Deploy `otlp2parquet` to AWS Lambda with S3 storage. Serverless, event-driven, and fully managed.

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **AWS CLI** configured (`aws configure`)
3. **AWS SAM CLI** (recommended) or CloudFormation
4. **cargo-lambda** for Rust builds

## Installation

### AWS SAM CLI

```bash
# macOS
brew install aws-sam-cli

# Linux/Windows
pip install aws-sam-cli

# Verify installation
sam --version
```

### cargo-lambda

```bash
# Install cargo-lambda for Rust builds
pip install cargo-lambda

# Or with Homebrew
brew tap cargo-lambda/cargo-lambda
brew install cargo-lambda

# Verify installation
cargo lambda --version
```

## Quick Start (Guided Deployment)

### 1. Clone Repository

```bash
git clone https://github.com/smithclay/otlp2parquet.git
cd otlp2parquet
```

### 2. Deploy with SAM (Interactive)

```bash
# Deploy with guided prompts
sam deploy --guided
```

You'll be prompted for:

```
Stack Name [otlp2parquet]:
AWS Region [us-east-1]:
Parameter BucketName [otlp-logs]:           # S3 bucket for Parquet files
Parameter LogRetentionDays [7]:             # CloudWatch log retention
Confirm changes before deploy [Y/n]:
Allow SAM CLI IAM role creation [Y/n]: Y
Disable rollback [y/N]: N
Save arguments to configuration file [Y/n]: Y
```

### 3. Get Your Endpoint

After deployment completes, SAM outputs your Function URL:

```
CloudFormation outputs from deployed stack
--------------------------------------------------------------------
Outputs
--------------------------------------------------------------------
Key                 FunctionUrl
Description         OTLP HTTP endpoint URL
Value               https://abc123xyz.lambda-url.us-east-1.on.aws/
--------------------------------------------------------------------
```

### 4. Test Your Deployment

```bash
# Get Function URL from SAM output
FUNCTION_URL="https://abc123xyz.lambda-url.us-east-1.on.aws"

# Send test OTLP log payload
curl -X POST "${FUNCTION_URL}/v1/logs" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @test-payload.pb

# Check S3 bucket for Parquet files
aws s3 ls s3://otlp-logs/logs/ --recursive
```

## Subsequent Deployments

After the initial guided deployment, you can deploy quickly:

```bash
# Deploy using saved configuration
sam deploy

# Or with specific environment
sam deploy --config-env production
```

## Build and Test Locally

### Build Lambda Binary

```bash
# Build for Lambda (ARM64/Graviton2)
cargo lambda build --release --features lambda --arm64

# Binary will be at: target/lambda/otlp2parquet/bootstrap
```

### Test Locally

```bash
# Start Lambda runtime locally
sam local start-api

# API will be available at http://localhost:3000

# Test with curl
curl -X POST http://localhost:3000/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @test-payload.pb
```

### Invoke Function Locally

```bash
# Create test event
cat > event.json <<EOF
{
  "headers": {
    "content-type": "application/x-protobuf"
  },
  "body": "base64-encoded-protobuf-data",
  "isBase64Encoded": true
}
EOF

# Invoke function
sam local invoke OtlpToParquetFunction -e event.json
```

## Configuration

### Parameters

Edit `samconfig.toml` to customize:

```toml
[default.deploy.parameters]
stack_name = "otlp2parquet"
region = "us-east-1"
parameter_overrides = [
    "BucketName=my-otlp-logs",       # S3 bucket name
    "LogRetentionDays=30"            # CloudWatch logs retention
]
```

### Environment-Specific Deployments

```bash
# Deploy to staging
sam deploy --config-env staging

# Deploy to production
sam deploy --config-env production
```

Configure environments in `samconfig.toml`:

```toml
[staging.deploy.parameters]
stack_name = "otlp2parquet-staging"
parameter_overrides = [
    "BucketName=otlp-logs-staging",
    "LogRetentionDays=7"
]

[production.deploy.parameters]
stack_name = "otlp2parquet-prod"
parameter_overrides = [
    "BucketName=otlp-logs-prod",
    "LogRetentionDays=30"
]
```

## Monitoring

### View Logs

```bash
# View CloudWatch logs
sam logs --tail --stack-name otlp2parquet

# View logs for specific invocation
sam logs -n OtlpToParquetFunction --filter "ERROR"

# Or use AWS CLI
aws logs tail /aws/lambda/otlp2parquet-OtlpToParquetFunction --follow
```

### Lambda Insights

```bash
# Enable Lambda Insights for advanced monitoring
aws lambda update-function-configuration \
  --function-name otlp2parquet-OtlpToParquetFunction \
  --layers arn:aws:lambda:us-east-1:580247275435:layer:LambdaInsightsExtension-Arm64:latest
```

### CloudWatch Metrics

Available metrics in CloudWatch:
- `Invocations` - Number of function invocations
- `Duration` - Execution time
- `Errors` - Number of errors
- `Throttles` - Number of throttled requests
- `ConcurrentExecutions` - Concurrent invocations

## Security

### Function URL Authentication

By default, Function URL uses `AuthType: NONE` (public access). To require authentication:

Edit `template.yaml`:

```yaml
FunctionUrlConfig:
  AuthType: AWS_IAM  # Require AWS SigV4 authentication
```

Then sign requests with AWS credentials:

```bash
# Use awscurl or AWS SDK
pip install awscurl

awscurl -X POST \
  --service lambda \
  --region us-east-1 \
  "${FUNCTION_URL}/v1/logs" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @test-payload.pb
```

### VPC Configuration

To run Lambda in VPC (for private S3 access):

Edit `template.yaml`:

```yaml
OtlpToParquetFunction:
  Type: AWS::Serverless::Function
  Properties:
    VpcConfig:
      SecurityGroupIds:
        - sg-12345678
      SubnetIds:
        - subnet-12345678
        - subnet-87654321
```

### Least Privilege IAM

The SAM template creates a minimal IAM role with:
- S3 read/write to specified bucket only
- CloudWatch Logs write access

## Performance Tuning

### Memory and Timeout

Edit `template.yaml`:

```yaml
Globals:
  Function:
    Timeout: 60        # Increase timeout (default: 30s)
    MemorySize: 1024   # Increase memory (default: 512MB)
```

**Note:** More memory = more CPU (and faster execution)

### Provisioned Concurrency

For consistent performance (eliminates cold starts):

```bash
# Add provisioned concurrency
aws lambda put-provisioned-concurrency-config \
  --function-name otlp2parquet-OtlpToParquetFunction \
  --provisioned-concurrent-executions 2
```

**Cost:** $0.015 per GB-hour provisioned

## Architecture

```
┌─────────────────────────────────────────────┐
│  OTLP Client (OpenTelemetry SDK)           │
└────────────┬────────────────────────────────┘
             │ HTTP POST /v1/logs
             │ (Protobuf)
             ▼
┌─────────────────────────────────────────────┐
│  Lambda Function URL (Public Endpoint)     │
└────────────┬────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────┐
│  otlp2parquet Lambda Function (ARM64)      │
│  - Parse OTLP protobuf                      │
│  - Convert to Arrow RecordBatch             │
│  - Write Parquet with Snappy compression    │
└────────────┬────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────┐
│  S3 Bucket (Partitioned Parquet Files)     │
│  logs/{service}/year=/month=/day=/hour=/    │
│  - Versioned bucket                         │
│  - Encrypted at rest (AES-256)              │
│  - Lifecycle policies (IA, Glacier)         │
└─────────────────────────────────────────────┘
```

## Cost Estimation

### Lambda Costs (us-east-1, ARM64)

| Component | Free Tier | Price | Example (1M logs/day) |
|-----------|-----------|-------|------------------------|
| Requests | 1M/month | $0.20 per 1M | $6/month |
| Duration (512MB) | 400,000 GB-sec/month | $0.0000133 per GB-sec | ~$5/month |
| **Lambda Total** | | | **~$11/month** |

### S3 Costs

| Component | Price | Example (1M logs/day) |
|-----------|-------|------------------------|
| Storage (Standard) | $0.023/GB | $2.30/month (100GB) |
| PUT requests | $0.005 per 1000 | $1.50/month (300k files) |
| GET requests | $0.0004 per 1000 | Minimal (for queries) |
| **S3 Total** | | **~$4/month** |

### CloudWatch Logs

| Component | Price | Example (1M logs/day) |
|-----------|-------|------------------------|
| Ingestion | $0.50/GB | ~$0.50/month |
| Storage | $0.03/GB | ~$0.30/month (10GB) |
| **Logs Total** | | **~$1/month** |

**Total estimated cost: ~$16/month for 1M logs/day**

## CI/CD with GitHub Actions

Create `.github/workflows/deploy-lambda.yml`:

```yaml
name: Deploy to Lambda

on:
  push:
    tags: ['v*']

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1

      - name: Install SAM CLI
        run: pip install aws-sam-cli

      - name: Build and deploy
        run: sam deploy --no-confirm-changeset --no-fail-on-empty-changeset
```

## Troubleshooting

### Deployment fails

```bash
# View CloudFormation events
aws cloudformation describe-stack-events \
  --stack-name otlp2parquet \
  --max-items 10

# Common issues:
# 1. Bucket name already exists (must be globally unique)
# 2. Insufficient IAM permissions
# 3. cargo-lambda not installed
```

### Function timeout

```bash
# Increase timeout in template.yaml
Timeout: 60  # seconds

# Redeploy
sam deploy
```

### Cold starts

```bash
# Check duration metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Duration \
  --dimensions Name=FunctionName,Value=otlp2parquet-OtlpToParquetFunction \
  --start-time 2025-01-01T00:00:00Z \
  --end-time 2025-01-02T00:00:00Z \
  --period 3600 \
  --statistics Average,Maximum

# Solution: Add provisioned concurrency (see Performance Tuning)
```

## Advanced: CDK Deployment (TypeScript)

For infrastructure-as-code with TypeScript:

```bash
cd cdk
npm install
cdk deploy
```

See [cdk/README.md](./cdk/README.md) for details.

## Clean Up

```bash
# Delete the stack and all resources
sam delete --stack-name otlp2parquet

# Confirm deletion
Are you sure you want to delete the stack otlp2parquet? [y/N]: y

# Note: S3 bucket must be empty before deletion
aws s3 rm s3://otlp-logs --recursive
```

## Next Steps

- [Configure VPC](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html)
- [Set up API Gateway](https://docs.aws.amazon.com/lambda/latest/dg/services-apigateway.html) for advanced routing
- [Enable X-Ray tracing](https://docs.aws.amazon.com/lambda/latest/dg/services-xray.html)
- [Main documentation](../../README.md)
