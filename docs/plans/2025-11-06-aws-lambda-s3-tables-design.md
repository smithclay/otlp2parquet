# AWS Lambda + S3 Tables Deployment Example

**Design Document**
**Date:** 2025-11-06
**Status:** Approved

## Overview

Create a production-ready example demonstrating event-driven OTLP ingestion using AWS Lambda with S3 Tables (Apache Iceberg). This example provides developers with a complete, runnable deployment including CloudFormation templates, automation scripts, and comprehensive documentation.

## Goals

1. **Easy to deploy** - Single command deployment from GitHub releases
2. **Production-ready** - Follows AWS best practices for IAM, logging, and resource organization
3. **Well-documented** - Clear explanation of architecture, configuration, and querying patterns
4. **Demonstrates S3 Tables integration** - Shows the complete Iceberg catalog workflow

## Non-Goals

- API Gateway integration (mentioned in docs as future extension)
- Multi-region deployment patterns
- CI/CD pipeline templates
- VPC networking configuration

## Architecture

### Components

1. **S3 Table Bucket** (`AWS::S3Tables::TableBucket`)
   - Purpose-built Iceberg storage with automatic maintenance
   - Stores both Parquet data files AND Iceberg metadata
   - No separate general-purpose S3 bucket needed
   - Provides REST catalog endpoint for table management

2. **Lambda Function** (`AWS::Lambda::Function`)
   - Runtime: `provided.al2023` (custom runtime)
   - Default Architecture: `arm64` (better price/performance than x86_64)
   - Handler: `bootstrap` (compiled Rust binary)
   - Memory: 512MB (configurable)
   - Timeout: 60 seconds
   - Code source: S3 bucket (uploaded from GitHub release)

3. **IAM Execution Role**
   - CloudWatch Logs: `CreateLogGroup`, `CreateLogStream`, `PutLogEvents`
   - S3 Tables: `s3tables:GetTableMetadataLocation`, `s3tables:CreateTable`, `s3tables:UpdateTableMetadataLocation`, `s3tables:PutTableData`, `s3tables:GetTableData`
   - S3 Objects: `s3:PutObject`, `s3:GetObject` (scoped to table bucket)

4. **CloudWatch Log Group**
   - Retention: 7 days (cost optimization)
   - Explicit resource creation for immediate log capture

### Data Flow

```
OTLP Request (JSON/Protobuf)
    ↓
Lambda Invocation (manual via AWS CLI for testing)
    ↓
Parse & Convert to Arrow RecordBatch
    ↓
Write Parquet to S3 Table Bucket
    ↓
Commit Iceberg Transaction (via REST catalog)
    ↓
Response (success/error)
```

### Configuration

Lambda environment variables:
- `OTLP2PARQUET_ICEBERG_REST_URI`: `https://s3tables.<region>.amazonaws.com/iceberg`
- `OTLP2PARQUET_ICEBERG_WAREHOUSE`: `arn:aws:s3tables:<region>:<accountID>:bucket/<bucket-name>`
- `OTLP2PARQUET_ICEBERG_NAMESPACE`: `otel`

This creates tables:
- `otel.logs`
- `otel.traces`
- `otel.metrics_gauge`
- `otel.metrics_sum`
- `otel.metrics_histogram`
- `otel.metrics_exponential_histogram`
- `otel.metrics_summary`

## Implementation Plan

### 1. Release Workflow Update

**File:** `.github/workflows/build-artifacts.yml`

Add Lambda zip packaging in the `native` job (after line 67):

```yaml
- name: Package Lambda as zip
  run: |
    set -euo pipefail
    LAMBDA_BINARY="target/${{ matrix.target }}/release/bootstrap"
    TMP_DIR="$(mktemp -d)"
    cp "$LAMBDA_BINARY" "$TMP_DIR/bootstrap"
    cd "$TMP_DIR"
    zip -j "$OLDPWD/target/${{ matrix.target }}/release/${{ matrix.asset_name }}.zip" bootstrap
    rm -rf "$TMP_DIR"

- name: Upload Lambda zip artifact
  uses: actions/upload-artifact@v5
  with:
    name: ${{ matrix.asset_name }}-zip
    path: target/${{ matrix.target }}/release/${{ matrix.asset_name }}.zip
```

Update `publish` job to include zip files in release:

```yaml
files: |
  artifacts/**/*.tar.gz
  artifacts/**/*.zip
  checksums.txt
```

**Result:** Releases include `otlp2parquet-lambda-linux-arm64.zip` and `otlp2parquet-lambda-linux-amd64.zip`

### 2. CloudFormation Template

**File:** `examples/aws-lambda-s3-tables/template.yaml`

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
    Description: Lambda architecture
    Default: arm64
    AllowedValues:
      - arm64
      - x86_64

Resources:
  # S3 Table Bucket for Iceberg storage
  OtelTableBucket:
    Type: AWS::S3Tables::TableBucket
    Properties:
      TableBucketName: !Sub 'otlp2parquet-${AWS::StackName}'

  # Lambda execution role
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
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
      Policies:
        - PolicyName: S3TablesAccess
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - s3tables:GetTableMetadataLocation
                  - s3tables:CreateTable
                  - s3tables:UpdateTableMetadataLocation
                  - s3tables:PutTableData
                  - s3tables:GetTableData
                Resource:
                  - !Sub '${OtelTableBucket.Arn}'
                  - !Sub '${OtelTableBucket.Arn}/*'
              - Effect: Allow
                Action:
                  - s3:PutObject
                  - s3:GetObject
                Resource:
                  - !Sub '${OtelTableBucket.Arn}/*'

  # CloudWatch log group
  LambdaLogGroup:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub '/aws/lambda/${OtlpIngestFunction}'
      RetentionInDays: 7

  # Lambda function
  OtlpIngestFunction:
    Type: AWS::Lambda::Function
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
          OTLP2PARQUET_ICEBERG_REST_URI: !Sub 'https://s3tables.${AWS::Region}.amazonaws.com/iceberg'
          OTLP2PARQUET_ICEBERG_WAREHOUSE: !Sub '${OtelTableBucket.Arn}'
          OTLP2PARQUET_ICEBERG_NAMESPACE: 'otel'

Outputs:
  LambdaFunctionArn:
    Description: Lambda function ARN
    Value: !GetAtt OtlpIngestFunction.Arn

  TableBucketName:
    Description: S3 Table Bucket name
    Value: !Ref OtelTableBucket

  TableBucketArn:
    Description: S3 Table Bucket ARN
    Value: !GetAtt OtelTableBucket.Arn

  IcebergRestEndpoint:
    Description: Iceberg REST catalog endpoint
    Value: !Sub 'https://s3tables.${AWS::Region}.amazonaws.com/iceberg'
```

### 3. Deployment Script

**File:** `examples/aws-lambda-s3-tables/deploy.sh`

Features:
- Download Lambda zip from GitHub releases (latest or specific version)
- Create deployment bucket (or use existing)
- Upload Lambda zip to S3
- Deploy CloudFormation stack
- Optional: `--delete` flag to clean up resources

```bash
#!/usr/bin/env bash
set -euo pipefail

ARCH="arm64"
REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="otlp2parquet"
VERSION="latest"
DELETE=false

# Parse arguments...
# Download from GitHub releases
# Upload to S3
# Deploy CloudFormation
```

### 4. Test Script

**File:** `examples/aws-lambda-s3-tables/test.sh`

Features:
- Invoke Lambda with testdata files
- Show CloudWatch logs
- Verify tables created in S3 Tables
- Example DuckDB queries

Uses existing test data from `../../testdata/`:
- `log.json` - Single log record
- `trace.json` - Single trace
- `metrics_gauge.json`, `metrics_sum.json`, etc.

### 5. Main Documentation

**File:** `docs/storage/aws-lambda-s3-tables.md`

Sections:
1. Overview - When to use Lambda vs server deployment
2. Prerequisites - AWS CLI, CloudFormation basics
3. Quick Start - 3-command deployment
4. Architecture - Diagram and component explanation
5. Configuration Reference - Parameters and environment variables
6. Querying Your Data - DuckDB and Athena examples
7. Production Considerations - API Gateway, VPC, monitoring
8. Troubleshooting - Common issues and solutions

### 6. Example README

**File:** `examples/aws-lambda-s3-tables/README.md`

Brief intro with:
- Link to main documentation
- Quick start commands
- File descriptions

## Design Decisions

### Why S3 Tables instead of Glue Catalog?

S3 Tables is purpose-built for Iceberg with simpler setup (no Hive compatibility layer) and provides a single REST endpoint. It also includes automatic table maintenance (compaction, snapshot management).

### Why arm64 as default?

Better price/performance ratio compared to x86_64. Lambda charges by GB-second, and arm64 (Graviton2) provides ~20% better price/performance.

### Why reference testdata instead of duplicating?

DRY principle - the example always uses the canonical test data maintained by the project. Ensures consistency and reduces maintenance burden.

### Why download from GitHub releases instead of bundling?

- Examples stay lightweight (no large binaries in git)
- Always uses official, tested releases
- Easier to update to new versions
- Follows AWS best practice of storing Lambda code in S3

### Why manual Lambda invocation instead of API Gateway?

Keeps the example focused on the core integration pattern. API Gateway adds complexity (auth, CORS, throttling) that distracts from the S3 Tables integration. Production considerations section mentions API Gateway as a natural extension.

## Success Criteria

1. Developer can deploy from scratch in <5 minutes
2. Test script demonstrates all signal types (logs, traces, 5 metric types)
3. Documentation explains both "how" and "why"
4. CloudFormation template follows AWS best practices
5. Scripts are idempotent and include error handling

## Future Enhancements

Not in scope for initial release, but documented as extensions:

- API Gateway integration example
- VPC networking configuration
- Multi-account deployment patterns
- Cost optimization strategies
- EventBridge integration for async ingestion
