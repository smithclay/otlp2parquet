# AWS Lambda Setup

This guide shows how to deploy `otlp2parquet` to AWS Lambda for serverless OTLP ingestion.

The provided CloudFormation template deploys a complete ingestion stack in about 3 minutes.

## Use Cases

This setup is a good fit for:
*   Integration with the AWS ecosystem (S3, Glue, S3 Tables).
*   Event-driven or batch OTLP ingestion.
*   Cost-sensitive, pay-per-use deployments with no idle costs.

## Prerequisites

*   An AWS account.
*   The AWS CLI, configured with credentials.

## Quick Start

1.  **Download Template:**
    ```bash
    curl -O https://raw.githubusercontent.com/smithclay/otlp2parquet/main/deploy/aws-lambda.yaml
    ```

2.  **Deploy Stack:**
    ```bash
    aws cloudformation deploy \
      --template-file aws-lambda.yaml \
      --stack-name otlp2parquet \
      --region us-west-2 \
      --capabilities CAPABILITY_NAMED_IAM
    ```

3.  **Get Endpoint URL:**
    ```bash
    aws cloudformation describe-stacks \
      --stack-name otlp2parquet \
      --region us-west-2 \
      --query 'Stacks[0].Outputs[?OutputKey==`FunctionUrl`].OutputValue' \
      --output text
    ```

4.  **Send Test Data:**
    ```bash
    curl -X POST <your-lambda-url>/v1/logs \
      -H "Content-Type: application/json" \
      -d @testdata/log.json
    ```

5.  **Query Data:**
    ```bash
    # With DuckDB and the Iceberg extension
    duckdb -c "
      INSTALL iceberg; LOAD iceberg;
      CREATE SECRET (TYPE S3, REGION 'us-west-2');

      SELECT Timestamp, ServiceName, Body
      FROM iceberg_scan('glue://otel.logs', aws_region='us-west-2');
    "
    ```

## Configuration

The Lambda function uses AWS S3 Tables for Apache Iceberg integration via ARN-based configuration.

### S3 Tables ARN Configuration

The CloudFormation template automatically sets the `OTLP2PARQUET_ICEBERG_BUCKET_ARN` environment variable with the created S3 Tables bucket ARN:

```bash
OTLP2PARQUET_ICEBERG_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/otlp2parquet-stack-123456789012
```

This ARN format enables native S3 Tables catalog support:
- `arn:aws:s3tables` - S3 Tables service
- `region` - AWS region (e.g., `us-west-2`)
- `account-id` - Your AWS account ID
- `bucket/bucket-name` - S3 Tables bucket name

### Template Parameters

Customize deployment by passing `--parameter-overrides` to `aws cloudformation deploy`:

```bash
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name otlp2parquet \
  --region us-west-2 \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides LambdaArchitecture=x86_64
```

**Available parameters:**
- `LambdaArchitecture`: `arm64` (default, recommended) or `x86_64`
- `S3TablesBucketName`: Custom bucket name (default: auto-generated)
- `LambdaCodeBucket`: S3 bucket containing deployment package
- `LambdaCodeKey`: S3 key for the Lambda zip file (default: `lambda/otlp2parquet-lambda.zip`)

For a full list of environment variables, see the [Configuration Guide](../concepts/configuration.md).

### Required IAM Permissions

The Lambda execution role requires these permissions for S3 Tables operations:

**S3 Tables Catalog Operations:**
- `s3tables:GetTable` - Read table metadata
- `s3tables:GetTableMetadataLocation` - Get metadata file location
- `s3tables:CreateTable` - Auto-create tables on first write
- `s3tables:UpdateTableMetadataLocation` - Commit new snapshots
- `s3tables:GetNamespace` - Read namespace metadata
- `s3tables:CreateNamespace` - Auto-create namespace if needed

**S3 Data File Operations:**
- `s3:PutObject` - Write Parquet data files
- `s3:GetObject` - Read Parquet data files
- `s3:DeleteObject` - Cleanup old files (compaction)
- `s3:ListBucket` - List objects in bucket

The CloudFormation template automatically creates a role with these permissions scoped to the S3 Tables bucket.

## Common Tasks

*   **Viewing Logs**: `aws logs tail /aws/lambda/otlp2parquet-ingest --follow --region us-west-2`
*   **Cleaning Up**: `aws cloudformation delete-stack --stack-name otlp2parquet --region us-west-2` (Note: The S3 bucket and Glue database are retained by default).

## Production Considerations

*   **Authentication**: The default Lambda function URL is public. For production, add an API Gateway to handle authentication (e.g., with IAM or API keys).
*   **Batching**: Use an OTel Collector to batch data before sending it to Lambda. This significantly reduces cost and improves performance by creating larger Parquet files.
*   **Monitoring**: Track Lambda invocations, duration, and errors in CloudWatch. Set up billing alerts for estimated charges in the AWS/Billing namespace. Enable CloudTrail logging for S3 Tables API calls for a full audit trail.
*   **IAM**: The default IAM role provides least-privilege access. Review and tighten permissions as needed for your environment. Scope permissions to specific resources and use separate roles for different environments.
*   **Cold Starts**: The first invocation of a Rust Lambda can take 5-10 seconds. Configure your OTLP exporter with a retry policy to handle this. For high-throughput use cases, consider provisioned concurrency.

## Troubleshooting

### Common S3 Tables Issues

**"Access Denied" errors in CloudWatch logs:**
- **Cause**: Missing IAM permissions for S3 Tables operations
- **Solution**: Verify the Lambda role has all required permissions listed above. Check:
  ```bash
  aws iam get-role-policy \
    --role-name <your-lambda-role-name> \
    --policy-name S3TablesAccess
  ```
- **Common missing permissions**: `s3tables:UpdateTableMetadataLocation`, `s3tables:CreateTable`

**Tables not appearing in S3 Tables console:**
- **Cause**: Tables are created on first write per signal type
- **Solution**: Send test data first, then check the S3 Tables console or use:
  ```bash
  aws s3tables list-tables \
    --table-bucket-arn arn:aws:s3tables:region:account:bucket/bucket-name \
    --namespace otel
  ```

**Lambda timeout errors:**
- **Cause**: Insufficient memory/CPU or processing large batches
- **Solution**: Increase `MemorySize` in the CloudFormation template (more memory = faster CPU):
  ```bash
  --parameter-overrides MemorySize=1024
  ```

**S3 Tables API throttling:**
- **Cause**: Exceeding S3 Tables API rate limits
- **Solution**:
  - Implement batching upstream (OTel Collector)
  - Request quota increase via AWS Support
  - Add exponential backoff in OTLP exporter

**Parquet files not visible:**
- **Cause**: Catalog commit failures (check CloudWatch logs for warnings)
- **Solution**: Parquet files are still written even if catalog fails. Files will be in S3 but not queryable via Iceberg until catalog is fixed. Check logs for specific error.

## Deeper Dive: How It Works

<details>
<summary><strong>Architecture and Write Process</strong></summary>

*   **Flow**: `OTLP Client` → `Lambda` → `icepick` → `S3 Tables Catalog + S3 Storage`
*   **Lambda Function**: A ~5MB Rust binary that handles OTLP HTTP requests
*   **icepick Integration**: Uses the [icepick](https://crates.io/crates/icepick) library for Parquet writing and catalog operations
*   **S3 Tables**: AWS-managed Apache Iceberg catalog service
*   **Write Process**: The integration uses an atomic write-and-commit pattern via icepick:
    1.  **Convert to Arrow**: OTLP data is converted to Arrow RecordBatch with ClickHouse-compatible schema
    2.  **Write via icepick**: The icepick writer handles both Parquet file creation and catalog operations
    3.  **Write Parquet File**: File is written to S3 storage in the table's location
    4.  **Commit to Catalog**: Metadata is committed to S3 Tables catalog atomically
    5.  **Warn-and-Succeed**: If catalog operations fail, warnings are logged but Parquet files are still written (data durability over catalog consistency)

</details>

## Next Steps

- [Sending Data Guide](../guides/sending-data.md)
- [Schema Reference](../concepts/schema.md)
- [Local Development with SAM CLI](../guides/lambda-local-development.md)
