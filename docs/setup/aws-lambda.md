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

The Lambda function is configured via parameters in the CloudFormation template. The template defaults to using AWS S3 Tables (Apache Iceberg), but can be configured for plain S3 storage.

To customize, pass `--parameter-overrides` to the `aws cloudformation deploy` command.
```bash
aws cloudformation deploy \
  ...
  --parameter-overrides LogLevel=debug IcebergNamespace=production
```

**Available parameters:**
- `Architecture`: `arm64` (default) or `x86_64`
- `BinaryVersion`: GitHub release tag (default: `latest`)
- `LogLevel`: `info` (default), `debug`, `trace`, `warn`, `error`
- `IcebergNamespace`: Glue database name (default: `otel`)

For a full list of settings, see the [Configuration Guide](../concepts/configuration.md).

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

*   **Lambda timeout errors**: Increase `MemorySize` (more memory = faster CPU) or `Timeout` in `aws-lambda.yaml`.
*   **"AccessDenied" errors**: Check CloudWatch logs. Common causes include the IAM role missing permissions for Glue, S3 Tables, or Lake Formation. Use the IAM Policy Simulator to test permissions before deploying.
*   **Tables not appearing**: Tables are created on the first write of a given signal type. Send test data first, then check Glue or Athena.
*   **Throttling**: If you hit S3 Tables API limits, request a quota increase via AWS Support and implement exponential backoff in your OTLP exporter.

## Deeper Dive: How It Works

<details>
<summary><strong>Architecture and Write Process</strong></summary>

*   **Flow**: `OTLP Client` → `Lambda` → `S3 Tables` → `Glue Catalog`
*   **Lambda Function**: A ~5MB Rust binary that handles OTLP HTTP requests.
*   **S3 Tables**: An AWS service for Iceberg tables that manages storage and metadata.
*   **Write Process**: The integration uses an atomic write-and-commit pattern.
    1.  **Load or Create Table**: The function calls the S3 Tables REST catalog to get metadata for the target table, creating it if it doesn't exist.
    2.  **Write Parquet File**: The function converts the OTLP data to an Arrow RecordBatch and writes a Parquet file directly to the table's warehouse location in S3.
    3.  **Commit Transaction**: The function commits the new file's path and statistics to the Iceberg catalog. The new data is now atomically available to all query engines.

</details>

## Next Steps

- [Sending Data Guide](../guides/sending-data.md)
- [Schema Reference](../concepts/schema.md)
- [Local Development with SAM CLI](../guides/lambda-local-development.md)
