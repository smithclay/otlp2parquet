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

### High-Level Data Flow

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

### Integrated Write Flow (Detailed)

The S3 Tables integration uses an **atomic write-and-commit** pattern that ensures ACID guarantees:

```
Lambda Handler Invocation
    │
    ▼
IcebergWriter::write_and_commit()
    │
    ├─→ Step 1: LoadTable/CreateTable
    │   │   (REST API call to S3 Tables catalog)
    │   │   GET /v1/namespaces/otel/tables/logs
    │   │   └─→ Returns: { location: "s3://bucket/warehouse/otel/logs", schema: {...} }
    │   │   OR
    │   │   POST /v1/namespaces/otel/tables (if table doesn't exist)
    │   │   └─→ Creates table from Arrow schema
    │   └─→ Result: Table metadata with warehouse location
    │
    ├─→ Step 2: Generate Warehouse Path
    │   │   format: {warehouse}/data/{timestamp}-{uuid}.parquet
    │   └─→ Example: s3://bucket/warehouse/otel/logs/data/1730934123456789-a1b2c3.parquet
    │
    ├─→ Step 3: Write Parquet File
    │   │   (OpenDAL → S3 API)
    │   │   Arrow RecordBatch → Parquet writer → S3 PutObject
    │   └─→ Result: ParquetWriteResult { path, row_count, file_size, column_sizes }
    │
    ├─→ Step 4: Build DataFile Metadata
    │   │   Convert ParquetWriteResult → Iceberg DataFile
    │   └─→ DataFile { file_path, record_count, file_size_in_bytes, column_sizes, ... }
    │
    └─→ Step 5: CommitTransaction
        │   (REST API call to S3 Tables catalog)
        │   POST /v1/namespaces/otel/tables/logs/transactions/commit
        │   Body: { "updates": [{ "action": "append", "data-file": {...} }] }
        │   └─→ Result: New snapshot committed atomically
        │
        └─→ Success: File is now visible to queries
            Failure: Log warning, file written but not cataloged (can be fixed later)
```

**Key Design Decisions:**

1. **Single Atomic Operation**: All steps happen in one Lambda invocation, ensuring the file write and catalog commit are closely coupled.

2. **On-Demand Table Creation**: Tables are created lazily when first data arrives, using Arrow schemas from `otlp2parquet-core`.

3. **Warehouse Path Generation**: Files are written directly to the warehouse location (not a staging area), with deterministic naming using timestamp+UUID.

4. **Warn-and-Succeed Pattern**: Catalog commit failures log warnings but don't fail the Lambda invocation, ensuring ingestion resilience.

5. **OpenDAL Abstraction**: Storage layer uses OpenDAL, allowing future support for other backends (R2, GCS, Azure) without changing Iceberg logic.

### Components

**S3 Table Bucket**
- Purpose-built AWS service for Apache Iceberg tables
- Stores both Parquet data files AND Iceberg catalog metadata in a single bucket
- Provides automatic table maintenance (compaction, snapshot cleanup, orphan file removal)
- No separate S3 bucket or Glue Catalog needed - everything is self-contained
- REST catalog endpoint: `https://s3tables.{region}.amazonaws.com/iceberg`
- Warehouse location: `arn:aws:s3tables:{region}:{account}:bucket/{name}`

**Lambda Function**
- Runtime: `provided.al2023` (custom runtime for Rust binary)
- Default: arm64 architecture (20% better price/performance vs x86_64)
- Binary size: ~5MB uncompressed, ~2MB compressed
- Cold start: 100-200ms typical
- Memory: 512MB (adjustable based on batch size, affects CPU allocation)
- Timeout: 60 seconds (increase for larger batches)
- Triggered manually for testing (extend with API Gateway for production)

**IcebergWriter Module** (Rust code)
- `otlp2parquet-storage/src/iceberg/writer.rs` - Main orchestration logic
- `IcebergCatalog` - REST client for S3 Tables catalog API
- `OpenDAL` - Storage abstraction for S3/R2/filesystem writes
- `ParquetWriter` - Arrow to Parquet conversion with column statistics

**IAM Role**
- CloudWatch Logs permissions (function logging)
- S3 Tables API permissions (LoadTable, CreateTable, CommitTransaction)
- S3 object permissions (GetObject, PutObject on warehouse bucket)

## Configuration

### Operating Modes

The Lambda function supports **dual-mode operation**, automatically detecting which mode to use based on environment variables:

#### Mode 1: S3 Tables with Iceberg (Recommended)

**Detection:** Presence of `OTLP2PARQUET_ICEBERG_REST_URI` environment variable

**Configuration:**
```bash
OTLP2PARQUET_ICEBERG_REST_URI=https://s3tables.us-west-2.amazonaws.com/iceberg
OTLP2PARQUET_ICEBERG_WAREHOUSE=arn:aws:s3tables:us-west-2:123456789012:bucket/otlp2parquet
OTLP2PARQUET_ICEBERG_NAMESPACE=otel
```

**Behavior:**
- Uses `IcebergWriter::write_and_commit()` for atomic write-and-commit operations
- Tables created on-demand from Arrow schemas when first data arrives
- Files written directly to warehouse location (no staging)
- Catalog commits tracked per invocation
- Failures logged but don't block ingestion (warn-and-succeed pattern)

**Benefits:**
- ACID transactions ensure data consistency
- Schema evolution supported (add/remove/rename columns)
- Faster queries via catalog metadata (file pruning, column statistics)
- Time travel queries to historical snapshots
- Multi-engine support (DuckDB, Spark, Athena, Trino)

**When to use:**
- Production deployments requiring data consistency
- Multi-user query scenarios with concurrent reads/writes
- Long-term data retention with schema evolution
- Integration with modern data lake ecosystems

#### Mode 2: Plain S3 without Iceberg (Legacy)

**Detection:** Absence of `OTLP2PARQUET_ICEBERG_REST_URI` variable

**Configuration:**
```bash
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_BUCKET=my-parquet-bucket
OTLP2PARQUET_S3_REGION=us-west-2
```

**Behavior:**
- Uses `ParquetWriter` for direct S3 writes
- Files written to partitioned directories: `{signal}/{service}/year=YYYY/month=MM/day=DD/hour=HH/`
- No catalog integration
- No metadata management

**Benefits:**
- Simple S3 storage without catalog overhead
- Direct file access for custom tooling
- Lower complexity for simple use cases
- Compatible with any Parquet reader

**When to use:**
- Simple archival scenarios
- Custom query pipelines that don't need Iceberg
- Cost optimization for write-once, read-rarely patterns
- Development and testing environments

### CloudFormation Parameters

When deploying the stack, you can customize these parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `LambdaCodeBucket` | S3 bucket containing Lambda zip | (set by deploy script) |
| `LambdaCodeKey` | S3 key for Lambda zip file | `lambda/otlp2parquet-lambda.zip` |
| `LambdaArchitecture` | Lambda architecture (arm64/x86_64) | `arm64` |

### Environment Variables Reference

**S3 Tables Mode (default in template):**

| Variable | Value | Purpose |
|----------|-------|---------|
| `OTLP2PARQUET_ICEBERG_REST_URI` | `https://s3tables.<region>.amazonaws.com/iceberg` | S3 Tables REST catalog endpoint |
| `OTLP2PARQUET_ICEBERG_WAREHOUSE` | `arn:aws:s3tables:...` | Table bucket ARN (warehouse location) |
| `OTLP2PARQUET_ICEBERG_NAMESPACE` | `otel` | Namespace for tables |
| `RUST_LOG` | `info` | Rust logging level (debug/info/warn/error) |

**Plain S3 Mode (comment out Iceberg vars, uncomment these):**

| Variable | Value | Purpose |
|----------|-------|---------|
| `OTLP2PARQUET_STORAGE_BACKEND` | `s3` | Storage backend type |
| `OTLP2PARQUET_S3_BUCKET` | `my-parquet-bucket` | S3 bucket name |
| `OTLP2PARQUET_S3_REGION` | `us-west-2` | AWS region |

### Tables Created (S3 Tables Mode Only)

The Lambda automatically creates these Iceberg tables on first write:

- `otel.logs` - Log records
- `otel.traces` - Trace spans
- `otel.metrics_gauge` - Gauge metrics
- `otel.metrics_sum` - Sum/counter metrics
- `otel.metrics_histogram` - Histogram metrics
- `otel.metrics_exponential_histogram` - Exponential histogram metrics
- `otel.metrics_summary` - Summary metrics

**Table Schema:** Generated from Arrow schemas in `otlp2parquet-core` crate, following ClickHouse OTel exporter naming conventions.

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

### IAM Permissions (Detailed)

The Lambda execution role requires two distinct sets of permissions:

#### 1. S3 Tables Catalog API Permissions

Required for Iceberg metadata operations (LoadTable, CreateTable, CommitTransaction):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3TablesCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "s3tables:GetTableMetadataLocation",
        "s3tables:CreateTable",
        "s3tables:UpdateTableMetadataLocation",
        "s3tables:GetTableBucket",
        "s3tables:GetNamespace",
        "s3tables:CreateNamespace"
      ],
      "Resource": [
        "arn:aws:s3tables:REGION:ACCOUNT:bucket/BUCKET_NAME",
        "arn:aws:s3tables:REGION:ACCOUNT:bucket/BUCKET_NAME/namespace/*"
      ]
    }
  ]
}
```

**Action Descriptions:**
- `GetTableMetadataLocation` - Load existing table metadata for writes
- `CreateTable` - Create tables on-demand from Arrow schemas
- `UpdateTableMetadataLocation` - Commit new snapshots to catalog
- `GetTableBucket` - Access table bucket configuration
- `GetNamespace` - Read namespace metadata
- `CreateNamespace` - Create namespaces if they don't exist (optional)

#### 2. S3 Object Data Permissions

Required for reading/writing Parquet files in the warehouse location:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3TablesDataAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::WAREHOUSE_BUCKET_NAME/*",
        "arn:aws:s3:::WAREHOUSE_BUCKET_NAME"
      ]
    }
  ]
}
```

**Action Descriptions:**
- `PutObject` - Write Parquet files to warehouse location
- `GetObject` - Read existing files (for metadata extraction)
- `DeleteObject` - Remove orphaned files during maintenance (optional)
- `ListBucket` - List warehouse contents (for debugging)

#### 3. CloudWatch Logs Permissions

Required for Lambda function logging:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudWatchLogsAccess",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:REGION:ACCOUNT:log-group:/aws/lambda/*"
    }
  ]
}
```

### IAM Best Practices

The provided IAM role follows least-privilege principles:
- **Scoped resources**: Permissions limited to specific table bucket ARN
- **No wildcard actions**: Only required S3 Tables API calls allowed
- **Separate policies**: S3 Tables catalog vs S3 objects vs CloudWatch logs
- **Minimal DeleteObject**: Only included if table maintenance is required

**Production recommendations:**
- Use IAM roles per environment (dev/staging/prod) with separate table buckets
- Add resource tags for cost allocation: `Environment`, `Team`, `Project`
- Implement SCPs for organization-wide guardrails (e.g., prevent public access)
- Enable CloudTrail logging for S3 Tables API calls for audit trail
- Use IAM policy conditions to restrict access by time/IP if needed:
  ```json
  "Condition": {
    "IpAddress": {
      "aws:SourceIp": ["10.0.0.0/8"]
    }
  }
  ```

**Troubleshooting IAM issues:**
- Check CloudWatch logs for `AccessDenied` errors with specific action names
- Use IAM Policy Simulator to test permissions before deployment
- Verify table bucket ARN matches exactly in IAM policy Resource field
- Ensure Lambda execution role has `AssumeRole` trust policy for `lambda.amazonaws.com`

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
