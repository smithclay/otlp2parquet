# Catalog Types Reference

Detailed guide for each Apache Iceberg catalog type supported by otlp2parquet.

## Overview

otlp2parquet supports multiple catalog backends for managing Iceberg table metadata:

| Catalog Type | Platforms | Use Case | Complexity |
|--------------|-----------|----------|------------|
| **Plain Parquet** | All | Simple object storage, no catalog | Low |
| **S3 Tables** | Lambda only | AWS-managed Iceberg with ARN config | Low |
| **R2 Data Catalog** | Cloudflare Workers only | Edge-native Iceberg on WASM | Medium |
| **Nessie** | Server only | Git-like version control for data | Medium |
| **AWS Glue** | Server only | AWS-managed REST catalog | Medium |

---

## Plain Parquet (All Platforms)

**No catalog** - Writes Parquet files directly to object storage without Iceberg metadata.

### When to Use

- Simplest setup with minimal configuration
- No need for ACID transactions or schema evolution
- Query engines that can read Parquet directly (DuckDB, Athena, Trino)
- Cost-sensitive deployments (no catalog API overhead)
- Prototyping and development

### Configuration

No Iceberg configuration needed. Just configure storage backend:

**Lambda:**
```yaml
Environment:
  Variables:
    OTLP2PARQUET_STORAGE_BACKEND: s3
    OTLP2PARQUET_S3_BUCKET: my-logs
    OTLP2PARQUET_S3_REGION: us-west-2
```

**Cloudflare Workers:**
```toml
[[r2_buckets]]
binding = "LOGS_BUCKET"
bucket_name = "otlp-logs"
```

**Server:**
```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-logs"
region = "us-west-2"
```

### Querying

Use standard Parquet readers:

```sql
-- DuckDB
SELECT * FROM parquet_scan('s3://my-logs/logs/**/*.parquet');

-- Athena
SELECT * FROM "my-logs"."logs";
```

### Limitations

- No ACID transactions
- No schema evolution
- No time travel queries
- Manual partition management
- Potential for inconsistent reads during writes

### Performance Characteristics

- **Write latency:** Lowest (no catalog overhead)
- **Query performance:** Good (direct Parquet reads)
- **Cost:** Lowest (no catalog API calls)

---

## S3 Tables (Lambda - Recommended)

**AWS-managed Iceberg catalog** with simplified ARN-based configuration. Only available on AWS Lambda (native AWS SDK required).

### When to Use

- AWS Lambda deployments
- Need ACID transactions without managing infrastructure
- Want automatic compaction and optimization
- CloudFormation-based deployments
- Integration with AWS analytics services (Athena, EMR, Glue)

### Setup

#### 1. Create S3 Tables Bucket (CloudFormation)

```yaml
S3TablesBucket:
  Type: AWS::S3Tables::TableBucket
  Properties:
    TableBucketName: otlp-data

S3TablesNamespace:
  Type: AWS::S3Tables::TableBucketNamespace
  Properties:
    TableBucketARN: !GetAtt S3TablesBucket.Arn
    Namespace: otel
```

#### 2. Configure Lambda Environment

```yaml
Environment:
  Variables:
    OTLP2PARQUET_ICEBERG_BUCKET_ARN: !GetAtt S3TablesBucket.Arn
```

The ARN format is: `arn:aws:s3tables:region:account-id:bucket/bucket-name`

**Example:**
```
arn:aws:s3tables:us-west-2:123456789012:bucket/otlp-data
```

### Required IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3tables:GetTable",
        "s3tables:GetTableMetadataLocation",
        "s3tables:CreateTable",
        "s3tables:UpdateTableMetadataLocation",
        "s3tables:GetNamespace",
        "s3tables:CreateNamespace"
      ],
      "Resource": [
        "arn:aws:s3tables:region:account:bucket/bucket-name",
        "arn:aws:s3tables:region:account:bucket/bucket-name/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3tables:region:account:bucket/bucket-name",
        "arn:aws:s3tables:region:account:bucket/bucket-name/*"
      ]
    }
  ]
}
```

### Querying

```sql
-- DuckDB with Iceberg extension
INSTALL iceberg; LOAD iceberg;
CREATE SECRET (TYPE S3, REGION 'us-west-2');

SELECT * FROM iceberg_scan('glue://otel.logs', aws_region='us-west-2');
```

### Features

- ✅ ACID transactions
- ✅ Schema evolution
- ✅ Time travel queries
- ✅ Automatic compaction (managed by AWS)
- ✅ Partition evolution
- ✅ CloudFormation support
- ✅ Native AWS integration

### Limitations

- ❌ AWS Lambda only (not WASM/Cloudflare)
- ❌ Requires AWS account and IAM permissions
- ❌ S3 Tables API rate limits apply

### Troubleshooting

**"Access Denied" errors:**
```bash
# Check IAM role has permissions
aws iam get-role-policy --role-name my-lambda-role --policy-name S3TablesAccess

# Verify bucket exists
aws s3tables get-table-bucket --table-bucket-arn arn:aws:s3tables:...
```

**Tables not appearing:**
```bash
# List tables (created on first write)
aws s3tables list-tables \
  --table-bucket-arn arn:aws:s3tables:region:account:bucket/name \
  --namespace otel
```

---

## R2 Data Catalog (Cloudflare Workers)

**Edge-native Iceberg catalog** for Cloudflare Workers. WASM-compatible catalog implementation for R2 storage.

### When to Use

- Cloudflare Workers deployments
- Need Iceberg features at the edge
- Global distribution with low latency
- Zero egress fees (R2)
- WASM binary size constraints acceptable

### Setup

#### 1. Create R2 Bucket with Data Catalog

Via Cloudflare dashboard or Wrangler CLI:

```bash
# Create R2 bucket
wrangler r2 bucket create otlp-data

# Enable Data Catalog (via dashboard)
# Go to R2 > Buckets > otlp-data > Settings > Enable Data Catalog
```

#### 2. Configure wrangler.toml

```toml
[[r2_buckets]]
binding = "OTLP_BUCKET"
bucket_name = "otlp-data"

[vars]
OTLP2PARQUET_CATALOG_TYPE = "r2"
```

### Features

- ✅ ACID transactions at the edge
- ✅ Schema evolution
- ✅ WASM-compatible (no native dependencies)
- ✅ Zero egress fees
- ✅ Global distribution
- ✅ Simple configuration

### Limitations

- ❌ Cloudflare Workers only
- ❌ Requires R2 Data Catalog feature (preview)
- ❌ Limited query engine support vs S3 Tables

### Querying

Query R2 Data Catalog tables using Iceberg-compatible engines:

```sql
-- DuckDB (via S3-compatible endpoint)
SELECT * FROM iceberg_scan('...');
```

### Troubleshooting

**Catalog operations failing:**
```bash
# Check logs
wrangler tail

# Verify catalog enabled
wrangler r2 bucket info otlp-data
```

**Binding mismatch:**
- Verify `binding` in wrangler.toml matches code expectations
- Check `OTLP2PARQUET_CATALOG_TYPE="r2"` is set

---

## Nessie (Server - Docker/Self-Hosted)

**Git-like version control for data** via Iceberg REST catalog. Best for development and testing.

### When to Use

- Self-hosted server deployments
- Development and testing environments
- Need version control and branching for data
- Docker Compose setups
- Local development

### Setup

#### 1. Start Nessie with Docker Compose

```yaml
version: '3.8'
services:
  nessie:
    image: projectnessie/nessie:latest
    ports:
      - "19120:19120"
    environment:
      - QUARKUS_HTTP_PORT=19120
```

#### 2. Configure otlp2parquet

```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-data-bucket"
region = "us-west-2"
endpoint = "http://minio:9000"  # For local MinIO

[iceberg]
rest_uri = "http://nessie:19120/api/v1"
warehouse = "warehouse"
namespace = "otel"
```

Or via environment variables:

```bash
OTLP2PARQUET_ICEBERG_REST_URI=http://nessie:19120/api/v1
OTLP2PARQUET_ICEBERG_WAREHOUSE=warehouse
OTLP2PARQUET_ICEBERG_NAMESPACE=otel
```

### Features

- ✅ Git-like branching and tagging
- ✅ ACID transactions
- ✅ Schema evolution
- ✅ Time travel
- ✅ Easy Docker deployment
- ✅ REST catalog API

### Limitations

- ❌ Server platform only (not Lambda/WASM)
- ❌ Requires separate Nessie deployment
- ❌ Not managed service (self-hosted)

### Querying

```sql
-- DuckDB
INSTALL iceberg; LOAD iceberg;

SELECT * FROM iceberg_scan(
  'http://nessie:19120/api/v1/trees/main/tables/otel.logs'
);
```

### Troubleshooting

**Connection errors:**
```bash
# Test Nessie REST endpoint
curl http://nessie:19120/api/v1/config

# Check Docker network connectivity
docker network inspect otlp2parquet_default
```

---

## AWS Glue (Server - AWS Managed)

**AWS-managed Iceberg REST catalog** for self-hosted server deployments. Alternative to S3 Tables for server use cases.

### When to Use

- Self-hosted servers on AWS (not Lambda)
- Integration with AWS Glue Data Catalog
- Need Lake Formation permissions
- Existing Glue catalog infrastructure

### Setup

#### 1. Create Glue Database

```bash
aws glue create-database --database-input '{"Name":"otel"}'
```

#### 2. Configure otlp2parquet

```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-data-bucket"
region = "us-west-2"

[iceberg]
rest_uri = "https://glue.us-west-2.amazonaws.com/iceberg"
warehouse = "123456789012"  # AWS account ID
namespace = "otel"
data_location = "s3://my-data-bucket"
```

Or via environment variables:

```bash
OTLP2PARQUET_ICEBERG_REST_URI=https://glue.us-west-2.amazonaws.com/iceberg
OTLP2PARQUET_ICEBERG_WAREHOUSE=123456789012
OTLP2PARQUET_ICEBERG_NAMESPACE=otel
OTLP2PARQUET_ICEBERG_DATA_LOCATION=s3://my-data-bucket
```

### Required IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "lakeformation:GetDataAccess",
      "Resource": "*"
    }
  ]
}
```

### Features

- ✅ AWS-managed service
- ✅ Integration with Glue Data Catalog
- ✅ Lake Formation support
- ✅ ACID transactions
- ✅ Schema evolution

### Limitations

- ❌ Server platform only
- ❌ Requires AWS account
- ❌ More complex than S3 Tables for Lambda

### Troubleshooting

**Permission errors:**
```bash
# Check Lake Formation permissions
aws lakeformation get-data-lake-settings

# Verify IAM role
aws iam get-role-policy --role-name my-server-role --policy-name GlueAccess
```

---

## Comparison Matrix

| Feature | Plain Parquet | S3 Tables | R2 Catalog | Nessie | Glue |
|---------|---------------|-----------|------------|--------|------|
| **ACID Transactions** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **Schema Evolution** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **Time Travel** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **Lambda Support** | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Cloudflare WASM** | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Server Support** | ✅ | ❌ | ❌ | ✅ | ✅ |
| **Managed Service** | N/A | ✅ | ✅ | ❌ | ✅ |
| **Setup Complexity** | Low | Low | Medium | Medium | Medium |
| **Cost** | Lowest | Medium | Low | Low | Medium |

---

## Related Documentation

- [Environment Variables Reference](./environment-variables.md) - All configuration variables
- [AWS Lambda Setup](../setup/aws-lambda.md) - S3 Tables configuration
- [Cloudflare Workers Setup](../setup/cloudflare.md) - R2 Data Catalog configuration
- [Storage Concepts](../concepts/storage.md) - Storage abstraction details
