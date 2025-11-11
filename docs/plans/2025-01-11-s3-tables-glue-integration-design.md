# AWS Glue Iceberg REST Integration for S3 Tables

**Date:** 2025-01-11
**Status:** Proposed
**Authors:** Claude + User

## Problem Statement

Our Lambda implementation successfully writes Parquet files to S3 Tables and authenticates via AWS SigV4, but catalog commits fail with HTTP 400 "invalid_metadata":

```
WARN Catalog commit failed for table 'logs': HTTP 400 error from catalog:
{"error":{"code":400,"message":"The specified metadata is not valid","type":"invalid_metadata"}}
```

**Root Cause:** AWS S3 Tables REST endpoint does not support snapshot management operations (add-snapshot, set-snapshot-ref). Per AWS documentation:

> "The endpoint only supports standard table metadata operations. For table maintenance, such as snapshot management and compaction, use S3 Tables maintenance API operations."

Our manifest-based commits require snapshot management, which S3 Tables REST explicitly does not support.

## Solution Overview

**Use AWS Glue Iceberg REST endpoint** - the same pattern AWS Firehose uses for S3 Tables integration.

Instead of calling S3 Tables REST API directly, we route through AWS Glue Data Catalog:

```
Lambda → Glue Iceberg REST → Glue Data Catalog → S3 Tables
         (standard Iceberg)   (metadata/snapshots) (Parquet storage)
```

**Key Insight:** AWS Firehose uses `glue:UpdateTable` to commit to S3 Tables, NOT S3 Tables native APIs. We should follow the same pattern.

## Architecture

### Component Roles

| Component | Role |
|-----------|------|
| **AWS Glue Data Catalog** | Iceberg catalog (stores table metadata, snapshots, schemas) |
| **Glue Iceberg REST endpoint** | `https://glue.<region>.amazonaws.com/iceberg` - Full Iceberg REST spec |
| **S3 Tables** | Storage backend for Parquet data files with automatic optimization |
| **Lake Formation** | Permission layer for fine-grained access control |

### Data Flow

1. Lambda receives OTLP data
2. Converts to Arrow RecordBatch
3. Writes Parquet file to S3 Tables bucket
4. Generates Iceberg manifest files (statistics)
5. Calls Glue Iceberg REST `updateTable` with add-snapshot
6. Glue commits snapshot metadata to Data Catalog
7. Data immediately queryable via DuckDB/Spark/Athena

### Why This Works

- **Full Iceberg REST support**: Glue implements complete spec including snapshot management
- **Proven at scale**: Same pattern AWS Firehose uses for streaming ingestion
- **Automatic conflict resolution**: Glue handles concurrent writes with optimistic locking
- **S3 Tables benefits preserved**: Automatic compaction, snapshot management, optimization
- **Standard Iceberg**: Works with any Iceberg-compatible query engine

## Setup Requirements

### One-Time AWS Setup (Per S3 Tables Bucket)

#### 1. Integrate S3 Tables with Glue Data Catalog

```bash
# Via AWS Console:
# S3 Tables → Table Bucket → Actions → Integrate with AWS services

# Or via CLI:
aws s3tables create-table-bucket-catalog \
  --table-bucket-arn arn:aws:s3tables:us-west-2:123456789012:bucket/otlp2parquet \
  --catalog-type GLUE \
  --region us-west-2
```

#### 2. Enable Lake Formation External Access

```bash
# Lake Formation Console → Data catalog settings
# Enable: "Allow external engines to access data in Amazon S3 locations with full table access"

# Or via CLI:
aws lakeformation update-data-lake-settings \
  --data-lake-settings '{
    "DataLakeAdmins": [...],
    "CreateDatabaseDefaultPermissions": [...],
    "CreateTableDefaultPermissions": [...],
    "ExternalDataFilteringAllowList": [
      {"DataLakePrincipal": {"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/otlp2parquet-lambda-role"}}
    ],
    "AuthorizedSessionTagValueList": []
  }' \
  --region us-west-2
```

#### 3. Create Namespace in S3 Tables

```bash
# Namespace = database in Glue terminology
aws s3tables create-namespace \
  --table-bucket-arn arn:aws:s3tables:us-west-2:123456789012:bucket/otlp2parquet \
  --namespace otel \
  --region us-west-2

# Verify in Glue
aws glue get-database --name otel --region us-west-2
```

#### 4. Grant Lake Formation Permissions

```bash
# Grant Lambda role permissions on database
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::123456789012:role/otlp2parquet-lambda-role \
  --resource Database={Name=otel} \
  --permissions CREATE_TABLE DESCRIBE \
  --region us-west-2

# Grant Lambda role permissions on tables (do this for each table: logs, traces, metrics_*)
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::123456789012:role/otlp2parquet-lambda-role \
  --resource Table={DatabaseName=otel,Name=logs} \
  --permissions SELECT INSERT ALTER DESCRIBE \
  --region us-west-2
```

### Lambda IAM Role Permissions

Add to Lambda execution role policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueDataCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:us-west-2:123456789012:catalog",
        "arn:aws:glue:us-west-2:123456789012:database/otel",
        "arn:aws:glue:us-west-2:123456789012:table/otel/*"
      ]
    },
    {
      "Sid": "LakeFormationAccess",
      "Effect": "Allow",
      "Action": [
        "lakeformation:GetDataAccess"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3TablesDataAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::otlp2parquet-data-*/*"
    }
  ]
}
```

### Lambda Environment Variables

```bash
ICEBERG_ENABLED=true
ICEBERG_REST_URI=https://glue.us-west-2.amazonaws.com/iceberg
ICEBERG_WAREHOUSE=123456789012:s3tablescatalog/otlp2parquet-otlp2parquet
ICEBERG_NAMESPACE=otel
AWS_REGION=us-west-2
```

**Important:** Warehouse format is `{account-id}:s3tablescatalog/{table-bucket-name}`, NOT an S3 URI!

## Code Changes

### Minimal Changes Required

The beauty of this approach: **almost zero code changes** needed!

#### 1. Update AwsSigV4HttpClient Signing Service

```rust
// File: crates/otlp2parquet-iceberg/src/aws.rs

impl AwsSigV4HttpClient {
    pub async fn new(region: &str) -> Result<Self> {
        let config = aws_config::from_env()
            .region(Region::new(region.to_string()))
            .load()
            .await;

        Ok(Self {
            client: ReqwestClient::new(),
            config,
            signing_service: "glue".to_string(), // Changed from "s3tables"
        })
    }
}
```

#### 2. Update Lambda Configuration Loading

```rust
// File: crates/otlp2parquet-lambda/src/lib.rs

// When initializing IcebergWriter
let iceberg_config = IcebergConfig {
    rest_uri: env::var("ICEBERG_REST_URI")
        .unwrap_or_else(|_| {
            format!("https://glue.{}.amazonaws.com/iceberg", region)
        }),
    warehouse: env::var("ICEBERG_WAREHOUSE")
        .expect("ICEBERG_WAREHOUSE required (format: account-id:s3tablescatalog/bucket-name)"),
    namespace: env::var("ICEBERG_NAMESPACE")
        .unwrap_or_else(|_| "otel".into()),
};

// Rest of initialization stays exactly the same
let http_client = AwsSigV4HttpClient::new(&s3.region).await?;
let catalog = Arc::new(
    IcebergCatalog::from_config(
        http_client,
        iceberg_config.rest_uri,
        namespace,
        tables,
    )
    .await?
);
let writer = Arc::new(IcebergWriter::new(catalog, storage, iceberg_config));
```

#### 3. CloudFormation Template Updates

```yaml
# File: examples/aws-lambda-s3-tables/template.yaml

Resources:
  LambdaFunction:
    Type: AWS::Lambda::Function
    Properties:
      Environment:
        Variables:
          ICEBERG_ENABLED: "true"
          ICEBERG_REST_URI: !Sub "https://glue.${AWS::Region}.amazonaws.com/iceberg"
          ICEBERG_WAREHOUSE: !Sub "${AWS::AccountId}:s3tablescatalog/${TableBucketName}"
          ICEBERG_NAMESPACE: "otel"
      Role: !GetAtt LambdaRole.Arn
      # ... rest of config

  LambdaRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument: # ...
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
      Policies:
        - PolicyName: GlueAndLakeFormationAccess
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action:
                  - glue:GetDatabase
                  - glue:GetTable
                  - glue:CreateTable
                  - glue:UpdateTable
                  - lakeformation:GetDataAccess
                Resource: "*"
              - Effect: Allow
                Action:
                  - s3:GetObject
                  - s3:PutObject
                  - s3:DeleteObject
                Resource: !Sub "arn:aws:s3:::${DataBucket}/*"
```

### What Does NOT Change

- **No new dependencies**: No aws-sdk-s3tables needed
- **No new modules**: No s3tables.rs, no metadata.rs
- **No retry logic**: Glue handles concurrency automatically
- **All existing Iceberg code**: catalog.rs, manifest.rs, writer.rs work as-is
- **Same authentication**: SigV4 client just changes signing service name
- **Same resilience**: Warn-and-succeed pattern preserved

## Deployment & Testing

### Deployment Steps

```bash
cd examples/aws-lambda-s3-tables

# 1. Run setup script (creates integration, namespace, permissions)
./setup-glue-integration.sh --region us-west-2

# 2. Deploy Lambda with updated config
./deploy.sh --region us-west-2

# 3. Verify integration
aws glue get-database --name otel --region us-west-2
```

### Testing Strategy

#### 1. Run Existing Tests

```bash
./test.sh --region us-west-2

# Should see in CloudWatch:
# ✓ "Successfully committed to catalog table 'logs' (snapshot: ...)"
# ✓ No "invalid_metadata" warnings
```

#### 2. Verify via Glue API

```bash
# Check table metadata
aws glue get-table \
  --database-name otel \
  --name logs \
  --region us-west-2

# Should show:
# - StorageDescriptor with S3 Tables location
# - Parameters with Iceberg metadata location
# - UpdateTime reflects recent commit
```

#### 3. Query with DuckDB

```bash
duckdb -c "
  INSTALL iceberg;
  LOAD iceberg;

  CREATE SECRET (
    TYPE S3,
    REGION 'us-west-2'
  );

  -- Query via Glue catalog
  SELECT
    COUNT(*) as total_logs,
    MAX(Timestamp) as latest_timestamp
  FROM iceberg_scan(
    'glue://otel.logs',
    aws_region='us-west-2'
  );
"
```

#### 4. Monitor CloudWatch Logs

```bash
aws logs tail /aws/lambda/otlp2parquet-ingest \
  --region us-west-2 \
  --follow \
  --filter-pattern "committed"
```

### Success Criteria

- ✅ Lambda invocations complete without errors
- ✅ CloudWatch shows "Successfully committed to catalog"
- ✅ Glue Data Catalog shows updated table snapshots
- ✅ DuckDB can query newly ingested data immediately
- ✅ No HTTP 400 "invalid_metadata" errors
- ✅ Data queryable within seconds of ingestion
- ✅ Concurrent Lambda invocations don't cause failures

### Rollback Plan

If issues arise during deployment:

1. Set `ICEBERG_ENABLED=false` in Lambda environment
2. Lambda continues writing Parquet files without catalog commits
3. Data still accessible via direct S3 queries
4. Investigate and fix Glue/Lake Formation permissions
5. Re-enable when ready

## Benefits

### Technical Benefits

| Benefit | Description |
|---------|-------------|
| **Zero new code** | Existing Iceberg REST implementation works as-is |
| **Standard Iceberg** | Portable to other catalogs (Glue, Nessie, Polaris) |
| **Proven pattern** | Same as AWS Firehose uses for S3 Tables |
| **Automatic concurrency** | Glue handles conflicts with optimistic locking |
| **Full Iceberg spec** | All operations supported (not limited subset) |
| **No binary bloat** | No new SDK dependencies needed |

### Operational Benefits

| Benefit | Description |
|---------|-------------|
| **Immediate queryability** | Data available in Glue seconds after ingestion |
| **S3 Tables optimization** | Automatic compaction, snapshot management preserved |
| **Lake Formation integration** | Fine-grained access control via LF |
| **AWS-native monitoring** | Glue/CloudWatch metrics and logs |
| **Multi-engine support** | Works with Athena, EMR, Spark, DuckDB, etc. |

### Development Benefits

| Benefit | Description |
|---------|-------------|
| **Simple testing** | Query with DuckDB/Athena immediately |
| **Clear separation** | Glue = catalog, S3 Tables = storage |
| **Easy debugging** | Glue API shows all table/snapshot metadata |
| **No custom retry logic** | Glue handles all concurrency edge cases |

## Comparison: S3 Tables Native vs Glue REST

| Aspect | S3 Tables Native API | Glue Iceberg REST |
|--------|---------------------|-------------------|
| **Code changes** | Major (new module, retry logic, metadata gen) | Minor (config + 1 line) |
| **Dependencies** | aws-sdk-s3tables (~200 crates) | None (reuse existing) |
| **Binary size** | +1-2 MB | No change |
| **Complexity** | High (manual metadata.json generation) | Low (standard REST) |
| **Concurrency** | Manual retry with backoff | Automatic (Glue handles) |
| **Portability** | AWS S3 Tables only | Any Iceberg catalog |
| **Proven** | Not documented/used by AWS services | Firehose uses this |
| **Maintenance** | Custom code to maintain | AWS-managed |

**Winner:** Glue Iceberg REST - simpler, proven, portable, zero bloat.

## Future Considerations

### When This Design Makes Sense

- Using S3 Tables for storage optimization
- Need immediate query access via standard tools
- Want AWS-managed catalog with automatic maintenance
- Multiple Lambdas writing concurrently
- Standard Iceberg compatibility required

### When to Consider Alternatives

- **Direct S3 writes**: If no catalog needed (Parquet only)
- **Different catalog**: Nessie for Git-like versioning, Polaris for multi-cloud
- **Self-managed**: PyIceberg/iceberg-rust for custom catalog logic

### Potential Enhancements

1. **Multi-region support**: CloudFormation stack sets for global deployment
2. **Cross-account access**: Lake Formation cross-account grants
3. **Schema evolution**: Leverage Glue's schema evolution capabilities
4. **Partition pruning**: Use Iceberg partition transforms for time-based queries
5. **Compaction strategy**: Configure Glue automatic compaction settings

## References

- [AWS S3 Tables Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html)
- [AWS Glue Iceberg REST Endpoint](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-glue-endpoint.html)
- [Firehose S3 Tables Integration](https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-destination.html)
- [Apache Iceberg REST Catalog Spec](https://iceberg.apache.org/rest-catalog-spec/)
- [Lake Formation Permissions](https://docs.aws.amazon.com/lake-formation/latest/dg/lake-formation-permissions.html)

## Conclusion

By using AWS Glue Iceberg REST endpoint instead of S3 Tables native APIs, we achieve:

- **Simpler implementation**: Config changes instead of new code
- **Better compatibility**: Standard Iceberg REST works everywhere
- **AWS-blessed pattern**: Same as Firehose uses
- **Operational benefits**: Automatic concurrency handling, immediate queryability

This design maintains all the benefits of S3 Tables (automatic optimization, managed storage) while using the standard Iceberg REST catalog pattern that AWS itself recommends.

**Recommendation:** Proceed with Glue integration. It's the right architectural choice for production S3 Tables + Lambda deployments.
