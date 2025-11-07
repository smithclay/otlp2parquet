# Task 11: End-to-End Testing - Test Plan

**Date:** 2025-11-07
**Task:** Task 11 from docs/plans/2025-11-06-s3-tables-integrated-writes.md
**Status:** Build verification completed - Ready for deployment testing

---

## Build Verification Results

### Lambda Binary Build
- **Status:** ✅ SUCCESS
- **Build command:** `cargo build --release --package otlp2parquet-lambda`
- **Binary location:** `/Users/clay/workspace/otlp2parquet/target/release/bootstrap`
- **Binary size:** 7.2 MB (uncompressed)
- **Architecture:** arm64 (Mach-O 64-bit executable)
- **Compilation:** Clean build with no errors
- **Warnings:** 2 clippy warnings (minor - `clone` can be replaced with `std::slice::from_ref`)

### Code Quality
- All compilation successful
- No blocking errors
- Minor clippy warnings (non-critical)
- Release mode optimizations applied

---

## Test Plan Overview

The following outlines the complete end-to-end testing process for S3 Tables integrated writes. These tests should be executed when ready to deploy to AWS.

---

## Pre-Deployment Checklist

### 1. Build Lambda Package
```bash
cd examples/aws-lambda-s3-tables
make build-lambda
```

**Expected output:**
- Creates `target/lambda/bootstrap.zip`
- Package ready for deployment

### 2. Validate CloudFormation Template
```bash
aws cloudformation validate-template \
  --template-body file://template.yaml \
  --region us-west-2
```

**Expected output:**
- "Valid template" confirmation
- No syntax errors

---

## Deployment Testing

### Step 1: Deploy CloudFormation Stack

**Command:**
```bash
./deploy.sh --stack-name otlp2parquet-test --region us-west-2
```

**What this does:**
1. Downloads or uses local Lambda binary
2. Creates deployment S3 bucket
3. Uploads Lambda code package
4. Deploys CloudFormation stack with:
   - S3 Table Bucket (Iceberg metadata storage)
   - Data Bucket (Parquet file storage)
   - Namespace: `otel`
   - Lambda function with execution role
   - CloudWatch log group

**Expected output:**
- Stack creates successfully
- All resources provisioned
- Outputs display:
  - Lambda function ARN
  - Table Bucket ARN
  - Data Bucket name
  - Namespace ARN
  - Iceberg REST endpoint

**Verification:**
```bash
aws cloudformation describe-stacks \
  --stack-name otlp2parquet-test \
  --region us-west-2 \
  --query 'Stacks[0].Outputs'
```

---

### Step 2: Environment Variable Verification

**Check Lambda configuration:**
```bash
aws lambda get-function-configuration \
  --function-name otlp2parquet-test-ingest \
  --region us-west-2 \
  --query 'Environment.Variables'
```

**Expected environment variables:**
```json
{
  "OTLP2PARQUET_ICEBERG_REST_URI": "https://s3tables.us-west-2.amazonaws.com/iceberg",
  "OTLP2PARQUET_ICEBERG_WAREHOUSE": "arn:aws:s3tables:us-west-2:ACCOUNT:bucket/otlp2parquet-test",
  "OTLP2PARQUET_ICEBERG_NAMESPACE": "otel",
  "OTLP2PARQUET_STORAGE_BACKEND": "s3",
  "OTLP2PARQUET_S3_BUCKET": "otlp2parquet-data-otlp2parquet-test-ACCOUNT",
  "OTLP2PARQUET_S3_REGION": "us-west-2",
  "RUST_LOG": "info"
}
```

---

### Step 3: Run Test Script

**Command:**
```bash
./test.sh --stack-name otlp2parquet-test --region us-west-2 --verbose
```

**What this tests:**

1. **Logs ingestion** (`testdata/log.json`)
   - POST to `/v1/logs`
   - JSON format OTLP payload
   - Creates `otel.logs` table automatically
   - Writes Parquet to warehouse location
   - Commits to Iceberg catalog

2. **Traces ingestion** (`testdata/trace.json`)
   - POST to `/v1/traces`
   - JSON format OTLP payload
   - Creates `otel.traces` table automatically
   - Writes Parquet with spans, events, links
   - Commits to Iceberg catalog

3. **Metrics ingestion** (all 5 types)
   - `metrics_gauge.json` → `otel.metrics_gauge` table
   - `metrics_sum.json` → `otel.metrics_sum` table
   - `metrics_histogram.json` → `otel.metrics_histogram` table
   - `metrics_exponential_histogram.json` → `otel.metrics_exponential_histogram` table
   - `metrics_summary.json` → `otel.metrics_summary` table (if exists)

**Expected output for each test:**
- ✓ Lambda invocation successful
- ✓ No errors in response
- HTTP 200 status code
- Response body confirms ingestion

**Test data files available:**
- `/Users/clay/workspace/otlp2parquet/testdata/log.json`
- `/Users/clay/workspace/otlp2parquet/testdata/trace.json`
- `/Users/clay/workspace/otlp2parquet/testdata/metrics_gauge.json`
- `/Users/clay/workspace/otlp2parquet/testdata/metrics_sum.json`
- `/Users/clay/workspace/otlp2parquet/testdata/metrics_histogram.json`
- `/Users/clay/workspace/otlp2parquet/testdata/metrics_exponential_histogram.json`

---

### Step 4: CloudWatch Logs Verification

**Check Lambda execution logs:**
```bash
aws logs tail /aws/lambda/otlp2parquet-test-ingest \
  --region us-west-2 \
  --follow \
  --since 10m
```

**Expected log patterns:**

1. **Mode detection:**
   ```
   Detected writer mode: Iceberg (S3 Tables)
   ```

2. **Table operations:**
   ```
   Loaded existing table: logs
   ```
   OR
   ```
   Creating new table: logs
   Table created successfully
   ```

3. **Write operations:**
   ```
   Writing Parquet to warehouse location: s3://...
   Parquet write completed: X rows, Y bytes
   ```

4. **Catalog commits:**
   ```
   Successfully committed data file to catalog
   ```
   OR (warning case):
   ```
   WARN: Failed to commit to Iceberg catalog: <reason>. File written but not cataloged.
   ```

**What to watch for:**
- No error messages
- Table creation happens on first write for each signal type
- Subsequent writes show "Loaded existing table"
- Commit success messages

---

### Step 5: Verify Tables Created in S3 Tables

**List tables in namespace:**
```bash
TABLE_BUCKET_ARN=$(aws cloudformation describe-stacks \
  --stack-name otlp2parquet-test \
  --region us-west-2 \
  --query 'Stacks[0].Outputs[?OutputKey==`TableBucketArn`].OutputValue' \
  --output text)

aws s3tables list-tables \
  --table-bucket-arn "$TABLE_BUCKET_ARN" \
  --namespace otel \
  --region us-west-2
```

**Expected tables:**
- `otel.logs`
- `otel.traces`
- `otel.metrics_gauge`
- `otel.metrics_sum`
- `otel.metrics_histogram`
- `otel.metrics_exponential_histogram`

**Get table details:**
```bash
aws s3tables get-table \
  --table-bucket-arn "$TABLE_BUCKET_ARN" \
  --namespace otel \
  --name logs \
  --region us-west-2
```

**Expected details:**
- Table ARN
- Warehouse location (S3 path)
- Creation timestamp
- Iceberg format version

---

### Step 6: Verify Parquet Files in Data Bucket

**List files written:**
```bash
DATA_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name otlp2parquet-test \
  --region us-west-2 \
  --query 'Stacks[0].Outputs[?OutputKey==`DataBucketName`].OutputValue' \
  --output text)

aws s3 ls "s3://${DATA_BUCKET}/otel/" --recursive --region us-west-2
```

**Expected structure:**
```
otel/logs/data/<timestamp>-<uuid>.parquet
otel/traces/data/<timestamp>-<uuid>.parquet
otel/metrics_gauge/data/<timestamp>-<uuid>.parquet
otel/metrics_sum/data/<timestamp>-<uuid>.parquet
otel/metrics_histogram/data/<timestamp>-<uuid>.parquet
otel/metrics_exponential_histogram/data/<timestamp>-<uuid>.parquet
```

**Check file metadata:**
```bash
aws s3api head-object \
  --bucket "$DATA_BUCKET" \
  --key "otel/logs/data/<filename>.parquet" \
  --region us-west-2
```

**Expected:**
- Content-Type: application/octet-stream (or binary)
- File size > 0
- Valid metadata

---

### Step 7: Query Data with DuckDB

**Prerequisites:**
- DuckDB installed (`brew install duckdb` or download)
- AWS credentials configured

**Test query:**
```sql
-- test-query.sql
INSTALL iceberg;
LOAD iceberg;

-- Configure S3 Tables catalog
CREATE SECRET s3tables (
    TYPE ICEBERG_REST,
    ENDPOINT 'https://s3tables.us-west-2.amazonaws.com/iceberg',
    WAREHOUSE 'arn:aws:s3tables:us-west-2:ACCOUNT:bucket/otlp2parquet-test',
    AWS_REGION 'us-west-2'
);

-- Query logs table
SELECT
    COUNT(*) as total_records,
    MIN(Timestamp) as earliest,
    MAX(Timestamp) as latest
FROM iceberg_scan('s3tables', 'otel', 'logs');

-- Show sample records
SELECT * FROM iceberg_scan('s3tables', 'otel', 'logs')
LIMIT 5;

-- Query traces table
SELECT
    COUNT(*) as total_spans,
    COUNT(DISTINCT TraceId) as unique_traces
FROM iceberg_scan('s3tables', 'otel', 'traces');

-- Query metrics
SELECT
    COUNT(*) as total_metrics,
    COUNT(DISTINCT MetricName) as unique_metric_names
FROM iceberg_scan('s3tables', 'otel', 'metrics_gauge');
```

**Run query:**
```bash
duckdb < test-query.sql
```

**Expected results:**
- Row counts match ingested data
- Timestamps are valid
- Data is readable and structured correctly
- All columns present (ClickHouse-compatible schema)

---

### Step 8: Test Dual-Mode Operation (Optional)

**Switch to Plain S3 mode:**

1. Update CloudFormation template:
   ```yaml
   # Comment out Iceberg variables
   # OTLP2PARQUET_ICEBERG_REST_URI: ...
   # OTLP2PARQUET_ICEBERG_WAREHOUSE: ...
   # OTLP2PARQUET_ICEBERG_NAMESPACE: ...

   # Keep only S3 variables
   OTLP2PARQUET_STORAGE_BACKEND: 's3'
   OTLP2PARQUET_S3_BUCKET: !Ref DataBucket
   OTLP2PARQUET_S3_REGION: !Ref AWS::Region
   ```

2. Update stack:
   ```bash
   ./deploy.sh --stack-name otlp2parquet-test --region us-west-2
   ```

3. Run tests again:
   ```bash
   ./test.sh --stack-name otlp2parquet-test --region us-west-2
   ```

4. Verify CloudWatch logs show:
   ```
   Detected writer mode: PlainS3 (no Iceberg)
   ```

5. Check files written directly to S3 (no catalog commits):
   ```bash
   aws s3 ls "s3://${DATA_BUCKET}/logs/" --recursive --region us-west-2
   ```

**Expected behavior:**
- Lambda detects mode automatically
- Writes Parquet files to S3
- No Iceberg catalog operations
- Files accessible directly via S3 paths

---

## Success Criteria

### Critical (Must Pass)
- ✅ Lambda binary builds successfully
- ✅ CloudFormation stack deploys without errors
- ✅ All test invocations return 200 OK
- ✅ Tables created automatically in S3 Tables
- ✅ Parquet files written to data bucket
- ✅ DuckDB queries return expected data
- ✅ CloudWatch logs show successful operations

### Important (Should Pass)
- ✅ All 5 metric types create separate tables
- ✅ Catalog commits succeed (or warn gracefully)
- ✅ Dual-mode switching works correctly
- ✅ Binary size under 10MB uncompressed

### Optional (Nice to Have)
- Query performance testing with large datasets
- Concurrent write testing
- Error handling validation
- Schema evolution testing

---

## Known Issues & Limitations

### Current Status
1. **Uncommitted changes**: Implementation code exists but not committed
2. **Existing AWS stack**: User has existing deployment - do not interfere
3. **Build warnings**: 2 minor clippy warnings (non-blocking)

### Blockers for Testing
- None - binary builds successfully
- Ready for deployment when user chooses

### Testing Constraints
- Do not deploy to existing AWS stack
- Do not run actual test scripts (user decision)
- Document process only

---

## Rollback Plan

If testing fails:

1. **Delete CloudFormation stack:**
   ```bash
   ./deploy.sh --delete --stack-name otlp2parquet-test --region us-west-2
   ```

2. **Verify cleanup:**
   ```bash
   aws cloudformation describe-stacks \
     --stack-name otlp2parquet-test \
     --region us-west-2
   ```
   Should return: "Stack not found"

3. **Manual cleanup (if needed):**
   - Delete S3 Table Bucket via console
   - Delete data bucket: `aws s3 rb s3://${DATA_BUCKET} --force`
   - Delete CloudWatch log group

---

## Post-Deployment Verification

### Metrics to Monitor
1. **Lambda execution:**
   - Duration (should be < 5 seconds per batch)
   - Memory usage (should be < 512MB)
   - Error rate (should be 0%)
   - Cold start time (should be < 2 seconds)

2. **Storage:**
   - Parquet file sizes (reasonable compression)
   - S3 Tables API calls
   - Data bucket growth

3. **Iceberg catalog:**
   - Table count matches signal types
   - Metadata updates successful
   - Commit transaction latency

### Cost Estimate
For 1000 test invocations:
- Lambda invocations: ~$0.0002
- Lambda compute: ~$0.001
- S3 Tables API: ~$0.0001
- S3 storage: negligible
- CloudWatch: negligible

**Total: < $0.01**

---

## Next Steps After Testing

1. **Document test results:**
   - Create `examples/aws-lambda-s3-tables/TEST_RESULTS.md`
   - Include CloudWatch log excerpts
   - Screenshot S3 Tables console showing tables
   - DuckDB query outputs

2. **Update documentation:**
   - Add any discovered gotchas to README
   - Update troubleshooting section
   - Add query examples that work

3. **Production considerations:**
   - Add API Gateway for HTTPS endpoint
   - Configure authentication (API keys or IAM)
   - Set up CloudWatch dashboards
   - Add alarms for errors
   - Consider provisioned concurrency

4. **Commit test results:**
   ```bash
   git add examples/aws-lambda-s3-tables/TEST_RESULTS.md
   git commit -m "docs: add S3 Tables integration test results"
   ```

---

## Files Checked

### Build-related
- `/Users/clay/workspace/otlp2parquet/target/release/bootstrap` - Lambda binary (7.2 MB)
- `/Users/clay/workspace/otlp2parquet/Cargo.toml` - Workspace configuration
- `/Users/clay/workspace/otlp2parquet/crates/otlp2parquet-lambda/Cargo.toml` - Lambda dependencies
- `/Users/clay/workspace/otlp2parquet/crates/otlp2parquet-lambda/src/lib.rs` - Lambda handler with dual-mode support

### Test infrastructure
- `/Users/clay/workspace/otlp2parquet/examples/aws-lambda-s3-tables/template.yaml` - CloudFormation template
- `/Users/clay/workspace/otlp2parquet/examples/aws-lambda-s3-tables/deploy.sh` - Deployment script
- `/Users/clay/workspace/otlp2parquet/examples/aws-lambda-s3-tables/test.sh` - Test automation script
- `/Users/clay/workspace/otlp2parquet/examples/aws-lambda-s3-tables/README.md` - Documentation

### Test data
- `/Users/clay/workspace/otlp2parquet/testdata/log.json` - Logs test payload
- `/Users/clay/workspace/otlp2parquet/testdata/trace.json` - Traces test payload
- `/Users/clay/workspace/otlp2parquet/testdata/metrics_gauge.json` - Gauge metrics
- `/Users/clay/workspace/otlp2parquet/testdata/metrics_sum.json` - Sum metrics
- `/Users/clay/workspace/otlp2parquet/testdata/metrics_histogram.json` - Histogram metrics
- `/Users/clay/workspace/otlp2parquet/testdata/metrics_exponential_histogram.json` - ExponentialHistogram metrics

### Documentation
- `/Users/clay/workspace/otlp2parquet/docs/plans/2025-11-06-s3-tables-integrated-writes.md` - Implementation plan

---

## Conclusion

### Build Status: ✅ SUCCESS
The Lambda binary compiles successfully with no blocking issues. The implementation is ready for deployment testing.

### Test Status: 📋 READY
Complete test infrastructure is in place:
- CloudFormation template configured for S3 Tables mode
- Deployment script ready
- Test automation script ready
- Test data available
- Documentation complete

### Recommendation: PROCEED WHEN READY
All prerequisites met for end-to-end testing. User can proceed with deployment at their discretion using the documented test plan above.

### No Action Taken
As instructed:
- ✅ Build verified only
- ❌ No deployment performed
- ❌ No test scripts executed
- ❌ No uncommitted changes made
- ✅ Documentation provided
