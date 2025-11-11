# AWS Glue Iceberg REST Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable Lambda to commit Iceberg snapshots via AWS Glue REST endpoint instead of S3 Tables native APIs.

**Architecture:** Route Lambda through Glue Iceberg REST (`https://glue.<region>.amazonaws.com/iceberg`) which manages metadata in Glue Data Catalog while S3 Tables provides optimized storage. This matches AWS Firehose's proven pattern.

**Tech Stack:** Rust, AWS Lambda, AWS Glue Data Catalog, S3 Tables, Lake Formation, Apache Iceberg

---

## Prerequisites

- S3 Tables bucket already created: `arn:aws:s3tables:us-west-2:156280089524:bucket/otlp2parquet-otlp2parquet`
- Lambda already deployed with S3 Tables REST integration (failing with "invalid_metadata")
- AWS CLI configured with appropriate permissions
- Current working directory: `/Users/clay/workspace/otlp2parquet`

---

## Task 1: Update SigV4 Signing Service

**Files:**
- Modify: `crates/otlp2parquet-iceberg/src/aws.rs:40-50`

**Step 1: Update signing service to "glue"**

Change the signing service from "s3tables" to "glue":

```rust
// In AwsSigV4HttpClient::new()
Ok(Self {
    client: ReqwestClient::new(),
    config,
    signing_service: "glue".to_string(), // Changed from "s3tables"
})
```

**Step 2: Verify compilation**

Run: `cargo check -p otlp2parquet-iceberg`
Expected: No errors

**Step 3: Run clippy**

Run: `cargo clippy -p otlp2parquet-iceberg -- -D warnings`
Expected: No warnings

**Step 4: Commit**

```bash
git add crates/otlp2parquet-iceberg/src/aws.rs
git commit -m "feat(iceberg): change SigV4 signing service to glue

Update AwsSigV4HttpClient to sign requests with 'glue' service
instead of 's3tables'. This enables using AWS Glue Iceberg REST
endpoint which implements full Iceberg REST catalog spec including
snapshot management operations."
```

---

## Task 2: Update Lambda Environment Variable Defaults

**Files:**
- Modify: `crates/otlp2parquet-lambda/src/lib.rs:530-545`

**Step 1: Update ICEBERG_REST_URI default**

Change the default Iceberg REST URI to point to Glue endpoint:

```rust
// Around line 540 in RuntimeConfig::load() or environment setup
let iceberg_rest_uri = env::var("ICEBERG_REST_URI")
    .unwrap_or_else(|_| {
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
        format!("https://glue.{}.amazonaws.com/iceberg", region)
    });
```

**Step 2: Add warehouse format validation**

Add a comment explaining the warehouse format:

```rust
// Warehouse format for S3 Tables + Glue: {account-id}:s3tablescatalog/{table-bucket-name}
// Example: 123456789012:s3tablescatalog/my-bucket
let warehouse = env::var("ICEBERG_WAREHOUSE")
    .context("ICEBERG_WAREHOUSE required (format: account-id:s3tablescatalog/bucket-name)")?;
```

**Step 3: Verify compilation**

Run: `cargo check -p otlp2parquet-lambda`
Expected: No errors

**Step 4: Commit**

```bash
git add crates/otlp2parquet-lambda/src/lib.rs
git commit -m "feat(lambda): default to Glue Iceberg REST endpoint

Set default ICEBERG_REST_URI to Glue endpoint and add validation
comment for warehouse format. Warehouse must use S3 Tables catalog
format: account-id:s3tablescatalog/bucket-name."
```

---

## Task 3: Update CloudFormation Template

**Files:**
- Modify: `examples/aws-lambda-s3-tables/template.yaml:50-90`

**Step 1: Update Lambda environment variables**

Replace Iceberg configuration section:

```yaml
# Around line 70 in LambdaFunction Environment section
Environment:
  Variables:
    STORAGE_BACKEND: s3
    STORAGE_S3_REGION: !Ref AWS::Region
    ICEBERG_ENABLED: "true"
    ICEBERG_REST_URI: !Sub "https://glue.${AWS::Region}.amazonaws.com/iceberg"
    ICEBERG_WAREHOUSE: !Sub "${AWS::AccountId}:s3tablescatalog/${TableBucketName}"
    ICEBERG_NAMESPACE: "otel"
```

**Step 2: Update IAM role policy for Glue access**

Add Glue and Lake Formation permissions to LambdaRole:

```yaml
# Around line 150 in LambdaRole Policies section
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

**Step 3: Add TableBucketName parameter if not present**

Check if TableBucketName parameter exists, if not add:

```yaml
# Near top of Parameters section
TableBucketName:
  Type: String
  Description: Name of the S3 Tables bucket (just the name, not full ARN)
  Default: otlp2parquet-otlp2parquet
```

**Step 4: Verify template syntax**

Run: `aws cloudformation validate-template --template-body file://examples/aws-lambda-s3-tables/template.yaml --region us-west-2`
Expected: Valid template confirmation

**Step 5: Commit**

```bash
git add examples/aws-lambda-s3-tables/template.yaml
git commit -m "feat(lambda): configure Glue Iceberg REST in CloudFormation

Update Lambda environment to use Glue endpoint and S3 Tables catalog
warehouse format. Add IAM permissions for Glue Data Catalog and Lake
Formation access. This enables full Iceberg REST support including
snapshot management."
```

---

## Task 4: Create Glue Integration Setup Script

**Files:**
- Create: `examples/aws-lambda-s3-tables/setup-glue-integration.sh`

**Step 1: Create setup script**

Create a bash script to integrate S3 Tables with Glue:

```bash
#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Parse arguments
REGION="us-west-2"
while [[ $# -gt 0 ]]; do
  case $1 in
    --region)
      REGION="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 --region <aws-region>"
      exit 1
      ;;
  esac
done

echo -e "${BLUE}==>${NC} Setting up Glue integration for S3 Tables"
echo -e "${BLUE}==>${NC} Region: $REGION"

# Load stack outputs
if [ ! -f stack-outputs.json ]; then
  echo -e "${YELLOW}==>${NC} stack-outputs.json not found. Run ./deploy.sh first."
  exit 1
fi

TABLE_BUCKET_ARN=$(jq -r '.[] | select(.OutputKey=="TableBucketArn") | .OutputValue' stack-outputs.json)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
LAMBDA_ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name otlp2parquet \
  --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaRoleArn`].OutputValue' \
  --output text 2>/dev/null || echo "")

echo -e "${BLUE}==>${NC} Table Bucket ARN: $TABLE_BUCKET_ARN"
echo -e "${BLUE}==>${NC} Account ID: $ACCOUNT_ID"

# Step 1: Integrate S3 Tables with Glue Data Catalog
echo -e "${GREEN}==>${NC} Step 1: Integrating S3 Tables with Glue Data Catalog..."
aws s3tables put-table-bucket-maintenance-configuration \
  --table-bucket-arn "$TABLE_BUCKET_ARN" \
  --type iceberg-compaction \
  --value '{"status":"enabled"}' \
  --region "$REGION" 2>/dev/null || echo "Maintenance already configured"

# Note: Actual Glue integration command varies - check latest AWS CLI
echo -e "${YELLOW}==>${NC} Manual step required: Integrate S3 Tables with AWS services via console"
echo -e "${YELLOW}    AWS Console → S3 Tables → $TABLE_BUCKET_ARN → Actions → Integrate with AWS services"

# Step 2: Create namespace
echo -e "${GREEN}==>${NC} Step 2: Creating namespace 'otel' in Glue..."
aws glue create-database \
  --database-input "{\"Name\":\"otel\",\"Description\":\"OpenTelemetry data lake\"}" \
  --region "$REGION" 2>/dev/null \
  && echo -e "${GREEN}✓${NC} Namespace created" \
  || echo -e "${YELLOW}✓${NC} Namespace already exists"

# Step 3: Verify integration
echo -e "${GREEN}==>${NC} Step 3: Verifying Glue Data Catalog..."
aws glue get-database --name otel --region "$REGION" > /dev/null \
  && echo -e "${GREEN}✓${NC} Database 'otel' accessible in Glue" \
  || echo -e "${YELLOW}✗${NC} Database 'otel' not found"

# Step 4: Configure Lake Formation (if Lambda role found)
if [ -n "$LAMBDA_ROLE_ARN" ]; then
  echo -e "${GREEN}==>${NC} Step 4: Configuring Lake Formation permissions..."

  # Grant database permissions
  aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier="$LAMBDA_ROLE_ARN" \
    --resource '{"Database":{"Name":"otel"}}' \
    --permissions CREATE_TABLE DESCRIBE \
    --region "$REGION" 2>/dev/null \
    && echo -e "${GREEN}✓${NC} Database permissions granted" \
    || echo -e "${YELLOW}✓${NC} Database permissions already exist"

  echo -e "${YELLOW}==>${NC} Table permissions must be granted after tables are created"
  echo -e "${YELLOW}    Run after first Lambda invocation:"
  echo -e "${YELLOW}    aws lakeformation grant-permissions \\"
  echo -e "${YELLOW}      --principal DataLakePrincipalIdentifier=$LAMBDA_ROLE_ARN \\"
  echo -e "${YELLOW}      --resource '{\"Table\":{\"DatabaseName\":\"otel\",\"Name\":\"logs\"}}' \\"
  echo -e "${YELLOW}      --permissions SELECT INSERT ALTER DESCRIBE \\"
  echo -e "${YELLOW}      --region $REGION"
else
  echo -e "${YELLOW}==>${NC} Step 4: Lambda role not found - skip Lake Formation setup for now"
fi

# Step 5: Display configuration
echo ""
echo -e "${BLUE}Configuration Summary:${NC}"
echo -e "  Glue Database: otel"
echo -e "  Glue Endpoint: https://glue.$REGION.amazonaws.com/iceberg"
echo -e "  Warehouse: $ACCOUNT_ID:s3tablescatalog/$(basename $TABLE_BUCKET_ARN)"
echo -e "  Region: $REGION"

echo ""
echo -e "${GREEN}==>${NC} Setup complete! Deploy Lambda with:"
echo -e "  ./deploy.sh --region $REGION"
```

**Step 2: Make script executable**

Run: `chmod +x examples/aws-lambda-s3-tables/setup-glue-integration.sh`

**Step 3: Test script syntax**

Run: `bash -n examples/aws-lambda-s3-tables/setup-glue-integration.sh`
Expected: No syntax errors

**Step 4: Commit**

```bash
git add examples/aws-lambda-s3-tables/setup-glue-integration.sh
git commit -m "feat(lambda): add Glue integration setup script

Add script to automate S3 Tables + Glue Data Catalog integration.
Handles namespace creation, Lake Formation permissions, and displays
configuration summary for Lambda deployment."
```

---

## Task 5: Update README Documentation

**Files:**
- Modify: `examples/aws-lambda-s3-tables/README.md`

**Step 1: Add Glue integration section**

Add a new section after deployment instructions:

```markdown
## Setup: Integrate S3 Tables with Glue Data Catalog

Before deploying, integrate your S3 Tables bucket with AWS Glue:

\`\`\`bash
# Run one-time setup
./setup-glue-integration.sh --region us-west-2
\`\`\`

This configures:
- S3 Tables integration with Glue Data Catalog
- Namespace (database) creation in Glue
- Lake Formation permissions for Lambda role

**Why Glue?** AWS S3 Tables REST endpoint doesn't support snapshot management
(add-snapshot operations). AWS Glue provides full Iceberg REST catalog support,
which is the same pattern AWS Firehose uses.

## Architecture

\`\`\`
Lambda → Glue Iceberg REST → Glue Data Catalog → S3 Tables
         (metadata API)      (snapshots/schema)   (Parquet storage)
\`\`\`
```

**Step 2: Update deployment section**

Update the deployment steps to reference setup script:

```markdown
## Deployment

1. **Setup Glue integration** (one-time):
   \`\`\`bash
   ./setup-glue-integration.sh --region us-west-2
   \`\`\`

2. **Deploy Lambda**:
   \`\`\`bash
   ./deploy.sh --region us-west-2
   \`\`\`

3. **Test ingestion**:
   \`\`\`bash
   ./test.sh --region us-west-2
   \`\`\`
```

**Step 3: Add querying section**

Add example queries using Glue catalog:

```markdown
## Querying Data

### DuckDB

\`\`\`bash
duckdb -c "
  INSTALL iceberg;
  LOAD iceberg;

  CREATE SECRET (
    TYPE S3,
    REGION 'us-west-2'
  );

  -- Query via Glue catalog
  SELECT COUNT(*) FROM iceberg_scan('glue://otel.logs', aws_region='us-west-2');
"
\`\`\`

### AWS Athena

\`\`\`sql
-- Tables automatically available in Athena
SELECT COUNT(*) FROM otel.logs;
SELECT COUNT(*) FROM otel.traces;
SELECT COUNT(*) FROM otel.metrics_gauge;
\`\`\`
```

**Step 4: Verify markdown syntax**

Run: `npx markdownlint examples/aws-lambda-s3-tables/README.md` (if available)
Or: Visual inspection for proper formatting

**Step 5: Commit**

```bash
git add examples/aws-lambda-s3-tables/README.md
git commit -m "docs(lambda): update README for Glue integration

Document Glue Data Catalog integration setup, architecture diagram,
and query examples. Explain why Glue is needed (S3 Tables REST API
limitation) and how it matches Firehose pattern."
```

---

## Task 6: Build and Deploy

**Files:**
- Build artifact: `target/lambda/bootstrap-arm64.zip`

**Step 1: Build Lambda with updated code**

Run: `make build-lambda`
Expected: Successful build, package size ~4.7 MB (no change)

**Step 2: Deploy to AWS**

Run: `cd examples/aws-lambda-s3-tables && ./deploy.sh --local-binary ../../target/lambda/bootstrap-arm64.zip --region us-west-2`
Expected: CloudFormation stack update successful

**Step 3: Verify Lambda environment variables**

Run: `aws lambda get-function-configuration --function-name otlp2parquet-ingest --region us-west-2 --query 'Environment.Variables' --output json`

Expected output includes:
```json
{
  "ICEBERG_ENABLED": "true",
  "ICEBERG_REST_URI": "https://glue.us-west-2.amazonaws.com/iceberg",
  "ICEBERG_WAREHOUSE": "156280089524:s3tablescatalog/otlp2parquet-otlp2parquet",
  "ICEBERG_NAMESPACE": "otel"
}
```

**Step 4: No commit (deployment step)**

---

## Task 7: Run Integration Tests

**Files:**
- N/A (testing only)

**Step 1: Run test suite**

Run: `cd examples/aws-lambda-s3-tables && ./test.sh --region us-west-2`
Expected: All 7 Lambda invocations succeed

**Step 2: Check CloudWatch logs for success**

Run: `aws logs tail /aws/lambda/otlp2parquet-ingest --region us-west-2 --since 5m --filter-pattern "Successfully committed" --format short`

Expected: Lines showing successful commits:
```
INFO Successfully committed to catalog table 'logs' (snapshot: 1762...)
INFO Successfully committed to catalog table 'traces' (snapshot: 1762...)
INFO Successfully committed to catalog table 'metrics_gauge' (snapshot: 1762...)
```

**Step 3: Verify NO "invalid_metadata" errors**

Run: `aws logs tail /aws/lambda/otlp2parquet-ingest --region us-west-2 --since 5m --filter-pattern "invalid_metadata" --format short`

Expected: No output (no errors)

**Step 4: Verify tables in Glue Data Catalog**

Run: `aws glue get-tables --database-name otel --region us-west-2 --query 'TableList[*].Name' --output json`

Expected: List of tables:
```json
["logs", "traces", "metrics_gauge", "metrics_sum", "metrics_histogram", "metrics_exponential_histogram", "metrics_summary"]
```

**Step 5: Query data with DuckDB**

Run:
```bash
duckdb -c "
  INSTALL iceberg;
  LOAD iceberg;
  CREATE SECRET (TYPE S3, REGION 'us-west-2');
  SELECT COUNT(*) as log_count FROM iceberg_scan('glue://otel.logs', aws_region='us-west-2');
"
```

Expected: Returns row count > 0 (data is queryable)

**Step 6: No commit (testing step)**

---

## Task 8: Disable DEBUG Logging

**Files:**
- Modify: `crates/otlp2parquet-lambda/src/lib.rs:523-526`

**Step 1: Change log level back to INFO**

Revert DEBUG logging added during troubleshooting:

```rust
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO) // Changed from DEBUG
    .without_time()
    .init();
```

**Step 2: Verify compilation**

Run: `cargo check -p otlp2parquet-lambda`
Expected: No errors

**Step 3: Rebuild and redeploy**

Run: `make build-lambda && cd examples/aws-lambda-s3-tables && ./deploy.sh --local-binary ../../target/lambda/bootstrap-arm64.zip --region us-west-2`
Expected: Successful deployment

**Step 4: Commit**

```bash
git add crates/otlp2parquet-lambda/src/lib.rs
git commit -m "chore(lambda): restore INFO log level

Revert DEBUG logging used during S3 Tables troubleshooting.
Production Lambda should use INFO level for reasonable log volume."
```

---

## Task 9: Update AGENTS.md Documentation

**Files:**
- Modify: `AGENTS.md:73-82`

**Step 1: Update Iceberg integration section**

Update the Apache Iceberg Integration section:

```markdown
## Apache Iceberg Integration

**Optional layer on top of Parquet files** - Provides ACID transactions, schema evolution, and faster queries
- **Platforms**: Server and Lambda only (not WASM/Cloudflare)
- **Catalogs**:
  - AWS Glue Data Catalog (recommended for S3 Tables - full Iceberg REST support)
  - AWS S3 Tables REST (limited - metadata operations only, no snapshot management)
  - Tabular, Polaris, Nessie (any standard Iceberg REST catalog)
- **Configuration**: Via `config.toml` `[iceberg]` section or `OTLP2PARQUET_ICEBERG_*` env vars
- **Behavior**: Two-step commit (write Parquet → commit to catalog via REST API)
- **Resilience**: Catalog failures log warnings but don't block ingestion
- **Tables**: One per schema (logs, traces, 5 metric types)
- **Concurrency**: Glue catalog handles optimistic locking automatically
```

**Step 2: Verify markdown formatting**

Run: Visual inspection for proper markdown syntax

**Step 3: Commit**

```bash
git add AGENTS.md
git commit -m "docs: update Iceberg integration documentation

Clarify catalog options with AWS Glue as recommended choice for
S3 Tables. Document Glue's full Iceberg REST support vs S3 Tables
REST limitations. Add concurrency handling note."
```

---

## Task 10: Final Verification and Cleanup

**Files:**
- N/A (verification only)

**Step 1: Run pre-commit hooks**

Run: `make pre-commit`
Expected: All hooks pass (fmt, clippy, conventional commits)

**Step 2: Run full test suite**

Run: `make test`
Expected: All tests pass

**Step 3: Verify Lambda test suite**

Run: `cd examples/aws-lambda-s3-tables && ./test.sh --region us-west-2`
Expected: All 7 invocations succeed, no errors in CloudWatch

**Step 4: Verify data queryability end-to-end**

Run:
```bash
# Send fresh data
cd examples/aws-lambda-s3-tables
./test.sh --region us-west-2

# Query immediately
duckdb -c "
  INSTALL iceberg; LOAD iceberg;
  CREATE SECRET (TYPE S3, REGION 'us-west-2');
  SELECT
    COUNT(*) as total,
    MAX(Timestamp) as latest
  FROM iceberg_scan('glue://otel.logs', aws_region='us-west-2');
"
```

Expected: Data from test run appears in query results

**Step 5: Check for unstaged changes**

Run: `git status`
Expected: Clean working tree (all changes committed)

**Step 6: Create summary commit if needed**

If any documentation or minor fixes were made, create a summary commit:

```bash
git add .
git commit -m "chore: final cleanup for Glue integration

Minor documentation updates and verification that all tests pass
with Glue Iceberg REST endpoint."
```

---

## Success Criteria

- ✅ Lambda builds successfully (~4.7 MB, no size increase)
- ✅ CloudFormation deploys with Glue endpoint configured
- ✅ All 7 test invocations succeed
- ✅ CloudWatch shows "Successfully committed" for all tables
- ✅ NO "invalid_metadata" errors in logs
- ✅ Tables appear in Glue Data Catalog
- ✅ Data queryable via DuckDB/Athena immediately after ingestion
- ✅ All pre-commit hooks pass
- ✅ All tests pass
- ✅ Documentation updated

## Rollback Plan

If Glue integration fails:

1. Set `ICEBERG_ENABLED=false` in Lambda environment:
   ```bash
   aws lambda update-function-configuration \
     --function-name otlp2parquet-ingest \
     --environment Variables={ICEBERG_ENABLED=false} \
     --region us-west-2
   ```

2. Lambda continues writing Parquet files without catalog commits

3. Investigate Glue/Lake Formation permission issues

4. Re-enable when fixed

## Estimated Time

- Task 1-5 (Code + CloudFormation): 30 minutes
- Task 6 (Build + Deploy): 15 minutes
- Task 7 (Testing + Verification): 30 minutes
- Task 8-10 (Cleanup + Docs): 15 minutes

**Total: ~90 minutes**

## Notes for Engineer

- **YAGNI**: We're NOT building S3 Tables native API integration - Glue handles everything
- **DRY**: Existing Iceberg code works as-is, just configuration changes
- **TDD**: Limited test changes needed (integration tests verify behavior)
- **Commits**: Small, focused commits with conventional commit format

This plan assumes you have:
- AWS CLI configured with admin access
- Rust toolchain installed
- S3 Tables bucket already created and deployed
- Basic understanding of Iceberg table format
