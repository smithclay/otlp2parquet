# Consolidate Lambda CloudFormation Templates

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Use a single CloudFormation template for both CLI deploy and smoke tests, eliminating duplicate SAM templates.

**Architecture:** Enhance the CLI template (`crates/otlp2parquet/templates/cloudformation.yaml`) with parameterized auth type, additional outputs for test verification, and fix env var naming. Smoke test harness uploads Lambda ZIP to S3 first, then deploys using plain CloudFormation with parameter overrides.

**Tech Stack:** AWS CloudFormation, Rust (test harness), aws-sdk-s3

---

## Summary of Changes

1. **Template changes** - Add `AuthType` parameter, fix env var names, add missing outputs
2. **Harness changes** - Upload Lambda ZIP to S3, use `aws cloudformation deploy` instead of SAM
3. **Cleanup** - Delete the two smoke test templates

---

### Task 1: Add AuthType Parameter to CloudFormation Template

**Files:**
- Modify: `crates/otlp2parquet/templates/cloudformation.yaml:6-21` (Parameters section)
- Modify: `crates/otlp2parquet/templates/cloudformation.yaml:109-132` (FunctionUrl and Permission)

**Step 1: Add AuthType parameter**

In `crates/otlp2parquet/templates/cloudformation.yaml`, add a new parameter after `LogRetentionDays`:

```yaml
Parameters:
  CatalogMode:
    Type: String
    Default: "{{CATALOG_MODE}}"
    AllowedValues:
      - iceberg
      - none

  BucketName:
    Type: String
    Default: "{{BUCKET_NAME}}"

  LogRetentionDays:
    Type: Number
    Default: {{LOG_RETENTION}}
    AllowedValues: [1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653]

  AuthType:
    Type: String
    Default: AWS_IAM
    AllowedValues:
      - AWS_IAM
      - NONE
    Description: Function URL authentication type (AWS_IAM for production, NONE for testing)
```

**Step 2: Update FunctionUrl to use parameter**

Replace hardcoded `AWS_IAM` with parameter reference:

```yaml
  FunctionUrl:
    Type: AWS::Lambda::Url
    Properties:
      AuthType: !Ref AuthType
      TargetFunctionArn: !GetAtt OtlpToParquetFunction.Arn
      Cors:
        AllowOrigins:
          - "*"
        AllowMethods:
          - POST
        AllowHeaders:
          - content-type
          - authorization
          - x-amz-date
          - x-amz-security-token
        MaxAge: 300

  FunctionUrlPermission:
    Type: AWS::Lambda::Permission
    Properties:
      FunctionName: !Ref OtlpToParquetFunction
      Action: lambda:InvokeFunctionUrl
      Principal: "*"
      FunctionUrlAuthType: !Ref AuthType
```

**Step 3: Verify template syntax**

Run: `aws cloudformation validate-template --template-body file://crates/otlp2parquet/templates/cloudformation.yaml`

Expected: Valid template response (ignore placeholder warnings)

**Step 4: Commit**

```bash
git add crates/otlp2parquet/templates/cloudformation.yaml
git commit -m "feat(deploy): add AuthType parameter to CloudFormation template"
```

---

### Task 2: Fix Environment Variable Names in Template

**Files:**
- Modify: `crates/otlp2parquet/templates/cloudformation.yaml:42-49` (Environment section)

The CLI template uses `OTLP2PARQUET_STORAGE_S3_BUCKET` but the code expects `OTLP2PARQUET_S3_BUCKET`.

**Step 1: Fix env var names**

Change from:
```yaml
      Environment:
        Variables:
          RUST_LOG: info
          OTLP2PARQUET_CATALOG_MODE: !Ref CatalogMode
          OTLP2PARQUET_STORAGE_S3_BUCKET: !If [UsePlainParquet, !Ref DataBucket, !Ref AWS::NoValue]
          OTLP2PARQUET_STORAGE_S3_REGION: !If [UsePlainParquet, !Ref AWS::Region, !Ref AWS::NoValue]
          OTLP2PARQUET_ICEBERG_BUCKET_ARN: !If [UseIcebergCatalog, !GetAtt S3TablesBucket.Arn, !Ref AWS::NoValue]
          OTLP2PARQUET_ICEBERG_NAMESPACE: otlp
```

To:
```yaml
      Environment:
        Variables:
          RUST_LOG: info
          OTLP2PARQUET_CATALOG_MODE: !Ref CatalogMode
          OTLP2PARQUET_S3_BUCKET: !If [UsePlainParquet, !Ref DataBucket, !Ref AWS::NoValue]
          OTLP2PARQUET_S3_REGION: !If [UsePlainParquet, !Ref AWS::Region, !Ref AWS::NoValue]
          OTLP2PARQUET_ICEBERG_BUCKET_ARN: !If [UseIcebergCatalog, !GetAtt S3TablesBucket.Arn, !Ref AWS::NoValue]
          OTLP2PARQUET_ICEBERG_NAMESPACE: otlp
```

**Step 2: Commit**

```bash
git add crates/otlp2parquet/templates/cloudformation.yaml
git commit -m "fix(deploy): correct S3 environment variable names"
```

---

### Task 3: Add Missing Outputs for Smoke Test Verification

**Files:**
- Modify: `crates/otlp2parquet/templates/cloudformation.yaml:157-172` (Outputs section)

The smoke test harness expects these outputs that are missing:
- `S3TablesBucketArn` (for Iceberg mode)
- `S3TablesBucketName` (for Iceberg mode)
- `DataBucketName` (for plain Parquet mode)

**Step 1: Add missing outputs**

Replace the Outputs section:

```yaml
Outputs:
  FunctionUrl:
    Description: OTLP HTTP endpoint URL
    Value: !GetAtt FunctionUrl.FunctionUrl

  FunctionArn:
    Description: Lambda function ARN
    Value: !GetAtt OtlpToParquetFunction.Arn

  FunctionName:
    Description: Lambda function name
    Value: !Ref OtlpToParquetFunction

  DataLocation:
    Description: Data storage location
    Value: !If
      - UsePlainParquet
      - !Sub 's3://${DataBucket}'
      - !GetAtt S3TablesBucket.Arn

  # Outputs for plain Parquet mode
  DataBucketName:
    Condition: UsePlainParquet
    Description: S3 bucket name for plain Parquet files
    Value: !Ref DataBucket

  # Outputs for Iceberg mode
  S3TablesBucketArn:
    Condition: UseIcebergCatalog
    Description: S3 Tables bucket ARN
    Value: !GetAtt S3TablesBucket.Arn

  S3TablesBucketName:
    Condition: UseIcebergCatalog
    Description: S3 Tables bucket name
    Value: !Ref S3TablesBucket
```

**Step 2: Validate template**

Run: `aws cloudformation validate-template --template-body file://crates/otlp2parquet/templates/cloudformation.yaml`

Expected: Valid template response

**Step 3: Commit**

```bash
git add crates/otlp2parquet/templates/cloudformation.yaml
git commit -m "feat(deploy): add bucket name outputs for smoke tests"
```

---

### Task 4: Add IcebergNamespace Parameter

**Files:**
- Modify: `crates/otlp2parquet/templates/cloudformation.yaml` (Parameters and Environment sections)

The smoke tests need to override the namespace. Currently it's hardcoded to `otlp`.

**Step 1: Add IcebergNamespace parameter**

Add after AuthType parameter:

```yaml
  IcebergNamespace:
    Type: String
    Default: otlp
    Description: Iceberg namespace for tables
```

**Step 2: Update environment variable to use parameter**

Change from:
```yaml
          OTLP2PARQUET_ICEBERG_NAMESPACE: otlp
```

To:
```yaml
          OTLP2PARQUET_ICEBERG_NAMESPACE: !Ref IcebergNamespace
```

**Step 3: Commit**

```bash
git add crates/otlp2parquet/templates/cloudformation.yaml
git commit -m "feat(deploy): add IcebergNamespace parameter"
```

---

### Task 5: Update Lambda Harness to Upload ZIP to S3

**Files:**
- Modify: `crates/otlp2parquet/tests/harness/lambda.rs:22-25` (imports)
- Modify: `crates/otlp2parquet/tests/harness/lambda.rs:37-50` (struct fields)
- Modify: `crates/otlp2parquet/tests/harness/lambda.rs:52-93` (constructor)

**Step 1: Add S3 client import**

Add to imports:

```rust
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
```

**Step 2: Update struct fields**

Replace the struct definition:

```rust
/// Lambda smoke test harness
pub struct LambdaHarness {
    /// CloudFormation stack name
    stack_name: String,
    /// AWS region
    region: String,
    /// S3 Tables namespace
    namespace: String,
    /// S3 bucket for Lambda ZIP upload
    lambda_bucket: String,
    /// S3 key for Lambda ZIP
    lambda_key: String,
    /// AWS SDK config
    aws_config: aws_config::SdkConfig,
    /// Catalog mode (S3 Tables or plain Parquet)
    catalog_mode: CatalogMode,
}
```

**Step 3: Update constructor to upload ZIP**

Replace `with_catalog_mode`:

```rust
impl LambdaHarness {
    /// Create harness with specific catalog mode
    pub async fn with_catalog_mode(catalog_mode: CatalogMode) -> Result<Self> {
        let stack_prefix =
            std::env::var("SMOKE_TEST_STACK_PREFIX").unwrap_or_else(|_| "smoke-lambda".to_string());
        let region =
            std::env::var("SMOKE_TEST_AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
        let namespace = std::env::var("SMOKE_TEST_S3_TABLES_NAMESPACE")
            .unwrap_or_else(|_| "otel_smoke".to_string());

        // Generate unique stack name for test isolation
        let test_id = uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap()
            .to_string();
        let stack_name = format!("{}-{}", stack_prefix, test_id);

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.clone()))
            .load()
            .await;

        // Upload Lambda ZIP to S3
        // Use a dedicated bucket or create temp one - for now use stack name as prefix
        let lambda_bucket = std::env::var("SMOKE_TEST_LAMBDA_BUCKET")
            .context("SMOKE_TEST_LAMBDA_BUCKET env var required")?;
        let lambda_key = format!("smoke-tests/{}/bootstrap-arm64.zip", stack_name);

        let s3_client = S3Client::new(&aws_config);
        let zip_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/lambda/bootstrap-arm64.zip");

        tracing::info!("Uploading Lambda ZIP to s3://{}/{}", lambda_bucket, lambda_key);

        let body = ByteStream::from_path(&zip_path)
            .await
            .context("Failed to read Lambda ZIP file")?;

        s3_client
            .put_object()
            .bucket(&lambda_bucket)
            .key(&lambda_key)
            .body(body)
            .send()
            .await
            .context("Failed to upload Lambda ZIP to S3")?;

        Ok(Self {
            stack_name,
            region,
            namespace,
            lambda_bucket,
            lambda_key,
            aws_config,
            catalog_mode,
        })
    }
}
```

**Step 4: Commit**

```bash
git add crates/otlp2parquet/tests/harness/lambda.rs
git commit -m "feat(smoke): upload Lambda ZIP to S3 in harness"
```

---

### Task 6: Update Harness Deploy to Use CloudFormation Instead of SAM

**Files:**
- Modify: `crates/otlp2parquet/tests/harness/lambda.rs:98-185` (deploy method)

**Step 1: Replace SAM deploy with CloudFormation deploy**

Replace the `deploy` method:

```rust
#[async_trait::async_trait]
impl SmokeTestHarness for LambdaHarness {
    async fn deploy(&self) -> Result<DeploymentInfo> {
        tracing::info!("Deploying Lambda smoke test stack: {}", self.stack_name);

        // Read and render the template
        let template_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("templates/cloudformation.yaml");
        let template_content = std::fs::read_to_string(&template_path)
            .context("Failed to read CloudFormation template")?;

        // Generate unique bucket name for data storage
        let data_bucket_name = format!("otlp2parquet-smoke-{}", &self.stack_name);

        // Render template placeholders
        let catalog_mode = match self.catalog_mode {
            CatalogMode::Enabled => "iceberg",
            CatalogMode::None => "none",
        };

        let rendered = template_content
            .replace("{{STACK_NAME}}", &self.stack_name)
            .replace("{{BUCKET_NAME}}", &data_bucket_name)
            .replace("{{CATALOG_MODE}}", catalog_mode)
            .replace("{{LOG_RETENTION}}", "7")
            .replace("{{LAMBDA_S3_BUCKET}}", &self.lambda_bucket)
            .replace("{{LAMBDA_S3_KEY}}", &self.lambda_key);

        // Write rendered template to temp file
        let temp_template = std::env::temp_dir().join(format!("{}-template.yaml", self.stack_name));
        std::fs::write(&temp_template, &rendered)
            .context("Failed to write rendered template")?;

        // Deploy using CloudFormation CLI
        let deploy = Command::new("aws")
            .args([
                "cloudformation", "deploy",
                "--template-file", temp_template.to_str().unwrap(),
                "--stack-name", &self.stack_name,
                "--region", &self.region,
                "--capabilities", "CAPABILITY_IAM",
                "--parameter-overrides",
                &format!("AuthType=NONE"),
                &format!("IcebergNamespace={}", self.namespace),
                "--no-fail-on-empty-changeset",
            ])
            .output()
            .await
            .context("Failed to execute CloudFormation deploy")?;

        if !deploy.status.success() {
            let stderr = String::from_utf8_lossy(&deploy.stderr);
            let stdout = String::from_utf8_lossy(&deploy.stdout);
            anyhow::bail!("CloudFormation deploy failed:\nstderr: {}\nstdout: {}", stderr, stdout);
        }

        tracing::info!("CloudFormation deployment complete");

        // Clean up temp template
        let _ = std::fs::remove_file(&temp_template);

        // Get stack outputs using CloudFormation SDK
        let cfn_client = CfnClient::new(&self.aws_config);

        // Wait a bit for stack to stabilize
        tokio::time::sleep(Duration::from_secs(5)).await;

        let outputs = self.get_stack_outputs(&cfn_client).await?;

        let endpoint = outputs
            .get("FunctionUrl")
            .context("FunctionUrl output not found")?
            .clone();

        // Extract catalog endpoint and bucket based on catalog mode
        let (catalog_endpoint, bucket) = match self.catalog_mode {
            CatalogMode::Enabled => {
                let s3_tables_arn = outputs
                    .get("S3TablesBucketArn")
                    .context("S3TablesBucketArn output not found")?
                    .clone();
                let s3_tables_name = outputs
                    .get("S3TablesBucketName")
                    .context("S3TablesBucketName output not found")?
                    .clone();
                (s3_tables_arn, s3_tables_name)
            }
            CatalogMode::None => {
                let data_bucket = outputs
                    .get("DataBucketName")
                    .context("DataBucketName output not found")?
                    .clone();
                // No catalog endpoint for plain Parquet mode
                ("".to_string(), data_bucket)
            }
        };

        tracing::info!("Lambda deployed successfully: {}", endpoint);

        // Strip trailing slash from Lambda Function URL
        let endpoint = endpoint.trim_end_matches('/').to_string();

        Ok(DeploymentInfo {
            endpoint,
            catalog_endpoint,
            bucket,
            namespace: self.namespace.clone(),
            resource_ids: HashMap::from([("stack_name".to_string(), self.stack_name.clone())]),
        })
    }
```

**Step 2: Commit**

```bash
git add crates/otlp2parquet/tests/harness/lambda.rs
git commit -m "refactor(smoke): use CloudFormation instead of SAM for deployment"
```

---

### Task 7: Update Harness Cleanup to Delete Uploaded ZIP

**Files:**
- Modify: `crates/otlp2parquet/tests/harness/lambda.rs:322-480` (cleanup method)

**Step 1: Add S3 cleanup at end of cleanup method**

Add before the final `Ok(())`:

```rust
        // Clean up the uploaded Lambda ZIP
        let s3_client = S3Client::new(&self.aws_config);
        tracing::info!("Deleting Lambda ZIP from s3://{}/{}", self.lambda_bucket, self.lambda_key);
        if let Err(e) = s3_client
            .delete_object()
            .bucket(&self.lambda_bucket)
            .key(&self.lambda_key)
            .send()
            .await
        {
            tracing::warn!("Failed to delete Lambda ZIP: {}", e);
        }

        Ok(())
```

**Step 2: Commit**

```bash
git add crates/otlp2parquet/tests/harness/lambda.rs
git commit -m "feat(smoke): clean up uploaded Lambda ZIP after test"
```

---

### Task 8: Remove Old Smoke Test Templates

**Files:**
- Delete: `scripts/smoke/lambda-template.yaml`
- Delete: `scripts/smoke/lambda-template-plain-parquet.yaml`

**Step 1: Remove template files**

```bash
rm scripts/smoke/lambda-template.yaml
rm scripts/smoke/lambda-template-plain-parquet.yaml
```

**Step 2: Remove empty directory if needed**

```bash
rmdir scripts/smoke 2>/dev/null || true
```

**Step 3: Commit**

```bash
git add -A scripts/smoke/
git commit -m "chore: remove duplicate Lambda smoke test templates"
```

---

### Task 9: Update Cargo.toml Dependencies

**Files:**
- Modify: `crates/otlp2parquet/Cargo.toml` (dev-dependencies)

**Step 1: Add aws-sdk-s3 to dev-dependencies**

Check if `aws-sdk-s3` is already a dev-dependency. If not, add it:

```toml
[dev-dependencies]
aws-sdk-s3 = { version = "1", features = ["behavior-version-latest"] }
```

**Step 2: Commit**

```bash
git add crates/otlp2parquet/Cargo.toml
git commit -m "chore: add aws-sdk-s3 dev dependency for smoke tests"
```

---

### Task 10: Run Smoke Tests to Verify

**Step 1: Build Lambda binary**

Run: `make build-lambda`

Expected: Successful build, creates `target/lambda/bootstrap-arm64.zip`

**Step 2: Set required environment variable**

```bash
export SMOKE_TEST_LAMBDA_BUCKET=your-existing-s3-bucket
```

**Step 3: Run plain Parquet smoke test**

Run: `cargo test --package otlp2parquet --test smoke lambda_plain_parquet -- --nocapture`

Expected: Test passes, deploys stack, sends signals, verifies data, cleans up

**Step 4: Run Iceberg smoke test (if S3 Tables available)**

Run: `cargo test --package otlp2parquet --test smoke lambda_iceberg -- --nocapture`

Expected: Test passes with Iceberg catalog

---

## Environment Variables Required for Smoke Tests

After this change, smoke tests require:

| Variable | Description | Example |
|----------|-------------|---------|
| `SMOKE_TEST_LAMBDA_BUCKET` | S3 bucket for Lambda ZIP upload | `my-deployment-bucket` |
| `SMOKE_TEST_STACK_PREFIX` | Stack name prefix (optional) | `smoke-lambda` |
| `SMOKE_TEST_AWS_REGION` | AWS region (optional) | `us-west-2` |
| `SMOKE_TEST_S3_TABLES_NAMESPACE` | Iceberg namespace (optional) | `otel_smoke` |

---

## Verification Checklist

- [ ] Template validates: `aws cloudformation validate-template`
- [ ] CLI deploy still works: `otlp2parquet deploy aws`
- [ ] Plain Parquet smoke test passes
- [ ] Iceberg smoke test passes
- [ ] Old templates deleted
- [ ] No SAM dependency in smoke tests
