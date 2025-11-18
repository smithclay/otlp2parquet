# Single Lambda -> S3 Tables

This example deploys **one** `otlp2parquet` Lambda function with debug logging to AWS S3 Tables. No Glue catalogs, Lake Formation configuration, or extra data buckets are involved--the function writes directly to the managed S3 Tables bucket created by the CloudFormation stack and you can hit its HTTP endpoint with OTLP requests.

```
OTLP client -> Lambda Function URL -> otlp2parquet Lambda -> S3 Tables bucket/namespace
                                                    \\-> CloudWatch Logs (debug)
```

## What's in this folder

- `template.yaml` - CloudFormation stack that provisions:
  - an S3 Tables bucket + `otel` namespace (Parquet + Iceberg metadata live together)
  - a single Lambda function with `RUST_LOG=debug`
  - IAM role + CloudWatch log group
- `lifecycle.sh` - helper script for packaging the Lambda zip, deploying the stack, printing outputs, streaming logs, and deleting everything when finished.

## Prerequisites

- AWS CLI + credentials with permission to create CloudFormation stacks, Lambda, S3, and S3 Tables.
- `jq`, `unzip`, and optionally `curl` for testing the HTTP endpoint.
- (Optional) `cargo-lambda` if you want to build the Lambda binary locally via `make build-lambda`.

## 1. Build or download the Lambda package

```bash
# Option A: build locally (produces target/lambda/bootstrap-arm64.zip)
make build-lambda

# Option B: use a published release in lifecycle.sh with --version <tag> or --version latest (default)
```

The `deploy` command accepts either the locally built `target/lambda/bootstrap-arm64.zip` (recommended while iterating) or it will download the requested release zip for you.

## 2. Deploy the stack (Lambda + S3 Tables)

```bash
# Deploy arm64 Lambda that writes to an auto-created S3 Tables bucket + namespace
./lifecycle.sh deploy \
  --region us-west-2 \
  --stack-name otlp2parquet \
  --local-binary target/lambda/bootstrap-arm64.zip
```

What happens:

1. The Lambda zip is uploaded to a small deployment bucket (`otlp2parquet-deployment-<account>`).
2. CloudFormation creates:
   - S3 Tables bucket and `otel` namespace
   - IAM role with `s3tables:*` and `s3:*` access to that bucket
   - Lambda function (arm64 by default) with `RUST_LOG=debug`
   - 7-day retention CloudWatch log group `/aws/lambda/<stack>-ingest`

All stack outputs (Lambda name, S3 Tables bucket ARN, Function URL, etc.) are saved to `stack-outputs.json`. Re-run `./lifecycle.sh status` anytime to refresh them.

## 3. Send OTLP data to the Lambda

The stack automatically creates a Lambda Function URL (unauthenticated) for you. Get the URL from stack outputs:

```bash
FUNCTION_URL=$(jq -r '.[] | select(.OutputKey=="FunctionUrl") | .OutputValue' stack-outputs.json)
echo "Function URL: $FUNCTION_URL"
```

You now have an HTTPS endpoint such as `https://<id>.lambda-url.us-west-2.on.aws/v1/logs` that accepts OTLP HTTP traffic directly.

Use any OTLP-capable client. For a quick sanity check, reuse the repo fixtures:

```bash
# Logs
curl -X POST "$FUNCTION_URL/v1/logs" \
  -H 'content-type: application/json' \
  --data-binary @../../testdata/log.json

# Traces
curl -X POST "$FUNCTION_URL/v1/traces" \
  -H 'content-type: application/json' \
  --data-binary @../../testdata/trace.json

# Metrics (one metric type per request)
curl -X POST "$FUNCTION_URL/v1/metrics" \
  -H 'content-type: application/json' \
  --data-binary @../../testdata/metrics_gauge.json
```

Successful invocations:

- write Parquet files into the S3 Tables bucket partitions (`logs/{service}/year=.../file.parquet`, etc.)
- update the Iceberg metadata that S3 Tables manages automatically
- emit verbose debug logs for every batch (visible in CloudWatch)

## 4. Query the data directly from S3 Tables

S3 Tables exposes Iceberg metadata natively. You can query it with DuckDB + the Iceberg extension:

```bash
BUCKET=$(jq -r '.[] | select(.OutputKey=="S3TablesBucketName") | .OutputValue' stack-outputs.json)
REGION=us-west-2

duckdb -c "
  INSTALL iceberg;
  LOAD iceberg;
  CREATE SECRET (TYPE S3, REGION '${REGION}');

  -- list batches written by the Lambda
  SELECT count(*) AS total_logs
  FROM iceberg_scan('s3tables://${BUCKET}/otel/logs');
"
```

The stack eagerly creates the following tables on first write:

- `otel_logs`
- `otel_traces`
- `otel_metrics_gauge`
- `otel_metrics_sum`
- `otel_metrics_histogram`
- `otel_metrics_exponential_histogram`
- `otel_metrics_summary`

Alternatively, point AWS Athena at S3 Tables--no Glue database configuration required.

## 5. Debug logging and observability

- `RUST_LOG=debug` is enabled by default in `template.yaml`. Every ingestion step is logged through `tracing`, which makes it easy to inspect batch contents, partition keys, and S3 Tables API calls.
- Tail logs while exercising the HTTP endpoint:

```bash
./lifecycle.sh logs --region us-west-2 --stack-name otlp2parquet --follow
```

## 6. Clean up

To remove the Lambda, S3 Tables bucket, namespace, IAM role, and log group:

```bash
./lifecycle.sh delete --region us-west-2 --stack-name otlp2parquet
```

Add `--retain-bucket` if you want to keep the deployment bucket that stored the Lambda package. The S3 Tables bucket is deleted automatically with the stack.

---

This example intentionally sticks to the smallest possible footprint: one Lambda binary, one managed S3 Tables bucket/namespace, debug logging enabled, and a Lambda Function URL you can hit directly from your OTLP client. Use it as the baseline before layering on API Gateway auth, provisioning concurrency, or alternative catalogs.
