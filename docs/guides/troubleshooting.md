# Troubleshooting Guide

Common issues and solutions for otlp2parquet across all deployment platforms.

---

## Platform Detection Issues

### Wrong Platform Detected

**Symptom:** Logs show unexpected platform initialization or configuration errors.

**Cause:** Platform detection environment variables not set correctly.

**Solution:**

1. Check platform detection variables:
   ```bash
   # Lambda should have:
   echo $AWS_LAMBDA_FUNCTION_NAME

   # Cloudflare Workers should have:
   echo $CF_WORKER
   ```

2. Verify no conflicting variables are set (e.g., both `AWS_LAMBDA_FUNCTION_NAME` and `CF_WORKER`)

3. For server deployments, ensure **neither** variable is set

---

## S3 Tables Issues (Lambda)

### "Access Denied" Errors

**Symptom:** CloudWatch logs show `AccessDenied` or permission errors when writing data.

**Cause:** Lambda execution role missing required S3 Tables permissions.

**Solution:**

1. Verify IAM role has all required permissions:
   ```bash
   aws iam get-role-policy \
     --role-name your-lambda-role-name \
     --policy-name S3TablesAccess
   ```

2. Required permissions checklist:
   - ✅ `s3tables:GetTable`
   - ✅ `s3tables:GetTableMetadataLocation`
   - ✅ `s3tables:CreateTable`
   - ✅ `s3tables:UpdateTableMetadataLocation`
   - ✅ `s3tables:GetNamespace`
   - ✅ `s3tables:CreateNamespace`
   - ✅ `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket`

3. Check the bucket ARN is correctly formatted:
   ```
   arn:aws:s3tables:region:account-id:bucket/bucket-name
   ```

4. Verify environment variable is set:
   ```bash
   aws lambda get-function-configuration \
     --function-name your-function \
     --query 'Environment.Variables.OTLP2PARQUET_ICEBERG_BUCKET_ARN'
   ```

### Tables Not Appearing in S3 Tables Console

**Symptom:** No tables visible in AWS console after sending data.

**Cause:** Tables are auto-created on first write per signal type.

**Solution:**

1. Send test data first:
   ```bash
   curl -X POST https://your-lambda-url/v1/logs \
     -H "Content-Type: application/json" \
     -d @testdata/log.json
   ```

2. Check tables exist:
   ```bash
   aws s3tables list-tables \
     --table-bucket-arn arn:aws:s3tables:region:account:bucket/name \
     --namespace otel
   ```

3. Check CloudWatch logs for creation errors:
   ```bash
   aws logs tail /aws/lambda/your-function --follow
   ```

### Lambda Timeout Errors

**Symptom:** Lambda function times out before completing writes.

**Cause:** Insufficient memory/CPU or processing large batches.

**Solution:**

1. Increase Lambda memory (more memory = more CPU):
   ```yaml
   # In CloudFormation
   MemorySize: 1024  # Default is 512
   ```

2. Increase timeout:
   ```yaml
   Timeout: 120  # Default is 60 seconds
   ```

3. Implement upstream batching (OTel Collector) to reduce per-request processing time

### S3 Tables API Throttling

**Symptom:** `ThrottlingException` or rate limit errors in logs.

**Cause:** Exceeding S3 Tables API rate limits.

**Solution:**

1. Implement batching upstream (OpenTelemetry Collector or Vector)
2. Request quota increase via AWS Support
3. Add exponential backoff in OTLP exporter configuration
4. Consider using larger batches to reduce API call frequency

---

## R2 / Cloudflare Workers Issues

### R2 Access Denied

**Symptom:** Worker logs show R2 permission errors.

**Cause:** Binding name mismatch or missing R2 permissions.

**Solution:**

1. Verify binding name in `wrangler.toml` matches code expectations:
   ```toml
   [[r2_buckets]]
   binding = "LOGS_BUCKET"  # Must match code
   bucket_name = "otlp-logs"
   ```

2. Check API token has R2 read/write permissions:
   - Go to Cloudflare Dashboard → API Tokens
   - Ensure token has "R2 Read" and "R2 Write" permissions

3. For R2 Data Catalog, verify bucket has catalog enabled:
   ```bash
   wrangler r2 bucket info otlp-data
   ```

### 413 Payload Too Large

**Symptom:** HTTP 413 error when sending OTLP data.

**Cause:** Request exceeds `OTLP2PARQUET_MAX_PAYLOAD_BYTES` limit.

**Solution:**

1. Increase payload limit in `wrangler.toml`:
   ```toml
   [vars]
   OTLP2PARQUET_MAX_PAYLOAD_BYTES = "20971520"  # 20MB
   ```

2. Monitor Worker memory usage (128MB limit):
   ```bash
   wrangler tail --format json | jq '.outcome.memoryUsage'
   ```

3. Implement upstream batching to send smaller, more frequent requests

### Worker Deployment Exceeds 3MB

**Symptom:** `wrangler deploy` fails with "Worker exceeds size limit" error.

**Cause:** WASM binary size exceeds Cloudflare's 3MB compressed limit.

**Solution:**

1. Check current size:
   ```bash
   make wasm-size
   ```
   Expected: ~1.3MB (43.8% of limit)

2. If size increased:
   - Review recently added dependencies
   - Ensure `default-features = false` for all dependencies
   - Run `make wasm-full` to rebuild with full optimizations

3. Profile binary size:
   ```bash
   make wasm-profile
   ```

### Catalog Operations Failing (R2 Data Catalog)

**Symptom:** Warning logs about catalog commit failures, but Parquet files are written.

**Cause:** R2 Data Catalog not enabled or misconfigured.

**Solution:**

1. Check logs for specific errors:
   ```bash
   wrangler tail
   ```

2. Verify `OTLP2PARQUET_CATALOG_TYPE="r2"` is set:
   ```toml
   [vars]
   OTLP2PARQUET_CATALOG_TYPE = "r2"
   ```

3. Confirm R2 bucket has Data Catalog feature enabled (Cloudflare dashboard)

4. **Remember:** Parquet files are written even if catalog fails (warn-and-succeed pattern). Data is not lost.

---

## Storage / Object Storage Issues

### Parquet Files Not Created

**Symptom:** No files appear in object storage (S3/R2) after sending data.

**Cause:** Storage credentials, bucket permissions, or configuration errors.

**Solution:**

1. Verify storage configuration:
   ```bash
   # Lambda
   aws lambda get-function-configuration \
     --function-name your-function \
     --query 'Environment.Variables'

   # Cloudflare Workers
   wrangler tail  # Check for storage errors
   ```

2. Check bucket exists and is accessible:
   ```bash
   # S3
   aws s3 ls s3://your-bucket/

   # R2
   wrangler r2 object list your-bucket
   ```

3. Verify IAM permissions (S3) or API tokens (R2):
   - S3: `s3:PutObject`, `s3:ListBucket`
   - R2: "R2 Write" permission

4. Check CloudWatch/Worker logs for specific error messages

### Incorrect Partition Paths

**Symptom:** Files are created but in unexpected locations.

**Cause:** Incorrect service name extraction or timestamp handling.

**Solution:**

1. Expected partition format:
   ```
   {signal}/{service}/year={year}/month={month}/day={day}/hour={hour}/file.parquet
   ```
   Example: `logs/my-service/year=2025/month=01/day=16/hour=14/abc123.parquet`

2. Verify service name in OTLP data:
   ```json
   {
     "resourceLogs": [{
       "resource": {
         "attributes": [{
           "key": "service.name",
           "value": { "stringValue": "my-service" }
         }]
       }
     }]
   }
   ```

3. Check timestamp is valid in OTLP data

---

## Catalog Connection Issues

### Catalog Connection Failures (Nessie / Glue)

**Symptom:** Logs show catalog connection errors or timeouts.

**Cause:** Network issues, incorrect REST URI, or catalog unavailable.

**Solution:**

1. Verify catalog is reachable:
   ```bash
   # Nessie
   curl http://nessie:19120/api/v1/config

   # Glue
   aws glue get-database --name otel
   ```

2. Check REST URI configuration:
   ```bash
   # Should match catalog endpoint
   echo $OTLP2PARQUET_ICEBERG_REST_URI
   ```

3. Verify network connectivity:
   ```bash
   # Docker
   docker network inspect otlp2parquet_default

   # Server
   ping nessie  # or telnet nessie 19120
   ```

4. **Remember:** Catalog failures don't block Parquet writes (warn-and-succeed pattern)

### Schema Mismatch Errors

**Symptom:** Catalog rejects writes due to schema incompatibility.

**Cause:** Schema evolution not supported or configured incorrectly.

**Solution:**

1. Check Iceberg format version supports schema evolution:
   ```toml
   [iceberg]
   format_version = 2  # Version 2 required for schema evolution
   ```

2. Verify catalog supports schema updates:
   - S3 Tables: ✅ Full support
   - R2 Data Catalog: ✅ Supported
   - Nessie: ✅ Supported
   - Glue: ✅ Supported

3. Review schema changes in logs for compatibility issues

---

## Performance Issues

### Slow Write Performance

**Symptom:** High latency when writing OTLP data.

**Cause:** Small batches, network latency, or catalog overhead.

**Solution:**

1. Enable batching (server only):
   ```toml
   [batch]
   enabled = true
   max_rows = 10000
   max_bytes = 10485760  # 10MB
   max_age_secs = 300     # 5 minutes
   ```

2. For Lambda/Workers, implement upstream batching (OTel Collector):
   ```yaml
   exporters:
     otlphttp:
       endpoint: https://your-endpoint
       sending_queue:
         queue_size: 1000
   ```

3. Increase Parquet row group size:
   ```toml
   [storage]
   parquet_row_group_size = 10000
   ```

4. Consider plain Parquet mode if catalog overhead is too high

### High Memory Usage

**Symptom:** Lambda/Worker runs out of memory or is throttled.

**Cause:** Large batches or payloads exceeding available memory.

**Solution:**

1. Reduce batch size:
   ```bash
   OTLP2PARQUET_BATCH_MAX_ROWS=5000
   OTLP2PARQUET_BATCH_MAX_BYTES=5242880  # 5MB
   ```

2. Reduce max payload size:
   ```bash
   OTLP2PARQUET_MAX_PAYLOAD_BYTES=5242880  # 5MB
   ```

3. Increase Lambda memory allocation:
   ```yaml
   MemorySize: 1024  # More memory = more CPU too
   ```

4. Monitor actual memory usage:
   ```bash
   # Lambda
   aws logs filter-pattern "Max Memory Used" \
     --log-group-name /aws/lambda/your-function
   ```

---

## Debugging Tips

### Enable Debug Logging

**Lambda:**
```yaml
Environment:
  Variables:
    RUST_LOG: debug
```

**Server:**
```toml
[server]
log_level = "debug"
```

**Cloudflare Workers:**
```bash
wrangler tail --format pretty
```

### Check CloudWatch/Worker Logs

**Lambda:**
```bash
aws logs tail /aws/lambda/your-function --follow --region us-west-2
```

**Cloudflare Workers:**
```bash
wrangler tail --format json | jq
```

### Verify Configuration

**Lambda:**
```bash
aws lambda get-function-configuration --function-name your-function
```

**Server:**
```bash
# Dry-run to see parsed config
RUST_LOG=debug ./otlp2parquet-server --help
```

### Test Catalog Connectivity

**Manually test catalog endpoints:**

```bash
# Nessie
curl http://nessie:19120/api/v1/config

# Glue (requires AWS credentials)
aws glue get-database --name otel

# S3 Tables
aws s3tables get-table-bucket --table-bucket-arn arn:aws:s3tables:...
```

---

## Common Error Messages

| Error Message | Cause | Solution |
|---------------|-------|----------|
| `AccessDenied` | Missing IAM permissions | Review and add required permissions |
| `ThrottlingException` | API rate limits exceeded | Implement batching, request quota increase |
| `TableNotFound` | Table not yet created | Send data first (tables auto-created) |
| `SchemaIncompatible` | Schema evolution issue | Check format version, review schema changes |
| `PayloadTooLarge` | Request exceeds size limit | Increase max payload or reduce batch size |
| `ConnectionRefused` | Catalog unreachable | Verify network, check catalog is running |
| `InvalidBucketARN` | Malformed ARN format | Verify ARN format: `arn:aws:s3tables:region:account:bucket/name` |

---

## Getting Help

If you're still stuck after reviewing this guide:

1. **Check logs** - Most issues have detailed error messages in logs
2. **Review configuration** - Verify all environment variables are set correctly
3. **Test incrementally** - Start with plain Parquet, then add catalog
4. **Search issues** - Check [GitHub Issues](https://github.com/smithclay/otlp2parquet/issues)
5. **Report bugs** - Open a new issue with logs and configuration (sanitized)

---

## Related Documentation

- [Environment Variables Reference](../reference/environment-variables.md) - All configuration options
- [Catalog Types Reference](../reference/catalog-types.md) - Detailed catalog setup
- [AWS Lambda Setup](../setup/aws-lambda.md) - Lambda-specific configuration
- [Cloudflare Workers Setup](../setup/cloudflare.md) - Workers-specific configuration
