# AWS S3 Tables Commit Transaction Issue

## Status: ⚠️ OPEN ISSUE

**Created**: 2025-11-07
**Component**: Iceberg REST catalog transaction commit
**Severity**: Medium - Tables are created and files are written, but not registered in catalog metadata

## Summary

After successfully fixing the table creation issue (duplicate field IDs), we discovered that AWS S3 Tables returns HTTP 404 "UnknownOperationException" when attempting to commit data files via the Iceberg REST API transaction endpoint.

**Impact**:
- ✅ Tables are created successfully
- ✅ Parquet files are written to S3
- ❌ Files are NOT registered in the Iceberg catalog metadata
- ❌ Queries will not find the data files

## Symptom

When Lambda attempts to commit Parquet data files after writing:

```
WARN Failed to commit to Iceberg catalog for table 'logs':
HTTP 404 error from catalog: <UnknownOperationException/>
. File written but not cataloged.
```

## Technical Details

### What Works
1. ✅ Table creation via REST API (`POST /v1/{prefix}/namespaces/{namespace}/tables`)
   - Returns HTTP 200
   - Tables visible in `aws s3tables list-tables`
   - Schema with unique field IDs accepted

2. ✅ Parquet file writing
   - Files successfully written to S3 data bucket
   - Field IDs in Parquet match Iceberg schema

### What Fails
❌ Transaction commit via REST API

**Endpoint being called**:
```
POST https://s3tables.us-west-2.amazonaws.com/iceberg/v1/{encoded_arn}/transactions/commit
```

**Request body** (standard Iceberg REST format):
```json
{
  "table-changes": [
    {
      "action": "append",
      "data-files": [
        {
          "file-path": "s3://bucket/path/file.parquet",
          "file-format": "PARQUET",
          "partition": {},
          "record-count": 100,
          "file-size-in-bytes": 12345,
          "column-sizes": {...},
          "value-counts": {...},
          ...
        }
      ]
    }
  ]
}
```

**Response**:
```xml
<UnknownOperationException/>
```

HTTP Status: 404

## Root Cause Analysis

### Hypothesis 1: S3 Tables Uses Different Commit Endpoint
S3 Tables may not implement the standard Iceberg REST `/transactions/commit` endpoint and instead requires a different approach.

**Evidence**:
- AWS documentation doesn't explicitly mention transaction commits via REST API
- S3 Tables may expect commits through AWS SDK APIs instead
- Other Iceberg REST catalogs (Nessie) support this endpoint

**Investigation needed**:
- Check AWS S3 Tables API documentation for commit operations
- Look for S3 Tables-specific commit endpoints
- Examine boto3/AWS SDK examples

### Hypothesis 2: Missing S3 Tables-Specific Headers/Parameters
The transaction commit may require additional AWS-specific headers or request parameters.

**Evidence**:
- Table creation works but uses different endpoint
- AWS SigV4 signing is working (otherwise we'd get 403)
- May need table ARN or specific metadata

**Investigation needed**:
- Compare with official AWS S3 Tables examples
- Check if commit requires table ARN in URL
- Review CloudTrail logs for successful commits

### Hypothesis 3: S3 Tables Doesn't Support REST Commits
S3 Tables may only support commits through its proprietary API, not the Iceberg REST standard.

**Evidence**:
- Limited documentation on S3 Tables REST API features
- S3 Tables is relatively new (2024)
- May have partial Iceberg REST support

**Investigation needed**:
- Contact AWS support for S3 Tables REST API capabilities
- Check if AWS provides alternative commit mechanisms
- Look for S3 Tables + Iceberg integration examples

## Code Location

**Commit implementation**: `crates/otlp2parquet-storage/src/iceberg/catalog.rs:305-444`

```rust
pub async fn commit_append(
    &self,
    table_name: &str,
    data_files: Vec<DataFile>,
) -> Result<()> {
    // Constructs POST /v1/{prefix}/transactions/commit
    let url = format!(
        "{}/v1/{}/transactions/commit",
        self.base_url, self.prefix
    );

    // Sends standard Iceberg REST transaction request
    // Returns 404 UnknownOperationException
}
```

## Workaround

Currently using "warn-and-succeed" pattern:
1. Parquet files are written to S3
2. Commit attempt fails with warning
3. Ingestion continues successfully
4. **Manual fix needed**: Register files in catalog separately

## Impact Assessment

### Production Readiness: ⚠️ PARTIAL

**What works for production**:
- OTLP data ingestion ✅
- Parquet file generation ✅
- S3 storage ✅
- Table schema management ✅

**What's missing**:
- Automatic catalog metadata updates ❌
- Query engines won't discover new data ❌
- Manual catalog maintenance required ❌

### Use Cases Affected

**✅ Works for**:
- Raw Parquet file storage
- Direct Parquet file querying (DuckDB, Pandas with explicit file paths)
- Batch catalog updates via external tools

**❌ Broken for**:
- Athena queries (needs catalog metadata)
- Spark queries via Iceberg catalog
- Time-travel queries
- Automated data discovery

## Next Steps

### Immediate (P0)
1. **Research S3 Tables documentation**
   - Find official commit operation examples
   - Check if REST API supports commits
   - Identify correct endpoint/method

2. **Test with AWS SDK**
   - Try committing via boto3 `update_table_metadata`
   - Compare with REST API approach
   - Document working method

3. **Contact AWS Support**
   - Ask about Iceberg REST API transaction support in S3 Tables
   - Request examples of data file commits
   - Clarify intended integration path

### Short-term (P1)
4. **Implement alternative commit mechanism**
   - Use AWS SDK if REST doesn't work
   - Add S3 Tables-specific commit path
   - Maintain compatibility with standard Iceberg catalogs

5. **Add integration tests**
   - Test commit operation end-to-end
   - Verify files are queryable after commit
   - Ensure catalog metadata is updated

### Long-term (P2)
6. **Document S3 Tables integration patterns**
   - Create guide for S3 Tables vs generic Iceberg REST
   - Document any S3 Tables-specific requirements
   - Add troubleshooting guide

7. **Consider alternative catalogs**
   - Evaluate AWS Glue as alternative
   - Consider Tabular.io or other managed catalogs
   - Document trade-offs

## References

### AWS Documentation
- [S3 Tables User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html)
- [S3 Tables Iceberg Integration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html)
- [S3 Tables API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_UpdateTableMetadataLocation.html)

### Iceberg Specifications
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/rest-catalog-spec/)
- [Iceberg Table Spec - Snapshots](https://iceberg.apache.org/spec/#snapshots)
- [REST OpenAPI Definition](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

### Related Issues
- [CREATE_TABLE_ISSUE.md](./CREATE_TABLE_ISSUE.md) - Duplicate field IDs issue (✅ RESOLVED)

## Testing

To reproduce the commit failure:

```bash
# Deploy Lambda
cd examples/aws-lambda-s3-tables
./deploy.sh --local-binary ../../target/lambda/bootstrap-arm64.zip

# Send test data
./test.sh --region us-west-2 --verbose

# Check CloudWatch logs
aws logs tail /aws/lambda/otlp2parquet-ingest --region us-west-2 --follow
```

**Expected behavior**: Files written, commit warning appears in logs
**Desired behavior**: Files written AND committed successfully to catalog

## Updates

### 2025-11-07 - Initial Documentation
- Documented issue after successful table creation fix
- Identified HTTP 404 on transaction commit endpoint
- Outlined investigation paths and workarounds
