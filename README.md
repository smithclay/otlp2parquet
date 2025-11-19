# otlp2parquet

[![CI](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Put your observability data in cost-effective cloud object storage, servers optional.

`otlp2parquet` ingests OpenTelemetry logs, metrics, and traces and stores them in object storage as Parquet files with optional Apache Iceberg catalog integration. Deploy in 3 minutes to AWS Lambda with S3 Tables, run self-hosted with Docker, or deploy to the edge with Cloudflare Workers using R2 Data Catalog.

**Key Features:**
- ✅ Ingests OTLP HTTP (protobuf, JSON, JSONL) for logs, metrics, and traces
- ✅ Writes Parquet files to object storage
- ✅ Optional Apache Iceberg support via [icepick](https://crates.io/crates/icepick)
- ✅ Native AWS Lambda -> AWS S3 Tables support (ARN-based configuration)
- ✅ R2 Data Catalog support for Cloudflare Workers (Webassembly)
- ✅ Small and fast: 1.3 MB WASM binary, <10ms cold start

---

## Quick Start: AWS Lambda (3 minutes)

Deploy serverless OTLP ingestion with S3 storage to AWS:

```bash
# 1. Download CloudFormation template
curl -O https://raw.githubusercontent.com/smithclay/otlp2parquet/main/examples/aws-lambda-s3-tables/template.yaml

# 2. Deploy stack
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name otlp2parquet \
  --region us-west-2 \
  --capabilities CAPABILITY_NAMED_IAM

# 3. Get function URL
aws cloudformation describe-stacks \
  --stack-name otlp2parquet \
  --query 'Stacks[0].Outputs[?OutputKey==`FunctionUrl`].OutputValue' \
  --output text

# 4. Send test data (requires curl 7.75+ for AWS SigV4 auth)
curl -X POST <function-url>/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"Hello"},"timeUnixNano":"1234567890"}]}]}]}' \
  --aws-sigv4 "aws:amz:us-west-2:lambda" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"

# 5. Query with duckdb
```

**That's it!** Data is written to S3 as Parquet files, ready to query.

📖 [Full AWS Lambda Guide →](docs/setup/aws-lambda.md)

---

## Other Deployment Options

### 🐳 Docker (Server)

Long-running HTTP server for continuous ingestion with in-memory batching:

```bash
docker-compose up
```

- In-memory batching for optimal Parquet file sizes
- Supports S3, R2, GCS, Azure, and local filesystem
- [Docker Deployment Guide →](docs/setup/docker.md)

### ⚡ Cloudflare Workers

Edge-native OTLP ingestion with WASM:

```bash
npx wrangler deploy
```

- 1.3MB compressed binary, <10ms cold start
- R2 Data Catalog support for Iceberg tables
- R2 storage integration
- [Cloudflare Workers Guide →](docs/setup/cloudflare.md)

---

## Platform & Feature Matrix

| Feature / Platform | Docker (Server) | Cloudflare Workers | AWS Lambda |
| :----------------- | :----------------- | :----------------- | :--------- |
| **OTLP Protocol Support** | | | |
| OTLP/HTTP-JSON | ✅ | ✅ | ✅ |
| OTLP/HTTP-Protobuf | ✅ | ✅ | ✅ |
| OTLP/gRPC | ❌ | ❌ | ❌ |
| **Signal Support** | | | |
| OTLP Logs | ✅ | ✅ | ✅ |
| OTLP Metrics | ✅ | ✅ | ✅ |
| OTLP Traces | ✅ | ✅ | ✅ |
| OTLP Profiles | ❌ | ❌ | ❌ |
| **Storage Support** | | | |
| Plain Object Storage (Parquet) | ✅ | ✅ | ✅ |
| Iceberg (REST Catalog API) | ✅ | ❌ | ✅ |
| AWS S3 Tables | ❌ | ❌ | ✅ |
| R2 Data Catalog | ❌ | ✅ | ❌ |
| **Serverless Function Authentication** | | | |
| Basic Auth Header | ❌ | ✅ | ❌ |
| AWS IAM (SigV4) | ❌ | ❌ | ✅ |
| **Advanced Features** | | | |
| In-Memory Batching | ✅ | ❌ | ❌ |

---

## Use Cases

- **Cost-effective observability** - Store TBs of telemetry for <$10/month with S3/R2
- **Long-term retention** - Keep logs and traces beyond typical 30-day vendor limits
- **Data lake integration** - Query telemetry with Spark, DuckDB, Athena, Trino
- **Serverless analytics** - Process telemetry without managing infrastructure
- **Compliance & audit** - Archive observability data for regulatory requirements

---

## How It Works

```
OTLP Client → otlp2parquet → Parquet files → Query Engines
                              (S3/R2/GCS)      (DuckDB/Athena/Spark)
```

1. **Ingest**: Applications send OTLP HTTP (protobuf/JSON) to otlp2parquet endpoint
2. **Transform**: Converts to Arrow RecordBatch with ClickHouse-compatible schema
3. **Store**: Writes Parquet files to object storage (S3, R2, GCS, Azure)
4. **Catalog** (optional): Commits to Apache Iceberg catalog for ACID guarantees
5. **Query**: Analyze with DuckDB, Athena, Spark, Trino, or other engines

---

## Documentation

➡️ [**View Full Documentation**](https://smithclay.github.io/otlp2parquet/)

### Getting Started

- [AWS Lambda Deployment](docs/setup/aws-lambda.md) - 3-minute serverless deployment
- [Docker Deployment](docs/setup/docker.md) - Self-hosted server
- [Cloudflare Workers Deployment](docs/setup/cloudflare.md) - Edge deployment

### Guides

- [Sending Data from Applications](docs/guides/sending-data.md)
- [Configuration Reference](docs/concepts/configuration.md)

### Advanced

- [Local Development with SAM CLI](docs/guides/lambda-local-development.md)
- [Using Plain S3 without Iceberg](docs/guides/plain-s3.md)
- [Architecture Overview](docs/concepts/architecture.md)

---

## Important Notes

### Batching for Cost Optimization

Lots of small files written to cloud object storage will explode your bill and impact performance.

For serverless deployments (Lambda, Workers), batching **must** be handled by an upstream agent like [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector) or [Vector](https://github.com/vectordotdev/vector). Apache Iceberg solutions like [S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-tables.html) can [compact Parquet files automatically](https://aws.amazon.com/blogs/storage/how-amazon-s3-tables-use-compaction-to-improve-query-performance-by-up-to-3-times/) to improve query performance.

For Docker (server) deployments, in-memory batching is built-in and enabled by default.

### Parquet Schema

The current schema is based on the ClickHouse OpenTelemetry exporter schema, similar to the [duckdb-otlp extension](https://github.com/smithclay/duckdb-otlp). This project serves as a bridge to the official [OpenTelemetry Arrow Protocol](https://github.com/open-telemetry/otel-arrow) (OTAP) under active development.

We plan to converge with the official protocol once it becomes generally available, which will make much of the OTLP→Arrow translation this tool does unnecessary.

### Project Status

While functional and used in production, the project is actively evolving. The API and schema may change as we align with OTAP standards. In production scenarios, otlp2parquet is typically paired with [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/), [Vector](https://vector.dev/), or similar tools to perform transformations, routing, and batching.

_Note: If you want to query or convert existing OTLP files, see the [otlp-duckdb](https://github.com/smithclay/duckdb-otlp) extension._

---

## Related Projects

Other projects with OTLP to Parquet support:

- https://github.com/open-telemetry/opentelemetry-rust
- https://github.com/Mooncake-Labs/moonlink
- https://github.com/streamfold/rotel

---

## License

Apache-2.0
