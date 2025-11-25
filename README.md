# otlp2parquet

[![CI](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Stream observability data to cloud object storage, servers optional.

`otlp2parquet` ingests OpenTelemetry logs, metrics, and traces and stores them in object storage as Parquet files. Optionally, you can [Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html), [Cloudflare R2 Data Catalog](https://developers.cloudflare.com/r2/data-catalog/) or other Apache Iceberg-compatible services to integrate into a data lake for analytics.

**Features:**
- ✅ Ingests OpenTelemetry (OTLP) over HTTP (protobuf, JSON, JSONL) for logs, metrics, and traces
- ✅ Writes Parquet files to object storage
- ✅ Native AWS Lambda -> AWS S3 Tables support (ARN-based configuration)
- ✅ Native Cloudflare Worker -> R2 Data Catalog support (Webassembly)
- ✅ Append-only Apache Iceberg support
- ✅ Small and fast: 2 MB WASM binary, <10ms cold start

---

## Platform & Feature Matrix

| Feature / Platform | CLI / Server | Cloudflare Workers | AWS Lambda |
| :----------------- | :----------------- | :----------------- | :--------- |
| **OTLP Protocol Support** | | | |
| OTLP/HTTP-JSON | ✅ | ✅ | ✅ |
| OTLP/HTTP-Protobuf | ✅ | ✅ | ✅ |
| OTLP/gRPC | ❌ | ❌ | ❌ |
| **Signal Support** | | | |
| OTLP Metrics, Logs, Traces | ✅ | ✅ | ✅ |
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
- **Data lake integration** - Query telemetry with Spark, duckdb, Athena, Trino
- **Serverless analytics** - Process telemetry without managing infrastructure
- **Compliance & audit** - Archive observability data for regulatory requirements

---

## Quick Start

### macOS/Linux CLI

**Install from source:**
```bash
git clone https://github.com/smithclay/otlp2parquet
cd otlp2parquet
make build-cli
./target/release/otlp2parquet
```

Server starts on http://localhost:4318 with data written to `./data`.

**Common options:**
```bash
# Custom port and output directory
otlp2parquet --port 8080 --output ./my-data

# Enable debug logging
otlp2parquet --log-level debug

# Use custom config file
otlp2parquet --config prod.toml
```

**Need help?** Run `otlp2parquet --help`

---

## How It Works

```
OTLP Client → otlp2parquet → Parquet files ⬅ Query Engines
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

- [CLI Deployment](docs/setup/cli.md) - Local development with native binary
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

### Major limitation: Batching support

Lots of small files written to cloud object storage will explode your bill, impact performance.

For serverless deployments (Lambda, Workers), batching **must** be handled by an upstream agent like [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector) or [Vector](https://github.com/vectordotdev/vector). Apache Iceberg solutions like [S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-tables.html) can [compact Parquet files automatically](https://aws.amazon.com/blogs/storage/how-amazon-s3-tables-use-compaction-to-improve-query-performance-by-up-to-3-times/) to improve query performance.

For Docker (server) deployments, in-memory batching is built-in and enabled by default.

### Schema

The current schema is based on the ClickHouse OpenTelemetry exporter schema, similar to the [duckdb-otlp extension](https://github.com/smithclay/duckdb-otlp). This is not the most efficicent schema from a storage perpsective but it is realatively easy to implement and query since signal data in consolidated in unique tables.

A longer-term goal of this project is converging with the [OpenTelemetry Arrow Protocol](https://github.com/open-telemetry/otel-arrow) (OTAP) schemas under active development.

We plan to converge with the official protocol once it becomes generally available, which will make much of the OTLP→Arrow translation this tool does unnecessary.

### Project Status

While functional, the project is actively evolving and experimental. The API and schema may change as we align with OTAP standards.

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
