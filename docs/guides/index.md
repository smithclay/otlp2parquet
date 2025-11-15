# Guides

This section contains practical, goal-oriented guides for common tasks and advanced setups.

*   [**Sending Telemetry Data**](./sending-data.md)
    *   Learn how to send logs, traces, and metrics, either directly or via the OpenTelemetry Collector.

*   [**Guide: AWS Lambda with S3 Tables**](./aws-lambda-s3-tables.md)
    *   A deep-dive into deploying a serverless ingestion pipeline using Lambda and Iceberg on S3 Tables.

*   [**Guide: Lambda Local Development**](./lambda-local-development.md)
    *   Develop and test Lambda functions locally using the AWS SAM CLI and a local S3 (MinIO) instance.

*   [**Guide: Plain S3 Storage**](./plain-s3.md)
    *   Learn how to use `otlp2parquet` without an Iceberg catalog, writing directly to S3.
