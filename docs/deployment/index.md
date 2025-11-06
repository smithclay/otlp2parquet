# Deployment Overview

`otlp2parquet` is designed to run in a variety of environments, from a local Docker container to a globally-distributed edge network. This guide provides a high-level comparison to help you choose the best deployment option for your needs.

## Comparison of Platforms

| Platform | Best For | Cost Model | Scalability | Setup | Guide |
| :--- | :--- | :--- | :--- | :--- | :--- |
| [**Docker**](docker.md) | Local development, private cloud, or self-hosting. | Fixed (cost of host) | Manual | **Low** | [View Guide](docker.md) |
| [**AWS Lambda**](aws-lambda.md) | Integration with S3 / S3 Tables / AWS analytics solutions. | Pay-per-use | Automatic | **Medium** | [View Guide](aws-lambda.md) |
| [**Cloudflare**](cloudflare.md) | Integration with R2 (free egress) or R2 Data Catalog | Pay-per-use | Automatic | **Medium** | [View Guide](cloudflare.md) |
