# Setup Overview

This guide helps you choose the best setup option for your needs.

`otlp2parquet` can run in various environments, from a local Docker container to a globally-distributed edge network.

## Comparison of Platforms

| Platform | Best For | Cost Model | Scalability | Complexity | Guide |
| :--- | :--- | :--- | :--- | :--- | :--- |
| [**Docker**](docker.md) | Local development, private cloud, or self-hosting. | Fixed (cost of host) | Manual | **Low** | [View Guide](./docker.md) |
| [**AWS Lambda**](aws-lambda.md) | Integration with the AWS ecosystem (S3, Glue, etc.). | Pay-per-use | Automatic | **Medium** | [View Guide](./aws-lambda.md) |
| [**Cloudflare**](cloudflare.md) | Integration with R2 and other Cloudflare services. | Pay-per-use | Automatic | **Medium** | [View Guide](./cloudflare.md) |
