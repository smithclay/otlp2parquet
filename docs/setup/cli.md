# CLI Deployment Guide

Run otlp2parquet as a native binary on macOS or Linux for local development.

## Installation

### From Source

```bash
git clone https://github.com/smithclay/otlp2parquet
cd otlp2parquet
make build-cli
```

Binary will be at `target/release/otlp2parquet`.

### Install to System

```bash
make build-cli
sudo make install-cli
```

Installs to `/usr/local/bin/otlp2parquet`.

## Usage

### Zero-Config Startup

The simplest way to run otlp2parquet:

```bash
otlp2parquet
```

This starts the server with sensible defaults:
- Listen on http://0.0.0.0:4318 (OTLP standard port)
- Write Parquet files to `./data`
- Log level: info
- Batching: enabled
- No Iceberg catalog (plain Parquet files)

### CLI Options

```bash
otlp2parquet [OPTIONS]

Options:
  -c, --config <FILE>      Path to configuration file
  -p, --port <PORT>        HTTP listen port [default: 4318]
  -o, --output <DIR>       Output directory for Parquet files [default: ./data]
  -v, --log-level <LEVEL>  Log level (trace, debug, info, warn, error) [default: info]
  -h, --help               Print help
  -V, --version            Print version
```

### Common Workflows

**Custom port and output:**
```bash
otlp2parquet --port 8080 --output /tmp/otlp-data
```

**Debug logging:**
```bash
otlp2parquet --log-level debug
```

**With config file:**
```bash
otlp2parquet --config ./config.toml
```

**All together:**
```bash
otlp2parquet -p 9000 -o ~/otlp-logs -v trace -c custom.toml
```

## Configuration Priority

Settings are loaded in this order (highest to lowest priority):

1. **CLI flags** - Explicit arguments like `--port 8080`
2. **Environment variables** - `OTLP2PARQUET_*` prefix
3. **Config file** - Specified via `--config` or default locations
4. **Platform defaults** - Built-in defaults for server mode

### Example

Given this config.toml:
```toml
[server]
listen_addr = "0.0.0.0:4318"
log_level = "info"
```

Running:
```bash
otlp2parquet --port 8080 --log-level debug
```

Results in:
- Port: 8080 (CLI flag wins)
- Log level: debug (CLI flag wins)
- Listen address: 0.0.0.0:8080 (port from CLI, host from default)

## Troubleshooting

### Permission Denied on Port

```
Error: Failed to bind to 0.0.0.0:80
Caused by: Permission denied (os error 13)
```

**Solution:** Ports below 1024 require root. Use a higher port:
```bash
otlp2parquet --port 8080
```

### Output Directory Not Writable

```
Error: Output directory '/restricted/path' is not writable
```

**Solution:** Choose a directory you have write access to:
```bash
otlp2parquet --output ~/otlp-data
```

### Config File Not Found

```
Error: Failed to load config from ./custom.toml
Caused by: No such file or directory
```

**Solution:** Check the path or omit `--config` to use defaults.

## Advanced Usage

### With Iceberg Catalog

Create config.toml:
```toml
catalog_mode = "iceberg"

[iceberg]
rest_uri = "http://localhost:19120/iceberg"
warehouse = "s3://my-bucket/warehouse"
namespace = "otel"
```

Run:
```bash
otlp2parquet --config config.toml
```

### With S3 Backend

Create config.toml:
```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-otlp-bucket"
region = "us-east-1"
```

Set AWS credentials:
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
otlp2parquet --config config.toml
```

Note: `--output` flag only works with filesystem backend.

## Next Steps

- [Send data from applications](../guides/sending-data.md)
- [Configuration reference](../concepts/configuration.md)
- [Query data with DuckDB](../guides/querying-data.md)
