# Standalone Server Deployment

## Prerequisites
- Linux or macOS host with systemd or container runtime.
- Rust stable toolchain and Cargo.
- Object storage credentials for filesystem, S3-compatible, or R2.
- Optional: `just` or `make` for convenience targets.

## Build
1. **Compile the optimized binary.**
   ```bash
   cargo build --release --features server
   ```
   Expected output: `Finished release [optimized] target(s) in ...`.
2. **Strip symbols to reduce binary size (optional).**
   ```bash
   strip ./target/release/otlp2parquet
   ```
   Expected output: Binary size reduced when running `ls -lh`.

## Configure
1. **Create environment file.**
   ```bash
   cat <<'ENV' > /etc/otlp2parquet.env
   STORAGE_BACKEND=s3
   S3_BUCKET=logs-<ENV>
   S3_REGION=<REGION>
   LOG_FORMAT=json
   ENV
   ```
   Expected output: `/etc/otlp2parquet.env` written with deployment values.
2. **Grant least-privilege IAM credentials.**
   ```bash
   aws iam create-policy --policy-name otlp2parquet-write \
     --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutObject","s3:AbortMultipartUpload"],"Resource":"arn:aws:s3:::logs-<ENV>/*"}]}'
   ```
   Expected output: Policy ARN for later attachment.

## Deploy
1. **Run as a systemd service.**
   ```bash
   sudo tee /etc/systemd/system/otlp2parquet.service <<'UNIT'
   [Unit]
   Description=otlp2parquet Server
   After=network.target

   [Service]
   EnvironmentFile=/etc/otlp2parquet.env
   ExecStart=/usr/local/bin/otlp2parquet
   Restart=on-failure

   [Install]
   WantedBy=multi-user.target
   UNIT
   ```
   Expected output: Service file created under `/etc/systemd/system/`.
2. **Install and start the service.**
   ```bash
   sudo install -m 755 ./target/release/otlp2parquet /usr/local/bin/otlp2parquet
   sudo systemctl daemon-reload
   sudo systemctl enable --now otlp2parquet
   ```
   Expected output: `Active: active (running)` from `systemctl status`.

## Verify
1. **Check health endpoint.**
   ```bash
   curl -sS http://127.0.0.1:8080/health
   ```
   Expected output: `{"status":"ok"}`.
2. **Send test log.**
   ```bash
   curl -sS -X POST http://127.0.0.1:8080/v1/logs \
     -H "Content-Type: application/json" \
     -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"server-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1710000000000000000","severityText":"INFO","body":{"stringValue":"Server log"}}]}]}]}'
   ```
   Expected output: `{}` (empty JSON response).
3. **Confirm object creation.**
   ```bash
   aws s3 ls s3://logs-<ENV>/logs/server-demo/ --recursive --summarize
   ```
   Expected output: Listing shows one Parquet object.

Now you should have otlp2parquet running at http://127.0.0.1:8080/v1/logs.

## Rollback
1. **Revert to previous binary.**
   ```bash
   sudo cp /usr/local/bin/otlp2parquet{,.bak}
   sudo cp /var/backups/otlp2parquet-previous /usr/local/bin/otlp2parquet
   sudo systemctl restart otlp2parquet
   ```
   Expected output: Service restarted with prior binary.
2. **Check journal for confirmation.**
   ```bash
   journalctl -u otlp2parquet -n 20
   ```
   Expected output: Log line `server mode started (rollback)`.

## Performance & Cost Tips
- Use filesystem backend for on-prem deployments to avoid egress costs.
- Configure `STORAGE_BUFFER_SIZE` (bytes) to batch writes; 8MB reduces S3 PUT frequency.
- Enable compression at the proxy layer (e.g., NGINX gzip) to reduce inbound bandwidth.
- Monitor memory via `PROMETHEUS_EXPORTER=true` to keep the process under 128MB for small hosts.
