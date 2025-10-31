# Cloudflare Workers Deployment Guide

Deploy `otlp2parquet` to Cloudflare Workers with R2 storage. Perfect for edge compute with global distribution.

## Prerequisites

1. **Cloudflare Account** (free tier works!)
2. **Node.js** 18+ and npm
3. **Wrangler CLI**

```bash
# Install Wrangler globally
npm install -g wrangler

# Or use with npx (no installation needed)
npx wrangler --version
```

## Quick Start (Deploy Button) ⚡

**The easiest way to deploy:**

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/smithclay/otlp2parquet)

This will:
1. Fork the repository to your GitHub account
2. Prompt you to connect your Cloudflare account
3. Create an R2 bucket for log storage
4. Deploy the Worker automatically

## Manual Deployment

### 1. Clone the Repository

```bash
git clone https://github.com/smithclay/otlp2parquet.git
cd otlp2parquet
```

### 2. Login to Cloudflare

```bash
wrangler login
```

This opens a browser window to authenticate.

### 3. Create R2 Bucket

```bash
# Create production bucket
wrangler r2 bucket create otlp-logs

# Optional: Create preview/dev bucket
wrangler r2 bucket create otlp-logs-preview
```

### 4. Update wrangler.toml

Edit `wrangler.toml` to set your bucket name:

```toml
[[r2_buckets]]
binding = "LOGS_BUCKET"
bucket_name = "otlp-logs"  # Change to your bucket name
preview_bucket_name = "otlp-logs-preview"
```

### 5. Build WASM Binary

```bash
# Install Rust toolchain if not already installed
rustup target add wasm32-unknown-unknown

# Install wasm-opt (for size optimization)
# macOS
brew install binaryen

# Linux
wget https://github.com/WebAssembly/binaryen/releases/latest/download/binaryen-*-x86_64-linux.tar.gz
tar xzf binaryen-*-x86_64-linux.tar.gz
sudo cp binaryen-*/bin/wasm-opt /usr/local/bin/

# Build optimized WASM
make build-cloudflare
```

### 6. Deploy to Cloudflare

```bash
# Deploy to production
wrangler deploy

# Deploy to specific environment
wrangler deploy --env staging
wrangler deploy --env production
```

You'll see output like:

```
✨ Successfully published your Worker!
🌍 https://otlp2parquet.your-subdomain.workers.dev
```

## Test Your Deployment

```bash
# Get your Worker URL from wrangler output
WORKER_URL="https://otlp2parquet.your-subdomain.workers.dev"

# Send a test OTLP log payload
curl -X POST "${WORKER_URL}/v1/logs" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @test-payload.pb

# Check R2 bucket for Parquet files
wrangler r2 object list otlp-logs
```

## Local Development

```bash
# Run Worker locally with Miniflare
wrangler dev

# Worker will be available at http://localhost:8787
# Uses preview R2 bucket (otlp-logs-preview)

# Test locally
curl -X POST http://localhost:8787/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @test-payload.pb
```

## Configuration

### Environment Variables

Set environment variables in `wrangler.toml`:

```toml
[vars]
LOG_LEVEL = "info"
MAX_BATCH_SIZE = "1000"
```

### Secrets (for sensitive data)

```bash
# Set secrets via CLI (not in wrangler.toml!)
wrangler secret put API_KEY
# Prompts for value

# List secrets
wrangler secret list

# Delete secret
wrangler secret delete API_KEY
```

### Custom Domain

```toml
# In wrangler.toml
[env.production]
route = "logs.example.com/*"
workers_dev = false
```

Then configure DNS in Cloudflare dashboard:
1. Go to DNS settings
2. Add CNAME record: `logs` → `your-subdomain.workers.dev`

## Multiple Environments

The included `wrangler.toml` supports three environments:

### Development
```bash
wrangler dev
# Uses otlp-logs-dev bucket
```

### Staging
```bash
wrangler deploy --env staging
# Deploys to: otlp2parquet-staging.workers.dev
# Uses otlp-logs-staging bucket
```

### Production
```bash
wrangler deploy --env production
# Deploys to your custom domain or otlp2parquet.workers.dev
# Uses otlp-logs-production bucket
```

## Monitoring

### View Logs

```bash
# Stream logs in real-time
wrangler tail

# Filter logs
wrangler tail --format pretty
wrangler tail --status error
```

### Cloudflare Dashboard

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. Navigate to Workers & Pages
3. Click your Worker name
4. View analytics, metrics, and logs

## Limits (Free Tier)

| Metric | Free Tier | Paid ($5/mo) |
|--------|-----------|--------------|
| Requests/day | 100,000 | 10M+ |
| CPU time | 10ms/req | 30s/req |
| R2 Storage | 10 GB | Unlimited* |
| R2 Reads | 1M/mo | Unlimited* |
| R2 Writes | 1M/mo | Unlimited* |

*Paid tier charges per operation after free tier

## CI/CD with GitHub Actions

Create `.github/workflows/deploy-cloudflare.yml`:

```yaml
name: Deploy to Cloudflare

on:
  push:
    branches: [main]
    tags: ['v*']

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5

      - name: Build WASM
        run: make build-cloudflare

      - name: Deploy to Cloudflare
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          accountId: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
```

Get API token from: https://dash.cloudflare.com/profile/api-tokens

## Troubleshooting

### Build fails - WASM too large

```bash
# Check WASM size
make wasm-size

# If over 3MB, review dependencies:
make wasm-profile

# Look for large dependencies and optimize
```

### R2 bucket access denied

```bash
# Verify bucket exists
wrangler r2 bucket list

# Check wrangler.toml binding name matches code
# Binding: "LOGS_BUCKET" in wrangler.toml
# Code: env.LOGS_BUCKET.put()
```

### Worker not receiving requests

```bash
# Check routes
wrangler deployments list

# Test with curl
curl -v https://your-worker.workers.dev/v1/logs

# Check Worker logs
wrangler tail
```

## Cost Estimation

**Free tier (sufficient for testing):**
- 100,000 requests/day
- ~3 requests/second sustained
- 10 GB R2 storage
- 1M R2 operations/month

**Paid tier ($5/month + usage):**
- $0.50 per million requests (after 10M)
- R2 storage: $0.015/GB/month
- R2 operations: Free up to limits, then $0.36 per million

**Example costs for 1M logs/day:**
- Requests: ~$0.50/day ($15/month)
- R2 storage (100GB Parquet): $1.50/month
- R2 writes: Free (within limits)

**Total: ~$20-25/month for 1M logs/day**

## Next Steps

- [Configure custom domain](https://developers.cloudflare.com/workers/configuration/routing/custom-domains/)
- [Set up analytics](https://developers.cloudflare.com/workers/observability/analytics/)
- [Enable rate limiting](https://developers.cloudflare.com/workers/runtime-apis/bindings/rate-limit/)
- [Main documentation](../../README.md)
