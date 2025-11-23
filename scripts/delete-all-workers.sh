#!/bin/bash
# Delete all Cloudflare Workers and R2 objects matching a prefix
# Usage: ./delete-all-workers.sh [worker-prefix] [bucket-name]
#
# Arguments:
#   worker-prefix: Worker name prefix filter (default: smoke-workers)
#   bucket-name: R2 bucket to clean (optional, if provided cleans objects)
#
# Environment:
#   CLOUDFLARE_API_TOKEN: Cloudflare API token (required)
#   CLOUDFLARE_ACCOUNT_ID: Cloudflare account ID (required)

set -e

WORKER_PREFIX="${1:-smoke-workers}"
BUCKET_NAME="${2:-}"

echo "Cleaning up Cloudflare resources"
echo "Worker prefix: $WORKER_PREFIX"
echo ""

# Delete Workers matching prefix
echo "=== Deleting Workers matching prefix '$WORKER_PREFIX' ==="

# Use wrangler CLI to list workers via API
# Since wrangler doesn't have a simple "list all workers" command,
# we need to use the Cloudflare API directly via curl
workers_json=$(curl -s -X GET \
  "https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/workers/scripts" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
  -H "Content-Type: application/json")

# Check if API call succeeded
if echo "$workers_json" | jq -e '.success == true' >/dev/null 2>&1; then
  # Extract worker names matching prefix using jq
  matching_workers=$(echo "$workers_json" | jq -r ".result[] | select(.id | startswith(\"$WORKER_PREFIX\")) | .id" 2>/dev/null || true)

  if [ -z "$matching_workers" ]; then
    echo "No workers found matching prefix '$WORKER_PREFIX'"
  else
    echo "$matching_workers" | while read -r worker_name; do
      [ -z "$worker_name" ] && continue
      echo "Deleting worker: $worker_name"
      # Use API directly to avoid KV namespace permission requirements
      delete_result=$(curl -s -X DELETE \
        "https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${worker_name}" \
        -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}")

      if echo "$delete_result" | jq -e '.success == true' >/dev/null 2>&1; then
        echo "  Successfully deleted worker"
      else
        echo "  Failed to delete worker"
        echo "  Response: $delete_result" | head -2
      fi
    done
  fi
else
  echo "Failed to list workers via API"
  echo "Response: $workers_json"
fi

# Clean R2 bucket if specified
if [ -n "$BUCKET_NAME" ]; then
  echo ""
  echo "=== Cleaning R2 bucket: $BUCKET_NAME ==="

  # List all objects with smoke prefix
  echo "Listing objects in bucket..."
  objects=$(npx wrangler r2 object list "$BUCKET_NAME" --prefix "smoke-" 2>&1 || true)

  if echo "$objects" | grep -qi "error\|not found"; then
    echo "No objects found or error listing bucket"
  else
    # Parse object keys from JSON output
    # wrangler outputs objects as JSON array
    echo "$objects" | grep -o '"key":"[^"]*"' | sed 's/"key":"\([^"]*\)"/\1/' | while read -r object_key; do
      [ -z "$object_key" ] && continue
      echo "Deleting object: $object_key"
      npx wrangler r2 object delete "$BUCKET_NAME/$object_key" 2>&1 || echo "  Failed to delete object"
    done
  fi
fi

echo ""
echo "=== Cleanup complete ==="
echo ""
echo "Note: Generated wrangler config files (wrangler-smoke-*.toml) are gitignored"
