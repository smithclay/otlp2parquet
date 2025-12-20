#!/bin/bash
# Delete Cloudflare resources: Workers, R2 buckets, and Durable Objects namespaces
# Usage: ./delete-all-workers.sh [--prefix PREFIX] [--force]
#
# Options:
#   --prefix PREFIX  Only delete resources matching this prefix
#   --force          Skip interactive prompts (requires typing DELETE ALL if no prefix)
#
# Environment:
#   CLOUDFLARE_API_TOKEN: Cloudflare API token (required)
#   CLOUDFLARE_ACCOUNT_ID: Cloudflare account ID (required)

set -e

# Auto-source .env file from otlp2parquet-cloudflare if env vars not set
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../crates/otlp2parquet-cloudflare/.env"

if [ -z "$CLOUDFLARE_API_TOKEN" ] || [ -z "$CLOUDFLARE_ACCOUNT_ID" ]; then
  if [ -f "$ENV_FILE" ]; then
    echo "Loading credentials from $ENV_FILE"
    set -a
    source "$ENV_FILE"
    set +a
  fi
fi

# Parse arguments
PREFIX=""
FORCE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --prefix)
      PREFIX="$2"
      shift 2
      ;;
    --force)
      FORCE=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--prefix PREFIX] [--force]"
      exit 1
      ;;
  esac
done

# Validate environment
if [ -z "$CLOUDFLARE_API_TOKEN" ]; then
  echo "Error: CLOUDFLARE_API_TOKEN environment variable required"
  exit 1
fi

if [ -z "$CLOUDFLARE_ACCOUNT_ID" ]; then
  echo "Error: CLOUDFLARE_ACCOUNT_ID environment variable required"
  exit 1
fi

API_BASE="https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}"

# Helper: Make authenticated API request
cf_api() {
  local method="$1"
  local endpoint="$2"
  local data="${3:-}"

  if [ -n "$data" ]; then
    curl -s -X "$method" "${API_BASE}${endpoint}" \
      -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "$data"
  else
    curl -s -X "$method" "${API_BASE}${endpoint}" \
      -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      -H "Content-Type: application/json"
  fi
}

# Helper: Filter items by prefix
filter_by_prefix() {
  local items="$1"
  local field="$2"

  if [ -n "$PREFIX" ]; then
    echo "$items" | jq -r ".[] | select(.${field} | startswith(\"$PREFIX\")) | .${field}"
  else
    echo "$items" | jq -r ".[].${field}"
  fi
}

# Helper: Prompt for selection
prompt_selection() {
  local resource_type="$1"
  local count="$2"

  if [ "$FORCE" = true ]; then
    echo "all"
    return
  fi

  echo -n "Delete $count $resource_type? [all/none/select] " >&2
  read -r response </dev/tty
  echo "$response"
}

# Helper: Prompt y/n for single item
prompt_item() {
  local item="$1"

  echo -n "  Delete '$item'? [y/N] " >&2
  read -r response </dev/tty
  [[ "$response" =~ ^[Yy]$ ]]
}

#=============================================================================
# Resource Discovery
#=============================================================================

echo "Cloudflare Resource Cleanup"
echo "==========================="
if [ -n "$PREFIX" ]; then
  echo "Prefix filter: $PREFIX"
fi
echo ""

# Discover Workers
echo "Discovering resources..."
workers_json=$(cf_api GET "/workers/scripts")
if ! echo "$workers_json" | jq -e '.success == true' >/dev/null 2>&1; then
  echo "Failed to list workers: $(echo "$workers_json" | jq -r '.errors[0].message // "Unknown error"')"
  exit 1
fi
workers_list=$(filter_by_prefix "$(echo "$workers_json" | jq '.result')" "id")
workers_count=$(echo "$workers_list" | grep -c . 2>/dev/null || true)
[ -z "$workers_list" ] && workers_count=0

# Discover R2 Buckets
buckets_json=$(cf_api GET "/r2/buckets")
if ! echo "$buckets_json" | jq -e '.success == true' >/dev/null 2>&1; then
  echo "Failed to list R2 buckets: $(echo "$buckets_json" | jq -r '.errors[0].message // "Unknown error"')"
  exit 1
fi
buckets_list=$(filter_by_prefix "$(echo "$buckets_json" | jq '.result.buckets // []')" "name")
buckets_count=$(echo "$buckets_list" | grep -c . 2>/dev/null || true)
[ -z "$buckets_list" ] && buckets_count=0

# Discover Durable Objects Namespaces
do_json=$(cf_api GET "/workers/durable_objects/namespaces")
if ! echo "$do_json" | jq -e '.success == true' >/dev/null 2>&1; then
  echo "Failed to list DO namespaces: $(echo "$do_json" | jq -r '.errors[0].message // "Unknown error"')"
  exit 1
fi
do_list=$(filter_by_prefix "$(echo "$do_json" | jq '.result // []')" "name")
do_ids=$(echo "$do_json" | jq -r '.result // [] | .[] | "\(.name):\(.id)"')
do_count=$(echo "$do_list" | grep -c . 2>/dev/null || true)
[ -z "$do_list" ] && do_count=0

#=============================================================================
# Preview Phase
#=============================================================================

echo ""
echo "=== Workers ($workers_count) ==="
if [ "$workers_count" -gt 0 ]; then
  echo "$workers_list" | while read -r name; do
    [ -n "$name" ] && echo "  - $name"
  done
else
  echo "  (none)"
fi

echo ""
echo "=== R2 Buckets ($buckets_count) ==="
if [ "$buckets_count" -gt 0 ]; then
  echo "$buckets_list" | while read -r name; do
    [ -n "$name" ] && echo "  - $name"
  done
else
  echo "  (none)"
fi

echo ""
echo "=== Durable Objects Namespaces ($do_count) ==="
if [ "$do_count" -gt 0 ]; then
  echo "$do_list" | while read -r name; do
    [ -n "$name" ] && echo "  - $name"
  done
else
  echo "  (none)"
fi

echo ""

# Check if anything to delete
total=$((workers_count + buckets_count + do_count))
if [ "$total" -eq 0 ]; then
  echo "No resources found to delete."
  exit 0
fi

#=============================================================================
# Safety Check for --force without --prefix
#=============================================================================

if [ "$FORCE" = true ] && [ -z "$PREFIX" ]; then
  echo "WARNING: --force without --prefix will delete ALL resources above."
  echo -n "Type 'DELETE ALL' to confirm: "
  read -r confirm </dev/tty
  if [ "$confirm" != "DELETE ALL" ]; then
    echo "Aborted."
    exit 1
  fi
  echo ""
fi

#=============================================================================
# Selection Phase
#=============================================================================

declare -a workers_to_delete=()
declare -a buckets_to_delete=()
declare -a do_to_delete=()

# Select Workers
if [ "$workers_count" -gt 0 ]; then
  echo ""
  selection=$(prompt_selection "workers" "$workers_count")
  case "$selection" in
    all)
      while IFS= read -r name; do
        [ -n "$name" ] && workers_to_delete+=("$name")
      done <<< "$workers_list"
      ;;
    none)
      echo "  Skipping workers"
      ;;
    select)
      while IFS= read -r name; do
        [ -z "$name" ] && continue
        if prompt_item "$name"; then
          workers_to_delete+=("$name")
        fi
      done <<< "$workers_list"
      ;;
    *)
      echo "  Invalid selection, skipping workers"
      ;;
  esac
fi

# Select R2 Buckets
if [ "$buckets_count" -gt 0 ]; then
  echo ""
  selection=$(prompt_selection "R2 buckets" "$buckets_count")
  case "$selection" in
    all)
      while IFS= read -r name; do
        [ -n "$name" ] && buckets_to_delete+=("$name")
      done <<< "$buckets_list"
      ;;
    none)
      echo "  Skipping R2 buckets"
      ;;
    select)
      while IFS= read -r name; do
        [ -z "$name" ] && continue
        if prompt_item "$name"; then
          buckets_to_delete+=("$name")
        fi
      done <<< "$buckets_list"
      ;;
    *)
      echo "  Invalid selection, skipping R2 buckets"
      ;;
  esac
fi

# Select Durable Objects Namespaces
if [ "$do_count" -gt 0 ]; then
  echo ""
  selection=$(prompt_selection "DO namespaces" "$do_count")
  case "$selection" in
    all)
      while IFS= read -r name; do
        [ -n "$name" ] && do_to_delete+=("$name")
      done <<< "$do_list"
      ;;
    none)
      echo "  Skipping DO namespaces"
      ;;
    select)
      while IFS= read -r name; do
        [ -z "$name" ] && continue
        if prompt_item "$name"; then
          do_to_delete+=("$name")
        fi
      done <<< "$do_list"
      ;;
    *)
      echo "  Invalid selection, skipping DO namespaces"
      ;;
  esac
fi

#=============================================================================
# Deletion Phase
#=============================================================================

deleted_count=0
failed_count=0

# Delete Workers
if [ ${#workers_to_delete[@]} -gt 0 ]; then
  echo ""
  echo "=== Deleting Workers ==="
  for name in "${workers_to_delete[@]}"; do
    echo "  Deleting worker: $name"
    result=$(cf_api DELETE "/workers/scripts/$name")
    if echo "$result" | jq -e '.success == true' >/dev/null 2>&1; then
      echo "    OK"
      ((deleted_count++))
    else
      echo "    FAILED: $(echo "$result" | jq -r '.errors[0].message // "Unknown error"')"
      ((failed_count++))
    fi
  done
fi

# Delete R2 Buckets (empty first via S3 API)
if [ ${#buckets_to_delete[@]} -gt 0 ]; then
  echo ""
  echo "=== Deleting R2 Buckets ==="

  # Check if we have S3 credentials for emptying buckets
  R2_ENDPOINT="https://${CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com"
  if [ -n "$AWS_ACCESS_KEY_ID" ] && [ -n "$AWS_SECRET_ACCESS_KEY" ]; then
    CAN_EMPTY=true
  else
    CAN_EMPTY=false
    echo "  Warning: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY not set, cannot empty buckets"
    echo "  Will attempt to delete buckets directly (will fail if not empty)"
  fi

  for bucket_name in "${buckets_to_delete[@]}"; do
    echo "  Processing bucket: $bucket_name"

    # Empty the bucket first via S3 API
    if [ "$CAN_EMPTY" = true ]; then
      echo "    Emptying bucket via S3 API..."
      if aws s3 rm "s3://$bucket_name" --recursive --endpoint-url "$R2_ENDPOINT" 2>&1 | grep -v "^delete:"; then
        echo "    Bucket emptied"
      else
        echo "    Bucket was empty or emptying complete"
      fi
    fi

    # Delete the bucket via Cloudflare API
    echo "    Deleting bucket..."
    result=$(cf_api DELETE "/r2/buckets/$bucket_name")
    if echo "$result" | jq -e '.success == true' >/dev/null 2>&1; then
      echo "    OK"
      ((deleted_count++))
    else
      echo "    FAILED: $(echo "$result" | jq -r '.errors[0].message // "Unknown error"')"
      ((failed_count++))
    fi
  done
fi

# Delete Durable Objects Namespaces
if [ ${#do_to_delete[@]} -gt 0 ]; then
  echo ""
  echo "=== Deleting Durable Objects Namespaces ==="
  for name in "${do_to_delete[@]}"; do
    # Find the ID for this namespace name
    ns_id=$(echo "$do_ids" | grep "^${name}:" | cut -d: -f2)

    if [ -z "$ns_id" ]; then
      echo "  Could not find ID for namespace: $name"
      ((failed_count++))
      continue
    fi

    echo "  Deleting DO namespace: $name (id: $ns_id)"
    result=$(cf_api DELETE "/workers/durable_objects/namespaces/$ns_id")
    if echo "$result" | jq -e '.success == true' >/dev/null 2>&1; then
      echo "    OK"
      ((deleted_count++))
    else
      echo "    FAILED: $(echo "$result" | jq -r '.errors[0].message // "Unknown error"')"
      ((failed_count++))
    fi
  done
fi

#=============================================================================
# Summary
#=============================================================================

echo ""
echo "=== Summary ==="
echo "Deleted: $deleted_count"
echo "Failed: $failed_count"

if [ "$failed_count" -gt 0 ]; then
  exit 1
fi
