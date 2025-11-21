#!/bin/bash
# Delete all S3 Tables resources in a region
# Usage: ./delete-all-s3-tables.sh [region] [prefix]
#
# Arguments:
#   region: AWS region (default: us-west-2)
#   prefix: Bucket name prefix filter (default: smoke-lambda)

set -e

REGION="${1:-us-west-2}"
PREFIX="${2:-smoke-lambda}"

echo "Cleaning up S3 Tables resources in region: $REGION"
echo "Filtering buckets with prefix: $PREFIX"
echo ""

# Get all matching S3 Tables bucket ARNs
bucket_arns=$(aws s3tables list-table-buckets \
  --region "$REGION" \
  --query "tableBuckets[?contains(name, \`$PREFIX\`)].arn" \
  --output text)

if [ -z "$bucket_arns" ]; then
  echo "No S3 Tables buckets found matching prefix '$PREFIX'"
  exit 0
fi

# Process each bucket
echo "$bucket_arns" | tr '\t' '\n' | while read -r bucket_arn; do
  [ -z "$bucket_arn" ] && continue

  echo "=== Processing bucket: $bucket_arn ==="

  # List all namespaces
  namespaces=$(aws s3tables list-namespaces \
    --region "$REGION" \
    --table-bucket-arn "$bucket_arn" \
    --query 'namespaces[].namespace' \
    --output text 2>&1 || true)

  if [ -z "$namespaces" ] || echo "$namespaces" | grep -qi "error\|exception"; then
    echo "  No namespaces found or error listing"
    continue
  fi

  # Process each namespace
  echo "$namespaces" | tr '\t' '\n' | while read -r namespace; do
    [ -z "$namespace" ] && continue
    echo "  Processing namespace: $namespace"

    # List all tables
    table_names=$(aws s3tables list-tables \
      --region "$REGION" \
      --table-bucket-arn "$bucket_arn" \
      --namespace "$namespace" \
      --query 'tables[].name' \
      --output text 2>&1 || true)

    if [ -z "$table_names" ]; then
      echo "    No tables found"
    elif echo "$table_names" | grep -qi "error\|exception"; then
      echo "    Error listing tables: $table_names"
    else
      # Delete each table
      echo "$table_names" | tr '\t' '\n' | while read -r table_name; do
        [ -z "$table_name" ] && continue
        echo "    Deleting table: $table_name"
        aws s3tables delete-table \
          --region "$REGION" \
          --table-bucket-arn "$bucket_arn" \
          --namespace "$namespace" \
          --name "$table_name" 2>&1 || echo "      Failed to delete table"
      done
    fi

    # Delete the namespace (after all tables are deleted)
    echo "    Deleting namespace: $namespace"
    aws s3tables delete-namespace \
      --region "$REGION" \
      --table-bucket-arn "$bucket_arn" \
      --namespace "$namespace" 2>&1 || echo "      Failed to delete namespace"
  done

  echo ""
done

echo "=== Cleanup complete ==="
echo ""
echo "Note: CloudFormation stacks can now be deleted with:"
echo "  aws cloudformation delete-stack --region $REGION --stack-name <stack-name>"
