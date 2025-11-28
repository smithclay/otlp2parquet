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
# Match any bucket name containing the prefix (handles both smoke-lambda-* and otlp2parquet-smoke-smoke-lambda-*)
bucket_arns=$(aws s3tables list-table-buckets \
  --region "$REGION" \
  --output json | jq -r ".tableBuckets[] | select(.name | contains(\"$PREFIX\")) | .arn")

if [ -z "$bucket_arns" ]; then
  echo "No S3 Tables buckets found matching prefix '$PREFIX'"
else

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

fi

echo "=== S3 Tables cleanup complete ==="
echo ""

# Empty and delete S3 buckets that match the prefix pattern
# Handles both otlp2parquet-smoke-{prefix} and otlp2parquet-plain-{prefix} patterns
echo "=== Emptying S3 buckets matching '$PREFIX' ==="

s3_buckets=$(aws s3 ls 2>/dev/null | awk '{print $3}' | grep -E "(otlp2parquet-smoke-|otlp2parquet-plain-).*$PREFIX" || true)

if [ -z "$s3_buckets" ]; then
  echo "No S3 buckets found matching pattern"
else
  echo "$s3_buckets" | while read -r bucket; do
    [ -z "$bucket" ] && continue
    echo "  Emptying and deleting bucket: $bucket"
    # First try simple delete
    aws s3 rm "s3://$bucket" --recursive 2>&1 || true
    # Try to delete the bucket
    if ! aws s3 rb "s3://$bucket" 2>&1; then
      # If failed, it might have versioning - use Python to delete all versions
      echo "    Bucket has versioning, deleting all versions..."
      uvx --quiet --with boto3 python3 -c "
import boto3
s3 = boto3.resource('s3')
bucket = s3.Bucket('$bucket')
bucket.object_versions.delete()
bucket.delete()
print('    Deleted versioned bucket')
" 2>&1 || echo "    Failed to delete versioned bucket"
    fi
  done
fi

echo ""

# Now delete CloudFormation stacks matching the prefix
echo "=== Deleting CloudFormation stacks matching '$PREFIX' ==="

stack_names=$(aws cloudformation list-stacks \
  --region "$REGION" \
  --stack-status-filter CREATE_COMPLETE CREATE_FAILED UPDATE_COMPLETE ROLLBACK_COMPLETE DELETE_FAILED \
  --query "StackSummaries[?contains(StackName, \`$PREFIX\`)].StackName" \
  --output text)

if [ -z "$stack_names" ]; then
  echo "No CloudFormation stacks found matching prefix '$PREFIX'"
else
  echo "$stack_names" | tr '\t' '\n' | while read -r stack_name; do
    [ -z "$stack_name" ] && continue
    echo "  Deleting stack: $stack_name"
    aws cloudformation delete-stack \
      --region "$REGION" \
      --stack-name "$stack_name" 2>&1 || echo "    Failed to delete stack"
  done
  echo ""
  echo "Stack deletions initiated. Monitor with:"
  echo "  aws cloudformation list-stacks --region $REGION --stack-status-filter DELETE_IN_PROGRESS DELETE_FAILED --query \"StackSummaries[?contains(StackName, '$PREFIX')].[StackName,StackStatus]\" --output table"
fi

echo ""
echo "=== Cleanup complete ==="
