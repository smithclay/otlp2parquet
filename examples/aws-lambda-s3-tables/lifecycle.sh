#!/usr/bin/env bash
set -euo pipefail

# lifecycle.sh - Manage the AWS Lambda + S3 Tables example stack end-to-end
#
# Commands:
#   deploy   - Package and deploy the Lambda stack
#   test     - Invoke the Lambda with sample OTLP payloads
#   logs     - Tail CloudWatch logs for the Lambda function
#   delete   - Delete the CloudFormation stack and optional buckets
#   status   - Print CloudFormation outputs and save stack-outputs.json
#   help     - Display usage information

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK_OUTPUTS="${SCRIPT_DIR}/stack-outputs.json"
TESTDATA_DIR="${SCRIPT_DIR}/../../testdata"

DEFAULT_REGION="${AWS_REGION:-us-west-2}"
DEFAULT_STACK="otlp"
DEFAULT_ARCH="arm64"
DEFAULT_VERSION="latest"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info() { echo -e "${GREEN}==>${NC} $*"; }
warn() { echo -e "${YELLOW}WARNING:${NC} $*" >&2; }
error() { echo -e "${RED}ERROR:${NC} $*" >&2; exit 1; }
section() { echo -e "\n${BLUE}### $*${NC}\n"; }

usage() {
  cat <<EOF
Usage: ./lifecycle.sh <command> [options]

Commands:
  deploy     Upload Lambda binary and deploy CloudFormation stack
  test       Invoke the Lambda with bundled OTLP fixtures
  logs       Tail CloudWatch logs for the deployed Lambda
  delete     Remove the CloudFormation stack and related buckets
  status     Show stack outputs and persist stack-outputs.json
  help       Show this message

Defaults:
  Region:      ${DEFAULT_REGION}
  Stack Name:  ${DEFAULT_STACK}
  Architecture:${DEFAULT_ARCH}
  Version:     ${DEFAULT_VERSION}

Examples:
  ./lifecycle.sh deploy --region us-west-2 --arch arm64
  ./lifecycle.sh test --stack-name my-stack --verbose
  ./lifecycle.sh logs --follow
  ./lifecycle.sh delete --force
EOF
}

require_tools() {
  local missing=()
  for tool in "$@"; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      missing+=("$tool")
    fi
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    error "Missing required tools: ${missing[*]}"
  fi
}

describe_stack_output() {
  local stack_name=$1
  local region=$2
  aws cloudformation describe-stacks \
    --stack-name "$stack_name" \
    --region "$region" \
    --query 'Stacks[0].Outputs' \
    --output json
}

save_stack_outputs() {
  local stack_name=$1
  local region=$2
  local outputs
  if ! outputs=$(describe_stack_output "$stack_name" "$region"); then
    error "Failed to retrieve CloudFormation outputs."
  fi
  echo "$outputs" | jq '.' > "$STACK_OUTPUTS"
  info "Stack outputs written to ${STACK_OUTPUTS}"
  echo "$outputs" | jq -r '.[] | "  \(.OutputKey): \(.OutputValue)"'
}

get_output_value() {
  local stack_name=$1
  local region=$2
  local key=$3
  aws cloudformation describe-stacks \
    --stack-name "$stack_name" \
    --region "$region" \
    --query "Stacks[0].Outputs[?OutputKey==\`${key}\`].OutputValue" \
    --output text
}

command_deploy() {
  local region="$DEFAULT_REGION"
  local stack_name="$DEFAULT_STACK"
  local arch="$DEFAULT_ARCH"
  local version="$DEFAULT_VERSION"
  local local_binary=""
  local bucket_arn=""

  while [[ $# -gt 0 ]]; do
    case $1 in
      --region)
        region="$2"
        shift 2
        ;;
      --stack-name)
        stack_name="$2"
        shift 2
        ;;
      --arch)
        arch="$2"
        shift 2
        ;;
      --version)
        version="$2"
        shift 2
        ;;
      --local-binary)
        local_binary="$2"
        shift 2
        ;;
      --bucket-arn)
        bucket_arn="$2"
        shift 2
        ;;
      --help)
        cat <<EOF
Usage: ./lifecycle.sh deploy [options]
  --region <aws-region>       Region to deploy (default: ${DEFAULT_REGION})
  --stack-name <name>         CloudFormation stack name (default: ${DEFAULT_STACK})
  --arch <arm64|amd64>        Lambda architecture (default: ${DEFAULT_ARCH})
  --version <tag>             GitHub release tag or 'latest' (default: ${DEFAULT_VERSION})
  --local-binary <path.zip>   Use local ZIP instead of downloading a release
  --bucket-arn <arn>          Use existing S3 Tables bucket ARN instead of creating new one
EOF
        return 0
        ;;
      *)
        error "Unknown option for deploy: $1"
        ;;
    esac
  done

  if [[ "$arch" != "arm64" && "$arch" != "amd64" ]]; then
    error "Invalid architecture '${arch}'. Choose arm64 or amd64."
  fi

  require_tools aws jq unzip

  local account_id
  if ! account_id=$(aws sts get-caller-identity --query Account --output text 2>/dev/null); then
    error "Unable to determine AWS account. Is the AWS CLI configured?"
  fi

  local deployment_bucket="otlp2parquet-deployment-${account_id}"
  local lambda_zip=""
  local asset_name=""

  if [[ -n "$local_binary" ]]; then
    if [[ ! -f "$local_binary" ]]; then
      error "Local binary not found: ${local_binary}"
    fi
    lambda_zip="$local_binary"
    asset_name="$(basename "$local_binary")"
    info "Using local Lambda zip: ${lambda_zip}"
  else
    asset_name="otlp2parquet-lambda-linux-${arch}.zip"
    lambda_zip="${SCRIPT_DIR}/${asset_name}"
    local download_url
    if [[ "$version" == "latest" ]]; then
      download_url="https://github.com/smithclay/otlp2parquet/releases/latest/download/${asset_name}"
    else
      download_url="https://github.com/smithclay/otlp2parquet/releases/download/${version}/${asset_name}"
    fi
    info "Downloading ${asset_name} (${version})..."
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL -o "$lambda_zip" "$download_url" || error "Download failed. Check that release '${version}' exists."
    elif command -v wget >/dev/null 2>&1; then
      wget -q -O "$lambda_zip" "$download_url" || error "Download failed. Check that release '${version}' exists."
    else
      error "Neither curl nor wget available for download."
    fi
  fi

  if ! unzip -t "$lambda_zip" >/dev/null 2>&1; then
    error "Lambda zip is invalid: ${lambda_zip}"
  fi

  info "Ensuring deployment bucket '${deployment_bucket}' exists..."
  if ! aws s3 ls "s3://${deployment_bucket}" --region "$region" >/dev/null 2>&1; then
    if [[ "$region" == "us-east-1" ]]; then
      aws s3api create-bucket --bucket "$deployment_bucket" --region "$region"
    else
      aws s3api create-bucket \
        --bucket "$deployment_bucket" \
        --region "$region" \
        --create-bucket-configuration "LocationConstraint=${region}"
    fi
    info "Deployment bucket created."
  else
    info "Deployment bucket already exists."
  fi

  local timestamp
  timestamp=$(date +%s)
  local s3_key="lambda/${timestamp}/${asset_name}"
  info "Uploading Lambda zip to s3://${deployment_bucket}/${s3_key}..."
  aws s3 cp "$lambda_zip" "s3://${deployment_bucket}/${s3_key}" --region "$region"

  local cfn_arch="$arch"
  if [[ "$arch" == "amd64" ]]; then
    cfn_arch="x86_64"
  fi

  section "Deploying CloudFormation stack (${stack_name})"

  local param_overrides="LambdaCodeBucket=${deployment_bucket} LambdaCodeKey=${s3_key} LambdaArchitecture=${cfn_arch}"
  if [[ -n "$bucket_arn" ]]; then
    param_overrides="${param_overrides} ExistingS3TablesBucketArn=${bucket_arn}"
    info "Using existing S3 Tables bucket: ${bucket_arn}"
  fi

  aws cloudformation deploy \
    --template-file "${SCRIPT_DIR}/template.yaml" \
    --stack-name "$stack_name" \
    --region "$region" \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides ${param_overrides} \
    --no-fail-on-empty-changeset

  section "Stack Outputs"
  save_stack_outputs "$stack_name" "$region"
  echo ""
  info "Next: ./lifecycle.sh test --region ${region} --stack-name ${stack_name}"
}

command_test() {
  local stack_name="$DEFAULT_STACK"
  local region="$DEFAULT_REGION"
  local verbose="false"

  while [[ $# -gt 0 ]]; do
    case $1 in
      --stack-name)
        stack_name="$2"
        shift 2
        ;;
      --region)
        region="$2"
        shift 2
        ;;
      --verbose)
        verbose="true"
        shift
        ;;
      --help)
        cat <<EOF
Usage: ./lifecycle.sh test [options]
  --stack-name <name>   Stack to test (default: ${DEFAULT_STACK})
  --region <region>     AWS region (default: ${DEFAULT_REGION})
  --verbose             Print Lambda responses
EOF
        return 0
        ;;
      *)
        error "Unknown option for test: $1"
        ;;
    esac
  done

  require_tools aws jq

  if [[ ! -d "$TESTDATA_DIR" ]]; then
    error "Testdata dir not found: ${TESTDATA_DIR}"
  fi

  local lambda_name
  if ! lambda_name=$(get_output_value "$stack_name" "$region" "LambdaFunctionName" 2>/dev/null); then
    error "Unable to find Lambda function name. Deploy the stack first."
  fi

  info "Lambda Function: ${lambda_name}"

  invoke_payload() {
    local label="$1"
    local payload_file="$2"
    local signal="$3"

    section "Testing ${label}"

    local otlp_path="/v1/${signal}"
    local otlp_body
    otlp_body=$(cat "$payload_file")

    local wrapped_event
    local req_id="test-$(date +%s)"
    local req_time="$(date -u +"%d/%b/%Y:%H:%M:%S +0000")"
    local req_epoch="$(date +%s)000"
    local body_json
    body_json=$(echo "$otlp_body" | jq -Rsa .)

    wrapped_event=$(cat <<EOF
{
  "version": "2.0",
  "routeKey": "\$default",
  "rawPath": "${otlp_path}",
  "rawQueryString": "",
  "headers": {
    "content-type": "application/json"
  },
  "requestContext": {
    "accountId": "anonymous",
    "apiId": "test",
    "domainName": "test.lambda-url",
    "domainPrefix": "test",
    "http": {
      "method": "POST",
      "path": "${otlp_path}",
      "protocol": "HTTP/1.1",
      "sourceIp": "127.0.0.1",
      "userAgent": "lifecycle-test"
    },
    "requestId": "${req_id}",
    "routeKey": "\$default",
    "stage": "\$default",
    "time": "${req_time}",
    "timeEpoch": ${req_epoch}
  },
  "body": ${body_json},
  "isBase64Encoded": false
}
EOF
)

    local request_file
    request_file=$(mktemp)
    local response_file
    response_file=$(mktemp)

    echo "$wrapped_event" > "$request_file"

    if aws lambda invoke \
      --function-name "$lambda_name" \
      --region "$region" \
      --payload "file://${request_file}" \
      --cli-binary-format raw-in-base64-out \
      "$response_file" >/tmp/lifecycle-invoke.log 2>&1; then
      info "Invocation succeeded."
      if [[ "$verbose" == "true" ]]; then
        cat "$response_file" | jq '.' || cat "$response_file"
      fi
      if jq -e '.errorMessage' "$response_file" >/dev/null 2>&1; then
        warn "Lambda reported error:"
        jq '.errorType, .errorMessage' "$response_file" || cat "$response_file"
      fi
    else
      local output
      output=$(cat /tmp/lifecycle-invoke.log)
      rm -f "$request_file" "$response_file"
      error "Lambda invocation failed:\n${output}"
    fi

    rm -f "$request_file" "$response_file" /tmp/lifecycle-invoke.log
  }

  [[ -f "${TESTDATA_DIR}/log.json" ]] && invoke_payload "Logs" "${TESTDATA_DIR}/log.json" "logs" || warn "log.json missing"
  [[ -f "${TESTDATA_DIR}/trace.json" ]] && invoke_payload "Traces" "${TESTDATA_DIR}/trace.json" "traces" || warn "trace.json missing"

  local metric_types=(gauge sum histogram exponential_histogram summary)
  for metric_type in "${metric_types[@]}"; do
    local metric_file="${TESTDATA_DIR}/metrics_${metric_type}.json"
    if [[ -f "$metric_file" ]]; then
      invoke_payload "Metrics (${metric_type})" "$metric_file" "metrics"
    else
      warn "Test fixture missing: ${metric_file}"
    fi
  done

  local log_group="/aws/lambda/${lambda_name}"
  section "Recent CloudWatch Logs"
  aws logs tail "$log_group" --region "$region" --since 5m --format short || \
    warn "No logs found yet. Invoke the Lambda to generate logs."

  # Query S3 Tables with DuckDB if available
  if command -v duckdb >/dev/null 2>&1; then
    local table_bucket_arn
    if table_bucket_arn=$(get_output_value "$stack_name" "$region" "S3TablesBucketArn" 2>/dev/null); then
      section "Querying S3 Tables with DuckDB"
      info "Connecting to S3 Tables bucket: ${table_bucket_arn}"

      # Query with DuckDB using S3 Tables endpoint
      # Note: DuckDB reads AWS credentials via credential_chain (from ~/.aws/credentials)
      duckdb -c "
-- Install required extensions (S3 Tables support is in stable)
INSTALL aws;
INSTALL httpfs;
INSTALL iceberg;
LOAD aws;
LOAD httpfs;
LOAD iceberg;

-- Create secret using credential_chain (auto-detects from ~/.aws/credentials)
CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain
);

-- Attach S3 Tables bucket as Iceberg catalog
ATTACH '${table_bucket_arn}' AS s3tables (
    TYPE iceberg,
    ENDPOINT_TYPE s3_tables
);

-- Show available tables
SHOW ALL TABLES;

-- Query logs table if it exists
SELECT * FROM s3tables.otlp.otel_logs LIMIT 5;
" 2>&1 || warn "DuckDB query failed. Table may not exist yet."
    fi
  else
    info "DuckDB not installed. Install it to query S3 Tables: https://duckdb.org/docs/installation/"
  fi

  info "Tests completed. Tail logs with: ./lifecycle.sh logs --stack-name ${stack_name} --region ${region}"
}

command_logs() {
  local stack_name="$DEFAULT_STACK"
  local region="$DEFAULT_REGION"
  local since="5m"
  local follow="false"

  while [[ $# -gt 0 ]]; do
    case $1 in
      --stack-name)
        stack_name="$2"
        shift 2
        ;;
      --region)
        region="$2"
        shift 2
        ;;
      --since)
        since="$2"
        shift 2
        ;;
      --follow)
        follow="true"
        shift
        ;;
      --help)
        cat <<EOF
Usage: ./lifecycle.sh logs [options]
  --stack-name <name>   Stack to inspect (default: ${DEFAULT_STACK})
  --region <region>     AWS region (default: ${DEFAULT_REGION})
  --since <duration>    Lookback window (default: 5m)
  --follow              Stream logs continuously
EOF
        return 0
        ;;
      *)
        error "Unknown option for logs: $1"
        ;;
    esac
  done

  require_tools aws

  local lambda_name
  if ! lambda_name=$(get_output_value "$stack_name" "$region" "LambdaFunctionName" 2>/dev/null); then
    error "Lambda function name not found. Deploy the stack first."
  fi

  local log_group="/aws/lambda/${lambda_name}"
  info "Tailing CloudWatch logs for ${log_group} (region: ${region}, since: ${since})"

  local tail_args=(--region "$region" --since "$since" --format short)
  if [[ "$follow" == "true" ]]; then
    tail_args+=(--follow)
  fi

  aws logs tail "$log_group" "${tail_args[@]}" || \
    warn "No logs returned. Invoke the Lambda first."
}

command_delete() {
  local stack_name="$DEFAULT_STACK"
  local region="$DEFAULT_REGION"
  local force="false"
  local retain_bucket="false"

  while [[ $# -gt 0 ]]; do
    case $1 in
      --stack-name)
        stack_name="$2"
        shift 2
        ;;
      --region)
        region="$2"
        shift 2
        ;;
      --force)
        force="true"
        shift
        ;;
      --retain-bucket)
        retain_bucket="true"
        shift
        ;;
      --help)
        cat <<EOF
Usage: ./lifecycle.sh delete [options]
  --stack-name <name>   Stack to delete (default: ${DEFAULT_STACK})
  --region <region>     AWS region (default: ${DEFAULT_REGION})
  --force               Skip interactive confirmation prompts
  --retain-bucket       Keep deployment bucket (default: delete)
EOF
        return 0
        ;;
      *)
        error "Unknown option for delete: $1"
        ;;
    esac
  done

  require_tools aws

  local account_id
  if ! account_id=$(aws sts get-caller-identity --query Account --output text 2>/dev/null); then
    error "Unable to determine AWS account."
  fi
  local deployment_bucket="otlp2parquet-deployment-${account_id}"

  local table_bucket=""
  if table_bucket=$(get_output_value "$stack_name" "$region" "S3TablesBucketName" 2>/dev/null); then
    if [[ "$table_bucket" == "None" ]]; then
      table_bucket=""
    fi
  fi

  if [[ "$force" != "true" ]]; then
    echo "About to delete CloudFormation stack '${stack_name}' in ${region}."
    if [[ -n "$table_bucket" ]]; then
      echo "Warning: This removes Iceberg tables stored in ${table_bucket}."
    fi
    read -p "Continue? (yes/no): " -r answer
    if [[ ! "$answer" =~ ^[Yy][Ee][Ss]$ ]]; then
      info "Deletion aborted."
      return 0
    fi
  fi

  info "Deleting CloudFormation stack ${stack_name}..."
  aws cloudformation delete-stack --stack-name "$stack_name" --region "$region"
  info "Waiting for stack deletion to complete..."
  if aws cloudformation wait stack-delete-complete --stack-name "$stack_name" --region "$region"; then
    info "Stack deleted."
  else
    warn "Stack deletion may have failed. Check AWS Console."
  fi

  if [[ "$retain_bucket" != "true" ]]; then
    info "Removing deployment bucket ${deployment_bucket}..."
    aws s3 rm "s3://${deployment_bucket}" --recursive --region "$region" >/dev/null 2>&1 || true
    aws s3 rb "s3://${deployment_bucket}" --region "$region" >/dev/null 2>&1 || true
    info "Deployment bucket cleanup complete (if bucket existed)."
  else
    info "Retaining deployment bucket ${deployment_bucket}."
  fi
}

command_status() {
  local stack_name="$DEFAULT_STACK"
  local region="$DEFAULT_REGION"

  while [[ $# -gt 0 ]]; do
    case $1 in
      --stack-name)
        stack_name="$2"
        shift 2
        ;;
      --region)
        region="$2"
        shift 2
        ;;
      --help)
        cat <<EOF
Usage: ./lifecycle.sh status [options]
  --stack-name <name>   Stack to inspect (default: ${DEFAULT_STACK})
  --region <region>     AWS region (default: ${DEFAULT_REGION})
EOF
        return 0
        ;;
      *)
        error "Unknown option for status: $1"
        ;;
    esac
  done

  require_tools aws jq
  section "CloudFormation Outputs for ${stack_name}"
  save_stack_outputs "$stack_name" "$region"
}

COMMAND="${1:-}"
if [[ -z "$COMMAND" || "$COMMAND" == "help" ]]; then
  usage
  exit 0
fi

shift || true

case "$COMMAND" in
  deploy)
    command_deploy "$@"
    ;;
  test)
    command_test "$@"
    ;;
  logs)
    command_logs "$@"
    ;;
  delete)
    command_delete "$@"
    ;;
  status)
    command_status "$@"
    ;;
  *)
    usage
    error "Unknown command: ${COMMAND}"
    ;;
esac
