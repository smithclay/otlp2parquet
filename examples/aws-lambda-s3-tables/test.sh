#!/usr/bin/env bash
set -euo pipefail

# test.sh - Test otlp2parquet Lambda deployment
#
# Usage:
#   ./test.sh [OPTIONS]
#
# Options:
#   --stack-name <name>   CloudFormation stack name (default: otlp2parquet)
#   --region <region>     AWS region (default: $AWS_REGION or us-east-1)
#   --verbose             Show verbose Lambda output
#   --help                Show this help message

# Default configuration
STACK_NAME="otlp2parquet"
REGION="${AWS_REGION:-us-east-1}"
VERBOSE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTDATA_DIR="${SCRIPT_DIR}/../../testdata"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print functions
info() { echo -e "${GREEN}==>${NC} $*"; }
warn() { echo -e "${YELLOW}WARNING:${NC} $*" >&2; }
error() { echo -e "${RED}ERROR:${NC} $*" >&2; exit 1; }
section() { echo -e "\n${BLUE}### $*${NC}\n"; }

# Show help
show_help() {
  grep '^#' "$0" | grep -v '#!/usr/bin/env' | sed 's/^# \?//'
  exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --stack-name)
      STACK_NAME="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --verbose)
      VERBOSE=true
      shift
      ;;
    --help)
      show_help
      ;;
    *)
      error "Unknown option: $1. Use --help for usage."
      ;;
  esac
done

# Check required tools
for cmd in aws jq; do
  if ! command -v "$cmd" &> /dev/null; then
    error "$cmd is required but not installed."
  fi
done

# Check testdata directory exists
if [[ ! -d "$TESTDATA_DIR" ]]; then
  error "Testdata directory not found: $TESTDATA_DIR"
fi

# Get stack outputs
info "Retrieving stack information..."

LAMBDA_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
  --output text 2>/dev/null) || \
  error "Failed to get Lambda function name. Is stack '$STACK_NAME' deployed?"

TABLE_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`TableBucketName`].OutputValue' \
  --output text)

ICEBERG_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`IcebergRestEndpoint`].OutputValue' \
  --output text)

info "Lambda Function: $LAMBDA_NAME"
info "Table Bucket: $TABLE_BUCKET"
info "Iceberg Endpoint: $ICEBERG_ENDPOINT"

# Function to invoke Lambda with test data
invoke_lambda() {
  local test_name=$1
  local payload_file=$2
  local signal_type=$3

  section "Testing: $test_name"

  info "Payload: $payload_file"

  # Determine OTLP endpoint path based on signal type
  local otlp_path="/v1/${signal_type}"

  # Read the OTLP JSON payload
  local otlp_body=$(cat "$payload_file")

  # Wrap in Lambda Function URL event structure (API Gateway v2 format)
  local lambda_event=$(cat <<EOF
{
  "version": "2.0",
  "routeKey": "\$default",
  "rawPath": "$otlp_path",
  "rawQueryString": "",
  "headers": {
    "content-type": "application/json"
  },
  "requestContext": {
    "accountId": "anonymous",
    "apiId": "test",
    "domainName": "test.lambda-url.us-west-2.on.aws",
    "domainPrefix": "test",
    "http": {
      "method": "POST",
      "path": "$otlp_path",
      "protocol": "HTTP/1.1",
      "sourceIp": "127.0.0.1",
      "userAgent": "test-script"
    },
    "requestId": "test-$(date +%s)",
    "routeKey": "\$default",
    "stage": "\$default",
    "time": "$(date -u +"%d/%b/%Y:%H:%M:%S +0000")",
    "timeEpoch": $(date +%s)000
  },
  "body": $(echo "$otlp_body" | jq -Rsa .),
  "isBase64Encoded": false
}
EOF
)

  # Create temporary files for request and response
  local request_file=$(mktemp)
  local response_file=$(mktemp)
  local output_file=$(mktemp)

  # Write wrapped event to file
  echo "$lambda_event" > "$request_file"

  # Invoke Lambda
  if aws lambda invoke \
    --function-name "$LAMBDA_NAME" \
    --region "$REGION" \
    --payload "file://${request_file}" \
    --cli-binary-format raw-in-base64-out \
    "$response_file" \
    > "$output_file" 2>&1; then

    info "✓ Lambda invocation successful"

    # Show response
    if [[ "$VERBOSE" == "true" ]]; then
      echo "Response:"
      cat "$response_file" | jq '.' || cat "$response_file"
    fi

    # Check for errors in response
    if grep -q "errorMessage" "$response_file" 2>/dev/null; then
      warn "Lambda returned an error:"
      cat "$response_file" | jq '.errorMessage, .errorType' || cat "$response_file"
    else
      info "✓ No errors in response"
    fi
  else
    error "Lambda invocation failed. Output:\n$(cat "$output_file")"
  fi

  # Cleanup
  rm -f "$request_file" "$response_file" "$output_file"

  echo ""
}

# Test logs
if [[ -f "${TESTDATA_DIR}/log.json" ]]; then
  invoke_lambda "Logs (JSON)" "${TESTDATA_DIR}/log.json" "logs"
else
  warn "Skipping logs test: ${TESTDATA_DIR}/log.json not found"
fi

# Test traces
if [[ -f "${TESTDATA_DIR}/trace.json" ]]; then
  invoke_lambda "Traces (JSON)" "${TESTDATA_DIR}/trace.json" "traces"
else
  warn "Skipping traces test: ${TESTDATA_DIR}/trace.json not found"
fi

# Test metrics - all types
for metric_type in gauge sum histogram exponential_histogram summary; do
  metric_file="${TESTDATA_DIR}/metrics_${metric_type}.json"
  if [[ -f "$metric_file" ]]; then
    invoke_lambda "Metrics: ${metric_type}" "$metric_file" "metrics"
  else
    warn "Skipping metrics_${metric_type} test: $metric_file not found"
  fi
done

# Show recent CloudWatch logs
section "Recent CloudWatch Logs (last 5 minutes)"

LOG_GROUP="/aws/lambda/${LAMBDA_NAME}"

info "Fetching logs from: $LOG_GROUP"

aws logs tail "$LOG_GROUP" \
  --region "$REGION" \
  --since 5m \
  --format short \
  2>/dev/null || warn "No recent logs found (this is normal for first run)"

echo ""

# Summary
section "Test Summary"

info "✓ All Lambda invocations completed"
info "Check CloudWatch Logs for detailed output:"
echo "  aws logs tail $LOG_GROUP --region $REGION --follow"
echo ""
info "Next steps:"
echo "  1. Verify tables created in S3 Tables console"
echo "  2. Query data with DuckDB (see README.md for examples)"
echo "  3. Set up API Gateway for production OTLP ingestion"
echo ""
