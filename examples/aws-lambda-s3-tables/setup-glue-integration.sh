#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Parse arguments
REGION="us-west-2"
while [[ $# -gt 0 ]]; do
  case $1 in
    --region)
      REGION="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 --region <aws-region>"
      exit 1
      ;;
  esac
done

echo -e "${BLUE}==>${NC} Setting up AWS Glue Data Catalog for Iceberg tables"
echo -e "${BLUE}==>${NC} Region: $REGION"

# Get AWS account ID and Lambda role ARN
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
LAMBDA_ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name otlp2parquet \
  --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaRoleArn`].OutputValue' \
  --output text 2>/dev/null || echo "")

echo -e "${BLUE}==>${NC} Account ID: $ACCOUNT_ID"
echo -e "${BLUE}==>${NC} Lambda Role: $LAMBDA_ROLE_ARN"

# Step 1: Create Glue database
echo -e "${GREEN}==>${NC} Step 1: Creating database 'otel' in Glue..."
aws glue create-database \
  --database-input "{\"Name\":\"otel\",\"Description\":\"OpenTelemetry data lake\"}" \
  --region "$REGION" 2>/dev/null \
  && echo -e "${GREEN}✓${NC} Namespace created" \
  || echo -e "${YELLOW}✓${NC} Namespace already exists"

# Step 2: Verify database
echo -e "${GREEN}==>${NC} Step 2: Verifying Glue Data Catalog..."
aws glue get-database --name otel --region "$REGION" > /dev/null \
  && echo -e "${GREEN}✓${NC} Database 'otel' accessible in Glue" \
  || echo -e "${YELLOW}✗${NC} Database 'otel' not found"

# Step 3: Configure Lake Formation (if Lambda role found)
if [ -n "$LAMBDA_ROLE_ARN" ]; then
  echo -e "${GREEN}==>${NC} Step 3: Configuring Lake Formation permissions..."

  # Grant database permissions
  aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier="$LAMBDA_ROLE_ARN" \
    --resource '{"Database":{"Name":"otel"}}' \
    --permissions CREATE_TABLE DESCRIBE \
    --region "$REGION" 2>/dev/null \
    && echo -e "${GREEN}✓${NC} Database permissions granted" \
    || echo -e "${YELLOW}✓${NC} Database permissions already exist"

  echo -e "${YELLOW}==>${NC} Table permissions must be granted after tables are created"
  echo -e "${YELLOW}    Run after first Lambda invocation:"
  echo -e "${YELLOW}    aws lakeformation grant-permissions \\"
  echo -e "${YELLOW}      --principal DataLakePrincipalIdentifier=$LAMBDA_ROLE_ARN \\"
  echo -e "${YELLOW}      --resource '{\"Table\":{\"DatabaseName\":\"otel\",\"Name\":\"logs\"}}' \\"
  echo -e "${YELLOW}      --permissions SELECT INSERT ALTER DESCRIBE \\"
  echo -e "${YELLOW}      --region $REGION"
else
  echo -e "${YELLOW}==>${NC} Step 3: Lambda role not found - skip Lake Formation setup for now"
fi

# Step 4: Display configuration
echo ""
echo -e "${BLUE}Configuration Summary:${NC}"
echo -e "  Glue Database: otel"
echo -e "  Glue Endpoint: https://glue.$REGION.amazonaws.com/iceberg"
echo -e "  Glue Catalog ID: $ACCOUNT_ID"
echo -e "  Region: $REGION"

echo ""
echo -e "${GREEN}==>${NC} Setup complete! Deploy Lambda with:"
echo -e "  ./deploy.sh --region $REGION"
