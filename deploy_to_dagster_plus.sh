#!/bin/bash

# Multi-Code Location Demo - Deploy to Dagster+
# Deploys all 4 code locations using PEX builds (faster than Docker)

set -e  # Exit on any error

# Configuration
# Set these environment variables before running:
#   export DAGSTER_CLOUD_ORGANIZATION="your-org"
#   export DAGSTER_CLOUD_API_TOKEN="your-token"
#   export DAGSTER_CLOUD_DEPLOYMENT="prod"
ORGANIZATION="${DAGSTER_CLOUD_ORGANIZATION:?Error: DAGSTER_CLOUD_ORGANIZATION environment variable is required}"
API_TOKEN="${DAGSTER_CLOUD_API_TOKEN:?Error: DAGSTER_CLOUD_API_TOKEN environment variable is required}"
DEPLOYMENT="${DAGSTER_CLOUD_DEPLOYMENT:-prod}"

# Color output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Deploying Multi-Code Location Demo to Dagster+${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Organization: $ORGANIZATION"
echo "Deployment:   $DEPLOYMENT"
echo "Build Type:   PEX (Python Executable)"
echo ""

# Code locations to deploy
declare -a LOCATIONS=(
    "fintech_alpha:fintech-alpha"
    "insurance_beta:insurance-beta"
    "healthcare_gamma:healthcare-gamma"
    "shared_analytics:shared-analytics"
)

# Deploy each code location
for location_pair in "${LOCATIONS[@]}"; do
    IFS=':' read -r folder location_name <<< "$location_pair"

    echo -e "${BLUE}========================================${NC}"
    echo -e "${GREEN}Deploying: $location_name${NC}"
    echo -e "${BLUE}========================================${NC}"

    cd "code_locations/$folder"

    dagster-cloud serverless deploy-python-executable \
        --organization "$ORGANIZATION" \
        --api-token "$API_TOKEN" \
        --deployment "$DEPLOYMENT" \
        --location-file dagster_cloud.yaml \
        --location-name "$location_name"

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Successfully deployed $location_name${NC}"
    else
        echo -e "${RED}✗ Failed to deploy $location_name${NC}"
        exit 1
    fi

    cd ../..
    echo ""
done

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}All code locations deployed!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "View your deployment at:"
echo "https://$ORGANIZATION.dagster.cloud/$DEPLOYMENT"
echo ""
echo "Code Locations:"
echo "  ✓ fintech-alpha        (S3 → Matillion → dbt → Databricks → Azure DW)"
echo "  ✓ insurance-beta       (Kafka → enrichment → fraud detection)"
echo "  ✓ healthcare-gamma     (EHR → HIPAA → analytics)"
echo "  ✓ shared-analytics     (Consolidated cross-company analytics)"
echo ""
