#!/bin/bash

set -e

STAGE=${1:-dev}
REGION=${2:-us-east-1}

echo "Deploying Bank Liquidity Risk Management System to $STAGE environment in $REGION"

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Deploy infrastructure with Terraform
echo "Deploying infrastructure..."
cd terraform
terraform init
terraform plan -var-file="terraform.tfvars" -var="stage=$STAGE"
terraform apply -var-file="terraform.tfvars" -var="stage=$STAGE" -auto-approve
cd ..

# Deploy serverless application
echo "Deploying serverless application..."
serverless deploy --stage $STAGE --region $REGION

# Create CloudWatch alarms
echo "Creating CloudWatch alarms..."
python -c "
from monitoring.cloudwatch_alarms import create_cloudwatch_alarms
create_cloudwatch_alarms('$REGION')
"

# Run tests
echo "Running tests..."
python -m pytest src/tests/ -v

echo "Deployment completed successfully!"