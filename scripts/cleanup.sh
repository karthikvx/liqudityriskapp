#!/bin/bash

STAGE=${1:-dev}
REGION=${2:-us-east-1}

echo "Cleaning up Bank Liquidity Risk Management System resources..."

# Remove CloudWatch alarms
echo "Removing CloudWatch alarms..."
aws cloudwatch delete-alarms \
    --alarm-names \
    "LiquidityRisk-Lambda-Errors" \
    "LiquidityRisk-DynamoDB-Throttles" \
    "LiquidityRisk-Kinesis-IncomingRecords-Low" \
    "LiquidityRisk-LCR-Breach" \
    --region $REGION || echo "Some alarms may not exist"

# Remove serverless application
echo "Removing serverless application..."
serverless remove --stage $STAGE --region $REGION

# Remove Terraform infrastructure
echo "Removing infrastructure..."
cd terraform
terraform destroy -var-file="terraform.tfvars" -var="stage=$STAGE" -auto-approve
cd ..

echo "Cleanup completed!"