import boto3
from typing import List, Dict

def create_cloudwatch_alarms(region: str = 'us-east-1'):
    """Create CloudWatch alarms for monitoring the liquidity risk system."""
    
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    
    alarms = [
        {
            'AlarmName': 'LiquidityRisk-Lambda-Errors',
            'ComparisonOperator': 'GreaterThanThreshold',
            'EvaluationPeriods': 2,
            'MetricName': 'Errors',
            'Namespace': 'AWS/Lambda',
            'Period': 300,
            'Statistic': 'Sum',
            'Threshold': 5.0,
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-east-1:ACCOUNT_ID:liquidity-risk-alerts'
            ],
            'AlarmDescription': 'Alarm when Lambda function errors exceed threshold',
            'Dimensions': [
                {
                    'Name': 'FunctionName',
                    'Value': 'bank-liquidity-risk-management-dev-kinesisProcessor'
                },
            ],
            'Unit': 'Count'
        },
        {
            'AlarmName': 'LiquidityRisk-DynamoDB-Throttles',
            'ComparisonOperator': 'GreaterThanThreshold',
            'EvaluationPeriods': 2,
            'MetricName': 'ThrottledRequests',
            'Namespace': 'AWS/DynamoDB',
            'Period': 300,
            'Statistic': 'Sum',
            'Threshold': 10.0,
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-east-1:ACCOUNT_ID:liquidity-risk-alerts'
            ],
            'AlarmDescription': 'Alarm when DynamoDB throttling occurs',
            'Dimensions': [
                {
                    'Name': 'TableName',
                    'Value': 'liquidity-risk-data-dev'
                },
            ],
            'Unit': 'Count'
        },
        {
            'AlarmName': 'LiquidityRisk-Kinesis-IncomingRecords-Low',
            'ComparisonOperator': 'LessThanThreshold',
            'EvaluationPeriods': 3,
            'MetricName': 'IncomingRecords',
            'Namespace': 'AWS/Kinesis',
            'Period': 600,
            'Statistic': 'Sum',
            'Threshold': 10.0,
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-east-1:ACCOUNT_ID:liquidity-risk-alerts'
            ],
            'AlarmDescription': 'Alarm when Kinesis incoming records are too low',
            'Dimensions': [
                {
                    'Name': 'StreamName',
                    'Value': 'liquidity-risk-stream-dev'
                },
            ],
            'Unit': 'Count'
        },
        {
            'AlarmName': 'LiquidityRisk-LCR-Breach',
            'ComparisonOperator': 'LessThanThreshold',
            'EvaluationPeriods': 1,
            'MetricName': 'LCR_Ratio',
            'Namespace': 'BankLiquidityRisk/Compliance',
            'Period': 300,
            'Statistic': 'Minimum',
            'Threshold': 1.0,
            'ActionsEnabled': True,
            'AlarmActions': [
                'arn:aws:sns:us-east-1:ACCOUNT_ID:liquidity-risk-critical-alerts'
            ],
            'AlarmDescription': 'Critical alarm when LCR falls below regulatory minimum',
            'Unit': 'None'
        }
    ]
    
    for alarm in alarms:
        try:
            cloudwatch.put_metric_alarm(**alarm)
            print(f"Created alarm: {alarm['AlarmName']}")
        except Exception as e:
            print(f"Error creating alarm {alarm['AlarmName']}: {str(e)}")
