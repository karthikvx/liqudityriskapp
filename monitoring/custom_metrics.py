import boto3
from datetime import datetime
from typing import Dict, List, Any

class CustomMetrics:
    """Custom CloudWatch metrics for liquidity risk monitoring."""
    
    def __init__(self, region: str = 'us-east-1'):
        self.cloudwatch = boto3.client('cloudwatch', region_name=region)
        self.namespace = 'BankLiquidityRisk'
    
    def publish_risk_metrics(self, bank_id: str, metrics: Dict[str, float]):
        """Publish risk calculation metrics to CloudWatch."""
        
        metric_data = []
        
        # LCR metric
        if 'lcr' in metrics:
            metric_data.append({
                'MetricName': 'LCR_Ratio',
                'Dimensions': [
                    {'Name': 'BankId', 'Value': bank_id}
                ],
                'Value': metrics['lcr'],
                'Unit': 'None',
                'Timestamp': datetime.utcnow()
            })
        
        # NSFR metric
        if 'nsfr' in metrics:
            metric_data.append({
                'MetricName': 'NSFR_Ratio',
                'Dimensions': [
                    {'Name': 'BankId', 'Value': bank_id}
                ],
                'Value': metrics['nsfr'],
                'Unit': 'None',
                'Timestamp': datetime.utcnow()
            })
        
        # HQLA metric
        if 'hqla' in metrics:
            metric_data.append({
                'MetricName': 'HQLA_Amount',
                'Dimensions': [
                    {'Name': 'BankId', 'Value': bank_id}
                ],
                'Value': metrics['hqla'],
                'Unit': 'None',
                'Timestamp': datetime.utcnow()
            })
        
        # Data quality score
        if 'data_quality_score' in metrics:
            metric_data.append({
                'MetricName': 'DataQualityScore',
                'Dimensions': [
                    {'Name': 'BankId', 'Value': bank_id}
                ],
                'Value': metrics['data_quality_score'],
                'Unit': 'Percent',
                'Timestamp': datetime.utcnow()
            })
        
        # Publish metrics in batches of 20 (CloudWatch limit)
        for i in range(0, len(metric_data), 20):
            batch = metric_data[i:i+20]
            try:
                self.cloudwatch.put_metric_data(
                    Namespace=f"{self.namespace}/Compliance",
                    MetricData=batch
                )
            except Exception as e:
                print(f"Error publishing metrics batch: {str(e)}")
    
    def publish_processing_metrics(self, metrics: Dict[str, int]):
        """Publish data processing metrics."""
        
        metric_data = []
        
        for metric_name, value in metrics.items():
            metric_data.append({
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count',
                'Timestamp': datetime.utcnow()
            })
        
        try:
            self.cloudwatch.put_metric_data(
                Namespace=f"{self.namespace}/Processing",
                MetricData=metric_data
            )
        except Exception as e:
            print(f"Error publishing processing metrics: {str(e)}")
