import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO, StringIO
from typing import Dict, List, Optional, Union
import json
import gzip
from aws_lambda_powertools import Logger

logger = Logger()

class S3Service:
    """Service for handling S3 operations with financial data."""
    
    def __init__(self, bucket_name: str, region: str = "us-east-1"):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region)
        self.region = region
    
    def read_csv_file(self, key: str, **kwargs) -> pd.DataFrame:
        """Read CSV file from S3 with error handling."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read()
            
            # Handle compressed files
            if key.endswith('.gz'):
                content = gzip.decompress(content)
            
            df = pd.read_csv(BytesIO(content), **kwargs)
            logger.info(f"Successfully read CSV file {key}, shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading CSV file {key}: {str(e)}")
            raise
    
    def read_parquet_file(self, key: str) -> pd.DataFrame:
        """Read Parquet file from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read()
            
            table = pq.read_table(BytesIO(content))
            df = table.to_pandas()
            logger.info(f"Successfully read Parquet file {key}, shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading Parquet file {key}: {str(e)}")
            raise
    
    def write_parquet_file(self, df: pd.DataFrame, key: str, 
                          compression: str = 'snappy') -> bool:
        """Write DataFrame to S3 as Parquet file."""
        try:
            buffer = BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer, compression=compression)
            
            buffer.seek(0)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Successfully wrote Parquet file {key}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing Parquet file {key}: {str(e)}")
            return False
    
    def read_json_file(self, key: str) -> Dict:
        """Read JSON file from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read()
            
            if key.endswith('.gz'):
                content = gzip.decompress(content)
            
            data = json.loads(content.decode('utf-8'))
            logger.info(f"Successfully read JSON file {key}")
            return data
            
        except Exception as e:
            logger.error(f"Error reading JSON file {key}: {str(e)}")
            raise
    
    def apply_lifecycle_policy(self, prefix: str = "") -> bool:
        """Apply lifecycle policy for data retention compliance."""
        try:
            lifecycle_config = {
                'Rules': [
                    {
                        'ID': 'ArchiveOldData',
                        'Status': 'Enabled',
                        'Filter': {'Prefix': prefix},
                        'Transitions': [
                            {
                                'Days': 30,
                                'StorageClass': 'STANDARD_IA'
                            },
                            {
                                'Days': 90,
                                'StorageClass': 'GLACIER'
                            },
                            {
                                'Days': 365,
                                'StorageClass': 'DEEP_ARCHIVE'
                            }
                        ],
                        'Expiration': {
                            'Days': 2557  # 7 years for compliance
                        }
                    }
                ]
            }
            
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration=lifecycle_config
            )
            
            logger.info("Successfully applied lifecycle policy")
            return True
            
        except Exception as e:
            logger.error(f"Error applying lifecycle policy: {str(e)}")
            return False