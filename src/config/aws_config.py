import boto3
from botocore.config import Config
from src.config.settings import settings
import logging

logger = logging.getLogger(__name__)

class AWSConfig:
    def __init__(self):
        self.config = Config(
            region_name=settings.AWS_REGION,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=50
        )
        
        self.session = boto3.Session(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
    
    def get_kinesis_client(self):
        return self.session.client(
            'kinesis',
            endpoint_url=settings.AWS_ENDPOINT_URL,
            config=self.config
        )
    
    def get_dynamodb_client(self):
        return self.session.client(
            'dynamodb',
            endpoint_url=settings.AWS_ENDPOINT_URL,
            config=self.config
        )
    
    def get_dynamodb_resource(self):
        return self.session.resource(
            'dynamodb',
            endpoint_url=settings.AWS_ENDPOINT_URL,
            config=self.config
        )
    
    def get_cloudwatch_client(self):
        return self.session.client(
            'cloudwatch',
            endpoint_url=settings.AWS_ENDPOINT_URL,
            config=self.config
        )
    
    def get_sns_client(self):
        return self.session.client(
            'sns',
            endpoint_url=settings.AWS_ENDPOINT_URL,
            config=self.config
        )

aws_config = AWSConfig()