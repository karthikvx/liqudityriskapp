import pytest
import json
import base64
from unittest.mock import Mock, patch, MagicMock
from src.handlers.s3_trigger_handler import lambda_handler as s3_handler
from src.handlers.kinesis_processor import lambda_handler as kinesis_handler

class TestS3TriggerHandler:
    
    @patch('src.handlers.s3_trigger_handler.S3Service')
    @patch('src.handlers.s3_trigger_handler.KinesisService')
    def test_s3_handler_csv_processing(self, mock_kinesis_service, mock_s3_service):
        # Setup
        mock_s3 = Mock()
        mock_kinesis = Mock()
        mock_s3_service.return_value = mock_s3
        mock_kinesis_service.return_value = mock_kinesis
        
        # Mock S3 service to return DataFrame
        import pandas as pd
        test_df = pd.DataFrame([{
            'bank_id': 'BANK001',
            'position_date': '2024-01-01',
            'asset_class': 'cash',
            'market_value': 1000000
        }])
        mock_s3.read_csv_file.return_value = test_df
        
        # Mock Kinesis service
        mock_kinesis.put_records_batch.return_value = {
            'success_count': 1,
            'failed_count': 0
        }
        
        # Create test event
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'test.csv', 'size': 1024}
                }
            }]
        }
        
        # Execute
        response = s3_handler(event, {})
        
        # Verify
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['total_processed'] == 1
        assert body['total_failed'] == 0

class TestKinesisProcessor:
    
    @patch('src.handlers.kinesis_processor.DynamoDBService')
    def test_kinesis_processor_raw_data(self, mock_dynamodb_service):
        # Setup
        mock_dynamodb = Mock()
        mock_dynamodb_service.return_value = mock_dynamodb
        mock_dynamodb.put_item.return_value = True
        
        # Create test event with Kinesis record
        test_data = {
            'bank_id': 'BANK001',
            'position_date': '2024-01-01T00:00:00',
            'asset_class': 'cash',
            'market_value': 1000000,
            'processing_stage': 'raw_ingestion'
        }
        
        encoded_data = base64.b64encode(json.dumps(test_data).encode()).decode()
        
        event = {
            'Records': [{
                'kinesis': {
                    'data': encoded_data,
                    'sequenceNumber': '123456'
                }
            }]
        }
        
        # Execute
        response = kinesis_handler(event, {})
        
        # Verify
        assert response['statusCode'] == 200
        mock_dynamodb.put_item.assert_called()