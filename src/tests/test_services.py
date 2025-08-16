import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from src.services.s3_service import S3Service
from src.services.kinesis_service import KinesisService
from src.services.dynamodb_service import DynamoDBService
from src.utils.transformers import LiquidityRiskTransformer
import json
from datetime import datetime

class TestS3Service:
    
    @patch('boto3.client')
    def test_read_csv_file_success(self, mock_boto_client):
        # Setup
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        csv_content = "bank_id,position_date,asset_class,market_value\nBANK001,2024-01-01,cash,1000000"
        mock_s3.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=csv_content.encode()))
        }
        
        service = S3Service("test-bucket")
        
        # Execute
        df = service.read_csv_file("test.csv")
        
        # Verify
        assert len(df) == 1
        assert df.iloc[0]['bank_id'] == 'BANK001'
        assert df.iloc[0]['market_value'] == 1000000
        mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="test.csv")
    
    @patch('boto3.client')
    def test_write_parquet_file_success(self, mock_boto_client):
        # Setup
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        service = S3Service("test-bucket")
        df = pd.DataFrame([{
            'bank_id': 'BANK001',
            'position_date': '2024-01-01',
            'asset_class': 'cash',
            'market_value': 1000000
        }])
        
        # Execute
        result = service.write_parquet_file(df, "test.parquet")
        
        # Verify
        assert result is True
        mock_s3.put_object.assert_called_once()

class TestKinesisService:
    
    @patch('boto3.client')
    def test_put_record_success(self, mock_boto_client):
        # Setup
        mock_kinesis = Mock()
        mock_boto_client.return_value = mock_kinesis
        mock_kinesis.put_record.return_value = {'SequenceNumber': '123456'}
        
        service = KinesisService("test-stream")
        
        # Execute
        result = service.put_record({'test': 'data'}, 'partition-key')
        
        # Verify
        assert result is True
        mock_kinesis.put_record.assert_called_once()
    
    @patch('boto3.client')
    def test_put_records_batch_success(self, mock_boto_client):
        # Setup
        mock_kinesis = Mock()
        mock_boto_client.return_value = mock_kinesis
        mock_kinesis.put_records.return_value = {
            'FailedRecordCount': 0,
            'Records': [{'SequenceNumber': '123'}, {'SequenceNumber': '456'}]
        }
        
        service = KinesisService("test-stream")
        records = [{'bank_id': 'BANK001'}, {'bank_id': 'BANK002'}]
        
        # Execute
        result = service.put_records_batch(records, 'bank_id')
        
        # Verify
        assert result['success_count'] == 2
        assert result['failed_count'] == 0
        mock_kinesis.put_records.assert_called_once()

class TestDynamoDBService:
    
    @patch('boto3.resource')
    def test_put_item_success(self, mock_boto_resource):
        # Setup
        mock_table = Mock()
        mock_dynamodb = Mock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto_resource.return_value = mock_dynamodb
        
        service = DynamoDBService("test-table")
        
        # Execute
        result = service.put_item({'partition_key': 'test', 'data': 'value'})
        
        # Verify
        assert result is True
        mock_table.put_item.assert_called_once()
    
    @patch('boto3.resource')
    def test_get_item_success(self, mock_boto_resource):
        # Setup
        mock_table = Mock()
        mock_dynamodb = Mock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto_resource.return_value = mock_dynamodb
        
        mock_table.get_item.return_value = {
            'Item': {'partition_key': 'test', 'data': 'value'}
        }
        
        service = DynamoDBService("test-table")
        
        # Execute
        result = service.get_item('test')
        
        # Verify
        assert result['partition_key'] == 'test'
        assert result['data'] == 'value'

class TestLiquidityRiskTransformer:
    
    def test_calculate_liquidity_coverage_ratio(self):
        # Setup
        transformer = LiquidityRiskTransformer()
        
        data = {
            'bank_id': ['BANK001', 'BANK001', 'BANK001'],
            'position_date': ['2024-01-01', '2024-01-01', '2024-01-01'],
            'asset_class': ['cash', 'government_bonds', 'corporate_bonds'],
            'market_value': [10000000, 50000000, 20000000],
            'notional_amount': [-5000000, 0, -10000000],  # Negative for outflows
            'liquidity_score': [1.0, 0.95, 0.7]
        }
        df = pd.DataFrame(data)
        df['position_date'] = pd.to_datetime(df['position_date'])
        
        # Execute
        result = transformer.calculate_liquidity_coverage_ratio(df)
        
        # Verify
        assert len(result) == 1  # One bank-date combination
        assert result.iloc[0]['bank_id'] == 'BANK001'
        assert 'lcr' in result.columns
        assert 'hqla' in result.columns
        assert 'net_cash_outflows' in result.columns
    
    def test_calculate_net_stable_funding_ratio(self):
        # Setup
        transformer = LiquidityRiskTransformer()
        
        data = {
            'bank_id': ['BANK001', 'BANK001'],
            'position_date': ['2024-01-01', '2024-01-01'],
            'asset_class': ['cash', 'corporate_bonds'],
            'market_value': [10000000, 20000000],
            'liquidity_score': [1.0, 0.7]
        }
        df = pd.DataFrame(data)
        df['position_date'] = pd.to_datetime(df['position_date'])
        
        # Execute
        result = transformer.calculate_net_stable_funding_ratio(df)
        
        # Verify
        assert len(result) == 1
        assert 'nsfr' in result.columns
        assert 'available_stable_funding' in result.columns
        assert 'required_stable_funding' in result.columns
    
    def test_perform_stress_testing(self):
        # Setup
        transformer = LiquidityRiskTransformer()
        
        data = {
            'bank_id': ['BANK001', 'BANK001'],
            'position_date': ['2024-01-01', '2024-01-01'],
            'asset_class': ['cash', 'equities'],
            'market_value': [10000000, 5000000],
            'notional_amount': [0, -2000000],
            'liquidity_score': [1.0, 0.6]
        }
        df = pd.DataFrame(data)
        df['position_date'] = pd.to_datetime(df['position_date'])
        
        scenarios = {'mild_stress': -0.10, 'severe_stress': -0.30}
        
        # Execute
        result = transformer.perform_stress_testing(df, scenarios)
        
        # Verify
        assert len(result) == 2  # Two scenarios
        assert 'scenario_name' in result.columns
        assert 'stressed_lcr' in result.columns
        assert 'risk_level' in result.columns