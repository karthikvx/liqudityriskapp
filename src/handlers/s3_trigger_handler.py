import json
import boto3
from typing import Dict, Any
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from src.services.s3_service import S3Service
from src.services.kinesis_service import KinesisService
from src.config.settings import settings
from src.utils.transformers import LiquidityRiskTransformer
import pandas as pd

logger = Logger()
tracer = Tracer()
metrics = Metrics()

@tracer.capture_lambda_handler
@logger.inject_lambda_context
def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Lambda handler triggered by S3 object creation events.
    Processes financial data files and sends to Kinesis for further processing.
    """
    
    try:
        # Initialize services
        s3_service = S3Service(settings.S3_BUCKET)
        kinesis_service = KinesisService(settings.KINESIS_STREAM)
        transformer = LiquidityRiskTransformer()
        
        processed_files = []
        failed_files = []
        
        # Process each S3 record
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']
            
            logger.info(f"Processing file: {object_key}")
            
            try:
                # Read file based on extension
                if object_key.endswith('.csv') or object_key.endswith('.csv.gz'):
                    df = s3_service.read_csv_file(object_key)
                elif object_key.endswith('.parquet'):
                    df = s3_service.read_parquet_file(object_key)
                elif object_key.endswith('.json') or object_key.endswith('.json.gz'):
                    json_data = s3_service.read_json_file(object_key)
                    df = pd.DataFrame([json_data] if isinstance(json_data, dict) else json_data)
                else:
                    logger.warning(f"Unsupported file format: {object_key}")
                    continue
                
                # Data validation and cleaning
                df = validate_and_clean_data(df)
                
                # Process data in chunks to avoid memory issues
                chunk_size = settings.CHUNK_SIZE
                total_records = 0
                
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    
                    # Convert DataFrame chunk to records
                    records = chunk.to_dict('records')
                    
                    # Add metadata
                    for record in records:
                        record['source_file'] = object_key
                        record['processing_stage'] = 'raw_ingestion'
                        record['file_size'] = record.get('s3', {}).get('object', {}).get('size', 0)
                    
                    # Send to Kinesis
                    result = kinesis_service.put_records_batch(records, 'bank_id')
                    total_records += result['success_count']
                    
                    if result['failed_count'] > 0:
                        logger.warning(f"Failed to process {result['failed_count']} records from chunk")
                
                processed_files.append({
                    'file': object_key,
                    'records_processed': total_records,
                    'status': 'success'
                })
                
                # Update metrics
                metrics.add_metric(name="FilesProcessed", unit=MetricUnit.Count, value=1)
                metrics.add_metric(name="RecordsProcessed", unit=MetricUnit.Count, value=total_records)
                
                logger.info(f"Successfully processed {object_key} with {total_records} records")
                
            except Exception as file_error:
                logger.error(f"Error processing file {object_key}: {str(file_error)}")
                failed_files.append({
                    'file': object_key,
                    'error': str(file_error),
                    'status': 'failed'
                })
                metrics.add_metric(name="FilesProcessingFailed", unit=MetricUnit.Count, value=1)
        
        # Publish metrics
        metrics.add_metadata(key="function", value="s3_trigger_handler")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'File processing completed',
                'processed_files': processed_files,
                'failed_files': failed_files,
                'total_processed': len(processed_files),
                'total_failed': len(failed_files)
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}")
        metrics.add_metric(name="LambdaErrors", unit=MetricUnit.Count, value=1)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e)
            })
        }

def validate_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean financial data."""
    
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Handle missing values
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)
    
    # Convert date columns
    date_columns = ['position_date', 'maturity_date', 'trade_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Validate currency codes
    if 'currency' in df.columns:
        valid_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD']
        df['currency'] = df['currency'].where(df['currency'].isin(valid_currencies), 'USD')
    
    # Remove invalid records
    df = df.dropna(subset=['bank_id', 'position_date'])
    
    logger.info(f"Data validation completed, {len(df)} valid records remaining")
    
    return df
