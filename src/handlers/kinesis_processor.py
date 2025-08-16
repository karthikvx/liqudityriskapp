import json
import base64
from typing import Dict, Any, List
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from src.services.dynamodb_service import DynamoDBService
from src.services.s3_service import S3Service
from src.config.settings import settings
from src.utils.transformers import LiquidityRiskTransformer
from src.models.data_models import LiquidityPosition, RiskMetrics, ComplianceRecord
import pandas as pd
from datetime import datetime
from decimal import Decimal

logger = Logger()
tracer = Tracer()
metrics = Metrics()

@tracer.capture_lambda_handler
@logger.inject_lambda_context
def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Lambda handler for processing Kinesis stream records.
    Performs risk calculations and stores results in DynamoDB.
    """
    
    try:
        # Initialize services
        dynamodb_service = DynamoDBService(settings.DYNAMODB_TABLE)
        s3_service = S3Service(settings.S3_BUCKET)
        transformer = LiquidityRiskTransformer()
        
        processed_records = 0
        failed_records = 0
        risk_calculations = []
        
        # Process Kinesis records
        for record in event['Records']:
            try:
                # Decode Kinesis data
                payload = base64.b64decode(record['kinesis']['data'])
                data = json.loads(payload.decode('utf-8'))
                
                # Process based on data type
                if data.get('processing_stage') == 'raw_ingestion':
                    # Process raw financial data
                    processed_data = process_raw_financial_data(data, transformer)
                    
                    # Store in DynamoDB
                    success = dynamodb_service.put_item(processed_data)
                    
                    if success:
                        processed_records += 1
                        risk_calculations.append(processed_data)
                    else:
                        failed_records += 1
                
                elif data.get('processing_stage') == 'risk_calculation':
                    # Process risk calculation results
                    risk_data = process_risk_calculation_data(data)
                    
                    # Store risk metrics
                    success = dynamodb_service.put_item(risk_data)
                    
                    if success:
                        processed_records += 1
                    else:
                        failed_records += 1
                
                else:
                    logger.warning(f"Unknown processing stage: {data.get('processing_stage')}")
                    failed_records += 1
                
            except Exception as record_error:
                logger.error(f"Error processing Kinesis record: {str(record_error)}")
                failed_records += 1
        
        # Perform batch risk calculations if we have enough data
        if len(risk_calculations) >= 10:
            perform_batch_risk_analysis(risk_calculations, transformer, dynamodb_service)
        
        # Update metrics
        metrics.add_metric(name="RecordsProcessed", unit=MetricUnit.Count, value=processed_records)
        metrics.add_metric(name="RecordsFailed", unit=MetricUnit.Count, value=failed_records)
        
        logger.info(f"Processed {processed_records} records, {failed_records} failed")
        
        return {
            'statusCode': 200,
            'batchItemFailures': []  # For partial batch failure handling
        }
        
    except Exception as e:
        logger.error(f"Kinesis processor error: {str(e)}")
        metrics.add_metric(name="ProcessorErrors", unit=MetricUnit.Count, value=1)
        
        # Return batch item failures for retry
        return {
            'statusCode': 500,
            'batchItemFailures': [
                {'itemIdentifier': record['kinesis']['sequenceNumber']} 
                for record in event['Records']
            ]
        }

def process_raw_financial_data(data: Dict[str, Any], 
                              transformer: LiquidityRiskTransformer) -> Dict[str, Any]:
    """Process raw financial data and prepare for storage."""
    
    # Create LiquidityPosition model for validation
    try:
        position = LiquidityPosition(**data)
        
        # Calculate additional metrics
        liquidity_score = calculate_liquidity_score(data)
        risk_weight = transformer.risk_weights.get(data.get('asset_class', 'other'), 1.0)
        
        # Prepare DynamoDB item
        processed_item = {
            'partition_key': f"POSITION#{data['bank_id']}",
            'sort_key': f"{data['position_date']}#{data.get('position_id', '')}",
            'gsi1_pk': f"BANK#{data['bank_id']}",
            'gsi1_sk': data['position_date'],
            'entity_type': 'LIQUIDITY_POSITION',
            'bank_id': data['bank_id'],
            'position_id': position.position_id,
            'position_date': data['position_date'],
            'asset_class': data['asset_class'],
            'instrument_type': data.get('instrument_type', 'unknown'),
            'notional_amount': float(data.get('notional_amount', 0)),
            'market_value': float(data.get('market_value', 0)),
            'liquidity_score': liquidity_score,
            'risk_weight': risk_weight,
            'currency': data.get('currency', 'USD'),
            'processing_timestamp': datetime.now().isoformat(),
            'source_file': data.get('source_file', ''),
            'data_quality_score': calculate_data_quality_score(data)
        }
        
        return processed_item
        
    except Exception as e:
        logger.error(f"Error processing raw financial data: {str(e)}")
        raise

def process_risk_calculation_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Process risk calculation results."""
    
    processed_item = {
        'partition_key': f"RISK_METRICS#{data['bank_id']}",
        'sort_key': f"{data['calculation_date']}#{data.get('metric_type', 'GENERAL')}",
        'gsi1_pk': f"BANK#{data['bank_id']}",
        'gsi1_sk': f"RISK#{data['calculation_date']}",
        'entity_type': 'RISK_METRICS',
        'bank_id': data['bank_id'],
        'calculation_date': data['calculation_date'],
        'lcr': data.get('lcr', 0),
        'nsfr': data.get('nsfr', 0),
        'hqla': data.get('hqla', 0),
        'net_cash_outflows': data.get('net_cash_outflows', 0),
        'risk_level': data.get('risk_level', 'UNKNOWN'),
        'compliance_status': determine_compliance_status(data),
        'processing_timestamp': datetime.now().isoformat()
    }
    
    return processed_item

def perform_batch_risk_analysis(positions: List[Dict[str, Any]], 
                               transformer: LiquidityRiskTransformer,
                               dynamodb_service: DynamoDBService):
    """Perform batch risk analysis on accumulated positions."""
    
    try:
        # Convert to DataFrame for analysis
        df = pd.DataFrame(positions)
        
        # Calculate LCR
        lcr_results = transformer.calculate_liquidity_coverage_ratio(df)
        
        # Calculate NSFR
        nsfr_results = transformer.calculate_net_stable_funding_ratio(df)
        
        # Perform stress testing
        stress_scenarios = {
            'mild_stress': -0.05,
            'moderate_stress': -0.15,
            'severe_stress': -0.30
        }
        stress_results = transformer.perform_stress_testing(df, stress_scenarios)
        
        # Store risk calculation results
        risk_items = []
        
        for _, row in lcr_results.iterrows():
            risk_item = {
                'partition_key': f"RISK_METRICS#{row['bank_id']}",
                'sort_key': f"{row['calculation_date']}#LCR",
                'gsi1_pk': f"BANK#{row['bank_id']}",
                'gsi1_sk': f"RISK#{row['calculation_date']}",
                'entity_type': 'RISK_METRICS',
                'bank_id': row['bank_id'],
                'calculation_date': row['calculation_date'].isoformat(),
                'metric_type': 'LCR',
                'lcr': float(row['lcr']),
                'hqla': float(row['hqla']),
                'net_cash_outflows': float(row['net_cash_outflows']),
                'compliant': bool(row['lcr_compliant']),
                'processing_timestamp': datetime.now().isoformat()
            }
            risk_items.append(risk_item)
        
        # Store NSFR results
        for _, row in nsfr_results.iterrows():
            risk_item = {
                'partition_key': f"RISK_METRICS#{row['bank_id']}",
                'sort_key': f"{row['calculation_date']}#NSFR",
                'gsi1_pk': f"BANK#{row['bank_id']}",
                'gsi1_sk': f"RISK#{row['calculation_date']}",
                'entity_type': 'RISK_METRICS',
                'bank_id': row['bank_id'],
                'calculation_date': row['calculation_date'].isoformat(),
                'metric_type': 'NSFR',
                'nsfr': float(row['nsfr']),
                'available_stable_funding': float(row['available_stable_funding']),
                'required_stable_funding': float(row['required_stable_funding']),
                'compliant': bool(row['nsfr_compliant']),
                'processing_timestamp': datetime.now().isoformat()
            }
            risk_items.append(risk_item)
        
        # Store stress test results
        for _, row in stress_results.iterrows():
            stress_item = {
                'partition_key': f"STRESS_TEST#{row['bank_id']}",
                'sort_key': f"{row['calculation_date']}#{row['scenario_name']}",
                'gsi1_pk': f"BANK#{row['bank_id']}",
                'gsi1_sk': f"STRESS#{row['calculation_date']}",
                'entity_type': 'STRESS_TEST',
                'bank_id': row['bank_id'],
                'calculation_date': row['calculation_date'].isoformat(),
                'scenario_name': row['scenario_name'],
                'stress_factor': float(row['stress_factor']),
                'stressed_lcr': float(row['stressed_lcr']),
                'stressed_nsfr': float(row['stressed_nsfr']) if row['stressed_nsfr'] is not None else None,
                'lcr_breach': bool(row['lcr_breach']),
                'nsfr_breach': bool(row['nsfr_breach']),
                'risk_level': row['risk_level'],
                'processing_timestamp': datetime.now().isoformat()
            }
            risk_items.append(stress_item)
        
        # Batch store risk items
        if risk_items:
            result = dynamodb_service.put_items_batch(risk_items)
            logger.info(f"Stored {result['success_count']} risk calculation results")
            
            # Update metrics
            metrics.add_metric(name="RiskCalculationsStored", 
                             unit=MetricUnit.Count, 
                             value=result['success_count'])
    
    except Exception as e:
        logger.error(f"Error in batch risk analysis: {str(e)}")
        raise

def calculate_liquidity_score(data: Dict[str, Any]) -> float:
    """Calculate liquidity score for an asset."""
    
    asset_class = data.get('asset_class', 'unknown')
    time_to_maturity = data.get('time_to_maturity_days', 365)
    market_value = data.get('market_value', 0)
    
    # Base liquidity scores by asset class
    base_scores = {
        'cash': 1.0,
        'government_bonds': 0.95,
        'corporate_bonds': 0.7,
        'equities': 0.6,
        'derivatives': 0.3,
        'real_estate': 0.1
    }
    
    base_score = base_scores.get(asset_class, 0.5)
    
    # Adjust for time to maturity
    maturity_adjustment = max(0.1, 1 - (time_to_maturity / 365) * 0.3)
    
    # Adjust for market value (larger positions may be harder to liquidate quickly)
    size_adjustment = max(0.8, 1 - (market_value / 1000000) * 0.1)  # Adjust for positions > $1M
    
    final_score = base_score * maturity_adjustment * size_adjustment
    
    return min(1.0, max(0.0, final_score))

def calculate_data_quality_score(data: Dict[str, Any]) -> float:
    """Calculate data quality score based on completeness and validity."""
    
    required_fields = ['bank_id', 'position_date', 'asset_class', 'notional_amount', 'market_value']
    optional_fields = ['instrument_type', 'currency', 'time_to_maturity_days']
    
    score = 0.0
    
    # Required fields (70% of score)
    required_present = sum(1 for field in required_fields if data.get(field) is not None)
    score += (required_present / len(required_fields)) * 0.7
    
    # Optional fields (20% of score)
    optional_present = sum(1 for field in optional_fields if data.get(field) is not None)
    score += (optional_present / len(optional_fields)) * 0.2
    
    # Data validity checks (10% of score)
    validity_score = 0.0
    
    # Check if amounts are positive
    if data.get('market_value', 0) > 0:
        validity_score += 0.5
    
    # Check if date is reasonable
    if data.get('position_date'):
        try:
            pos_date = pd.to_datetime(data['position_date'])
            if pos_date <= datetime.now():
                validity_score += 0.5
        except:
            pass
    
    score += validity_score * 0.1
    
    return min(1.0, max(0.0, score))

def determine_compliance_status(data: Dict[str, Any]) -> str:
    """Determine compliance status based on risk metrics."""
    
    lcr = data.get('lcr', 0)
    nsfr = data.get('nsfr', 0)
    
    if lcr >= 1.0 and nsfr >= 1.0:
        return 'COMPLIANT'
    elif lcr >= 0.8 and nsfr >= 0.8:
        return 'UNDER_REVIEW'
    else:
        return 'NON_COMPLIANT'
