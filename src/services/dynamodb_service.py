import boto3
from boto3.dynamodb.conditions import Key, Attr
from typing import Dict, List, Any, Optional
from aws_lambda_powertools import Logger
from datetime import datetime
from decimal import Decimal
import json

logger = Logger()

class DynamoDBService:
    """Service for handling DynamoDB operations."""
    
    def __init__(self, table_name: str, region: str = "us-east-1"):
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.region = region
    
    def put_item(self, item: Dict[str, Any]) -> bool:
        """Put item to DynamoDB table."""
        try:
            # Convert floats to Decimal for DynamoDB compatibility
            item = self._convert_floats_to_decimal(item)
            
            # Add metadata
            item['created_at'] = datetime.now().isoformat()
            item['ttl'] = int((datetime.now().timestamp()) + (7 * 365 * 24 * 3600))  # 7 years TTL
            
            response = self.table.put_item(Item=item)
            logger.info(f"Successfully put item with partition key: {item.get('partition_key')}")
            return True
            
        except Exception as e:
            logger.error(f"Error putting item to DynamoDB: {str(e)}")
            return False
    
    def put_items_batch(self, items: List[Dict[str, Any]]) -> Dict[str, int]:
        """Put multiple items to DynamoDB in batch."""
        try:
            success_count = 0
            failed_count = 0
            
            # Process items in batches of 25 (DynamoDB limit)
            for i in range(0, len(items), 25):
                batch = items[i:i+25]
                
                with self.table.batch_writer() as writer:
                    for item in batch:
                        try:
                            # Convert floats to Decimal
                            item = self._convert_floats_to_decimal(item)
                            
                            # Add metadata
                            item['created_at'] = datetime.now().isoformat()
                            item['ttl'] = int((datetime.now().timestamp()) + (7 * 365 * 24 * 3600))
                            
                            writer.put_item(Item=item)
                            success_count += 1
                            
                        except Exception as e:
                            logger.error(f"Error in batch item: {str(e)}")
                            failed_count += 1
            
            logger.info(f"Batch put results - Success: {success_count}, Failed: {failed_count}")
            
            return {
                'success_count': success_count,
                'failed_count': failed_count
            }
            
        except Exception as e:
            logger.error(f"Error putting items batch to DynamoDB: {str(e)}")
            return {'success_count': 0, 'failed_count': len(items)}
    
    def get_item(self, partition_key: str, sort_key: str = None) -> Optional[Dict]:
        """Get item from DynamoDB table."""
        try:
            key = {'partition_key': partition_key}
            if sort_key:
                key['sort_key'] = sort_key
            
            response = self.table.get_item(Key=key)
            
            if 'Item' in response:
                return self._convert_decimal_to_float(response['Item'])
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting item from DynamoDB: {str(e)}")
            return None
    
    def query_items(self, partition_key: str, 
                   sort_key_condition: str = None,
                   filter_expression: str = None,
                   limit: int = None) -> List[Dict]:
        """Query items from DynamoDB table."""
        try:
            key_condition = Key('partition_key').eq(partition_key)
            
            if sort_key_condition:
                # This is a simplified example - you'd need to parse the condition properly
                key_condition = key_condition & Key('sort_key').begins_with(sort_key_condition)
            
            query_kwargs = {
                'KeyConditionExpression': key_condition
            }
            
            if filter_expression:
                query_kwargs['FilterExpression'] = Attr('status').eq(filter_expression)
            
            if limit:
                query_kwargs['Limit'] = limit
            
            response = self.table.query(**query_kwargs)
            
            items = [self._convert_decimal_to_float(item) for item in response['Items']]
            logger.info(f"Successfully queried {len(items)} items")
            
            return items
            
        except Exception as e:
            logger.error(f"Error querying items from DynamoDB: {str(e)}")
            return []
    
    def _convert_floats_to_decimal(self, obj):
        """Convert float values to Decimal for DynamoDB compatibility."""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self._convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_floats_to_decimal(v) for v in obj]
        else:
            return obj
    
    def _convert_decimal_to_float(self, obj):
        """Convert Decimal values back to float."""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_decimal_to_float(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimal_to_float(v) for v in obj]
        else:
            return obj