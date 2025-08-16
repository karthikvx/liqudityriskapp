import os
from typing import Optional

class Settings:
    """Application settings and configuration."""
    
    # AWS Configuration
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    S3_BUCKET: str = os.getenv("S3_BUCKET", "bank-liquidity-data")
    KINESIS_STREAM: str = os.getenv("KINESIS_STREAM", "liquidity-risk-stream")
    DYNAMODB_TABLE: str = os.getenv("DYNAMODB_TABLE", "liquidity-risk-data")
    
    # Processing Configuration
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", "10000"))
    
    # Compliance Configuration
    RETENTION_DAYS: int = int(os.getenv("RETENTION_DAYS", "2557"))  # 7 years
    ENCRYPTION_KEY_ID: Optional[str] = os.getenv("ENCRYPTION_KEY_ID")
    
    # Risk Management Thresholds
    LIQUIDITY_RATIO_THRESHOLD: float = float(os.getenv("LIQUIDITY_RATIO_THRESHOLD", "0.03"))
    STRESS_TEST_THRESHOLD: float = float(os.getenv("STRESS_TEST_THRESHOLD", "0.1"))
    
    @classmethod
    def get_settings(cls) -> "Settings":
        return cls()

settings = Settings.get_settings()