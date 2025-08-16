from pydantic import BaseModel, Field, validator
from typing import Dict, List, Optional, Union
from datetime import datetime
from decimal import Decimal
import uuid

class LiquidityPosition(BaseModel):
    """Model for liquidity position data."""
    
    position_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    bank_id: str
    account_id: str
    position_date: datetime
    asset_class: str
    instrument_type: str
    notional_amount: Decimal
    market_value: Decimal
    liquidity_score: float = Field(ge=0.0, le=1.0)
    haircut_percentage: float = Field(ge=0.0, le=1.0)
    time_to_maturity_days: Optional[int] = None
    currency: str = "USD"
    risk_weight: float = Field(ge=0.0, le=1.0)
    
    @validator('position_date')
    def validate_position_date(cls, v):
        if v > datetime.now():
            raise ValueError('Position date cannot be in the future')
        return v
    
    @validator('currency')
    def validate_currency(cls, v):
        valid_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD']
        if v not in valid_currencies:
            raise ValueError(f'Currency must be one of {valid_currencies}')
        return v

class RiskMetrics(BaseModel):
    """Model for calculated risk metrics."""
    
    metric_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    calculation_date: datetime
    bank_id: str
    liquidity_coverage_ratio: float
    net_stable_funding_ratio: float
    high_quality_liquid_assets: Decimal
    total_net_cash_outflows: Decimal
    available_stable_funding: Decimal
    required_stable_funding: Decimal
    stress_test_result: Dict[str, float]
    
    @validator('liquidity_coverage_ratio')
    def validate_lcr(cls, v):
        if v < 0:
            raise ValueError('Liquidity Coverage Ratio cannot be negative')
        return v

class ComplianceRecord(BaseModel):
    """Model for compliance and audit records."""
    
    record_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.now)
    regulation_type: str  # CCAR, DFAST, CECL, IFRS9
    bank_id: str
    compliance_status: str  # COMPLIANT, NON_COMPLIANT, UNDER_REVIEW
    violations: List[str] = []
    remediation_actions: List[str] = []
    risk_rating: str  # LOW, MEDIUM, HIGH, CRITICAL
    metadata: Dict = {}
