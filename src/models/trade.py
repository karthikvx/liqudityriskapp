from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any
from datetime import datetime
from decimal import Decimal
from enum import Enum

class TradeType(str, Enum):
    BUY = "buy"
    SELL = "sell"

class InstrumentType(str, Enum):
    GOVERNMENT_BOND = "government_bond"
    CORPORATE_BOND = "corporate_bond" 
    EQUITY = "equity"
    CASH = "cash"
    DERIVATIVE = "derivative"
    REPO = "repo"
    REVERSE_REPO = "reverse_repo"

class Trade(BaseModel):
    trade_id: str = Field(..., description="Unique trade identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    trade_type: TradeType
    instrument_type: InstrumentType
    instrument_id: str = Field(..., description="Instrument identifier")
    counterparty_id: str = Field(..., description="Counterparty identifier")
    counterparty_rating: str = Field(..., description="Credit rating")
    notional_amount: Decimal = Field(..., gt=0, description="Trade notional amount")
    currency: str = Field(..., min_length=3, max_length=3, description="Currency code")
    maturity_date: Optional[datetime] = None
    settlement_date: datetime
    trader_id: str = Field(..., description="Trader identifier")
    book_id: str = Field(..., description="Trading book identifier")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('currency')
    def validate_currency(cls, v):
        return v.upper()
    
    @validator('counterparty_rating')
    def validate_rating(cls, v):
        valid_ratings = [
            'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
            'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-',
            'B+', 'B', 'B-', 'CCC', 'CC', 'C', 'D', 'UNRATED'
        ]
        if v not in valid_ratings:
            raise ValueError(f'Invalid rating: {v}')
        return v
    
    @validator('settlement_date')
    def settlement_after_trade(cls, v, values):
        if 'timestamp' in values and v < values['timestamp']:
            raise ValueError('Settlement date must be after trade date')
        return v

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: str(v)
        }

class TradeResponse(BaseModel):
    trade_id: str
    status: str
    lcr_impact: Optional[Decimal] = None
    nsfr_impact: Optional[Decimal] = None
    risk_weight: Optional[Decimal] = None
    processed_at: datetime