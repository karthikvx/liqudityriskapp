from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from decimal import Decimal
from enum import Enum

class RiskMetricType(str, Enum):
    LCR = "lcr"
    NSFR = "nsfr" 
    LEVERAGE_RATIO = "leverage_ratio"
    CONCENTRATION = "concentration"
    STRESS_TEST = "stress_test"

class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

class LCRMetrics(BaseModel):
    """Liquidity Coverage Ratio metrics"""
    total_hqla: Decimal = Field(..., description="High Quality Liquid Assets")
    level_1_assets: Decimal = Field(default=Decimal('0'))
    level_2a_assets: Decimal = Field(default=Decimal('0'))
    level_2b_assets: Decimal = Field(default=Decimal('0'))
    total_net_outflows: Decimal = Field(..., description="Total net cash outflows")
    lcr_ratio: Decimal = Field(..., description="LCR ratio as percentage")
    minimum_requirement: Decimal = Field(default=Decimal('100'), description="Regulatory minimum")
    
    @validator('lcr_ratio')
    def calculate_lcr(cls, v, values):
        if 'total_hqla' in values and 'total_net_outflows' in values:
            if values['total_net_outflows'] > 0:
                calculated = (values['total_hqla'] / values['total_net_outflows']) * 100
                return calculated
        return v

class NSFRMetrics(BaseModel):
    """Net Stable Funding Ratio metrics"""
    available_stable_funding: Decimal = Field(..., description="Available stable funding")
    required_stable_funding: Decimal = Field(..., description="Required stable funding")  
    nsfr_ratio: Decimal = Field(..., description="NSFR ratio as percentage")
    minimum_requirement: Decimal = Field(default=Decimal('100'), description="Regulatory minimum")
    
    @validator('nsfr_ratio')
    def calculate_nsfr(cls, v, values):
        if 'available_stable_funding' in values and 'required_stable_funding' in values:
            if values['required_stable_funding'] > 0:
                calculated = (values['available_stable_funding'] / values['required_stable_funding']) * 100
                return calculated
        return v

class ConcentrationMetrics(BaseModel):
    """Concentration risk metrics"""
    largest_counterparty_exposure: Decimal
    largest_sector_exposure: Decimal
    largest_geography_exposure: Decimal
    counterparty_concentration_ratio: Decimal
    sector_concentration_ratio: Decimal
    geography_concentration_ratio: Decimal

class RiskAlert(BaseModel):
    alert_id: str = Field(..., description="Unique alert identifier")
    metric_type: RiskMetricType
    severity: AlertSeverity
    threshold_breached: Decimal
    current_value: Decimal
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    acknowledged: bool = Field(default=False)
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None

class RiskMetrics(BaseModel):
    """Complete risk metrics snapshot"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    institution_id: str = Field(..., description="Institution identifier")
    business_date: datetime = Field(..., description="Business date for metrics")
    
    # Core metrics
    lcr_metrics: LCRMetrics
    nsfr_metrics: NSFRMetrics
    concentration_metrics: ConcentrationMetrics
    
    # Additional metrics
    leverage_ratio: Optional[Decimal] = None
    total_assets: Decimal = Field(..., description="Total assets")
    total_liabilities: Decimal = Field(..., description="Total liabilities")
    tier1_capital: Decimal = Field(..., description="Tier 1 capital")
    
    # Alerts
    active_alerts: List[RiskAlert] = Field(default_factory=list)
    
    # Metadata
    calculation_version: str = Field(default="1.0")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def get_overall_risk_score(self) -> Decimal:
        """Calculate overall risk score based on all metrics"""
        score = Decimal('100')
        
        # LCR penalty
        if self.lcr_metrics.lcr_ratio < 100:
            score -= (100 - self.lcr_metrics.lcr_ratio) * Decimal('0.5')
        
        # NSFR penalty  
        if self.nsfr_metrics.nsfr_ratio < 100:
            score -= (100 - self.nsfr_metrics.nsfr_ratio) * Decimal('0.3')
            
        # Concentration penalty
        if self.concentration_metrics.counterparty_concentration_ratio > 10:
            score -= (self.concentration_metrics.counterparty_concentration_ratio - 10) * Decimal('2')
            
        return max(score, Decimal('0'))

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: str(v)
        }

class RiskMetricsResponse(BaseModel):
    institution_id: str
    business_date: datetime
    lcr_ratio: Decimal
    nsfr_ratio: Decimal
    overall_risk_score: Decimal
    alert_count: int
    critical_alerts: int
    calculated_at: datetime