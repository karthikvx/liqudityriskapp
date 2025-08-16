from typing import Dict, List
from enum import Enum

class LiquidityBucket(Enum):
    """Basel III Liquidity Coverage Ratio buckets"""
    LEVEL_1 = "level_1"      # 0% haircut
    LEVEL_2A = "level_2a"    # 15% haircut
    LEVEL_2B = "level_2b"    # 25-50% haircut
    OTHER = "other"          # 100% haircut

class NSFRCategory(Enum):
    """Net Stable Funding Ratio categories"""
    STABLE_FUNDING = "stable_funding"
    LESS_STABLE_FUNDING = "less_stable_funding" 
    WHOLESALE_FUNDING = "wholesale_funding"
    OTHER_LIABILITIES = "other_liabilities"

# LCR Configuration
LCR_CONFIG = {
    "high_quality_liquid_assets": {
        LiquidityBucket.LEVEL_1: {
            "haircut": 0.0,
            "cap_percentage": None,
            "instruments": ["central_bank_reserves", "government_bonds"]
        },
        LiquidityBucket.LEVEL_2A: {
            "haircut": 0.15,
            "cap_percentage": 40.0,
            "instruments": ["corporate_bonds_aa", "covered_bonds"]
        },
        LiquidityBucket.LEVEL_2B: {
            "haircut": 0.50,
            "cap_percentage": 15.0,
            "instruments": ["equities", "corporate_bonds_bbb"]
        }
    },
    "cash_outflows": {
        "retail_deposits": {
            "stable": 0.05,      # 5% outflow
            "less_stable": 0.10  # 10% outflow
        },
        "wholesale_deposits": {
            "operational": 0.25,  # 25% outflow
            "non_operational": 1.0 # 100% outflow
        },
        "secured_funding": 0.0,   # 0% outflow if HQLA collateral
        "unsecured_funding": 1.0, # 100% outflow
        "derivatives": 0.20       # 20% outflow
    },
    "cash_inflows": {
        "cap_percentage": 75.0,   # Max 75% of outflows
        "retail_lending": 0.50,   # 50% inflow
        "wholesale_lending": 1.0  # 100% inflow
    }
}

# NSFR Configuration
NSFR_CONFIG = {
    "available_stable_funding": {
        "capital": 1.0,                    # 100% ASF factor
        "preferred_stock": 1.0,            # 100% ASF factor
        "stable_deposits": 0.95,           # 95% ASF factor
        "less_stable_deposits": 0.90,      # 90% ASF factor
        "wholesale_funding_1year": 1.0,    # 100% ASF factor
        "wholesale_funding_6m_1y": 0.50    # 50% ASF factor
    },
    "required_stable_funding": {
        "cash": 0.0,                       # 0% RSF factor
        "central_bank_exposure": 0.0,      # 0% RSF factor
        "level_1_assets": 0.05,            # 5% RSF factor
        "level_2a_assets": 0.20,           # 20% RSF factor
        "level_2b_assets": 0.50,           # 50% RSF factor
        "corporate_loans": 0.85,           # 85% RSF factor
        "retail_mortgages": 0.65,          # 65% RSF factor
        "other_loans": 1.0                 # 100% RSF factor
    }
}

# Risk Weight Configuration
RISK_WEIGHTS = {
    "AAA": 0.0,
    "AA+": 0.0, "AA": 0.0, "AA-": 0.0,
    "A+": 0.20, "A": 0.20, "A-": 0.20,
    "BBB+": 0.50, "BBB": 0.50, "BBB-": 0.50,
    "BB+": 1.0, "BB": 1.0, "BB-": 1.0,
    "B+": 1.5, "B": 1.5, "B-": 1.5,
    "CCC": 2.0,
    "CC": 2.5,
    "C": 3.0,
    "D": 3.0,
    "UNRATED": 1.0
}

# Counterparty Categories
COUNTERPARTY_CATEGORIES = {
    "CENTRAL_BANK": {
        "risk_weight": 0.0,
        "lcr_treatment": "level_1",
        "nsfr_treatment": "stable_funding"
    },
    "GOVERNMENT": {
        "risk_weight": 0.0,
        "lcr_treatment": "level_1",
        "nsfr_treatment": "stable_funding"
    },
    "BANK": {
        "risk_weight": 0.20,
        "lcr_treatment": "level_2a",
        "nsfr_treatment": "less_stable_funding"
    },
    "CORPORATE": {
        "risk_weight": 1.0,
        "lcr_treatment": "other",
        "nsfr_treatment": "wholesale_funding"
    },
    "RETAIL": {
        "risk_weight": 0.75,
        "lcr_treatment": "other",
        "nsfr_treatment": "stable_funding"
    }
}

# Alert Thresholds
ALERT_THRESHOLDS = {
    "lcr": {
        "critical": 100.0,    # Below 100% is critical
        "warning": 110.0,     # Below 110% is warning
        "target": 120.0       # Target is 120%
    },
    "nsfr": {
        "critical": 100.0,    # Below 100% is critical
        "warning": 105.0,     # Below 105% is warning
        "target": 110.0       # Target is 110%
    },
    "concentration": {
        "single_counterparty": 0.10,  # 10% of capital
        "sector": 0.25,               # 25% of capital
        "geography": 0.30             # 30% of capital
    }
}