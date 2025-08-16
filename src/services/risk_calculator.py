import logging
from typing import Dict, Any, List
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta
from src.models.trade import Trade
from src.models.deposit import Deposit
from src.models.risk_metrics import RiskMetrics, LCRMetrics, NSFRMetrics, ConcentrationMetrics
from src.config.regulatory_config import LCR_CONFIG, NSFR_CONFIG, RISK_WEIGHTS, COUNTERPARTY_CATEGORIES
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class RiskCalculator:
    def __init__(self):
        self.precision = Decimal('0.01')  # 2 decimal places
    
    def calculate_lcr(self, trades: List[Trade], deposits: List[Deposit]) -> LCRMetrics:
        """Calculate Liquidity Coverage Ratio"""
        logger.info("Calculating LCR metrics")
        
        # Calculate High Quality Liquid Assets (HQLA)
        hqla_assets = self._calculate_hqla(trades)
        
        # Calculate cash outflows
        outflows = self._calculate_cash_outflows(deposits, trades)
        
        # Calculate cash inflows (capped at 75% of outflows)
        inflows = self._calculate_cash_inflows(trades)
        max_inflows = outflows * Decimal('0.75')
        capped_inflows = min(inflows, max_inflows)
        
        # Net cash outflows
        net_outflows = max(outflows - capped_inflows, outflows * Decimal('0.25'))
        
        # LCR ratio
        lcr_ratio = (hqla_assets['total'] / net_outflows * 100) if net_outflows > 0 else Decimal('0')
        
        return LCRMetrics(
            total_hqla=hqla_assets['total'],
            level_1_assets=hqla_assets['level_1'],
            level_2a_assets=hqla_assets['level_2a'],
            level_2b_assets=hqla_assets['level_2b'],
            total_net_outflows=net_outflows,
            lcr_ratio=lcr_ratio.quantize(self.precision, rounding=ROUND_HALF_UP)
        )
    
    def calculate_nsfr(self, trades: List[Trade], deposits: List[Deposit]) -> NSFRMetrics:
        """Calculate Net Stable Funding Ratio"""
        logger.info("Calculating NSFR metrics")
        
        # Calculate Available Stable Funding (ASF)
        asf = self._calculate_available_stable_funding(deposits)
        
        # Calculate Required Stable Funding (RSF)
        rsf = self._calculate_required_stable_funding(trades)
        
        # NSFR ratio
        nsfr_ratio = (asf / rsf * 100) if rsf > 0 else Decimal('0')
        
        return NSFRMetrics(
            available_stable_funding=asf,
            required_stable_funding=rsf,
            nsfr_ratio=nsfr_ratio.quantize(self.precision, rounding=ROUND_HALF_UP)
        )
    
    def calculate_concentration_risk(self, trades: List[Trade]) -> ConcentrationMetrics:
        """Calculate concentration risk metrics"""
        logger.info("Calculating concentration risk metrics")
        
        # Group exposures by counterparty, sector, geography
        counterparty_exposures = {}
        sector_exposures = {}
        geography_exposures = {}
        total_exposure = Decimal('0')
        
        for trade in trades:
            exposure = trade.notional_amount
            total_exposure += exposure
            
            # Counterparty concentration
            counterparty_exposures[trade.counterparty_id] = \
                counterparty_exposures.get(trade.counterparty_id, Decimal('0')) + exposure
            
            # Sector concentration (from metadata)
            sector = trade.metadata.get('sector', 'Unknown')
            sector_exposures[sector] = sector_exposures.get(sector, Decimal('0')) + exposure
            
            # Geography concentration (from metadata)
            geography = trade.metadata.get('geography', 'Unknown')
            geography_exposures[geography] = geography_exposures.get(geography, Decimal('0')) + exposure
        
        # Find largest exposures
        largest_counterparty = max(counterparty_exposures.values()) if counterparty_exposures else Decimal('0')
        largest_sector = max(sector_exposures.values()) if sector_exposures else Decimal('0')
        largest_geography = max(geography_exposures.values()) if geography_exposures else Decimal('0')
        
        # Calculate concentration ratios
        counterparty_ratio = (largest_counterparty / total_exposure * 100) if total_exposure > 0 else Decimal('0')
        sector_ratio = (largest_sector / total_exposure * 100) if total_exposure > 0 else Decimal('0')
        geography_ratio = (largest_geography / total_exposure * 100) if total_exposure > 0 else Decimal('0')
        
        return ConcentrationMetrics(
            largest_counterparty_exposure=largest_counterparty,
            largest_sector_exposure=largest_sector,
            largest_geography_exposure=largest_geography,
            counterparty_concentration_ratio=counterparty_ratio.quantize(self.precision),
            sector_concentration_ratio=sector_ratio.quantize(self.precision),
            geography_concentration_ratio=geography_ratio.quantize(self.precision)
        )
    
    def _calculate_hqla(self, trades: List[Trade]) -> Dict[str, Decimal]:
        """Calculate High Quality Liquid Assets by level"""
        hqla = {
            'level_1': Decimal('0'),
            'level_2a': Decimal('0'),
            'level_2b': Decimal('0'),
            'total': Decimal('0')
        }
        
        for trade in trades:
            if trade.instrument_type in ['government_bond', 'central_bank_reserves']:
                # Level 1 assets - no haircut
                hqla['level_1'] += trade.notional_amount
            elif trade.instrument_type == 'corporate_bond' and trade.counterparty_rating in ['AAA', 'AA+', 'AA', 'AA-']:
                # Level 2A assets - 15% haircut
                hqla['level_2a'] += trade.notional_amount * Decimal('0.85')
            elif trade.instrument_type in ['equity', 'corporate_bond']:
                # Level 2B assets - 50% haircut
                hqla['level_2b'] += trade.notional_amount * Decimal('0.50')
        
        # Apply caps: Level 2A max 40% of total, Level 2B max 15% of total
        total_before_caps = hqla['level_1'] + hqla['level_2a'] + hqla['level_2b']
        
        if total_before_caps > 0:
            level_2a_cap = total_before_caps * Decimal('0.40')
            level_2b_cap = total_before_caps * Decimal('0.15')
            
            hqla['level_2a'] = min(hqla['level_2a'], level_2a_cap)
            hqla['level_2b'] = min(hqla['level_2b'], level_2b_cap)
        
        hqla['total'] = hqla['level_1'] + hqla['level_2a'] + hqla['level_2b']
        return hqla
    
    def _calculate_cash_outflows(self, deposits: List[Deposit], trades: List[Trade]) -> Decimal:
        """Calculate total cash outflows for LCR"""
        total_outflows = Decimal('0')
        
        # Deposit outflows
        for deposit in deposits:
            outflow_rate = deposit.get_lcr_outflow_rate()
            total_outflows += deposit.amount * outflow_rate
        
        # Trading outflows (simplified)
        for trade in trades:
            if trade.instrument_type == 'derivative':
                total_outflows += trade.notional_amount * Decimal('0.20')  # 20% for derivatives
        
        return total_outflows
    
    def _calculate_cash_inflows(self, trades: List[Trade]) -> Decimal:
        """Calculate cash inflows for LCR"""
        total_inflows = Decimal('0')
        
        for trade in trades:
            if trade.trade_type == 'sell' and trade.maturity_date:
                # Assume 50% inflow rate for maturing assets
                days_to_maturity = (trade.maturity_date - datetime.utcnow()).days
                if days_to_maturity <= 30:  # Within 30-day LCR window
                    total_inflows += trade.notional_amount * Decimal('0.50')
        
        return total_inflows
    
    def _calculate_available_stable_funding(self, deposits: List[Deposit]) -> Decimal:
        """Calculate Available Stable Funding for NSFR"""
        total_asf = Decimal('0')
        
        for deposit in deposits:
            if deposit.is_stable_funding():
                asf_factor = Decimal('0.95')  # 95% for stable deposits
            else:
                asf_factor = Decimal('0.90')  # 90% for less stable deposits
            
            total_asf += deposit.amount * asf_factor
        
        return total_asf
    
    def _calculate_required_stable_funding(self, trades: List[Trade]) -> Decimal:
        """Calculate Required Stable Funding for NSFR"""
        total_rsf = Decimal('0')
        
        for trade in trades:
            if trade.instrument_type == 'cash':
                rsf_factor = Decimal('0.00')  # 0% for cash
            elif trade.instrument_type == 'government_bond':
                rsf_factor = Decimal('0.05')  # 5% for Level 1 assets
            elif trade.instrument_type == 'corporate_bond':
                if trade.counterparty_rating in ['AAA', 'AA+', 'AA', 'AA-']:
                    rsf_factor = Decimal('0.20')  # 20% for Level 2A assets
                else:
                    rsf_factor = Decimal('0.50')  # 50% for Level 2B assets
            else:
                rsf_factor = Decimal('1.00')  # 100% for other assets
            
            total_rsf += trade.notional_amount * rsf_factor
        
        return total_rsf