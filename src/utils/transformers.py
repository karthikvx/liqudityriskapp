import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import re
from aws_lambda_powertools import Logger

logger = Logger()

class LiquidityRiskTransformer:
    """Transformer for liquidity risk calculations and data processing."""
    
    def __init__(self):
        self.risk_weights = {
            'cash': 0.0,
            'government_bonds': 0.0,
            'corporate_bonds': 0.2,
            'equities': 0.5,
            'derivatives': 0.8,
            'real_estate': 1.0
        }
        
        self.haircut_rates = {
            'aaa_bonds': 0.005,
            'aa_bonds': 0.02,
            'a_bonds': 0.05,
            'bbb_bonds': 0.15,
            'equities': 0.25,
            'real_estate': 0.5
        }
    
    def calculate_liquidity_coverage_ratio(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Liquidity Coverage Ratio (LCR) for each bank."""
        try:
            # Group by bank and date
            grouped = df.groupby(['bank_id', 'position_date'])
            
            lcr_results = []
            
            for (bank_id, date), group in grouped:
                # Calculate High Quality Liquid Assets (HQLA)
                hqla = self._calculate_hqla(group)
                
                # Calculate Total Net Cash Outflows
                net_cash_outflows = self._calculate_net_cash_outflows(group)
                
                # Calculate LCR
                lcr = (hqla / net_cash_outflows) if net_cash_outflows > 0 else float('inf')
                
                lcr_results.append({
                    'bank_id': bank_id,
                    'calculation_date': date,
                    'hqla': hqla,
                    'net_cash_outflows': net_cash_outflows,
                    'lcr': lcr,
                    'lcr_compliant': lcr >= 1.0
                })
            
            result_df = pd.DataFrame(lcr_results)
            logger.info(f"Calculated LCR for {len(result_df)} bank-date combinations")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error calculating LCR: {str(e)}")
            raise
    
    def calculate_net_stable_funding_ratio(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Net Stable Funding Ratio (NSFR) for each bank."""
        try:
            grouped = df.groupby(['bank_id', 'position_date'])
            
            nsfr_results = []
            
            for (bank_id, date), group in grouped:
                # Calculate Available Stable Funding
                asf = self._calculate_available_stable_funding(group)
                
                # Calculate Required Stable Funding
                rsf = self._calculate_required_stable_funding(group)
                
                # Calculate NSFR
                nsfr = (asf / rsf) if rsf > 0 else float('inf')
                
                nsfr_results.append({
                    'bank_id': bank_id,
                    'calculation_date': date,
                    'available_stable_funding': asf,
                    'required_stable_funding': rsf,
                    'nsfr': nsfr,
                    'nsfr_compliant': nsfr >= 1.0
                })
            
            result_df = pd.DataFrame(nsfr_results)
            logger.info(f"Calculated NSFR for {len(result_df)} bank-date combinations")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error calculating NSFR: {str(e)}")
            raise
    
    def perform_stress_testing(self, df: pd.DataFrame, 
                             stress_scenarios: Dict[str, float]) -> pd.DataFrame:
        """Perform stress testing on liquidity positions."""
        try:
            stress_results = []
            
            for scenario_name, stress_factor in stress_scenarios.items():
                scenario_df = df.copy()
                
                # Apply stress factor to market values
                scenario_df['stressed_market_value'] = (
                    scenario_df['market_value'] * (1 + stress_factor)
                )
                
                # Recalculate liquidity metrics under stress
                stressed_lcr = self.calculate_liquidity_coverage_ratio(scenario_df)
                stressed_nsfr = self.calculate_net_stable_funding_ratio(scenario_df)
                
                # Combine results
                for _, lcr_row in stressed_lcr.iterrows():
                    bank_id = lcr_row['bank_id']
                    date = lcr_row['calculation_date']
                    
                    # Find corresponding NSFR row
                    nsfr_row = stressed_nsfr[
                        (stressed_nsfr['bank_id'] == bank_id) & 
                        (stressed_nsfr['calculation_date'] == date)
                    ].iloc[0] if len(stressed_nsfr[
                        (stressed_nsfr['bank_id'] == bank_id) & 
                        (stressed_nsfr['calculation_date'] == date)
                    ]) > 0 else None
                    
                    stress_results.append({
                        'scenario_name': scenario_name,
                        'stress_factor': stress_factor,
                        'bank_id': bank_id,
                        'calculation_date': date,
                        'stressed_lcr': lcr_row['lcr'],
                        'stressed_nsfr': nsfr_row['nsfr'] if nsfr_row is not None else None,
                        'lcr_breach': lcr_row['lcr'] < 1.0,
                        'nsfr_breach': nsfr_row['nsfr'] < 1.0 if nsfr_row is not None else False,
                        'risk_level': self._determine_risk_level(lcr_row['lcr'], 
                                                               nsfr_row['nsfr'] if nsfr_row is not None else 1.0)
                    })
            
            result_df = pd.DataFrame(stress_results)
            logger.info(f"Completed stress testing for {len(stress_scenarios)} scenarios")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error performing stress testing: {str(e)}")
            raise
    
    def _calculate_hqla(self, group: pd.DataFrame) -> float:
        """Calculate High Quality Liquid Assets."""
        # Level 1 assets (no haircut): cash, central bank reserves, government bonds
        level1_assets = group[group['asset_class'].isin(['cash', 'government_bonds'])]
        level1_hqla = level1_assets['market_value'].sum()
        
        # Level 2A assets (15% haircut): high-quality corporate bonds
        level2a_assets = group[
            (group['asset_class'] == 'corporate_bonds') & 
            (group['liquidity_score'] >= 0.85)
        ]
        level2a_hqla = level2a_assets['market_value'].sum() * 0.85
        
        # Level 2B assets (25-50% haircut): lower quality assets
        level2b_assets = group[
            (group['asset_class'] == 'corporate_bonds') & 
            (group['liquidity_score'] < 0.85) & 
            (group['liquidity_score'] >= 0.5)
        ]
        level2b_hqla = level2b_assets['market_value'].sum() * 0.5
        
        total_hqla = level1_hqla + min(level2a_hqla + level2b_hqla, level1_hqla * 0.67)
        
        return float(total_hqla)
    
    def _calculate_net_cash_outflows(self, group: pd.DataFrame) -> float:
        """Calculate net cash outflows for LCR."""
        # Simplified calculation - in reality this would be much more complex
        total_outflows = group[group['notional_amount'] < 0]['notional_amount'].sum()
        total_inflows = group[group['notional_amount'] > 0]['notional_amount'].sum()
        
        # Apply run-off rates based on asset type
        stressed_outflows = abs(total_outflows) * 1.25  # 25% stress factor
        capped_inflows = min(abs(total_inflows) * 0.75, stressed_outflows * 0.75)
        
        net_outflows = max(stressed_outflows - capped_inflows, stressed_outflows * 0.25)
        
        return float(net_outflows)
    
    def _calculate_available_stable_funding(self, group: pd.DataFrame) -> float:
        """Calculate Available Stable Funding for NSFR."""
        # This is a simplified calculation
        equity_and_liabilities = group['market_value'].sum()
        
        # Apply ASF factors based on funding type
        # Assuming most funding is wholesale funding (50% ASF factor)
        asf = equity_and_liabilities * 0.5
        
        return float(asf)
    
    def _calculate_required_stable_funding(self, group: pd.DataFrame) -> float:
        """Calculate Required Stable Funding for NSFR."""
        total_rsf = 0
        
        for _, row in group.iterrows():
            asset_class = row['asset_class']
            market_value = row['market_value']
            
            # Apply RSF factors based on asset type
            if asset_class in ['cash', 'government_bonds']:
                rsf_factor = 0.05
            elif asset_class == 'corporate_bonds':
                rsf_factor = 0.85 if row['liquidity_score'] > 0.7 else 1.0
            elif asset_class == 'equities':
                rsf_factor = 0.85
            else:
                rsf_factor = 1.0
            
            total_rsf += market_value * rsf_factor
        
        return float(total_rsf)
    
    def _determine_risk_level(self, lcr: float, nsfr: float) -> str:
        """Determine overall risk level based on ratios."""
        if lcr < 0.8 or nsfr < 0.8:
            return 'CRITICAL'
        elif lcr < 1.0 or nsfr < 1.0:
            return 'HIGH'
        elif lcr < 1.2 or nsfr < 1.1:
            return 'MEDIUM'
        else:
            return 'LOW'
