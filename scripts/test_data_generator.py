import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random
import uuid
from decimal import Decimal

class TestDataGenerator:
    """Generate test data for the liquidity risk management system."""
    
    def __init__(self):
        self.banks = ['BANK001', 'BANK002', 'BANK003', 'BANK004', 'BANK005']
        self.asset_classes = ['cash', 'government_bonds', 'corporate_bonds', 'equities', 'derivatives']
        self.currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CHF']
        self.instruments = {
            'cash': ['demand_deposits', 'time_deposits'],
            'government_bonds': ['treasury_bills', 'treasury_notes', 'treasury_bonds'],
            'corporate_bonds': ['investment_grade', 'high_yield', 'convertible'],
            'equities': ['common_stock', 'preferred_stock', 'etf'],
            'derivatives': ['interest_rate_swap', 'fx_forward', 'credit_default_swap']
        }
    
    def generate_liquidity_positions(self, num_records: int = 1000, 
                                   start_date: datetime = None,
                                   end_date: datetime = None) -> pd.DataFrame:
        """Generate synthetic liquidity position data."""
        
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()
        
        records = []
        
        for _ in range(num_records):
            bank_id = random.choice(self.banks)
            asset_class = random.choice(self.asset_classes)
            currency = random.choice(self.currencies)
            
            # Generate position date
            days_diff = (end_date - start_date).days
            position_date = start_date + timedelta(days=random.randint(0, days_diff))
            
            # Generate amounts based on asset class
            if asset_class == 'cash':
                notional = random.uniform(1000000, 100000000)  # $1M - $100M
                market_value = notional
            elif asset_class == 'government_bonds':
                notional = random.uniform(5000000, 500000000)  # $5M - $500M
                market_value = notional * random.uniform(0.95, 1.05)
            elif asset_class == 'corporate_bonds':
                notional = random.uniform(1000000, 200000000)  # $1M - $200M
                market_value = notional * random.uniform(0.80, 1.10)
            elif asset_class == 'equities':
                notional = random.uniform(500000, 50000000)  # $500K - $50M
                market_value = notional * random.uniform(0.70, 1.30)
            else:  # derivatives
                notional = random.uniform(10000000, 1000000000)  # $10M - $1B
                market_value = notional * random.uniform(-0.20, 0.20)
            
            record = {
                'position_id': str(uuid.uuid4()),
                'bank_id': bank_id,
                'account_id': f"ACC{random.randint(10000, 99999)}",
                'position_date': position_date.isoformat(),
                'asset_class': asset_class,
                'instrument_type': random.choice(self.instruments.get(asset_class, ['unknown'])),
                'notional_amount': round(notional, 2),
                'market_value': round(market_value, 2),
                'currency': currency,
                'time_to_maturity_days': random.randint(1, 3650) if asset_class != 'cash' else None,
                'counterparty': f"COUNTERPARTY{random.randint(1, 100)}",
                'rating': random.choice(['AAA', 'AA', 'A', 'BBB', 'BB', 'B']) if asset_class in ['corporate_bonds', 'derivatives'] else None
            }
            
            records.append(record)
        
        df = pd.DataFrame(records)
        return df
    
    def generate_csv_files(self, output_dir: str = './test_data', 
                          num_files: int = 5, 
                          records_per_file: int = 1000):
        """Generate CSV files for testing."""
        
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        for i in range(num_files):
            df = self.generate_liquidity_positions(records_per_file)
            filename = f"liquidity_positions_{datetime.now().strftime('%Y%m%d')}_{i+1:03d}.csv"
            filepath = os.path.join(output_dir, filename)
            df.to_csv(filepath, index=False)
            print(f"Generated {filepath} with {len(df)} records")
    
    def generate_stress_scenarios(self) -> dict:
        """Generate stress test scenarios."""
        
        return {
            'baseline': 0.0,
            'mild_recession': -0.05,
            'moderate_recession': -0.15,
            'severe_recession': -0.30,
            'financial_crisis': -0.50,
            'liquidity_crisis': -0.40,
            'credit_crunch': -0.25,
            'market_volatility': -0.20
        }

if __name__ == "__main__":
    generator = TestDataGenerator()
    
    # Generate test data files
    generator.generate_csv_files(num_files=10, records_per_file=500)
    
    # Generate sample data for manual testing
    sample_df = generator.generate_liquidity_positions(100)
    print("\nSample data:")
    print(sample_df.head())
    print(f"\nData shape: {sample_df.shape}")
    print(f"Banks: {sample_df['bank_id'].unique()}")
    print(f"Asset classes: {sample_df['asset_class'].unique()}")
