#!/usr/bin/env python3
"""
Historical Transaction Data Generator
Generates 365 days of transaction history for FSI demo
"""

import os
import json
from datetime import datetime
from transaction_generator import TransactionGenerator

def generate_historical_data(days: int = 365, output_file: str = None):
    """Generate historical transaction data and save to JSON file"""
    
    generator = TransactionGenerator()
    
    # Generate historical data
    print(f"🏦 FSI Demo - Historical Transaction Generator")
    print(f"📅 Generating {days} days of historical data...")
    
    transactions = generator.generate_historical_data(days)
    
    # Statistics
    total_amount = sum(t['transaction_amount'] for t in transactions)
    anomaly_count = sum(1 for t in transactions if t.get('is_anomaly', False))
    
    print(f"\n📊 Generation Summary:")
    print(f"   📝 Total transactions: {len(transactions):,}")
    print(f"   💰 Total amount: ${total_amount:,.2f}")
    print(f"   🔥 Anomalies: {anomaly_count:,} ({anomaly_count/len(transactions)*100:.1f}%)")
    print(f"   👥 Customers: 1001-1100 (100 customers)")
    print(f"   🏷️  Transaction types: {len(generator.transaction_types)} types")
    
    # Set default output file if not provided
    if output_file is None:
        output_file = f"historical_transactions_{days}days_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Save to JSON lines format (for Snowflake COPY command)
    print(f"\n💾 Saving to: {output_file}")
    with open(output_file, 'w') as f:
        for transaction in transactions:
            # Remove debug fields for database insert
            db_transaction = {k: v for k, v in transaction.items() if k != 'is_anomaly'}
            f.write(json.dumps(db_transaction) + '\n')
    
    print(f"✅ Historical data saved successfully!")
    print(f"\n🚀 Ready to load into Snowflake:")
    print(f"   snow sql --connection nfm_demo_keypair --query \"")
    print(f"   PUT file://{os.path.abspath(output_file)} @FSI_DEMO.RAW_DATA.temp_stage;")
    print(f"   COPY INTO FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE")
    print(f"   FROM @FSI_DEMO.RAW_DATA.temp_stage/{os.path.basename(output_file)}")
    print(f"   FILE_FORMAT = (TYPE = 'JSON');\"")
    
    return output_file, transactions

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate historical transaction data for FSI demo')
    parser.add_argument('--days', type=int, default=365, help='Number of days of historical data (default: 365)')
    parser.add_argument('--output', type=str, help='Output file name (default: auto-generated)')
    
    args = parser.parse_args()
    
    generate_historical_data(args.days, args.output)
