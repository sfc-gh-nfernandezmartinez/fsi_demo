"""
Shared Transaction Generation Logic
Generates realistic leisure/lifestyle payment transactions for FSI demo
"""

import random
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
import uuid

class TransactionGenerator:
    """Generates realistic transaction data for FSI demo"""
    
    def __init__(self):
        # Customer and loan configuration (100 customers, 1:1 mapping)
        self.customer_range = range(1001, 1101)  # 100 customers: 1001-1100
        
        # Realistic leisure/lifestyle transaction types with weights
        self.transaction_types = [
            'leisure_payment',      # 40% - Most common (restaurants, entertainment)
            'subscription_fee',     # 15% - Streaming, gym, etc.
            'travel_expense',       # 10% - Hotels, flights
            'shopping',            # 10% - Retail purchases
            'dining',              # 7%  - Restaurants, food delivery
            'entertainment',       # 5%  - Movies, events, games
            'fitness_wellness',    # 5%  - Gym, spa, health
            'education',          # 3%  - Courses, training
            'luxury_purchase',    # 3%  - High-end items (can be anomaly)
            'miscellaneous'       # 2%  - Other lifestyle expenses
        ]
        
        self.transaction_weights = [40, 15, 10, 10, 7, 5, 5, 3, 3, 2]
        
        # Amount ranges by transaction type (normal distribution)
        self.amount_ranges = {
            'leisure_payment': (50, 500),
            'subscription_fee': (10, 100),
            'travel_expense': (200, 2000),
            'shopping': (30, 800),
            'dining': (20, 200),
            'entertainment': (25, 300),
            'fitness_wellness': (50, 500),
            'education': (100, 1000),
            'luxury_purchase': (500, 5000),  # Can be anomaly
            'miscellaneous': (20, 300)
        }
        
        # Anomaly configuration
        self.anomaly_probability = 0.05  # 5% chance
        self.anomaly_amount_range = (25000, 75000)  # Large amounts
        

    
    def generate_amount(self, transaction_type: str, is_anomaly: bool = False) -> float:
        """Generate transaction amount based on type and anomaly flag"""
        if is_anomaly:
            return round(random.uniform(*self.anomaly_amount_range), 2)
        
        min_amount, max_amount = self.amount_ranges.get(transaction_type, (50, 500))
        # Add some randomness with normal distribution
        mean = (min_amount + max_amount) / 2
        std_dev = (max_amount - min_amount) / 6  # 99.7% within range
        
        amount = random.normalvariate(mean, std_dev)
        # Clamp to range and round
        amount = max(min_amount, min(max_amount, amount))
        return round(amount, 2)
    
    def generate_transaction(self, transaction_date: datetime, transaction_id: int = None) -> Dict[str, Any]:
        """Generate a single transaction record"""
        # Select customer (loan_id will be derived from customer_id in queries)
        customer_id = random.choice(self.customer_range)
        
        # Select transaction type (weighted random)
        transaction_type = random.choices(
            population=self.transaction_types,
            weights=self.transaction_weights,
            k=1
        )[0]
        
        # Determine if this is an anomaly
        is_anomaly = random.random() < self.anomaly_probability
        
        # Generate amount
        amount = self.generate_amount(transaction_type, is_anomaly)
        
        # Auto-generate transaction_id if not provided
        if transaction_id is None:
            # Use timestamp + random for uniqueness
            transaction_id = int(transaction_date.timestamp() * 1000) + random.randint(1, 999)
        
        return {
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "transaction_date": transaction_date.strftime("%Y-%m-%d"),
            "transaction_amount": amount,
            "transaction_type": transaction_type,
            "is_anomaly": is_anomaly  # For monitoring/debugging (not stored in DB)
        }
    
    def generate_daily_transactions(self, date: datetime, min_transactions: int = 50, max_transactions: int = 200) -> List[Dict[str, Any]]:
        """Generate realistic number of transactions for a given day"""
        # Weekend adjustment (fewer transactions)
        if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            min_transactions = int(min_transactions * 0.7)
            max_transactions = int(max_transactions * 0.7)
        
        num_transactions = random.randint(min_transactions, max_transactions)
        transactions = []
        
        for i in range(num_transactions):
            # Spread transactions throughout the day
            hour = random.randint(6, 23)  # 6 AM to 11 PM
            minute = random.randint(0, 59)
            
            transaction_time = date.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # Generate unique transaction_id for the day
            transaction_id = int(f"{date.strftime('%Y%m%d')}{i+1:04d}")
            
            transaction = self.generate_transaction(transaction_time, transaction_id)
            transactions.append(transaction)
        
        return transactions
    
    def generate_historical_data(self, days: int = 365) -> List[Dict[str, Any]]:
        """Generate historical transaction data for specified number of days"""
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=days-1)
        
        all_transactions = []
        current_date = start_date
        
        print(f"Generating {days} days of historical data...")
        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        while current_date <= end_date:
            daily_transactions = self.generate_daily_transactions(current_date)
            all_transactions.extend(daily_transactions)
            
            if current_date.day % 30 == 0:  # Progress indicator
                print(f"Generated data through {current_date.strftime('%Y-%m-%d')} - {len(all_transactions)} transactions so far")
            
            current_date += timedelta(days=1)
        
        print(f"Historical data generation complete: {len(all_transactions)} transactions")
        return all_transactions
    
    def transactions_to_json_lines(self, transactions: List[Dict[str, Any]]) -> str:
        """Convert transactions to JSON lines format for Snowflake"""
        json_lines = []
        for transaction in transactions:
            # Remove debug fields for database insert
            db_transaction = {k: v for k, v in transaction.items() if k != 'is_anomaly'}
            json_lines.append(json.dumps(db_transaction))
        
        return '\n'.join(json_lines)
