#!/usr/bin/env python3
"""
FSI Demo - Customer Data Generator for Iceberg Table
====================================================

Generates realistic customer data that:
- Links to existing mortgage loan_ids 
- Uses customer_ids compatible with transaction data (1001-1100)
- Includes PII fields for governance testing
- Outputs data ready for Iceberg table insertion
"""

import random
import json
from datetime import datetime
from pathlib import Path
import click
from rich.console import Console
from rich.table import Table
from rich.progress import track

console = Console()

class CustomerGenerator:
    def __init__(self):
        # Sample data for realistic generation
        self.first_names = [
            'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
            'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
            'Thomas', 'Sarah', 'Christopher', 'Karen', 'Charles', 'Nancy', 'Daniel', 'Lisa',
            'Matthew', 'Betty', 'Anthony', 'Helen', 'Mark', 'Sandra', 'Donald', 'Donna',
            'Steven', 'Carol', 'Paul', 'Ruth', 'Andrew', 'Sharon', 'Joshua', 'Michelle',
            'Kenneth', 'Laura', 'Kevin', 'Sarah', 'Brian', 'Kimberly', 'George', 'Deborah',
            'Timothy', 'Dorothy', 'Ronald', 'Lisa', 'Jason', 'Nancy', 'Edward', 'Karen',
            'Jeffrey', 'Betty', 'Ryan', 'Helen', 'Jacob', 'Sandra', 'Gary', 'Donna',
            'Nicholas', 'Carol', 'Eric', 'Ruth', 'Jonathan', 'Sharon', 'Stephen', 'Michelle',
            'Larry', 'Laura', 'Justin', 'Sarah', 'Scott', 'Kimberly', 'Brandon', 'Deborah',
            'Benjamin', 'Dorothy', 'Samuel', 'Amy', 'Gregory', 'Angela', 'Alexander', 'Emily',
            'Patrick', 'Brenda', 'Frank', 'Emma', 'Raymond', 'Olivia', 'Jack', 'Cynthia'
        ]
        
        self.last_names = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
            'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas',
            'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White',
            'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young',
            'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
            'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell',
            'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans', 'Turner', 'Diaz', 'Parker',
            'Cruz', 'Edwards', 'Collins', 'Reyes', 'Stewart', 'Morris', 'Morales', 'Murphy',
            'Cook', 'Rogers', 'Gutierrez', 'Ortiz', 'Morgan', 'Cooper', 'Peterson', 'Bailey',
            'Reed', 'Kelly', 'Howard', 'Ramos', 'Kim', 'Cox', 'Ward', 'Richardson',
            'Watson', 'Brooks', 'Chavez', 'Wood', 'James', 'Bennett', 'Gray', 'Mendoza',
            'Ruiz', 'Hughes', 'Price', 'Alvarez', 'Castillo', 'Sanders', 'Patel', 'Myers'
        ]
        
        # US area codes for realistic phone numbers
        self.area_codes = [
            '212', '646', '917', '718', '347', '929',  # NY
            '415', '628', '650', '925', '510',         # CA Bay Area  
            '213', '323', '310', '424', '747',         # CA LA
            '305', '786', '954', '561', '239',         # FL
            '312', '773', '630', '708', '847',         # IL
            '617', '857', '781', '339', '508',         # MA
            '206', '253', '425', '360', '564',         # WA
            '303', '720', '970', '719',                # CO
            '404', '678', '770', '470',                # GA
            '512', '737', '713', '281', '832', '409'   # TX
        ]

    def generate_phone_number(self):
        """Generate a realistic US phone number"""
        area_code = random.choice(self.area_codes)
        exchange = f"{random.randint(200, 999)}"
        number = f"{random.randint(1000, 9999)}"
        return f"({area_code}) {exchange}-{number}"

    def generate_customer(self, customer_id, loan_id):
        """Generate a single customer record"""
        return {
            'customer_id': customer_id,
            'loan_id': str(loan_id),  # Ensure string format to match mortgage data
            'first_name': random.choice(self.first_names),
            'last_name': random.choice(self.last_names),
            'phone_number': self.generate_phone_number()
        }

    def get_sample_loan_ids(self, num_loans=100):
        """
        Generate sample loan IDs that would exist in mortgage data.
        In real scenario, this would query the MORTGAGE_TABLE.
        """
        # Based on the mortgage CSV sample, loan_ids appear to be like '361354'
        base_loan_ids = [
            '361354', '361355', '361356', '361357', '361358', '361359', '361360',
            '361361', '361362', '361363', '361364', '361365', '361366', '361367',
            '361368', '361369', '361370', '361371', '361372', '361373', '361374'
        ]
        
        # Generate more loan IDs in similar pattern
        additional_loans = [str(361000 + i) for i in range(100, 200)]
        all_loans = base_loan_ids + additional_loans
        
        # Return a sample ensuring we have enough for our customers
        return random.sample(all_loans, min(num_loans, len(all_loans)))

    def generate_customers(self, num_customers=100):
        """Generate a dataset of customers with loan associations"""
        console.print(f"🏦 Generating {num_customers} customer records...")
        
        # Customer IDs from 1001-1100 (matching transaction generator range)
        customer_ids = list(range(1001, 1001 + num_customers))
        
        # Get sample loan IDs (some customers may share loans)
        available_loan_ids = self.get_sample_loan_ids(80)  # 80 unique loans for 100 customers
        
        customers = []
        for i, customer_id in track(enumerate(customer_ids), description="Generating customers"):
            # Some customers share loans (family members, co-signers)
            loan_id = random.choice(available_loan_ids)
            customer = self.generate_customer(customer_id, loan_id)
            customers.append(customer)
            
        console.print(f"✅ Generated {len(customers)} customers")
        console.print(f"📊 Using {len(set(c['loan_id'] for c in customers))} unique loan IDs")
        return customers

    def save_to_json(self, customers, filename):
        """Save customers to JSON file for staging"""
        output_path = Path(filename)
        output_path.parent.mkdir(exist_ok=True)
        
        with open(output_path, 'w') as f:
            for customer in customers:
                json.dump(customer, f)
                f.write('\n')  # NDJSON format for Snowflake
                
        console.print(f"💾 Saved customer data to: {output_path}")
        return output_path

    def generate_insert_statements(self, customers):
        """Generate SQL INSERT statements for Iceberg table"""
        statements = []
        statements.append("-- Generated customer data INSERT statements")
        statements.append(f"-- Generated: {datetime.now().isoformat()}")
        statements.append("-- Target: CUSTOMER_TABLE (Iceberg)")
        statements.append("")
        
        for customer in customers:
            sql = f"""INSERT INTO CUSTOMER_TABLE (customer_id, loan_id, first_name, last_name, phone_number) 
VALUES ({customer['customer_id']}, '{customer['loan_id']}', '{customer['first_name']}', '{customer['last_name']}', '{customer['phone_number']}');"""
            statements.append(sql)
            
        return '\n'.join(statements)

    def display_sample(self, customers, num_samples=10):
        """Display sample of generated customers in a nice table"""
        table = Table(title=f"Sample Customer Data (showing {num_samples} of {len(customers)})")
        table.add_column("Customer ID", style="cyan")
        table.add_column("Loan ID", style="green") 
        table.add_column("First Name", style="yellow")
        table.add_column("Last Name", style="magenta")
        table.add_column("Phone Number", style="blue")
        
        for customer in customers[:num_samples]:
            table.add_row(
                str(customer['customer_id']),
                customer['loan_id'],
                customer['first_name'],
                customer['last_name'],
                customer['phone_number']
            )
            
        console.print(table)

@click.group()
def cli():
    """FSI Demo Customer Data Generator"""
    pass

@cli.command()
@click.option('--customers', '-c', default=100, help='Number of customers to generate')
@click.option('--output-dir', '-o', default='Cursor_Tests', help='Output directory for files')
@click.option('--preview', is_flag=True, help='Show preview of generated data')
def generate(customers, output_dir, preview):
    """Generate customer data for Iceberg table"""
    
    generator = CustomerGenerator()
    
    # Generate customer data
    customer_data = generator.generate_customers(customers)
    
    if preview:
        generator.display_sample(customer_data, 10)
    
    # Save outputs
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Save JSON for staging
    json_file = generator.save_to_json(customer_data, output_dir / 'customer_data.json')
    
    # Generate SQL INSERT statements
    sql_statements = generator.generate_insert_statements(customer_data)
    sql_file = output_dir / 'customer_inserts.sql'
    with open(sql_file, 'w') as f:
        f.write(sql_statements)
    
    console.print(f"📄 Generated SQL INSERT statements: {sql_file}")
    
    # Summary
    console.print("\n🎯 [bold green]Customer Data Generation Complete![/bold green]")
    console.print(f"📊 Generated: {len(customer_data)} customers")
    console.print(f"📁 Files saved to: {output_dir}")
    console.print(f"🔗 Loan IDs range: {min(c['loan_id'] for c in customer_data)} - {max(c['loan_id'] for c in customer_data)}")
    console.print(f"👥 Customer IDs: 1001-{1000 + len(customer_data)}")
    
    console.print("\n📋 [bold blue]Next Steps:[/bold blue]")
    console.print("1. Upload JSON to AWS S3: aws s3 cp customer_data.json s3://fsi-demo-nfm/")
    console.print("2. Run Iceberg table creation in 02_ingestion_setup.sql")
    console.print("3. Insert data into CUSTOMER_TABLE using generated SQL")

@cli.command()
@click.argument('filename')
def preview(filename):
    """Preview customer data from JSON file"""
    
    with open(filename, 'r') as f:
        customers = [json.loads(line.strip()) for line in f if line.strip()]
    
    generator = CustomerGenerator()
    generator.display_sample(customers, 15)
    
    console.print(f"\n📊 Total customers in file: {len(customers)}")
    console.print(f"🔗 Unique loan IDs: {len(set(c['loan_id'] for c in customers))}")
    console.print(f"👥 Customer ID range: {min(c['customer_id'] for c in customers)}-{max(c['customer_id'] for c in customers)}")

if __name__ == '__main__':
    cli()
