#!/usr/bin/env python3
"""
FSI Demo - Transaction Streaming CLI

Simple command-line tool for generating realistic leisure/lifestyle transactions
for Snowflake FSI demonstration purposes.
"""

import os
import sys
import click
from rich.console import Console

# Add streaming module to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'fsi_demo', 'streaming'))

console = Console()

@click.group()
def cli():
    """FSI Demo Transaction Streamer"""
    pass

@cli.command()
@click.option('--rate', default=1.0, help='Transactions per second')
@click.option('--anomaly-rate', default=0.05, help='Probability of anomaly transactions')
@click.option('--duration', type=int, help='Duration in seconds (default: run until stopped)')
def start(rate, anomaly_rate, duration):
    """Start streaming transactions to Snowflake"""
    try:
        from simple_realtime_streamer import SimpleRealtimeStreamer
        
        streamer = SimpleRealtimeStreamer(rate=rate, anomaly_rate=anomaly_rate)
        streamer.stream_transactions(duration_seconds=duration)
        
    except ImportError as e:
        console.print(f"❌ [red]Module import error: {e}[/red]")
        console.print("💡 Make sure you're in the project root directory")
    except Exception as e:
        console.print(f"❌ [red]Error: {e}[/red]")

@cli.command()
@click.option('--days', default=365, help='Days of historical data to generate')
@click.option('--output', help='Output file name (default: auto-generated)')
def historical(days, output):
    """Generate historical transaction data"""
    try:
        from historical_generator import generate_historical_data
        
        generate_historical_data(days=days, output_file=output)
        
    except ImportError as e:
        console.print(f"❌ [red]Module import error: {e}[/red]")
        console.print("💡 Make sure you're in the project root directory")
    except Exception as e:
        console.print(f"❌ [red]Error: {e}[/red]")

@cli.command()
def cleanup():
    """Clean up today's transaction data"""
    try:
        from simple_realtime_streamer import SimpleRealtimeStreamer
        
        streamer = SimpleRealtimeStreamer()
        streamer.cleanup_streaming_data()
        
    except ImportError as e:
        console.print(f"❌ [red]Module import error: {e}[/red]")
        console.print("💡 Make sure you're in the project root directory")
    except Exception as e:
        console.print(f"❌ [red]Error: {e}[/red]")

@cli.command()
def test():
    """Test transaction generation (generates sample data without streaming)"""
    try:
        from transaction_generator import TransactionGenerator
        from datetime import datetime
        
        console.print("🧪 [bold cyan]Testing Transaction Generator[/bold cyan]")
        
        generator = TransactionGenerator()
        
        # Generate a few sample transactions
        console.print("\n📊 Sample Transactions:")
        for i in range(5):
            transaction = generator.generate_transaction(datetime.now())
            anomaly_flag = "🔥" if transaction.get('is_anomaly', False) else ""
            
            console.print(f"   {i+1}. Customer {transaction['customer_id']} | "
                        f"${transaction['transaction_amount']:,.2f} | "
                        f"{transaction['transaction_type']} {anomaly_flag}")
        
        console.print(f"\n✅ Transaction generator working correctly!")
        console.print(f"👥 Customer range: 1001-6000")
        console.print(f"🏷️  Transaction types: {len(generator.transaction_types)}")
        
    except ImportError as e:
        console.print(f"❌ [red]Module import error: {e}[/red]")
    except Exception as e:
        console.print(f"❌ [red]Error: {e}[/red]")

if __name__ == '__main__':
    cli()
