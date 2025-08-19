#!/usr/bin/env python3
"""
FSI Demo - Snowpipe Streaming CLI
High-throughput transaction streaming using Snowpipe file-based ingestion
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
    """FSI Demo Snowpipe Transaction Streamer (Production Scale)"""
    pass

@cli.command()
@click.option('--rate', default=10.0, help='Transactions per second (default: 10.0)')
@click.option('--batch-size', default=100, help='Transactions per batch file (default: 100)')
@click.option('--anomaly-rate', default=0.05, help='Probability of anomaly transactions')
@click.option('--duration', type=int, help='Duration in seconds (default: run until stopped)')
def start(rate, batch_size, anomaly_rate, duration):
    """Start Snowpipe file-based streaming"""
    try:
        from snowpipe_streamer import SnowpipeFileStreamer
        
        console.print(f"🌊 [bold blue]Starting Snowpipe Streaming[/bold blue]")
        console.print(f"📊 Rate: {rate} TPS | 📦 Batch: {batch_size} | 🔥 Anomalies: {anomaly_rate:.1%}")
        
        streamer = SnowpipeFileStreamer(rate=rate, batch_size=batch_size, anomaly_rate=anomaly_rate)
        streamer.stream_transactions(duration_seconds=duration)
        
    except ImportError as e:
        console.print(f"❌ [red]Module import error: {e}[/red]")
        console.print("💡 Make sure you're in the project root directory")
        console.print("💡 Run: snow sql --filename fsi_demo/sql/02_B_ingestion_setup_snowpipestreaming.sql")
    except Exception as e:
        console.print(f"❌ [red]Error: {e}[/red]")

@cli.command()
@click.option('--days', default=365, help='Days of historical data to generate')
@click.option('--output', help='Output file name (default: auto-generated)')
def historical(days, output):
    """Generate historical transaction data (same as regular stream_demo.py)"""
    try:
        from historical_generator import generate_historical_data
        
        generate_historical_data(days=days, output_file=output)
        
    except ImportError as e:
        console.print(f"❌ [red]Module import error: {e}[/red]")
        console.print("💡 Make sure you're in the project root directory")
    except Exception as e:
        console.print(f"❌ [red]Error: {e}[/red]")

@cli.command()
@click.option('--date', default=None, help='Specific date to cleanup (YYYY-MM-DD), default: today')
def cleanup(date):
    """Clean up Snowpipe streaming transaction data"""
    try:
        from snowpipe_streamer import SnowpipeFileStreamer
        
        streamer = SnowpipeFileStreamer()
        streamer.cleanup_streaming_data(date)
        
    except ImportError as e:
        console.print(f"❌ [red]Module import error: {e}[/red]")
        console.print("💡 Make sure you're in the project root directory")
    except Exception as e:
        console.print(f"❌ [red]Error: {e}[/red]")

@cli.command()
def test():
    """Test transaction generation (same as regular stream_demo.py)"""
    try:
        from transaction_generator import TransactionGenerator
        from datetime import datetime
        
        console.print("🧪 [bold cyan]Testing Transaction Generator (Snowpipe Mode)[/bold cyan]")
        
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
        console.print(f"👥 Customer range: 1001-1100")
        console.print(f"🏷️  Transaction types: {len(generator.transaction_types)}")
        console.print(f"🌊 Ready for Snowpipe streaming!")
        
    except ImportError as e:
        console.print(f"❌ [red]Module import error: {e}[/red]")
    except Exception as e:
        console.print(f"❌ [red]Error: {e}[/red]")

@cli.command()
def setup():
    """Show setup instructions for Snowpipe streaming"""
    console.print("🛠️ [bold blue]Snowpipe Streaming Setup Instructions[/bold blue]")
    console.print("\n📋 Prerequisites:")
    console.print("   1. Business Critical+ Snowflake edition (for true streaming)")
    console.print("   2. Additional infrastructure setup required")
    console.print("\n🚀 Setup Steps:")
    console.print("   1. Run infrastructure setup:")
    console.print("      [cyan]snow sql --connection nfm_demo_keypair --filename fsi_demo/sql/02_B_ingestion_setup_snowpipestreaming.sql[/cyan]")
    console.print("\n   2. Verify streaming warehouse:")
    console.print("      [cyan]snow sql --query \"SHOW WAREHOUSES LIKE 'STREAMING_WH_%'\"[/cyan]")
    console.print("\n   3. Test Snowpipe streaming:")
    console.print("      [cyan]python stream_demo_snowpipe.py start --rate 10 --duration 30[/cyan]")
    console.print("\n📊 Performance Characteristics:")
    console.print("   • Target: 10-100+ TPS")
    console.print("   • Latency: 2-10 seconds")
    console.print("   • Batch size: 100 transactions per file")
    console.print("   • Cost: Higher than direct INSERT")
    console.print("\n💡 Use this for production-scale streaming requirements!")

@cli.command()
def compare():
    """Compare streaming approaches"""
    console.print("⚖️ [bold blue]Streaming Approaches Comparison[/bold blue]")
    
    console.print("\n🔹 [bold green]Direct INSERT (stream_demo.py)[/bold green]")
    console.print("   • Throughput: 1-5 TPS")
    console.print("   • Latency: <1 second")
    console.print("   • Setup: Simple")
    console.print("   • Cost: $2-5/month")
    console.print("   • Best for: Demos, development")
    
    console.print("\n🔹 [bold cyan]Snowpipe Streaming (stream_demo_snowpipe.py)[/bold cyan]")
    console.print("   • Throughput: 10-1000+ TPS") 
    console.print("   • Latency: 2-10 seconds")
    console.print("   • Setup: Complex (requires infrastructure)")
    console.print("   • Cost: $25-100/month")
    console.print("   • Best for: Production, high volume")
    
    console.print("\n🎯 [bold yellow]Recommendation:[/bold yellow]")
    console.print("   • Use [green]stream_demo.py[/green] for demos and development")
    console.print("   • Use [cyan]stream_demo_snowpipe.py[/cyan] for production scale")

if __name__ == '__main__':
    cli()
