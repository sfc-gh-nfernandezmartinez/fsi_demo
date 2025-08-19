#!/usr/bin/env python3
"""
Real-time Transaction Streaming Generator
Streams live transaction data to Snowflake for FSI demo
"""

import time
import json
import signal
import sys
from datetime import datetime
from typing import Optional
from transaction_generator import TransactionGenerator
from rich.console import Console
from rich.live import Live
from rich.table import Table

console = Console()

class TransactionStreamer:
    """Real-time transaction streaming to Snowflake"""
    
    def __init__(self, rate: float = 1.0, anomaly_rate: float = 0.05):
        self.generator = TransactionGenerator()
        self.generator.anomaly_probability = anomaly_rate
        self.rate = rate  # transactions per second
        self.running = False
        self.stats = {
            'total_transactions': 0,
            'anomalies': 0,
            'start_time': None,
            'current_date': None
        }
        
        # Set up graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        console.print("\n🛑 [yellow]Stopping streamer gracefully...[/yellow]")
        self.running = False
    
    def _create_status_table(self) -> Table:
        """Create status table for live display"""
        table = Table(title="🏦 FSI Transaction Streamer", title_style="bold blue")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        if self.stats['start_time']:
            runtime = datetime.now() - self.stats['start_time']
            runtime_str = f"{runtime.total_seconds():.0f}s"
        else:
            runtime_str = "0s"
        
        table.add_row("📅 Date", self.stats['current_date'] or "Not started")
        table.add_row("⚡ Rate", f"{self.rate} TPS")
        table.add_row("🔥 Anomaly Rate", f"{self.generator.anomaly_probability:.1%}")
        table.add_row("📊 Total Transactions", f"{self.stats['total_transactions']:,}")
        table.add_row("💥 Anomalies", f"{self.stats['anomalies']} ({self.stats['anomalies']/max(1,self.stats['total_transactions'])*100:.1f}%)")
        table.add_row("⏱️  Runtime", runtime_str)
        table.add_row("👥 Customers", "1001-1100 (100 customers)")
        
        return table
    
    def _send_to_snowflake(self, transaction: dict) -> bool:
        """Send transaction to Snowflake (placeholder for now)"""
        # TODO: Implement Snowpipe streaming or COPY command
        # For now, just simulate the send
        time.sleep(0.001)  # Simulate network latency
        return True
    
    def stream_transactions(self, duration_seconds: Optional[int] = None):
        """Stream transactions in real-time"""
        self.running = True
        self.stats['start_time'] = datetime.now()
        self.stats['current_date'] = datetime.now().strftime("%Y-%m-%d")
        
        console.print("🚀 [bold green]Starting FSI Transaction Streamer[/bold green]")
        console.print(f"📊 Target rate: {self.rate} transactions/second")
        console.print(f"🔥 Anomaly rate: {self.generator.anomaly_probability:.1%}")
        console.print(f"📅 Date: {self.stats['current_date']}")
        console.print("\nPress Ctrl+C to stop...\n")
        
        interval = 1.0 / self.rate if self.rate > 0 else 1.0
        end_time = None
        if duration_seconds:
            end_time = time.time() + duration_seconds
        
        with Live(self._create_status_table(), refresh_per_second=2) as live:
            while self.running:
                if end_time and time.time() >= end_time:
                    break
                
                # Generate transaction for current timestamp
                current_time = datetime.now()
                transaction = self.generator.generate_transaction(current_time)
                
                # Send to Snowflake (placeholder)
                success = self._send_to_snowflake(transaction)
                
                if success:
                    self.stats['total_transactions'] += 1
                    if transaction.get('is_anomaly', False):
                        self.stats['anomalies'] += 1
                    
                    # Update live display
                    live.update(self._create_status_table())
                
                # Wait for next transaction
                time.sleep(interval)
        
        # Final summary
        runtime = datetime.now() - self.stats['start_time']
        console.print(f"\n✅ [bold green]Streaming completed![/bold green]")
        console.print(f"📊 Streamed {self.stats['total_transactions']} transactions in {runtime.total_seconds():.1f}s")
        console.print(f"💥 Anomalies: {self.stats['anomalies']} ({self.stats['anomalies']/max(1,self.stats['total_transactions'])*100:.1f}%)")
    
    def cleanup_today_data(self):
        """Clean up today's transaction data from Snowflake"""
        current_date = datetime.now().strftime("%Y-%m-%d")
        console.print(f"🧹 [bold red]Cleaning up transactions for {current_date}[/bold red]")
        
        # TODO: Implement actual cleanup
        # query = f"DELETE FROM FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE WHERE transaction_date = '{current_date}'"
        
        console.print("✅ Cleanup complete!")

def main():
    """Main entry point for real-time streaming"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Real-time transaction streaming for FSI demo')
    parser.add_argument('--rate', type=float, default=1.0, help='Transactions per second (default: 1.0)')
    parser.add_argument('--anomaly-rate', type=float, default=0.05, help='Anomaly probability (default: 0.05)')
    parser.add_argument('--duration', type=int, help='Duration in seconds (default: infinite)')
    parser.add_argument('--cleanup', action='store_true', help='Clean up today\'s data and exit')
    
    args = parser.parse_args()
    
    streamer = TransactionStreamer(rate=args.rate, anomaly_rate=args.anomaly_rate)
    
    if args.cleanup:
        streamer.cleanup_today_data()
        return
    
    try:
        streamer.stream_transactions(duration_seconds=args.duration)
    except KeyboardInterrupt:
        console.print("\n👋 [yellow]Streamer stopped by user[/yellow]")
    except Exception as e:
        console.print(f"\n❌ [red]Error: {e}[/red]")
        sys.exit(1)

if __name__ == "__main__":
    main()
