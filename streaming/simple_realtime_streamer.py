#!/usr/bin/env python3
"""
Simplified Real-time Streaming Implementation for FSI Demo
Direct INSERT streaming to Snowflake without complex Snowpipe setup
"""

import os
import time
import signal
import sys
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from transaction_generator import TransactionGenerator
from rich.console import Console
from rich.live import Live
from rich.table import Table
import snowflake.connector

console = Console()

class SimpleRealtimeStreamer:
    """Simplified real-time transaction streaming using direct Snowflake INSERTs"""
    
    def __init__(self, rate: float = 1.0, anomaly_rate: float = 0.05):
        self.generator = TransactionGenerator()
        self.generator.anomaly_probability = anomaly_rate
        self.rate = rate  # transactions per second
        self.running = False
        self.stats = {
            'total_transactions': 0,
            'successful_sends': 0,
            'failed_sends': 0,
            'anomalies': 0,
            'start_time': None,
            'current_date': None
        }
        
        # Snowflake connection config
        self.sf_config = self._get_snowflake_config()
        self.sf_connection = None
        self._setup_connection()
        
        # Set up graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _get_snowflake_config(self) -> Dict[str, str]:
        """Get Snowflake connection configuration from CLI config"""
        try:
            # Read from Snowflake CLI connections.toml
            try:
                import toml
                config_path = os.path.expanduser("~/.snowflake/connections.toml")
                
                if os.path.exists(config_path):
                    with open(config_path, 'r') as f:
                        config = toml.load(f)
                    
                    # Use the default connection (nfm_demo_keypair)
                    conn_name = "nfm_demo_keypair"
                    if conn_name in config:
                        sf_config = config[conn_name]
                        return {
                            'account': sf_config.get('account', ''),
                            'user': sf_config.get('user', ''),
                            'database': sf_config.get('database', 'FSI_DEMO'),
                            'schema': sf_config.get('schema', 'RAW_DATA'),
                            'warehouse': sf_config.get('warehouse', 'INGESTION_WH_XS'),
                            'role': sf_config.get('role', 'data_engineer_role'),
                            'private_key_path': os.path.expanduser("~/.snowflake/keys/nfm_demo_key.pem")
                        }
            except ImportError:
                console.print("⚠️ [yellow]toml package not available, using environment variables[/yellow]")
            
            # Fallback to environment variables
            return {
                'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
                'user': os.getenv('SNOWFLAKE_USER', ''),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'FSI_DEMO'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAW_DATA'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'INGESTION_WH_XS'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'data_engineer_role'),
                'private_key_path': os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH', '')
            }
            
        except Exception as e:
            console.print(f"❌ [red]Error reading Snowflake config: {e}[/red]")
            return {}
    
    def _setup_connection(self):
        """Setup Snowflake connection"""
        try:
            console.print("📡 [yellow]Setting up Snowflake connection for streaming...[/yellow]")
            
            # Read private key
            private_key_path = self.sf_config.get('private_key_path', '')
            if not os.path.exists(private_key_path):
                console.print(f"❌ [red]Private key not found at: {private_key_path}[/red]")
                return
            
            with open(private_key_path, 'rb') as key_file:
                private_key = key_file.read()
            
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend
            
            p_key = serialization.load_pem_private_key(
                private_key,
                password=None,
                backend=default_backend()
            )
            
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            self.sf_connection = snowflake.connector.connect(
                account=self.sf_config['account'],
                user=self.sf_config['user'],
                private_key=pkb,
                database=self.sf_config['database'],
                schema=self.sf_config['schema'],
                warehouse=self.sf_config['warehouse'],
                role=self.sf_config['role']
            )
            
            console.print(f"✅ [green]Connected to {self.sf_config['database']}.{self.sf_config['schema']}.TRANSACTIONS_TABLE[/green]")
            
        except Exception as e:
            console.print(f"❌ [red]Failed to setup Snowflake connection: {e}[/red]")
            self.sf_connection = None
    
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        console.print("\n🛑 [yellow]Stopping streamer gracefully...[/yellow]")
        self.running = False
    
    def _create_status_table(self) -> Table:
        """Create status table for live display"""
        table = Table(title="🚀 Real-time Streaming - FSI Transaction Streamer", title_style="bold blue")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        if self.stats['start_time']:
            runtime = datetime.now() - self.stats['start_time']
            runtime_str = f"{runtime.total_seconds():.0f}s"
        else:
            runtime_str = "0s"
        
        success_rate = self.stats['successful_sends'] / max(1, self.stats['total_transactions']) * 100
        
        table.add_row("📅 Date", self.stats['current_date'] or "Not started")
        table.add_row("⚡ Rate", f"{self.rate} TPS")
        table.add_row("🔥 Anomaly Rate", f"{self.generator.anomaly_probability:.1%}")
        table.add_row("📊 Total Transactions", f"{self.stats['total_transactions']:,}")
        table.add_row("✅ Successful Sends", f"{self.stats['successful_sends']:,}")
        table.add_row("❌ Failed Sends", f"{self.stats['failed_sends']:,}")
        table.add_row("📈 Success Rate", f"{success_rate:.1f}%")
        table.add_row("💥 Anomalies", f"{self.stats['anomalies']} ({self.stats['anomalies']/max(1,self.stats['total_transactions'])*100:.1f}%)")
        table.add_row("⏱️  Runtime", runtime_str)
        table.add_row("👥 Customers", "1001-6000 (5000 customers)")
        
        return table
    
    def _send_to_snowflake(self, transaction: dict) -> bool:
        """Send transaction to Snowflake using direct INSERT"""
        if not self.sf_connection:
            return False
        
        try:
            # Add data_source to transaction for consistency with historical data
            enhanced_transaction = transaction.copy()
            enhanced_transaction['data_source'] = 'STREAMING'
            
            # Direct INSERT for real-time streaming
            cursor = self.sf_connection.cursor()
            insert_query = """
            INSERT INTO TRANSACTIONS_TABLE (
                transaction_id, customer_id, transaction_date,
                transaction_amount, transaction_type, data_source
            ) VALUES (%(transaction_id)s, %(customer_id)s, %(transaction_date)s,
                     %(transaction_amount)s, %(transaction_type)s, %(data_source)s)
            """
            
            cursor.execute(insert_query, enhanced_transaction)
            cursor.close()
            
            return True
            
        except Exception as e:
            console.print(f"❌ [red]Failed to send transaction: {e}[/red]")
            return False
    
    def stream_transactions(self, duration_seconds: Optional[int] = None):
        """Stream transactions in real-time"""
        if not self.sf_connection:
            console.print("❌ [red]No Snowflake connection available. Cannot stream.[/red]")
            return
        
        self.running = True
        self.stats['start_time'] = datetime.now()
        self.stats['current_date'] = datetime.now().strftime("%Y-%m-%d")
        
        console.print("🚀 [bold green]Starting Real-time Streaming[/bold green]")
        console.print(f"📊 Target rate: {self.rate} transactions/second")
        console.print(f"🔥 Anomaly rate: {self.generator.anomaly_probability:.1%}")
        console.print(f"📅 Date: {self.stats['current_date']}")
        console.print(f"🗄️  Database: {self.sf_config['database']}.{self.sf_config['schema']}.TRANSACTIONS_TABLE")
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
                
                # Send to Snowflake via direct INSERT
                success = self._send_to_snowflake(transaction)
                
                self.stats['total_transactions'] += 1
                if success:
                    self.stats['successful_sends'] += 1
                else:
                    self.stats['failed_sends'] += 1
                
                if transaction.get('is_anomaly', False):
                    self.stats['anomalies'] += 1
                
                # Update live display
                live.update(self._create_status_table())
                
                # Wait for next transaction
                time.sleep(interval)
        
        # Close connection
        if self.sf_connection:
            self.sf_connection.close()
        
        # Final summary
        runtime = datetime.now() - self.stats['start_time']
        console.print(f"\n✅ [bold green]Real-time Streaming completed![/bold green]")
        console.print(f"📊 Streamed {self.stats['successful_sends']}/{self.stats['total_transactions']} transactions in {runtime.total_seconds():.1f}s")
        console.print(f"💥 Anomalies: {self.stats['anomalies']} ({self.stats['anomalies']/max(1,self.stats['total_transactions'])*100:.1f}%)")
        console.print(f"📈 Success rate: {self.stats['successful_sends']/max(1,self.stats['total_transactions'])*100:.1f}%")
    
    def cleanup_streaming_data(self, date_filter: Optional[str] = None):
        """Clean up streaming data from Snowflake"""
        try:
            # Create fresh connection for cleanup
            self._setup_connection()
            
            if not self.sf_connection:
                console.print("❌ [red]No Snowflake connection available.[/red]")
                return
            
            cursor = self.sf_connection.cursor()
            
            if date_filter:
                query = """
                DELETE FROM TRANSACTIONS_TABLE 
                WHERE data_source = 'STREAMING' 
                  AND DATE(ingestion_timestamp) = %s
                """
                cursor.execute(query, (date_filter,))
                console.print(f"🧹 [bold red]Cleaned up streaming data for {date_filter}[/bold red]")
            else:
                # Clean up today's data by default
                query = """
                DELETE FROM TRANSACTIONS_TABLE 
                WHERE data_source = 'STREAMING' 
                  AND DATE(ingestion_timestamp) = CURRENT_DATE()
                """
                cursor.execute(query)
                console.print(f"🧹 [bold red]Cleaned up today's streaming data[/bold red]")
            
            rows_deleted = cursor.rowcount
            cursor.close()
            self.sf_connection.close()
            
            console.print(f"✅ Deleted {rows_deleted} streaming records")
            
        except Exception as e:
            console.print(f"❌ [red]Error during cleanup: {e}[/red]")

def main():
    """Main entry point for real-time streaming"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Simple real-time streaming for FSI demo')
    parser.add_argument('--rate', type=float, default=2.0, help='Transactions per second (default: 2.0)')
    parser.add_argument('--anomaly-rate', type=float, default=0.05, help='Anomaly probability (default: 0.05)')
    parser.add_argument('--duration', type=int, help='Duration in seconds (default: infinite)')
    parser.add_argument('--cleanup', action='store_true', help='Clean up today\'s streaming data and exit')
    parser.add_argument('--cleanup-date', type=str, help='Clean up streaming data for specific date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    streamer = SimpleRealtimeStreamer(rate=args.rate, anomaly_rate=args.anomaly_rate)
    
    if args.cleanup or args.cleanup_date:
        streamer.cleanup_streaming_data(args.cleanup_date)
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
