#!/usr/bin/env python3
"""
Snowpipe Streaming Implementation for FSI Demo
Production-scale streaming using Snowpipe file-based ingestion with auto-retry
"""

import os
import time
import json
import signal
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path
from transaction_generator import TransactionGenerator
from rich.console import Console
from rich.live import Live
from rich.table import Table
import snowflake.connector

console = Console()

class SnowpipeFileStreamer:
    """Production-scale streaming using Snowpipe file-based ingestion"""
    
    def __init__(self, rate: float = 10.0, batch_size: int = 100, anomaly_rate: float = 0.05):
        self.generator = TransactionGenerator()
        self.generator.anomaly_probability = anomaly_rate
        self.rate = rate  # transactions per second
        self.batch_size = batch_size  # transactions per file
        self.running = False
        self.stats = {
            'total_transactions': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'files_uploaded': 0,
            'anomalies': 0,
            'start_time': None,
            'current_date': None
        }
        
        # Snowflake connection config
        self.sf_config = self._get_snowflake_config()
        self.sf_connection = None
        self.temp_dir = Path("temp_streaming_files")
        self.temp_dir.mkdir(exist_ok=True)
        
        self._setup_connection()
        
        # Set up graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _get_snowflake_config(self) -> Dict[str, str]:
        """Get Snowflake connection configuration from CLI config"""
        try:
            import toml
            config_path = os.path.expanduser("~/.snowflake/connections.toml")
            
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = toml.load(f)
                
                conn_name = "nfm_demo_keypair"
                if conn_name in config:
                    sf_config = config[conn_name]
                    return {
                        'account': sf_config.get('account', ''),
                        'user': sf_config.get('user', ''),
                        'database': sf_config.get('database', 'FSI_DEMO'),
                        'schema': sf_config.get('schema', 'RAW_DATA'),
                        'warehouse': sf_config.get('warehouse', 'STREAMING_WH_XS'),
                        'role': sf_config.get('role', 'data_engineer_role'),
                        'private_key_path': os.path.expanduser("~/.snowflake/keys/nfm_demo_key.pem")
                    }
            
            # Fallback to environment variables
            return {
                'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
                'user': os.getenv('SNOWFLAKE_USER', ''),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'FSI_DEMO'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAW_DATA'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'STREAMING_WH_XS'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'data_engineer_role'),
                'private_key_path': os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH', '')
            }
            
        except Exception as e:
            console.print(f"❌ [red]Error reading Snowflake config: {e}[/red]")
            return {}
    
    def _setup_connection(self):
        """Setup Snowflake connection for file uploads"""
        try:
            console.print("📡 [yellow]Setting up Snowflake connection for Snowpipe streaming...[/yellow]")
            
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
            
            console.print(f"✅ [green]Connected to Snowpipe streaming ({self.sf_config['warehouse']})[/green]")
            
        except Exception as e:
            console.print(f"❌ [red]Failed to setup Snowflake connection: {e}[/red]")
            self.sf_connection = None
    
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        console.print("\n🛑 [yellow]Stopping Snowpipe streamer gracefully...[/yellow]")
        self.running = False
    
    def _create_status_table(self) -> Table:
        """Create status table for live display"""
        table = Table(title="🌊 Snowpipe Streaming - FSI Transaction Streamer", title_style="bold blue")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        if self.stats['start_time']:
            runtime = datetime.now() - self.stats['start_time']
            runtime_str = f"{runtime.total_seconds():.0f}s"
        else:
            runtime_str = "0s"
        
        success_rate = self.stats['successful_batches'] / max(1, self.stats['successful_batches'] + self.stats['failed_batches']) * 100
        effective_tps = self.stats['total_transactions'] / max(1, (datetime.now() - self.stats['start_time']).total_seconds()) if self.stats['start_time'] else 0
        
        table.add_row("📅 Date", self.stats['current_date'] or "Not started")
        table.add_row("⚡ Target Rate", f"{self.rate} TPS")
        table.add_row("📊 Effective Rate", f"{effective_tps:.1f} TPS")
        table.add_row("🔥 Anomaly Rate", f"{self.generator.anomaly_probability:.1%}")
        table.add_row("📈 Total Transactions", f"{self.stats['total_transactions']:,}")
        table.add_row("📦 Batch Size", f"{self.batch_size}")
        table.add_row("✅ Successful Batches", f"{self.stats['successful_batches']:,}")
        table.add_row("❌ Failed Batches", f"{self.stats['failed_batches']:,}")
        table.add_row("📁 Files Uploaded", f"{self.stats['files_uploaded']:,}")
        table.add_row("📈 Success Rate", f"{success_rate:.1f}%")
        table.add_row("💥 Anomalies", f"{self.stats['anomalies']} ({self.stats['anomalies']/max(1,self.stats['total_transactions'])*100:.1f}%)")
        table.add_row("⏱️  Runtime", runtime_str)
        table.add_row("👥 Customers", "1001-1100 (100 customers)")
        
        return table
    
    def _create_batch_file(self, transactions: list) -> str:
        """Create JSON file with batch of transactions"""
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        filename = f"streaming_transactions_{batch_id}.json"
        filepath = self.temp_dir / filename
        
        # Add batch metadata to each transaction
        enhanced_transactions = []
        streaming_timestamp = datetime.now(timezone.utc).isoformat()
        
        for transaction in transactions:
            enhanced_transaction = transaction.copy()
            enhanced_transaction['data_source'] = 'STREAMING'
            enhanced_transaction['batch_id'] = batch_id
            enhanced_transaction['streaming_timestamp'] = streaming_timestamp
            # Remove debug field
            enhanced_transaction.pop('is_anomaly', None)
            enhanced_transactions.append(enhanced_transaction)
        
        # Write JSON lines format
        with open(filepath, 'w') as f:
            for transaction in enhanced_transactions:
                f.write(json.dumps(transaction) + '\n')
        
        return str(filepath)
    
    def _upload_and_ingest(self, filepath: str) -> bool:
        """Upload file to Snowflake stage and trigger ingestion"""
        if not self.sf_connection:
            return False
        
        try:
            cursor = self.sf_connection.cursor()
            filename = Path(filepath).name
            
            # Upload file to streaming stage
            put_query = f"PUT file://{filepath} @streaming_files_stage"
            cursor.execute(put_query)
            
            # The Snowpipe will auto-ingest from the stage (if auto-ingest is enabled)
            # For manual testing, we can also trigger a manual COPY
            copy_query = f"""
            COPY INTO TRANSACTIONS_TABLE (
                transaction_id, customer_id, transaction_date,
                transaction_amount, transaction_type, data_source,
                batch_id, streaming_timestamp
            ) FROM (
                SELECT 
                    $1:transaction_id::NUMBER,
                    $1:customer_id::NUMBER,
                    $1:transaction_date::DATE,
                    $1:transaction_amount::NUMBER(10,2),
                    $1:transaction_type::VARCHAR,
                    $1:data_source::VARCHAR,
                    $1:batch_id::VARCHAR,
                    $1:streaming_timestamp::TIMESTAMP_LTZ
                FROM @streaming_files_stage/{filename}.gz
            ) FILE_FORMAT = (TYPE = 'JSON')
            """
            
            cursor.execute(copy_query)
            rows_loaded = cursor.rowcount
            cursor.close()
            
            # Clean up local file
            os.remove(filepath)
            
            console.print(f"✅ [green]Uploaded batch: {filename} ({rows_loaded} rows)[/green]")
            return True
            
        except Exception as e:
            console.print(f"❌ [red]Failed to upload batch: {e}[/red]")
            return False
    
    def stream_transactions(self, duration_seconds: Optional[int] = None):
        """Stream transactions using Snowpipe file-based ingestion"""
        if not self.sf_connection:
            console.print("❌ [red]No Snowflake connection available. Cannot stream.[/red]")
            return
        
        self.running = True
        self.stats['start_time'] = datetime.now()
        self.stats['current_date'] = datetime.now().strftime("%Y-%m-%d")
        
        console.print("🌊 [bold green]Starting Snowpipe File-based Streaming[/bold green]")
        console.print(f"📊 Target rate: {self.rate} TPS")
        console.print(f"📦 Batch size: {self.batch_size} transactions per file")
        console.print(f"🔥 Anomaly rate: {self.generator.anomaly_probability:.1%}")
        console.print(f"📅 Date: {self.stats['current_date']}")
        console.print(f"🗄️  Database: {self.sf_config['database']}.{self.sf_config['schema']}.TRANSACTIONS_TABLE")
        console.print("\nPress Ctrl+C to stop...\n")
        
        batch_transactions = []
        transaction_interval = 1.0 / self.rate if self.rate > 0 else 1.0
        end_time = None
        if duration_seconds:
            end_time = time.time() + duration_seconds
        
        with Live(self._create_status_table(), refresh_per_second=2) as live:
            while self.running:
                if end_time and time.time() >= end_time:
                    break
                
                # Generate transaction
                current_time = datetime.now()
                transaction = self.generator.generate_transaction(current_time)
                batch_transactions.append(transaction)
                
                self.stats['total_transactions'] += 1
                if transaction.get('is_anomaly', False):
                    self.stats['anomalies'] += 1
                
                # When batch is full, upload to Snowflake
                if len(batch_transactions) >= self.batch_size:
                    filepath = self._create_batch_file(batch_transactions)
                    success = self._upload_and_ingest(filepath)
                    
                    if success:
                        self.stats['successful_batches'] += 1
                        self.stats['files_uploaded'] += 1
                    else:
                        self.stats['failed_batches'] += 1
                    
                    batch_transactions = []  # Reset batch
                
                # Update live display
                live.update(self._create_status_table())
                
                # Wait for next transaction
                time.sleep(transaction_interval)
            
            # Upload remaining transactions in final batch
            if batch_transactions:
                filepath = self._create_batch_file(batch_transactions)
                success = self._upload_and_ingest(filepath)
                if success:
                    self.stats['successful_batches'] += 1
                    self.stats['files_uploaded'] += 1
                else:
                    self.stats['failed_batches'] += 1
        
        # Close connection
        if self.sf_connection:
            self.sf_connection.close()
        
        # Clean up temp directory
        try:
            self.temp_dir.rmdir()
        except:
            pass  # Directory not empty or other issues
        
        # Final summary
        runtime = datetime.now() - self.stats['start_time']
        effective_tps = self.stats['total_transactions'] / runtime.total_seconds()
        
        console.print(f"\n✅ [bold green]Snowpipe Streaming completed![/bold green]")
        console.print(f"📊 Streamed {self.stats['total_transactions']} transactions in {runtime.total_seconds():.1f}s")
        console.print(f"⚡ Effective rate: {effective_tps:.1f} TPS")
        console.print(f"📦 Batches: {self.stats['successful_batches']} successful, {self.stats['failed_batches']} failed")
        console.print(f"💥 Anomalies: {self.stats['anomalies']} ({self.stats['anomalies']/max(1,self.stats['total_transactions'])*100:.1f}%)")
    
    def cleanup_streaming_data(self, date_filter: Optional[str] = None):
        """Clean up streaming data from Snowflake"""
        try:
            self._setup_connection()
            
            if not self.sf_connection:
                console.print("❌ [red]No Snowflake connection available.[/red]")
                return
            
            cursor = self.sf_connection.cursor()
            
            if date_filter:
                query = """
                DELETE FROM TRANSACTIONS_TABLE 
                WHERE data_source = 'STREAMING' 
                  AND DATE(streaming_timestamp) = %s
                """
                cursor.execute(query, (date_filter,))
                console.print(f"🧹 [bold red]Cleaned up Snowpipe streaming data for {date_filter}[/bold red]")
            else:
                query = """
                DELETE FROM TRANSACTIONS_TABLE 
                WHERE data_source = 'STREAMING' 
                  AND DATE(COALESCE(streaming_timestamp, ingestion_timestamp)) = CURRENT_DATE()
                """
                cursor.execute(query)
                console.print(f"🧹 [bold red]Cleaned up today's Snowpipe streaming data[/bold red]")
            
            rows_deleted = cursor.rowcount
            cursor.close()
            self.sf_connection.close()
            
            console.print(f"✅ Deleted {rows_deleted} Snowpipe streaming records")
            
        except Exception as e:
            console.print(f"❌ [red]Error during cleanup: {e}[/red]")

def main():
    """Main entry point for Snowpipe streaming"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Snowpipe file-based streaming for FSI demo')
    parser.add_argument('--rate', type=float, default=10.0, help='Transactions per second (default: 10.0)')
    parser.add_argument('--batch-size', type=int, default=100, help='Transactions per batch file (default: 100)')
    parser.add_argument('--anomaly-rate', type=float, default=0.05, help='Anomaly probability (default: 0.05)')
    parser.add_argument('--duration', type=int, help='Duration in seconds (default: infinite)')
    parser.add_argument('--cleanup', action='store_true', help='Clean up today\'s streaming data and exit')
    parser.add_argument('--cleanup-date', type=str, help='Clean up streaming data for specific date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    streamer = SnowpipeFileStreamer(
        rate=args.rate, 
        batch_size=args.batch_size, 
        anomaly_rate=args.anomaly_rate
    )
    
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