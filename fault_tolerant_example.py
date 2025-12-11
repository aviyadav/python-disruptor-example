from disruptor import Disruptor, Consumer
import time
import random
import json
from datetime import datetime, timedelta
from pathlib import Path
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from benchmark import measure_performance
import logging
from typing import Optional, Callable
import traceback


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_complex_json():
    """Generate a complex random JSON object."""
    return {
        "id": random.randint(1000, 9999),
        "timestamp": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
        "user": {
            "user_id": f"user_{random.randint(1, 10000)}",
            "name": random.choice(["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]),
            "email": f"user{random.randint(1, 10000)}@example.com",
            "age": random.randint(18, 80),
            "premium": random.choice([True, False]),
            "preferences": {
                "theme": random.choice(["dark", "light", "auto"]),
                "language": random.choice(["en", "es", "fr", "de", "ja", "zh"]),
                "notifications": random.choice([True, False])
            }
        },
        "transaction": {
            "amount": round(random.uniform(10.0, 5000.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "JPY", "CNY"]),
            "status": random.choice(["pending", "completed", "failed", "refunded"]),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "crypto", "bank_transfer"]),
            "items": [
                {
                    "product_id": f"prod_{random.randint(100, 999)}",
                    "name": random.choice(["Widget", "Gadget", "Tool", "Device", "Accessory"]),
                    "quantity": random.randint(1, 10),
                    "price": round(random.uniform(5.0, 500.0), 2)
                }
                for _ in range(random.randint(1, 5))
            ]
        },
        "metadata": {
            "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "user_agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Mozilla/5.0 (X11; Linux x86_64)",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)"
            ]),
            "session_id": f"sess_{random.randint(100000, 999999)}",
            "referrer": random.choice(["google.com", "facebook.com", "twitter.com", "direct", "email"]),
            "device_type": random.choice(["desktop", "mobile", "tablet"])
        },
        "analytics": {
            "page_views": random.randint(1, 100),
            "time_on_site": random.randint(10, 3600),
            "bounce_rate": round(random.uniform(0, 1), 2),
            "conversion": random.choice([True, False]),
            "tags": random.sample(["electronics", "fashion", "home", "sports", "books", "toys", "food"], k=random.randint(1, 4))
        }
    }


class FaultTolerantBatchConsumer(Consumer):
    """
    Fault-tolerant consumer with error handling, retry logic, and dead letter queue.
    """
    
    def __init__(
        self, 
        name: str, 
        batch_size: int = 10, 
        output_dir: str = "data",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        enable_dlq: bool = True
    ):
        self.name = name
        self.batch_size = batch_size
        self.processed_count = 0
        self.error_count = 0
        self.retry_count = 0
        self.batch_buffer = []
        self.file_counter = 0
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.enable_dlq = enable_dlq
        
        # Create output directory structure
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Consumer-specific directories
        self.consumer_dir = self.output_dir / self.name.replace(" ", "_").lower()
        self.consumer_dir.mkdir(parents=True, exist_ok=True)
        
        # Dead Letter Queue directory for failed messages
        if self.enable_dlq:
            self.dlq_dir = self.consumer_dir / "dlq"
            self.dlq_dir.mkdir(parents=True, exist_ok=True)
        
        # Checkpoint file for recovery
        self.checkpoint_file = self.consumer_dir / "checkpoint.json"
        self._load_checkpoint()
        
        logger.info(f"{self.name}: Initialized. Writing to {self.consumer_dir}")
        logger.info(f"{self.name}: Max retries: {self.max_retries}, DLQ enabled: {self.enable_dlq}")
    
    def _load_checkpoint(self):
        """Load checkpoint to resume from last successful batch."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.file_counter = checkpoint.get('last_batch_number', 0)
                    self.processed_count = checkpoint.get('processed_count', 0)
                    logger.info(f"{self.name}: Resumed from checkpoint - batch {self.file_counter}, processed {self.processed_count}")
            except Exception as e:
                logger.warning(f"{self.name}: Failed to load checkpoint: {e}")
    
    def _save_checkpoint(self):
        """Save checkpoint after successful batch processing."""
        try:
            checkpoint = {
                'last_batch_number': self.file_counter,
                'processed_count': self.processed_count,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
        except Exception as e:
            logger.error(f"{self.name}: Failed to save checkpoint: {e}")
    
    def consume(self, elements):
        """Consume elements with error handling."""
        try:
            self.batch_buffer.extend(elements)
            
            # Process complete batches
            while len(self.batch_buffer) >= self.batch_size:
                batch = self.batch_buffer[:self.batch_size]
                self.batch_buffer = self.batch_buffer[self.batch_size:]
                self._process_batch_with_retry(batch)
        
        except Exception as e:
            logger.error(f"{self.name}: Critical error in consume: {e}")
            logger.error(traceback.format_exc())
            self.error_count += 1
    
    def _process_batch_with_retry(self, batch, retry_count: int = 0):
        """Process batch with retry logic."""
        try:
            self._process_batch(batch)
            
        except Exception as e:
            logger.error(f"{self.name}: Error processing batch (attempt {retry_count + 1}/{self.max_retries}): {e}")
            
            if retry_count < self.max_retries:
                # Retry with exponential backoff
                delay = self.retry_delay * (2 ** retry_count)
                logger.info(f"{self.name}: Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
                self.retry_count += 1
                self._process_batch_with_retry(batch, retry_count + 1)
            else:
                # Max retries exceeded - send to DLQ
                logger.error(f"{self.name}: Max retries exceeded. Sending batch to DLQ.")
                self.error_count += 1
                if self.enable_dlq:
                    self._send_to_dlq(batch, str(e))
    
    def _process_batch(self, batch):
        """Process a batch of JSON objects and write to Parquet file."""
        # Simulate some processing time
        time.sleep(random.uniform(0.01, 0.05))
        
        # Simulate random failures for demonstration (5% failure rate)
        if random.random() < 0.05:
            raise Exception("Simulated processing error")
        
        # Convert batch to Polars DataFrame with error handling
        try:
            df = pl.from_dicts(batch)
            df = df.unnest('user').unnest('transaction').unnest('metadata').unnest('analytics')
        except Exception as e:
            logger.error(f"{self.name}: Error creating DataFrame: {e}")
            raise
        
        # Calculate statistics
        try:
            total_amount = df.select(pl.col('amount').sum()).item() if 'amount' in df.columns else 0
        except Exception as e:
            logger.warning(f"{self.name}: Error calculating statistics: {e}")
            total_amount = 0
        
        # Write to Parquet file with error handling
        self.file_counter += 1
        filename = f"batch_{self.file_counter:04d}.parquet"
        filepath = self.consumer_dir / filename
        
        try:
            df.write_parquet(filepath, compression='snappy')
        except Exception as e:
            logger.error(f"{self.name}: Error writing Parquet file: {e}")
            self.file_counter -= 1  # Rollback counter
            raise
        
        self.processed_count += len(batch)
        
        # Save checkpoint after successful processing
        self._save_checkpoint()
        
        logger.info(f"{self.name}: ✓ Batch {self.file_counter} of {len(batch)} items | "
                   f"Amount: ${total_amount:.2f} | "
                   f"Total: {self.processed_count} | "
                   f"Errors: {self.error_count} | "
                   f"File: {filename}")
    
    def _send_to_dlq(self, batch, error_message: str):
        """Send failed batch to Dead Letter Queue."""
        try:
            dlq_filename = f"dlq_batch_{int(time.time())}_{random.randint(1000, 9999)}.json"
            dlq_filepath = self.dlq_dir / dlq_filename
            
            dlq_entry = {
                "timestamp": datetime.now().isoformat(),
                "error": error_message,
                "batch_size": len(batch),
                "data": batch
            }
            
            with open(dlq_filepath, 'w') as f:
                json.dump(dlq_entry, f, indent=2)
            
            logger.warning(f"{self.name}: Batch sent to DLQ: {dlq_filename}")
        
        except Exception as e:
            logger.error(f"{self.name}: Failed to write to DLQ: {e}")
    
    def close(self):
        """Handle cleanup and process any remaining items."""
        try:
            if self.batch_buffer:
                logger.info(f"{self.name}: Processing final {len(self.batch_buffer)} items")
                self._process_batch_with_retry(self.batch_buffer)
                self.batch_buffer = []
            
            logger.info(f"{self.name}: ✓ Finished. Total processed: {self.processed_count}")
            logger.info(f"{self.name}: Total errors: {self.error_count}, Total retries: {self.retry_count}")
            logger.info(f"{self.name}: Parquet files: {self.file_counter} in {self.consumer_dir}")
            
            if self.enable_dlq:
                dlq_files = list(self.dlq_dir.glob("*.json"))
                if dlq_files:
                    logger.warning(f"{self.name}: DLQ contains {len(dlq_files)} failed batches")
        
        except Exception as e:
            logger.error(f"{self.name}: Error during cleanup: {e}")


def custom_error_handler(consumer, input_data, error):
    """Custom error handler for the Disruptor."""
    logger.error(f"Disruptor error handler called for {consumer.name}")
    logger.error(f"Error: {error}")
    logger.error(f"Input data: {input_data[:100]}...")  # Log first 100 chars


@measure_performance
def main():
    """Main function demonstrating fault-tolerant batch processing."""
    
    # Create fault-tolerant consumers
    consumer_one = FaultTolerantBatchConsumer(
        name="FT-Consumer-1", 
        batch_size=100,
        max_retries=3,
        retry_delay=0.5,
        enable_dlq=True
    )
    consumer_two = FaultTolerantBatchConsumer(
        name="FT-Consumer-2", 
        batch_size=100,
        max_retries=3,
        retry_delay=0.5,
        enable_dlq=True
    )
    
    # Create disruptor with error handler
    disruptor = Disruptor(
        name='FaultTolerantExample', 
        size=2048,
        consumer_error_handler=custom_error_handler
    )
    
    try:
        # Register consumers
        disruptor.register_consumer(consumer_one)
        disruptor.register_consumer(consumer_two)
        
        logger.info("=" * 80)
        logger.info("Starting fault-tolerant batch processing...")
        logger.info("Features: Retry logic, DLQ, checkpointing, error handling")
        logger.info("=" * 80)
        
        # Produce JSON objects
        total_objects = 1000
        for i in range(total_objects):
            json_obj = generate_complex_json()
            disruptor.produce([json_obj])
            
            if (i + 1) % 100 == 0:
                logger.info(f"Produced {i + 1}/{total_objects} objects...")
        
        logger.info("\n" + "=" * 80)
        logger.info("All objects produced. Waiting for consumers to finish...")
        logger.info("=" * 80 + "\n")
        
        # Wait for consumers to process all items
        time.sleep(5)
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Shutting down gracefully...")
    
    except Exception as e:
        logger.error(f"Critical error in main: {e}")
        logger.error(traceback.format_exc())
    
    finally:
        # Clean up
        disruptor.close()
        logger.info("\n" + "=" * 80)
        logger.info("Disruptor closed successfully")
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
