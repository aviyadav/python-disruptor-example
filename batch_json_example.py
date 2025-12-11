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


class BatchJsonConsumer(Consumer):
    """Consumer that processes JSON objects in batches and writes to Parquet files."""
    
    def __init__(self, name: str, batch_size: int = 10, output_dir: str = "data"):
        self.name = name
        self.batch_size = batch_size
        self.processed_count = 0
        self.batch_buffer = []
        self.file_counter = 0
        
        # Create output directory if it doesn't exist
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create consumer-specific subdirectory
        self.consumer_dir = self.output_dir / self.name.replace(" ", "_").lower()
        self.consumer_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"{self.name}: Initialized. Writing to {self.consumer_dir}")
    
    def consume(self, elements):
        """Consume elements and process them in batches."""
        self.batch_buffer.extend(elements)
        
        # Process complete batches
        while len(self.batch_buffer) >= self.batch_size:
            batch = self.batch_buffer[:self.batch_size]
            self.batch_buffer = self.batch_buffer[self.batch_size:]
            self._process_batch(batch)
    
    def _process_batch(self, batch):
        """Process a batch of JSON objects and write to Parquet file."""
        # Simulate some processing time
        time.sleep(random.uniform(0.01, 0.05))
        
        # Convert batch to Polars DataFrame
        df = pl.from_dicts(batch)
        
        # Flatten nested structures for better Parquet compatibility
        df = df.unnest('user').unnest('transaction').unnest('metadata').unnest('analytics')
        
        # Calculate statistics
        total_amount = df.select(pl.col('amount').sum()).item() if 'amount' in df.columns else 0
        
        # Write to Parquet file
        self.file_counter += 1
        filename = f"batch_{self.file_counter:04d}.parquet"
        filepath = self.consumer_dir / filename
        
        df.write_parquet(filepath, compression='snappy')
        
        self.processed_count += len(batch)
        
        print(f"{self.name}: Processed batch {self.file_counter} of {len(batch)} items | "
              f"Total amount: ${total_amount:.2f} | "
              f"Total processed: {self.processed_count} | "
              f"Written to: {filename}")
    
    def close(self):
        """Handle cleanup and process any remaining items."""
        if self.batch_buffer:
            print(f"{self.name}: Processing final {len(self.batch_buffer)} items")
            self._process_batch(self.batch_buffer)
            self.batch_buffer = []
        
        print(f"{self.name}: Finished. Total items processed: {self.processed_count}")
        print(f"{self.name}: Total Parquet files written: {self.file_counter} in {self.consumer_dir}")


@measure_performance
def main():
    """Main function to demonstrate batch JSON processing with Disruptor."""
    
    # Create consumers with batch size of 10
    consumer_one = BatchJsonConsumer(name="Consumer-1", batch_size=100)
    consumer_two = BatchJsonConsumer(name="Consumer-2", batch_size=100)
    
    # Create disruptor with larger buffer
    disruptor = Disruptor(name='BatchJsonExample', size=2048)
    
    try:
        # Register consumers
        disruptor.register_consumer(consumer_one)
        disruptor.register_consumer(consumer_two)
        
        print("=" * 80)
        print("Starting to produce 100 complex JSON objects...")
        print("Consumers will write batches to Parquet files in data/ directory")
        print("=" * 80)
        
        # Produce 1000 JSON objects
        for i in range(10_000):
            json_obj = generate_complex_json()
            disruptor.produce([json_obj])
            
            if (i + 1) % 100 == 0:
                print(f"Produced {i + 1}/1000 objects...")
        
        print("\n" + "=" * 80)
        print("All objects produced. Waiting for consumers to finish...")
        print("=" * 80 + "\n")
        
        # Wait for consumers to process all items
        time.sleep(5)
        
    finally:
        # Clean up
        disruptor.close()
        print("\n" + "=" * 80)
        print("Disruptor closed successfully")
        print("=" * 80)


if __name__ == "__main__":
    main()
