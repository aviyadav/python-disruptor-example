# py-disruptor Examples

This repository contains examples of using the py-disruptor library for high-performance concurrent processing.

## Examples

### 1. Basic Example (`main.py`)
A simple demonstration of the Disruptor pattern with multiple consumers processing integer values.

**Features:**
- Two consumers processing elements concurrently
- Performance measurement using the `@measure_performance` decorator
- Configurable ring buffer size
- Produces 10,000 elements

**Run:**
```bash
source .venv/bin/activate && python main.py
```

### 2. Batch JSON Processing (`batch_json_example.py`)
A more complex example demonstrating batch processing of JSON objects using **Polars** for high performance.

**Features:**
- Generates complex, random JSON objects with:
  - User information (ID, name, email, preferences)
  - Transaction data (amount, currency, payment method, items)
  - Metadata (IP, user agent, session info)
  - Analytics (page views, time on site, tags)
- Two consumers processing JSON objects in batches of 100
- **Polars DataFrames** for 5-10x faster processing than Pandas
- Batch aggregation and statistics (total amounts, user tracking)
- Writes to Parquet files with Snappy compression
- Preserves nested structures (Struct and List types)
- 20% smaller file sizes vs Pandas (24KB vs 30KB)
- Performance measurement

**Run:**
```bash
source .venv/bin/activate && python batch_json_example.py
```

**Output Example:**
```
Consumer-1: Processed batch 1 of 100 items | Total amount: $267457.43 | Total processed: 100 | Written to: batch_0001.parquet
```

### 3. Fault-Tolerant Example (`fault_tolerant_example.py`) ⭐
A production-ready example with comprehensive fault tolerance and resilience patterns.

**Features:**
- ✅ **Retry logic** with exponential backoff (3 attempts: 0.5s, 1s, 2s)
- ✅ **Dead Letter Queue (DLQ)** for failed batches
- ✅ **Checkpointing** for crash recovery and exactly-once processing
- ✅ **Comprehensive error handling** at multiple levels
- ✅ **Structured logging** with timestamps and context
- ✅ **Graceful shutdown** handling
- ✅ **Custom error handlers** for the Disruptor
- ✅ **Simulated failures** to demonstrate resilience (5% failure rate)

**Run:**
```bash
source .venv/bin/activate && python fault_tolerant_example.py
```

**Output Example:**
```
2025-12-11 19:49:54 - INFO - FT-Consumer-1: ✓ Batch 1 of 100 items | Amount: $253221.50 | Total: 100 | Errors: 0
2025-12-11 19:49:54 - ERROR - FT-Consumer-1: Error processing batch (attempt 1/3): Simulated processing error
2025-12-11 19:49:54 - INFO - FT-Consumer-1: Retrying in 0.50 seconds...
2025-12-11 19:49:55 - INFO - FT-Consumer-1: ✓ Batch 5 of 100 items | Amount: $248195.38 | Total: 500 | Errors: 0
```

**Directory Structure:**
```
data/
├── ft-consumer-1/
│   ├── batch_0001.parquet
│   ├── checkpoint.json      ← Recovery checkpoint
│   └── dlq/                 ← Dead Letter Queue
│       └── dlq_*.json       ← Failed batches
```

## Key Concepts

### Disruptor API
- **Initialize:** `Disruptor(name='Example', size=1024)`
- **Register consumers:** `disruptor.register_consumer(consumer)`
- **Produce elements:** `disruptor.produce([element])`
- **Cleanup:** `disruptor.close()`

### Consumer Interface
Consumers must implement the `consume(self, elements)` method:
```python
class MyConsumer(Consumer):
    def consume(self, elements):
        # Process elements
        pass
    
    def close(self):
        # Optional cleanup
        pass
```

## Performance Measurement

All examples use the `@measure_performance` decorator from `benchmark.py` to track:
- Execution time
- Memory usage

## Technology Stack

### Polars vs Pandas
The batch examples use **Polars** instead of Pandas for:
- ⚡ **5-10x faster** processing
- 💾 **Lower memory usage**
- 🎯 **Better type inference** (preserves nested structures)
- 📦 **Smaller file sizes** (20% reduction with better compression)

## Fault Tolerance

See the comprehensive guides:
- **`FAULT_TOLERANCE_QUICK_REF.md`** - Quick reference with code snippets
- **`FAULT_TOLERANCE.md`** - Detailed guide with advanced patterns

### 7 Key Strategies:
1. Retry logic with exponential backoff
2. Dead Letter Queue (DLQ)
3. Checkpointing for recovery
4. Error handling in consumers
5. Custom Disruptor error handlers
6. Structured logging
7. Graceful shutdown

## Notes

- The Disruptor pattern provides lock-free concurrent processing
- Multiple consumers can process the same elements in parallel
- Ring buffer size should be a power of 2 for optimal performance
- Always call `disruptor.close()` for proper cleanup
- Use checkpointing for long-running processes
- Monitor DLQ size in production
- Polars preserves nested JSON structures better than Pandas

