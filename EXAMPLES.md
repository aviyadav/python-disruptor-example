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
A more complex example demonstrating batch processing of JSON objects.

**Features:**
- Generates 1000 complex, random JSON objects with:
  - User information (ID, name, email, preferences)
  - Transaction data (amount, currency, payment method, items)
  - Metadata (IP, user agent, session info)
  - Analytics (page views, time on site, tags)
- Two consumers processing JSON objects in batches of 10
- Batch aggregation and statistics (total amounts, user tracking)
- Proper cleanup handling for remaining items
- Performance measurement

**Run:**
```bash
source .venv/bin/activate && python batch_json_example.py
```

**Output Example:**
```
Consumer-1: Processed batch of 10 items | Total amount: $25397.66 | Total processed: 990
Consumer-2: Processed batch of 10 items | Total amount: $20955.77 | Total processed: 1000
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

Both examples use the `@measure_performance` decorator from `benchmark.py` to track:
- Execution time
- Memory usage

## Notes

- The Disruptor pattern provides lock-free concurrent processing
- Multiple consumers can process the same elements in parallel
- Ring buffer size should be a power of 2 for optimal performance
- Always call `disruptor.close()` for proper cleanup
