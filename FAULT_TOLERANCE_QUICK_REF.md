# Making py-disruptor Fault Tolerant - Quick Reference

## 7 Key Strategies for Fault Tolerance

### 1. ✅ **Retry Logic with Exponential Backoff**
Automatically retry failed operations with increasing delays.

```python
def _process_with_retry(self, batch, retry_count=0):
    try:
        self._process_batch(batch)
    except Exception as e:
        if retry_count < self.max_retries:
            delay = self.retry_delay * (2 ** retry_count)  # 0.5s, 1s, 2s, 4s...
            time.sleep(delay)
            self._process_with_retry(batch, retry_count + 1)
        else:
            self._send_to_dlq(batch, str(e))  # Give up after max retries
```

**When to use:** Transient failures (network timeouts, temporary service unavailability)

---

### 2. 💀 **Dead Letter Queue (DLQ)**
Store failed messages for later inspection and reprocessing.

```python
def _send_to_dlq(self, batch, error_message):
    dlq_entry = {
        "timestamp": datetime.now().isoformat(),
        "error": error_message,
        "data": batch
    }
    dlq_filepath = self.dlq_dir / f"dlq_{int(time.time())}.json"
    with open(dlq_filepath, 'w') as f:
        json.dump(dlq_entry, f, indent=2)
```

**When to use:** Permanent failures, data validation errors, max retries exceeded

---

### 3. 📍 **Checkpointing**
Save progress to resume after crashes.

```python
def _save_checkpoint(self):
    checkpoint = {
        'last_batch_number': self.file_counter,
        'processed_count': self.processed_count,
        'timestamp': datetime.now().isoformat()
    }
    with open(self.checkpoint_file, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def _load_checkpoint(self):
    if self.checkpoint_file.exists():
        checkpoint = json.load(open(self.checkpoint_file))
        self.file_counter = checkpoint['last_batch_number']
        self.processed_count = checkpoint['processed_count']
```

**When to use:** Long-running processes, exactly-once processing requirements

---

### 4. 🛡️ **Error Handling in Consumers**
Wrap processing logic in try-catch blocks.

```python
def consume(self, elements):
    try:
        self.batch_buffer.extend(elements)
        while len(self.batch_buffer) >= self.batch_size:
            batch = self.batch_buffer[:self.batch_size]
            self.batch_buffer = self.batch_buffer[self.batch_size:]
            self._process_batch_with_retry(batch)
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        self.error_count += 1
```

**When to use:** Always! Prevents consumer crashes from affecting the Disruptor

---

### 5. 🎯 **Custom Disruptor Error Handler**
Centralized error handling for all consumers.

```python
def custom_error_handler(consumer, input_data, error):
    logger.error(f"Error in {consumer.name}: {error}")
    # Send alerts, log to monitoring, etc.

disruptor = Disruptor(
    name='Example',
    size=1024,
    consumer_error_handler=custom_error_handler
)
```

**When to use:** Monitoring, alerting, centralized error tracking

---

### 6. 📊 **Structured Logging**
Track processing progress and errors.

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info(f"✓ Batch {batch_num} processed: {count} items")
logger.error(f"Error processing batch: {e}", exc_info=True)
logger.warning(f"Batch sent to DLQ: {filename}")
```

**When to use:** Always! Essential for debugging and monitoring

---

### 7. 🚪 **Graceful Shutdown**
Handle interruptions properly.

```python
try:
    for i in range(total_objects):
        disruptor.produce([data])
except KeyboardInterrupt:
    logger.warning("Shutting down gracefully...")
finally:
    disruptor.close()  # Process remaining items, save checkpoints
```

**When to use:** Always! Prevents data loss during shutdown

---

## Complete Example

See `fault_tolerant_example.py` for a production-ready implementation with all 7 strategies.

### Features:
- ✅ Retry logic (3 attempts with exponential backoff)
- ✅ Dead Letter Queue for failed batches
- ✅ Checkpointing (auto-resume after crashes)
- ✅ Comprehensive error handling
- ✅ Structured logging with timestamps
- ✅ Graceful shutdown
- ✅ Custom error handlers

### Run it:
```bash
source .venv/bin/activate && python fault_tolerant_example.py
```

### Output:
```
2025-12-11 19:49:54 - INFO - FT-Consumer-1: Initialized. Writing to data/ft-consumer-1
2025-12-11 19:49:54 - INFO - FT-Consumer-1: Max retries: 3, DLQ enabled: True
2025-12-11 19:49:54 - INFO - FT-Consumer-1: ✓ Batch 1 of 100 items | Amount: $253221.50 | Total: 100 | Errors: 0
2025-12-11 19:49:54 - ERROR - FT-Consumer-1: Error processing batch (attempt 1/3): Simulated processing error
2025-12-11 19:49:54 - INFO - FT-Consumer-1: Retrying in 0.50 seconds...
2025-12-11 19:49:55 - INFO - FT-Consumer-1: ✓ Batch 5 of 100 items | Amount: $248195.38 | Total: 500 | Errors: 0
```

---

## Directory Structure

```
data/
├── ft-consumer-1/
│   ├── batch_0001.parquet
│   ├── batch_0002.parquet
│   ├── ...
│   ├── checkpoint.json          ← Recovery checkpoint
│   └── dlq/                     ← Dead Letter Queue
│       └── dlq_1234567890.json  ← Failed batches
└── ft-consumer-2/
    ├── batch_0001.parquet
    ├── ...
    ├── checkpoint.json
    └── dlq/
```

---

## Monitoring Metrics

Track these in production:

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `processed_count` | Total items processed | - |
| `error_count` | Total errors encountered | > 1% of processed |
| `retry_count` | Total retry attempts | > 5% of batches |
| `dlq_size` | Number of files in DLQ | > 0 (investigate) |
| `processing_rate` | Items per second | < expected rate |
| `checkpoint_age` | Time since last checkpoint | > 5 minutes |

---

## Recovery Procedures

### After a Crash:
1. Restart the application
2. Consumers automatically load checkpoints
3. Processing resumes from last successful batch

### Reprocess DLQ Items:
```python
dlq_files = Path("data/ft-consumer-1/dlq").glob("*.json")
for dlq_file in dlq_files:
    with open(dlq_file) as f:
        dlq_entry = json.load(f)
        # Investigate error, fix issue, then reprocess
        disruptor.produce(dlq_entry['data'])
```

### Reset Checkpoint (start from beginning):
```python
checkpoint = {
    'last_batch_number': 0,
    'processed_count': 0,
    'timestamp': datetime.now().isoformat()
}
with open('data/ft-consumer-1/checkpoint.json', 'w') as f:
    json.dump(checkpoint, f)
```

---

## Best Practices

1. ✅ **Set appropriate retry limits** - Balance between resilience and latency
2. ✅ **Monitor DLQ growth** - Growing DLQ = systemic problem
3. ✅ **Save checkpoints frequently** - After each batch or every N items
4. ✅ **Use idempotent operations** - Safe to retry without side effects
5. ✅ **Log with context** - Include consumer name, batch ID, timestamps
6. ✅ **Test failure scenarios** - Simulate errors to verify fault tolerance
7. ✅ **Set up alerts** - Monitor error rates and DLQ size

---

## Advanced Patterns

### Circuit Breaker
Prevent cascading failures by temporarily stopping requests to failing services.

### Rate Limiting
Control processing rate to avoid overwhelming downstream systems.

### Bulkhead Pattern
Isolate failures by using separate thread pools for different operations.

See `FAULT_TOLERANCE.md` for detailed implementations.

---

## Summary

Making your Disruptor fault-tolerant requires:
1. **Retry logic** - Handle transient failures
2. **DLQ** - Capture permanent failures
3. **Checkpointing** - Enable recovery
4. **Error handling** - Prevent crashes
5. **Logging** - Track everything
6. **Graceful shutdown** - Clean exits
7. **Monitoring** - Know what's happening

The `fault_tolerant_example.py` demonstrates all these patterns in a production-ready implementation.
