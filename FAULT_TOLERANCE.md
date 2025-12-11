# Fault Tolerance Guide for py-disruptor

This guide explains how to make your Disruptor-based applications fault-tolerant and resilient.

## Fault Tolerance Strategies

### 1. **Error Handling in Consumers**

Wrap your consumer logic in try-catch blocks to handle errors gracefully:

```python
class FaultTolerantConsumer(Consumer):
    def consume(self, elements):
        try:
            # Process elements
            self._process(elements)
        except Exception as e:
            logger.error(f"Error processing: {e}")
            # Handle error (retry, log, send to DLQ, etc.)
```

### 2. **Retry Logic with Exponential Backoff**

Implement retry logic for transient failures:

```python
def _process_with_retry(self, batch, retry_count=0):
    try:
        self._process_batch(batch)
    except Exception as e:
        if retry_count < self.max_retries:
            delay = self.retry_delay * (2 ** retry_count)  # Exponential backoff
            time.sleep(delay)
            self._process_with_retry(batch, retry_count + 1)
        else:
            # Max retries exceeded - send to DLQ
            self._send_to_dlq(batch, str(e))
```

**Benefits:**
- Handles transient failures (network issues, temporary unavailability)
- Exponential backoff prevents overwhelming downstream systems
- Configurable retry attempts

### 3. **Dead Letter Queue (DLQ)**

Store failed messages for later inspection and reprocessing:

```python
def _send_to_dlq(self, batch, error_message):
    dlq_entry = {
        "timestamp": datetime.now().isoformat(),
        "error": error_message,
        "data": batch
    }
    with open(dlq_filepath, 'w') as f:
        json.dump(dlq_entry, f)
```

**Benefits:**
- Prevents data loss
- Allows manual inspection of failures
- Enables batch reprocessing

### 4. **Checkpointing**

Save progress periodically to enable recovery:

```python
def _save_checkpoint(self):
    checkpoint = {
        'last_batch_number': self.file_counter,
        'processed_count': self.processed_count,
        'timestamp': datetime.now().isoformat()
    }
    with open(self.checkpoint_file, 'w') as f:
        json.dump(checkpoint, f)

def _load_checkpoint(self):
    if self.checkpoint_file.exists():
        checkpoint = json.load(open(self.checkpoint_file))
        self.file_counter = checkpoint['last_batch_number']
```

**Benefits:**
- Resume from last successful batch after crashes
- Prevents reprocessing of already-handled data
- Enables exactly-once processing semantics

### 5. **Disruptor Error Handler**

Register a custom error handler with the Disruptor:

```python
def custom_error_handler(consumer, input_data, error):
    logger.error(f"Error in {consumer.name}: {error}")
    # Send alert, log to monitoring system, etc.

disruptor = Disruptor(
    name='Example',
    size=1024,
    consumer_error_handler=custom_error_handler
)
```

**Benefits:**
- Centralized error handling
- Prevents consumer crashes from affecting the Disruptor
- Enables monitoring and alerting

### 6. **Structured Logging**

Use proper logging for observability:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("Processing batch...")
logger.error("Error occurred", exc_info=True)
```

**Benefits:**
- Track processing progress
- Debug issues in production
- Monitor error rates

### 7. **Graceful Shutdown**

Handle interruptions properly:

```python
try:
    # Main processing loop
    disruptor.produce([data])
except KeyboardInterrupt:
    logger.warning("Shutting down gracefully...")
finally:
    disruptor.close()  # Ensure cleanup
```

**Benefits:**
- Process remaining items before shutdown
- Save checkpoints
- Close file handles properly

## Complete Fault-Tolerant Implementation

See `fault_tolerant_example.py` for a complete implementation with:

- ✅ Retry logic with exponential backoff
- ✅ Dead Letter Queue (DLQ)
- ✅ Checkpointing for recovery
- ✅ Structured logging
- ✅ Error handling at multiple levels
- ✅ Graceful shutdown
- ✅ Custom error handlers

## Running the Example

```bash
source .venv/bin/activate && python fault_tolerant_example.py
```

## Monitoring and Observability

Track these metrics for production systems:

1. **Processing Metrics:**
   - Total items processed
   - Processing rate (items/second)
   - Batch processing time

2. **Error Metrics:**
   - Error count
   - Retry count
   - DLQ size

3. **Health Metrics:**
   - Consumer lag
   - Ring buffer utilization
   - Memory usage

## Best Practices

1. **Set appropriate retry limits** - Too many retries can cause delays
2. **Monitor DLQ size** - Growing DLQ indicates systemic issues
3. **Implement circuit breakers** - Prevent cascading failures
4. **Use idempotent operations** - Safe to retry without side effects
5. **Save checkpoints frequently** - Balance between performance and recovery time
6. **Log with context** - Include consumer name, batch ID, timestamps
7. **Test failure scenarios** - Simulate errors to verify fault tolerance
8. **Set up alerts** - Monitor error rates and DLQ growth

## Recovery Procedures

### Recovering from Crashes

1. Restart the application
2. Consumers will load checkpoints automatically
3. Processing resumes from last successful batch

### Reprocessing DLQ Items

```python
# Read DLQ files
dlq_files = Path("data/consumer-1/dlq").glob("*.json")

for dlq_file in dlq_files:
    with open(dlq_file) as f:
        dlq_entry = json.load(f)
        batch = dlq_entry['data']
        # Reprocess or investigate
```

### Manual Checkpoint Reset

```python
# Reset checkpoint to reprocess from beginning
checkpoint = {
    'last_batch_number': 0,
    'processed_count': 0,
    'timestamp': datetime.now().isoformat()
}
with open('data/consumer-1/checkpoint.json', 'w') as f:
    json.dump(checkpoint, f)
```

## Advanced Patterns

### Circuit Breaker

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
            raise
```

### Rate Limiting

```python
class RateLimiter:
    def __init__(self, max_calls, time_window):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
    
    def allow(self):
        now = time.time()
        self.calls = [t for t in self.calls if now - t < self.time_window]
        
        if len(self.calls) < self.max_calls:
            self.calls.append(now)
            return True
        return False
```

## Conclusion

Fault tolerance is critical for production systems. The strategies outlined here will help you build resilient Disruptor-based applications that can handle failures gracefully and recover automatically.
