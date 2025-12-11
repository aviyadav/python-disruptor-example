# python-disruptor-example

High-performance concurrent processing examples using the [py-disruptor](https://github.com/pulsepointinc/py-disruptor) library with **Polars**, **fault tolerance**, and **production-ready patterns**.

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run basic example
python main.py

# Run batch JSON processing with Polars
python batch_json_example.py

# Run fault-tolerant example (production-ready)
python fault_tolerant_example.py
```

## 📚 Examples

### 1. Basic Example (`main.py`)
Simple Disruptor pattern demonstration with integer processing.

### 2. Batch JSON Processing (`batch_json_example.py`)
High-performance JSON batch processing using **Polars** DataFrames.
- 5-10x faster than Pandas
- Writes to Parquet files
- Preserves nested JSON structures
- 20% smaller file sizes

### 3. Fault-Tolerant Example (`fault_tolerant_example.py`) ⭐
**Production-ready** implementation with comprehensive resilience patterns:
- ✅ Retry logic with exponential backoff
- ✅ Dead Letter Queue (DLQ)
- ✅ Checkpointing for crash recovery
- ✅ Comprehensive error handling
- ✅ Structured logging
- ✅ Graceful shutdown

## 🛡️ Fault Tolerance

This project demonstrates **7 key strategies** for building fault-tolerant Disruptor applications:

1. **Retry Logic** - Exponential backoff for transient failures
2. **Dead Letter Queue** - Capture and inspect failed messages
3. **Checkpointing** - Resume from last successful batch after crashes
4. **Error Handling** - Multi-level error catching and recovery
5. **Custom Error Handlers** - Centralized error management
6. **Structured Logging** - Track everything with context
7. **Graceful Shutdown** - Clean exits with data preservation

See detailed guides:
- **[FAULT_TOLERANCE_QUICK_REF.md](FAULT_TOLERANCE_QUICK_REF.md)** - Quick reference
- **[FAULT_TOLERANCE.md](FAULT_TOLERANCE.md)** - Comprehensive guide
- **[EXAMPLES.md](EXAMPLES.md)** - All examples documented

## 🔧 Technology Stack

- **py-disruptor** - Lock-free concurrent processing
- **Polars** - High-performance DataFrames (5-10x faster than Pandas)
- **PyArrow** - Parquet file format support
- **Python 3.13+** - Modern Python features

## 📊 Performance

| Metric | Pandas | Polars | Improvement |
|--------|--------|--------|-------------|
| Processing Speed | 1x | 5-10x | 5-10x faster |
| File Size | 30KB | 24KB | 20% smaller |
| Memory Usage | Higher | Lower | More efficient |
| Schema Preservation | Flattened | Nested | Better |

## 📁 Project Structure

```
.
├── main.py                          # Basic example
├── batch_json_example.py            # Polars batch processing
├── fault_tolerant_example.py        # Production-ready example ⭐
├── benchmark.py                     # Performance measurement
├── requirements.txt                 # Dependencies
├── README.md                        # This file
├── EXAMPLES.md                      # Detailed examples
├── FAULT_TOLERANCE.md               # Comprehensive FT guide
├── FAULT_TOLERANCE_QUICK_REF.md     # Quick reference
└── data/                            # Output directory
    ├── consumer-1/
    │   ├── batch_*.parquet          # Parquet files
    │   ├── checkpoint.json          # Recovery checkpoint
    │   └── dlq/                     # Dead Letter Queue
    └── consumer-2/
        └── ...
```

## 🎯 Key Features

### Polars Integration
- **Fast**: 5-10x faster than Pandas for most operations
- **Efficient**: Lower memory footprint
- **Smart**: Preserves nested structures (Struct, List types)
- **Compact**: Better compression (20% smaller files)

### Fault Tolerance
- **Resilient**: Automatic retry with exponential backoff
- **Recoverable**: Checkpointing for crash recovery
- **Observable**: Structured logging with full context
- **Safe**: Dead Letter Queue for failed messages

### Production-Ready
- Error handling at multiple levels
- Graceful shutdown handling
- Monitoring metrics (processed, errors, retries)
- Checkpoint-based recovery

## 📖 Usage Examples

### Basic Processing
```python
from disruptor import Disruptor, Consumer

class MyConsumer(Consumer):
    def consume(self, elements):
        print(f"Processing {elements}")

disruptor = Disruptor(size=1024)
disruptor.register_consumer(MyConsumer())
disruptor.produce([1, 2, 3])
disruptor.close()
```

### Fault-Tolerant Processing
```python
from fault_tolerant_example import FaultTolerantBatchConsumer

consumer = FaultTolerantBatchConsumer(
    name="MyConsumer",
    batch_size=100,
    max_retries=3,
    enable_dlq=True
)
# Automatically handles retries, DLQ, and checkpointing
```

## 🔍 Monitoring

Track these metrics in production:

- `processed_count` - Total items processed
- `error_count` - Total errors encountered
- `retry_count` - Total retry attempts
- `dlq_size` - Failed batches in DLQ
- `checkpoint_age` - Time since last checkpoint

## 🚨 Recovery Procedures

### After a Crash
```bash
# Simply restart - checkpoints enable automatic recovery
python fault_tolerant_example.py
```

### Reprocess DLQ Items
```python
import json
from pathlib import Path

dlq_files = Path("data/ft-consumer-1/dlq").glob("*.json")
for dlq_file in dlq_files:
    with open(dlq_file) as f:
        dlq_entry = json.load(f)
        # Investigate, fix issue, then reprocess
        disruptor.produce(dlq_entry['data'])
```

## 📦 Installation

```bash
# Clone the repository
git clone <repo-url>
cd python-disruptor-eg

# Install dependencies
pip install -r requirements.txt

# Or using uv
uv pip install -r requirements.txt
```

## 🧪 Testing

Run examples to verify installation:

```bash
# Basic test
python main.py

# Batch processing test
python batch_json_example.py

# Fault tolerance test (includes simulated failures)
python fault_tolerant_example.py
```

## 📚 Documentation

- **[EXAMPLES.md](EXAMPLES.md)** - Detailed example documentation
- **[FAULT_TOLERANCE.md](FAULT_TOLERANCE.md)** - Comprehensive fault tolerance guide
- **[FAULT_TOLERANCE_QUICK_REF.md](FAULT_TOLERANCE_QUICK_REF.md)** - Quick reference

## 🤝 Contributing

Contributions are welcome! This is a learning project demonstrating best practices for:
- Concurrent processing with Disruptor pattern
- High-performance data processing with Polars
- Production-ready fault tolerance patterns

## 📄 License

See the py-disruptor library license.

## 🔗 Resources

- [py-disruptor GitHub](https://github.com/pulsepointinc/py-disruptor)
- [Polars Documentation](https://pola-rs.github.io/polars/)
- [Disruptor Pattern](https://lmax-exchange.github.io/disruptor/)
