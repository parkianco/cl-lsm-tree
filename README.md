# cl-lsm-tree

A pure Common Lisp implementation of a Log-Structured Merge-tree (LSM-tree) key-value storage engine.

## Features

- **Zero External Dependencies** - Uses only SBCL built-ins (sb-thread, sb-ext)
- **MemTable** - In-memory sorted table using skip list (O(log n) operations)
- **SSTable** - On-disk sorted string tables with block-based layout
- **Write-Ahead Log (WAL)** - Durability and crash recovery
- **Compaction** - Background merging with leveled/tiered/FIFO strategies
- **Bloom Filters** - Efficient negative lookups
- **Block Cache** - LRU cache for read optimization
- **Snapshots** - Point-in-time consistent reads
- **Range Scans** - Efficient key range iteration

## Installation

```bash
git clone https://github.com/yourusername/cl-lsm-tree.git
```

Load with ASDF:

```lisp
(asdf:load-system :cl-lsm-tree)
```

## Quick Start

```lisp
(use-package :cl-lsm-tree)

;; Basic usage with automatic lifecycle management
(with-engine (engine "/path/to/data/")
  ;; Write data
  (engine-put engine "hello" "world")
  (engine-put engine "foo" "bar")

  ;; Read data
  (multiple-value-bind (value found-p)
      (engine-get engine "hello")
    (when found-p
      (format t "Value: ~A~%" value)))

  ;; Delete data
  (engine-delete engine "foo")

  ;; Range scan
  (dolist (pair (engine-scan engine "a" "z"))
    (format t "~A -> ~A~%" (car pair) (cdr pair))))
```

## API Reference

### Engine Lifecycle

- `(make-lsm-engine directory &key cache-size)` - Create engine instance
- `(engine-open engine)` - Open engine, recover from WAL
- `(engine-close engine)` - Close engine, flush all data
- `(engine-destroy engine)` - Close and delete all data
- `(with-engine (var directory &rest options) &body body)` - Scoped engine usage

### Core Operations

- `(engine-put engine key value)` - Store key-value pair
- `(engine-get engine key)` - Retrieve value (returns value, found-p)
- `(engine-delete engine key)` - Delete key (tombstone)
- `(engine-contains-p engine key)` - Check if key exists
- `(engine-scan engine &optional start end limit)` - Range scan
- `(engine-scan-range engine start end)` - Scan [start, end)

### Batch Operations

```lisp
(let ((batch (make-write-batch)))
  (batch-put batch "key1" "value1")
  (batch-put batch "key2" "value2")
  (batch-delete batch "old-key")
  (batch-apply engine batch))
```

### Snapshots

```lisp
(let ((snap (create-snapshot engine)))
  ;; Read consistent view even as engine changes
  (snapshot-get snap "key")
  (snapshot-scan snap "a" "z")
  ;; Release when done
  (release-snapshot engine snap))
```

### Compaction

- `(trigger-compaction engine &optional level)` - Start background compaction
- `(await-compaction engine)` - Wait for compaction to complete
- `(set-compaction-strategy engine strategy)` - Set strategy (:leveled, :tiered, :fifo)

### Statistics

```lisp
(engine-statistics engine)
;; => (:writes 100 :reads 50 :flushes 2 :memtable-size 1024
;;     :memtable-count 10 :sstable-count 3 :cache-hit-rate 0.85
;;     :snapshot-count 1)
```

## Architecture

```
Write Path:
  Client -> WAL -> MemTable -> (flush) -> Level-0 SSTable

Read Path:
  Client -> MemTable -> Block Cache -> SSTables (L0..Ln)

Background:
  Compaction Manager -> Merge SSTables across levels
```

### File Format

**SSTable Layout:**
```
[Magic: 4 bytes]
[Data Blocks...]
[Index Block]
[Bloom Filter]
[Footer: 16 bytes]
```

**WAL Record Format:**
```
[Type: 1 byte (1=PUT, 2=DELETE)]
[Key Length: 2 bytes]
[Value Length: 4 bytes (PUT only)]
[Key bytes]
[Value bytes (PUT only)]
```

## Configuration

```lisp
;; Custom cache size (default: 256MB)
(make-lsm-engine "/data/" :cache-size (* 512 1024 1024))
```

Internal constants (modify source to change):
- `+block-size+` - 4KB block size
- `+memtable-size-threshold+` - 64MB memtable flush threshold
- `+bloom-bits-per-key+` - 10 bits per key in bloom filter
- `+max-levels+` - 7 SSTable levels

## Running Tests

```lisp
(asdf:test-system :cl-lsm-tree)
```

Or manually:

```lisp
(asdf:load-system :cl-lsm-tree/test)
(cl-lsm-tree/test:run-tests)
```

## Thread Safety

All public functions are thread-safe:
- MemTable uses mutex-protected skip list
- Block cache uses mutex for LRU operations
- Engine operations are mutex-protected

## Performance Characteristics

- **Writes**: O(log n) amortized, sequential I/O
- **Reads**: O(log n) with caching, may require disk seeks
- **Range scans**: O(k log n) where k is result size
- **Space amplification**: ~1.1x with leveled compaction

## License

MIT License - See LICENSE file

## Attribution

Extracted from [CLPIC](https://github.com/yourusername/clpic) - Pure Common Lisp P2P Intellectual Property Chain.
