# FlintDB

A lightweight, embedded Log-Structured Merge-tree (LSM-tree) based key-value storage engine written in Go.

## Overview

FlintDB is a persistent key-value database that implements the LSM-tree data structure, optimized for write-heavy workloads. It provides durability through a Write-Ahead Log (WAL) and uses SSTables (Sorted String Tables) for efficient disk storage.

## Features

- **LSM-Tree Architecture**: Fast writes with efficient compaction
- **Write-Ahead Log (WAL)**: Ensures durability and crash recovery
- **Red-Black Tree Memtable**: Efficient in-memory indexing
- **SSTable Storage**: Sorted on-disk data structures with index
- **Level-Based Compaction**: Automatic background compaction to optimize read performance
- **Tombstone Support**: Efficient key deletion
- **Concurrent Operations**: Thread-safe read/write operations
- **Crash Recovery**: Automatic replay from WAL on startup


### Components

- **Memtable**: In-memory Red-Black tree for fast writes
- **WAL**: Append-only log for durability
- **SSTables**: Immutable sorted files with key-value data
- **Manifest**: Tracks active SSTables and their metadata
- **Compaction**: Background process to merge and optimize SSTables

## Installation

```bash
git clone https://github.com/Ankit-1204/FlintDB.git
cd FlintDB
go mod download
```

## Usage

### Basic Example

```go
package main

import (
    "fmt"
    "github.com/Ankit-1204/FlintDB/internals"
)

func main() {
    // Open or create database
    db, err := internals.Open("mydb")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Write data
    err = db.Put("user:1", []byte("Alice"))
    if err != nil {
        panic(err)
    }

    // Read data
    value := db.Get("user:1")
    fmt.Println(string(value)) // Output: Alice

    // Delete data
    err = db.Delete("user:1")
    if err != nil {
        panic(err)
    }
}
```

### CLI Demo

Run the interactive CLI:

```bash
go run cmd/server.go
```

Example session:
```
DBname: 
testdb
give operation: 
P
give key and value: 
mykey myvalue
give operation: 
G
give key and value: 
mykey
myvalue
```

Operations:
- `P` - Put (write key-value pair)
- `G` - Get (read value by key)
- `D` - Delete (mark key as deleted)

## API Reference

### `Open(dbName string) (*Database, error)`
Opens or creates a database at the specified path.

### `Put(key string, value []byte) error`
Writes a key-value pair to the database.

### `Get(key string) []byte`
Retrieves the value for a given key. Returns `nil` if not found or deleted.

### `Delete(key string) error`
Marks a key as deleted (tombstone).

### `Close() error`
Closes the database and flushes pending writes.

## Project Structure

```
FlintDB/
├── cmd/
│   └── server.go           # CLI demo application
├── internals/
│   ├── server.go           # Main database engine
│   ├── memtable/
│   │   └── memtable.go     # Red-Black tree implementation
│   ├── wal/
│   │   └── wal.go          # Write-Ahead Log
│   ├── sstable/
│   │   └── sstable.go      # SSTable read/write
│   └── formats/
│       ├── formats.go      # Data structures
│       └── heap.go         # Min-heap for compaction
├── go.mod
└── README.md
```

## Configuration

Key parameters (currently hardcoded):
- **Memtable Size**: 32 MB (flushes to SSTable when exceeded)
- **WAL Rotation**: 32 MB per log file
- **Compaction Trigger**: 4+ SSTables per level

## Current Status

⚠️ **FlintDB is currently in active development**

### Known Issues
- `Get()` implementation needs enhancement for SSTable search
- Some error handling improvements needed
- Performance optimizations pending (bloom filters, caching)

See [fixes.md](fixes.md) and [critique.md](critique.md) for detailed technical analysis.

### What Works
✅ Write operations with WAL durability  
✅ Memtable with Red-Black tree  
✅ SSTable flush and persistence  
✅ Crash recovery via WAL replay  
✅ Tombstone-based deletion  
✅ Background compaction worker  

### In Progress
🔧 Full SSTable read path  
🔧 Bloom filters for faster lookups  
🔧 Comprehensive test suite  
🔧 Performance benchmarks  

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Write (Put/Delete) | O(log n) | RB-tree insertion + WAL append |
| Read (Get) | O(log n) + O(k) | Memtable + k SSTables |
| Flush | O(n log n) | In-order traversal |
| Compaction | O(n log k) | k-way merge |
<!-- ## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request -->

## Technical References

This project implements concepts from:
- [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) - O'Neil et al.
- [LevelDB](https://github.com/google/leveldb) - Google's LSM-tree implementation
- [RocksDB](https://rocksdb.org/) - Facebook's fork of LevelDB

## License

MIT License - see LICENSE file for details

## Acknowledgments

- Inspired by LevelDB, RocksDB, and other LSM-tree implementations
- Red-Black tree implementation based on classic algorithms

## Author

**Ankit** - [GitHub](https://github.com/Ankit-1204)

---

**Note**: FlintDB is an educational project to understand LSM-tree internals. For production use, consider mature alternatives like LevelDB, RocksDB, or BadgerDB.
