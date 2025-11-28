# FSDB Python Bindings

ython bindings for FSDB with full Delta Lake and NFS server support.

## What's Included

### Core Database Operations
- ✓ Database creation/opening (local and S3)
- ✓ Data insertion (JSON format)
- ✓ **Buffered insert for high performance** (batches small writes)
- ✓ SQL queries via DataFusion
- ✓ Row-level deletion
- ✓ Schema management

### Time Travel
- ✓ Query by version
- ✓ Query by timestamp
- ✓ Historical data access

### Delta Lake Operations
- ✓ OPTIMIZE (file compaction)
- ✓ VACUUM (cleanup old files)
- ✓ Z-ORDER (multi-dimensional clustering)
- ✓ Data skipping statistics

### Authentication & Security
- ✓ User management
- ✓ Role-based access control
- ✓ Credentials-based authentication

### Backup & Restore
- ✓ Full backups
- ✓ Incremental backups
- ✓ Point-in-time recovery

### Monitoring
- ✓ Real-time metrics
- ✓ Health checks
- ✓ Data skipping stats

### **NFS Server (MAJOR FEATURE)**
- ✓ Mount Delta Lake as POSIX filesystem
- ✓ Access via standard Unix tools:
  - `ls` - List files
  - `cat` - Read data
  - `grep` - Search data
  - `awk` - Process columns
  - `sed` - Transform data
  - `wc` - Count lines
  - `sort` - Sort data
  - `stat` - File metadata
- ✓ **No special drivers required** - uses OS built-in NFS client
- ✓ CSV file views of Parquet data
- ✓ Direct Parquet file access

## Installation

```bash
# 1. Build Rust library
cd fsdb
cargo build --release

# 2. Generate Python bindings
cargo run --bin uniffi-bindgen -- generate \
    --library target/release/libfsdb.dylib \
    --language python \
    --out-dir ../bindings/python

# 3. Copy library
cp target/release/libfsdb.dylib ../bindings/python/

# 4. Install Python package
pip install -e ../bindings/python/
```

## Example: Buffered Insert (High Performance)

```python
from fsdb import DatabaseOps, Schema, Field

schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
])
db = DatabaseOps.create("/path/to/db", schema)

# Insert many small batches efficiently (buffers up to 1000 rows)
for i in range(100):
    db.insert_buffered_json(f'[{{"id": {i}, "name": "User_{i}"}}]')

# Flush buffer to commit all data
db.flush_write_buffer()

# Result: ~1-2 Delta Lake transactions instead of 100!
```

## Example: POSIX Access to Delta Lake

```python
from fsdb import DatabaseOps, Schema, Field, NfsServer

# Create database
schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
])
db = DatabaseOps.create("/path/to/db", schema)

# Insert data
db.insert_json('[{"id": 1, "name": "Alice"}]')

# Start NFS server
nfs = NfsServer(db, 12049)

# Mount filesystem (requires sudo)
# sudo mount_nfs -o nolocks,vers=3,tcp,port=12049,mountport=12049 localhost:/ /mnt/fsdb

# Now use standard Unix tools:
# $ cat /mnt/fsdb/data/data.csv
# id,name
# 1,Alice
#
# $ grep "Alice" /mnt/fsdb/data/data.csv | awk -F',' '{print $2}'
# Alice
#
# $ wc -l /mnt/fsdb/data/data.csv
# 2 /mnt/fsdb/data/data.csv
```

## Performance

- **Buffered inserts**: Batch multiple small writes into fewer transactions (~10x faster for small batches)
- Zero-copy where possible
- Async Rust operations wrapped for Python
- Tokio runtime for optimal concurrency
- Efficient JSON serialization via Arrow
- Auto-flush at 1000 rows (configurable in Rust)

## Error Handling

All Rust errors properly mapped to Python exceptions:
- `FsdbError.IoError`
- `FsdbError.SerializationError`
- `FsdbError.DeltaLakeError`
- `FsdbError.InvalidOperation`
- And 11 more specific error types

## What Makes This Awesome

1. **Performance**: Rust-powered with buffered inserts (~10x faster for small batches)
2. **No Special Drivers**: Uses OS built-in NFS client
3. **Real POSIX Access**: Mount Delta Lake as filesystem - **game changer**
4. **Standard Tools**: grep/awk/sed work on your data lake
5. **Smart Batching**: Auto-flushes at 1000 rows, reducing transaction overhead

**Access Delta Lake data with grep, awk, sed, and every Unix tool** - no special libraries, no drivers, just mount and go. Plus buffered inserts for 10x performance when loading many small batches. This is the power of FSDB's NFS server + optimized Python bindings.