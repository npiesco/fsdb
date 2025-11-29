# POSIX File Operations Progress Tracker - FSDB NFS Server

## Commands That Trigger Delta Lake Operations (Hooked Up)

These commands **modify data** and trigger ACID Delta Lake transactions:

### ✅ INSERT Operations
- **`echo >>`** - Appends new rows → Triggers `INSERT` transaction
  ```bash
  echo "3,Charlie,28" >> /mnt/fsdb/data/data.csv
  # → Delta Lake INSERT with ACID guarantees
  ```
- **`cat >>`** - Appends data from file → Triggers `INSERT` transaction
  ```bash
  cat new_rows.csv >> /mnt/fsdb/data/data.csv
  # → Delta Lake INSERT transaction
  ```

### ✅ UPDATE/DELETE/INSERT Operations (MERGE)
- **`sed -i`** - In-place editing → Triggers `MERGE` transaction (UPDATE/DELETE/INSERT)
  ```bash
  sed -i 's/Alice,30/Alice,31/' /mnt/fsdb/data/data.csv
  # → Delta Lake MERGE (UPDATE operation)
  
  sed -i '/Bob/d' /mnt/fsdb/data/data.csv
  # → Delta Lake MERGE (DELETE operation)
  ```
- **`vim` / `nano`** - Text editors that save → Triggers `MERGE` transaction
  ```bash
  vim /mnt/fsdb/data/data.csv
  # Edit and save → Delta Lake MERGE (detects INSERT/UPDATE/DELETE)
  ```
- **`cat >`** - Overwrite file → Triggers `MERGE` transaction
  ```bash
  cat modified.csv > /mnt/fsdb/data/data.csv
  # → Delta Lake MERGE (compares old vs new, detects all changes)
  ```
- **`grep -v` + redirect** - Filter and overwrite → Triggers `MERGE` transaction
  ```bash
  grep -v "Alice" data.csv > temp && cat temp > data.csv
  # → Delta Lake MERGE (DELETE operation)
  ```

### ✅ DELETE ALL (Truncate Table)
- **`rm`** - Delete file → Triggers `DELETE ALL` transaction (truncates table)
  ```bash
  rm /mnt/fsdb/data/data.csv
  # → Delta Lake DELETE ALL (marks all rows as deleted)
  ```

---

## Commands That Work (Read-Only, Query Data)

These commands **read data** but don't trigger Delta operations. They query the Delta Lake and return results:

### ✅ Query/Read Operations
- **`cat`** - Display file content → Queries Delta Lake, returns CSV
  ```bash
  cat /mnt/fsdb/data/data.csv
  # → Queries all Parquet files, generates CSV on-demand
  ```
- **`grep`** - Search patterns → Reads CSV, searches content
  ```bash
  grep "Alice" /mnt/fsdb/data/data.csv
  # → First read generates CSV (cached), grep searches cached content
  ```
- **`awk`** - Process columns → Reads CSV, processes fields
  ```bash
  awk -F',' '{print $2, $3}' /mnt/fsdb/data/data.csv
  # → Reads CSV, processes columns
  ```
- **`head`** - First N lines → Reads CSV, returns first portion
- **`tail`** - Last N lines → Reads CSV, returns last portion
- **`sort`** - Sort lines → Reads CSV, sorts output
  ```bash
  sort -t',' -k2 /mnt/fsdb/data/data.csv
  # → Reads CSV, sorts by column
  ```
- **`wc`** - Count lines → Reads CSV, counts lines
  ```bash
  wc -l /mnt/fsdb/data/data.csv
  # → Reads CSV, counts lines (includes header)
  ```
- **`cut`** - Extract fields → Reads CSV, extracts columns
- **`tr`** - Translate characters → Reads CSV, transforms characters

### ✅ Directory Operations
- **`ls`** - List directory → Lists files in NFS mount
  ```bash
  ls -la /mnt/fsdb/
  # → Lists: data/, schema.sql, .query, .stats
  ls -la /mnt/fsdb/data/
  # → Lists: data.csv, *.parquet files
  ```
- **`cd`** - Change directory → Standard directory navigation
- **`pwd`** - Print working directory → Standard directory operation
- **`find`** - Search files → Can list files (read-only)
- **`mkdir`** - Create directories → Creates new directories in NFS mount
  ```bash
  mkdir /mnt/fsdb/testdir
  # → Creates new directory with unique file ID
  ```

---

## Commands That Don't Work (Not Implemented)

These commands return `NFS3ERR_NOTSUPP` (not supported):

### ❌ File Operations
- **`cp`** - Copy files → Not supported (no `create` implementation)
- **`mv`** - Move/rename files → Not supported (no `rename` implementation)
- **`rmdir`** - Remove directories → Not supported

---

## Summary

| Category | Commands | Status |
|----------|----------|--------|
| **INSERT** | `echo >>`, `cat >>` | ✅ Triggers Delta INSERT |
| **MERGE (UPDATE/DELETE/INSERT)** | `sed -i`, `vim`, `nano`, `cat >`, `grep -v` + redirect | ✅ Triggers Delta MERGE |
| **DELETE ALL** | `rm` (file deletion) | ✅ Triggers Delta DELETE ALL |
| **Query/Read** | `cat`, `grep`, `awk`, `head`, `tail`, `sort`, `wc`, `cut`, `tr` | ✅ Works (reads Delta Lake) |
| **Directory** | `ls`, `cd`, `pwd`, `find` | ✅ Works (read-only) |
| **Directory Creation** | `mkdir` | ✅ Implemented |
| **Not Supported** | `cp`, `mv`, `rmdir` | ❌ Not implemented |

---

## Implementation Details

### Write Operations (Delta Lake Transactions)

1. **Append (`echo >>`, `cat >>`)**:
   - NFS `write()` called with new CSV rows
   - `CsvFileView::handle_csv_append()` parses CSV
   - `DatabaseOps::insert()` → Delta Lake INSERT transaction

2. **Overwrite (`sed -i`, `vim`, `cat >`)**:
   - NFS `write()` called with full CSV content
   - `CsvFileView::handle_csv_overwrite()` compares old vs new CSV
   - Detects INSERT/UPDATE/DELETE by comparing row IDs
   - `DatabaseOps::merge()` → Delta Lake MERGE transaction (atomic INSERT/UPDATE)
   - `DatabaseOps::delete_rows_where()` → Delta Lake DELETE (for deleted rows)

3. **File Deletion (`rm`)**:
   - NFS `remove()` called
   - `DatabaseOps::delete_rows_where("1=1")` → Delta Lake DELETE ALL (truncate)

### Read Operations (Query Delta Lake)

1. **Read (`cat`, `grep`, etc.)**:
   - NFS `read()` called
   - `CsvFileView::generate_csv()` queries Delta Lake: `SELECT * FROM data`
   - Converts Parquet files → RecordBatch → CSV
   - Two-tier cache (memory + disk) for performance
   - Returns CSV content to POSIX command

---

## Notes

- **Caching**: First read generates CSV and caches it. Subsequent reads (like `grep` scanning) use cached content for speed.
- **Large Files**: Files ≥1MB use memory-mapped I/O (`mmap`) for zero-copy access.
- **Atomicity**: All write operations are ACID transactions. MERGE operations are atomic (all changes succeed or all fail).
- **Performance**: Buffered inserts available via Python API (`insert_buffered_json`) for 10x performance improvement.

