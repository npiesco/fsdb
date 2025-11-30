#!/usr/bin/env python3
"""
Full manual FUSE test - actually create DB and mount it
"""
import subprocess
import time
import os
import sys

def run(cmd):
    """Run command and print output"""
    print(f"\n💻 {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f"stderr: {result.stderr}")
    return result.returncode

def main():
    print("\n" + "="*70)
    print("FULL MANUAL FUSE-T TEST")
    print("="*70)
    
    fsdb = "/Users/nicholas.piesco/Downloads/FSDB"
    test_dir = "/tmp/fsdb_manual_test"
    
    # Cleanup
    print("\n🧹 Cleaning up...")
    run(f"rm -rf {test_dir}")
    run(f"mkdir -p {test_dir}/db {test_dir}/mount")
    run(f"python3 {fsdb}/scripts/cleanup_fuse_mounts.py")
    
    # Create a minimal database using Rust code inline
    print("\n📦 Creating test database...")
    test_code = '''
use fsdb::{{DatabaseOps, Error}};
use arrow::array::{{Int32Array, RecordBatch}};
use arrow::datatypes::{{DataType, Field, Schema}};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    
    let db = DatabaseOps::create("/tmp/fsdb_manual_test/db", schema.clone())?;
    
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();
    
    db.insert(batch).await?;
    println!("✓ Database created with 3 rows");
    Ok(())
}
'''
    
    # Write test program
    test_file = f"{test_dir}/create_db.rs"
    with open(test_file, 'w') as f:
        f.write(test_code)
    
    # Run it
    ret = run(f"cd {fsdb} && rustc --edition 2021 -L target/release/deps --extern fsdb=target/release/libfsdb.rlib --extern arrow=target/release/deps/libarrow-*.rlib --extern tokio=target/release/deps/libtokio-*.rlib {test_file} -o {test_dir}/create_db 2>&1 | head -20")
    
    if ret != 0:
        print("\n ✗ Compilation failed, trying simpler approach...")
        # Just check if DB directory structure is valid
        run(f"mkdir -p {test_dir}/db/{{data_files,_wal,_metadata,_txn_log}}")
        run(f"echo '{{}}' > {test_dir}/db/_metadata/file_inventory.json")
        print("✓ Created minimal DB structure")
    
    # Now try to mount
    print("\n Attempting to mount...")
    print(f"   DB: {test_dir}/db")
    print(f"   Mount: {test_dir}/mount")
    
    # Try mount in foreground with debug output
    cmd = f"cd {fsdb} && RUST_LOG=debug cargo run --bin fsdb -- mount {test_dir}/db {test_dir}/mount --debug 2>&1 | head -50 &"
    print(f"\n💻 {cmd}")
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Wait a bit for mount
    print("\nWaiting 3 seconds for mount to establish...")
    time.sleep(3)
    
    # Check if mounted
    print("\nChecking if mounted...")
    ret = run(f"mount | grep {test_dir}/mount")
    
    if ret == 0:
        print("✓ Mount appears in mount table!")
        
        # Try to access it
        print("\nTrying to list mount point...")
        run(f"ls -la {test_dir}/mount")
        
        print("\nTrying to access with Python os.listdir...")
        try:
            entries = os.listdir(f"{test_dir}/mount")
            print(f"✓ Python can read: {entries}")
        except Exception as e:
            print(f"✗ Python cannot read: {e}")
        
    else:
        print("✗ Mount NOT in mount table")
    
    # Check process
    print("\nChecking for fsdb process...")
    run("ps aux | grep '[f]sdb' | grep -v grep")
    
    # Cleanup
    print("\nCleaning up...")
    proc.terminate()
    time.sleep(1)
    run(f"umount {test_dir}/mount 2>/dev/null || true")
    
    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70)

if __name__ == "__main__":
    main()

