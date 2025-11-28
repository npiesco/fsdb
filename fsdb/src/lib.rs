// FSDB - FileStoreDatabase
// A Delta Lake native database with SQL support

// Core modules
pub mod storage;
pub mod metadata;
pub mod query;
pub mod error;
pub mod security;
pub mod delta_lake;
pub mod transaction;
pub mod batch_buffer;

// Database operations
pub mod database_ops;

// POSIX interface (NFS server)
pub mod nfs;

// Python bindings (UniFFI)
pub mod python;

// Public API
pub use database_ops::DatabaseOps;
pub use transaction::Transaction;
pub use error::{Error, Result};

// Generate UniFFI scaffolding (0.29+ uses proc-macros)
uniffi::setup_scaffolding!();
