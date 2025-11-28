//! Delta Lake Integration
//!
//! Native Delta Lake format support with column statistics and operations.

pub mod stats;
pub mod operations;
pub mod data_skipping;
pub mod merge;

pub use stats::{ColumnStats, compute_column_statistics, get_column_statistics_from_delta};
pub use operations::{OptimizeMetrics, optimize_table, vacuum_table, vacuum_dry_run, zorder_table};
pub use data_skipping::{FileStats, get_file_statistics, extract_predicates, can_skip_file};
pub use merge::{MergeBuilder, MergeMetrics, MatchedUpdateClause, MatchedDeleteClause, NotMatchedInsertClause};

