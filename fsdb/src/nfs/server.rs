//! NFS Server implementation for FSDB
//! Exposes database as NFSv3 filesystem with CSV file views

use crate::database_ops::DatabaseOps;
use crate::nfs::attr_cache::AttrCache;
use crate::nfs::cache::NfsCache;
use crate::nfs::file_views::CsvFileView;

use async_trait::async_trait;
use nfsserve::{
    nfs::{fattr3, fileid3, filename3, ftype3, nfsstat3, nfstime3, sattr3, specdata3},
    vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, error, debug};

// File ID constants
const ROOT_ID: fileid3 = 1;
const DATA_DIR_ID: fileid3 = 2;
const DATA_CSV_ID: fileid3 = 3;
const PARQUET_FILE_ID_START: fileid3 = 100;

/// FSDB NFS Filesystem
/// Maps database operations to NFS file operations
pub struct FsdbFilesystem {
    db: Arc<DatabaseOps>,
    /// Cache of Parquet file IDs to paths
    pub(crate) parquet_files: Arc<Mutex<HashMap<fileid3, String>>>,
    /// Two-tier cache (memory + disk) for file content
    cache: Option<Arc<NfsCache>>,
    /// Attribute cache to prevent mount disconnections during concurrent writes
    attr_cache: Arc<AttrCache>,
}

impl FsdbFilesystem {
    pub fn new(db: Arc<DatabaseOps>) -> Self {
        Self {
            db,
            parquet_files: Arc::new(Mutex::new(HashMap::new())),
            cache: None,
            attr_cache: Arc::new(AttrCache::new()),
        }
    }
    
    /// Create a new filesystem with caching enabled
    pub fn with_cache(db: Arc<DatabaseOps>, cache: Arc<NfsCache>) -> Self {
        Self {
            db,
            parquet_files: Arc::new(Mutex::new(HashMap::new())),
            cache: Some(cache),
            attr_cache: Arc::new(AttrCache::new()),
        }
    }
    
    /// Get current timestamp for file attributes
    fn now() -> nfstime3 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        nfstime3 {
            seconds: now.as_secs() as u32,
            nseconds: now.subsec_nanos(),
        }
    }
    
    /// Create directory attributes
    fn dir_attr(id: fileid3) -> fattr3 {
        let now = Self::now();
        fattr3 {
            ftype: ftype3::NF3DIR,
            mode: 0o755,
            nlink: 2,
            uid: 1000,
            gid: 1000,
            size: 4096,
            used: 4096,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: id,
            atime: now,
            mtime: now,
            ctime: now,
        }
    }
    
    /// Create file attributes
    fn file_attr(id: fileid3, size: u64) -> fattr3 {
        let now = Self::now();
        fattr3 {
            ftype: ftype3::NF3REG,
            mode: 0o644,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            size,
            used: size,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: id,
            atime: now,
            mtime: now,
            ctime: now,
        }
    }
    
    /// Refresh Parquet file cache (Delta Lake mode)
    pub(crate) async fn refresh_parquet_files(&self) -> std::result::Result<(), nfsstat3> {
        // For Delta Lake, scan the base directory for parquet files
        // Delta Lake stores parquet files in the root table directory
        let mut parquet_files = self.parquet_files.lock().await;
        parquet_files.clear();
        
        let base_path = self.db.base_path();
        
        // Scan for .parquet files in the base directory
        match std::fs::read_dir(base_path) {
            Ok(entries) => {
                let mut file_id = PARQUET_FILE_ID_START;
                
                for entry in entries {
                    if let Ok(entry) = entry {
                        let path = entry.path();
                        
                        // Check if it's a parquet file
                        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                            if let Some(filename) = path.file_name() {
                                let filename_str = filename.to_string_lossy().to_string();
                                parquet_files.insert(file_id, filename_str);
                                file_id += 1;
                            }
                        }
                    }
                }
                
                info!("NFS: Found {} parquet files in Delta Lake table", parquet_files.len());
        Ok(())
            }
            Err(e) => {
                error!("Failed to read Delta Lake directory: {}", e);
                Err(nfsstat3::NFS3ERR_IO)
            }
        }
    }
}

#[async_trait]
impl NFSFileSystem for FsdbFilesystem {
    fn root_dir(&self) -> fileid3 {
        ROOT_ID
    }
    
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }
    
    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> std::result::Result<fileid3, nfsstat3> {
        let name = String::from_utf8_lossy(filename.as_ref());
        info!("NFS LOOKUP: dir={}, filename={}", dirid, name);
        
        match dirid {
            ROOT_ID => {
                if name == "data" {
                    Ok(DATA_DIR_ID)
                } else {
                    Err(nfsstat3::NFS3ERR_NOENT)
                }
            }
            DATA_DIR_ID => {
                if name == "data.csv" {
                    Ok(DATA_CSV_ID)
                } else {
                    // Check Parquet files
                    self.refresh_parquet_files().await?;
                    let parquet_files = self.parquet_files.lock().await;
                    
                    for (id, path) in parquet_files.iter() {
                        let basename = std::path::Path::new(path)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(path);
                        if basename == name {
                            return Ok(*id);
                        }
                    }
                    
                    Err(nfsstat3::NFS3ERR_NOENT)
                }
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }
    
    async fn getattr(&self, id: fileid3) -> std::result::Result<fattr3, nfsstat3> {
        info!("NFS GETATTR: id={}", id);
        
        // Check attribute cache first
        if let Some(cached_attr) = self.attr_cache.get(id).await {
            return Ok(cached_attr);
        }
        
        // Cache miss - compute attributes
        let attr = match id {
            ROOT_ID => Self::dir_attr(ROOT_ID),
            DATA_DIR_ID => Self::dir_attr(DATA_DIR_ID),
            DATA_CSV_ID => {
                // Call size() without holding the lock across await
                let db = self.db.clone();
                let view = CsvFileView::new(db);
                let size = match view.size().await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to get CSV size: {}", e);
                        0
                    }
                };
                Self::file_attr(DATA_CSV_ID, size)
            }
            id if id >= PARQUET_FILE_ID_START => {
                // Parquet file (Delta Lake mode)
                let parquet_files = self.parquet_files.lock().await;
                if let Some(file_path) = parquet_files.get(&id) {
                    // Get file size directly from filesystem
                    let full_path = self.db.base_path().join(file_path);
                    if let Ok(metadata) = std::fs::metadata(&full_path) {
                        Self::file_attr(id, metadata.len())
                    } else {
                        error!("File not found: {}", file_path);
                        return Err(nfsstat3::NFS3ERR_NOENT);
                    }
                } else {
                    return Err(nfsstat3::NFS3ERR_NOENT);
                }
            }
            _ => return Err(nfsstat3::NFS3ERR_NOENT),
        };
        
        // Store in cache
        self.attr_cache.set(id, attr).await;
        
        Ok(attr)
    }
    
    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> std::result::Result<fattr3, nfsstat3> {
        // Not implemented yet
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
    
    async fn read(&self, id: fileid3, offset: u64, count: u32) -> std::result::Result<(Vec<u8>, bool), nfsstat3> {
        info!("NFS READ: id={}, offset={}, count={}", id, offset, count);
        
        match id {
            DATA_CSV_ID => {
                // Try cache first if enabled
                if let Some(ref cache) = self.cache {
                    if let Ok(Some(cached_content)) = cache.get("csv:data").await {
                        info!("Cache HIT for data.csv");
                        let end = (offset + count as u64).min(cached_content.len() as u64) as usize;
                        let start = offset.min(cached_content.len() as u64) as usize;
                        let data = cached_content[start..end].to_vec();
                        let eof = end >= cached_content.len();
                        return Ok((data, eof));
                    }
                }
                
                // Cache miss or no cache - generate content without holding lock
                let db = self.db.clone();
                let view = CsvFileView::new(db);
                
                let data = view.read(offset, count).await.map_err(|e| {
                    error!("Read error: {}", e);
                    nfsstat3::NFS3ERR_IO
                })?;
                
                // Store in cache if enabled (only on first read, offset==0)
                if offset == 0 {
                    if let Some(ref cache) = self.cache {
                        if let Ok(full_content) = view.get_full_content().await {
                            let _ = cache.insert("csv:data".to_string(), full_content).await;
                        }
                    }
                }
                
                let size = view.size().await.unwrap_or(0);
                let eof = offset + data.len() as u64 >= size;
                Ok((data, eof))
            }
            id if id >= PARQUET_FILE_ID_START => {
                // Read individual Parquet file as CSV
                let file_path = {
                let parquet_files = self.parquet_files.lock().await;
                    parquet_files.get(&id)
                        .ok_or(nfsstat3::NFS3ERR_NOENT)?
                        .clone()
                };
                let cache_key = format!("csv:file:{}", file_path);
                
                // Try cache first if enabled
                if let Some(ref cache) = self.cache {
                    if let Ok(Some(cached_content)) = cache.get(&cache_key).await {
                        info!("Cache HIT for {}", file_path);
                        let end = (offset + count as u64).min(cached_content.len() as u64) as usize;
                        let start = offset.min(cached_content.len() as u64) as usize;
                        let data = cached_content[start..end].to_vec();
                        let eof = end >= cached_content.len();
                        return Ok((data, eof));
                    }
                }
                
                // Cache miss - generate content
                let file_view = CsvFileView::new_for_file(self.db.clone(), file_path.clone());
                let data = file_view.read(offset, count).await.map_err(|e| {
                    error!("Read error for Parquet file: {}", e);
                    nfsstat3::NFS3ERR_IO
                })?;
                
                // Store in cache if enabled (only on first read)
                if offset == 0 {
                    if let Some(ref cache) = self.cache {
                        if let Ok(full_content) = file_view.get_full_content().await {
                            let _ = cache.insert(cache_key, full_content).await;
                        }
                    }
                }
                
                let size = file_view.size().await.unwrap_or(0);
                let eof = offset + data.len() as u64 >= size;
                Ok((data, eof))
            }
            _ => Err(nfsstat3::NFS3ERR_ISDIR),
        }
    }
    
    async fn write(&self, id: fileid3, _offset: u64, data: &[u8]) -> std::result::Result<fattr3, nfsstat3> {
        info!("NFS WRITE: id={}, data_len={}", id, data.len());
        
        match id {
            DATA_CSV_ID => {
                // Fetch cached content BEFORE invalidating (for performance)
                let cached_content = if let Some(ref cache) = self.cache {
                    match cache.get("csv:data").await {
                        Ok(Some(content)) => {
                            info!("Using cached CSV for write diff ({} bytes)", content.len());
                            Some(content)
                        }
                        _ => {
                            debug!("No cached CSV available for write diff");
                            None
                        }
                    }
                } else {
                    None
                };
                
                // Don't hold lock across await - create temporary view
                let db = self.db.clone();
                let view = CsvFileView::new(db);
                
                view.apply_write(data, cached_content).await.map_err(|e| {
                    error!("Write error: {}", e);
                    nfsstat3::NFS3ERR_IO
                })?;
                
                // UPDATE content cache after write (don't invalidate!)
                // This keeps subsequent reads fast by avoiding CSV regeneration
                if let Some(ref cache) = self.cache {
                    // Generate fresh CSV content after write
                    let fresh_csv = view.generate_csv().await.map_err(|e| {
                        error!("Failed to generate CSV for cache: {}", e);
                        nfsstat3::NFS3ERR_IO
                    })?;
                    let fresh_size = fresh_csv.len();
                    
                    // Update cache with new content
                    if let Err(e) = cache.insert("csv:data".to_string(), fresh_csv).await {
                        error!("Failed to update cache after write: {}", e);
                        // Don't fail the write if cache update fails
                    } else {
                        info!("Content cache UPDATED for data.csv after write ({} bytes)", fresh_size);
                    }
                }
                
                // IMPORTANT: Update attr_cache with NEW file size after write
                // We must return accurate file size or OS NFS clients will truncate reads!
                let size = view.size().await.unwrap_or(0);
                let attr = Self::file_attr(DATA_CSV_ID, size);
                self.attr_cache.set(DATA_CSV_ID, attr).await;
                info!("Write completed, attr cache updated with new size: {} bytes", size);
                
                Ok(attr)
            }
            _ => Err(nfsstat3::NFS3ERR_ROFS),
        }
    }
    
    async fn create(&self, _dirid: fileid3, _filename: &filename3, _attr: sattr3) -> std::result::Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
    
    async fn create_exclusive(&self, _dirid: fileid3, _filename: &filename3) -> std::result::Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
    
    async fn mkdir(&self, _dirid: fileid3, _dirname: &filename3) -> std::result::Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
    
    async fn remove(&self, dirid: fileid3, filename: &filename3) -> std::result::Result<(), nfsstat3> {
        let filename_str = String::from_utf8_lossy(filename);
        info!("NFS REMOVE: dir={}, file={}", dirid, filename_str);
        
        // Only support deletion of data.csv from /data directory (truncate table)
        if dirid == DATA_DIR_ID && filename_str == "data.csv" {
            info!("Deleting data.csv - truncating table");
            
            // Delete all rows using deletion vectors (efficient, no rewrite)
            let db = self.db.clone();
            db.delete_rows_where("1=1").await.map_err(|e| {
                error!("Failed to truncate table: {}", e);
                nfsstat3::NFS3ERR_IO
            })?;
            
            // Invalidate cache after deletion
            if let Some(ref cache) = self.cache {
                let _ = cache.remove("csv:data").await;
                info!("Content cache invalidated for data.csv after deletion");
            }
            
            info!("Table truncated successfully");
            Ok(())
        } else {
            // Other file deletions not supported
            info!("File deletion not supported for: {}", filename_str);
            Err(nfsstat3::NFS3ERR_NOTSUPP)
        }
    }
    
    async fn rename(&self, _from_dirid: fileid3, _from_filename: &filename3, _to_dirid: fileid3, _to_filename: &filename3) -> std::result::Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
    
    async fn readdir(&self, dirid: fileid3, start_after: fileid3, max_entries: usize) -> std::result::Result<ReadDirResult, nfsstat3> {
        info!("NFS READDIR: dir={}, start_after={}, max={}", dirid, start_after, max_entries);
        
        let mut entries = Vec::new();
        
        match dirid {
            ROOT_ID => {
                if start_after < DATA_DIR_ID {
                    entries.push(DirEntry {
                        fileid: DATA_DIR_ID,
                        name: "data".as_bytes().into(),
                        attr: Self::dir_attr(DATA_DIR_ID),
                    });
                }
            }
            DATA_DIR_ID => {
                // Always include data.csv
                if start_after < DATA_CSV_ID {
                    let db = self.db.clone();
                    let view = CsvFileView::new(db);
                    let size = match view.size().await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to get CSV size in readdir: {}", e);
                            0
                        }
                    };
                    entries.push(DirEntry {
                        fileid: DATA_CSV_ID,
                        name: "data.csv".as_bytes().into(),
                        attr: Self::file_attr(DATA_CSV_ID, size),
                    });
                }
                
                // Add Parquet files (Delta Lake mode)
                self.refresh_parquet_files().await?;
                let parquet_files = self.parquet_files.lock().await;
                
                for (id, file_path) in parquet_files.iter() {
                    if *id > start_after && entries.len() < max_entries {
                        let basename = std::path::Path::new(file_path)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(file_path)
                            .as_bytes()
                            .into();
                        
                        // Get real file size from filesystem
                        let full_path = self.db.base_path().join(file_path);
                        let size = std::fs::metadata(&full_path)
                            .map(|m| m.len())
                            .unwrap_or(1024); // Fallback to 1024 if not found
                        
                        entries.push(DirEntry {
                            fileid: *id,
                            name: basename,
                            attr: Self::file_attr(*id, size),
                        });
                    }
                }
            }
            _ => return Err(nfsstat3::NFS3ERR_NOTDIR),
        }
        
        Ok(ReadDirResult {
            entries,
            end: true,
        })
    }
    
    async fn symlink(&self, _dirid: fileid3, _linkname: &filename3, _symlink_data: &nfsserve::nfs::nfspath3, _attr: &sattr3) -> std::result::Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
    
    async fn readlink(&self, _id: fileid3) -> std::result::Result<nfsserve::nfs::nfspath3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}


