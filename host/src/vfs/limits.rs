//! Limit configuration.

/// Limits for virtual filesystems.
///
/// # Depth
/// Note that we do NOT per se limit the depth of the file system, since it is virtually not different from limiting
/// [the number of inodes](Self::inodes). Expensive path traversal is further limited by
/// [`max_path_length`](Self::max_path_length).
#[derive(Debug, Clone)]
#[expect(missing_copy_implementations, reason = "allow later extensions")]
pub struct VfsLimits {
    /// Maximum number of inodes.
    pub inodes: u64,

    /// Maximum path length, in bytes.
    pub max_path_length: u64,

    /// Maximum path segment size, in bytes.
    ///
    /// Keep this to a rather small size to prevent super-linear complexity due to string hashing.
    pub max_path_segment_size: u64,

    /// Maximum total VFS storage in bytes.
    ///
    /// This limits the total amount of data that can be stored in the VFS to prevent DoS attacks.
    pub max_storage_bytes: u64,

    /// Maximum size of a single file in bytes.
    ///
    /// This prevents a single file from consuming all available storage.
    pub max_file_size: u64,

    /// Maximum number of write operations per second.
    ///
    /// This rate-limits write operations to prevent DoS attacks through excessive I/O.
    pub max_write_ops_per_sec: u32,
}

impl Default for VfsLimits {
    fn default() -> Self {
        Self {
            inodes: 10_000,
            max_path_length: 255,
            max_path_segment_size: 50,
            max_storage_bytes: 100 * 1024 * 1024, // 100MB total storage
            max_file_size: 10 * 1024 * 1024,      // 10MB per file
            max_write_ops_per_sec: 1000,          // 1000 write ops/sec
        }
    }
}
