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

    /// Maximum number of bytes in size.
    pub bytes: u64,

    /// Maximum path length, in bytes.
    pub max_path_length: u64,

    /// Maximum path segment size, in bytes.
    ///
    /// Keep this to a rather small size to prevent super-linear complexity due to string hashing.
    pub max_path_segment_size: u64,
}

impl Default for VfsLimits {
    fn default() -> Self {
        Self {
            inodes: 10_000,
            // 100MB
            bytes: 100 * 1024 * 1024,
            max_path_length: 255,
            max_path_segment_size: 50,
        }
    }
}
