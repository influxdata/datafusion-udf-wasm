#![allow(unfulfilled_lint_expectations)]
//! Limit configuration.

use tokio::time::{Duration, Instant};
use wasmtime_wasi::p2::{FsError, bindings::filesystem::types::ErrorCode};

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
            max_storage_bytes: 10 * 1024 * 1024, // 10 MB
            max_file_size: 1024 * 1024,          // 1 MB
            max_write_ops_per_sec: 1000,
        }
    }
}

/// Rate limiter for write operations to excessive writes.
#[derive(Debug)]
#[expect(
    dead_code,
    reason = "currently not used, but will be used in future implementations"
)]
pub(crate) struct WriteRateLimiter {
    /// Window start time.
    window_start: Instant,

    /// Number of operations in current window.
    ops_count: u32,

    /// Maximum operations per second.
    max_ops_per_sec: u32,
}

impl WriteRateLimiter {
    /// Create a new rate limiter.
    pub(crate) fn new(max_ops_per_sec: u32) -> Self {
        Self {
            window_start: Instant::now(),
            ops_count: 0,
            max_ops_per_sec,
        }
    }

    /// Check if a write operation is allowed.
    pub(crate) fn _check_write_allowed(&mut self) -> Result<(), FsError> {
        let now = Instant::now();
        let window_duration = Duration::from_secs(1);

        if now.duration_since(self.window_start) >= window_duration {
            self.window_start = now;
            self.ops_count = 0;
        }

        if self.ops_count >= self.max_ops_per_sec {
            return Err(FsError::trap(ErrorCode::Busy));
        }

        self.ops_count += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::{sync::Mutex, task::JoinSet};

    #[tokio::test]
    async fn test_write_rate_limiter() {
        // Create a basic write rate limiter allowing 5 ops per second.
        // We then spawn 6 tasks trying to perform a write operation simultaneously.
        let limiter = Arc::new(Mutex::new(WriteRateLimiter::new(5)));

        let mut tasks = JoinSet::new();
        for _ in 0..6 {
            tasks.spawn({
                let limiter = Arc::clone(&limiter);
                async move {
                    let mut limiter = limiter.lock().await;
                    limiter._check_write_allowed()
                }
            });
        }

        let completed = tasks.join_all().await;

        assert_eq!(completed.len(), 6);
        assert_eq!(completed.iter().filter(|res| res.is_ok()).count(), 5);
        assert!(completed.last().unwrap().is_err());

        let limiter = Arc::new(Mutex::new(WriteRateLimiter::new(5)));

        // We will now spawn another set of tasks, and have the last one
        // wait a couple seconds before trying again.
        let mut tasks = JoinSet::new();
        for i in 0..6 {
            tasks.spawn({
                let limiter = Arc::clone(&limiter);
                async move {
                    if i == 5 {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    let mut limiter = limiter.lock().await;
                    limiter._check_write_allowed()
                }
            });
        }

        let completed = tasks.join_all().await;

        assert_eq!(completed.len(), 6);
        // All tasks should have succeeded now.
        assert_eq!(completed.iter().filter(|res| res.is_ok()).count(), 6);
    }
}
