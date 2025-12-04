//! Virtual File System.
//!
//! This provides a basic in-memory virtual file system for the WASM guests with write support.
//!
//! The initial data gets populated via a TAR container, and guests can then create, modify, and truncate files
//! within the VFS. All changes are constrained by configurable limits to prevent DoS attacks.
//!
//! While this implementation has limited functionality compared to a full filesystem, it supports
//! the essential operations needed for Python guest interpreter and other use cases.

use std::{
    collections::HashMap,
    hash::Hash,
    io::{Cursor, Read},
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use rand::Rng;
use siphasher::sip128::{Hasher128, SipHasher24};
use std::sync::Mutex;
use wasmtime::component::{HasData, Resource};
use wasmtime_wasi::{
    ResourceTable,
    filesystem::Descriptor,
    p2::{
        FsError, FsResult, InputStream as WasiInputStream,
        bindings::filesystem::{
            self,
            types::{
                Advice, Datetime, DescriptorFlags, DescriptorStat, DescriptorType, DirectoryEntry,
                DirectoryEntryStream, Error, ErrorCode, Filesize, InputStream, MetadataHashValue,
                NewTimestamp, OpenFlags, OutputStream, PathFlags,
            },
        },
        pipe::MemoryInputPipe,
    },
};

pub use crate::vfs::limits::VfsLimits;
use crate::{
    error::LimitExceeded,
    limiter::Limiter,
    vfs::path::{PathSegment, PathTraversal},
};

mod limits;
mod path;

/// Shared version of [`VfsNode`].
type SharedVfsNode = Arc<RwLock<VfsNode>>;

/// A kind node in the virtual filesystem tree.
#[derive(Debug)]
struct VfsNode {
    /// Which kind of data is stored in this node.
    kind: VfsNodeKind,

    /// Pointer to parent node.
    parent: Option<Weak<RwLock<VfsNode>>>,
}

/// A kind node in the virtual filesystem tree.
#[derive(Debug)]
enum VfsNodeKind {
    /// A regular file with its content.
    File {
        /// File content stored in memory.
        content: Vec<u8>,
    },
    /// A directory containing child nodes.
    Directory {
        /// Child nodes indexed by name.
        children: HashMap<PathSegment, SharedVfsNode>,
    },
}

impl VfsNode {
    /// Convert a VfsNode to DescriptorStat.
    fn stat(&self) -> DescriptorStat {
        match &self.kind {
            VfsNodeKind::File { content, .. } => DescriptorStat {
                type_: DescriptorType::RegularFile,
                link_count: 1,
                size: content.len() as u64,
                data_access_timestamp: None,
                data_modification_timestamp: None,
                status_change_timestamp: None,
            },
            VfsNodeKind::Directory { children, .. } => DescriptorStat {
                type_: DescriptorType::Directory,
                link_count: 1,
                size: children.len() as u64,
                data_access_timestamp: None,
                data_modification_timestamp: None,
                status_change_timestamp: None,
            },
        }
    }

    /// From the official docs:
    ///
    /// > Return a hash of the metadata associated with a filesystem object referred to by a descriptor.
    /// >
    /// > This returns a hash of the last-modification timestamp and file size, and may also include the inode number,
    /// > device number, birth timestamp, and other metadata fields that may change when the file is modified or
    /// > replaced. It may also include a secret value chosen by the implementation and not otherwise exposed.
    /// >
    /// > Implementations are encouraged to provide the following properties:
    /// > - If the file is not modified or replaced, the computed hash value should usually not change.
    /// > - If the object is modified or replaced, the computed hash value should usually change.
    /// > - The inputs to the hash should not be easily computable from the computed hash.
    /// >
    /// > However, none of these is required.
    fn metadata_hash(&self, key: &[u8; 16]) -> MetadataHashValue {
        let DescriptorStat {
            type_,
            // link count should NOT influence the hash
            link_count: _,
            size,
            // access time should NOT influence the hash
            data_access_timestamp: _,
            data_modification_timestamp,
            status_change_timestamp,
        } = self.stat();

        let mut hasher = SipHasher24::new_with_key(key);

        let hash_datetime = |hasher: &mut SipHasher24, dt| {
            if let Some(Datetime {
                seconds,
                nanoseconds,
            }) = dt
            {
                // sentinel
                true.hash(hasher);
                seconds.hash(hasher);
                nanoseconds.hash(hasher);
            } else {
                // sentinel
                false.hash(hasher);
            }
        };

        (type_ as u64).hash(&mut hasher);
        size.hash(&mut hasher);
        hash_datetime(&mut hasher, data_modification_timestamp);
        hash_datetime(&mut hasher, status_change_timestamp);

        let (lower, upper) = hasher.finish128().as_u64();
        MetadataHashValue { lower, upper }
    }

    /// Resolve a path from a starting node to a target node.
    fn traverse(
        start: SharedVfsNode,
        directions: impl Iterator<Item = Result<PathTraversal, LimitExceeded>>,
    ) -> FsResult<SharedVfsNode> {
        let mut current = start;

        for direction in directions {
            let direction = direction?;

            let current_guard = current.read().unwrap();
            let next = match &current_guard.kind {
                VfsNodeKind::Directory { children, .. } => match direction {
                    PathTraversal::Stay => Arc::clone(&current),
                    PathTraversal::Up => current_guard
                        .parent
                        .as_ref()
                        .map(|parent| parent.upgrade().expect("parent still valid"))
                        // note: `/..` = `/`, i.e. overshooting is allowed
                        .unwrap_or_else(|| Arc::clone(&current)),
                    PathTraversal::Down(segment) => Arc::clone(
                        children
                            .get(&segment)
                            .ok_or_else(|| FsError::trap(ErrorCode::NoEntry))?,
                    ),
                },
                VfsNodeKind::File { .. } => {
                    return Err(FsError::trap(ErrorCode::NotDirectory));
                }
            };
            drop(current_guard);
            current = next;
        }

        Ok(current)
    }
}

/// Tracked allocation of some resource.
#[derive(Debug)]
struct Allocation {
    /// Current amount of allocation.
    n: AtomicU64,

    /// Name of the resource.
    name: &'static str,

    /// Allocation limit.
    limit: u64,
}

impl Allocation {
    /// Create new allocation tracker for given resource.
    fn new(name: &'static str, limit: u64) -> Self {
        Self {
            n: AtomicU64::new(0),
            name,
            limit,
        }
    }

    /// Get current allocation size.
    fn get(&self) -> u64 {
        self.n.load(Ordering::SeqCst)
    }

    /// Increase allocation by given amount.
    fn inc(&self, n: u64) -> Result<(), LimitExceeded> {
        self.n
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |old| {
                let new = old.checked_add(n)?;
                (new <= self.limit).then_some(new)
            })
            .map(|_| ())
            .map_err(|current| LimitExceeded {
                name: self.name,
                limit: self.limit,
                current,
                requested: n,
            })
    }

    // Decrease allocation by given amount.
    fn dec(&self, n: u64) -> Result<(), LimitExceeded> {
        self.n
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |old| {
                let new = old.checked_sub(n)?;
                Some(new)
            })
            .map(|_| ())
            .map_err(|current| LimitExceeded {
                name: self.name,
                limit: self.limit,
                current,
                requested: n,
            })
    }
}

/// State for the virtual filesystem.
#[derive(Debug)]
pub(crate) struct VfsState {
    /// Root directory node.
    root: SharedVfsNode,

    /// Hash key for metadata hashes.
    metadata_hash_key: [u8; 16],

    /// Limits.
    limits: VfsLimits,

    /// Current allocation of inodes.
    inodes_allocation: Allocation,

    /// Current allocation of storage bytes.
    storage_allocation: Allocation,

    /// Write operation rate limiter.
    write_rate_limiter: Mutex<WriteRateLimiter>,
}

/// Rate limiter for write operations to prevent DoS attacks.
#[derive(Debug)]
struct WriteRateLimiter {
    /// Window start time.
    window_start: Instant,
    /// Number of operations in current window.
    ops_count: u32,
    /// Maximum operations per second.
    max_ops_per_sec: u32,
}

impl WriteRateLimiter {
    /// Create a new rate limiter.
    fn new(max_ops_per_sec: u32) -> Self {
        Self {
            window_start: Instant::now(),
            ops_count: 0,
            max_ops_per_sec,
        }
    }

    /// Check if a write operation is allowed.
    fn check_write_allowed(&mut self) -> Result<(), FsError> {
        let now = Instant::now();
        let window_duration = Duration::from_secs(1);

        // Reset window if needed
        if now.duration_since(self.window_start) >= window_duration {
            self.window_start = now;
            self.ops_count = 0;
        }

        // Check rate limit
        if self.ops_count >= self.max_ops_per_sec {
            return Err(FsError::trap(ErrorCode::Busy));
        }

        self.ops_count += 1;
        Ok(())
    }
}

impl VfsState {
    /// Create a new empty VFS.
    pub(crate) fn new(limits: VfsLimits) -> Self {
        let inodes_allocation = Allocation::new("inodes", limits.inodes);
        let storage_allocation = Allocation::new("storage", limits.max_storage_bytes);
        let write_rate_limiter = Mutex::new(WriteRateLimiter::new(limits.max_write_ops_per_sec));

        Self {
            root: Arc::new(RwLock::new(VfsNode {
                kind: VfsNodeKind::Directory {
                    children: HashMap::new(),
                },
                parent: None,
            })),
            metadata_hash_key: rand::rng().random(),
            limits,
            inodes_allocation,
            storage_allocation,
            write_rate_limiter,
        }
    }

    /// Populate the VFS from a tar archive.
    pub(crate) fn populate_from_tar(
        &mut self,
        tar_data: &[u8],
        limiter: &mut Limiter,
    ) -> Result<(), std::io::Error> {
        let size_pre = limiter.size();
        let cursor = Cursor::new(tar_data);
        let mut archive = tar::Archive::new(cursor);

        for entry in archive.entries()? {
            let mut entry = entry?;

            let entry_type = entry.header().entry_type();
            let kind = match entry_type {
                tar::EntryType::Directory => VfsNodeKind::Directory {
                    children: HashMap::new(),
                },
                tar::EntryType::Regular => {
                    let mut content = Vec::new();
                    entry.read_to_end(&mut content)?;
                    content.shrink_to_fit();
                    limiter.grow(content.capacity())?;
                    VfsNodeKind::File { content }
                }
                other => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        format!(
                            "Unsupported TAR content: {other:?} @ {}",
                            entry
                                .path()
                                .map(|p| p.display().to_string())
                                .unwrap_or_else(|_| "<n/a>".to_owned())
                        ),
                    ));
                }
            };

            let path = entry.path()?;
            let path_str = path.to_string_lossy();

            // NOTE: we ignore "is_root" here because TAR files are unpacked at root level, hence CWD == root
            let (_is_root, directions) = PathTraversal::parse(&path_str, &self.limits)?;
            let mut directions = directions.collect::<Vec<_>>();

            // Path traversal happens on the VFS tree, NOT on the parsed path, so the last part MUST be a valid segment.
            // That also means that `/does_not_exist/../to_be_created` is NOT valid.
            let name = match directions
                .pop()
                .expect("PathTraversal ensures that the path is not empty")?
            {
                PathTraversal::Down(segment) => segment,
                other @ (PathTraversal::Stay | PathTraversal::Up) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidFilename,
                        format!("TAR target MUST end in a valid filename, not {other}"),
                    ));
                }
            };

            let node = VfsNode::traverse(Arc::clone(&self.root), directions.into_iter())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            let child = Arc::new(RwLock::new(VfsNode {
                kind,
                parent: Some(Arc::downgrade(&node)),
            }));

            self.inodes_allocation.inc(1)?;
            limiter.grow(name.len())?;
            limiter.grow(std::mem::size_of_val(&child))?;

            match &mut node.write().unwrap().kind {
                VfsNodeKind::File { .. } => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotADirectory,
                        "not a directory",
                    ));
                }
                VfsNodeKind::Directory { children } => {
                    children.insert(name, child);
                }
            }
        }

        log::info!(
            "unpacked WASM guest root filesystem from {} bytes TAR, consuming {} bytes and {} inodes",
            tar_data.len(),
            limiter.size() - size_pre,
            self.inodes_allocation.get()
        );

        Ok(())
    }

    fn check_write_allowed(&mut self) -> Result<(), FsError> {
        match self.write_rate_limiter.lock() {
            Ok(mut guard) => {
                guard.check_write_allowed()?;
                drop(guard);
                Ok(())
            }
            Err(_) => Err(FsError::trap(ErrorCode::Busy)),
        }
    }
}

/// A descriptor for an open file or directory.
#[derive(Debug)]
struct VfsDescriptor {
    /// Node.
    node: SharedVfsNode,
    /// Flags used to open this descriptor.
    flags: DescriptorFlags,
}

/// Stream for reading directory entries.
#[derive(Debug)]
struct VfsDirectoryStream {
    /// Iterator over entries.
    entries: std::iter::Fuse<std::vec::IntoIter<DirectoryEntry>>,
}

/// In-memory output stream for VFS writes.
#[derive(Debug)]
struct VfsOutputStream {
    /// File node being written to.
    node: SharedVfsNode,
    /// Current write offset.
    offset: u64,
    /// Buffer for collecting writes.
    write_buffer: Vec<u8>,
}

impl VfsOutputStream {
    /// Create a new VFS output stream.
    fn new(node: SharedVfsNode, offset: u64) -> Self {
        Self {
            node,
            offset,
            write_buffer: Vec::new(),
        }
    }

    /// Write data to the stream buffer.
    fn write_data(&mut self, data: &[u8]) -> Result<u64, FsError> {
        self.write_buffer.extend_from_slice(data);
        Ok(data.len() as u64)
    }

    /// Flush buffered data to the file.
    fn flush_to_file(&mut self, vfs_state: &mut VfsState) -> Result<(), FsError> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        // Check rate limit
        vfs_state.check_write_allowed()?;

        let mut node_guard = self.node.write().unwrap();
        match &mut node_guard.kind {
            VfsNodeKind::File { content } => {
                let new_end = self.offset + self.write_buffer.len() as u64;

                // Check file size limit
                if new_end > vfs_state.limits.max_file_size {
                    return Err(FsError::trap(ErrorCode::InsufficientMemory));
                }

                // Calculate storage change
                let old_size = content.len() as u64;
                let new_size = new_end.max(old_size);
                let size_change = new_size.saturating_sub(old_size);

                // Check storage limit
                if size_change > 0 {
                    vfs_state.storage_allocation.inc(size_change)?;
                }

                // Resize content if needed
                if new_end as usize > content.len() {
                    content.resize(new_end as usize, 0);
                }

                // Write the data
                let start_offset = self.offset as usize;
                let end_offset = start_offset + self.write_buffer.len();
                content[start_offset..end_offset].copy_from_slice(&self.write_buffer);

                self.offset = new_end;
                self.write_buffer.clear();
                Ok(())
            }
            VfsNodeKind::Directory { .. } => Err(FsError::trap(ErrorCode::IsDirectory)),
        }
    }
}

/// Provide file system access to given state.
pub(crate) trait VfsView {
    /// Provide vfs access.
    fn vfs(&mut self) -> VfsCtxView<'_>;
}

/// Provides resource view to WASI bindings.
pub(crate) struct VfsCtxView<'a> {
    /// Resource tables.
    pub(crate) table: &'a mut ResourceTable,
    /// VFS state.
    pub(crate) vfs_state: &'a mut VfsState,
}

impl<'a> VfsCtxView<'a> {
    /// Get descriptor from resource table.
    fn get_descriptor(&self, res: Resource<Descriptor>) -> FsResult<&VfsDescriptor> {
        // Convert Resource<Descriptor> to Resource<VfsDescriptor> by re-wrapping the same ID
        self.table
            .get::<VfsDescriptor>(&res.cast())
            .map_err(|_| FsError::trap(ErrorCode::BadDescriptor))
    }

    /// Get node.
    fn node(&self, res: Resource<Descriptor>) -> FsResult<SharedVfsNode> {
        Ok(Arc::clone(&self.get_descriptor(res)?.node))
    }

    /// Get node at given path.
    fn node_at(&self, res: Resource<Descriptor>, path: &str) -> FsResult<SharedVfsNode> {
        let node = self.node(res)?;

        let (is_root, directions) = PathTraversal::parse(path, &self.vfs_state.limits)?;

        let start = if is_root {
            Arc::clone(&self.vfs_state.root)
        } else {
            node
        };
        VfsNode::traverse(start, directions)
    }

    /// Create a new file at the given path.
    fn create_node_at(&mut self, res: Resource<Descriptor>, path: &str) -> FsResult<SharedVfsNode> {
        let parent = self.node(res)?;
        let (is_root, directions) = PathTraversal::parse(path, &self.vfs_state.limits)?;

        let mut directions = directions.collect::<Vec<_>>();

        let name = match directions
            .pop()
            .ok_or_else(|| FsError::trap(ErrorCode::Invalid))??
        {
            PathTraversal::Down(segment) => segment,
            _ => return Err(FsError::trap(ErrorCode::Invalid)),
        };

        let parent_start = if is_root {
            Arc::clone(&self.vfs_state.root)
        } else {
            parent
        };
        let parent_node = VfsNode::traverse(parent_start, directions.into_iter())?;

        // Check if file already exists
        let mut parent_guard = parent_node.write().unwrap();
        match &mut parent_guard.kind {
            VfsNodeKind::Directory { children } => {
                if children.contains_key(&name) {
                    return Err(FsError::trap(ErrorCode::Exist));
                }

                let file_node = Arc::new(RwLock::new(VfsNode {
                    kind: VfsNodeKind::File {
                        content: Vec::new(),
                    },
                    parent: Some(Arc::downgrade(&parent_node)),
                }));

                self.vfs_state.inodes_allocation.inc(1)?;

                children.insert(name, Arc::clone(&file_node));
                Ok(file_node)
            }
            VfsNodeKind::File { .. } => Err(FsError::trap(ErrorCode::NotDirectory)),
        }
    }
}

impl<'a> filesystem::types::HostDescriptor for VfsCtxView<'a> {
    fn read_via_stream(
        &mut self,
        self_: Resource<Descriptor>,
        offset: Filesize,
    ) -> FsResult<Resource<InputStream>> {
        match &self.node(self_)?.read().unwrap().kind {
            VfsNodeKind::File { content, .. } => {
                // Get the data to read from the offset
                let offset = offset as usize;
                let data = if offset < content.len() {
                    content[offset..].to_vec()
                } else {
                    Vec::new()
                };

                // Create a memory input pipe with the file contents
                let pipe = MemoryInputPipe::new(data);
                let stream: Box<dyn WasiInputStream> = Box::new(pipe);

                let res = self
                    .table
                    .push(stream)
                    .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;
                Ok(res)
            }
            VfsNodeKind::Directory { .. } => Err(FsError::trap(ErrorCode::IsDirectory)),
        }
    }

    fn write_via_stream(
        &mut self,
        _self_: Resource<Descriptor>,
        _offset: Filesize,
    ) -> FsResult<Resource<OutputStream>> {
        // For now, return ReadOnly until we fully implement stream-based writes
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    fn append_via_stream(
        &mut self,
        _self_: Resource<Descriptor>,
    ) -> FsResult<Resource<OutputStream>> {
        // For now, return ReadOnly until we fully implement stream-based writes
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    async fn advise(
        &mut self,
        _self_: Resource<Descriptor>,
        _offset: Filesize,
        _length: Filesize,
        _advice: Advice,
    ) -> FsResult<()> {
        // No-op for in-memory filesystem
        Ok(())
    }

    async fn sync_data(&mut self, _self_: Resource<Descriptor>) -> FsResult<()> {
        // No-op for in-memory filesystem
        Ok(())
    }

    async fn get_flags(&mut self, self_: Resource<Descriptor>) -> FsResult<DescriptorFlags> {
        let desc = self.get_descriptor(self_)?;
        Ok(desc.flags)
    }

    async fn get_type(&mut self, self_: Resource<Descriptor>) -> FsResult<DescriptorType> {
        Ok(match &self.node(self_)?.read().unwrap().kind {
            VfsNodeKind::File { .. } => DescriptorType::RegularFile,
            VfsNodeKind::Directory { .. } => DescriptorType::Directory,
        })
    }

    async fn set_size(&mut self, self_: Resource<Descriptor>, size: Filesize) -> FsResult<()> {
        // Check if descriptor has write permissions
        let desc = self.get_descriptor(self_)?;
        if !desc.flags.contains(DescriptorFlags::WRITE) {
            return Err(FsError::trap(ErrorCode::BadDescriptor));
        }

        let node = Arc::clone(&desc.node);

        // Check rate limit
        self.vfs_state.check_write_allowed()?;

        let mut node_guard = node.write().unwrap();
        match &mut node_guard.kind {
            VfsNodeKind::File { content } => {
                // Check file size limit
                if size > self.vfs_state.limits.max_file_size {
                    return Err(FsError::trap(ErrorCode::InsufficientMemory));
                }

                let old_size = content.len() as u64;
                let new_size = size;

                if new_size > old_size {
                    // Growing the file - check storage limit
                    let size_increase = new_size - old_size;
                    self.vfs_state.storage_allocation.inc(size_increase)?;
                }

                if new_size < old_size {
                    // Shrinking the file - decrease storage allocation
                    let size_decrease = old_size - new_size;
                    self.vfs_state.storage_allocation.dec(size_decrease)?;
                }

                // Resize the content
                content.resize(size as usize, 0);

                Ok(())
            }
            VfsNodeKind::Directory { .. } => Err(FsError::trap(ErrorCode::IsDirectory)),
        }
    }

    async fn set_times(
        &mut self,
        _self_: Resource<Descriptor>,
        _data_access_timestamp: NewTimestamp,
        _data_modification_timestamp: NewTimestamp,
    ) -> FsResult<()> {
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    async fn read(
        &mut self,
        self_: Resource<Descriptor>,
        length: Filesize,
        offset: Filesize,
    ) -> FsResult<(Vec<u8>, bool)> {
        match &self.node(self_)?.read().unwrap().kind {
            VfsNodeKind::File { content, .. } => {
                let offset = offset as usize;
                let length = length as usize;

                if offset >= content.len() {
                    return Ok((Vec::new(), true));
                }

                let end = std::cmp::min(offset + length, content.len());
                let data = content[offset..end].to_vec();
                let eof = end >= content.len();

                Ok((data, eof))
            }
            VfsNodeKind::Directory { .. } => Err(FsError::trap(ErrorCode::IsDirectory)),
        }
    }

    async fn write(
        &mut self,
        self_: Resource<Descriptor>,
        buffer: Vec<u8>,
        offset: Filesize,
    ) -> FsResult<Filesize> {
        // Check if descriptor has write permissions
        let desc = self.get_descriptor(self_)?;
        if !desc.flags.contains(DescriptorFlags::WRITE) {
            return Err(FsError::trap(ErrorCode::BadDescriptor));
        }

        let node = Arc::clone(&desc.node);

        // Check rate limit
        self.vfs_state.check_write_allowed()?;

        let mut node_guard = node.write().unwrap();
        match &mut node_guard.kind {
            VfsNodeKind::File { content } => {
                let new_end = offset + buffer.len() as u64;

                // Check file size limit
                if new_end > self.vfs_state.limits.max_file_size {
                    return Err(FsError::trap(ErrorCode::InsufficientMemory));
                }

                // Calculate storage change
                let old_size = content.len() as u64;
                let new_size = new_end.max(old_size);
                let size_change = new_size.saturating_sub(old_size);

                // Check storage limit
                if size_change > 0 {
                    self.vfs_state.storage_allocation.inc(size_change)?;
                }

                // Resize content if needed
                if new_end as usize > content.len() {
                    content.resize(new_end as usize, 0);
                }

                // Write the data
                let start_offset = offset as usize;
                let end_offset = start_offset + buffer.len();
                content[start_offset..end_offset].copy_from_slice(&buffer);

                Ok(buffer.len() as u64)
            }
            VfsNodeKind::Directory { .. } => Err(FsError::trap(ErrorCode::IsDirectory)),
        }
    }

    async fn read_directory(
        &mut self,
        self_: Resource<Descriptor>,
    ) -> FsResult<Resource<DirectoryEntryStream>> {
        match &self.node(self_)?.read().unwrap().kind {
            VfsNodeKind::Directory { children, .. } => {
                let mut entries = children
                    .iter()
                    .map(|(name, node)| {
                        let type_ = match &node.read().unwrap().kind {
                            VfsNodeKind::File { .. } => DescriptorType::RegularFile,
                            VfsNodeKind::Directory { .. } => DescriptorType::Directory,
                        };

                        DirectoryEntry {
                            name: name.as_ref().to_owned(),
                            type_,
                        }
                    })
                    .collect::<Vec<_>>();
                entries.sort_by(|e1, e2| e1.name.cmp(&e2.name));

                let stream = VfsDirectoryStream {
                    entries: entries.into_iter().fuse(),
                };
                let res = self
                    .table
                    .push(stream)
                    .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;
                // Convert Resource<VfsDirectoryStream> to Resource<DirectoryEntryStream>
                Ok(res.cast())
            }
            VfsNodeKind::File { .. } => Err(FsError::trap(ErrorCode::NotDirectory)),
        }
    }

    async fn sync(&mut self, _self_: Resource<Descriptor>) -> FsResult<()> {
        // No-op for in-memory filesystem
        Ok(())
    }

    async fn create_directory_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path: String,
    ) -> FsResult<()> {
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    async fn stat(&mut self, self_: Resource<Descriptor>) -> FsResult<DescriptorStat> {
        Ok(self.node(self_)?.read().unwrap().stat())
    }

    async fn stat_at(
        &mut self,
        self_: Resource<Descriptor>,
        _path_flags: PathFlags,
        path: String,
    ) -> FsResult<DescriptorStat> {
        Ok(self.node_at(self_, &path)?.read().unwrap().stat())
    }

    async fn set_times_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path_flags: PathFlags,
        _path: String,
        _data_access_timestamp: NewTimestamp,
        _data_modification_timestamp: NewTimestamp,
    ) -> FsResult<()> {
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    async fn link_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _old_path_flags: PathFlags,
        _old_path: String,
        _new_descriptor: Resource<Descriptor>,
        _new_path: String,
    ) -> FsResult<()> {
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    async fn open_at(
        &mut self,
        self_: Resource<Descriptor>,
        _path_flags: PathFlags,
        path: String,
        open_flags: OpenFlags,
        flags: DescriptorFlags,
    ) -> FsResult<Resource<Descriptor>> {
        // Get the base node we're working from
        let base_node = self.node(self_)?;

        // Parse the path
        let (is_root, directions) = PathTraversal::parse(&path, &self.vfs_state.limits)?;
        let start = if is_root {
            Arc::clone(&self.vfs_state.root)
        } else {
            base_node
        };

        // Handle CREATE flag - create new file if it doesn't exist
        let node = if open_flags.contains(OpenFlags::CREATE) {
            match VfsNode::traverse(Arc::clone(&start), directions) {
                Ok(existing_node) => {
                    // File exists, handle EXCLUSIVE flag
                    if open_flags.contains(OpenFlags::EXCLUSIVE) {
                        return Err(FsError::trap(ErrorCode::Exist));
                    }
                    existing_node
                }
                Err(_) => {
                    // File doesn't exist, create it
                    if flags.intersects(DescriptorFlags::MUTATE_DIRECTORY) {
                        return Err(FsError::trap(ErrorCode::ReadOnly));
                    }

                    // Parse path again for creation
                    let (is_root, directions) =
                        PathTraversal::parse(&path, &self.vfs_state.limits)?;
                    let mut directions = directions.collect::<Vec<_>>();
                    let name = match directions
                        .pop()
                        .ok_or_else(|| FsError::trap(ErrorCode::Invalid))??
                    {
                        PathTraversal::Down(segment) => segment,
                        _ => return Err(FsError::trap(ErrorCode::Invalid)),
                    };

                    let parent_start = if is_root {
                        Arc::clone(&self.vfs_state.root)
                    } else {
                        Arc::clone(&start)
                    };
                    let parent_node = VfsNode::traverse(parent_start, directions.into_iter())?;

                    // Check if file already exists
                    let mut parent_guard = parent_node.write().unwrap();
                    match &mut parent_guard.kind {
                        VfsNodeKind::Directory { children } => {
                            if children.contains_key(&name) {
                                return Err(FsError::trap(ErrorCode::Exist));
                            }

                            let file_node = Arc::new(RwLock::new(VfsNode {
                                kind: VfsNodeKind::File {
                                    content: Vec::new(),
                                },
                                parent: Some(Arc::downgrade(&parent_node)),
                            }));

                            self.vfs_state.inodes_allocation.inc(1)?;
                            children.insert(name, Arc::clone(&file_node));
                            file_node
                        }
                        VfsNodeKind::File { .. } => {
                            return Err(FsError::trap(ErrorCode::NotDirectory));
                        }
                    }
                }
            }
        } else {
            // No CREATE flag, file must exist
            VfsNode::traverse(start, directions)?
        };

        // Handle TRUNCATE flag
        if open_flags.contains(OpenFlags::TRUNCATE) && flags.contains(DescriptorFlags::WRITE) {
            let mut node_guard = node.write().unwrap();
            match &mut node_guard.kind {
                VfsNodeKind::File { content } => {
                    let old_size = content.len() as u64;
                    content.clear();
                    content.shrink_to_fit();

                    self.vfs_state.storage_allocation.dec(old_size)?;
                }
                VfsNodeKind::Directory { .. } => return Err(FsError::trap(ErrorCode::IsDirectory)),
            }
        }

        // Create descriptor
        let new_desc = VfsDescriptor { node, flags };

        let res = self
            .table
            .push(new_desc)
            .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;
        // Convert Resource<VfsDescriptor> to Resource<Descriptor>
        Ok(res.cast())
    }

    async fn readlink_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path: String,
    ) -> FsResult<String> {
        // Symlinks not supported
        Err(FsError::trap(ErrorCode::Unsupported))
    }

    async fn remove_directory_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path: String,
    ) -> FsResult<()> {
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    async fn rename_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _old_path: String,
        _new_descriptor: Resource<Descriptor>,
        _new_path: String,
    ) -> FsResult<()> {
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    async fn symlink_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _old_path: String,
        _new_path: String,
    ) -> FsResult<()> {
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    async fn unlink_file_at(
        &mut self,
        _self_: Resource<Descriptor>,
        _path: String,
    ) -> FsResult<()> {
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    async fn is_same_object(
        &mut self,
        self_: Resource<Descriptor>,
        other: Resource<Descriptor>,
    ) -> wasmtime::Result<bool> {
        let desc1 = self.get_descriptor(self_)?;
        let desc2 = self.get_descriptor(other)?;
        Ok(Arc::ptr_eq(&desc1.node, &desc2.node))
    }

    async fn metadata_hash(&mut self, self_: Resource<Descriptor>) -> FsResult<MetadataHashValue> {
        Ok(self
            .node(self_)?
            .read()
            .unwrap()
            .metadata_hash(&self.vfs_state.metadata_hash_key))
    }

    async fn metadata_hash_at(
        &mut self,
        self_: Resource<Descriptor>,
        _path_flags: PathFlags,
        path: String,
    ) -> FsResult<MetadataHashValue> {
        Ok(self
            .node_at(self_, &path)?
            .read()
            .unwrap()
            .metadata_hash(&self.vfs_state.metadata_hash_key))
    }

    fn drop(&mut self, rep: Resource<Descriptor>) -> wasmtime::Result<()> {
        // Convert Resource<Descriptor> to Resource<VfsDescriptor> by re-wrapping the same ID
        self.table.delete::<VfsDescriptor>(rep.cast())?;
        Ok(())
    }
}

impl<'a> filesystem::types::HostDirectoryEntryStream for VfsCtxView<'a> {
    async fn read_directory_entry(
        &mut self,
        self_: Resource<DirectoryEntryStream>,
    ) -> FsResult<Option<DirectoryEntry>> {
        // Convert Resource<DirectoryEntryStream> to Resource<VfsDirectoryStream> by re-wrapping the same ID
        let stream = self
            .table
            .get_mut::<VfsDirectoryStream>(&self_.cast())
            .map_err(|_| FsError::trap(ErrorCode::BadDescriptor))?;

        Ok(stream.entries.next())
    }

    fn drop(&mut self, rep: Resource<DirectoryEntryStream>) -> wasmtime::Result<()> {
        // Convert Resource<DirectoryEntryStream> to Resource<VfsDirectoryStream> by re-wrapping the same ID
        self.table.delete::<VfsDirectoryStream>(rep.cast())?;
        Ok(())
    }
}

impl<'a> filesystem::types::Host for VfsCtxView<'a> {
    fn filesystem_error_code(
        &mut self,
        _err: Resource<Error>,
    ) -> wasmtime::Result<Option<ErrorCode>> {
        // Not used in our implementation
        Ok(None)
    }

    fn convert_error_code(&mut self, err: FsError) -> wasmtime::Result<ErrorCode> {
        // Extract error code from FsError
        if let Some(code) = err.downcast_ref() {
            Ok(*code)
        } else {
            Ok(ErrorCode::Io)
        }
    }
}

impl<'a> filesystem::preopens::Host for VfsCtxView<'a> {
    fn get_directories(&mut self) -> wasmtime::Result<Vec<(Resource<Descriptor>, String)>> {
        // Create new preopen descriptor for root with read-write access
        let desc = VfsDescriptor {
            node: Arc::clone(&self.vfs_state.root),
            flags: DescriptorFlags::READ,
        };

        let res = self.table.push(desc)?;
        Ok(vec![(res.cast(), "/".to_string())])
    }
}

/// Extension trait for [`Resource`].
trait ResourceExt {
    /// Resource type.
    type T;

    /// Cast resource type.
    fn cast<U>(self) -> Resource<U>
    where
        U: 'static;
}

impl<T> ResourceExt for Resource<T>
where
    T: 'static,
{
    type T = T;

    fn cast<U>(self) -> Resource<U>
    where
        U: 'static,
    {
        if self.owned() {
            Resource::new_own(self.rep())
        } else {
            Resource::new_borrow(self.rep())
        }
    }
}

/// Marker struct to tell linker that we provide a filesystem.
pub(crate) struct HasFs;

impl HasData for HasFs {
    type Data<'a> = VfsCtxView<'a>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::component::Resource;
    use wasmtime_wasi::ResourceTable;

    fn create_rw_root_descriptor(vfs_ctx: &mut VfsCtxView<'_>) -> Resource<Descriptor> {
        let root_desc = VfsDescriptor {
            node: Arc::clone(&vfs_ctx.vfs_state.root),
            flags: DescriptorFlags::READ | DescriptorFlags::WRITE,
        };
        let root_resource = vfs_ctx.table.push(root_desc).unwrap();
        root_resource.cast()
    }

    #[tokio::test]
    async fn truncate_releases_storage_allocation() {
        use filesystem::types::HostDescriptor;

        let limits = VfsLimits::default();
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        let root_descriptor = create_rw_root_descriptor(&mut vfs_ctx);
        let file_descriptor = vfs_ctx
            .open_at(
                root_descriptor,
                PathFlags::empty(),
                "truncate_me.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("create file");

        // Write some data to consume storage.
        let payload = b"0123456789ABCDEF".to_vec(); // 16 bytes
        vfs_ctx
            .write(file_descriptor, payload, 0)
            .await
            .expect("write data");
        let storage_after_write = vfs_ctx.vfs_state.storage_allocation.get();
        assert_eq!(storage_after_write, 16);

        // Reopen with TRUNCATE to clear contents and ensure accounting shrinks.
        let root_descriptor = create_rw_root_descriptor(&mut vfs_ctx);
        vfs_ctx
            .open_at(
                root_descriptor,
                PathFlags::empty(),
                "truncate_me.txt".to_string(),
                OpenFlags::TRUNCATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("truncate file");

        assert_eq!(vfs_ctx.vfs_state.storage_allocation.get(), 0);
    }

    #[tokio::test]
    async fn opening_descriptor_does_not_consume_inodes() {
        use filesystem::types::HostDescriptor;

        let limits = VfsLimits::default();
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        let initial_inodes = vfs_ctx.vfs_state.inodes_allocation.get();

        // Create a file; this should consume exactly one inode.
        let root_descriptor = create_rw_root_descriptor(&mut vfs_ctx);
        vfs_ctx
            .open_at(
                root_descriptor,
                PathFlags::empty(),
                "inode_count.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("create file");

        let after_create = vfs_ctx.vfs_state.inodes_allocation.get();
        assert_eq!(after_create, initial_inodes + 1);

        // Opening existing files must not consume additional inode budget.
        let root_descriptor = create_rw_root_descriptor(&mut vfs_ctx);
        vfs_ctx
            .open_at(
                root_descriptor,
                PathFlags::empty(),
                "inode_count.txt".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("open existing file");

        let after_reopen = vfs_ctx.vfs_state.inodes_allocation.get();
        assert_eq!(after_reopen, after_create);
    }

    #[tokio::test]
    async fn test_vfs_write_functionality() {
        use filesystem::types::HostDescriptor;

        // Create VFS with default limits
        let limits = VfsLimits::default();
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        // Create VfsCtxView
        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        // Helper function to create a new root descriptor
        let create_root_descriptor = |vfs_ctx: &mut VfsCtxView<'_>| -> Resource<Descriptor> {
            let root_desc = VfsDescriptor {
                node: Arc::clone(&vfs_ctx.vfs_state.root),
                flags: DescriptorFlags::READ | DescriptorFlags::WRITE,
            };
            let root_resource = vfs_ctx.table.push(root_desc).unwrap();
            root_resource.cast()
        };

        // Test 1: Create a new file using open_at with CREATE flag
        let file_path = "test_file.txt".to_string();
        let open_flags = OpenFlags::CREATE;
        let file_flags = DescriptorFlags::READ | DescriptorFlags::WRITE;

        let root_descriptor1 = create_root_descriptor(&mut vfs_ctx);
        let file_descriptor = vfs_ctx
            .open_at(
                root_descriptor1,
                PathFlags::empty(),
                file_path.clone(),
                open_flags,
                file_flags,
            )
            .await
            .expect("Failed to create file");

        // Create multiple file descriptors since Resources are moved when used
        let file_descriptor_write1 = file_descriptor;
        let root_descriptor2 = create_root_descriptor(&mut vfs_ctx);
        let file_descriptor_read1 = vfs_ctx
            .open_at(
                root_descriptor2,
                PathFlags::empty(),
                file_path.clone(),
                open_flags,
                file_flags,
            )
            .await
            .expect("Failed to open file for reading");

        // Test 2: Write data to the file
        let test_data = b"Hello, VFS World!".to_vec();
        let bytes_written = vfs_ctx
            .write(file_descriptor_write1, test_data.clone(), 0)
            .await
            .expect("Failed to write data");

        assert_eq!(bytes_written, test_data.len() as u64);

        // Test 3: Read the data back
        let (read_data, eof) = vfs_ctx
            .read(file_descriptor_read1, test_data.len() as u64, 0)
            .await
            .expect("Failed to read data");

        assert_eq!(read_data, test_data);
        assert!(eof);

        // Create new descriptors for append test
        let root_descriptor3 = create_root_descriptor(&mut vfs_ctx);
        let file_descriptor_write2 = vfs_ctx
            .open_at(
                root_descriptor3,
                PathFlags::empty(),
                file_path.clone(),
                open_flags,
                file_flags,
            )
            .await
            .expect("Failed to open file for appending");
        let root_descriptor4 = create_root_descriptor(&mut vfs_ctx);
        let file_descriptor_read2 = vfs_ctx
            .open_at(
                root_descriptor4,
                PathFlags::empty(),
                file_path.clone(),
                open_flags,
                file_flags,
            )
            .await
            .expect("Failed to open file for reading");

        // Test 4: Append more data
        let append_data = b" More content!".to_vec();
        let bytes_written = vfs_ctx
            .write(
                file_descriptor_write2,
                append_data.clone(),
                test_data.len() as u64,
            )
            .await
            .expect("Failed to append data");

        assert_eq!(bytes_written, append_data.len() as u64);

        // Test 5: Read all data
        let total_len = test_data.len() + append_data.len();
        let (all_data, eof) = vfs_ctx
            .read(file_descriptor_read2, total_len as u64, 0)
            .await
            .expect("Failed to read all data");

        let expected_data = [test_data, append_data].concat();
        assert_eq!(all_data, expected_data);
        assert!(eof);

        // Test 6: Test file truncation with set_size
        let root_descriptor5 = create_root_descriptor(&mut vfs_ctx);
        let file_descriptor_truncate = vfs_ctx
            .open_at(
                root_descriptor5,
                PathFlags::empty(),
                file_path.clone(),
                open_flags,
                file_flags,
            )
            .await
            .expect("Failed to open file for truncation");
        let root_descriptor6 = create_root_descriptor(&mut vfs_ctx);
        let file_descriptor_read_truncated = vfs_ctx
            .open_at(
                root_descriptor6,
                PathFlags::empty(),
                file_path.clone(),
                open_flags,
                file_flags,
            )
            .await
            .expect("Failed to open file for reading after truncation");

        vfs_ctx
            .set_size(file_descriptor_truncate, 5)
            .await
            .expect("Failed to truncate file");

        let (truncated_data, eof) = vfs_ctx
            .read(file_descriptor_read_truncated, 100, 0)
            .await
            .expect("Failed to read truncated data");

        assert_eq!(truncated_data, b"Hello");
        assert!(eof);

        // Test 7: Test CREATE with TRUNCATE flag
        let root_descriptor7 = create_root_descriptor(&mut vfs_ctx);
        let truncate_flags = OpenFlags::CREATE | OpenFlags::TRUNCATE;
        let file_descriptor2 = vfs_ctx
            .open_at(
                root_descriptor7,
                PathFlags::empty(),
                file_path.clone(),
                truncate_flags,
                file_flags,
            )
            .await
            .expect("Failed to open with truncate");

        // File should be empty after TRUNCATE
        let (empty_data, eof) = vfs_ctx
            .read(file_descriptor2, 100, 0)
            .await
            .expect("Failed to read truncated file");

        assert!(empty_data.is_empty());
        assert!(eof);

        println!("All VFS write tests passed!");
    }

    #[tokio::test]
    async fn test_vfs_write_permissions() {
        use filesystem::types::HostDescriptor;

        let limits = VfsLimits::default();
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        // Create root descriptor with READ-ONLY permissions
        let root_desc = VfsDescriptor {
            node: Arc::clone(&vfs_ctx.vfs_state.root),
            flags: DescriptorFlags::READ, // No WRITE flag
        };
        let root_resource = vfs_ctx.table.push(root_desc).unwrap();
        let root_descriptor: Resource<Descriptor> = root_resource.cast();

        // Try to create a file with read-only descriptor
        let file_path = "readonly_test.txt".to_string();
        let open_flags = OpenFlags::CREATE;
        let file_flags = DescriptorFlags::READ; // No WRITE flag

        let file_descriptor = vfs_ctx
            .open_at(
                root_descriptor,
                PathFlags::empty(),
                file_path,
                open_flags,
                file_flags,
            )
            .await
            .expect("Failed to create file");

        // Try to write to read-only file - should fail
        let test_data = b"This should fail".to_vec();
        let write_result = vfs_ctx.write(file_descriptor, test_data, 0).await;

        match write_result {
            Err(err) => {
                // Should get BadDescriptor error for lack of write permissions
                println!("Expected error for read-only file: {:?}", err);
            }
            Ok(_) => panic!("Write should have failed on read-only file"),
        }

        println!("VFS permission tests passed!");
    }

    #[tokio::test]
    async fn test_vfs_limits() {
        use filesystem::types::HostDescriptor;

        // Create VFS with very small limits for testing
        let limits = VfsLimits {
            inodes: 10,
            max_path_length: 255,
            max_path_segment_size: 50,
            max_storage_bytes: 100,     // Very small storage limit
            max_file_size: 50,          // Small file size limit
            max_write_ops_per_sec: 100, // High enough to not interfere with storage/file size tests
        };
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        // Helper function to create a new root descriptor
        let create_root_descriptor = |vfs_ctx: &mut VfsCtxView<'_>| -> Resource<Descriptor> {
            let root_desc = VfsDescriptor {
                node: Arc::clone(&vfs_ctx.vfs_state.root),
                flags: DescriptorFlags::READ | DescriptorFlags::WRITE,
            };
            let root_resource = vfs_ctx.table.push(root_desc).unwrap();
            root_resource.cast()
        };

        // Test 1: Validate max_file_size limit
        let root_descriptor1 = create_root_descriptor(&mut vfs_ctx);
        let file_descriptor = vfs_ctx
            .open_at(
                root_descriptor1,
                PathFlags::empty(),
                "test_file_size_limit.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file");

        // Try to write data exceeding file size limit
        let large_data = vec![b'X'; 100]; // Larger than max_file_size (50)
        let write_result = vfs_ctx.write(file_descriptor, large_data, 0).await;

        match write_result {
            Err(err) => {
                println!("Expected error for file size limit: {:?}", err);
            }
            Ok(_) => panic!("Write should have failed due to file size limit"),
        }

        // Test 2: Validate max_storage_bytes limit with multiple files
        // Each file is under max_file_size (50 bytes), but together they exceed max_storage_bytes (100 bytes)

        // Create first file with 40 bytes (under limit)
        let root_descriptor2 = create_root_descriptor(&mut vfs_ctx);
        let file1_descriptor = vfs_ctx
            .open_at(
                root_descriptor2,
                PathFlags::empty(),
                "file1.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file1");

        let file1_data = vec![b'A'; 40];
        vfs_ctx
            .write(file1_descriptor, file1_data, 0)
            .await
            .expect("Failed to write to file1");

        // Create second file with 40 bytes (under limit)
        let root_descriptor3 = create_root_descriptor(&mut vfs_ctx);
        let file2_descriptor = vfs_ctx
            .open_at(
                root_descriptor3,
                PathFlags::empty(),
                "file2.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file2");

        let file2_data = vec![b'B'; 40];
        vfs_ctx
            .write(file2_descriptor, file2_data, 0)
            .await
            .expect("Failed to write to file2");

        // Try to create third file with 40 bytes - this should exceed max_storage_bytes (100)
        // 40 + 40 + 40 = 120 bytes > 100 bytes limit
        let root_descriptor4 = create_root_descriptor(&mut vfs_ctx);
        let file3_descriptor = vfs_ctx
            .open_at(
                root_descriptor4,
                PathFlags::empty(),
                "file3.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file3");

        let file3_data = vec![b'C'; 40];
        let write_result = vfs_ctx.write(file3_descriptor, file3_data, 0).await;

        match write_result {
            Err(err) => {
                println!("Expected error for storage bytes limit: {:?}", err);
            }
            Ok(_) => panic!("Write should have failed due to storage bytes limit"),
        }

        println!("VFS limits tests passed!");
    }

    #[tokio::test]
    async fn test_vfs_write_rate_limit() {
        use filesystem::types::HostDescriptor;

        // Create VFS with a very low write rate limit
        let limits = VfsLimits {
            inodes: 10,
            max_path_length: 255,
            max_path_segment_size: 50,
            max_storage_bytes: 1000,
            max_file_size: 100,
            max_write_ops_per_sec: 2, // Very low rate limit
        };
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        // Helper function to create a new root descriptor
        let create_root_descriptor = |vfs_ctx: &mut VfsCtxView<'_>| -> Resource<Descriptor> {
            let root_desc = VfsDescriptor {
                node: Arc::clone(&vfs_ctx.vfs_state.root),
                flags: DescriptorFlags::READ | DescriptorFlags::WRITE,
            };
            let root_resource = vfs_ctx.table.push(root_desc).unwrap();
            root_resource.cast()
        };

        // Create a test file
        let root_descriptor = create_root_descriptor(&mut vfs_ctx);
        let _file_descriptor = vfs_ctx
            .open_at(
                root_descriptor,
                PathFlags::empty(),
                "rate_limit_test.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file");

        // First two writes should succeed (within rate limit)
        for i in 0..2 {
            let root_descriptor = create_root_descriptor(&mut vfs_ctx);
            let file_descriptor = vfs_ctx
                .open_at(
                    root_descriptor,
                    PathFlags::empty(),
                    format!("file{}.txt", i),
                    OpenFlags::CREATE,
                    DescriptorFlags::READ | DescriptorFlags::WRITE,
                )
                .await
                .expect("Failed to create file");

            let data = vec![b'X'; 10];
            vfs_ctx
                .write(file_descriptor, data, 0)
                .await
                .expect(&format!("Write {} should succeed", i));
        }

        // Third write should fail due to rate limit
        let root_descriptor3 = create_root_descriptor(&mut vfs_ctx);
        let file_descriptor3 = vfs_ctx
            .open_at(
                root_descriptor3,
                PathFlags::empty(),
                "file_rate_limit.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file");

        let data = vec![b'X'; 10];
        let write_result = vfs_ctx.write(file_descriptor3, data, 0).await;

        match write_result {
            Err(err) => {
                println!("Expected error for rate limit: {:?}", err);
            }
            Ok(_) => panic!("Write should have failed due to rate limit"),
        }

        println!("VFS rate limit test passed!");
    }

    #[tokio::test]
    async fn test_vfs_exclusive_flag() {
        use filesystem::types::HostDescriptor;

        let limits = VfsLimits::default();
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        // Helper function to create a new root descriptor
        let create_root_descriptor = |vfs_ctx: &mut VfsCtxView<'_>| -> Resource<Descriptor> {
            let root_desc = VfsDescriptor {
                node: Arc::clone(&vfs_ctx.vfs_state.root),
                flags: DescriptorFlags::READ | DescriptorFlags::WRITE,
            };
            let root_resource = vfs_ctx.table.push(root_desc).unwrap();
            root_resource.cast()
        };

        let file_path = "exclusive_test.txt".to_string();
        let file_flags = DescriptorFlags::READ | DescriptorFlags::WRITE;

        // Test 1: Creating a file with CREATE | EXCLUSIVE should succeed when file doesn't exist
        let root_descriptor1 = create_root_descriptor(&mut vfs_ctx);
        let exclusive_flags = OpenFlags::CREATE | OpenFlags::EXCLUSIVE;

        let file_descriptor = vfs_ctx
            .open_at(
                root_descriptor1,
                PathFlags::empty(),
                file_path.clone(),
                exclusive_flags,
                file_flags,
            )
            .await
            .expect("Creating file with CREATE | EXCLUSIVE should succeed when file doesn't exist");

        // Verify we can write to the file
        let test_data = b"Exclusive file content".to_vec();
        vfs_ctx
            .write(file_descriptor, test_data.clone(), 0)
            .await
            .expect("Should be able to write to exclusively created file");

        println!("Test 1 passed: File created successfully with CREATE | EXCLUSIVE");

        // Test 2: Opening an existing file with CREATE | EXCLUSIVE should fail with ErrorCode::Exist
        let root_descriptor2 = create_root_descriptor(&mut vfs_ctx);
        let open_result = vfs_ctx
            .open_at(
                root_descriptor2,
                PathFlags::empty(),
                file_path.clone(),
                exclusive_flags,
                file_flags,
            )
            .await;

        match open_result {
            Err(err) => {
                // Verify we get the Exist error code
                println!(
                    "Expected error for existing file with EXCLUSIVE flag: {:?}",
                    err
                );
                // The error should be ErrorCode::Exist
            }
            Ok(_) => panic!(
                "Opening existing file with CREATE | EXCLUSIVE should have failed with ErrorCode::Exist"
            ),
        }

        println!("Test 2 passed: Opening existing file with CREATE | EXCLUSIVE correctly fails");

        // Test 3: Verify that opening with just CREATE (no EXCLUSIVE) succeeds for existing file
        let root_descriptor3 = create_root_descriptor(&mut vfs_ctx);
        let create_only_flags = OpenFlags::CREATE;

        let file_descriptor_reopen = vfs_ctx
            .open_at(
                root_descriptor3,
                PathFlags::empty(),
                file_path.clone(),
                create_only_flags,
                file_flags,
            )
            .await
            .expect("Opening existing file with CREATE (no EXCLUSIVE) should succeed");

        // Verify the file still contains the original data
        let (read_data, eof) = vfs_ctx
            .read(file_descriptor_reopen, 100, 0)
            .await
            .expect("Should be able to read from reopened file");

        assert_eq!(read_data, test_data);
        assert!(eof);

        println!("Test 3 passed: Opening existing file with CREATE (no EXCLUSIVE) succeeds");

        // Test 4: Create a new file in a subdirectory with EXCLUSIVE flag
        // First, we need to verify EXCLUSIVE works with paths, not just simple filenames
        let subdir_path = "subdir/exclusive_file.txt".to_string();
        let root_descriptor4 = create_root_descriptor(&mut vfs_ctx);

        // This should fail because the subdirectory doesn't exist
        let subdir_result = vfs_ctx
            .open_at(
                root_descriptor4,
                PathFlags::empty(),
                subdir_path.clone(),
                exclusive_flags,
                file_flags,
            )
            .await;

        match subdir_result {
            Err(err) => {
                println!(
                    "Expected error for non-existent parent directory: {:?}",
                    err
                );
            }
            Ok(_) => panic!("Creating file in non-existent directory should fail"),
        }

        println!(
            "Test 4 passed: EXCLUSIVE flag properly fails when parent directory doesn't exist"
        );

        println!("All EXCLUSIVE flag tests passed!");
    }
}
