//! Virtual File System.
//!
//! This provides a very crude, read-only in-mem virtual file system for the WASM guests.
//!
//! The data gets populated via a TAR container.
//!
//! While this implementation has rather limited functionality, it is sufficient to get a Python guest interpreter
//! running.

use std::{
    collections::{HashMap, hash_map::Entry},
    hash::Hash,
    io::{Cursor, Read},
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use rand::Rng;
use siphasher::sip128::{Hasher128, SipHasher24};
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

use crate::{
    error::LimitExceeded,
    limiter::Limiter,
    state::WasmStateImpl,
    vfs::{
        limits::VfsLimits,
        path::{PathSegment, PathTraversal},
    },
};

pub(crate) mod limits;
mod path;

impl VfsView for WasmStateImpl {
    fn vfs(&mut self) -> VfsCtxView<'_> {
        VfsCtxView {
            table: &mut self.resource_table,
            vfs_state: &mut self.vfs_state,
        }
    }
}

/// Shared version of [`VfsNode`].
type SharedVfsNode = Arc<RwLock<VfsNode>>;

/// A kind node in the virtual filesystem tree.
#[derive(Debug)]
struct VfsNode {
    /// Which kind of data is stored in this node.
    kind: VfsNodeKind,

    /// Pointer to parent node.
    parent: Option<Weak<RwLock<Self>>>,
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

    /// Decrease allocation by given amount.
    fn dec(&self, n: u64) {
        self.n.fetch_sub(n, Ordering::SeqCst);
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

    /// Storage limiter.
    limiter: Limiter,
}

impl VfsState {
    /// Create a new empty VFS.
    pub(crate) fn new(limits: VfsLimits, limiter: Limiter) -> Self {
        let inodes_allocation = Allocation::new("inodes", limits.inodes);

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
            limiter,
        }
    }

    /// Populate the VFS from a tar archive.
    pub(crate) fn populate_from_tar(&mut self, tar_data: &[u8]) -> Result<(), std::io::Error> {
        let size_pre = self.limiter.size();
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
                    let expected_size = entry.size() as usize;
                    self.limiter.grow(expected_size)?;
                    let mut content = Vec::with_capacity(expected_size);
                    entry.read_to_end(&mut content)?;
                    assert_eq!(expected_size, content.len());
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
            self.limiter.grow(name.len())?;
            self.limiter.grow(std::mem::size_of_val(&child))?;

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
            self.limiter.size() - size_pre,
            self.inodes_allocation.get()
        );

        Ok(())
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
        self.get_node_from_start(path, node)
    }

    /// Get node at given path from given starting node.
    fn get_node_from_start(&self, path: &str, node: SharedVfsNode) -> FsResult<SharedVfsNode> {
        let (is_root, directions) = PathTraversal::parse(path, &self.vfs_state.limits)?;

        let start = if is_root {
            Arc::clone(&self.vfs_state.root)
        } else {
            node
        };
        VfsNode::traverse(start, directions)
    }

    /// Get the parent node and base name for a given path.
    fn parent_node_and_name(
        &self,
        node: SharedVfsNode,
        path: &str,
    ) -> FsResult<(SharedVfsNode, PathSegment)> {
        let (is_root, directions) = PathTraversal::parse(path, &self.vfs_state.limits)?;
        let mut directions = directions.collect::<Vec<_>>();

        let start = if is_root {
            Arc::clone(&self.vfs_state.root)
        } else {
            node
        };

        let name = match directions
            .pop()
            .ok_or_else(|| FsError::trap(ErrorCode::Invalid))?
        {
            Ok(PathTraversal::Down(segment)) => segment,
            _other @ (Ok(PathTraversal::Stay) | Ok(PathTraversal::Up)) => {
                return Err(FsError::trap(ErrorCode::InvalidSeek));
            }
            Err(_) => {
                return Err(FsError::trap(ErrorCode::Invalid));
            }
        };

        let parent = VfsNode::traverse(start, directions.into_iter())?;

        Ok((parent, name))
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
        Err(FsError::trap(ErrorCode::ReadOnly))
    }

    fn append_via_stream(
        &mut self,
        _self_: Resource<Descriptor>,
    ) -> FsResult<Resource<OutputStream>> {
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

    async fn set_size(&mut self, _self_: Resource<Descriptor>, _size: Filesize) -> FsResult<()> {
        Err(FsError::trap(ErrorCode::ReadOnly))
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
        // Per POSIX: "If nbyte is zero and the file is a regular file, the write()
        // function may detect and return errors as described below. In the absence
        // of errors, or if error detection is not performed, the write() function
        // shall return zero and have no other results."
        if buffer.is_empty() {
            // Still check for valid descriptor and permissions
            let desc = self.get_descriptor(self_)?;
            if !desc.flags.contains(DescriptorFlags::WRITE) {
                // Per POSIX [EBADF]: "The fildes argument is not a valid file
                // descriptor open for writing."
                return Err(FsError::trap(ErrorCode::BadDescriptor));
            }
            // Check it's not a directory
            let node = Arc::clone(&desc.node);
            let guard = node.read().unwrap();
            if matches!(guard.kind, VfsNodeKind::Directory { .. }) {
                // Per POSIX [EISDIR]: Writing to a directory is not allowed
                return Err(FsError::trap(ErrorCode::IsDirectory));
            }
            return Ok(0);
        }

        let desc = self.get_descriptor(self_)?;

        // Per POSIX [EBADF]: "The fildes argument is not a valid file descriptor
        // open for writing."
        if !desc.flags.contains(DescriptorFlags::WRITE) {
            return Err(FsError::trap(ErrorCode::BadDescriptor));
        }

        let node = Arc::clone(&desc.node);
        let offset = offset as usize;
        let buffer_len = buffer.len();

        // Calculate how much the file needs to grow
        let mut guard = node.write().unwrap();
        match &mut guard.kind {
            VfsNodeKind::File { content } => {
                let current_len = content.len();
                let write_end = offset.saturating_add(buffer_len);

                // Calculate how much new memory we need
                // Per POSIX: "On a regular file, if the position of the last byte
                // written is greater than or equal to the length of the file, the
                // length of the file shall be set to this position plus one."
                let growth = if write_end > current_len {
                    write_end.saturating_sub(current_len)
                } else {
                    0
                };

                // Account for memory growth before modifying content
                if growth > 0 {
                    // Per POSIX [ENOSPC]: "There was no free space remaining on
                    // the device containing the file."
                    // We map this to InsufficientMemory for our in-memory VFS.
                    self.vfs_state
                        .limiter
                        .grow(growth)
                        .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;
                }

                // Extend the file with zeros if writing past the current end
                // Per POSIX: Writing past EOF extends the file
                if offset > current_len {
                    content.resize(offset, 0);
                }

                // Ensure we have enough capacity for the write
                if write_end > content.len() {
                    content.resize(write_end, 0);
                }

                // Per POSIX: "The write() function shall attempt to write nbyte
                // bytes from the buffer pointed to by buf to the file"
                content[offset..write_end].copy_from_slice(&buffer);

                // Per POSIX: "Upon successful completion, these functions shall
                // return the number of bytes actually written to the file"
                Ok(buffer_len as Filesize)
            }
            VfsNodeKind::Directory { .. } => {
                // Per POSIX [EISDIR]: Writing to a directory is not allowed
                Err(FsError::trap(ErrorCode::IsDirectory))
            }
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
        self_: Resource<Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let desc = self.get_descriptor(self_)?;
        if !desc.flags.contains(DescriptorFlags::MUTATE_DIRECTORY) {
            return Err(FsError::trap(ErrorCode::ReadOnly));
        }

        let (parent_node, name) = self.parent_node_and_name(Arc::clone(&desc.node), &path)?;

        let new_dir = Arc::new(RwLock::new(VfsNode {
            kind: VfsNodeKind::Directory {
                children: HashMap::new(),
            },
            parent: Some(Arc::downgrade(&parent_node)),
        }));

        self.vfs_state
            .inodes_allocation
            .inc(1)
            .map_err(FsError::trap)?;
        self.vfs_state
            .limiter
            .grow(name.len())
            .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;
        self.vfs_state
            .limiter
            .grow(std::mem::size_of_val(&new_dir))
            .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;

        match &mut parent_node.write().unwrap().kind {
            VfsNodeKind::File { .. } => {
                return Err(FsError::trap(ErrorCode::NotDirectory));
            }
            VfsNodeKind::Directory { children } => match children.entry(name) {
                Entry::Vacant(entry) => {
                    entry.insert(new_dir);
                }
                Entry::Occupied(_) => {
                    return Err(FsError::trap(ErrorCode::Exist));
                }
            },
        }

        Ok(())
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
        let base_desc = self.get_descriptor(self_)?;
        let base_node = Arc::clone(&base_desc.node);
        let base_flags = base_desc.flags;

        let wants_create = open_flags.contains(OpenFlags::CREATE);
        let wants_exclusive = open_flags.contains(OpenFlags::EXCLUSIVE);
        let wants_directory = open_flags.contains(OpenFlags::DIRECTORY);
        let wants_truncate = open_flags.contains(OpenFlags::TRUNCATE);

        // Per POSIX: O_CREAT only creates regular files, not directories.
        // https://github.com/WebAssembly/WASI/blob/184b0c0e9fd437e5e5601d6e327a28feddbbd7f7/proposals/filesystem/wit/types.wit#L145-L146
        // "If O_CREAT and O_DIRECTORY are set and the requested access mode is neither
        // O_WRONLY nor O_RDWR, the result is unspecified."
        // We choose to disallow this combination entirely.
        if wants_create && wants_directory {
            return Err(FsError::trap(ErrorCode::Invalid));
        }

        // Try to resolve the path to an existing node
        let existing = self.get_node_from_start(&path, Arc::clone(&base_node));

        // Per POSIX O_DIRECTORY: "If path resolves to a non-directory file, fail and set errno to [ENOTDIR]."
        let node = match existing {
            Ok(node) if wants_directory => {
                let guard = node.read().unwrap();
                if !matches!(guard.kind, VfsNodeKind::Directory { .. }) {
                    return Err(FsError::trap(ErrorCode::NotDirectory));
                }
                drop(guard);
                node
            }
            Ok(node) if wants_exclusive => {
                if wants_create {
                    // Per POSIX: "O_CREAT and O_EXCL are set, open() shall fail if the file exists"
                    return Err(FsError::trap(ErrorCode::Exist));
                }
                node
            }
            Ok(node) if wants_truncate => {
                let mut guard = node.write().unwrap();
                match &mut guard.kind {
                    VfsNodeKind::File { content } => {
                        // Per POSIX: "The result of using O_TRUNC without either O_RDWR
                        // or O_WRONLY is undefined." We allow it but it's a no-op
                        // unless write permission is granted.
                        if flags.contains(DescriptorFlags::WRITE) {
                            content.clear();
                        }
                    }
                    VfsNodeKind::Directory { .. } => {
                        // Per POSIX: O_TRUNC "shall have no effect on... terminal device files.
                        // Its effect on other file types is implementation-defined."
                        // POSIX also says [EISDIR] for "The named file is a directory and
                        // oflag includes O_WRONLY or O_RDWR"
                        return Err(FsError::trap(ErrorCode::IsDirectory));
                    }
                }
                drop(guard);
                node
            }
            Ok(node) => node,
            Err(_) => {
                if !wants_create {
                    // Per POSIX [ENOENT]: "O_CREAT is not set and a component of path does
                    // not name an existing file"
                    return Err(FsError::trap(ErrorCode::NoEntry));
                }

                if !base_flags.contains(DescriptorFlags::MUTATE_DIRECTORY) {
                    return Err(FsError::trap(ErrorCode::ReadOnly));
                }

                let (parent_node, name) =
                    self.parent_node_and_name(Arc::clone(&base_node), &path)?;

                let new_file = Arc::new(RwLock::new(VfsNode {
                    kind: VfsNodeKind::File {
                        content: Vec::new(),
                    },
                    parent: Some(Arc::downgrade(&parent_node)),
                }));

                // Account for resource usage
                self.vfs_state
                    .inodes_allocation
                    .inc(1)
                    .map_err(FsError::trap)?;
                let growth = name.len() + std::mem::size_of_val(&new_file);
                self.vfs_state.limiter.grow(growth).map_err(|_| {
                    // Rollback inode allocation since we failed to account for
                    // the new file's name and node size
                    self.vfs_state.inodes_allocation.dec(1);
                    FsError::trap(ErrorCode::InsufficientMemory)
                })?;

                // Insert the new file into the parent directory
                match &mut parent_node.write().unwrap().kind {
                    VfsNodeKind::File { .. } => {
                        // Parent is a file, not a directory
                        // Per POSIX [ENOTDIR]: "A component of the path prefix names an
                        // existing file that is neither a directory nor a symbolic link to a directory"
                        return Err(FsError::trap(ErrorCode::NotDirectory));
                    }
                    VfsNodeKind::Directory { children } => {
                        match children.entry(name) {
                            Entry::Vacant(entry) => {
                                entry.insert(Arc::clone(&new_file));
                            }
                            Entry::Occupied(_) => {
                                // Race condition: file was created between our check and insert
                                // Per POSIX with O_EXCL, this should return EEXIST
                                // Without O_EXCL, we could open the existing file, but for
                                // simplicity we return EEXIST since this is a race condition
                                return Err(FsError::trap(ErrorCode::Exist));
                            }
                        }
                    }
                }

                new_file
            }
        };

        let res = self
            .table
            .push(VfsDescriptor { node, flags })
            .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;
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
            flags: DescriptorFlags::READ
                | DescriptorFlags::MUTATE_DIRECTORY
                | DescriptorFlags::WRITE,
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
    use std::sync::Arc;

    use crate::vfs::DescriptorFlags;
    use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
    use wasmtime_wasi::p2::bindings::filesystem::types::HostDescriptor;

    use super::*;
    use crate::limiter::{Limiter, StaticResourceLimits};

    /// Create a test VfsCtxView with default limits
    fn create_test_vfs() -> (ResourceTable, VfsState) {
        let limits = VfsLimits::default();
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let limiter = Limiter::new(StaticResourceLimits::default(), &pool);
        let vfs_state = VfsState::new(limits, limiter);
        let table = ResourceTable::new();

        (table, vfs_state)
    }

    /// Create a test descriptor with the given flags
    fn create_test_descriptor(
        ctx: &mut VfsCtxView<'_>,
        flags: DescriptorFlags,
    ) -> Resource<Descriptor> {
        let desc = VfsDescriptor {
            node: Arc::clone(&ctx.vfs_state.root),
            flags,
        };
        let res = ctx.table.push(desc).unwrap();
        res.cast()
    }

    /// Helper to create a file in the VFS for testing
    async fn create_test_file_via_open(ctx: &mut VfsCtxView<'_>, name: &str) {
        let desc = create_test_descriptor(
            ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let _ = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                name.to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            )
            .await
            .expect("file creation should succeed");
    }

    /// Helper to create a directory in the VFS for testing
    async fn create_test_directory(ctx: &mut VfsCtxView<'_>, name: &str) {
        let desc = create_test_descriptor(
            ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        ctx.create_directory_at(desc, name.to_string())
            .await
            .expect("directory creation should succeed");
    }

    /// Helper to create a file with content in the VFS
    async fn create_file_with_content(
        ctx: &mut VfsCtxView<'_>,
        name: &str,
        content: Vec<u8>,
    ) -> SharedVfsNode {
        // First create the file
        let desc = create_test_descriptor(
            ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        ctx.open_at(
            desc,
            PathFlags::empty(),
            name.to_string(),
            OpenFlags::CREATE,
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await
        .unwrap();

        // Get the node and set the content
        let desc = create_test_descriptor(ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, name).unwrap();

        // Update content
        {
            let mut guard = node.write().unwrap();
            if let VfsNodeKind::File { content: c } = &mut guard.kind {
                *c = content;
            }
        }

        node
    }

    /// Macro for VFS unit tests to reduce boilerplate.
    ///
    /// # Variants
    ///
    /// ## Basic test with default VFS:
    /// ```ignore
    /// vfs_test!(test_name, |ctx| async move {
    ///     // test body using ctx: &mut VfsCtxView<'_>
    /// });
    /// ```
    ///
    /// ## Test with custom VFS setup:
    /// ```ignore
    /// vfs_test!(test_name, setup: |table, vfs_state| { /* custom setup */ }, |ctx| async move {
    ///     // test body
    /// });
    /// ```
    macro_rules! vfs_test {
        // Basic test with default VFS
        ($name:ident, |$ctx:ident| async move $body:block) => {
            #[tokio::test]
            async fn $name() {
                let (mut table, mut vfs_state) = create_test_vfs();
                let mut $ctx = VfsCtxView {
                    table: &mut table,
                    vfs_state: &mut vfs_state,
                };
                $body
            }
        };

        // Test with custom VFS limits (inodes)
        ($name:ident, inodes: $inodes:expr, |$ctx:ident| async move $body:block) => {
            #[tokio::test]
            async fn $name() {
                let limits = VfsLimits {
                    inodes: $inodes,
                    max_path_length: 255,
                    max_path_segment_size: 50,
                };
                let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
                let limiter = Limiter::new(StaticResourceLimits::default(), &pool);
                let mut vfs_state = VfsState::new(limits, limiter);
                let mut table = ResourceTable::new();
                let mut $ctx = VfsCtxView {
                    table: &mut table,
                    vfs_state: &mut vfs_state,
                };
                $body
            }
        };

        // Test with custom memory pool size
        ($name:ident, memory_pool_bytes: $bytes:expr, |$ctx:ident| async move $body:block) => {
            #[tokio::test]
            async fn $name() {
                let limits = VfsLimits {
                    inodes: 100,
                    max_path_length: 255,
                    max_path_segment_size: 100,
                };
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new($bytes));
                let static_limits = StaticResourceLimits {
                    n_elements_per_table: 100,
                    n_instances: 100,
                    n_tables: 100,
                    n_memories: 100,
                };
                let limiter = Limiter::new(static_limits, &pool);
                let mut vfs_state = VfsState::new(limits, limiter);
                let mut table = ResourceTable::new();
                let mut $ctx = VfsCtxView {
                    table: &mut table,
                    vfs_state: &mut vfs_state,
                };
                $body
            }
        };

        // Test with very limited memory (for space exhaustion tests)
        ($name:ident, limited_space: $bytes:expr, |$ctx:ident| async move $body:block) => {
            #[tokio::test]
            async fn $name() {
                let limits = VfsLimits::default();
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new($bytes));
                let static_limits = StaticResourceLimits {
                    n_elements_per_table: 1,
                    n_instances: 1,
                    n_tables: 1,
                    n_memories: 1,
                };
                let limiter = Limiter::new(static_limits, &pool);
                let mut vfs_state = VfsState::new(limits, limiter);
                let mut table = ResourceTable::new();
                let mut $ctx = VfsCtxView {
                    table: &mut table,
                    vfs_state: &mut vfs_state,
                };
                $body
            }
        };
    }

    /// Macro for asserting an error result with a specific ErrorCode
    macro_rules! assert_error_code {
        ($result:expr, $code:expr) => {{
            assert!($result.is_err());
            let err = $result.unwrap_err();
            assert_eq!(*err.downcast_ref().unwrap(), $code);
        }};
    }

    /// Macro for asserting a node is a file with specific content
    macro_rules! assert_file_content {
        ($node:expr, $expected:expr) => {{
            let guard = $node.read().unwrap();
            match &guard.kind {
                VfsNodeKind::File { content } => {
                    assert_eq!(*content, $expected);
                }
                VfsNodeKind::Directory { .. } => {
                    panic!("Expected file, got directory");
                }
            }
        }};
    }

    /// Macro for asserting a node is an empty file
    macro_rules! assert_empty_file {
        ($node:expr) => {{ assert_file_content!($node, Vec::<u8>::new()) }};
    }

    /// Macro for asserting a node is a directory
    macro_rules! assert_is_directory {
        ($node:expr) => {{
            let guard = $node.read().unwrap();
            match &guard.kind {
                VfsNodeKind::Directory { .. } => {}
                VfsNodeKind::File { .. } => {
                    panic!("Expected directory, got file");
                }
            }
        }};
    }

    // ==================== create_directory_at tests ====================

    vfs_test!(
        test_create_directory_readonly_descriptor_fails,
        |ctx| async move {
            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
            assert_error_code!(result, ErrorCode::ReadOnly);
        }
    );

    vfs_test!(
        test_create_directory_already_exists_fails,
        |ctx| async move {
            // First creation should succeed
            create_test_directory(&mut ctx, "testdir").await;

            // Second creation should fail
            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
            assert_error_code!(result, ErrorCode::Exist);
        }
    );

    vfs_test!(test_create_directory_success, |ctx| async move {
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
        assert!(result.is_ok());

        // Verify the directory was created
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testdir").unwrap();
        assert_is_directory!(node);
    });

    vfs_test!(test_create_directory_insufficient_inodes_fails, inodes: 1, |ctx| async move {
        // First directory creation should succeed (uses the 1 allowed inode)
        create_test_directory(&mut ctx, "testdir").await;

        // Second creation should fail due to insufficient inodes
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx.create_directory_at(desc, "testdir2".to_string()).await;
        assert!(result.is_err());
    });

    vfs_test!(test_create_directory_insufficient_space_fails, limited_space: 2, |ctx| async move {
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx
            .create_directory_at(
                desc,
                "/very_long_directory_name_with_a_bunch_of_limits".to_string(),
            )
            .await;
        assert_error_code!(result, ErrorCode::InsufficientMemory);
    });

    vfs_test!(
        test_create_directory_nested_path_success,
        |ctx| async move {
            // First create parent directory
            create_test_directory(&mut ctx, "parent").await;

            // Then create child directory
            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx
                .create_directory_at(desc, "parent/child".to_string())
                .await;
            assert!(result.is_ok());

            // Verify both directories were created
            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            assert!(ctx.node_at(desc, "parent").is_ok());

            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            assert!(ctx.node_at(desc, "parent/child").is_ok());
        }
    );

    vfs_test!(
        test_create_directory_invalid_parent_fails,
        |ctx| async move {
            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx
                .create_directory_at(desc, "nonexistent/child".to_string())
                .await;
            assert_error_code!(result, ErrorCode::NoEntry);
        }
    );

    // ==================== open_at tests ====================

    vfs_test!(
        test_open_at_directory_flag_on_nonexistent_path_fails,
        |ctx| async move {
            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "nonexistent".to_string(),
                    OpenFlags::DIRECTORY,
                    DescriptorFlags::READ,
                )
                .await;
            assert_error_code!(result, ErrorCode::NoEntry);
        }
    );

    vfs_test!(
        test_open_at_directory_flag_on_file_fails,
        |ctx| async move {
            create_test_file_via_open(&mut ctx, "afile").await;

            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "afile".to_string(),
                    OpenFlags::DIRECTORY,
                    DescriptorFlags::READ,
                )
                .await;
            assert_error_code!(result, ErrorCode::NotDirectory);
        }
    );

    vfs_test!(
        test_open_at_directory_flag_on_directory_succeeds,
        |ctx| async move {
            create_test_directory(&mut ctx, "adir").await;

            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "adir".to_string(),
                    OpenFlags::DIRECTORY,
                    DescriptorFlags::READ,
                )
                .await;
            assert!(result.is_ok());
        }
    );

    vfs_test!(test_open_at_create_and_directory_fails, |ctx| async move {
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "newdir".to_string(),
                OpenFlags::CREATE | OpenFlags::DIRECTORY,
                DescriptorFlags::READ,
            )
            .await;
        assert_error_code!(result, ErrorCode::Invalid);
    });

    vfs_test!(
        test_open_at_create_exists_exclusive_fails,
        |ctx| async move {
            create_test_file_via_open(&mut ctx, "existingfile").await;

            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "existingfile".to_string(),
                    OpenFlags::CREATE | OpenFlags::EXCLUSIVE,
                    DescriptorFlags::READ,
                )
                .await;
            assert_error_code!(result, ErrorCode::Exist);
        }
    );

    vfs_test!(
        test_open_at_create_exists_not_exclusive_success,
        |ctx| async move {
            create_test_file_via_open(&mut ctx, "existingfile").await;

            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "existingfile".to_string(),
                    OpenFlags::CREATE,
                    DescriptorFlags::READ,
                )
                .await;
            assert!(result.is_ok());
        }
    );

    vfs_test!(
        test_open_at_create_no_mutate_permission_fails,
        |ctx| async move {
            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "newfile".to_string(),
                    OpenFlags::CREATE,
                    DescriptorFlags::READ,
                )
                .await;
            assert_error_code!(result, ErrorCode::ReadOnly);
        }
    );

    vfs_test!(test_open_at_create_new_file_success, |ctx| async move {
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "newfile".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            )
            .await;
        assert!(result.is_ok());

        // Verify the file was created
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "newfile").unwrap();
        assert_empty_file!(node);
    });

    vfs_test!(test_open_at_create_parent_is_file_fails, |ctx| async move {
        create_test_file_via_open(&mut ctx, "afile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "afile/child".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            )
            .await;
        assert_error_code!(result, ErrorCode::NotDirectory);
    });

    vfs_test!(
        test_open_at_create_exclusive_new_file_success,
        |ctx| async move {
            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "brandnewfile".to_string(),
                    OpenFlags::CREATE | OpenFlags::EXCLUSIVE,
                    DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
                )
                .await;
            assert!(result.is_ok());

            // Verify file was created
            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let node = ctx.node_at(desc, "brandnewfile");
            assert!(node.is_ok());
        }
    );

    vfs_test!(test_open_at_create_insufficient_inodes_fails, inodes: 0, |ctx| async move {
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "newfile".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            )
            .await;
        assert!(result.is_err());
    });

    vfs_test!(test_open_at_create_insufficient_memory_for_name_fails, memory_pool_bytes: 10, |ctx| async move {
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "long_filename_to_exceed_limits".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            )
            .await;
        assert_error_code!(result, ErrorCode::InsufficientMemory);
    });

    vfs_test!(
        test_open_at_no_create_nonexistent_file_fails,
        |ctx| async move {
            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "nonexistent".to_string(),
                    OpenFlags::empty(),
                    DescriptorFlags::READ,
                )
                .await;
            assert_error_code!(result, ErrorCode::NoEntry);
        }
    );

    vfs_test!(test_open_at_truncate_file_success, |ctx| async move {
        let content = vec![1, 2, 3, 4, 5];
        let file_node = create_file_with_content(&mut ctx, "testfile", content).await;

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ | DescriptorFlags::WRITE);
        let result = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::TRUNCATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await;
        assert!(result.is_ok());

        // Verify the file was truncated
        assert_empty_file!(file_node);
    });

    vfs_test!(test_open_at_truncate_directory_fails, |ctx| async move {
        create_test_directory(&mut ctx, "adir").await;

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ | DescriptorFlags::WRITE);
        let result = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "adir".to_string(),
                OpenFlags::TRUNCATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await;
        assert_error_code!(result, ErrorCode::IsDirectory);
    });

    vfs_test!(
        test_open_at_existing_file_no_flags_success,
        |ctx| async move {
            create_test_file_via_open(&mut ctx, "existingfile").await;

            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "existingfile".to_string(),
                    OpenFlags::empty(),
                    DescriptorFlags::READ,
                )
                .await;
            assert!(result.is_ok());
        }
    );

    vfs_test!(
        test_open_at_create_with_nested_path_success,
        |ctx| async move {
            create_test_directory(&mut ctx, "subdir").await;

            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "subdir/newfile".to_string(),
                    OpenFlags::CREATE,
                    DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
                )
                .await;
            assert!(result.is_ok());

            // Verify the file was created
            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let node = ctx.node_at(desc, "subdir/newfile");
            assert!(node.is_ok());
        }
    );

    vfs_test!(
        test_open_at_create_nonexistent_parent_path_fails,
        |ctx| async move {
            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "nonexistent/newfile".to_string(),
                    OpenFlags::CREATE,
                    DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
                )
                .await;
            assert_error_code!(result, ErrorCode::NoEntry);
        }
    );

    vfs_test!(
        test_open_at_create_and_truncate_new_file,
        |ctx| async move {
            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "newfile".to_string(),
                    OpenFlags::CREATE | OpenFlags::TRUNCATE,
                    DescriptorFlags::READ | DescriptorFlags::WRITE,
                )
                .await;
            assert!(result.is_ok());

            // Verify file exists and is empty
            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let node = ctx.node_at(desc, "newfile").unwrap();
            assert_empty_file!(node);
        }
    );

    vfs_test!(
        test_open_at_create_and_truncate_existing_file,
        |ctx| async move {
            let content = vec![1, 2, 3, 4, 5];
            let file_node = create_file_with_content(&mut ctx, "existingfile", content).await;

            let desc = create_test_descriptor(
                &mut ctx,
                DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
            );
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "existingfile".to_string(),
                    OpenFlags::CREATE | OpenFlags::TRUNCATE,
                    DescriptorFlags::READ | DescriptorFlags::WRITE,
                )
                .await;
            assert!(result.is_ok());

            // Verify file was truncated
            assert_empty_file!(file_node);
        }
    );

    vfs_test!(
        test_open_at_truncate_without_write_permission_no_truncate,
        |ctx| async move {
            let content = vec![1, 2, 3, 4, 5];
            let file_node = create_file_with_content(&mut ctx, "testfile", content.clone()).await;

            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "testfile".to_string(),
                    OpenFlags::TRUNCATE,
                    DescriptorFlags::READ,
                )
                .await;
            assert!(result.is_ok());

            // Verify file was NOT truncated (since no WRITE permission)
            assert_file_content!(file_node, content);
        }
    );

    vfs_test!(
        test_open_at_exclusive_without_create_is_ignored,
        |ctx| async move {
            create_test_file_via_open(&mut ctx, "existingfile").await;

            let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
            let result = ctx
                .open_at(
                    desc,
                    PathFlags::empty(),
                    "existingfile".to_string(),
                    OpenFlags::EXCLUSIVE,
                    DescriptorFlags::READ,
                )
                .await;
            assert!(result.is_ok());
        }
    );

    // ==================== write tests ====================

    /// Helper to open a file descriptor for a file
    async fn open_file_descriptor(
        ctx: &mut VfsCtxView<'_>,
        name: &str,
        flags: DescriptorFlags,
    ) -> Resource<Descriptor> {
        let desc = create_test_descriptor(
            ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        ctx.open_at(
            desc,
            PathFlags::empty(),
            name.to_string(),
            OpenFlags::empty(),
            flags,
        )
        .await
        .expect("file open should succeed")
    }

    vfs_test!(test_write_zero_bytes_returns_zero, |ctx| async move {
        // Per POSIX: "If nbyte is zero and the file is a regular file...
        // the write() function shall return zero and have no other results."
        create_test_file_via_open(&mut ctx, "testfile").await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let result = ctx.write(file_desc, vec![], 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    });

    vfs_test!(
        test_write_zero_bytes_checks_write_permission,
        |ctx| async move {
            // Per POSIX: Even zero-byte writes should check permissions
            create_test_file_via_open(&mut ctx, "testfile").await;

            let file_desc = open_file_descriptor(&mut ctx, "testfile", DescriptorFlags::READ).await;

            let result = ctx.write(file_desc, vec![], 0).await;
            assert_error_code!(result, ErrorCode::BadDescriptor);
        }
    );

    vfs_test!(test_write_zero_bytes_to_directory_fails, |ctx| async move {
        // Per POSIX: Writing to a directory should fail with EISDIR
        create_test_directory(&mut ctx, "testdir").await;

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let dir_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testdir".to_string(),
                OpenFlags::DIRECTORY,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let result = ctx.write(dir_desc, vec![], 0).await;
        assert_error_code!(result, ErrorCode::IsDirectory);
    });

    vfs_test!(
        test_write_without_write_permission_fails,
        |ctx| async move {
            // Per POSIX [EBADF]: "The fildes argument is not a valid file
            // descriptor open for writing."
            create_test_file_via_open(&mut ctx, "testfile").await;

            let file_desc = open_file_descriptor(&mut ctx, "testfile", DescriptorFlags::READ).await;

            let result = ctx.write(file_desc, vec![1, 2, 3], 0).await;
            assert_error_code!(result, ErrorCode::BadDescriptor);
        }
    );

    vfs_test!(test_write_to_directory_fails, |ctx| async move {
        // Per POSIX [EISDIR]: Writing to a directory is not allowed
        create_test_directory(&mut ctx, "testdir").await;

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let dir_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testdir".to_string(),
                OpenFlags::DIRECTORY,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let result = ctx.write(dir_desc, vec![1, 2, 3], 0).await;
        assert_error_code!(result, ErrorCode::IsDirectory);
    });

    vfs_test!(test_write_basic_success, |ctx| async move {
        // Per POSIX: "The write() function shall attempt to write nbyte bytes
        // from the buffer pointed to by buf to the file"
        create_test_file_via_open(&mut ctx, "testfile").await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![1, 2, 3, 4, 5];
        let result = ctx.write(file_desc, data.clone(), 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);

        // Verify the content was written
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, data);
    });

    vfs_test!(test_write_at_offset_success, |ctx| async move {
        // Per POSIX: "the actual writing of data shall proceed from the position
        // in the file indicated by the file offset"
        let initial_content = vec![0, 0, 0, 0, 0];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![1, 2, 3];
        let result = ctx.write(file_desc, data, 1).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);

        // Verify the content: [0, 1, 2, 3, 0]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, vec![0, 1, 2, 3, 0]);
    });

    vfs_test!(test_write_extends_file, |ctx| async move {
        // Per POSIX: "On a regular file, if the position of the last byte written
        // is greater than or equal to the length of the file, the length of the
        // file shall be set to this position plus one."
        let initial_content = vec![1, 2, 3];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![4, 5, 6, 7];
        let result = ctx.write(file_desc, data, 3).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 4);

        // Verify the content: [1, 2, 3, 4, 5, 6, 7]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, vec![1, 2, 3, 4, 5, 6, 7]);
    });

    vfs_test!(test_write_past_eof_fills_with_zeros, |ctx| async move {
        // Per POSIX: Writing past EOF extends the file, gap filled with zeros
        let initial_content = vec![1, 2, 3];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![7, 8, 9];
        let result = ctx.write(file_desc, data, 6).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);

        // Verify the content: [1, 2, 3, 0, 0, 0, 7, 8, 9]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, vec![1, 2, 3, 0, 0, 0, 7, 8, 9]);
    });

    vfs_test!(test_write_overwrites_existing_data, |ctx| async move {
        // Per POSIX: "Any subsequent successful write() to the same byte position
        // in the file shall overwrite that file data."
        let initial_content = vec![1, 2, 3, 4, 5];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![9, 9, 9];
        let result = ctx.write(file_desc, data, 1).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);

        // Verify the content: [1, 9, 9, 9, 5]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, vec![1, 9, 9, 9, 5]);
    });

    vfs_test!(test_write_returns_bytes_written, |ctx| async move {
        // Per POSIX: "Upon successful completion, these functions shall return
        // the number of bytes actually written to the file"
        create_test_file_via_open(&mut ctx, "testfile").await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let result = ctx.write(file_desc, data, 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);
    });

    vfs_test!(test_write_to_empty_file, |ctx| async move {
        create_test_file_via_open(&mut ctx, "testfile").await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![1, 2, 3];
        let result = ctx.write(file_desc, data.clone(), 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, data);
    });

    vfs_test!(test_write_multiple_times, |ctx| async move {
        create_test_file_via_open(&mut ctx, "testfile").await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        // First write
        let result = ctx.write(file_desc, vec![1, 2, 3], 0).await;
        assert!(result.is_ok());

        // Need a new descriptor for another write
        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        // Second write
        let result = ctx.write(file_desc, vec![4, 5, 6], 3).await;
        assert!(result.is_ok());

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, vec![1, 2, 3, 4, 5, 6]);
    });

    vfs_test!(test_write_insufficient_space_fails, limited_space: 50, |ctx| async move {
        // Per POSIX [ENOSPC]: "There was no free space remaining on the device
        // containing the file."
        create_test_file_via_open(&mut ctx, "testfile").await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        // Try to write a very large buffer that exceeds memory limits
        let large_data = vec![0u8; 1000];
        let result = ctx.write(file_desc, large_data, 0).await;
        assert_error_code!(result, ErrorCode::InsufficientMemory);
    });

    vfs_test!(test_write_at_offset_zero_to_empty_file, |ctx| async move {
        create_test_file_via_open(&mut ctx, "testfile").await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![42];
        let result = ctx.write(file_desc, data.clone(), 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, data);
    });

    vfs_test!(test_write_at_large_offset_to_empty_file, |ctx| async move {
        // Writing at a large offset should fill with zeros
        create_test_file_via_open(&mut ctx, "testfile").await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![1, 2];
        let result = ctx.write(file_desc, data, 5).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);

        // Should be: [0, 0, 0, 0, 0, 1, 2]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, vec![0, 0, 0, 0, 0, 1, 2]);
    });

    vfs_test!(test_write_partial_overwrite_and_extend, |ctx| async move {
        // Write that partially overwrites existing data and extends the file
        let initial_content = vec![1, 2, 3];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let file_desc = open_file_descriptor(
            &mut ctx,
            "testfile",
            DescriptorFlags::READ | DescriptorFlags::WRITE,
        )
        .await;

        let data = vec![8, 9, 10, 11];
        let result = ctx.write(file_desc, data, 2).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 4);

        // Should be: [1, 2, 8, 9, 10, 11]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap();
        assert_file_content!(node, vec![1, 2, 8, 9, 10, 11]);
    });
}
