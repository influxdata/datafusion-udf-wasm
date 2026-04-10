//! Virtual File System.
//!
//! This provides a very crude, read-only in-mem virtual file system for the WASM guests.
//!
//! While this implementation has rather limited functionality, it is sufficient to get a Python guest interpreter
//! running.

use std::{
    collections::{HashMap, hash_map::Entry},
    hash::Hash,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use rand::RngExt;
use siphasher::sip128::{Hasher128, SipHasher24};
use wasmtime::component::{HasData, Resource};
use wasmtime_wasi::{
    ResourceTable, async_trait,
    filesystem::Descriptor,
    p2::{
        FsError, FsResult, InputStream as WasiInputStream, OutputStream as WasiOutputStream,
        Pollable, StreamError, StreamResult,
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
use wasmtime_wasi_io::bytes;

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

/// Output stream for writing to a VFS file.
struct VfsOutputStream {
    /// The file node to write to.
    node: SharedVfsNode,
    /// Current write offset in the file.
    offset: u64,
    /// Resource limiter for memory accounting.
    limiter: Limiter,
}

impl std::fmt::Debug for VfsOutputStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VfsOutputStream")
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Pollable for VfsOutputStream {
    async fn ready(&mut self) {
        // Wait until the stream is ready for writing. For an in-memory
        // stream, this is always the case, so we can just return
        // immediately.
    }
}

impl WasiOutputStream for VfsOutputStream {
    fn write(&mut self, buf: bytes::Bytes) -> StreamResult<()> {
        if buf.is_empty() {
            return Ok(());
        }

        match perform_write(&self.node, self.offset as usize, &buf, &self.limiter) {
            Ok(nbyte) => {
                self.offset += nbyte;
                Ok(())
            }
            Err(e) => Err(StreamError::Trap(e.into())),
        }
    }

    fn flush(&mut self) -> StreamResult<()> {
        // No-op for in-memory filesystem
        Ok(())
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        // Allow writes up to 64KB at a time
        Ok(64 * 1024)
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
    fn node_at(&self, res: Resource<Descriptor>, path: &str) -> FsResult<Option<SharedVfsNode>> {
        let node = self.node(res)?;
        self.get_node_from_start(path, node)
    }

    /// Get node at given path from given starting node.
    fn get_node_from_start(
        &self,
        path: &str,
        node: SharedVfsNode,
    ) -> FsResult<Option<SharedVfsNode>> {
        if path.is_empty() {
            return Err(FsError::trap(ErrorCode::Invalid));
        }

        let (is_root, directions) = PathTraversal::parse(path, &self.vfs_state.limits)?;

        let start = if is_root {
            Arc::clone(&self.vfs_state.root)
        } else {
            node
        };

        match VfsNode::traverse(start, directions) {
            Ok(node) => Ok(Some(node)),
            Err(e) => match e.downcast_ref() {
                Some(ErrorCode::NoEntry) => Ok(None),
                _ => Err(e),
            },
        }
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
            Ok(PathTraversal::Stay) => PathSegment::new(".", &self.vfs_state.limits)?,
            Ok(PathTraversal::Up) => PathSegment::new("..", &self.vfs_state.limits)?,
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
        self_: Resource<Descriptor>,
        offset: Filesize,
    ) -> FsResult<Resource<OutputStream>> {
        let desc = self.get_descriptor(self_)?;
        if !desc.flags.contains(DescriptorFlags::WRITE) {
            return Err(FsError::trap(ErrorCode::NotPermitted));
        }

        let node = Arc::clone(&desc.node);
        let limiter = self.vfs_state.limiter.clone();

        match &node.read().unwrap().kind {
            VfsNodeKind::File { .. } => {
                let stream = VfsOutputStream {
                    node: Arc::clone(&node),
                    offset,
                    limiter,
                };
                let stream: Box<dyn WasiOutputStream> = Box::new(stream);
                let res = self
                    .table
                    .push(stream)
                    .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;
                Ok(res)
            }
            VfsNodeKind::Directory { .. } => Err(FsError::trap(ErrorCode::IsDirectory)),
        }
    }

    fn append_via_stream(
        &mut self,
        _self_: Resource<Descriptor>,
    ) -> FsResult<Resource<OutputStream>> {
        Err(FsError::trap(ErrorCode::Unsupported))
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
        // Check if the descriptor has write permission
        let desc = self.get_descriptor(self_)?;
        if !desc.flags.contains(DescriptorFlags::WRITE) {
            return Err(FsError::trap(ErrorCode::NotPermitted));
        }

        let node = Arc::clone(&desc.node);

        // Per POSIX: "if nbyte is zero and the file is a regular file, the write() function
        // may detect and return errors as described below. In the absence of errors, or if
        // error detection is not performed, the write() function shall return zero and have
        // no other results." We return an error here, as writing an empty buffer does *not*
        // make much sense.
        if buffer.is_empty() {
            // Validate that this is a file, not a directory
            let guard = node.read().unwrap();
            return match &guard.kind {
                VfsNodeKind::File { .. } => Err(FsError::trap(ErrorCode::Invalid)),
                VfsNodeKind::Directory { .. } => Err(FsError::trap(ErrorCode::IsDirectory)),
            };
        }

        perform_write(&node, offset as usize, &buffer, &self.vfs_state.limiter)
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
        let node = match self.node_at(self_, &path)? {
            Some(node) => node,
            None => return Err(FsError::trap(ErrorCode::NoEntry)),
        };

        Ok(node.read().unwrap().stat())
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

        let create = open_flags.contains(OpenFlags::CREATE);
        let directory = open_flags.contains(OpenFlags::DIRECTORY);
        let exclusive = open_flags.contains(OpenFlags::EXCLUSIVE);
        let truncate = open_flags.contains(OpenFlags::TRUNCATE);

        // Try to resolve the path to an existing node
        let existing = self
            .get_node_from_start(&path, Arc::clone(&base_node))
            .map_err(FsError::trap)?;

        let node = match (existing, create, directory, exclusive, truncate) {
            (_, true, true, _, _) => {
                // Per POSIX: O_CREAT only creates regular files, not directories.
                // https://github.com/WebAssembly/WASI/blob/184b0c0e9fd437e5e5601d6e327a28feddbbd7f7/proposals/filesystem/wit/types.wit#L145-L146
                // "If O_CREAT and O_DIRECTORY are set and the requested access mode is
                // neither O_WRONLY nor O_RDWR, the result is unspecified." We choose to
                // disallow this combination entirely.
                return Err(FsError::trap(ErrorCode::Invalid));
            }
            (Some(node), _, true, _, _) if flags.contains(DescriptorFlags::WRITE) => {
                if matches!(node.read().unwrap().kind, VfsNodeKind::Directory { .. }) {
                    // Disallow opening directories with write permissions.
                    // POSIX isn't clear here, so we choose to disallow this
                    // combination entirely.
                    return Err(FsError::trap(ErrorCode::IsDirectory));
                } else {
                    // Per POSIX: "O_DIRECTORY: "If path resolves to a
                    // non-directory file, fail and set errno to [ENOTDIR]."
                    return Err(FsError::trap(ErrorCode::NotDirectory));
                }
            }
            (Some(node), true, _, false, false) => node, // Per POSIX: "If the file exists, O_CREAT has no effect except as noted under O_EXCL below.
            (Some(_), true, _, true, _) => {
                // Per POSIX: "O_CREAT and O_EXCL are set, open() shall fail if the file exists"
                return Err(FsError::trap(ErrorCode::Exist));
            }
            (Some(node), false, true, false, false) => {
                // Per POSIX: "O_DIRECTORY: "If path resolves to a non-directory file, fail and set errno to [ENOTDIR]."
                let guard = node.read().unwrap();
                if !matches!(guard.kind, VfsNodeKind::Directory { .. }) {
                    return Err(FsError::trap(ErrorCode::NotDirectory));
                }
                drop(guard);
                node
            }
            (Some(node), _, _, _, true) => {
                let mut guard = node.write().unwrap();
                match &mut guard.kind {
                    VfsNodeKind::File { content } => {
                        // Per POSIX: "The result of using O_TRUNC without either O_RDWR
                        // or O_WRONLY is undefined." We allow it but it's a no-op
                        // unless write permission is granted.
                        if flags.contains(DescriptorFlags::WRITE) {
                            self.vfs_state
                                .limiter
                                .shrink(content.capacity())
                                .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;
                            *content = Vec::new();
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
            (Some(node), _, _, _, _) => node,
            (None, false, _, _, _) => {
                // "If O_CREAT is not set and the file does not exist, open()
                // shall fail and set errno to [ENOENT]."
                return Err(FsError::trap(ErrorCode::NoEntry));
            }
            (None, true, _, _, _) => {
                // "If O_CREAT is set and the file does not exist, it shall be
                // created as a regular file with permissions"
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

                // Insert the new file into the parent directory
                match &mut parent_node.write().unwrap().kind {
                    VfsNodeKind::File { .. } => {
                        // Parent is a file, not a directory
                        // Per POSIX [ENOTDIR]: "A component of the path prefix names an
                        // existing file that is neither a directory nor a symbolic link to a directory"
                        return Err(FsError::trap(ErrorCode::NotDirectory));
                    }
                    VfsNodeKind::Directory { children } => {
                        let growth = name.len() + std::mem::size_of_val(&new_file);
                        match children.entry(name) {
                            Entry::Vacant(entry) => {
                                // Account for resource usage
                                self.vfs_state
                                    .inodes_allocation
                                    .inc(1)
                                    .map_err(FsError::trap)?;
                                self.vfs_state.limiter.grow(growth).map_err(|_| {
                                    // Rollback inode allocation since we failed to account for
                                    // the new file's name and node size
                                    self.vfs_state.inodes_allocation.dec(1);
                                    FsError::trap(ErrorCode::InsufficientMemory)
                                })?;
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
        // Symlinks not supported, hence a path can NEVER be a symlink and we shall return `Invalid`/`EINVAL`.
        Err(FsError::trap(ErrorCode::Invalid))
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
        let node = match self.node_at(self_, &path)? {
            Some(node) => node,
            None => return Err(FsError::trap(ErrorCode::NoEntry)),
        };

        Ok(node
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

/// Helper function to perform write operation
fn perform_write(
    node: &SharedVfsNode,
    offset: usize,
    buffer: &[u8],
    limiter: &Limiter,
) -> FsResult<Filesize> {
    let mut guard = node.write().unwrap();
    match &mut guard.kind {
        VfsNodeKind::File { content } => {
            let nbyte = buffer.len();
            let new_end = offset.saturating_add(nbyte);
            let old_len = content.len();

            if new_end > old_len {
                let growth = new_end - old_len;
                limiter
                    .grow(growth)
                    .map_err(|_| FsError::trap(ErrorCode::InsufficientMemory))?;
                content.resize(new_end, 0);
            }

            content[offset..offset + nbyte].copy_from_slice(buffer);
            Ok(nbyte as Filesize)
        }
        VfsNodeKind::Directory { .. } => Err(FsError::trap(ErrorCode::IsDirectory)),
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

    /// Parameters for creating a test VFS context.
    struct VfsTestParams {
        /// Maximum number of inodes allowed.
        inodes: u64,
        /// Maximum path length.
        max_path_length: u64,
        /// Maximum path segment size.
        max_path_segment_size: u64,
        /// Memory pool size in bytes. If `None`, uses unbounded memory.
        memory_pool_bytes: Option<usize>,
        /// Static resource limits for the limiter.
        static_limits: StaticResourceLimits,
    }

    impl Default for VfsTestParams {
        fn default() -> Self {
            Self {
                inodes: 100,
                max_path_length: 255,
                max_path_segment_size: 100,
                memory_pool_bytes: None,
                static_limits: StaticResourceLimits::default(),
            }
        }
    }

    impl VfsTestParams {
        /// Create params with a specific inode limit.
        fn with_inodes(mut self, inodes: u64) -> Self {
            self.inodes = inodes;
            self
        }

        /// Create params with a specific memory pool size.
        fn with_memory_pool_bytes(mut self, bytes: usize) -> Self {
            self.memory_pool_bytes = Some(bytes);
            self
        }

        /// Create params for limited space tests (very constrained resources).
        fn with_limited_space(mut self, bytes: usize) -> Self {
            self.memory_pool_bytes = Some(bytes);
            self.static_limits = StaticResourceLimits {
                n_elements_per_table: 1,
                n_instances: 1,
                n_tables: 1,
                n_memories: 1,
            };
            self
        }

        /// Build the VFS state and resource table from these parameters.
        fn build(self) -> (ResourceTable, VfsState) {
            let limits = VfsLimits {
                inodes: self.inodes,
                max_path_length: self.max_path_length,
                max_path_segment_size: self.max_path_segment_size,
            };

            let pool: Arc<dyn MemoryPool> = match self.memory_pool_bytes {
                Some(bytes) => Arc::new(GreedyMemoryPool::new(bytes)),
                None => Arc::new(UnboundedMemoryPool::default()),
            };

            let limiter = Limiter::new(self.static_limits, &pool);
            let vfs_state = VfsState::new(limits, limiter);
            let table = ResourceTable::new();
            (table, vfs_state)
        }
    }

    /// Assert that a result is an error with a specific ErrorCode.
    #[track_caller]
    fn assert_error_code<T: std::fmt::Debug>(result: FsResult<T>, expected: ErrorCode) {
        assert!(result.is_err(), "Expected error, got {:?}", result);
        let err = result.unwrap_err();
        let actual = err.downcast_ref().expect("Error should contain ErrorCode");
        assert_eq!(*actual, expected, "Error code mismatch");
    }

    /// Assert that a node is a file with specific content.
    #[track_caller]
    fn assert_file_content(node: &SharedVfsNode, expected: &[u8]) {
        let guard = node.read().unwrap();
        match &guard.kind {
            VfsNodeKind::File { content } => {
                assert_eq!(content.as_slice(), expected, "File content mismatch");
            }
            VfsNodeKind::Directory { .. } => {
                panic!("Expected file, got directory");
            }
        }
    }

    /// Assert that a node is an empty file.
    #[track_caller]
    fn assert_empty_file(node: &SharedVfsNode) {
        assert_file_content(node, &[]);
    }

    /// Assert that a node is a directory.
    #[track_caller]
    fn assert_is_directory(node: &SharedVfsNode) {
        let guard = node.read().unwrap();
        match &guard.kind {
            VfsNodeKind::Directory { .. } => {}
            VfsNodeKind::File { .. } => {
                panic!("Expected directory, got file");
            }
        }
    }

    /// Create a test descriptor with the given flags.
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

    /// Helper to create a file in the VFS for testing.
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

    /// Helper to create a directory in the VFS for testing.
    async fn create_test_directory(ctx: &mut VfsCtxView<'_>, name: &str) {
        let desc = create_test_descriptor(
            ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        ctx.create_directory_at(desc, name.to_string())
            .await
            .expect("directory creation should succeed");
    }

    /// Helper to create a file with content in the VFS.
    async fn create_file_with_content(
        ctx: &mut VfsCtxView<'_>,
        name: &str,
        content: Vec<u8>,
    ) -> SharedVfsNode {
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

        let desc = create_test_descriptor(ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, name).unwrap();

        {
            let node = node.unwrap();
            let mut guard = node.write().unwrap();
            if let VfsNodeKind::File { content: c } = &mut guard.kind {
                *c = content;
            }
            drop(guard);
            node
        }
    }

    // ==================== create_directory_at tests ====================

    #[tokio::test]
    async fn test_create_directory_readonly_descriptor_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
        assert_error_code(result, ErrorCode::ReadOnly);
    }

    #[tokio::test]
    async fn test_create_directory_already_exists_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_directory(&mut ctx, "testdir").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
        assert_error_code(result, ErrorCode::Exist);
    }

    #[tokio::test]
    async fn test_create_directory_success() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
        assert!(result.is_ok());

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testdir").unwrap();
        assert_is_directory(&node.unwrap());
    }

    #[tokio::test]
    async fn test_create_directory_insufficient_inodes_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().with_inodes(1).build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_directory(&mut ctx, "testdir").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx.create_directory_at(desc, "testdir2".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_directory_insufficient_space_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().with_limited_space(2).build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::InsufficientMemory);
    }

    #[tokio::test]
    async fn test_create_directory_nested_path_success() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_directory(&mut ctx, "parent").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx
            .create_directory_at(desc, "parent/child".to_string())
            .await;
        assert!(result.is_ok());

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        assert!(ctx.node_at(desc, "parent").is_ok());

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        assert!(ctx.node_at(desc, "parent/child").is_ok());
    }

    #[tokio::test]
    async fn test_create_directory_invalid_parent_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let result = ctx
            .create_directory_at(desc, "nonexistent/child".to_string())
            .await;
        assert_error_code(result, ErrorCode::NoEntry);
    }

    // ==================== open_at tests ====================

    #[tokio::test]
    async fn test_open_at_directory_flag_on_nonexistent_path_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::NoEntry);
    }

    #[tokio::test]
    async fn test_open_at_directory_flag_on_file_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::NotDirectory);
    }

    #[tokio::test]
    async fn test_open_at_directory_flag_on_directory_succeeds() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

    #[tokio::test]
    async fn test_open_at_create_and_directory_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::Invalid);
    }

    #[tokio::test]
    async fn test_open_at_create_exists_exclusive_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::Exist);
    }

    #[tokio::test]
    async fn test_open_at_create_exists_not_exclusive_success() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

    #[tokio::test]
    async fn test_open_at_create_no_mutate_permission_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::ReadOnly);
    }

    #[tokio::test]
    async fn test_open_at_create_new_file_success() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "newfile").unwrap();
        assert_empty_file(&node.unwrap());
    }

    #[tokio::test]
    async fn test_open_at_create_parent_is_file_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_at_create_exclusive_new_file_success() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "brandnewfile");
        assert!(node.is_ok());
    }

    #[tokio::test]
    async fn test_open_at_create_insufficient_inodes_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().with_inodes(0).build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
    }

    #[tokio::test]
    async fn test_open_at_create_insufficient_memory_for_name_fails() {
        let (mut table, mut vfs_state) =
            VfsTestParams::default().with_memory_pool_bytes(10).build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::InsufficientMemory);
    }

    #[tokio::test]
    async fn test_open_at_no_create_nonexistent_file_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::NoEntry);
    }

    #[tokio::test]
    async fn test_open_at_truncate_file_success() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

        assert_empty_file(&file_node);
    }

    #[tokio::test]
    async fn test_open_at_truncate_directory_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::IsDirectory);
    }

    #[tokio::test]
    async fn test_open_at_existing_file_no_flags_success() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

    #[tokio::test]
    async fn test_open_at_create_with_nested_path_success() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "subdir/newfile");
        assert!(node.is_ok());
    }

    #[tokio::test]
    async fn test_open_at_create_nonexistent_parent_path_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
        assert_error_code(result, ErrorCode::NoEntry);
    }

    #[tokio::test]
    async fn test_open_at_create_and_truncate_new_file() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "newfile").unwrap();
        assert_empty_file(&node.unwrap());
    }

    #[tokio::test]
    async fn test_open_at_create_and_truncate_existing_file() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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
                OpenFlags::TRUNCATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await;
        assert!(result.is_ok());

        assert_empty_file(&file_node);
    }

    #[tokio::test]
    async fn test_open_at_truncate_without_write_permission_no_truncate() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

        assert_file_content(&file_node, &content);
    }

    #[tokio::test]
    async fn test_open_at_exclusive_without_create_is_ignored() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

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

    #[tokio::test]
    async fn test_write_without_permission_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        // Open file with READ only (no WRITE permission)
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ,
            )
            .await
            .unwrap();

        let result = ctx.write(file_desc, vec![1, 2, 3], 0).await;
        assert_error_code(result, ErrorCode::NotPermitted);
    }

    #[tokio::test]
    async fn test_write_empty_buffer_to_file_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let result = ctx.write(file_desc, vec![], 0).await;
        assert_error_code(result, ErrorCode::Invalid);
    }

    #[tokio::test]
    async fn test_write_to_empty_file_success() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let data = vec![1, 2, 3, 4, 5];
        let result = ctx.write(file_desc, data.clone(), 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data.len() as Filesize);

        // Verify content
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &data);
    }

    #[tokio::test]
    async fn test_write_returns_bytes_written() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let data = vec![0u8; 100];
        let result = ctx.write(file_desc, data, 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
    }

    #[tokio::test]
    async fn test_write_at_offset_extends_file() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3];
        let file_node = create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Write at offset 3 (end of file)
        let new_data = vec![4, 5, 6];
        let result = ctx.write(file_desc, new_data, 3).await;
        assert!(result.is_ok());

        // Verify content is [1, 2, 3, 4, 5, 6]
        assert_file_content(&file_node, &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_write_beyond_file_length_fills_with_zeros() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3];
        let file_node = create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Write at offset 5 (beyond current file length of 3)
        let new_data = vec![7, 8, 9];
        let result = ctx.write(file_desc, new_data, 5).await;
        assert!(result.is_ok());

        // Verify content is [1, 2, 3, 0, 0, 7, 8, 9]
        assert_file_content(&file_node, &[1, 2, 3, 0, 0, 7, 8, 9]);
    }

    #[tokio::test]
    async fn test_write_overwrites_existing_content() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3, 4, 5];
        let file_node = create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Overwrite bytes at offset 1
        let new_data = vec![10, 11];
        let result = ctx.write(file_desc, new_data, 1).await;
        assert!(result.is_ok());

        // Verify content is [1, 10, 11, 4, 5]
        assert_file_content(&file_node, &[1, 10, 11, 4, 5]);
    }

    #[tokio::test]
    async fn test_write_at_offset_zero_overwrites_beginning() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3, 4, 5];
        let file_node = create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Overwrite bytes at offset 0
        let new_data = vec![10, 11, 12];
        let result = ctx.write(file_desc, new_data, 0).await;
        assert!(result.is_ok());

        // Verify content is [10, 11, 12, 4, 5]
        assert_file_content(&file_node, &[10, 11, 12, 4, 5]);
    }

    #[tokio::test]
    async fn test_write_insufficient_memory_fails() {
        let (mut table, mut vfs_state) =
            VfsTestParams::default().with_memory_pool_bytes(10).build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        // Create file first (this uses some memory)
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "f".to_string(), // Short name to minimize memory usage
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Try to write more data than memory allows
        let large_data = vec![0u8; 1000];
        let result = ctx.write(file_desc, large_data, 0).await;
        assert_error_code(result, ErrorCode::InsufficientMemory);
    }

    #[tokio::test]
    async fn test_write_partial_overwrite_and_extend() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3];
        let file_node = create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Write at offset 2, overwriting last byte and extending
        let new_data = vec![10, 11, 12];
        let result = ctx.write(file_desc, new_data, 2).await;
        assert!(result.is_ok());

        // Verify content is [1, 2, 10, 11, 12]
        assert_file_content(&file_node, &[1, 2, 10, 11, 12]);
    }

    #[tokio::test]
    async fn test_write_multiple_writes_to_same_file() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // First write
        let result1 = ctx
            .write(Resource::new_borrow(file_desc.rep()), vec![1, 2, 3], 0)
            .await;
        assert!(result1.is_ok());

        // Second write at end
        let result2 = ctx
            .write(Resource::new_borrow(file_desc.rep()), vec![4, 5], 3)
            .await;
        assert!(result2.is_ok());

        // Third write overwriting middle
        let result3 = ctx.write(file_desc, vec![10], 2).await;
        assert!(result3.is_ok());

        // Verify content is [1, 2, 10, 4, 5]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[1, 2, 10, 4, 5]);
    }

    #[tokio::test]
    async fn test_write_single_byte() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let result = ctx.write(file_desc, vec![42], 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[42]);
    }

    #[tokio::test]
    async fn test_write_via_stream_without_write_permission_fails() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        // Open file with READ only (no WRITE permission)
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ,
            )
            .await
            .unwrap();

        let result = ctx.write_via_stream(file_desc, 0);
        assert_error_code(result, ErrorCode::NotPermitted);
    }

    #[tokio::test]
    async fn test_write_via_stream_writes_content() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let stream_res = ctx.write_via_stream(file_desc, 0).unwrap();

        // Get the stream and write to it
        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();
        let data = bytes::Bytes::from_static(&[1, 2, 3, 4, 5]);
        let write_result = stream.write(data);
        assert!(write_result.is_ok());

        // Verify content
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_write_via_stream_at_offset() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3, 4, 5];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Create stream at offset 2
        let stream_res = ctx.write_via_stream(file_desc, 2).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();
        let data = bytes::Bytes::from_static(&[10, 11]);
        let write_result = stream.write(data);
        assert!(write_result.is_ok());

        // Verify content is [1, 2, 10, 11, 5]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[1, 2, 10, 11, 5]);
    }

    #[tokio::test]
    async fn test_write_via_stream_extends_file() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Create stream at offset 3 (end of file)
        let stream_res = ctx.write_via_stream(file_desc, 3).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();
        let data = bytes::Bytes::from_static(&[4, 5, 6]);
        let write_result = stream.write(data);
        assert!(write_result.is_ok());

        // Verify content is [1, 2, 3, 4, 5, 6]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_write_via_stream_beyond_file_length_fills_with_zeros() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Create stream at offset 5 (beyond current file length of 3)
        let stream_res = ctx.write_via_stream(file_desc, 5).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();
        let data = bytes::Bytes::from_static(&[7, 8, 9]);
        let write_result = stream.write(data);
        assert!(write_result.is_ok());

        // Verify content is [1, 2, 3, 0, 0, 7, 8, 9]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[1, 2, 3, 0, 0, 7, 8, 9]);
    }

    #[tokio::test]
    async fn test_write_via_stream_empty_write_is_noop() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3];
        create_file_with_content(&mut ctx, "testfile", initial_content.clone()).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let stream_res = ctx.write_via_stream(file_desc, 0).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();
        // Write empty buffer
        let data = bytes::Bytes::new();
        let write_result = stream.write(data);
        assert!(write_result.is_ok());

        // Verify content unchanged
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &initial_content);
    }

    #[tokio::test]
    async fn test_write_via_stream_multiple_writes_advance_offset() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let stream_res = ctx.write_via_stream(file_desc, 0).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();

        // First write
        let data1 = bytes::Bytes::from_static(&[1, 2, 3]);
        let write_result1 = stream.write(data1);
        assert!(write_result1.is_ok());

        // Second write should continue from offset 3
        let data2 = bytes::Bytes::from_static(&[4, 5]);
        let write_result2 = stream.write(data2);
        assert!(write_result2.is_ok());

        // Third write should continue from offset 5
        let data3 = bytes::Bytes::from_static(&[6]);
        let write_result3 = stream.write(data3);
        assert!(write_result3.is_ok());

        // Verify content is [1, 2, 3, 4, 5, 6]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_write_via_stream_insufficient_memory_fails() {
        let (mut table, mut vfs_state) =
            VfsTestParams::default().with_memory_pool_bytes(50).build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        // Create file first (this uses some memory)
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "f".to_string(), // Short name to minimize memory usage
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let stream_res = ctx.write_via_stream(file_desc, 0).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();

        // Try to write more data than memory allows
        let large_data = bytes::Bytes::from(vec![0u8; 1000]);
        let result = stream.write(large_data);
        assert!(result.is_err());
        match result {
            Err(StreamError::Trap(_)) => {} // Expected
            other => panic!("Expected StreamError::Trap, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_write_via_stream_flush_succeeds() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let stream_res = ctx.write_via_stream(file_desc, 0).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();

        // Flush should succeed (no-op)
        let flush_result = stream.flush();
        assert!(flush_result.is_ok());
    }

    #[tokio::test]
    async fn test_write_via_stream_check_write_returns_64kb() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let stream_res = ctx.write_via_stream(file_desc, 0).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();

        // check_write should return 64KB
        let check_result = stream.check_write();
        assert!(check_result.is_ok());
        assert_eq!(check_result.unwrap(), 64 * 1024);
    }

    #[tokio::test]
    async fn test_write_via_stream_single_byte() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        create_test_file_via_open(&mut ctx, "testfile").await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let stream_res = ctx.write_via_stream(file_desc, 0).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();
        let data = bytes::Bytes::from_static(&[42]);
        let write_result = stream.write(data);
        assert!(write_result.is_ok());

        // Verify content
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[42]);
    }

    #[tokio::test]
    async fn test_write_via_stream_overwrites_existing_content() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3, 4, 5];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Write at beginning to overwrite first 3 bytes
        let stream_res = ctx.write_via_stream(file_desc, 0).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();
        let data = bytes::Bytes::from_static(&[10, 11, 12]);
        let write_result = stream.write(data);
        assert!(write_result.is_ok());

        // Verify content is [10, 11, 12, 4, 5]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[10, 11, 12, 4, 5]);
    }

    #[tokio::test]
    async fn test_write_via_stream_partial_overwrite_and_extend() {
        let (mut table, mut vfs_state) = VfsTestParams::default().build();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let initial_content = vec![1, 2, 3];
        create_file_with_content(&mut ctx, "testfile", initial_content).await;

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::WRITE | DescriptorFlags::MUTATE_DIRECTORY,
        );
        let file_desc = ctx
            .open_at(
                desc,
                PathFlags::empty(),
                "testfile".to_string(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        // Write at offset 2, overwriting last byte and extending
        let stream_res = ctx.write_via_stream(file_desc, 2).unwrap();

        let stream = ctx
            .table
            .get_mut::<Box<dyn WasiOutputStream>>(&stream_res)
            .unwrap();
        let data = bytes::Bytes::from_static(&[10, 11, 12]);
        let write_result = stream.write(data);
        assert!(write_result.is_ok());

        // Verify content is [1, 2, 10, 11, 12]
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);
        let node = ctx.node_at(desc, "testfile").unwrap().unwrap();
        assert_file_content(&node, &[1, 2, 10, 11, 12]);
    }
}
