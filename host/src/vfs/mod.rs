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
            _other @ (Ok(PathTraversal::Stay) | Ok(PathTraversal::Up) | Err(_)) => {
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
        _self_: Resource<Descriptor>,
        _buffer: Vec<u8>,
        _offset: Filesize,
    ) -> FsResult<Filesize> {
        Err(FsError::trap(ErrorCode::ReadOnly))
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
        // Handle write-like flags
        if open_flags.intersects(OpenFlags::CREATE | OpenFlags::TRUNCATE)
            || flags.intersects(DescriptorFlags::MUTATE_DIRECTORY | DescriptorFlags::WRITE)
        {
            return Err(FsError::trap(ErrorCode::ReadOnly));
        }

        // get node
        let node = self.node_at(self_, &path)?;

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

    #[tokio::test]
    async fn test_create_directory_readonly_descriptor_fails() {
        let (mut table, mut vfs_state) = create_test_vfs();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        // Create descriptor with READ flags only (no MUTATE_DIRECTORY)
        let desc = create_test_descriptor(&mut ctx, DescriptorFlags::READ);

        // Attempt to create directory should fail with ReadOnly error
        let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(*err.downcast_ref().unwrap(), ErrorCode::ReadOnly);
    }

    #[tokio::test]
    async fn test_create_directory_already_exists_fails() {
        let (mut table, mut vfs_state) = create_test_vfs();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );

        // First creation should succeed
        let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
        assert!(result.is_ok());

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );

        // Second creation should fail with Exist error
        let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(*err.downcast_ref().unwrap(), ErrorCode::Exist);
    }

    #[tokio::test]
    async fn test_create_directory_success() {
        let (mut table, mut vfs_state) = create_test_vfs();
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

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );

        // Verify the directory was created by checking it exists
        let node_result = ctx.node_at(desc, "testdir");
        assert!(node_result.is_ok());

        let node = node_result.unwrap();
        let node_guard = node.read().unwrap();
        match &node_guard.kind {
            VfsNodeKind::Directory { .. } => {
                // Success - it's a directory
            }
            VfsNodeKind::File { .. } => {
                panic!("Expected directory, got file");
            }
        }
    }

    #[tokio::test]
    async fn test_create_directory_insufficient_inodes_fails() {
        // Create VFS with very limited inodes (1 inode, does not include root)
        let limits = VfsLimits {
            inodes: 1,
            max_path_length: 255,
            max_path_segment_size: 50,
        };
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let limiter = Limiter::new(StaticResourceLimits::default(), &pool);
        let mut vfs_state = VfsState::new(limits, limiter);
        let mut table = ResourceTable::new();

        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        // Create descriptor with proper flags
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );

        // Directory creation should fail due to insufficient inodes
        let result = ctx.create_directory_at(desc, "testdir".to_string()).await;
        assert!(result.is_ok());

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );

        let result = ctx.create_directory_at(desc, "testdir2".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_directory_insufficient_space_fails() {
        // Create VFS with very limited space (10 bytes)
        let limits = VfsLimits::default();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2));
        let static_limits = StaticResourceLimits {
            n_elements_per_table: 1,
            n_instances: 1,
            n_tables: 1,
            n_memories: 1,
        };
        let limiter = Limiter::new(static_limits, &pool);
        let mut vfs_state = VfsState::new(limits, limiter);
        let mut table = ResourceTable::new();

        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        // Create descriptor with proper flags
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );

        // Directory creation should fail due to insufficient space for name
        let result = ctx
            .create_directory_at(
                desc,
                "very_long_directory_name_with_a_bunch_of_limits".to_string(),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_directory_nested_path_success() {
        let (mut table, mut vfs_state) = create_test_vfs();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        // Create descriptor with proper flags
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );

        // First create parent directory
        let result = ctx.create_directory_at(desc, "parent".to_string()).await;
        assert!(result.is_ok());

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );

        // Then create child directory using relative path
        let result = ctx
            .create_directory_at(desc, "parent/child".to_string())
            .await;
        assert!(result.is_ok());

        // Verify both directories were created
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        assert!(ctx.node_at(desc, "parent").is_ok());

        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );
        assert!(ctx.node_at(desc, "parent/child").is_ok());
    }

    #[tokio::test]
    async fn test_create_directory_invalid_parent_fails() {
        let (mut table, mut vfs_state) = create_test_vfs();
        let mut ctx = VfsCtxView {
            table: &mut table,
            vfs_state: &mut vfs_state,
        };

        // Create descriptor with proper flags
        let desc = create_test_descriptor(
            &mut ctx,
            DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY,
        );

        // Try to create directory with non-existent parent
        let result = ctx
            .create_directory_at(desc, "nonexistent/child".to_string())
            .await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(*err.downcast_ref().unwrap(), ErrorCode::NoEntry);
    }
}
