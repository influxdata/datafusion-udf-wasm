//! Virtual File System.
//!
//! This provides a very crude, read-only in-mem virtual file system for the WASM guests.
//!
//! The data gets populated via a TAR container.
//!
//! While this implementation has rather limited functionality, it is sufficient to get a Python guest interpreter
//! running.

use std::{
    collections::HashMap,
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
}

impl VfsState {
    /// Create a new empty VFS.
    pub(crate) fn new(limits: VfsLimits) -> Self {
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
                    let expected_size = entry.header().size()?;
                    let mut content = Vec::new();
                    entry.read_to_end(&mut content)?;
                    // Ensure that we read exactly expected_size bytes
                    assert!(content.len() as u64 == expected_size);
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
        let d = self.get_descriptor(self_)?;
        if !d.flags.contains(DescriptorFlags::WRITE) {
            return Err(FsError::trap(ErrorCode::Access));
        }

        let node = Arc::clone(&d.node);

        match &mut node.write().unwrap().kind {
            VfsNodeKind::File { content } => {
                let old_size = content.len() as u64;
                let new_size = offset + buffer.len() as u64;

                if new_size > old_size {
                    content.resize(new_size as usize, 0);
                }

                if new_size != old_size {
                    let start = offset as usize;
                    let end = start + buffer.len();
                    content[start..end].copy_from_slice(&buffer);
                }

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
        let node = self.node(self_)?;

        // Parse the path
        let (is_root, directions) = PathTraversal::parse(&path, &self.vfs_state.limits)?;
        let start = if is_root {
            Arc::clone(&self.vfs_state.root)
        } else {
            node
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
            VfsNode::traverse(start, directions)?
        };

        if open_flags.contains(OpenFlags::TRUNCATE) && flags.contains(DescriptorFlags::WRITE) {
            let mut node_guard = node.write().unwrap();
            match &mut node_guard.kind {
                VfsNodeKind::File { content } => {
                    content.clear();
                    content.shrink_to_fit();
                }
                VfsNodeKind::Directory { .. } => return Err(FsError::trap(ErrorCode::IsDirectory)),
            }
        }

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

    use assert_matches::assert_matches;
    use filesystem::types::HostDescriptor;
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
    async fn test_write() {
        let limits = VfsLimits::default();
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        let file_path = "bikes.txt".to_string();
        let open_flags = OpenFlags::CREATE;
        let file_flags = DescriptorFlags::READ | DescriptorFlags::WRITE;

        // Create the file
        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let write_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                file_path.clone(),
                open_flags,
                file_flags,
            )
            .await
            .expect("Failed to create file");

        // Write to it!
        let data = b"pinarello,canyon,factor".to_vec(); // 23 bytes
        let bytes_written = vfs_ctx
            .write(write_desc, data.clone(), 0)
            .await
            .expect("Failed to write data");

        assert_eq!(bytes_written, data.len() as u64);

        // Can we read it back?
        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let read_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                file_path.clone(),
                OpenFlags::empty(),
                DescriptorFlags::READ,
            )
            .await
            .expect("Failed to open file for reading");
        let (read_data, eof) = vfs_ctx
            .read(read_desc, data.len() as u64, 0)
            .await
            .expect("Failed to read data");

        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_append_and_overwrite() {
        let limits = VfsLimits::default();
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        let file_path = "bikes.txt".to_string();
        let open_flags = OpenFlags::CREATE;
        let file_flags = DescriptorFlags::READ | DescriptorFlags::WRITE;

        // Create the file
        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let write_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                file_path.clone(),
                open_flags,
                file_flags,
            )
            .await
            .expect("Failed to create file");

        // Write to it!
        let data = b"pinarello,canyon,factor".to_vec(); // 23 bytes
        let bytes_written = vfs_ctx
            .write(write_desc, data.clone(), 0)
            .await
            .expect("Failed to write data");

        assert_eq!(bytes_written, data.len() as u64);

        // Open the file for appending
        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let write_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                file_path.clone(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to open file for appending");

        // Append to it!
        let appended = b"bianchi,look,merida".to_vec(); // 19 bytes
        let bytes_written = vfs_ctx
            .write(write_desc, appended.clone(), data.len() as u64)
            .await
            .expect("Failed to append data");

        assert_eq!(bytes_written, appended.len() as u64);

        // Can we read the appended data back?
        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let read_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                file_path.clone(),
                OpenFlags::empty(),
                DescriptorFlags::READ,
            )
            .await
            .expect("Failed to open file for reading appended data");
        let total_len = data.len() + appended.len();
        let (actual, eof) = vfs_ctx
            .read(read_desc, total_len as u64, 0)
            .await
            .expect("Failed to read all data");

        let expected = [data, appended.clone()].concat();
        assert_eq!(expected, actual);
        assert!(eof);

        // Open the file for overwriting
        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let write_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                file_path.clone(),
                OpenFlags::empty(),
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to open file for writing");

        // Overwrite data!
        let overwritten = b"specialized,trek,cervelo".to_vec(); // 24 bytes
        let bytes_written = vfs_ctx
            .write(write_desc, overwritten.clone(), 0)
            .await
            .expect("Failed to overwrite data");

        assert_eq!(bytes_written, overwritten.len() as u64);

        // Can read the overwritten data back?
        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let read_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                file_path.clone(),
                OpenFlags::empty(),
                DescriptorFlags::READ,
            )
            .await
            .expect("Failed to open file for reading overwritten data");
        let (actual, eof) = vfs_ctx
            .read(read_desc, total_len as u64, 0)
            .await
            .expect("Failed to read overwritten data");

        // Before the overwrite, the buffer should contain 42 bytes:
        //
        // "pinarello,canyon,factorbianchi,look,merida"
        //  -----------------------^------------------
        //         23 bytes               19 bytes
        //
        // After the overwrite, it should contain the same number of bytes, with
        // the first 24 bytes having been overwritten. This implies that of the
        // last 19 bytes, only the last 18 remain, EG:
        //
        // "specialized,trek,cerveloianchi,look,merida"
        //  ------------------------^-----------------
        //  24 bytes                 18 bytes
        let partially_overwritten = appended[1..].to_vec();
        let expected = [overwritten, partially_overwritten].concat();
        assert_eq!(expected, actual);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_write_permissions() {
        let limits = VfsLimits::default();
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        let root_desc = VfsDescriptor {
            node: Arc::clone(&vfs_ctx.vfs_state.root),
            flags: DescriptorFlags::READ,
        };
        let root_resource = vfs_ctx.table.push(root_desc).unwrap();
        let root_descriptor: Resource<Descriptor> = root_resource.cast();

        let file_path = "bikes.txt".to_string();
        let open_flags = OpenFlags::CREATE;
        let file_flags = DescriptorFlags::READ;

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

        let data = b"failure".to_vec();
        let res = vfs_ctx.write(file_descriptor, data, 0).await;

        assert!(res.is_err());

        assert_matches!(res.err().unwrap().downcast_ref(), Some(ErrorCode::Access));
    }

    #[tokio::test]
    async fn test_vfs_limits() {
        use filesystem::types::HostDescriptor;

        // Create VFS with very small limits for testing
        let limits = VfsLimits {
            inodes: 10,
            max_path_length: 255,
            max_path_segment_size: 50,
        };
        let mut vfs_state = VfsState::new(limits);
        let mut resource_table = ResourceTable::new();

        let mut vfs_ctx = VfsCtxView {
            table: &mut resource_table,
            vfs_state: &mut vfs_state,
        };

        // ************************************
        // Test 1: Validate max_file_size limit
        // ************************************
        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let file_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                "test_file_size_limit.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file");

        // Try to write data exceeding file size limit, should get Io error
        let exceed_limit = vec![b'X'; 100]; // Larger than max_file_size (50)
        let res = vfs_ctx.write(file_desc, exceed_limit, 0).await;
        assert!(res.is_err());
        assert_matches!(res.err().unwrap().downcast_ref(), Some(ErrorCode::Io));

        // ******************************************************************
        // Test 2: Validate max_storage_bytes limit with multiple small files
        // ******************************************************************

        // Use buffer size of 40 bytes for each file
        let data = vec![b'A'; 40];

        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let file_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                "file1.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file1");

        vfs_ctx
            .write(file_desc, data.clone(), 0)
            .await
            .expect("Failed to write to file1");

        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let file_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                "file2.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file2");

        vfs_ctx
            .write(file_desc, data.clone(), 0)
            .await
            .expect("Failed to write to file2");

        let root_desc = create_rw_root_descriptor(&mut vfs_ctx);
        let file_desc = vfs_ctx
            .open_at(
                root_desc,
                PathFlags::empty(),
                "file3.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .expect("Failed to create file3");

        // Try to create third file with 40 bytes - this should exceed max_storage_bytes (100)
        // 40 + 40 + 40 = 120 bytes > 100 bytes limit
        let res = vfs_ctx.write(file_desc, data.clone(), 0).await;

        assert!(res.is_err());
        assert_matches!(res.err().unwrap().downcast_ref(), Some(ErrorCode::Io));
    }
}
