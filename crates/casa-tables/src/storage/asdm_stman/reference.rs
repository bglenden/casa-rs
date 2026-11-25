// SPDX-License-Identifier: LGPL-3.0-or-later
//! Relocate BDF filenames without changing the visibility data or descriptors.

use std::borrow::Cow;
use std::fs::File;
#[cfg(unix)]
use std::fs::Metadata;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::Mutex;

use casa_aipsio::AipsIo;

use super::{IndexHeader, StorageError, TYPE, invalid};
use crate::{TableError, TableWriteLock, publication};

/// The index file for the lazy ASDM storage manager attached to a column.
#[derive(Debug)]
pub struct AsdmBdfIndex {
    table: PathBuf,
    path: PathBuf,
    #[cfg(unix)]
    first: Mutex<Option<CachedName>>,
    #[cfg(unix)]
    binding: TableBinding,
}

#[cfg(unix)]
#[derive(Debug)]
struct TableBinding {
    _directory: File,
    _descriptor: File,
    identity: (u64, u64),
    descriptor: IndexVersion,
}

#[cfg(unix)]
impl TableBinding {
    fn capture(table: &Path) -> Result<Self, StorageError> {
        let directory = publication::open_directory_identity(table)?;
        let descriptor = File::open(table.join("table.dat"))?;
        Ok(Self {
            identity: IndexVersion::from(&directory.metadata()?).identity,
            descriptor: IndexVersion::from(&descriptor.metadata()?),
            _directory: directory,
            _descriptor: descriptor,
        })
    }

    fn matches(&self, table: &Path) -> bool {
        std::fs::metadata(table).is_ok_and(|dir| IndexVersion::from(&dir).identity == self.identity)
            && std::fs::metadata(table.join("table.dat"))
                .is_ok_and(|descriptor| IndexVersion::from(&descriptor) == self.descriptor)
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct CachedName {
    version: IndexVersion,
    path: Option<PathBuf>,
}

#[cfg(unix)]
#[derive(Debug, PartialEq, Eq)]
struct IndexVersion {
    identity: (u64, u64),
    size: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

#[cfg(unix)]
impl From<&Metadata> for IndexVersion {
    fn from(file: &Metadata) -> Self {
        Self {
            identity: (file.dev(), file.ino()),
            size: file.len(),
            modified: (file.mtime(), file.mtime_nsec()),
            changed: (file.ctime(), file.ctime_nsec()),
        }
    }
}

impl AsdmBdfIndex {
    pub(crate) fn new(table: PathBuf, sequence: u32) -> Result<Self, TableError> {
        let path = table.join(format!("table.f{sequence}asdmindex"));
        Ok(Self {
            #[cfg(unix)]
            binding: TableBinding::capture(&table)?,
            table,
            path,
            #[cfg(unix)]
            first: Mutex::new(None),
        })
    }

    /// Physical table directory whose write lock guards the index.
    pub fn table_path(&self) -> &Path {
        &self.table
    }

    /// Read the stored directory containing the first file's BDF directory.
    ///
    /// The caller provides variable and home-directory expansion for the
    /// persisted paths. Pass an identity function to use literal paths.
    /// The directory is absent when the index lists no BDF files. The second
    /// result is true when the index was read again or read for the first time.
    ///
    /// # Errors
    /// The index cannot be read or its filename block is malformed.
    pub fn reference(&self, expand: PathExpansion) -> Result<(Option<String>, bool), TableError> {
        #[cfg(unix)]
        {
            let mut cached = self
                .first
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(current) = cached.as_ref()
                && std::fs::metadata(&self.path)
                    .is_ok_and(|file| current.version == IndexVersion::from(&file))
            {
                return Ok((reference_name(current.path.as_deref(), expand)?, false));
            }
            let input = File::open(&self.path).map_err(StorageError::from)?;
            let file = input.metadata().map_err(StorageError::from)?;
            let header = read_header(input, file.len())?.1;
            let path = header.bdfs.into_iter().next();
            let result = reference_name(path.as_deref(), expand);
            *cached = Some(CachedName {
                version: IndexVersion::from(&file),
                path,
            });
            Ok((result?, true))
        }
        #[cfg(not(unix))]
        {
            let input = File::open(&self.path).map_err(StorageError::from)?;
            let size = input.metadata().map_err(StorageError::from)?.len();
            let (_, header) = read_header(input, size)?;
            Ok((
                reference_name(header.bdfs.first().map(PathBuf::as_path), expand)?,
                true,
            ))
        }
    }

    /// Replace the ASDM directory in the index's stored BDF paths.
    ///
    /// Hold the physical table's write lock and verify the current binding.
    /// The caller provides variable and home-directory expansion. The files
    /// can be offline during this operation. The stage preserves the
    /// BDF endian marker, descriptor bytes, and mode of the existing index.
    /// On Linux and macOS, a symbolic link keeps its binding and the update
    /// is published to its captured referent.
    ///
    /// # Errors
    /// The lock or index is invalid, or the replacement cannot be published.
    pub fn relocate(
        &self,
        replacement: &str,
        lock: &TableWriteLock,
        expand: PathExpansion,
    ) -> Result<String, TableError> {
        if !self.matches_table(lock) {
            return Err(invalid("ASDM index write does not hold its physical table lock").into());
        }
        if replacement.contains('\0') {
            return Err(invalid("an ASDM pathname cannot contain NUL").into());
        }
        if self.reference(expand)?.0.is_none() {
            return Ok(String::new());
        }
        let changed = self.replace_names(replacement, expand, lock)?;
        if changed {
            // Only the independent index changed. Like AsdmStMan::writeIndex,
            // this leaves the parent table descriptor and sync data untouched.
            Ok(replacement.to_owned())
        } else {
            Ok(String::new())
        }
    }

    fn matches_table(&self, lock: &TableWriteLock) -> bool {
        #[cfg(unix)]
        if !self.binding.matches(&self.table) {
            return false;
        }
        lock.matches_table_path(&self.table)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn replace_names(
        &self,
        replacement: &str,
        expand: PathExpansion,
        lock: &TableWriteLock,
    ) -> Result<bool, StorageError> {
        let parent = publication::open_directory_identity(&self.table)?;
        publication::check_write_access(&self.path)?;
        let name = self.path.file_name().expect("ASDM index file name");
        let verify_table = || {
            if self.matches_table(lock) {
                Ok(())
            } else {
                Err(invalid("ASDM table changed during index publication"))
            }
        };
        match publication::open_regular_directory_entry(&parent, name) {
            Ok(original) => replace_existing(
                &self.path,
                &parent,
                original,
                replacement,
                expand,
                verify_table,
            ),
            Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
                let link = publication::open_symlink_directory_entry(&parent, name)?;
                let linked = IndexVersion::from(&link.metadata()?).identity;
                let target = self.path.canonicalize()?;
                let directory = target
                    .parent()
                    .ok_or_else(|| invalid("missing ASDM index parent"))?;
                let pinned = publication::open_directory_identity(directory)?;
                let pinned_identity = IndexVersion::from(&pinned.metadata()?).identity;
                let original = publication::open_regular_directory_entry(
                    &pinned,
                    target
                        .file_name()
                        .ok_or_else(|| invalid("missing ASDM index name"))?,
                )?;
                replace_existing(&target, &pinned, original, replacement, expand, || {
                    verify_table()?;
                    verify_index_link(&self.path, linked, &target, directory, pinned_identity)
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn replace_names(
        &self,
        replacement: &str,
        expand: PathExpansion,
        lock: &TableWriteLock,
    ) -> Result<bool, StorageError> {
        let original = self.path.symlink_metadata()?;
        if !original.is_file() {
            return Err(invalid("ASDM index replacement needs a regular index file"));
        }
        publication::check_write_access(&self.path)?;
        let staged = tempfile::NamedTempFile::new_in(&self.table)?;
        if !write_index(
            File::open(&self.path)?,
            staged.reopen()?,
            replacement,
            expand,
        )? {
            return Ok(false);
        }
        staged.as_file().set_permissions(original.permissions())?;
        publication::sync_publication_file(staged.as_file())?;
        let current = self.path.symlink_metadata()?;
        if !self.matches_table(lock)
            || !current.is_file()
            || current.len() != original.len()
            || current.modified()? != original.modified()?
        {
            return Err(invalid("ASDM index changed during publication"));
        }
        staged.persist(&self.path).map_err(|error| error.error)?;
        Ok(true)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn replace_existing(
    path: &Path,
    parent: &File,
    original: File,
    replacement: &str,
    expand: PathExpansion,
    verify: impl FnOnce() -> Result<(), StorageError>,
) -> Result<bool, StorageError> {
    let identity = original.metadata()?;
    if identity.nlink() > 1 {
        return Err(invalid(
            "a hard-linked ASDM index cannot be replaced atomically",
        ));
    }
    let directory = path
        .parent()
        .ok_or_else(|| invalid("missing ASDM index parent"))?;
    let name = path
        .file_name()
        .ok_or_else(|| invalid("missing ASDM index name"))?;
    let mut staged = publication::StagedDirectoryFile::new(parent, directory)?;
    if !write_index(
        original.try_clone()?,
        staged.file_mut().try_clone()?,
        replacement,
        expand,
    )? {
        return Ok(false);
    }
    staged.file_mut().set_permissions(identity.permissions())?;
    publication::sync_publication_file(staged.file_mut())?;
    verify()?;
    if original.metadata()?.nlink() != 1 {
        return Err(invalid("ASDM index links changed during publication"));
    }
    staged.replace(name, &identity)?;
    Ok(true)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn verify_index_link(
    entry: &Path,
    linked: (u64, u64),
    target: &Path,
    directory: &Path,
    pinned_directory: (u64, u64),
) -> Result<(), StorageError> {
    if entry
        .symlink_metadata()
        .is_ok_and(|file| IndexVersion::from(&file).identity == linked)
        && entry.canonicalize().is_ok_and(|path| path == target)
        && directory
            .symlink_metadata()
            .is_ok_and(|dir| IndexVersion::from(&dir).identity == pinned_directory)
    {
        return Ok(());
    }
    Err(invalid("ASDM index link changed during publication"))
}

fn read_header(input: File, size: u64) -> Result<(AipsIo, IndexHeader), StorageError> {
    let mut io = AipsIo::new_read_only(input);
    let header = IndexHeader::read(&mut io, size)?;
    Ok((io, header))
}

fn write_index(
    input: File,
    output: File,
    replacement: &str,
    expand: PathExpansion,
) -> Result<bool, StorageError> {
    let size = input.metadata()?.len();
    let (io, header) = read_header(input, size)?;
    let Some(first) = header.bdfs.first() else {
        return Ok(false);
    };
    let first = expanded_name(first, expand)?;
    let directory = expand_name(dirname(&first), expand)?;
    let binary_directory = basename(&directory);
    expand_name(replacement, expand)?;
    let separator = if replacement.ends_with('/') { "" } else { "/" };
    let prefix = format!("{replacement}{separator}{binary_directory}/");
    // The validated prefix ends at the unchanged descriptor count. Copy
    // the suffix verbatim so no scale factors or offsets are regenerated.
    let mut input = io.into_inner_typed::<File>()?;
    let mut output = AipsIo::new_buffered_write_only_file(output);
    output.putstart(TYPE, header.version)?;
    output.put_bool(header.big_endian)?;
    output.putstart("Block", header.block_version)?;
    output.put_u32(header.bdfs.len() as u32)?;
    for path in &header.bdfs {
        let name = expanded_name(path, expand)?;
        let filename = basename(&name);
        if filename.is_empty() {
            return Err(invalid("ASDM BDF path has no filename"));
        }
        output.put_string(&format!("{prefix}{filename}"))?;
    }
    output.putend()?;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        output.put_u8_slice(&buffer[..count], false)?;
    }
    output.putend()?;
    output.close()?;
    Ok(true)
}

fn reference_name(
    first: Option<&Path>,
    expand: PathExpansion,
) -> Result<Option<String>, StorageError> {
    let Some(first) = first else {
        return Ok(None);
    };
    let name = expanded_name(first, expand)?;
    let directory = expand_name(dirname(&name), expand)?;
    Ok(Some(dirname(&directory).to_owned()))
}

fn expanded_name(path: &Path, expand: PathExpansion) -> Result<Cow<'_, str>, StorageError> {
    expand_name(
        path.to_str()
            .ok_or_else(|| invalid("non-UTF-8 ASDM BDF path"))?,
        expand,
    )
}

type PathExpansion = fn(&str) -> Option<Cow<'_, str>>;

fn expand_name(name: &str, expand: PathExpansion) -> Result<Cow<'_, str>, StorageError> {
    expand(name).ok_or_else(|| invalid("Path::expandName: recursive environment variable"))
}

// Match casacore Path's single trailing-separator removal without losing
// repeated separators or explicit dot components in the stored path.
fn dirname(name: &str) -> &str {
    let name = if name == "/" {
        name
    } else {
        name.strip_suffix('/').unwrap_or(name)
    };
    name.rfind('/').map_or(".", |slash| &name[..slash.max(1)])
}

fn basename(name: &str) -> &str {
    let name = name.strip_suffix('/').unwrap_or(name);
    name.rsplit('/').next().unwrap_or_default()
}
