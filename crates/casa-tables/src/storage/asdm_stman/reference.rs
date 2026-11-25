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
use crate::{TableError, TableWriteLock, expand_casacore_path, publication};

/// The index file for the lazy ASDM storage manager attached to a column.
#[derive(Debug)]
pub struct AsdmBdfIndex {
    table: PathBuf,
    path: PathBuf,
    #[cfg(unix)]
    first: Mutex<Option<CachedName>>,
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
    pub(crate) fn new(table: PathBuf, sequence: u32) -> Self {
        let path = table.join(format!("table.f{sequence}asdmindex"));
        Self {
            table,
            path,
            #[cfg(unix)]
            first: Mutex::new(None),
        }
    }

    /// Physical table directory whose write lock guards the index.
    pub fn table_path(&self) -> &Path {
        &self.table
    }

    /// Read the stored directory containing the first file's BDF directory.
    ///
    /// # Errors
    /// The index cannot be read or its filename block is malformed.
    pub fn reference(&self) -> Result<String, TableError> {
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
                return Ok(reference_name(current.path.as_deref())?);
            }
            let input = File::open(&self.path).map_err(StorageError::from)?;
            let file = input.metadata().map_err(StorageError::from)?;
            let header = read_header(input, file.len())?.1;
            let path = header.bdfs.into_iter().next();
            let result = reference_name(path.as_deref());
            *cached = Some(CachedName {
                version: IndexVersion::from(&file),
                path,
            });
            Ok(result?)
        }
        #[cfg(not(unix))]
        {
            let input = File::open(&self.path).map_err(StorageError::from)?;
            let size = input.metadata().map_err(StorageError::from)?.len();
            let (_, header) = read_header(input, size)?;
            Ok(reference_name(header.bdfs.first().map(PathBuf::as_path))?)
        }
    }

    /// Replace the ASDM directory in the index's stored BDF paths.
    ///
    /// Hold the physical table's write lock and verify the current binding.
    /// The files can be offline during this operation. The stage preserves the
    /// BDF endian marker, descriptor bytes, and mode of the existing index.
    ///
    /// # Errors
    /// The lock or index is invalid, or the replacement cannot be published.
    pub fn relocate(&self, replacement: &str, lock: &TableWriteLock) -> Result<String, TableError> {
        if !lock.matches_table_path(&self.table) {
            return Err(invalid("ASDM index write does not hold its physical table lock").into());
        }
        let changed = self.replace_names(replacement)?;
        if changed {
            // Only the independent index changed. Like AsdmStMan::writeIndex,
            // this leaves the parent table descriptor and sync data untouched.
            Ok(replacement.to_owned())
        } else {
            Ok(String::new())
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn replace_names(&self, replacement: &str) -> Result<bool, StorageError> {
        let parent = publication::open_directory_identity(&self.table)?;
        publication::check_write_access(&self.path)?;
        let name = self.path.file_name().expect("ASDM index file name");
        let original = publication::open_regular_directory_entry(&parent, name)?;
        let identity = original.metadata()?;
        let mut staged = publication::StagedDirectoryFile::new(&parent, &self.table)?;
        if !write_index(original, staged.file_mut().try_clone()?, replacement)? {
            return Ok(false);
        }
        staged.file_mut().set_permissions(identity.permissions())?;
        publication::sync_publication_file(staged.file_mut())?;
        staged.replace(name, &identity)?;
        Ok(true)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn replace_names(&self, replacement: &str) -> Result<bool, StorageError> {
        let original = self.path.symlink_metadata()?;
        if !original.is_file() {
            return Err(invalid("ASDM index replacement needs a regular index file"));
        }
        publication::check_write_access(&self.path)?;
        let staged = tempfile::NamedTempFile::new_in(&self.table)?;
        if !write_index(File::open(&self.path)?, staged.reopen()?, replacement)? {
            return Ok(false);
        }
        staged.as_file().set_permissions(original.permissions())?;
        publication::sync_publication_file(staged.as_file())?;
        let current = self.path.symlink_metadata()?;
        if !current.is_file()
            || current.len() != original.len()
            || current.modified()? != original.modified()?
        {
            return Err(invalid("ASDM index changed during publication"));
        }
        staged.persist(&self.path).map_err(|error| error.error)?;
        Ok(true)
    }
}

fn read_header(input: File, size: u64) -> Result<(AipsIo, IndexHeader), StorageError> {
    let mut io = AipsIo::new_read_only(input);
    let header = IndexHeader::read(&mut io, size)?;
    Ok((io, header))
}

fn write_index(input: File, output: File, replacement: &str) -> Result<bool, StorageError> {
    let size = input.metadata()?.len();
    let (io, header) = read_header(input, size)?;
    let Some(first) = header.bdfs.first() else {
        return Ok(false);
    };
    let first = expanded_name(first)?;
    let directory = expand_name(dirname(&first))?;
    let binary_directory = basename(&directory);
    expand_name(replacement)?;
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
        let name = expanded_name(path)?;
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

fn reference_name(first: Option<&Path>) -> Result<String, StorageError> {
    let Some(first) = first else {
        return Ok(String::new());
    };
    let name = expanded_name(first)?;
    let directory = expand_name(dirname(&name))?;
    Ok(dirname(&directory).to_owned())
}

fn expanded_name(path: &Path) -> Result<Cow<'_, str>, StorageError> {
    expand_name(
        path.to_str()
            .ok_or_else(|| invalid("non-UTF-8 ASDM BDF path"))?,
    )
}

fn expand_name(name: &str) -> Result<Cow<'_, str>, StorageError> {
    expand_casacore_path(name)
        .ok_or_else(|| invalid("Path::expandName: recursive environment variable"))
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
