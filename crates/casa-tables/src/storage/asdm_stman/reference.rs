// SPDX-License-Identifier: LGPL-3.0-or-later
//! Relocate BDF filenames without changing the visibility data or descriptors.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use casa_aipsio::AipsIo;

use super::{IndexHeader, StorageError, TYPE, invalid};
use crate::{TableError, TableWriteLock, publication};

/// The index file for the lazy ASDM storage manager attached to a column.
#[derive(Debug)]
pub struct AsdmBdfIndex {
    table: PathBuf,
    path: PathBuf,
}

impl AsdmBdfIndex {
    pub(crate) fn new(table: PathBuf, sequence: u32) -> Self {
        let path = table.join(format!("table.f{sequence}asdmindex"));
        Self { table, path }
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
        let (_, header) = self.read_header()?;
        Ok(location(&header.bdfs)
            .map_or_else(String::new, |(path, _)| path.to_string_lossy().into_owned()))
    }

    /// Replace the ASDM directory in the index's stored BDF paths.
    ///
    /// Hold the physical table's write lock and verify the current binding.
    /// The files can be offline during this operation. The stage preserves the
    /// BDF endian marker, descriptor bytes, and mode of the existing index.
    ///
    /// # Errors
    /// The lock or index is invalid, or the replacement cannot be published.
    pub fn relocate(
        &self,
        replacement: &str,
        lock: &mut TableWriteLock,
    ) -> Result<String, TableError> {
        if !lock.matches_table_path(&self.table) {
            return Err(invalid("ASDM index write does not hold its physical table lock").into());
        }
        let changed = self.replace_names(replacement)?;
        if changed {
            lock.record_write();
            Ok(replacement.to_owned())
        } else {
            Ok(String::new())
        }
    }

    fn read_header(&self) -> Result<(AipsIo, IndexHeader), StorageError> {
        read_header(File::open(&self.path)?)
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

fn read_header(input: File) -> Result<(AipsIo, IndexHeader), StorageError> {
    let size = input.metadata()?.len();
    let mut io = AipsIo::new_read_only(input);
    let header = IndexHeader::read(&mut io, size)?;
    Ok((io, header))
}

fn write_index(input: File, output: File, replacement: &str) -> Result<bool, StorageError> {
    let (io, header) = read_header(input)?;
    let Some((_, binary_directory)) = location(&header.bdfs) else {
        return Ok(false);
    };
    let new_directory = Path::new(replacement).join(binary_directory);
    // The validated prefix ends at the unchanged descriptor count. Copy
    // the suffix verbatim so no scale factors or offsets are regenerated.
    let mut input = io.into_inner_typed::<File>()?;
    let mut descriptors = Vec::new();
    input.read_to_end(&mut descriptors)?;
    let mut output = AipsIo::new_write_only(output);
    output.putstart(TYPE, header.version)?;
    output.put_bool(header.big_endian)?;
    output.putstart("Block", header.block_version)?;
    output.put_u32(header.bdfs.len() as u32)?;
    for path in &header.bdfs {
        let filename = path
            .file_name()
            .ok_or_else(|| invalid("ASDM BDF path has no filename"))?;
        let name = new_directory.join(filename);
        output.put_string(
            name.to_str()
                .ok_or_else(|| invalid("non-UTF-8 ASDM BDF path"))?,
        )?;
    }
    output.putend()?;
    output.put_u8_slice(&descriptors, false)?;
    output.putend()?;
    output.close()?;
    Ok(true)
}

fn location(names: &[PathBuf]) -> Option<(&Path, &std::ffi::OsStr)> {
    let directory = names.first()?.parent()?;
    Some((
        directory
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
        directory.file_name().unwrap_or_default(),
    ))
}
