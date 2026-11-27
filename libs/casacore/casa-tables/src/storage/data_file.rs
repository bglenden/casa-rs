// SPDX-License-Identifier: LGPL-3.0-or-later
//! The same read interface for a separate data-manager file and a logical
//! file in a casacore MultiFile container.

use std::fs::{self, File, Metadata};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use casa_aipsio::AipsIo;

use super::multifile::{MultiFile, MultiFileEntry, manager_sequence, supports_multifile};
use super::table_control::{TableDatResult, read_table_dat_dispatch};

pub(super) enum DataFile {
    Separate(File),
    Packed(MultiFileEntry),
}

impl DataFile {
    pub(super) fn is_file(path: &Path) -> bool {
        match packed_member(path) {
            Ok(Some(_)) => true,
            Ok(None) => path.is_file(),
            Err(_) => false,
        }
    }

    pub(super) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        if let Some(member) = packed_member(path)? {
            return Ok(Self::Packed(member));
        }
        File::open(path).map(Self::Separate)
    }

    pub(super) fn metadata(path: &Path) -> io::Result<Metadata> {
        match packed_location(path)? {
            Some((container, _)) => fs::metadata(container),
            None => fs::metadata(path),
        }
    }

    pub(super) fn aips(path: impl AsRef<Path>) -> io::Result<AipsIo> {
        Ok(AipsIo::new_read_only(Self::open(path)?))
    }

    pub(super) fn read_all(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
        let mut file = Self::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    pub(super) fn len(&self) -> io::Result<u64> {
        match self {
            Self::Separate(file) => Ok(file.metadata()?.len()),
            Self::Packed(file) => Ok(file.len()),
        }
    }

    #[cfg(unix)]
    pub(super) fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;

        match self {
            Self::Separate(file) => file.read_exact_at(bytes, offset),
            Self::Packed(file) => file.read_exact_at(bytes, offset),
        }
    }
}

fn packed_member(path: &Path) -> io::Result<Option<MultiFileEntry>> {
    let Some((container, member)) = packed_location(path)? else {
        return Ok(None);
    };
    MultiFile::open(&container)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        .entry(member)
        .map(Some)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing MultiFile member {member}"),
            )
        })
}

fn packed_location(path: &Path) -> io::Result<Option<(std::path::PathBuf, &str)>> {
    let Some((directory, member)) = path
        .parent()
        .zip(path.file_name().and_then(|name| name.to_str()))
    else {
        return Ok(None);
    };
    let Some(sequence) = manager_sequence(member) else {
        return Ok(None);
    };
    let container = directory.join("table.mf");
    if !container.is_file() {
        return Ok(None);
    }
    let table = read_table_dat_dispatch(&directory.join(super::TABLE_CONTROL_FILE))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let TableDatResult::Plain(table) = table else {
        return Ok(None);
    };
    if !table.uses_multifile()
        || !table
            .column_set
            .data_managers
            .iter()
            .any(|manager| manager.seq_nr == sequence && supports_multifile(&manager.type_name))
    {
        return Ok(None);
    }
    Ok(Some((container, member)))
}

pub(super) trait DataRead: Read + Seek {
    fn data_len(&self) -> io::Result<u64>;
}

impl DataRead for File {
    fn data_len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
}

impl DataRead for DataFile {
    fn data_len(&self) -> io::Result<u64> {
        self.len()
    }
}

impl Read for DataFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Separate(file) => file.read(bytes),
            Self::Packed(file) => file.read(bytes),
        }
    }
}

impl Seek for DataFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Separate(file) => file.seek(position),
            Self::Packed(file) => file.seek(position),
        }
    }
}

impl Write for DataFile {
    fn write(&mut self, _data: &[u8]) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
