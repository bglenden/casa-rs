// SPDX-License-Identifier: LGPL-3.0-or-later
//! The same read interface for a separate data-manager file and a logical
//! file in a casacore MultiFile container.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use casa_aipsio::AipsIo;

use super::multifile::{MultiFile, MultiFileEntry};

pub(super) enum DataFile {
    Separate(File),
    Packed(MultiFileEntry),
}

impl DataFile {
    pub(super) fn is_file(path: &Path) -> bool {
        path.is_file() || path.parent().is_some_and(|parent| parent.join("table.mf").is_file())
            && Self::open(path).is_ok()
    }

    pub(super) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let missing = match File::open(path) {
            Ok(file) => return Ok(Self::Separate(file)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => error,
            Err(error) => return Err(error),
        };
        let Some((directory, member)) = path.parent().zip(path.file_name().and_then(|v| v.to_str())) else {
            return Err(missing);
        };
        if !member.starts_with("table.f") {
            return Err(missing);
        }
        let container = directory.join("table.mf");
        if !container.is_file() {
            return Err(missing);
        }
        MultiFile::open(&container)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            .entry(member)
            .map(Self::Packed)
            .ok_or(missing)
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
