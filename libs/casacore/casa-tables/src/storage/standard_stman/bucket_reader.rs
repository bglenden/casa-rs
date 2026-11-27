// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded read-ahead for small, adjacent source buckets.

use std::io::BufReader;

use super::*;

/// Retain a single bucket for rows or payloads that share it.
#[derive(Default)]
pub(super) struct LastBucket {
    number: Option<u32>,
    data: Vec<u8>,
}

impl LastBucket {
    pub(super) fn read(
        &mut self,
        file: &mut impl DataRead,
        header: &SsmHeader,
        number: u32,
    ) -> Result<&[u8], StorageError> {
        if self.number != Some(number) {
            self.data = read_bucket(file, header, number)?;
            self.number = Some(number);
        }
        Ok(&self.data)
    }
}

/// One operation owns this buffer; no cached bytes survive a table read.
pub(super) struct ReadBuckets<F> {
    file: BufReader<F>,
    length: u64,
    position: Option<u64>,
    bucket: Vec<u8>,
}

impl<F: DataRead> ReadBuckets<F> {
    pub(super) fn new(file: F) -> Result<Self, StorageError> {
        let length = file.data_len()?;
        Ok(Self {
            file: BufReader::with_capacity(32 * 1024, file),
            length,
            position: None,
            bucket: Vec::new(),
        })
    }

    pub(super) fn read(&mut self, header: &SsmHeader, number: u32) -> Result<&[u8], StorageError> {
        let offset = SSM_HEADER_SIZE + u64::from(number) * u64::from(header.bucket_size);
        let end = offset + u64::from(header.bucket_size);
        if end > self.length {
            return Err(StorageError::FormatMismatch(format!(
                "SSM bucket {number} out of range: offset={offset}, bucket_size={}, file_len={}",
                header.bucket_size, self.length
            )));
        }
        let size = header.bucket_size as usize;
        if size > self.bucket.len() {
            self.bucket
                .try_reserve_exact(size - self.bucket.len())
                .map_err(|error| {
                    StorageError::FormatMismatch(format!("cannot allocate SSM bucket: {error}"))
                })?;
        }
        let relative = self
            .position
            .take()
            .and_then(|position| i64::try_from(i128::from(offset) - i128::from(position)).ok());
        if let Some(relative) = relative {
            self.file.seek_relative(relative)?;
        } else {
            self.file.seek(SeekFrom::Start(offset))?;
        }
        self.bucket.resize(size, 0);
        self.file.read_exact(&mut self.bucket)?;
        self.position = Some(end);
        Ok(&self.bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_backward_and_invalid_bucket_reads_preserve_bytes() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("buckets");
        let bytes = (0..512 + 1024 * 128)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&path, &bytes).unwrap();
        let header = SsmHeader {
            bucket_size: 128,
            nr_buckets: 1024,
            pers_cache_size: 0,
            nr_free_buckets: 0,
            first_free_bucket: -1,
            nr_idx_buckets: 0,
            first_idx_bucket: -1,
            idx_bucket_offset: 0,
            last_string_bucket: -1,
            index_length: 0,
            nr_indices: 0,
            big_endian: false,
            io_order: ByteOrder::LittleEndian,
        };
        let mut reader = ReadBuckets::new(File::open(path).unwrap()).unwrap();
        for number in [2, 0, 2, 1, 1000, 500, 11, 0] {
            let start = SSM_HEADER_SIZE as usize + number as usize * 128;
            assert_eq!(
                reader.read(&header, number).unwrap(),
                &bytes[start..start + 128]
            );
        }
        assert!(reader.read(&header, 1024).is_err());
        assert_eq!(reader.read(&header, 0).unwrap(), &bytes[512..640]);
    }
}
