// SPDX-License-Identifier: LGPL-3.0-or-later
//! The index and header of a StandardStMan file, written after its data and
//! string buckets.
//!
//! The index is casacore's `SSMIndex` AipsIO object: for each data bucket
//! its last row and its number. `SSMBase` keeps it in index buckets after
//! the data and string buckets, each of which starts with an 8-byte chain
//! header (the next index bucket twice, big-endian, -1 for the last: a
//! check number and the link) before its share of the object's bytes. The
//! writer generates the object as it streams it into those buckets, holding
//! one bucket whatever the number of data buckets.
//!
//! An index that fits half an index bucket's data area is stated to start
//! at the chain header's end ([`index_bucket_offset`]), as
//! `SSMBase::writeIndex` states it: casacore then rewrites it into the
//! other half of that bucket, not into a new one.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

use casa_aipsio::{AipsIo, ByteOrder};

use super::{StorageError, bucket_offset, last_string_bucket, serialize_ssm_header};

/// Bytes of an index bucket's chain header.
const CHAIN_BYTES: usize = 8;

/// The header's index bucket offset for an index of `index_length` bytes in
/// buckets of `bucket_bytes` (`SSMBase::itsIdxBucketOffset`): the chain
/// header's end when the index fits half a bucket's data area, so that
/// casacore alternates between the halves when it rewrites it, and 0, the
/// index spanning whole buckets, otherwise.
pub(super) fn index_bucket_offset(index_length: usize, bucket_bytes: usize) -> u32 {
    if index_length <= (bucket_bytes - CHAIN_BYTES) / 2 {
        CHAIN_BYTES as u32
    } else {
        0
    }
}

/// Write the chain header of an index bucket followed by index bucket
/// `next` (-1 for none): `next` twice, as casacore writes it.
pub(super) fn write_chain_header(bucket: &mut [u8], next: i32) {
    bucket[0..4].copy_from_slice(&next.to_be_bytes());
    bucket[4..8].copy_from_slice(&next.to_be_bytes());
}

/// What the index and header of a StandardStMan file describe.
pub(super) struct IndexLayout {
    pub(super) nrrow: usize,
    pub(super) ncol: usize,
    pub(super) rows_per_bucket: u32,
    pub(super) bucket_size: u32,
    pub(super) nr_data_buckets: usize,
    pub(super) nr_string_buckets: usize,
}

/// Write the index buckets after the data and string buckets, then the
/// header, and size the file to its buckets.
pub(super) fn write_index_and_header(
    file: &mut File,
    layout: IndexLayout,
    big_endian: bool,
) -> Result<(), StorageError> {
    let IndexLayout {
        nrrow,
        ncol,
        rows_per_bucket,
        bucket_size,
        nr_data_buckets,
        nr_string_buckets,
    } = layout;
    let bucket_bytes = bucket_size as usize;
    let first_idx_bucket = nr_data_buckets + nr_string_buckets;
    let bucket_rows = rows_per_bucket as usize;
    let (nr_idx_buckets, index_length) =
        IndexBucketSink::new(file.try_clone()?, first_idx_bucket, bucket_bytes)
            .write_index(
                nr_data_buckets as u32,
                rows_per_bucket,
                ncol as i32,
                (0..nr_data_buckets)
                    .map(|bucket| (((bucket + 1) * bucket_rows - 1).min(nrrow - 1)) as u64),
                0..nr_data_buckets as u32,
                big_endian,
            )?
            .finish()?;
    let nr_buckets = first_idx_bucket + nr_idx_buckets;
    let header = serialize_ssm_header(
        bucket_size,
        nr_buckets as u32,
        0,
        -1,
        nr_idx_buckets as u32,
        first_idx_bucket as i32,
        index_bucket_offset(index_length, bucket_bytes),
        last_string_bucket(nr_data_buckets, nr_string_buckets),
        index_length as u32,
        1,
        big_endian,
    )?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    // Bucket bytes nothing wrote are zero.
    file.set_len(bucket_offset(nr_buckets, bucket_bytes))?;
    Ok(())
}

/// The `SSMIndex` object's byte stream, laid onto the data areas of
/// consecutive index buckets of a file.
///
/// It is written front to back, except that AipsIO goes back to fill in
/// each object's length once the object ends. The bucket being filled is
/// held until the stream moves into the next; a length that falls in an
/// earlier bucket is written straight to the file.
struct IndexBucketSink {
    file: File,
    first_bucket: usize,
    bucket_bytes: usize,
    /// Position in the stream.
    position: u64,
    /// Length of the stream so far.
    end: u64,
    /// The index bucket being filled (counted from the first) and its bytes.
    open: Option<(usize, Vec<u8>)>,
}

impl IndexBucketSink {
    fn new(file: File, first_bucket: usize, bucket_bytes: usize) -> Self {
        Self {
            file,
            first_bucket,
            bucket_bytes,
            position: 0,
            end: 0,
            open: None,
        }
    }

    /// Stream the `SSMIndex` of `n_used` data buckets of `rows_per_bucket`
    /// rows, their last rows and their bucket numbers, in the file's byte
    /// order.
    fn write_index(
        self,
        n_used: u32,
        rows_per_bucket: u32,
        nr_columns: i32,
        last_row: impl ExactSizeIterator<Item = u64>,
        bucket_number: impl ExactSizeIterator<Item = u32>,
        big_endian: bool,
    ) -> Result<Self, StorageError> {
        let order = if big_endian {
            ByteOrder::BigEndian
        } else {
            ByteOrder::LittleEndian
        };
        let mut io = AipsIo::new_write_only_with_order(self, order);
        // A top-level object, so it starts with AipsIO's magic number.
        io.putstart("SSMIndex", 2)?;
        io.put_u32(n_used)?;
        io.put_u32(rows_per_bucket)?;
        io.put_i32(nr_columns)?;
        // The free-space map: an empty SimpleOrderedMap.
        io.putstart("SimpleOrderedMap", 1)?;
        io.put_i32(0)?;
        io.put_u32(0)?;
        io.put_u32(0)?;
        io.putend()?;
        // lastRow: a Block<Int64>.
        io.putstart("Block", 1)?;
        io.put_u32(last_row.len() as u32)?;
        for row in last_row {
            io.put_u64(row)?;
        }
        io.putend()?;
        // bucketNumber: a Block<uInt>.
        io.putstart("Block", 1)?;
        io.put_u32(bucket_number.len() as u32)?;
        for bucket in bucket_number {
            io.put_u32(bucket)?;
        }
        io.putend()?;
        io.putend()?;
        Ok(io.into_inner_typed()?)
    }

    /// End the chain at the last bucket and write it; return the number of
    /// index buckets and the length of the index.
    fn finish(mut self) -> io::Result<(usize, usize)> {
        let buckets = match &mut self.open {
            Some((bucket, bytes)) => {
                write_chain_header(bytes, -1);
                *bucket + 1
            }
            None => 0,
        };
        self.write_open()?;
        Ok((buckets, self.end as usize))
    }

    /// File offset of index bucket `bucket`, counted from the first.
    fn bucket_start(&self, bucket: usize) -> u64 {
        bucket_offset(self.first_bucket + bucket, self.bucket_bytes)
    }

    fn write_open(&mut self) -> io::Result<()> {
        if let Some((bucket, bytes)) = &self.open {
            self.file
                .seek(SeekFrom::Start(self.bucket_start(*bucket)))?;
            self.file.write_all(bytes)?;
        }
        Ok(())
    }

    /// Write the open bucket and start index bucket `bucket`, the next one,
    /// chained to the one after it.
    fn open_bucket(&mut self, bucket: usize) -> io::Result<()> {
        let next = self.open.as_ref().map_or(0, |(open, _)| open + 1);
        if bucket != next {
            return Err(io::Error::other(format!(
                "SSM index stream skips from index bucket {next} to {bucket}"
            )));
        }
        self.write_open()?;
        let mut bytes = self.open.take().map_or_else(Vec::new, |(_, bytes)| bytes);
        bytes.clear();
        bytes.resize(self.bucket_bytes, 0);
        write_chain_header(&mut bytes, (self.first_bucket + bucket + 1) as i32);
        self.open = Some((bucket, bytes));
        Ok(())
    }
}

impl Write for IndexBucketSink {
    /// Write as much of `buf` as fits in the index bucket it starts in.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let data_bytes = (self.bucket_bytes - CHAIN_BYTES) as u64;
        let bucket = (self.position / data_bytes) as usize;
        let within = (self.position % data_bytes) as usize;
        let count = buf.len().min(data_bytes as usize - within);
        let chunk = &buf[..count];
        let open = self.open.as_ref().map(|(open, _)| *open);
        if open.is_some_and(|open| bucket < open) {
            let offset = self.bucket_start(bucket) + (CHAIN_BYTES + within) as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(chunk)?;
        } else {
            if open != Some(bucket) {
                self.open_bucket(bucket)?;
            }
            let (_, bytes) = self.open.as_mut().expect("an open index bucket");
            bytes[CHAIN_BYTES + within..CHAIN_BYTES + within + count].copy_from_slice(chunk);
        }
        self.position += count as u64;
        self.end = self.end.max(self.position);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for IndexBucketSink {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "SSM index seek out of range");
        self.position = match position {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(delta) => self
                .position
                .checked_add_signed(delta)
                .ok_or_else(invalid)?,
            SeekFrom::End(delta) => self.end.checked_add_signed(delta).ok_or_else(invalid)?,
        };
        Ok(self.position)
    }
}

impl Read for IndexBucketSink {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the SSM index stream is written, not read",
        ))
    }
}

#[cfg(test)]
mod tests {
    //! The streamed index buckets hold the bytes of the serialized index cut
    //! into the buckets' data areas, however the index falls across them.

    use super::super::serialize_ssm_index;
    use super::*;

    /// The index buckets as the in-memory writers lay them out: the
    /// serialized index in chunks, each after its chain header.
    fn chunked(index: &[u8], first_bucket: usize, bucket_bytes: usize) -> Vec<u8> {
        let chunks = index.chunks(bucket_bytes - CHAIN_BYTES).collect::<Vec<_>>();
        let mut bytes = Vec::new();
        for (number, chunk) in chunks.iter().enumerate() {
            let mut bucket = vec![0u8; bucket_bytes];
            let next = if number + 1 < chunks.len() {
                (first_bucket + number + 1) as i32
            } else {
                -1
            };
            bucket[0..4].copy_from_slice(&next.to_be_bytes());
            bucket[4..8].copy_from_slice(&next.to_be_bytes());
            bucket[CHAIN_BYTES..CHAIN_BYTES + chunk.len()].copy_from_slice(chunk);
            bytes.extend_from_slice(&bucket);
        }
        bytes
    }

    /// An index that fits half an index bucket's data area is stated to
    /// start after the chain header, as `SSMBase::writeIndex` states it; a
    /// longer one spans whole buckets. A 32 KiB bucket holds 32,760 bytes
    /// of index.
    #[test]
    fn an_index_within_half_a_bucket_starts_after_the_chain_header() {
        assert_eq!(index_bucket_offset(0, 32_768), 8);
        assert_eq!(index_bucket_offset(16_380, 32_768), 8);
        assert_eq!(index_bucket_offset(16_381, 32_768), 0);
        assert_eq!(index_bucket_offset(40_000, 32_768), 0);
        // 97-byte buckets: 89 bytes of index, of which half is 44.
        assert_eq!(index_bucket_offset(44, 97), 8);
        assert_eq!(index_bucket_offset(45, 97), 0);
    }

    #[test]
    fn streamed_index_buckets_hold_the_serialized_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (rows_per_bucket, ncol, first_bucket) = (7u32, 3usize, 2usize);
        let mut bucket_counts = Vec::new();
        let mut ends_on_a_boundary = false;
        for big_endian in [false, true] {
            for bucket_bytes in [40usize, 64, 97, 128, 200] {
                for nr_data_buckets in 0..=64usize {
                    let nrrow = (nr_data_buckets * rows_per_bucket as usize).saturating_sub(3);
                    let last_row = (0..nr_data_buckets)
                        .map(|bucket| {
                            (((bucket + 1) * rows_per_bucket as usize - 1).min(nrrow - 1)) as u64
                        })
                        .collect::<Vec<_>>();
                    let bucket_number = (0..nr_data_buckets as u32).collect::<Vec<_>>();
                    let index = serialize_ssm_index(
                        nr_data_buckets as u32,
                        rows_per_bucket,
                        ncol as i32,
                        &last_row,
                        &bucket_number,
                        big_endian,
                    )
                    .unwrap();
                    let expected = chunked(&index, first_bucket, bucket_bytes);

                    let path = dir.path().join("index");
                    let file = File::create(&path).unwrap();
                    let (buckets, length) = IndexBucketSink::new(file, first_bucket, bucket_bytes)
                        .write_index(
                            nr_data_buckets as u32,
                            rows_per_bucket,
                            ncol as i32,
                            last_row.iter().copied(),
                            bucket_number.iter().copied(),
                            big_endian,
                        )
                        .unwrap()
                        .finish()
                        .unwrap();
                    let case = format!(
                        "{nr_data_buckets} data buckets, {bucket_bytes}-byte buckets, \
                         big_endian {big_endian}"
                    );
                    assert_eq!(length, index.len(), "{case}");
                    assert_eq!(buckets * bucket_bytes, expected.len(), "{case}");
                    let written = std::fs::read(&path).unwrap();
                    let start = bucket_offset(first_bucket, bucket_bytes) as usize;
                    assert_eq!(written.len(), start + expected.len(), "{case}");
                    assert!(written[..start].iter().all(|&byte| byte == 0), "{case}");
                    assert!(written[start..] == expected[..], "{case}: bytes differ");
                    bucket_counts.push(buckets);
                    ends_on_a_boundary |= length.is_multiple_of(bucket_bytes - CHAIN_BYTES);
                }
            }
        }
        for count in [1, 2] {
            assert!(
                bucket_counts.contains(&count),
                "an index of {count} buckets"
            );
        }
        assert!(
            bucket_counts.iter().any(|&count| count >= 10),
            "a long index"
        );
        assert!(ends_on_a_boundary, "an index that fills its last bucket");
    }
}
