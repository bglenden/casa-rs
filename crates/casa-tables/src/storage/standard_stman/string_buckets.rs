// SPDX-License-Identifier: LGPL-3.0-or-later
//! The string buckets of a StandardStMan file being written: where long
//! scalar strings and string arrays are stored (casacore's `SSMStringHandler`).

use super::StorageError;

/// Bytes of a string bucket's header: four canonical (big-endian) ints,
/// the free link, the used length, the deleted count and the next bucket.
const HEADER_BYTES: usize = 16;

/// String buckets as they are filled.
///
/// Strings are only ever appended to the last bucket, so the ones before it
/// are final. A writer that streams its file writes them out as the next
/// one starts and releases them ([`Self::release`]), holding one bucket
/// whatever the row count; a writer that assembles the file in memory keeps
/// them all. Bucket numbers count the released buckets too.
#[derive(Debug, Default)]
pub(super) struct StringBuckets {
    /// Buckets written out and dropped.
    released: usize,
    /// Buckets still held, numbered from `released`; the last one is open.
    held: Vec<Vec<u8>>,
}

impl StringBuckets {
    /// Buckets made so far, released or held.
    pub(super) fn len(&self) -> usize {
        self.released + self.held.len()
    }

    /// The buckets still held, numbered from the count of released ones.
    pub(super) fn held(&self) -> &[Vec<u8>] {
        &self.held
    }

    /// Store `data` in the open bucket, or in a new one of `bucket_size`
    /// bytes when it does not fit, and return the bucket's number and the
    /// offset of `data` in its data area.
    pub(super) fn allocate(&mut self, data: &[u8], bucket_size: usize) -> (usize, usize) {
        let capacity = bucket_size - HEADER_BYTES;
        if let Some(open) = self.held.last_mut() {
            let used = i32::from_be_bytes([open[4], open[5], open[6], open[7]]) as usize;
            if used + data.len() <= capacity {
                open[HEADER_BYTES + used..HEADER_BYTES + used + data.len()].copy_from_slice(data);
                open[4..8].copy_from_slice(&((used + data.len()) as i32).to_be_bytes());
                return (self.len() - 1, used);
            }
        }
        let mut bucket = vec![0u8; bucket_size];
        // No free link; the data's length; nothing deleted; no next bucket.
        bucket[0..4].copy_from_slice(&(-1i32).to_be_bytes());
        bucket[4..8].copy_from_slice(&(data.len() as i32).to_be_bytes());
        bucket[8..12].copy_from_slice(&0i32.to_be_bytes());
        bucket[12..16].copy_from_slice(&(-1i32).to_be_bytes());
        bucket[HEADER_BYTES..HEADER_BYTES + data.len()].copy_from_slice(data);
        self.held.push(bucket);
        (self.len() - 1, 0)
    }

    /// Pass each finished bucket, and with `all` the open one too, to
    /// `write` with its number, in order, then drop them.
    pub(super) fn release(
        &mut self,
        all: bool,
        mut write: impl FnMut(usize, &[u8]) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        let finished = if all {
            self.held.len()
        } else {
            self.held.len().saturating_sub(1)
        };
        for (index, bucket) in self.held.drain(..finished).enumerate() {
            write(self.released + index, &bucket)?;
        }
        self.released += finished;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn released_buckets_keep_their_numbers() {
        let bucket_size = HEADER_BYTES + 8;
        let mut kept = StringBuckets::default();
        let mut streamed = StringBuckets::default();
        let mut written = Vec::new();
        for string in [&b"abcde"[..], b"fgh", b"ijklmn", b"op", b"qrstuvwx"] {
            let place = kept.allocate(string, bucket_size);
            assert_eq!(streamed.allocate(string, bucket_size), place);
            streamed
                .release(false, |number, bucket| {
                    written.push((number, bucket.to_vec()));
                    Ok(())
                })
                .unwrap();
            assert!(streamed.held().len() <= 1);
        }
        streamed
            .release(true, |number, bucket| {
                written.push((number, bucket.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(kept.len(), 3);
        assert_eq!(streamed.len(), 3);
        assert!(streamed.held().is_empty());
        let expected = kept.held().iter().cloned().enumerate().collect::<Vec<_>>();
        assert_eq!(written, expected);
    }
}
