// SPDX-License-Identifier: LGPL-3.0-or-later
//! The string buckets of a StandardStMan file being written: where long
//! scalar strings and string arrays are stored (casacore's `SSMStringHandler`).

use std::convert::Infallible;

use super::StorageError;

/// Bytes of a string bucket's header: four canonical (big-endian) ints,
/// the free link (zero while the bucket is in use, as casacore leaves it:
/// its bucket cache links a bucket into its free list there once it is
/// freed), the used length, the deleted count (the bytes not in use) and
/// the next bucket of a string that continues there.
const HEADER_BYTES: usize = 16;

/// Free bytes of the open bucket below which data that does not fit
/// starts a new bucket rather than continuing from it (casacore's
/// `SSMStringHandler::put`).
const SPLIT_FREE_BYTES: usize = 50;

/// String buckets as they are filled.
///
/// Strings are only ever appended to the last bucket, so the ones before it
/// are final. A writer that streams its file writes each one out as the
/// next one starts and drops it ([`Self::stream`]), so it holds one bucket
/// whatever the row count and however many buckets a string spans; a writer
/// that assembles the file in memory keeps them all ([`Self::allocate`]).
/// Bucket numbers count the released buckets too.
#[derive(Debug, Default)]
pub(super) struct StringBuckets {
    /// The file's bucket number of the first, which a bucket's link to the
    /// next names (casacore's bucket numbers count the data buckets).
    first: usize,
    /// Buckets written out and dropped.
    released: usize,
    /// Buckets still held, numbered from `released`; the last one is open.
    held: Vec<Vec<u8>>,
}

impl StringBuckets {
    /// No buckets yet, the first to be the file's bucket `first`, after
    /// its data buckets.
    pub(super) fn after(first: usize) -> Self {
        Self {
            first,
            ..Self::default()
        }
    }

    /// Buckets made so far, released or held.
    pub(super) fn len(&self) -> usize {
        self.released + self.held.len()
    }

    /// The buckets still held, numbered from the count of released ones.
    pub(super) fn held(&self) -> &[Vec<u8>] {
        &self.held
    }

    /// Store `data` in buckets of `bucket_size` bytes, keeping every
    /// bucket, and return where it starts ([`Self::place`]).
    pub(super) fn allocate(&mut self, data: &[u8], bucket_size: usize) -> (usize, usize) {
        let Ok(start) = self.place(data, bucket_size, |_| Ok::<_, Infallible>(()));
        start
    }

    /// Store `data` in buckets of `bucket_size` bytes, passing each bucket
    /// to `write` with its number as soon as it is finished, before the
    /// next one starts, and dropping it; return where `data` starts
    /// ([`Self::place`]). Only the open bucket is held afterwards.
    pub(super) fn stream(
        &mut self,
        data: &[u8],
        bucket_size: usize,
        mut write: impl FnMut(usize, &[u8]) -> Result<(), StorageError>,
    ) -> Result<(usize, usize), StorageError> {
        self.place(data, bucket_size, |buckets| buckets.release(&mut write))
    }

    /// Store `data` and return the number (among these buckets) of the
    /// bucket it starts in and its offset in that bucket's data area, as
    /// casacore's `SSMStringHandler::put` places it: in the open bucket,
    /// unless it does not fit there and fewer than [`SPLIT_FREE_BYTES`] are
    /// free, when it starts a new one. What does not fit continues in new
    /// buckets, each linked by its file bucket number from the full one
    /// before it, so only the last bucket is ever open. `finish` is given
    /// the buckets each time the open one is final, before the next starts.
    fn place<E>(
        &mut self,
        data: &[u8],
        bucket_size: usize,
        mut finish: impl FnMut(&mut Self) -> Result<(), E>,
    ) -> Result<(usize, usize), E> {
        let capacity = bucket_size - HEADER_BYTES;
        let free = self.held.last().map(|open| capacity - used(open));
        if free.is_none_or(|free| data.len() > free && free < SPLIT_FREE_BYTES) {
            self.open_next(bucket_size, &mut finish)?;
        }
        let start = (
            self.len() - 1,
            self.held.last().map_or(0, |open| used(open)),
        );
        let mut rest = data;
        loop {
            // The file bucket number a continuation bucket would take.
            let next = (self.first + self.len()) as i32;
            let open = self.held.last_mut().expect("an open bucket");
            let used = used(open);
            let (now, later) = rest.split_at(rest.len().min(capacity - used));
            open[HEADER_BYTES + used..HEADER_BYTES + used + now.len()].copy_from_slice(now);
            let used = used + now.len();
            open[4..8].copy_from_slice(&(used as i32).to_be_bytes());
            open[8..12].copy_from_slice(&((capacity - used) as i32).to_be_bytes());
            if later.is_empty() {
                return Ok(start);
            }
            open[12..16].copy_from_slice(&next.to_be_bytes());
            self.open_next(bucket_size, &mut finish)?;
            rest = later;
        }
    }

    /// Finish the open bucket, if there is one, and start a new, empty one
    /// after it: a zero free link, nothing used, all of it unused, no next
    /// bucket.
    fn open_next<E>(
        &mut self,
        bucket_size: usize,
        finish: &mut impl FnMut(&mut Self) -> Result<(), E>,
    ) -> Result<(), E> {
        if !self.held.is_empty() {
            finish(self)?;
        }
        let mut bucket = vec![0u8; bucket_size];
        bucket[8..12].copy_from_slice(&((bucket_size - HEADER_BYTES) as i32).to_be_bytes());
        bucket[12..16].copy_from_slice(&(-1i32).to_be_bytes());
        self.held.push(bucket);
        Ok(())
    }

    /// Pass each held bucket, the open one included, to `write` with its
    /// number, in order, then drop them.
    pub(super) fn release(
        &mut self,
        mut write: impl FnMut(usize, &[u8]) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        let count = self.held.len();
        for (index, bucket) in self.held.drain(..).enumerate() {
            write(self.released + index, &bucket)?;
        }
        self.released += count;
        Ok(())
    }
}

/// The bytes of `bucket`'s data area in use.
fn used(bucket: &[u8]) -> usize {
    i32::from_be_bytes([bucket[4], bucket[5], bucket[6], bucket[7]]) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Store `data` in `streamed`, recording each bucket written, and check
    /// that it is placed as in `kept` and that only the open bucket is held.
    fn stream_beside(
        (kept, streamed): (&mut StringBuckets, &mut StringBuckets),
        data: &[u8],
        bucket_size: usize,
        written: &mut Vec<(usize, Vec<u8>)>,
    ) {
        let place = kept.allocate(data, bucket_size);
        let streamed_place = streamed
            .stream(data, bucket_size, |number, bucket| {
                written.push((number, bucket.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(streamed_place, place);
        assert_eq!(streamed.held().len(), 1);
        assert_eq!(written.len() + 1, streamed.len());
    }

    #[test]
    fn released_buckets_keep_their_numbers() {
        let bucket_size = HEADER_BYTES + 8;
        let mut kept = StringBuckets::default();
        let mut streamed = StringBuckets::default();
        let mut written = Vec::new();
        for string in [&b"abcde"[..], b"fgh", b"ijklmn", b"op", b"qrstuvwx"] {
            stream_beside(
                (&mut kept, &mut streamed),
                string,
                bucket_size,
                &mut written,
            );
        }
        streamed
            .release(|number, bucket| {
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

    /// A bucket header's four canonical ints.
    fn header(bucket: &[u8]) -> [i32; 4] {
        [0, 4, 8, 12].map(|at| i32::from_be_bytes(bucket[at..at + 4].try_into().unwrap()))
    }

    /// `length` bytes from bucket `number`'s data area at `offset`, then
    /// along its chain, of `buckets` from the file's bucket `first` on.
    fn chained(
        (buckets, first): (&[Vec<u8>], usize),
        (number, offset): (usize, usize),
        length: usize,
    ) -> Vec<u8> {
        let (mut number, mut offset, mut read) = (number, offset, Vec::new());
        while read.len() < length {
            let bucket = &buckets[number];
            let data = &bucket[HEADER_BYTES + offset..];
            read.extend_from_slice(&data[..data.len().min(length - read.len())]);
            if read.len() < length {
                let next = usize::try_from(header(bucket)[3]).expect("a next bucket");
                number = next - first;
                offset = 0;
            }
        }
        read
    }

    /// Data longer than a bucket fills it and continues in new buckets,
    /// each linked by its file bucket number (after five data buckets) from
    /// the full one before it; every header states its used and unused
    /// bytes, as casacore's `SSMStringHandler` writes them.
    #[test]
    fn long_data_continues_in_linked_buckets() {
        let bucket_size = HEADER_BYTES + 100;
        let mut buckets = StringBuckets::after(5);
        let long: Vec<u8> = (0..250u32).map(|byte| byte as u8).collect();
        let place = buckets.allocate(&long, bucket_size);
        assert_eq!(place, (0, 0));
        let held = buckets.held();
        assert_eq!(held.len(), 3);
        assert_eq!(header(&held[0]), [0, 100, 0, 6]);
        assert_eq!(header(&held[1]), [0, 100, 0, 7]);
        assert_eq!(header(&held[2]), [0, 50, 50, -1]);
        assert_eq!(chained((held, 5), place, long.len()), long);
    }

    /// Data that does not fit starts in the open bucket where plenty is
    /// free, 70 bytes, and in a new bucket where little is, 40 bytes.
    #[test]
    fn data_that_does_not_fit_continues_from_a_roomy_bucket_only() {
        let bucket_size = HEADER_BYTES + 100;
        let mut buckets = StringBuckets::default();
        let data = |length: usize, fill: u8| vec![fill; length];
        let placed: Vec<_> = [(30, b'a'), (90, b'b'), (40, b'c'), (85, b'd')]
            .into_iter()
            .map(|(length, fill)| {
                let data = data(length, fill);
                (buckets.allocate(&data, bucket_size), data)
            })
            .collect();
        let places: Vec<_> = placed.iter().map(|(place, _)| *place).collect();
        // 70 bytes free: the 90 start there; 40 free: the 85 start anew.
        assert_eq!(places, [(0, 0), (0, 30), (1, 20), (2, 0)]);
        let held = buckets.held();
        assert_eq!(header(&held[0]), [0, 100, 0, 1]);
        assert_eq!(header(&held[1]), [0, 60, 40, -1]);
        assert_eq!(header(&held[2]), [0, 85, 15, -1]);
        for (place, data) in &placed {
            assert_eq!(&chained((held, 0), *place, data.len()), data);
        }
    }

    /// The split rule at its edge, in a 100-byte data area: with exactly 50
    /// bytes free, 51 bytes start there and continue; with 49 free, 50
    /// bytes start a new bucket; and 49 bytes that fit the last 49 stay.
    #[test]
    fn fifty_free_bytes_continue_and_forty_nine_start_anew() {
        /// Store `first` bytes, then `second`, and check where the second
        /// start and the buckets' headers.
        fn check((first, second): (usize, usize), place: (usize, usize), headers: &[[i32; 4]]) {
            let bucket_size = HEADER_BYTES + 100;
            let mut buckets = StringBuckets::default();
            let before = vec![b'a'; first];
            let data = vec![b'b'; second];
            assert_eq!(buckets.allocate(&before, bucket_size), (0, 0));
            let placed = buckets.allocate(&data, bucket_size);
            assert_eq!(placed, place, "{second} bytes after {first}");
            let held = buckets.held();
            let stated: Vec<_> = held.iter().map(|bucket| header(bucket)).collect();
            assert_eq!(stated, headers, "{second} bytes after {first}");
            assert_eq!(chained((held, 0), (0, 0), first), before);
            assert_eq!(chained((held, 0), placed, second), data);
        }
        check((50, 51), (0, 50), &[[0, 100, 0, 1], [0, 1, 99, -1]]);
        check((51, 50), (1, 0), &[[0, 51, 49, -1], [0, 50, 50, -1]]);
        check((51, 49), (0, 51), &[[0, 100, 0, -1]]);
    }

    /// A writer that streams its buckets writes what one that keeps them
    /// holds, with data longer than a bucket among them, writing each
    /// bucket as it finishes, so that it holds only the open one.
    #[test]
    fn streamed_long_data_matches_kept() {
        let bucket_size = HEADER_BYTES + 64;
        let mut kept = StringBuckets::default();
        let mut streamed = StringBuckets::default();
        let mut written = Vec::new();
        for (length, fill) in [(20, b'a'), (300, b'b'), (10, b'c'), (64, b'd'), (129, b'e')] {
            stream_beside(
                (&mut kept, &mut streamed),
                &vec![fill; length],
                bucket_size,
                &mut written,
            );
        }
        streamed
            .release(|number, bucket| {
                written.push((number, bucket.to_vec()));
                Ok(())
            })
            .unwrap();
        let expected = kept.held().iter().cloned().enumerate().collect::<Vec<_>>();
        assert_eq!(written, expected);
    }
}
