// SPDX-License-Identifier: LGPL-3.0-or-later
//! Writing a streamed tiled column's bands of tiles on a thread of their
//! own, so the thread that lays rows into tiles never waits on the file.
//!
//! A band is every tile of one row of tiles, written in file order. A
//! writer that holds two bands fills one while this thread writes the
//! other, and swaps a filled band for the band written before it; one that
//! holds one band, when memory is short, waits for it to be written. Writing copies a band into the page cache at the rate the
//! system allows (about 3.3 GB/s on an M4, whatever the writers), so laying
//! out the next band beside it takes that time off the column's critical
//! path (#855: at 4096 channels, 2.2 s of every 12.4 s the DATA writer
//! spent).

use std::io::{BufWriter, Write as IoWrite};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;
use std::time::Instant;

use super::CountingFile;
use crate::storage::StorageError;

/// What the writing thread did, for the column's telemetry.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct BandStats {
    pub(super) write_seconds: f64,
    pub(super) bytes_written: usize,
    pub(super) logical_write_calls: usize,
    pub(super) max_logical_write_bytes: usize,
}

/// A band: every tile of one row of tiles, in file order.
type Band = Vec<Vec<u8>>;

/// What the writing thread gives back: the file's writer and what it did.
type Written = Result<(BufWriter<CountingFile>, BandStats), StorageError>;

/// The thread that writes a column's bands, and the bands passing to and
/// from it.
pub(super) struct BandWriter {
    filled: Option<SyncSender<Band>>,
    written: Receiver<Band>,
    thread: Option<JoinHandle<Written>>,
}

impl BandWriter {
    /// Start writing to `writer`. With a `spare` band the laying thread
    /// fills it while its first band is written; without one, each band
    /// comes back once written, so laying and writing take turns as they
    /// did on one thread.
    pub(super) fn start(
        writer: BufWriter<CountingFile>,
        spare: Option<Band>,
    ) -> Result<Self, StorageError> {
        let (filled, bands) = mpsc::sync_channel::<Band>(1);
        let (returns, written) = mpsc::sync_channel::<Band>(2);
        if let Some(spare) = spare {
            returns
                .send(spare)
                .expect("the receiver is held, and the channel has room");
        }
        let thread = std::thread::Builder::new()
            .name("tiled-band-writer".to_string())
            .spawn(move || write_bands(writer, &bands, &returns))?;
        Ok(Self {
            filled: Some(filled),
            written,
            thread: Some(thread),
        })
    }

    /// Hand `band`, filled, over to be written, and take back a written band
    /// to fill: waits while the thread is still writing the band before.
    pub(super) fn swap(&mut self, band: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, StorageError> {
        let sent = self
            .filled
            .as_ref()
            .expect("bands are swapped only before the writer finishes")
            .send(band);
        match sent.ok().and_then(|()| self.written.recv().ok()) {
            Some(band) => Ok(band),
            // The thread stopped: it failed, and says why when joined.
            None => Err(self.join().err().unwrap_or_else(|| {
                StorageError::FormatMismatch("the band writer stopped without an error".into())
            })),
        }
    }

    /// Write every band handed over, and give back the file's writer, not
    /// flushed, and what the thread did.
    pub(super) fn finish(mut self) -> Result<(BufWriter<CountingFile>, BandStats), StorageError> {
        self.join()
    }

    fn join(&mut self) -> Result<(BufWriter<CountingFile>, BandStats), StorageError> {
        self.filled = None;
        let thread = self.thread.take().expect("the band writer is joined once");
        thread
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }
}

impl Drop for BandWriter {
    fn drop(&mut self) {
        // A writer dropped unfinished (an error elsewhere) still stops its
        // thread before the file it writes is cleaned up.
        if self.thread.is_some() {
            let _ = self.join();
        }
    }
}

/// The band-writing thread: write each band received, in order, then send
/// it back to be filled again.
fn write_bands(
    mut writer: BufWriter<CountingFile>,
    bands: &Receiver<Vec<Vec<u8>>>,
    returns: &SyncSender<Vec<Vec<u8>>>,
) -> Result<(BufWriter<CountingFile>, BandStats), StorageError> {
    let mut stats = BandStats::default();
    for band in bands {
        let started = Instant::now();
        // A channel-tiled band's tiles are complete on-disk buckets: write
        // them straight to the file in order, past the buffered writer. A
        // band of one tile goes through it, which joins small buckets.
        let direct = band.len() > 1;
        if direct {
            writer.flush()?;
        }
        for tile in &band {
            if direct {
                writer.get_mut().write_all(tile)?;
            } else {
                writer.write_all(tile)?;
            }
            stats.logical_write_calls += 1;
            stats.max_logical_write_bytes = stats.max_logical_write_bytes.max(tile.len());
            stats.bytes_written += tile.len();
        }
        stats.write_seconds += started.elapsed().as_secs_f64();
        // The laying thread may have stopped (an error there); the band is
        // then not needed.
        let _ = returns.send(band);
    }
    Ok((writer, stats))
}
