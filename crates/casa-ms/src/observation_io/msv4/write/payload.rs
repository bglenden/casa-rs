// SPDX-License-Identifier: LGPL-3.0-or-later
//! The writer's payload: each batch checked against the declaration before
//! any byte is written, encoded from its views where they are already in
//! the stored layout, and the dataset published from a staging directory
//! of its own.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_observation::{ObservationError, ScanId, Shared, VisibilityBatch};
use casa_types::measures::{EpochView, MjdHighPrec, UvwView};
use ndarray::{ArrayView1, ArrayView3};
use num_complex::Complex32;
use zarrs::array::ArraySubset;

use super::{Msv4Writer, request_error};
use crate::observation_io::msv4::metadata::{UNIX_EPOCH_MJD, invalid, storage};

/// Reusable working storage for what is not encoded straight from a view:
/// the time centroids re-expressed from the Unix epoch, the exposures per
/// sample, and a weight shared by every channel written for each channel.
#[derive(Default)]
pub(super) struct Scratch {
    weights: Vec<f32>,
    centroid: Vec<f64>,
    exposure: Vec<f64>,
}

/// Where a batch goes: its first time in the dataset and how many times it
/// holds.
struct Placement {
    first_time: usize,
    times: usize,
}

/// A batch the declaration admits: the views a write encodes, each proven
/// present, and where its times go.
struct Checked<'b> {
    values: ArrayView3<'b, Complex32>,
    flags: ArrayView3<'b, bool>,
    weights: ArrayView3<'b, f32>,
    uvw: UvwView<'b>,
    centroid: EpochView<'b>,
    exposure: Shared<'b, f64>,
    samples_per_time: usize,
    placement: Placement,
}

/// `view`'s elements as the one slice they occupy in standard layout.
fn contiguous<'a, T>(
    view: ndarray::ArrayView3<'a, T>,
    name: &str,
) -> Result<&'a [T], ObservationError> {
    view.to_slice()
        .ok_or_else(|| request_error(format!("the batch's {name} are not contiguous")))
}

fn mismatch(what: &str) -> ObservationError {
    request_error(format!(
        "the batch's {what} is not the declared dataset's; the writer converts nothing"
    ))
}

impl Msv4Writer {
    /// Write `batch`: whole times of the declared grid, every channel,
    /// every product, every role.
    ///
    /// Before any byte is written the batch is checked against the
    /// declaration: its partition, baseline table, channel frequencies and
    /// frame, correlation products, visibility units and UVW frame must be
    /// the declared ones (not merely the same counts); each time's instant,
    /// field, scan and interval must be the grid's. The batch's grid is the
    /// dataset's, so its visibilities and flags are encoded straight from
    /// their views; a weight shared by every channel is written for each
    /// channel (counted as packed).
    ///
    /// # Errors
    ///
    /// When a check fails, a time is written twice, or the batch is a
    /// channel window. Nothing of the batch is then written.
    pub fn write_batch(&mut self, batch: &VisibilityBatch<'_>) -> Result<(), ObservationError> {
        let Checked {
            values,
            flags,
            weights,
            uvw,
            centroid,
            exposure,
            samples_per_time,
            placement,
        } = self.check(batch)?;
        self.gather_times(centroid, exposure, samples_per_time);
        let [_, baselines, channels, correlations] = self.shape;
        let rows = placement.first_time as u64..(placement.first_time + placement.times) as u64;
        let cube = ArraySubset::new_with_ranges(&[
            rows.clone(),
            0..baselines as u64,
            0..channels as u64,
            0..correlations as u64,
        ]);
        let triple = ArraySubset::new_with_ranges(&[rows.clone(), 0..baselines as u64, 0..3]);
        let plane = ArraySubset::new_with_ranges(&[rows, 0..baselines as u64]);
        let options = &self.options;
        let payload = self.payload.as_ref().expect("created");
        let scratch = &mut self.scratch;
        payload
            .values
            .store_array_subset_opt(&cube, contiguous(values, "visibilities")?, options)
            .map_err(storage)?;
        payload
            .flags
            .store_array_subset_opt(&cube, contiguous(flags, "flags")?, options)
            .map_err(storage)?;
        if weights.is_standard_layout() {
            payload
                .weights
                .store_array_subset_opt(&cube, weights.to_slice().expect("standard"), options)
                .map_err(storage)?;
        } else {
            scratch.weights.clear();
            scratch.weights.extend(weights.iter());
            self.counters.packed_elements += scratch.weights.len();
            payload
                .weights
                .store_array_subset_opt(&cube, scratch.weights.as_slice(), options)
                .map_err(storage)?;
        }
        let metres = uvw.metres();
        payload
            .uvw
            .store_array_subset_opt(
                &triple,
                metres.as_standard_layout().as_slice().expect("standard"),
                options,
            )
            .map_err(storage)?;
        payload
            .centroid
            .store_array_subset_opt(&plane, scratch.centroid.as_slice(), options)
            .map_err(storage)?;
        payload
            .exposure
            .store_array_subset_opt(&plane, scratch.exposure.as_slice(), options)
            .map_err(storage)?;
        let samples = batch.samples();
        self.counters.partial_chunks += 6 * partial_chunks(
            placement.first_time..placement.first_time + placement.times,
            self.chunk_times,
            self.written.len(),
        );
        self.counters.region_writes += 6;
        self.counters.elements += 3 * samples * channels * correlations + 5 * samples;
        let rows = placement.first_time..placement.first_time + placement.times;
        self.written[rows].iter_mut().for_each(|done| *done = true);
        Ok(())
    }

    /// Everything the write needs of `batch`, checked against the
    /// declaration before any byte is written.
    fn check<'b>(&self, batch: &VisibilityBatch<'b>) -> Result<Checked<'b>, ObservationError> {
        self.check_context(batch)?;
        let timing = &batch.timing;
        let (Some(centroid), Some(exposure), Some(_), Some(scan)) = (
            timing.centroid,
            timing.exposure_s,
            timing.interval_s,
            batch.scan,
        ) else {
            return Err(request_error(
                "a written batch carries its time centroid, interval, exposure and scan",
            ));
        };
        let group = &batch.group;
        let (Some(values), Some(flags), Some(weights), Some(uvw)) =
            (group.values, group.flags, group.weights, group.uvw)
        else {
            return Err(request_error("a written batch carries every role"));
        };
        Ok(Checked {
            values,
            flags,
            weights,
            uvw,
            centroid,
            exposure,
            samples_per_time: batch.samples_per_time(),
            placement: self.place(batch, scan)?,
        })
    }

    /// The batch's context against the declaration's, before any sample.
    fn check_context(&self, batch: &VisibilityBatch<'_>) -> Result<(), ObservationError> {
        let target = &self.target;
        let entry = &target.entry;
        if batch.partition != target.partition {
            return Err(mismatch("partition"));
        }
        if batch.baselines != target.grid.baselines.as_slice() {
            return Err(mismatch("baseline table"));
        }
        if batch.first_channel != 0 || batch.channel_count() != entry.frequencies_hz.len() {
            return Err(ObservationError::Unsupported {
                what: "writing a channel window",
            });
        }
        if batch.frequency.reference() != entry.frequency_frame
            || batch
                .frequency
                .hz()
                .iter()
                .zip(&entry.frequencies_hz)
                .any(|(a, b)| a.to_bits() != b.to_bits())
        {
            return Err(mismatch("channel frequencies or spectral frame"));
        }
        if batch.correlations != entry.correlations.as_slice() {
            return Err(mismatch("correlation products"));
        }
        let group = &batch.group;
        if group.entry.units != target.group.units {
            return Err(mismatch("visibility units"));
        }
        if group
            .uvw
            .is_some_and(|uvw| uvw.reference() != target.group.uvw_frame.effective)
        {
            return Err(mismatch("UVW frame"));
        }
        if batch.time.reference() != target.grid.time_scale {
            return Err(mismatch("time scale"));
        }
        Ok(())
    }

    /// The batch's times in the dataset, with each time's field, scan and
    /// interval checked against the grid: contiguous and not yet written.
    fn place(
        &self,
        batch: &VisibilityBatch<'_>,
        scan: ArrayView1<'_, ScanId>,
    ) -> Result<Placement, ObservationError> {
        let target = &self.target;
        let grid = &target.grid;
        let unix = MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
        let times = batch.times();
        if times == 0 {
            return Err(request_error("an empty batch"));
        }
        let per_time = batch.samples_per_time();
        let index_of = |t: usize| {
            let seconds = batch.time.seconds_since(t, unix);
            target
                .times
                .get(&seconds.to_bits())
                .copied()
                .ok_or_else(|| invalid(format!("time {seconds} is not in the dataset")))
        };
        let first_time = index_of(0)?;
        for t in 0..times {
            let index = index_of(t)?;
            if batch.field[t] != grid.field_per_time[index] {
                return Err(mismatch("field at a time"));
            }
            if scan[t] != grid.scan_per_time[index] {
                return Err(mismatch("scan at a time"));
            }
            // An absent acquisition (padding) has no interval: NaN.
            if (t * per_time..(t + 1) * per_time).any(|sample| {
                batch
                    .interval_s(sample)
                    .is_none_or(|interval| !interval.is_nan() && interval != grid.interval_s)
            }) {
                return Err(mismatch("integration interval"));
            }
            if index != first_time + t {
                return Err(invalid("a batch's times are not contiguous in the dataset"));
            }
        }
        if first_time + times > self.written.len()
            || self.written[first_time..first_time + times]
                .iter()
                .any(|done| *done)
        {
            return Err(invalid("a time is written twice or beyond the dataset"));
        }
        Ok(Placement { first_time, times })
    }

    /// The time centroids (in Unix seconds) and exposures per sample,
    /// counted as packed unless the batch already holds them that way.
    fn gather_times(
        &mut self,
        centroid: EpochView<'_>,
        exposure: Shared<'_, f64>,
        samples_per_time: usize,
    ) {
        let unix = MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
        let in_place = centroid.origin() == unix && matches!(exposure, Shared::PerSample(_));
        let scratch = &mut self.scratch;
        let samples = centroid.len();
        scratch.centroid.clear();
        scratch.exposure.clear();
        for sample in 0..samples {
            scratch.centroid.push(centroid.seconds_since(sample, unix));
            scratch
                .exposure
                .push(exposure.get(sample, samples_per_time));
        }
        if !in_place {
            self.counters.packed_elements += 2 * samples;
        }
    }

    /// Check every time was written and publish the processing set by
    /// renaming this writer's staging directory to the destination, which
    /// must still not exist: the rename refuses to replace it (Linux
    /// `renameat2(RENAME_NOREPLACE)`, macOS `renamex_np(RENAME_EXCL)`). A
    /// writer dropped or failing removes its staging directory; a crash
    /// leaves it under a name no later writer reuses.
    ///
    /// # Errors
    ///
    /// When a time was not written, or the destination appeared meanwhile
    /// (another writer published first): the staging directory is then
    /// removed and nothing is replaced.
    pub fn finish(mut self) -> Result<PathBuf, ObservationError> {
        if let Some(time) = self.written.iter().position(|done| !done) {
            return Err(invalid(format!("time {time} was never written")));
        }
        drop(self.payload.take());
        let staging = self.staging.take().expect("staging until finish");
        match rename_noreplace(&staging, &self.destination) {
            Ok(()) => Ok(std::mem::take(&mut self.destination)),
            Err(error) => {
                let _ = std::fs::remove_dir_all(&staging);
                if error.kind() == io::ErrorKind::AlreadyExists {
                    Err(request_error(format!(
                        "{} appeared while the dataset was written; not replaced",
                        self.destination.display()
                    )))
                } else {
                    Err(storage(error))
                }
            }
        }
    }
}

impl Drop for Msv4Writer {
    fn drop(&mut self) {
        if let Some(staging) = self.staging.take() {
            drop(self.payload.take());
            let _ = std::fs::remove_dir_all(staging);
        }
    }
}

/// A new staging directory beside `destination`, created exclusively under
/// a name unique to this process and call: two live writers, or a retry
/// after a crash, never share one, and an existing directory is never
/// adopted.
pub(super) fn create_staging(destination: &Path) -> Result<PathBuf, ObservationError> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let name = destination
        .file_name()
        .ok_or_else(|| request_error(format!("{} has no file name", destination.display())))?
        .to_string_lossy()
        .into_owned();
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    loop {
        let candidate = parent.join(format!(
            ".{name}.staging-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(storage(error)),
        }
    }
}

/// Chunks of `chunk` times that `times` cover only partly, in a dataset of
/// `total` times (the last chunk ends at `total`).
fn partial_chunks(times: std::ops::Range<usize>, chunk: usize, total: usize) -> usize {
    if times.is_empty() {
        return 0;
    }
    let (first, last) = (times.start / chunk, (times.end - 1) / chunk);
    let partial = |index: usize| {
        let whole = index * chunk..((index + 1) * chunk).min(total);
        usize::from(times.start > whole.start || times.end < whole.end)
    };
    if first == last {
        partial(first)
    } else {
        partial(first) + partial(last)
    }
}

/// Rename `from` to `to`, failing with `AlreadyExists` rather than replace
/// anything at `to`: `renameat2(RENAME_NOREPLACE)`, or the checked rename
/// where the filesystem does not offer it.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    let (c_from, c_to) = (c_path(from)?, c_path(to)?);
    // SAFETY: both paths are NUL-terminated C strings that outlive the
    // call; renameat2 reads them only.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            c_from.as_ptr(),
            libc::AT_FDCWD,
            c_to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) {
        checked_rename(from, to)
    } else {
        Err(error)
    }
}

/// [`rename_noreplace`] through `renamex_np(RENAME_EXCL)`.
#[cfg(target_os = "macos")]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    let (c_from, c_to) = (c_path(from)?, c_path(to)?);
    // SAFETY: as for renameat2: NUL-terminated paths, read only.
    let result = unsafe { libc::renamex_np(c_from.as_ptr(), c_to.as_ptr(), libc::RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// [`rename_noreplace`] where no exclusive rename is available.
#[cfg(not(any(target_os = "macos", all(target_os = "linux", target_env = "gnu"))))]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    checked_rename(from, to)
}

/// A check and a rename: a narrow race this trial accepts where the
/// platform or filesystem offers no exclusive rename.
#[cfg(not(target_os = "macos"))]
fn checked_rename(from: &Path, to: &Path) -> io::Result<()> {
    if to.exists() {
        return Err(io::Error::from(io::ErrorKind::AlreadyExists));
    }
    std::fs::rename(from, to)
}

/// `path` as a C string.
#[cfg(any(target_os = "macos", all(target_os = "linux", target_env = "gnu")))]
fn c_path(path: &Path) -> io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    Ok(std::ffi::CString::new(path.as_os_str().as_bytes())?)
}

#[cfg(test)]
mod tests {
    use super::partial_chunks;

    #[test]
    fn a_batch_covers_whole_chunks_only_when_aligned() {
        assert_eq!(partial_chunks(0..5, 5, 12), 0);
        assert_eq!(partial_chunks(0..10, 5, 12), 0);
        assert_eq!(partial_chunks(10..12, 5, 12), 0, "the short last chunk");
        assert_eq!(partial_chunks(0..4, 5, 12), 1);
        assert_eq!(partial_chunks(4..8, 5, 12), 2);
        assert_eq!(partial_chunks(1..3, 5, 12), 1);
        assert_eq!(partial_chunks(8..12, 5, 12), 1);
    }
}
