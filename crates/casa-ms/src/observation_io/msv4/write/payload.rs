// SPDX-License-Identifier: LGPL-3.0-or-later
//! The writer's payload: each batch checked against the declaration before
//! any byte is written, placed into the stored layout when it is not
//! already in it, and the dataset published from a staging directory of
//! its own.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_observation::{ObservationError, SampleTopology, Shared, VisibilityBatch, WeightView};
use casa_types::measures::MjdHighPrec;
use num_complex::Complex32;
use zarrs::array::ArraySubset;

use super::{Msv4Writer, request_error};
use crate::observation_io::msv4::metadata::{UNIX_EPOCH_MJD, invalid, storage};

/// Reusable working storage: each sample's slot in the dataset, and dense
/// staging for batches that need reordering.
#[derive(Default)]
pub(super) struct Scratch {
    /// `(time - first time) × baselines + baseline` of each sample.
    slots: Vec<usize>,
    filled: Vec<bool>,
    values: Vec<Complex32>,
    flags: Vec<bool>,
    weights: Vec<f32>,
    uvw: Vec<f64>,
    centroid: Vec<f64>,
    exposure: Vec<f64>,
}

/// Where a batch goes: its first time in the dataset and how many times it
/// holds.
struct Placement {
    first_time: usize,
    times: usize,
    /// Whether every sample is already at its slot.
    in_order: bool,
}

fn mismatch(what: &str) -> ObservationError {
    request_error(format!(
        "the batch's {what} is not the declared dataset's; the writer converts nothing"
    ))
}

impl Msv4Writer {
    /// Write `batch`: every sample of whole times, each time's baselines
    /// complete, every channel, every role.
    ///
    /// Before any byte is written the batch is checked against the
    /// declaration: its partition, channel frequencies and frame,
    /// correlation products, visibility units and UVW frame must be the
    /// declared ones (not merely the same counts); each sample's time,
    /// baseline, field, scan and interval must be the grid's; and no sample
    /// may carry a row flag, which MSv4 cannot represent. A batch already in
    /// the dataset's order is encoded straight from its views; any other is
    /// first placed into dense time × baseline order, a per-correlation
    /// weight written for each channel (both counted as packed).
    ///
    /// # Errors
    ///
    /// When a check fails, a time is written twice or incompletely, or the
    /// batch is a channel window. Nothing of the batch is then written.
    pub fn write_batch(&mut self, batch: &VisibilityBatch<'_>) -> Result<(), ObservationError> {
        self.check_context(batch)?;
        let placement = self.place(batch)?;
        let group = &batch.group;
        let (Some(values), Some(flags), Some(weights), Some(uvw)) =
            (group.values, group.flags, group.weights, group.uvw)
        else {
            return Err(request_error("a written batch carries every role"));
        };
        let values = values.as_complex32().ok_or(ObservationError::Unsupported {
            what: "writing complex128",
        })?;
        let direct = placement.in_order
            && matches!(batch.topology, SampleTopology::TimeBaseline(_))
            && matches!(weights, WeightView::Spectral(_))
            && values.is_standard_layout()
            && flags.is_standard_layout();
        if !direct {
            self.pack(batch)?;
        }
        self.gather_times(batch, direct);
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
        let scratch = &self.scratch;
        if direct {
            let WeightView::Spectral(weights) = weights else {
                unreachable!("checked spectral")
            };
            payload
                .values
                .store_array_subset_opt(&cube, values.as_slice().expect("standard layout"), options)
                .map_err(storage)?;
            payload
                .flags
                .store_array_subset_opt(&cube, flags.as_slice().expect("standard layout"), options)
                .map_err(storage)?;
            payload
                .weights
                .store_array_subset_opt(
                    &cube,
                    weights.as_standard_layout().as_slice().expect("standard"),
                    options,
                )
                .map_err(storage)?;
            payload
                .uvw
                .store_array_subset_opt(
                    &triple,
                    uvw.metres()
                        .as_standard_layout()
                        .as_slice()
                        .expect("standard"),
                    options,
                )
                .map_err(storage)?;
        } else {
            payload
                .values
                .store_array_subset_opt(&cube, scratch.values.as_slice(), options)
                .map_err(storage)?;
            payload
                .flags
                .store_array_subset_opt(&cube, scratch.flags.as_slice(), options)
                .map_err(storage)?;
            payload
                .weights
                .store_array_subset_opt(&cube, scratch.weights.as_slice(), options)
                .map_err(storage)?;
            payload
                .uvw
                .store_array_subset_opt(&triple, scratch.uvw.as_slice(), options)
                .map_err(storage)?;
        }
        payload
            .centroid
            .store_array_subset_opt(&plane, scratch.centroid.as_slice(), options)
            .map_err(storage)?;
        payload
            .exposure
            .store_array_subset_opt(&plane, scratch.exposure.as_slice(), options)
            .map_err(storage)?;
        let samples = batch.samples();
        self.counters.region_writes += 6;
        self.counters.elements += 3 * samples * channels * correlations + 5 * samples;
        let rows = placement.first_time..placement.first_time + placement.times;
        self.written[rows].iter_mut().for_each(|done| *done = true);
        Ok(())
    }

    /// The batch's context against the declaration's, before any sample.
    fn check_context(&self, batch: &VisibilityBatch<'_>) -> Result<(), ObservationError> {
        let target = &self.target;
        let entry = &target.entry;
        if batch.partition != target.partition {
            return Err(mismatch("partition"));
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
        if *group.units != target.group.units {
            return Err(mismatch("visibility units"));
        }
        if group
            .uvw
            .is_some_and(|uvw| uvw.reference() != target.group.uvw_frame.effective)
        {
            return Err(mismatch("UVW frame"));
        }
        if batch.topology.time().reference() != target.grid.time_scale {
            return Err(mismatch("time scale"));
        }
        if group
            .row_flags
            .is_some_and(|flags| flags.iter().any(|flagged| *flagged))
        {
            return Err(ObservationError::Unsupported {
                what: "writing a row flag, which MSv4 cannot represent: fold it into the \
                       element flags first, deliberately",
            });
        }
        let timing = &batch.timing;
        if timing.centroid.is_none()
            || timing.exposure_s.is_none()
            || timing.interval_s.is_none()
            || batch.scan.is_none()
        {
            return Err(request_error(
                "a written batch carries its time centroid, interval, exposure and scan",
            ));
        }
        Ok(())
    }

    /// Each sample's slot, with its time, baseline, field, scan and
    /// interval checked against the grid, and the times it covers: whole,
    /// contiguous, complete and not yet written.
    fn place(&mut self, batch: &VisibilityBatch<'_>) -> Result<Placement, ObservationError> {
        let target = &self.target;
        let grid = &target.grid;
        let baselines = self.shape[1];
        let time = batch.topology.time();
        let per_time = batch.topology.samples_per_time();
        let unix = MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
        let samples = batch.samples();
        if samples == 0 || !samples.is_multiple_of(baselines) {
            return Err(ObservationError::Unsupported {
                what: "writing an incomplete time: every baseline of a time is required",
            });
        }
        let times = samples / baselines;
        let scratch = &mut self.scratch;
        scratch.slots.clear();
        scratch.filled.clear();
        scratch.filled.resize(samples, false);
        let mut first_time = None;
        for sample in 0..samples {
            let coordinate = batch.topology.sample(sample);
            let seconds = time.seconds_since(sample / per_time, unix);
            let t = *target
                .times
                .get(&seconds.to_bits())
                .ok_or_else(|| invalid(format!("time {seconds} is not in the dataset")))?;
            let b = *target
                .baselines
                .get(&coordinate.baseline)
                .ok_or_else(|| mismatch("baseline"))?;
            if coordinate.field != grid.field_per_time[t] {
                return Err(mismatch("field at a time"));
            }
            if batch.scan(sample) != Some(grid.scan_per_time[t]) {
                return Err(mismatch("scan at a time"));
            }
            if batch.interval_s(sample) != Some(grid.interval_s) {
                return Err(mismatch("integration interval"));
            }
            let first = *first_time.get_or_insert(t);
            if t < first || t >= first + times {
                return Err(invalid("a batch's times are not contiguous"));
            }
            let slot = (t - first) * baselines + b;
            if std::mem::replace(&mut scratch.filled[slot], true) {
                return Err(ObservationError::Unsupported {
                    what: "two acquisitions in one time/baseline slot",
                });
            }
            scratch.slots.push(slot);
        }
        let first_time = first_time.expect("samples");
        if scratch.filled.iter().any(|filled| !filled) {
            return Err(ObservationError::Unsupported {
                what: "writing an incomplete time: every baseline of a time is required",
            });
        }
        if first_time + times > self.written.len()
            || self.written[first_time..first_time + times]
                .iter()
                .any(|done| *done)
        {
            return Err(invalid("a time is written twice or beyond the dataset"));
        }
        let in_order = scratch
            .slots
            .iter()
            .enumerate()
            .all(|(sample, slot)| sample == *slot);
        Ok(Placement {
            first_time,
            times,
            in_order,
        })
    }

    /// Place `batch`'s payload at its slots ([`Self::place`]) in dense
    /// `[time, baseline]` order.
    fn pack(&mut self, batch: &VisibilityBatch<'_>) -> Result<(), ObservationError> {
        let [_, _, channels, correlations] = self.shape;
        let cell = channels * correlations;
        let slots = batch.samples();
        let scratch = &mut self.scratch;
        scratch.values.clear();
        scratch
            .values
            .resize(slots * cell, Complex32::new(f32::NAN, f32::NAN));
        scratch.flags.clear();
        scratch.flags.resize(slots * cell, false);
        scratch.weights.clear();
        scratch.weights.resize(slots * cell, f32::NAN);
        scratch.uvw.clear();
        scratch.uvw.resize(slots * 3, f64::NAN);
        let group = &batch.group;
        let values = group
            .values
            .and_then(|v| v.as_complex32())
            .expect("checked");
        let flags = group.flags.expect("checked");
        let weights = group.weights.expect("checked");
        let uvw = group.uvw.expect("checked");
        for (sample, &slot) in scratch.slots.iter().enumerate() {
            for channel in 0..channels {
                for correlation in 0..correlations {
                    let index = slot * cell + channel * correlations + correlation;
                    scratch.values[index] = values[[sample, channel, correlation]];
                    scratch.flags[index] = flags[[sample, channel, correlation]];
                    scratch.weights[index] = weights.get(sample, channel, correlation);
                }
            }
            scratch.uvw[slot * 3..slot * 3 + 3].copy_from_slice(&uvw.get(sample));
        }
        self.counters.packed_elements += 3 * slots * cell + 3 * slots;
        Ok(())
    }

    /// The time centroids (in Unix seconds) and exposures at their slots.
    /// They are copied unless the batch is in order, its centroids already
    /// count from the Unix epoch and its exposures are per sample: then the
    /// copies are the views' own values and nothing is counted as packed.
    fn gather_times(&mut self, batch: &VisibilityBatch<'_>, direct: bool) {
        let unix = MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
        let centroid = batch.timing.centroid.expect("checked");
        let in_place = direct
            && centroid.origin() == unix
            && matches!(batch.timing.exposure_s, Some(Shared::PerSample(_)));
        let scratch = &mut self.scratch;
        let slots = scratch.slots.len();
        scratch.centroid.clear();
        scratch.centroid.resize(slots, f64::NAN);
        scratch.exposure.clear();
        scratch.exposure.resize(slots, f64::NAN);
        for (sample, &slot) in scratch.slots.iter().enumerate() {
            scratch.centroid[slot] = centroid.seconds_since(sample, unix);
            scratch.exposure[slot] = batch.exposure_s(sample).expect("checked");
        }
        if !in_place {
            self.counters.packed_elements += 2 * slots;
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

/// Rename `from` to `to`, failing with `AlreadyExists` rather than replace
/// anything at `to`.
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        use std::os::unix::ffi::OsStrExt;
        let from = std::ffi::CString::new(from.as_os_str().as_bytes())?;
        let target = std::ffi::CString::new(to.as_os_str().as_bytes())?;
        // SAFETY: both paths are NUL-terminated C strings that outlive the
        // call; renameat2 reads them only.
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                target.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        // A filesystem without RENAME_NOREPLACE falls through to the
        // checked rename below.
        if !matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) {
            return Err(error);
        }
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::ffi::OsStrExt;
        let from = std::ffi::CString::new(from.as_os_str().as_bytes())?;
        let target = std::ffi::CString::new(to.as_os_str().as_bytes())?;
        // SAFETY: as above, for renamex_np.
        let result = unsafe { libc::renamex_np(from.as_ptr(), target.as_ptr(), libc::RENAME_EXCL) };
        return if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        };
    }
    // Elsewhere, a check and a rename: a narrow race this trial accepts.
    #[cfg(not(target_os = "macos"))]
    {
        if to.exists() {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }
        std::fs::rename(from, to)
    }
}
