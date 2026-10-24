// SPDX-License-Identifier: LGPL-3.0-or-later
//! Caller-owned reusable storage that a cursor fills and a batch borrows,
//! and the one checked path from filled arrays to a batch.

use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, EpochView, FrequencyView, MjdHighPrec, UvwView};
use ndarray::{ArrayView1, ArrayView2, ArrayView3};
use num_complex::Complex32;

use crate::batch::{
    DataGroupView, IrregularSamples, Occupancy, SampleTiming, SampleTopology, Shared,
    TimeBaselineSamples, VisibilityBatch, VisibilityValues, WeightView,
};
use crate::catalogue::{Correlation, OrderedBaseline, PartitionId};
use crate::grouping::GroupFragment;
use crate::request::Roles;
use crate::{ObservationError, VisibilityUnits};

/// The storage behind one [`VisibilityBatch`].
///
/// Owned by the caller (or a work unit), filled in place by a cursor and
/// lent to the batch. Its arrays are private: a cursor fills them through
/// [`Self::begin`], and [`BatchFill::finish`] checks them against the
/// batch's [`BatchHeader`] once, which the buffers then keep. The batch is
/// built from that header and nothing else, so no second description of
/// the same data can drift from the arrays.
///
/// Clearing keeps capacity, so after the first fill of the largest batch a
/// traversal allocates nothing. The buffers are `Send`: a pool of them can
/// be handed between work units whole, which is how a batch's lifetime is
/// separated from a traversal's.
#[derive(Debug, Default)]
pub struct BatchBuffers {
    arrays: BatchArrays,
    header: Option<BatchHeader>,
}

/// The arrays of one batch, as a cursor fills them.
#[derive(Clone, Debug, Default)]
pub struct BatchArrays {
    /// Visibilities, `[sample][channel][correlation]`.
    pub values: Vec<Complex32>,
    /// Flags, the same shape.
    pub flags: Vec<bool>,
    /// `FLAG_ROW` per sample, for stores that have one.
    pub row_flags: Vec<bool>,
    /// Weights, `[sample][correlation]` or `[sample][channel][correlation]`.
    pub weights: Vec<f32>,
    /// UVW, `[sample][3]`.
    pub uvw: Vec<f64>,
    /// Time offsets in seconds: per sample, or per time of a rectangle.
    pub time_offsets_s: Vec<f64>,
    /// Time-centroid offsets in seconds, per sample.
    pub centroid_offsets_s: Vec<f64>,
    /// Intervals in seconds, in the header's sharing.
    pub interval_s: Vec<f64>,
    /// Exposures in seconds, in the header's sharing.
    pub exposure_s: Vec<f64>,
    /// Scans, in the header's sharing.
    pub scan: Vec<u32>,
    /// First antenna per sample (irregular topology).
    pub antenna1: Vec<u32>,
    /// Second antenna per sample (irregular topology).
    pub antenna2: Vec<u32>,
    /// Field per sample, or per time of a rectangle.
    pub field: Vec<u32>,
    /// Baselines of a rectangle.
    pub baselines: Vec<OrderedBaseline>,
    /// Channel frequencies in hertz.
    pub frequency_hz: Vec<f64>,
    /// Correlation products.
    pub correlations: Vec<Correlation>,
    /// Scientific-group fragments.
    pub fragments: Vec<GroupFragment>,
    /// The data group's name.
    pub group_name: String,
    /// The data group's units.
    pub units: VisibilityUnits,
}

/// How a per-sample quantity is stored in a batch ([`Shared`]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Sharing<T> {
    /// One value per sample.
    PerSample,
    /// One value per time of a factored batch.
    PerTime,
    /// One value for every sample; the arrays hold none.
    Constant(T),
}

/// The sample layout of a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologyShape {
    /// `samples` listed samples.
    Irregular {
        /// Samples.
        samples: usize,
    },
    /// `times × baselines` samples; the baselines are the arrays'.
    TimeBaseline {
        /// Times.
        times: usize,
        /// Whether every slot is a measured acquisition.
        occupancy: Occupancy,
    },
}

/// Everything about a batch except its arrays' contents: what a cursor
/// decides before it fills, and what [`BatchFill::finish`] checks the
/// arrays against.
#[derive(Clone, Debug, PartialEq)]
pub struct BatchHeader {
    /// The partition.
    pub partition: PartitionId,
    /// Listed or factored samples.
    pub topology: TopologyShape,
    /// The partition's native channel of the first channel.
    pub first_channel: usize,
    /// The roles filled.
    pub roles: Roles,
    /// Whether weights are per channel.
    pub spectral_weights: bool,
    /// Whether `row_flags` is filled.
    pub row_flags: bool,
    /// Time scale of the times and centroids.
    pub time_scale: EpochRef,
    /// Origin of the time offsets.
    pub time_origin: MjdHighPrec,
    /// Spectral frame of the frequencies.
    pub frequency_frame: FrequencyRef,
    /// Effective frame of UVW.
    pub uvw_frame: DirectionRef,
    /// How intervals are stored, when the roles include them.
    pub interval: Sharing<f64>,
    /// How exposures are stored, when the roles include them.
    pub exposure: Sharing<f64>,
    /// How scans are stored, when the roles include them.
    pub scan: Sharing<u32>,
    /// One more than the largest valid antenna, field and scan number: the
    /// catalogue's counts.
    pub limits: IdLimits,
}

/// The catalogue's counts, against which a batch's identities are checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdLimits {
    /// Antennas.
    pub antennas: usize,
    /// Fields.
    pub fields: usize,
    /// Scans.
    pub scans: usize,
}

/// A batch being filled: the arrays, cleared, and the header they must
/// match. Dropping it without [`Self::finish`] leaves the buffers empty.
#[derive(Debug)]
pub struct BatchFill<'b> {
    buffers: &'b mut BatchBuffers,
    header: BatchHeader,
}

impl BatchBuffers {
    /// Empty every array, keeping its capacity, and forget the batch.
    pub fn clear(&mut self) {
        self.header = None;
        let arrays = &mut self.arrays;
        arrays.values.clear();
        arrays.flags.clear();
        arrays.row_flags.clear();
        arrays.weights.clear();
        arrays.uvw.clear();
        arrays.time_offsets_s.clear();
        arrays.centroid_offsets_s.clear();
        arrays.interval_s.clear();
        arrays.exposure_s.clear();
        arrays.scan.clear();
        arrays.antenna1.clear();
        arrays.antenna2.clear();
        arrays.field.clear();
        arrays.baselines.clear();
        arrays.frequency_hz.clear();
        arrays.correlations.clear();
        arrays.fragments.clear();
        arrays.group_name.clear();
    }

    /// Start filling a batch described by `header`: the arrays are cleared
    /// (keeping capacity) and lent through the returned fill.
    pub fn begin(&mut self, header: BatchHeader) -> BatchFill<'_> {
        self.clear();
        BatchFill {
            buffers: self,
            header,
        }
    }

    /// The arrays, read-only: what the batch's views borrow.
    #[must_use]
    pub fn arrays(&self) -> &BatchArrays {
        &self.arrays
    }

    /// The header of the batch the buffers hold, if they hold one.
    #[must_use]
    pub fn header(&self) -> Option<&BatchHeader> {
        self.header.as_ref()
    }

    /// Bytes the arrays hold now, from their capacities.
    #[must_use]
    pub fn capacity_bytes(&self) -> usize {
        let arrays = &self.arrays;
        arrays.values.capacity() * size_of::<Complex32>()
            + (arrays.flags.capacity() + arrays.row_flags.capacity()) * size_of::<bool>()
            + arrays.weights.capacity() * size_of::<f32>()
            + (arrays.uvw.capacity()
                + arrays.time_offsets_s.capacity()
                + arrays.centroid_offsets_s.capacity()
                + arrays.interval_s.capacity()
                + arrays.exposure_s.capacity()
                + arrays.frequency_hz.capacity())
                * size_of::<f64>()
            + (arrays.antenna1.capacity()
                + arrays.antenna2.capacity()
                + arrays.field.capacity()
                + arrays.scan.capacity())
                * size_of::<u32>()
            + arrays.baselines.capacity() * size_of::<OrderedBaseline>()
            + arrays.correlations.capacity() * size_of::<Correlation>()
            + arrays.fragments.capacity() * size_of::<GroupFragment>()
    }

    /// The batch the buffers hold, if a fill finished.
    #[must_use]
    pub fn batch(&self) -> Option<VisibilityBatch<'_>> {
        let header = self.header.as_ref()?;
        let arrays = &self.arrays;
        let channels = arrays.frequency_hz.len();
        let correlations = arrays.correlations.len();
        let time = EpochView::new(
            header.time_scale,
            header.time_origin,
            ArrayView1::from(&arrays.time_offsets_s[..]),
        );
        let (topology, samples) = match header.topology {
            TopologyShape::Irregular { samples } => (
                SampleTopology::Irregular(IrregularSamples {
                    time,
                    antenna1: ArrayView1::from(&arrays.antenna1[..]),
                    antenna2: ArrayView1::from(&arrays.antenna2[..]),
                    field: ArrayView1::from(&arrays.field[..]),
                }),
                samples,
            ),
            TopologyShape::TimeBaseline { times, occupancy } => (
                SampleTopology::TimeBaseline(TimeBaselineSamples {
                    time,
                    baselines: &arrays.baselines,
                    field: ArrayView1::from(&arrays.field[..]),
                    occupancy,
                }),
                times * arrays.baselines.len(),
            ),
        };
        let cube = [samples, channels, correlations];
        let roles = header.roles;
        Some(VisibilityBatch {
            partition: header.partition,
            topology,
            timing: SampleTiming {
                centroid: roles.time_centroid.then(|| {
                    EpochView::new(
                        header.time_scale,
                        header.time_origin,
                        ArrayView1::from(&arrays.centroid_offsets_s[..]),
                    )
                }),
                interval_s: roles
                    .interval
                    .then(|| shared(header.interval, &arrays.interval_s)),
                exposure_s: roles
                    .exposure
                    .then(|| shared(header.exposure, &arrays.exposure_s)),
            },
            scan: roles.scan.then(|| shared(header.scan, &arrays.scan)),
            first_channel: header.first_channel,
            frequency: FrequencyView::new(
                header.frequency_frame,
                ArrayView1::from(&arrays.frequency_hz[..]),
            ),
            correlations: &arrays.correlations,
            group: DataGroupView {
                name: &arrays.group_name,
                units: &arrays.units,
                values: roles
                    .values
                    .then(|| VisibilityValues::Complex32(cube_view(cube, &arrays.values))),
                flags: roles.flags.then(|| cube_view(cube, &arrays.flags)),
                row_flags: header
                    .row_flags
                    .then(|| ArrayView1::from(&arrays.row_flags[..])),
                weights: roles.weights.then(|| {
                    if header.spectral_weights {
                        WeightView::Spectral(cube_view(cube, &arrays.weights))
                    } else {
                        WeightView::PerCorrelation(
                            ArrayView2::from_shape([samples, correlations], &arrays.weights[..])
                                .expect("checked weights"),
                        )
                    }
                }),
                uvw: roles.uvw.then(|| {
                    UvwView::new(
                        header.uvw_frame,
                        ArrayView2::from_shape([samples, 3], &arrays.uvw[..]).expect("checked uvw"),
                    )
                }),
            },
            fragments: &arrays.fragments,
        })
    }
}

/// `values` in `sharing`.
fn shared<T: Copy>(sharing: Sharing<T>, values: &[T]) -> Shared<'_, T> {
    match sharing {
        Sharing::PerSample => Shared::PerSample(ArrayView1::from(values)),
        Sharing::PerTime => Shared::PerTime(ArrayView1::from(values)),
        Sharing::Constant(value) => Shared::Constant(value),
    }
}

/// A `[sample, channel, correlation]` view of `data`, whose length
/// [`BatchFill::finish`] checked.
fn cube_view<T>(cube: [usize; 3], data: &[T]) -> ArrayView3<'_, T> {
    ArrayView3::from_shape(cube, data).expect("length checked at finish")
}

impl BatchFill<'_> {
    /// The arrays to fill.
    pub fn arrays(&mut self) -> &mut BatchArrays {
        &mut self.buffers.arrays
    }

    /// The header the arrays must match.
    #[must_use]
    pub fn header(&self) -> &BatchHeader {
        &self.header
    }

    /// Check every array against the header, once, and keep the header:
    /// lengths (with overflow-checked sizes), antenna, field and scan
    /// identities against the catalogue's counts, and the fragments, which
    /// must tile the samples in order with factored batches split only at
    /// whole times.
    ///
    /// # Errors
    ///
    /// [`ObservationError::Shape`] or [`ObservationError::InvalidBatch`]: a
    /// cursor bug or a store the adapter should have refused, never a
    /// property a consumer must handle. The buffers then hold no batch.
    pub fn finish(self) -> Result<(), ObservationError> {
        let Self { buffers, header } = self;
        check(&buffers.arrays, &header)?;
        buffers.header = Some(header);
        Ok(())
    }
}

/// `a × b`, or a shape error naming `array`.
fn product(array: &'static str, a: usize, b: usize) -> Result<usize, ObservationError> {
    a.checked_mul(b).ok_or(ObservationError::InvalidBatch {
        reason: format!("{array} size overflows"),
    })
}

fn length(array: &'static str, actual: usize, expected: usize) -> Result<(), ObservationError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ObservationError::Shape {
            array,
            actual,
            expected,
        })
    }
}

fn identities(
    what: &'static str,
    ids: impl IntoIterator<Item = u32>,
    limit: usize,
) -> Result<(), ObservationError> {
    match ids.into_iter().find(|id| *id as usize >= limit) {
        Some(id) => Err(ObservationError::InvalidBatch {
            reason: format!("{what} {id} is not in the catalogue's {limit}"),
        }),
        None => Ok(()),
    }
}

fn check(arrays: &BatchArrays, header: &BatchHeader) -> Result<(), ObservationError> {
    let channels = arrays.frequency_hz.len();
    let correlations = arrays.correlations.len();
    if channels == 0 || correlations == 0 {
        return Err(ObservationError::InvalidBatch {
            reason: "a batch has at least one channel and one correlation".to_string(),
        });
    }
    let limits = header.limits;
    let (samples, times) = match header.topology {
        TopologyShape::Irregular { samples } => {
            length("antenna1", arrays.antenna1.len(), samples)?;
            length("antenna2", arrays.antenna2.len(), samples)?;
            identities("antenna", arrays.antenna1.iter().copied(), limits.antennas)?;
            identities("antenna", arrays.antenna2.iter().copied(), limits.antennas)?;
            (samples, samples)
        }
        TopologyShape::TimeBaseline { times, .. } => {
            identities(
                "antenna",
                arrays
                    .baselines
                    .iter()
                    .flat_map(|b| [b.antenna1.0, b.antenna2.0]),
                limits.antennas,
            )?;
            (product("samples", times, arrays.baselines.len())?, times)
        }
    };
    length("time", arrays.time_offsets_s.len(), times)?;
    length("field", arrays.field.len(), times)?;
    identities("field", arrays.field.iter().copied(), limits.fields)?;
    let cell = product("cube", samples, product("cube", channels, correlations)?)?;
    let roles = header.roles;
    let sized = |wanted: bool, name: &'static str, actual: usize, expected: usize| {
        length(name, actual, if wanted { expected } else { 0 })
    };
    sized(roles.values, "values", arrays.values.len(), cell)?;
    sized(roles.flags, "flags", arrays.flags.len(), cell)?;
    sized(
        header.row_flags,
        "row flags",
        arrays.row_flags.len(),
        samples,
    )?;
    let weights = if header.spectral_weights {
        cell
    } else {
        product("weights", samples, correlations)?
    };
    sized(roles.weights, "weights", arrays.weights.len(), weights)?;
    sized(
        roles.uvw,
        "uvw",
        arrays.uvw.len(),
        product("uvw", samples, 3)?,
    )?;
    sized(
        roles.time_centroid,
        "time centroid",
        arrays.centroid_offsets_s.len(),
        samples,
    )?;
    let shared = |sharing: Sharing<()>, name: &'static str| match sharing {
        Sharing::PerSample => Ok(samples),
        Sharing::PerTime if matches!(header.topology, TopologyShape::TimeBaseline { .. }) => {
            Ok(times)
        }
        Sharing::PerTime => Err(ObservationError::InvalidBatch {
            reason: format!("{name} per time in a batch of listed samples"),
        }),
        Sharing::Constant(()) => Ok(0),
    };
    let unit = |sharing: Sharing<f64>| match sharing {
        Sharing::PerSample => Sharing::PerSample,
        Sharing::PerTime => Sharing::PerTime,
        Sharing::Constant(_) => Sharing::Constant(()),
    };
    sized(
        roles.interval,
        "interval",
        arrays.interval_s.len(),
        shared(unit(header.interval), "interval")?,
    )?;
    sized(
        roles.exposure,
        "exposure",
        arrays.exposure_s.len(),
        shared(unit(header.exposure), "exposure")?,
    )?;
    let scan_sharing = match header.scan {
        Sharing::PerSample => Sharing::PerSample,
        Sharing::PerTime => Sharing::PerTime,
        Sharing::Constant(scan) => {
            if roles.scan {
                identities("scan", [scan], limits.scans)?;
            }
            Sharing::Constant(())
        }
    };
    sized(
        roles.scan,
        "scan",
        arrays.scan.len(),
        shared(scan_sharing, "scan")?,
    )?;
    identities("scan", arrays.scan.iter().copied(), limits.scans)?;
    fragments(arrays, header, samples)
}

/// The fragments tile `0..samples` in order, each non-empty, and split a
/// factored batch only between times.
fn fragments(
    arrays: &BatchArrays,
    header: &BatchHeader,
    samples: usize,
) -> Result<(), ObservationError> {
    if arrays.fragments.is_empty() {
        return Ok(());
    }
    let per_time = match header.topology {
        TopologyShape::Irregular { .. } => 1,
        TopologyShape::TimeBaseline { .. } => arrays.baselines.len().max(1),
    };
    let mut covered = 0;
    for fragment in &arrays.fragments {
        let range = &fragment.samples;
        if range.start != covered
            || range.end <= range.start
            || !range.end.is_multiple_of(per_time)
            || fragment.key.partition != header.partition
        {
            return Err(ObservationError::InvalidBatch {
                reason: format!("fragment {fragment:?} does not continue the batch at {covered}"),
            });
        }
        covered = range.end;
    }
    if covered == samples {
        Ok(())
    } else {
        Err(ObservationError::InvalidBatch {
            reason: format!("fragments cover {covered} of {samples} samples"),
        })
    }
}

impl BatchBuffers {
    /// Copy `batch` (typically a compute window of a larger batch) into
    /// these buffers, replacing what they held, through the same checked
    /// fill a cursor uses, and return the payload elements copied: the
    /// explicit, accounted packing that gives a window storage of its own.
    /// Strided views are gathered into standard order; a per-correlation
    /// weight stays per-correlation; shared quantities keep their sharing.
    ///
    /// # Errors
    ///
    /// When `batch` holds `Complex<f64>` visibilities, which these buffers
    /// do not store.
    pub fn pack(&mut self, batch: &VisibilityBatch<'_>) -> Result<usize, ObservationError> {
        let group = &batch.group;
        let time = batch.topology.time();
        let sharing = |shared: Option<Shared<'_, f64>>| match shared {
            Some(Shared::PerTime(_)) => Sharing::PerTime,
            Some(Shared::Constant(value)) => Sharing::Constant(value),
            Some(Shared::PerSample(_)) | None => Sharing::PerSample,
        };
        let header = BatchHeader {
            partition: batch.partition,
            topology: match batch.topology {
                SampleTopology::Irregular(samples) => TopologyShape::Irregular {
                    samples: samples.time.len(),
                },
                SampleTopology::TimeBaseline(samples) => TopologyShape::TimeBaseline {
                    times: samples.time.len(),
                    occupancy: samples.occupancy,
                },
            },
            first_channel: batch.first_channel,
            roles: Roles {
                values: group.values.is_some(),
                flags: group.flags.is_some(),
                weights: group.weights.is_some(),
                uvw: group.uvw.is_some(),
                time_centroid: batch.timing.centroid.is_some(),
                interval: batch.timing.interval_s.is_some(),
                exposure: batch.timing.exposure_s.is_some(),
                scan: batch.scan.is_some(),
            },
            spectral_weights: matches!(group.weights, Some(WeightView::Spectral(_))),
            row_flags: group.row_flags.is_some(),
            time_scale: time.reference(),
            time_origin: time.origin(),
            frequency_frame: batch.frequency.reference(),
            uvw_frame: group.uvw.map_or(DirectionRef::J2000, |uvw| uvw.reference()),
            interval: sharing(batch.timing.interval_s),
            exposure: sharing(batch.timing.exposure_s),
            scan: match batch.scan {
                Some(Shared::PerTime(_)) => Sharing::PerTime,
                Some(Shared::Constant(scan)) => Sharing::Constant(scan),
                Some(Shared::PerSample(_)) | None => Sharing::PerSample,
            },
            // The batch's identities were checked when it was filled.
            limits: IdLimits {
                antennas: usize::MAX,
                fields: usize::MAX,
                scans: usize::MAX,
            },
        };
        let mut fill = self.begin(header);
        let arrays = fill.arrays();
        arrays.group_name.push_str(group.name);
        arrays.units.clone_from(group.units);
        arrays.correlations.extend_from_slice(batch.correlations);
        arrays.frequency_hz.extend(batch.frequency.hz().iter());
        arrays.fragments.extend_from_slice(batch.fragments);
        arrays.time_offsets_s.extend(time.offsets_s().iter());
        match batch.topology {
            SampleTopology::Irregular(samples) => {
                arrays.antenna1.extend(samples.antenna1.iter());
                arrays.antenna2.extend(samples.antenna2.iter());
                arrays.field.extend(samples.field.iter());
            }
            SampleTopology::TimeBaseline(samples) => {
                arrays.baselines.extend_from_slice(samples.baselines);
                arrays.field.extend(samples.field.iter());
            }
        }
        if let Some(centroid) = batch.timing.centroid {
            arrays
                .centroid_offsets_s
                .extend(centroid.offsets_s().iter());
        }
        for (shared, target) in [
            (batch.timing.interval_s, &mut arrays.interval_s),
            (batch.timing.exposure_s, &mut arrays.exposure_s),
        ] {
            if let Some(Shared::PerSample(values) | Shared::PerTime(values)) = shared {
                target.extend(values.iter());
            }
        }
        if let Some(Shared::PerSample(scans) | Shared::PerTime(scans)) = batch.scan {
            arrays.scan.extend(scans.iter());
        }
        let mut copied = 0;
        if let Some(values) = group.values {
            let VisibilityValues::Complex32(values) = values else {
                return Err(ObservationError::Unsupported {
                    what: "packing Complex<f64> visibilities",
                });
            };
            arrays.values.extend(values.iter());
            copied += values.len();
        }
        if let Some(flags) = group.flags {
            arrays.flags.extend(flags.iter());
            copied += flags.len();
        }
        if let Some(row_flags) = group.row_flags {
            arrays.row_flags.extend(row_flags.iter());
            copied += row_flags.len();
        }
        match group.weights {
            Some(WeightView::Spectral(weights)) => {
                arrays.weights.extend(weights.iter());
                copied += weights.len();
            }
            Some(WeightView::PerCorrelation(weights)) => {
                arrays.weights.extend(weights.iter());
                copied += weights.len();
            }
            None => {}
        }
        if let Some(uvw) = group.uvw {
            arrays.uvw.extend(uvw.metres().iter());
            copied += uvw.metres().len();
        }
        fill.finish()?;
        Ok(copied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::AntennaId;
    use crate::grouping::{FragmentPosition, GroupKey};

    fn header(times: usize) -> BatchHeader {
        BatchHeader {
            partition: PartitionId(0),
            topology: TopologyShape::TimeBaseline {
                times,
                occupancy: Occupancy::Unknown,
            },
            first_channel: 0,
            roles: Roles {
                values: false,
                weights: false,
                uvw: false,
                ..Roles::ALL
            },
            spectral_weights: true,
            row_flags: false,
            time_scale: EpochRef::UTC,
            time_origin: MjdHighPrec::new(40_587.0, 0.0),
            frequency_frame: FrequencyRef::LSRK,
            uvw_frame: DirectionRef::J2000,
            interval: Sharing::Constant(10.0),
            exposure: Sharing::PerSample,
            scan: Sharing::PerTime,
            limits: IdLimits {
                antennas: 3,
                fields: 1,
                scans: 1,
            },
        }
    }

    fn key() -> GroupKey {
        GroupKey {
            partition: PartitionId(0),
            field: None,
            bin: 0,
        }
    }

    /// Two times of two baselines, two channels, one correlation, closing
    /// its group in one fragment.
    fn fill(
        buffers: &mut BatchBuffers,
        tweak: impl FnOnce(&mut BatchArrays),
    ) -> Result<(), ObservationError> {
        let mut fill = buffers.begin(header(2));
        let arrays = fill.arrays();
        let baseline = |a, b| OrderedBaseline {
            antenna1: AntennaId(a),
            antenna2: AntennaId(b),
        };
        arrays.baselines.extend([baseline(0, 1), baseline(1, 2)]);
        arrays.time_offsets_s.extend([0.0, 10.0]);
        arrays.field.extend([0, 0]);
        arrays.frequency_hz.extend([1.0e9, 1.1e9]);
        arrays.correlations.push(Correlation::XX);
        arrays.flags.extend([false; 8]);
        arrays.centroid_offsets_s.extend([0.0, 0.0, 10.0, 10.0]);
        arrays.exposure_s.extend([10.0; 4]);
        arrays.scan.extend([0, 0]);
        arrays.fragments.push(GroupFragment {
            key: key(),
            samples: 0..4,
            position: FragmentPosition::Whole,
        });
        tweak(arrays);
        fill.finish()
    }

    #[test]
    fn a_checked_fill_builds_the_batch_its_header_describes() {
        let mut buffers = BatchBuffers::default();
        fill(&mut buffers, |_| {}).expect("fill");
        let batch = buffers.batch().expect("batch");
        assert_eq!(batch.samples(), 4);
        assert_eq!(batch.interval_s(3), Some(10.0));
        assert_eq!(batch.scan(3), Some(crate::ScanId(0)));
        assert_eq!(batch.timing.centroid.expect("centroid").len(), 4);
        assert_eq!(batch.fragments.len(), 1);
        let window = batch.channel_window(1..2).expect("window");
        assert!(window.fragments.is_empty(), "a window never closes a group");
        assert_eq!(window.scan(3), Some(crate::ScanId(0)));
        let times = batch.sample_window(2..4).expect("one time");
        assert!(times.fragments.is_empty());
        assert_eq!(
            times.timing.centroid.expect("centroid").offsets_s()[0],
            10.0
        );
    }

    #[test]
    fn a_fill_that_disagrees_with_its_header_is_refused() {
        let mut buffers = BatchBuffers::default();
        type Tweak = fn(&mut BatchArrays);
        let cases: [(&str, Tweak); 6] = [
            ("short flags", |a| {
                a.flags.pop();
            }),
            ("antenna out of the catalogue", |a| {
                a.baselines[1].antenna2 = AntennaId(3)
            }),
            ("scan out of the catalogue", |a| a.scan[1] = 1),
            ("a gap before the fragment", |a| {
                a.fragments[0].samples = 2..4
            }),
            ("a fragment inside a time", |a| {
                a.fragments[0].samples = 0..3
            }),
            ("a short exposure", |a| {
                a.exposure_s.pop();
            }),
        ];
        for (label, tweak) in cases {
            assert!(fill(&mut buffers, tweak).is_err(), "{label}");
            assert!(
                buffers.batch().is_none(),
                "{label}: no batch after a refused fill"
            );
        }
    }

    #[test]
    fn packing_goes_through_the_same_checks_and_keeps_sharing() {
        let mut buffers = BatchBuffers::default();
        fill(&mut buffers, |_| {}).expect("fill");
        let batch = buffers.batch().expect("batch");
        let mut packed = BatchBuffers::default();
        let copied = packed
            .pack(&batch.channel_window(0..1).expect("window"))
            .expect("pack");
        assert_eq!(copied, 4, "four flags");
        let piece = packed.batch().expect("piece");
        assert_eq!(piece.channel_count(), 1);
        assert!(matches!(piece.scan, Some(Shared::PerTime(_))));
        assert!(matches!(
            piece.timing.interval_s,
            Some(Shared::Constant(10.0))
        ));
        assert!(piece.fragments.is_empty());
    }
}
