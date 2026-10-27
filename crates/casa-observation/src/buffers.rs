// SPDX-License-Identifier: LGPL-3.0-or-later
//! Caller-owned reusable storage that a cursor fills and a batch borrows,
//! and the one checked path from filled arrays to a batch.

use std::sync::Arc;

use casa_types::measures::{EpochView, FrequencyView, UvwView};
use ndarray::{ArrayView1, ArrayView2, ArrayView3, Axis, ShapeBuilder};
use num_complex::Complex32;

use crate::ObservationError;
use crate::batch::{DataGroupView, SampleTiming, Shared, VisibilityBatch};
use crate::catalogue::{Catalogue, FieldId, PartitionEntry, PartitionId, ScanId};
use crate::grouping::GroupFragment;
use crate::request::{CorrelationAxis, Roles, WeightRule};

/// The storage behind one [`VisibilityBatch`].
///
/// Owned by the caller (or a [`BufferPool`](crate::BufferPool)), filled in
/// place by a cursor and lent to the batch. Its arrays are private: a
/// cursor fills them through [`Self::begin`], which checks the batch's
/// [`BatchHeader`] against the observation's [`Catalogue`] and reserves
/// exactly the arrays it describes, and [`BatchFill::finish`] checks the
/// filled arrays once; the buffers then keep the header and a shared
/// reference to that catalogue. The batch is built from the header, the
/// arrays and the catalogue's partition entry and nothing else, so no
/// second description of the same data can drift from the arrays: the
/// per-partition constants (frequencies, correlations, baselines, time
/// system, data group) are never copied into a batch, and no other
/// catalogue can be paired with the arrays.
///
/// Clearing keeps capacity, so after the first fill of the largest batch a
/// traversal allocates nothing. Buffers made [`Self::with_envelope`] never
/// hold more than their envelope, whatever sequence of batch shapes and
/// roles they are filled with. The buffers are `Send`: they can be handed
/// between work units whole, which is how a batch's lifetime is separated
/// from a traversal's.
#[derive(Debug, Default)]
pub struct BatchBuffers {
    arrays: BatchArrays,
    header: Option<BatchHeader>,
    catalogue: Option<Arc<Catalogue>>,
    envelope: Option<usize>,
}

/// The arrays of one batch, as a cursor fills them.
#[derive(Clone, Debug, Default)]
pub struct BatchArrays {
    /// Visibilities, `[sample][channel][correlation]`.
    pub values: Vec<Complex32>,
    /// Flags, the same shape.
    pub flags: Vec<bool>,
    /// Weights, `[sample][channel][correlation]`, or `[sample][correlation]`
    /// when the header says they are not per channel.
    pub weights: Vec<f32>,
    /// UVW, `[sample][3]`.
    pub uvw: Vec<f64>,
    /// Time offsets in seconds from the partition's origin, per time.
    pub time_offsets_s: Vec<f64>,
    /// Time-centroid offsets in seconds, per sample.
    pub centroid_offsets_s: Vec<f64>,
    /// Intervals in seconds, in the header's sharing.
    pub interval_s: Vec<f64>,
    /// Exposures in seconds, in the header's sharing.
    pub exposure_s: Vec<f64>,
    /// Field per time.
    pub field: Vec<FieldId>,
    /// Scan per time, when the roles include it.
    pub scan: Vec<ScanId>,
    /// Scientific-group fragments.
    pub fragments: Vec<GroupFragment>,
}

/// How a per-sample quantity is stored in a batch ([`Shared`]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Sharing<T> {
    /// One value per sample.
    PerSample,
    /// One value per time.
    PerTime,
    /// One value for every sample; the arrays hold none.
    Constant(T),
}

/// Everything about a batch except its arrays' contents and what the
/// catalogue already states: what a cursor decides before it fills, and
/// what [`BatchFill::finish`] checks the arrays against.
#[derive(Clone, Debug, PartialEq)]
pub struct BatchHeader {
    /// The partition.
    pub partition: PartitionId,
    /// The data group: an index into the partition's
    /// [`data_groups`](PartitionEntry::data_groups).
    pub data_group: usize,
    /// Whole times in the batch; the samples are these times by the
    /// partition's baselines.
    pub times: usize,
    /// The partition's native channel of the first channel.
    pub first_channel: usize,
    /// Channels.
    pub channels: usize,
    /// The delivered correlation products: the traversal's resolved axis,
    /// shared, never a copy of the list.
    pub correlations: CorrelationAxis,
    /// The roles filled.
    pub roles: Roles,
    /// Whether the weights are per channel (`[sample][channel][correlation]`)
    /// or one per correlation shared by every channel.
    pub weights_per_channel: bool,
    /// What the weights are: stored, or made from the sigmas.
    pub weight_rule: WeightRule,
    /// How intervals are stored, when the roles include them.
    pub interval: Sharing<f64>,
    /// How exposures are stored, when the roles include them.
    pub exposure: Sharing<f64>,
}

/// A batch being filled: the arrays, cleared and reserved, and the header
/// they must match. Dropping it without [`Self::finish`] leaves the buffers
/// empty.
#[derive(Debug)]
pub struct BatchFill<'b> {
    buffers: &'b mut BatchBuffers,
    header: BatchHeader,
    catalogue: Arc<Catalogue>,
    lengths: Lengths,
}

/// The length of every array of the batch a header describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Lengths {
    values: usize,
    flags: usize,
    weights: usize,
    uvw: usize,
    times: usize,
    centroid: usize,
    interval: usize,
    exposure: usize,
    scan: usize,
}

impl Lengths {
    /// The lengths for `header` over `baselines`, with overflow-checked
    /// sizes; the fragments have room for one per time.
    fn of(
        header: &BatchHeader,
        baselines: usize,
        correlations: usize,
    ) -> Result<Self, ObservationError> {
        let times = header.times;
        let samples = product("samples", times, baselines)?;
        let cell = product(
            "cube",
            samples,
            product("cube", header.channels, correlations)?,
        )?;
        let roles = header.roles;
        let wanted = |role: bool, length: usize| if role { length } else { 0 };
        let shared = |sharing: Sharing<f64>| match sharing {
            Sharing::PerSample => samples,
            Sharing::PerTime => times,
            Sharing::Constant(_) => 0,
        };
        let weights = if header.weights_per_channel {
            cell
        } else {
            product("weights", samples, correlations)?
        };
        Ok(Self {
            values: wanted(roles.values, cell),
            flags: wanted(roles.flags, cell),
            weights: wanted(roles.weights, weights),
            uvw: wanted(roles.uvw, product("uvw", samples, 3)?),
            times,
            centroid: wanted(roles.time_centroid, samples),
            interval: wanted(roles.interval, shared(header.interval)),
            exposure: wanted(roles.exposure, shared(header.exposure)),
            scan: wanted(roles.scan, times),
        })
    }

    /// Bytes the arrays hold at these lengths
    /// ([`PartitionEstimate::buffer_bytes`](crate::PartitionEstimate::buffer_bytes)
    /// for a full batch).
    fn bytes(&self) -> usize {
        // Saturating: a header too large to allocate exceeds any envelope.
        [
            (self.values, size_of::<Complex32>()),
            (self.flags, size_of::<bool>()),
            (self.weights, size_of::<f32>()),
            (self.uvw, size_of::<f64>()),
            (self.times, size_of::<f64>()),
            (self.centroid, size_of::<f64>()),
            (self.interval, size_of::<f64>()),
            (self.exposure, size_of::<f64>()),
            (
                self.times,
                size_of::<FieldId>() + size_of::<GroupFragment>(),
            ),
            (self.scan, size_of::<ScanId>()),
        ]
        .into_iter()
        .fold(0, |total: usize, (length, size)| {
            total.saturating_add(length.saturating_mul(size))
        })
    }

    /// Bytes `arrays` would hold once reserved to these lengths, keeping
    /// any larger capacity they have.
    fn retained(&self, arrays: &BatchArrays) -> usize {
        fn held<T>(array: &Vec<T>, length: usize) -> usize {
            array.capacity().max(length).saturating_mul(size_of::<T>())
        }
        [
            held(&arrays.values, self.values),
            held(&arrays.flags, self.flags),
            held(&arrays.weights, self.weights),
            held(&arrays.uvw, self.uvw),
            held(&arrays.time_offsets_s, self.times),
            held(&arrays.centroid_offsets_s, self.centroid),
            held(&arrays.interval_s, self.interval),
            held(&arrays.exposure_s, self.exposure),
            held(&arrays.field, self.times),
            held(&arrays.scan, self.scan),
            held(&arrays.fragments, self.times),
        ]
        .into_iter()
        .fold(0, usize::saturating_add)
    }

    /// Reserve exactly these lengths in the cleared `arrays`; with `trim`,
    /// first free every array whose capacity exceeds its length.
    fn reserve(&self, arrays: &mut BatchArrays, trim: bool) {
        fn exact<T>(array: &mut Vec<T>, length: usize, trim: bool) {
            if trim && array.capacity() > length {
                *array = Vec::new();
            }
            array.reserve_exact(length);
        }
        exact(&mut arrays.values, self.values, trim);
        exact(&mut arrays.flags, self.flags, trim);
        exact(&mut arrays.weights, self.weights, trim);
        exact(&mut arrays.uvw, self.uvw, trim);
        exact(&mut arrays.time_offsets_s, self.times, trim);
        exact(&mut arrays.centroid_offsets_s, self.centroid, trim);
        exact(&mut arrays.interval_s, self.interval, trim);
        exact(&mut arrays.exposure_s, self.exposure, trim);
        exact(&mut arrays.field, self.times, trim);
        exact(&mut arrays.scan, self.scan, trim);
        exact(&mut arrays.fragments, self.times, trim);
    }
}

impl BatchBuffers {
    /// Buffers that never hold more than `envelope` bytes: a fill whose
    /// batch needs more is refused before anything is allocated, and arrays
    /// kept from earlier fills are freed when keeping them would exceed it.
    #[must_use]
    pub fn with_envelope(envelope: usize) -> Self {
        Self {
            envelope: Some(envelope),
            ..Self::default()
        }
    }

    /// The bytes these buffers may hold, if bounded.
    #[must_use]
    pub fn envelope(&self) -> Option<usize> {
        self.envelope
    }

    /// Empty every array, keeping its capacity, and forget the batch.
    pub fn clear(&mut self) {
        self.header = None;
        self.catalogue = None;
        let arrays = &mut self.arrays;
        arrays.values.clear();
        arrays.flags.clear();
        arrays.weights.clear();
        arrays.uvw.clear();
        arrays.time_offsets_s.clear();
        arrays.centroid_offsets_s.clear();
        arrays.interval_s.clear();
        arrays.exposure_s.clear();
        arrays.field.clear();
        arrays.scan.clear();
        arrays.fragments.clear();
    }

    /// Start filling a batch described by `header`, for an observation
    /// with `catalogue`: the header is checked against the catalogue (the
    /// partition, data group, channel window and correlation axis exist),
    /// and the arrays are cleared and reserved to exactly the batch's
    /// lengths, then lent through the returned fill.
    ///
    /// # Errors
    ///
    /// [`ObservationError::InvalidBatch`] when the header does not fit the
    /// catalogue; [`ObservationError::InvalidRequest`] when the batch needs
    /// more than the buffers' envelope. Nothing has been allocated, and the
    /// buffers hold no batch.
    pub fn begin(
        &mut self,
        header: BatchHeader,
        catalogue: &Arc<Catalogue>,
    ) -> Result<BatchFill<'_>, ObservationError> {
        self.clear();
        let entry = context(&header, catalogue)?;
        let lengths = Lengths::of(
            &header,
            entry.baselines.len(),
            header.correlations.products(entry).len(),
        )?;
        let trim = match self.envelope {
            Some(envelope) if lengths.bytes() > envelope => {
                return Err(ObservationError::InvalidRequest {
                    reason: format!(
                        "a batch of {} B exceeds its buffer's {envelope} B envelope",
                        lengths.bytes()
                    ),
                });
            }
            Some(envelope) => lengths.retained(&self.arrays) > envelope,
            None => false,
        };
        lengths.reserve(&mut self.arrays, trim);
        Ok(BatchFill {
            buffers: self,
            header,
            catalogue: Arc::clone(catalogue),
            lengths,
        })
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
            + arrays.flags.capacity() * size_of::<bool>()
            + arrays.weights.capacity() * size_of::<f32>()
            + (arrays.uvw.capacity()
                + arrays.time_offsets_s.capacity()
                + arrays.centroid_offsets_s.capacity()
                + arrays.interval_s.capacity()
                + arrays.exposure_s.capacity())
                * size_of::<f64>()
            + arrays.field.capacity() * size_of::<FieldId>()
            + arrays.scan.capacity() * size_of::<ScanId>()
            + arrays.fragments.capacity() * size_of::<GroupFragment>()
    }

    /// The batch the buffers hold, if a fill finished: views of the arrays
    /// and of the entry for the batch's partition in the catalogue the fill
    /// was checked against.
    #[must_use]
    pub fn batch<'a>(&'a self) -> Option<VisibilityBatch<'a>> {
        let (header, catalogue) = (self.header.as_ref()?, self.catalogue.as_ref()?);
        let entry = catalogue.partition(header.partition);
        let arrays = &self.arrays;
        let baselines = entry.baselines.len();
        let samples = header.times * baselines;
        let channels = header.channels;
        let products = header.correlations.products(entry);
        let correlations = products.len();
        let cube = [samples, channels, correlations];
        let roles = header.roles;
        let epochs = |offsets: &'a [f64]| {
            EpochView::new(
                entry.time_scale,
                entry.time_origin,
                ArrayView1::from(offsets),
            )
        };
        let group = &entry.data_groups[header.data_group];
        Some(VisibilityBatch {
            catalogue,
            partition: header.partition,
            time: epochs(&arrays.time_offsets_s),
            baselines: &entry.baselines,
            field: ArrayView1::from(&arrays.field[..]),
            scan: roles.scan.then(|| ArrayView1::from(&arrays.scan[..])),
            timing: SampleTiming {
                centroid: roles
                    .time_centroid
                    .then(|| epochs(&arrays.centroid_offsets_s)),
                interval_s: roles
                    .interval
                    .then(|| shared(header.interval, &arrays.interval_s)),
                exposure_s: roles
                    .exposure
                    .then(|| shared(header.exposure, &arrays.exposure_s)),
            },
            first_channel: header.first_channel,
            frequency: FrequencyView::new(
                entry.frequency_frame,
                ArrayView1::from(
                    &entry.frequencies_hz[header.first_channel..header.first_channel + channels],
                ),
            ),
            correlations: products,
            axis: &header.correlations,
            group: DataGroupView {
                entry: group,
                values: roles.values.then(|| cube_view(cube, &arrays.values)),
                flags: roles.flags.then(|| cube_view(cube, &arrays.flags)),
                weights: roles.weights.then(|| {
                    if header.weights_per_channel {
                        cube_view(cube, &arrays.weights)
                    } else {
                        // One weight per correlation, shared by every
                        // channel: the same view with a zero channel stride.
                        ArrayView3::from_shape(
                            cube.strides([correlations, 0, 1]),
                            &arrays.weights[..],
                        )
                        .expect("length checked at finish")
                    }
                }),
                weight_rule: header.weight_rule,
                uvw: roles.uvw.then(|| {
                    UvwView::new(
                        group.uvw_frame.effective,
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

    /// Check every array against the header, once, and keep the header and
    /// the catalogue: the lengths [`BatchBuffers::begin`] reserved; field
    /// and scan identities against the catalogue's counts; and the
    /// fragments, which must tile the samples in order by whole times.
    ///
    /// # Errors
    ///
    /// [`ObservationError::Shape`] or [`ObservationError::InvalidBatch`]: a
    /// cursor bug or a store the adapter should have refused, never a
    /// property a consumer must handle. The buffers then hold no batch.
    pub fn finish(self) -> Result<(), ObservationError> {
        let Self {
            buffers,
            header,
            catalogue,
            lengths,
        } = self;
        check(&buffers.arrays, &header, &catalogue, lengths)?;
        buffers.header = Some(header);
        buffers.catalogue = Some(catalogue);
        Ok(())
    }
}

fn invalid(reason: impl Into<String>) -> ObservationError {
    ObservationError::InvalidBatch {
        reason: reason.into(),
    }
}

/// `a × b`, or a shape error naming `array`.
fn product(array: &'static str, a: usize, b: usize) -> Result<usize, ObservationError> {
    a.checked_mul(b)
        .ok_or_else(|| invalid(format!("{array} size overflows")))
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
        Some(id) => Err(invalid(format!(
            "{what} {id} is not in the catalogue's {limit}"
        ))),
        None => Ok(()),
    }
}

/// The header's partition, data group, channel window and correlations, as
/// the catalogue describes them.
fn context<'c>(
    header: &BatchHeader,
    catalogue: &'c Catalogue,
) -> Result<&'c PartitionEntry, ObservationError> {
    let entry = catalogue
        .partitions
        .get(header.partition.0 as usize)
        .ok_or_else(|| invalid(format!("no partition {:?}", header.partition)))?;
    if header.data_group >= entry.data_groups.len() {
        return Err(invalid(format!("no data group {}", header.data_group)));
    }
    if header.channels == 0
        || header
            .first_channel
            .checked_add(header.channels)
            .is_none_or(|end| end > entry.frequencies_hz.len())
    {
        return Err(invalid(format!(
            "channels {}..{} outside the partition's {}",
            header.first_channel,
            header.first_channel.saturating_add(header.channels),
            entry.frequencies_hz.len()
        )));
    }
    if !header.correlations.fits(entry) {
        return Err(invalid(format!(
            "correlations {:?} are not the partition's {:?} in its order",
            header.correlations, entry.correlations
        )));
    }
    if header.times == 0 || entry.baselines.is_empty() {
        return Err(invalid(
            "a batch has at least one time, one baseline and one correlation",
        ));
    }
    Ok(entry)
}

fn check(
    arrays: &BatchArrays,
    header: &BatchHeader,
    catalogue: &Catalogue,
    lengths: Lengths,
) -> Result<(), ObservationError> {
    for (array, actual, expected) in [
        ("time", arrays.time_offsets_s.len(), lengths.times),
        ("field", arrays.field.len(), lengths.times),
        ("scan", arrays.scan.len(), lengths.scan),
        ("values", arrays.values.len(), lengths.values),
        ("flags", arrays.flags.len(), lengths.flags),
        ("weights", arrays.weights.len(), lengths.weights),
        ("uvw", arrays.uvw.len(), lengths.uvw),
        (
            "time centroid",
            arrays.centroid_offsets_s.len(),
            lengths.centroid,
        ),
        ("interval", arrays.interval_s.len(), lengths.interval),
        ("exposure", arrays.exposure_s.len(), lengths.exposure),
    ] {
        length(array, actual, expected)?;
    }
    identities(
        "field",
        arrays.field.iter().map(|field| field.0),
        catalogue.fields.len(),
    )?;
    identities(
        "scan",
        arrays.scan.iter().map(|scan| scan.0),
        catalogue.scans.len(),
    )?;
    let per_time = catalogue.partition(header.partition).baselines.len();
    fragments(arrays, header, per_time, lengths.times * per_time)
}

/// The fragments tile `0..samples` in order, each non-empty, split only
/// between times, all of this partition.
fn fragments(
    arrays: &BatchArrays,
    header: &BatchHeader,
    per_time: usize,
    samples: usize,
) -> Result<(), ObservationError> {
    if arrays.fragments.is_empty() {
        return Ok(());
    }
    let mut covered = 0;
    for fragment in &arrays.fragments {
        let range = &fragment.samples;
        if range.start != covered
            || range.end <= range.start
            || !range.end.is_multiple_of(per_time)
            || fragment.key.partition != header.partition
        {
            return Err(invalid(format!(
                "fragment {fragment:?} does not continue the batch at {covered}"
            )));
        }
        covered = range.end;
    }
    if covered == samples {
        Ok(())
    } else {
        Err(invalid(format!(
            "fragments cover {covered} of {samples} samples"
        )))
    }
}

impl BatchBuffers {
    /// Copy `batch` (typically a compute window of a larger batch) into
    /// these buffers, replacing what they held, through the same checked
    /// fill a cursor uses, and return the payload elements copied: the
    /// explicit, accounted packing that gives a window storage of its own.
    /// Strided views are gathered into standard order; a weight shared by
    /// every channel stays one per correlation; shared quantities keep
    /// their sharing; the piece refers to the batch's catalogue.
    ///
    /// # Errors
    ///
    /// When the batch's data group is not in the catalogue's partition, or
    /// the piece needs more than the buffers' envelope.
    pub fn pack(&mut self, batch: &VisibilityBatch<'_>) -> Result<usize, ObservationError> {
        let catalogue = batch.catalogue;
        let group = &batch.group;
        let data_group = catalogue
            .partition(batch.partition)
            .data_group(&group.entry.name)
            .map(|(index, _)| index)
            .ok_or_else(|| invalid(format!("no data group {}", group.entry.name)))?;
        let sharing = |shared: Option<Shared<'_, f64>>| match shared {
            Some(Shared::PerTime(_)) => Sharing::PerTime,
            Some(Shared::Constant(value)) => Sharing::Constant(value),
            Some(Shared::PerSample(_)) | None => Sharing::PerSample,
        };
        let weights_per_channel = group
            .weights
            .is_none_or(|weights| weights.strides()[1] != 0);
        let header = BatchHeader {
            partition: batch.partition,
            data_group,
            times: batch.times(),
            first_channel: batch.first_channel,
            channels: batch.channel_count(),
            correlations: batch.axis.clone(),
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
            weights_per_channel,
            weight_rule: group.weight_rule,
            interval: sharing(batch.timing.interval_s),
            exposure: sharing(batch.timing.exposure_s),
        };
        let mut fill = self.begin(header, catalogue)?;
        let arrays = fill.arrays();
        arrays.fragments.extend_from_slice(batch.fragments);
        arrays.time_offsets_s.extend(batch.time.offsets_s().iter());
        arrays.field.extend(batch.field.iter());
        if let Some(scan) = batch.scan {
            arrays.scan.extend(scan.iter());
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
        let mut copied = 0;
        if let Some(values) = group.values {
            arrays.values.extend(values.iter());
            copied += values.len();
        }
        if let Some(flags) = group.flags {
            arrays.flags.extend(flags.iter());
            copied += flags.len();
        }
        if let Some(weights) = group.weights {
            if weights_per_channel {
                arrays.weights.extend(weights.iter());
                copied += weights.len();
            } else {
                let per_correlation = weights.index_axis(Axis(1), 0);
                arrays.weights.extend(per_correlation.iter());
                copied += per_correlation.len();
            }
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
pub(crate) mod tests {
    use casa_types::Correlation;
    use casa_types::measures::EpochRef;
    use casa_types::measures::MjdHighPrec;
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;

    use super::*;
    use crate::catalogue::{
        AntennaId, DataGroupEntry, FieldEntry, ObservationId, OrderedBaseline, ScanEntry, UvwFrame,
        VisibilityUnits,
    };
    use crate::grouping::{FragmentPosition, GroupKey};

    /// Three antennas, two baselines, two channels, one correlation.
    pub(crate) fn catalogue() -> Arc<Catalogue> {
        let baseline = |a, b| OrderedBaseline {
            antenna1: AntennaId(a),
            antenna2: AntennaId(b),
        };
        let antenna = |name: &str| crate::AntennaEntry {
            name: name.to_string(),
            station: name.to_string(),
            position: casa_types::measures::position::MPosition::new_itrf(0.0, 0.0, 0.0),
            mount: "ALT-AZ".to_string(),
            dish_diameter: casa_types::quanta::Quantity::new(25.0, "m").expect("quantity"),
            receptors: vec!["X".to_string()],
            receptor_angles: Vec::new(),
        };
        Arc::new(Catalogue {
            antennas: vec![antenna("A"), antenna("B"), antenna("C")],
            fields: vec![FieldEntry {
                name: "f".to_string(),
                phase_centre: casa_types::measures::direction::MDirection::from_angles(
                    0.0,
                    0.0,
                    DirectionRef::J2000,
                ),
                reference_epoch: None,
                source_name: None,
            }],
            scans: vec![ScanEntry {
                name: "1".to_string(),
                number: Some(1),
                intents: Vec::new(),
            }],
            observations: vec![crate::ObservationEntry::default()],
            partitions: vec![PartitionEntry {
                spectral_window: "spw".to_string(),
                frequencies_hz: vec![1.0e9, 1.1e9],
                frequency_frame: FrequencyRef::LSRK,
                channel_widths_hz: vec![1.0e8; 2],
                correlations: vec![Correlation::XX, Correlation::YY],
                intents: Vec::new(),
                observation: ObservationId(0),
                feeds: None,
                fields: vec![FieldId(0)],
                scans: vec![ScanId(0)],
                baselines: vec![baseline(0, 1), baseline(1, 2)],
                time_scale: EpochRef::UTC,
                time_origin: MjdHighPrec::new(40_587.0, 0.0),
                data_groups: vec![DataGroupEntry {
                    name: "base".to_string(),
                    visibilities: "VISIBILITY".to_string(),
                    flags: "FLAG".to_string(),
                    weights: "WEIGHT".to_string(),
                    uvw: "UVW".to_string(),
                    units: VisibilityUnits::Undeclared,
                    uvw_frame: UvwFrame {
                        effective: DirectionRef::J2000,
                        stored_label: None,
                    },
                }],
            }],
        })
    }

    fn header(times: usize) -> BatchHeader {
        BatchHeader {
            partition: PartitionId(0),
            data_group: 0,
            times,
            first_channel: 0,
            channels: 2,
            correlations: CorrelationAxis::Subset {
                products: [Correlation::XX].into(),
                indices: [0].into(),
            },
            roles: Roles {
                values: false,
                uvw: false,
                ..Roles::ALL
            },
            weights_per_channel: false,
            weight_rule: WeightRule::Stored,
            interval: Sharing::Constant(10.0),
            exposure: Sharing::PerSample,
        }
    }

    fn key() -> GroupKey {
        GroupKey {
            partition: PartitionId(0),
            field: None,
            scan: None,
            bin: 0,
        }
    }

    /// Two times of two baselines, two channels, one correlation, closing
    /// its group in one fragment.
    pub(crate) fn fill(
        buffers: &mut BatchBuffers,
        tweak: impl FnOnce(&mut BatchHeader, &mut BatchArrays),
    ) -> Result<(), ObservationError> {
        let mut header = header(2);
        let mut arrays = BatchArrays::default();
        arrays.time_offsets_s.extend([0.0, 10.0]);
        arrays.field.extend([FieldId(0), FieldId(0)]);
        arrays.flags.extend([false; 8]);
        arrays.weights.extend([1.0, 2.0, 3.0, 4.0]);
        arrays.centroid_offsets_s.extend([0.0, 0.0, 10.0, 10.0]);
        arrays.exposure_s.extend([10.0; 4]);
        arrays.scan.extend([ScanId(0), ScanId(0)]);
        arrays.fragments.push(GroupFragment {
            key: key(),
            samples: 0..4,
            position: FragmentPosition::Whole,
            clipped: false,
        });
        tweak(&mut header, &mut arrays);
        let mut fill = buffers.begin(header, &catalogue())?;
        *fill.arrays() = arrays;
        fill.finish()
    }

    #[test]
    fn a_checked_fill_builds_the_batch_its_header_describes() {
        let mut buffers = BatchBuffers::default();
        fill(&mut buffers, |_, _| {}).expect("fill");
        let batch = buffers.batch().expect("batch");
        assert_eq!(batch.samples(), 4);
        assert_eq!(batch.shape(), [4, 2, 1]);
        assert_eq!(batch.interval_s(3), Some(10.0));
        assert_eq!(batch.scan(3), Some(ScanId(0)));
        assert_eq!(batch.timing.centroid.expect("centroid").len(), 4);
        assert_eq!(batch.fragments.len(), 1);
        assert_eq!(batch.frequency.hz()[1], 1.1e9, "from the catalogue");
        assert_eq!(batch.baselines.len(), 2, "from the catalogue");
        let weights = batch.group.weights.expect("weights");
        assert_eq!(
            weights.strides(),
            [1, 0, 1],
            "one weight per correlation, every channel"
        );
        assert_eq!(weights[[2, 1, 0]], 3.0);
        assert!(weights.as_slice().is_none());
        let window = batch.channel_window(1..2).expect("window");
        assert!(window.fragments.is_empty(), "a window never closes a group");
        assert_eq!(window.scan(3), Some(ScanId(0)));
        assert_eq!(window.frequency.hz()[0], 1.1e9);
        let times = batch.sample_window(2..4).expect("one time");
        assert!(times.fragments.is_empty());
        assert_eq!(
            times.timing.centroid.expect("centroid").offsets_s()[0],
            10.0
        );
        assert!(batch.sample_window(1..3).is_err(), "splits a time");
    }

    #[test]
    fn a_fill_that_disagrees_with_its_header_or_catalogue_is_refused() {
        let mut buffers = BatchBuffers::default();
        type Tweak = fn(&mut BatchHeader, &mut BatchArrays);
        let cases: [(&str, Tweak); 9] = [
            ("short flags", |_, a| {
                a.flags.pop();
            }),
            ("scan out of the catalogue", |_, a| a.scan[1] = ScanId(1)),
            ("field out of the catalogue", |_, a| a.field[0] = FieldId(1)),
            ("a gap before the fragment", |_, a| {
                a.fragments[0].samples = 2..4
            }),
            ("a fragment inside a time", |_, a| {
                a.fragments[0].samples = 0..3
            }),
            ("a short exposure", |_, a| {
                a.exposure_s.pop();
            }),
            ("a channel window past the partition", |h, _| {
                h.first_channel = 1
            }),
            ("a correlation the partition lacks", |h, _| {
                h.correlations = CorrelationAxis::Subset {
                    products: [Correlation::RR].into(),
                    indices: [0].into(),
                }
            }),
            ("products out of the partition's order", |h, _| {
                h.correlations = CorrelationAxis::Subset {
                    products: [Correlation::YY, Correlation::XX].into(),
                    indices: [1, 0].into(),
                }
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
        fill(&mut buffers, |_, _| {}).expect("fill");
        let batch = buffers.batch().expect("batch");
        let mut packed = BatchBuffers::default();
        let copied = packed
            .pack(&batch.channel_window(0..1).expect("window"))
            .expect("pack");
        assert_eq!(copied, 4 + 4, "four flags and four per-correlation weights");
        let piece = packed.batch().expect("piece");
        assert_eq!(piece.channel_count(), 1);
        assert_eq!(piece.group.weights.expect("weights")[[3, 0, 0]], 4.0);
        assert!(matches!(
            piece.timing.interval_s,
            Some(Shared::Constant(10.0))
        ));
        assert!(piece.fragments.is_empty());
    }
}
