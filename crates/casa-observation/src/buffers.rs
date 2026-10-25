// SPDX-License-Identifier: LGPL-3.0-or-later
//! Caller-owned reusable storage that a cursor fills and a batch borrows,
//! and the one checked path from filled arrays to a batch.

use std::sync::Arc;

use casa_types::Correlation;
use casa_types::measures::{EpochView, FrequencyView, UvwView};
use ndarray::{ArrayView1, ArrayView2, ArrayView3, Axis, ShapeBuilder};
use num_complex::Complex32;

use crate::ObservationError;
use crate::batch::{DataGroupView, SampleTiming, Shared, VisibilityBatch};
use crate::catalogue::{Catalogue, FieldId, PartitionEntry, PartitionId, ScanId};
use crate::grouping::GroupFragment;
use crate::request::Roles;

/// The storage behind one [`VisibilityBatch`].
///
/// Owned by the caller (or a [`BufferPool`](crate::BufferPool)), filled in
/// place by a cursor and lent to the batch. Its arrays are private: a
/// cursor fills them through [`Self::begin`], and [`BatchFill::finish`]
/// checks them against the batch's [`BatchHeader`] and the observation's
/// [`Catalogue`] once; the buffers then keep the header and a shared
/// reference to that catalogue. The batch is built from the header, the
/// arrays and the catalogue's partition entry and nothing else, so no
/// second description of the same data can drift from the arrays: the
/// per-partition constants (frequencies, correlations, baselines, time
/// system, data group) are never copied into a batch, and no other
/// catalogue can be paired with the arrays.
///
/// Clearing keeps capacity, so after the first fill of the largest batch a
/// traversal allocates nothing. The buffers are `Send`: they can be handed
/// between work units whole, which is how a batch's lifetime is separated
/// from a traversal's.
#[derive(Debug, Default)]
pub struct BatchBuffers {
    arrays: BatchArrays,
    header: Option<BatchHeader>,
    catalogue: Option<Arc<Catalogue>>,
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
    /// The delivered correlation products, in the partition's order.
    pub correlations: Vec<Correlation>,
    /// The roles filled.
    pub roles: Roles,
    /// Whether the weights are per channel (`[sample][channel][correlation]`)
    /// or one per correlation shared by every channel.
    pub weights_per_channel: bool,
    /// How intervals are stored, when the roles include them.
    pub interval: Sharing<f64>,
    /// How exposures are stored, when the roles include them.
    pub exposure: Sharing<f64>,
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
        let correlations = header.correlations.len();
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
            correlations: &header.correlations,
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

    /// Check every array against the header and `catalogue`, once, and
    /// keep the header and the catalogue: the partition, data group, channel
    /// window and correlations exist in the catalogue; lengths (with
    /// overflow-checked sizes); field and scan identities against the
    /// catalogue's counts; and the fragments, which must tile the samples in
    /// order by whole times.
    ///
    /// # Errors
    ///
    /// [`ObservationError::Shape`] or [`ObservationError::InvalidBatch`]: a
    /// cursor bug or a store the adapter should have refused, never a
    /// property a consumer must handle. The buffers then hold no batch.
    pub fn finish(self, catalogue: &Arc<Catalogue>) -> Result<(), ObservationError> {
        let Self { buffers, header } = self;
        check(&buffers.arrays, &header, catalogue)?;
        buffers.header = Some(header);
        buffers.catalogue = Some(Arc::clone(catalogue));
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
    let mut last = None;
    for correlation in &header.correlations {
        let index = entry
            .correlation_index(*correlation)
            .ok_or_else(|| invalid(format!("{correlation:?} is not in the partition")))?;
        if last.is_some_and(|last| index <= last) {
            return Err(invalid("correlations are not in the partition's order"));
        }
        last = Some(index);
    }
    if header.correlations.is_empty() || header.times == 0 || entry.baselines.is_empty() {
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
) -> Result<(), ObservationError> {
    let entry = context(header, catalogue)?;
    let times = header.times;
    let samples = product("samples", times, entry.baselines.len())?;
    let correlations = header.correlations.len();
    length("time", arrays.time_offsets_s.len(), times)?;
    length("field", arrays.field.len(), times)?;
    identities(
        "field",
        arrays.field.iter().map(|field| field.0),
        catalogue.fields.len(),
    )?;
    let roles = header.roles;
    let sized = |wanted: bool, name: &'static str, actual: usize, expected: usize| {
        length(name, actual, if wanted { expected } else { 0 })
    };
    sized(roles.scan, "scan", arrays.scan.len(), times)?;
    identities(
        "scan",
        arrays.scan.iter().map(|scan| scan.0),
        catalogue.scans.len(),
    )?;
    let cell = product(
        "cube",
        samples,
        product("cube", header.channels, correlations)?,
    )?;
    sized(roles.values, "values", arrays.values.len(), cell)?;
    sized(roles.flags, "flags", arrays.flags.len(), cell)?;
    let weights = if header.weights_per_channel {
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
    let shared = |sharing: Sharing<f64>| match sharing {
        Sharing::PerSample => samples,
        Sharing::PerTime => times,
        Sharing::Constant(_) => 0,
    };
    sized(
        roles.interval,
        "interval",
        arrays.interval_s.len(),
        shared(header.interval),
    )?;
    sized(
        roles.exposure,
        "exposure",
        arrays.exposure_s.len(),
        shared(header.exposure),
    )?;
    fragments(arrays, header, entry.baselines.len(), samples)
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
    /// When the batch's data group is not in the catalogue's partition.
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
            correlations: batch.correlations.to_vec(),
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
            interval: sharing(batch.timing.interval_s),
            exposure: sharing(batch.timing.exposure_s),
        };
        let mut fill = self.begin(header);
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
        fill.finish(catalogue)?;
        Ok(copied)
    }
}

#[cfg(test)]
pub(crate) mod tests {
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
            correlations: vec![Correlation::XX],
            roles: Roles {
                values: false,
                uvw: false,
                ..Roles::ALL
            },
            weights_per_channel: false,
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
        });
        tweak(&mut header, &mut arrays);
        let mut fill = buffers.begin(header);
        *fill.arrays() = arrays;
        fill.finish(&catalogue())
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
        let cases: [(&str, Tweak); 8] = [
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
                h.correlations = vec![Correlation::RR]
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
