// SPDX-License-Identifier: LGPL-3.0-or-later
//! Owned native rows of one bounded source block.

use casa_imaging_operator::{NativeRow, RowContext, RowSpectrum};
use num_complex::Complex32;

/// Where a row lives in its MeasurementSet, for the model-column writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowAddress {
    /// Physical MAIN row.
    pub physical_row: u64,
    /// MAIN `DATA_DESC_ID`.
    pub data_description: i32,
}

impl RowAddress {
    /// The address of a row no MeasurementSet row holds: what a source
    /// that writes nothing back gives (the observation-batch route until
    /// it writes through an update session, step 5 of #932).
    pub const UNADDRESSED: Self = Self {
        physical_row: u64::MAX,
        data_description: -1,
    };
}

/// A row's baseline in the frame of one image domain: CASA rotates each
/// mapper's uvw to its own phase centre (`FTMachine::rotateUVW`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DomainProjection {
    /// Baseline `[u, v, w]` in metres in the domain's phase-centre frame.
    pub uvw_m: [f64; 3],
    /// Path-length shift to the domain's phase centre in metres.
    pub phase_shift_m: f64,
    /// Offset of the row's pointing centre from the domain's reference
    /// direction as direction cosines `[Δl, Δm]` along the image axes, in
    /// radians ([`NativeRow::pointing_offset_rad`]).
    pub pointing_offset_rad: [f64; 2],
}

/// Values of one native row shared by every image domain.
#[derive(Clone, Copy, Debug)]
pub struct NativeRowHeader {
    /// Row flag (MAIN `FLAG_ROW`, or a row CASA excludes from imaging).
    pub row_flag: bool,
    /// Per-row context for kernel-set cell selection.
    pub context: RowContext,
    /// MeasurementSet address.
    pub address: RowAddress,
}

/// The largest block a source delivers ([`super::BoundedSource::maximum_block`]):
/// what each pass sizes its per-block storage by, and admits before it
/// allocates it.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BlockShape {
    /// Rows.
    pub rows: usize,
    /// Selected channels per row and their frequencies' extent, which bound
    /// the samples a row places ([`RowSpectrum`]).
    pub spectrum: RowSpectrum,
    /// Selected correlations per row.
    pub correlations: usize,
    /// Image domains each row is projected on.
    pub domains: usize,
}

impl BlockShape {
    /// Selected channels per row.
    #[must_use]
    pub const fn channels(self) -> usize {
        self.spectrum.channels
    }

    /// Bytes a [`NativeBlock`] of this shape holds
    /// ([`NativeBlock::with_capacity`]).
    #[must_use]
    pub const fn bytes(self) -> u64 {
        let channels = self.channels();
        let cells = channels * self.correlations;
        let per_row = size_of::<NativeRowHeader>()
            + size_of::<u32>()
            + self.domains * size_of::<DomainProjection>()
            + channels * size_of::<f64>()
            + cells * (size_of::<Complex32>() + size_of::<f32>() + size_of::<bool>());
        (self.rows * per_row + (channels + self.correlations) * size_of::<u32>()) as u64
    }

    /// Bytes a [`NativeBlock`] of this shape holds when its rows share one
    /// spectrum and their samples ([`NativeBlock::shared_with_capacity`]):
    /// each row's header, order entry and projections, and one row's
    /// frequencies, values, weights and flags.
    #[must_use]
    pub const fn shared_bytes(self) -> u64 {
        let channels = self.channels();
        let cells = channels * self.correlations;
        let per_row = size_of::<NativeRowHeader>()
            + size_of::<u32>()
            + self.domains * size_of::<DomainProjection>();
        let samples = channels * size_of::<f64>()
            + cells * (size_of::<Complex32>() + size_of::<f32>() + size_of::<bool>());
        (self.rows * per_row + samples + (channels + self.correlations) * size_of::<u32>()) as u64
    }
}

/// One block of native rows sharing a channel and correlation layout,
/// projected on every image domain of the pass.
///
/// Arrays are `[row][channel][correlation]`, correlation fastest, over the
/// selected channels and correlations in the operator's routing order;
/// projections are `[row][domain]`. Frequencies are per row and channel
/// because Doppler tracking moves them. Capacity survives
/// [`NativeBlock::reset`], so a source allocates once.
///
/// Rows that all have one spectrum and, in every sample, a zero value, a
/// unit weight and no flag, as a simulator's rows to predict do, can share
/// them ([`NativeBlock::reset_shared`]): the block keeps them once, and a
/// row adds only its header and projections.
///
/// The rows also have a locality order ([`Self::order_rows_by_baseline`]):
/// a baseline's successive integrations fall on nearly the same uv cells,
/// so a pass that reads them together reuses the grid cells it just read
/// and wrote.
#[derive(Clone, Debug, Default)]
pub struct NativeBlock {
    domains: usize,
    rows: Vec<NativeRowHeader>,
    /// Row indices in locality order; empty in arrival order.
    order: Vec<u32>,
    projections: Vec<DomainProjection>,
    channel_indices: Vec<u32>,
    correlation_indices: Vec<u32>,
    frequencies_hz: Vec<f64>,
    values: Vec<Complex32>,
    weights: Vec<f32>,
    flags: Vec<bool>,
    /// Whether every row reads the first row's frequencies, values, weights
    /// and flags ([`Self::reset_shared`]).
    shared: bool,
}

impl NativeBlock {
    /// An empty block with room for every block of `shape`, so filling it
    /// never grows it beyond [`BlockShape::bytes`].
    #[must_use]
    pub fn with_capacity(shape: BlockShape) -> Self {
        Self::sized(shape, shape.rows)
    }

    /// An empty block with room for every block of `shape` whose rows share
    /// their spectrum and samples ([`Self::reset_shared`]), so filling it
    /// never grows it beyond [`BlockShape::shared_bytes`].
    #[must_use]
    pub fn shared_with_capacity(shape: BlockShape) -> Self {
        Self::sized(shape, 1)
    }

    /// An empty block with room for `shape`'s rows and the spectra and
    /// samples of `sample_rows` of them.
    fn sized(shape: BlockShape, sample_rows: usize) -> Self {
        let channels = shape.channels();
        let cells = sample_rows * channels * shape.correlations;
        Self {
            domains: shape.domains,
            rows: Vec::with_capacity(shape.rows),
            order: Vec::with_capacity(shape.rows),
            projections: Vec::with_capacity(shape.rows * shape.domains),
            channel_indices: Vec::with_capacity(channels),
            correlation_indices: Vec::with_capacity(shape.correlations),
            frequencies_hz: Vec::with_capacity(sample_rows * channels),
            values: Vec::with_capacity(cells),
            weights: Vec::with_capacity(cells),
            flags: Vec::with_capacity(cells),
            shared: false,
        }
    }

    /// Bytes the block holds now, from its capacities: at most
    /// [`BlockShape::bytes`] for a block [`Self::with_capacity`] made and
    /// filled within its shape.
    #[must_use]
    pub fn capacity_bytes(&self) -> u64 {
        (self.rows.capacity() * size_of::<NativeRowHeader>()
            + self.projections.capacity() * size_of::<DomainProjection>()
            + (self.order.capacity()
                + self.channel_indices.capacity()
                + self.correlation_indices.capacity())
                * size_of::<u32>()
            + self.frequencies_hz.capacity() * size_of::<f64>()
            + self.values.capacity() * size_of::<Complex32>()
            + self.weights.capacity() * size_of::<f32>()
            + self.flags.capacity() * size_of::<bool>()) as u64
    }

    /// Empty the block and set the layout of the rows that follow: the
    /// number of image domains each row is projected on, and the native
    /// channel and correlation indices every row selects.
    pub fn reset(&mut self, domains: usize, channel_indices: &[u32], correlation_indices: &[u32]) {
        assert!(
            domains > 0 && !channel_indices.is_empty() && !correlation_indices.is_empty(),
            "a native block projects on one domain and selects one channel and correlation"
        );
        self.domains = domains;
        self.shared = false;
        self.rows.clear();
        self.order.clear();
        self.projections.clear();
        self.frequencies_hz.clear();
        self.values.clear();
        self.weights.clear();
        self.flags.clear();
        self.channel_indices.clear();
        self.channel_indices.extend_from_slice(channel_indices);
        self.correlation_indices.clear();
        self.correlation_indices
            .extend_from_slice(correlation_indices);
    }

    /// [`Self::reset`] for rows that all have `frequencies_hz` and, in
    /// every channel and correlation, a zero value, a unit weight and no
    /// flag: the block keeps them once, and the rows that follow come from
    /// [`Self::push_shared_row`]. Each row then reads exactly as
    /// [`Self::push_row`] would have made it with those samples.
    pub fn reset_shared(
        &mut self,
        domains: usize,
        channel_indices: &[u32],
        correlation_indices: &[u32],
        frequencies_hz: &[f64],
    ) {
        self.reset(domains, channel_indices, correlation_indices);
        assert_eq!(
            frequencies_hz.len(),
            channel_indices.len(),
            "frequencies per row"
        );
        let cells = channel_indices.len() * correlation_indices.len();
        self.frequencies_hz.extend_from_slice(frequencies_hz);
        self.values.resize(cells, Complex32::default());
        self.weights.resize(cells, 1.0);
        self.flags.resize(cells, false);
        self.shared = true;
    }

    /// Append one row of the shared spectrum and samples
    /// ([`Self::reset_shared`]): its header and its projection on every
    /// domain.
    pub fn push_shared_row(&mut self, header: NativeRowHeader, projections: &[DomainProjection]) {
        assert!(self.shared, "shared rows follow reset_shared");
        assert_eq!(projections.len(), self.domains, "one projection per domain");
        self.rows.push(header);
        self.order.clear();
        self.projections.extend_from_slice(projections);
    }

    /// Append one row: its projection on every domain, `frequencies_hz` per
    /// channel, `values`, `weights` and `flags` per channel and correlation.
    pub fn push_row(
        &mut self,
        header: NativeRowHeader,
        projections: &[DomainProjection],
        frequencies_hz: &[f64],
        values: &[Complex32],
        weights: &[f32],
        flags: &[bool],
    ) {
        let cells = self.channels() * self.correlations();
        assert!(
            values.len() == cells && weights.len() == cells && flags.len() == cells,
            "values, weights and flags per row must be channels × correlations"
        );
        self.push_row_with(header, projections, frequencies_hz, |v, w, f| {
            v.copy_from_slice(values);
            w.copy_from_slice(weights);
            f.copy_from_slice(flags);
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap_or_else(|never| match never {});
    }

    /// [`Self::push_row`], with `fill` writing the row's values, weights and
    /// flags in place: it receives them zeroed, `channels × correlations`
    /// each, so a source copies each sample once. When `fill` fails the
    /// block is left as it was and the error returned.
    pub fn push_row_with<E>(
        &mut self,
        header: NativeRowHeader,
        projections: &[DomainProjection],
        frequencies_hz: &[f64],
        fill: impl FnOnce(&mut [Complex32], &mut [f32], &mut [bool]) -> Result<(), E>,
    ) -> Result<(), E> {
        assert!(!self.shared, "a block of shared rows takes push_shared_row");
        let cells = self.channels() * self.correlations();
        assert_eq!(projections.len(), self.domains, "one projection per domain");
        assert_eq!(frequencies_hz.len(), self.channels(), "frequencies per row");
        let start = self.values.len();
        self.values.resize(start + cells, Complex32::default());
        self.weights.resize(start + cells, 0.0);
        self.flags.resize(start + cells, false);
        if let Err(error) = fill(
            &mut self.values[start..],
            &mut self.weights[start..],
            &mut self.flags[start..],
        ) {
            self.values.truncate(start);
            self.weights.truncate(start);
            self.flags.truncate(start);
            return Err(error);
        }
        self.rows.push(header);
        self.order.clear();
        self.projections.extend_from_slice(projections);
        self.frequencies_hz.extend_from_slice(frequencies_hz);
        Ok(())
    }

    /// Order the rows for locality: by antenna pair, a baseline's rows in
    /// arrival order (time order for a MeasurementSet). Appending a row
    /// returns the block to arrival order.
    pub fn order_rows_by_baseline(&mut self) {
        let rows = &self.rows;
        self.order.clear();
        self.order.extend(0..rows.len() as u32);
        self.order
            .sort_unstable_by_key(|&index| (rows[index as usize].context.antennas, index));
    }

    /// The index of the row at `position` of the locality order: the row
    /// itself in arrival order.
    #[must_use]
    pub fn ordered_row(&self, position: usize) -> usize {
        self.order
            .get(position)
            .map_or(position, |&index| index as usize)
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the block holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Image domains each row is projected on.
    #[must_use]
    pub fn domains(&self) -> usize {
        self.domains
    }

    /// Selected channels per row.
    #[must_use]
    pub fn channels(&self) -> usize {
        self.channel_indices.len()
    }

    /// Selected correlations per row.
    #[must_use]
    pub fn correlations(&self) -> usize {
        self.correlation_indices.len()
    }

    /// Native channel index of each selected channel.
    #[must_use]
    pub fn channel_indices(&self) -> &[u32] {
        &self.channel_indices
    }

    /// Correlation index of each selected correlation.
    #[must_use]
    pub fn correlation_indices(&self) -> &[u32] {
        &self.correlation_indices
    }

    /// Header of row `index`.
    #[must_use]
    pub fn header(&self, index: usize) -> &NativeRowHeader {
        &self.rows[index]
    }

    /// Row `index` as the operator of image domain `domain` reads it.
    #[must_use]
    pub fn row(&self, domain: usize, index: usize) -> NativeRow<'_> {
        assert!(
            domain < self.domains,
            "the block projects on {} domains",
            self.domains
        );
        let header = &self.rows[index];
        let projection = &self.projections[index * self.domains + domain];
        let channels = self.channels();
        let cells = channels * self.correlations();
        // Shared rows all read the one row of samples the block keeps.
        let samples = if self.shared { 0 } else { index };
        NativeRow {
            uvw_m: projection.uvw_m,
            phase_shift_m: projection.phase_shift_m,
            pointing_offset_rad: projection.pointing_offset_rad,
            frequencies_hz: &self.frequencies_hz[samples * channels..(samples + 1) * channels],
            values: &self.values[samples * cells..(samples + 1) * cells],
            weights: &self.weights[samples * cells..(samples + 1) * cells],
            flags: &self.flags[samples * cells..(samples + 1) * cells],
            row_flag: header.row_flag,
            context: header.context,
        }
    }
}

/// The row at `position`: of the block's locality order when `ordered`
/// ([`NativeBlock::ordered_row`]), else the row itself.
pub(super) fn row_at(block: &NativeBlock, ordered: bool, position: usize) -> usize {
    if ordered {
        block.ordered_row(position)
    } else {
        position
    }
}

/// The first native cell of the row at `position` of a block of `channels`
/// channels.
pub(super) fn cell_of(position: usize, channels: usize) -> u32 {
    u32::try_from(position * channels).expect("a block's native cells fit u32 indices")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(antennas: [u32; 2], row: u64) -> NativeRowHeader {
        NativeRowHeader {
            row_flag: false,
            context: RowContext {
                original_uvw_m: None,
                time_s: row as f64,
                antennas,
                antenna_types: [0, 0],
                parallactic_angle_rad: [0.0, 0.0],
                field: 0,
                spectral_window: 0,
            },
            address: RowAddress {
                physical_row: row,
                data_description: 0,
            },
        }
    }

    /// The locality order is a permutation of the rows that groups each
    /// baseline and keeps its rows in arrival order; appending a row
    /// returns the block to arrival order.
    #[test]
    fn rows_ordered_by_baseline_keep_each_baselines_arrival_order() {
        let baselines = [[0, 1], [0, 2], [1, 2]];
        let mut block = NativeBlock::default();
        block.reset(1, &[0], &[0]);
        let projection = DomainProjection {
            uvw_m: [0.0; 3],
            phase_shift_m: 0.0,
            pointing_offset_rad: [0.0; 2],
        };
        let push = |block: &mut NativeBlock, row: u64| {
            block.push_row(
                header(baselines[row as usize % 3], row),
                &[projection],
                &[1.0e9],
                &[Complex32::default()],
                &[1.0],
                &[false],
            );
        };
        for row in 0..9 {
            push(&mut block, row);
        }
        assert_eq!(block.ordered_row(4), 4, "arrival order until ordered");
        block.order_rows_by_baseline();
        let ordered = (0..9)
            .map(|position| block.ordered_row(position))
            .collect::<Vec<_>>();
        assert_eq!(ordered, [0, 3, 6, 1, 4, 7, 2, 5, 8]);
        push(&mut block, 9);
        assert_eq!(block.ordered_row(1), 1, "appending restores arrival order");
    }

    /// Rows that share the spectrum and samples read exactly as the same
    /// rows pushed whole, on every domain, and a block made for them holds
    /// no more than [`BlockShape::shared_bytes`] however many it takes.
    #[test]
    fn shared_rows_read_as_rows_pushed_whole() {
        let (channels, correlations, domains, rows) = (5, 2, 2, 7);
        let frequencies = [1.0e9, 1.1e9, 1.2e9, 1.3e9, 1.4e9];
        let channel_indices = [0, 1, 2, 3, 4];
        let correlation_indices = [0, 3];
        let cells = channels * correlations;
        let shape = BlockShape {
            rows,
            spectrum: RowSpectrum::evenly_spaced(channels),
            correlations,
            domains,
        };
        let projections = |row: usize| {
            [0, 1].map(|domain| DomainProjection {
                uvw_m: [row as f64, domain as f64, 0.5],
                phase_shift_m: 0.25 * domain as f64,
                pointing_offset_rad: [0.0; 2],
            })
        };
        let mut whole = NativeBlock::with_capacity(shape);
        whole.reset(domains, &channel_indices, &correlation_indices);
        let mut shared = NativeBlock::shared_with_capacity(shape);
        for _ in 0..2 {
            shared.reset_shared(
                domains,
                &channel_indices,
                &correlation_indices,
                &frequencies,
            );
            for row in 0..rows {
                shared.push_shared_row(header([0, row as u32 + 1], row as u64), &projections(row));
            }
        }
        for row in 0..rows {
            whole.push_row(
                header([0, row as u32 + 1], row as u64),
                &projections(row),
                &frequencies,
                &vec![Complex32::default(); cells],
                &vec![1.0; cells],
                &vec![false; cells],
            );
        }
        assert_eq!(shared.len(), rows);
        for row in 0..rows {
            for domain in 0..domains {
                let (a, b) = (whole.row(domain, row), shared.row(domain, row));
                assert_eq!(a.uvw_m, b.uvw_m, "row {row}, domain {domain}");
                assert_eq!(a.phase_shift_m, b.phase_shift_m);
                assert_eq!(a.frequencies_hz, b.frequencies_hz);
                assert_eq!(a.values, b.values);
                assert_eq!(a.weights, b.weights);
                assert_eq!(a.flags, b.flags);
                assert_eq!(a.context.antennas, b.context.antennas);
                assert_eq!(a.row_flag, b.row_flag);
            }
        }
        assert!(
            shared.capacity_bytes() <= shape.shared_bytes(),
            "{} against {}",
            shared.capacity_bytes(),
            shape.shared_bytes()
        );
        assert!(shape.shared_bytes() < shape.bytes());
    }
}
