// SPDX-License-Identifier: LGPL-3.0-or-later
//! How a pass splits accumulation across workers and planes across waves.

use std::ops::Range;

use casa_imaging_deconvolution::resident_bytes_per_cell;
use casa_imaging_operator::{
    Basis, CellHold, MeasurementOperator, ModeSet, Placement, PlaneRange, Tile, grid_planning_bytes,
};

use super::{BackendChoice, BlockShape, PassDomain};

/// How one pass divides an image domain's grid accumulation among owners
/// (one per worker).
#[derive(Clone, Debug, PartialEq)]
pub enum Partition {
    /// Each owner accumulates a disjoint contiguous range of each wave's
    /// planes. Nothing is merged, so the result does not depend on the owner
    /// count.
    Planes {
        /// Number of owners.
        owners: usize,
    },
    /// Each owner accumulates the samples whose anchor row lies in its
    /// region, on a tile of the region plus `halo` rows; tiles are added in
    /// region order before the transform.
    Regions {
        /// The owners' regions, top to bottom.
        regions: Vec<Region>,
        /// Rows each tile extends past its region: the kernel set's largest
        /// half support along `y`.
        halo: usize,
    },
}

/// One owner of a [`Partition::Regions`] pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Region {
    /// Anchor rows (grid `y`) owned.
    pub rows: Range<usize>,
    /// The accumulated window: the owned rows plus the halo, clipped to the grid.
    pub tile: Tile,
}

impl Partition {
    /// `owners` horizontal strips of `operator`'s grid of near-equal height,
    /// each with the kernel set's largest half support
    /// ([`casa_imaging_operator::ConvolutionFunctionSet::max_half_support`])
    /// as a halo on both sides, so every routed sample's support lies in its
    /// tile.
    #[must_use]
    pub fn regions(operator: &MeasurementOperator, owners: usize) -> Self {
        let [_, ny] = operator.geometry().grid_shape();
        let owners = owners.clamp(1, ny);
        let bounds = (0..=owners)
            .map(|owner| owner * ny / owners)
            .collect::<Vec<_>>();
        Self::from_bounds(operator, &bounds)
    }

    /// `owners` horizontal strips of `operator`'s grid holding near-equal
    /// shares of `row_work`, the gridding work of each anchor row that an
    /// earlier pass measured ([`super::PassSummary::row_work`]), each with
    /// [`Self::regions`]' halo. A uv grid's samples gather towards its
    /// centre, so strips of equal height leave most of the work to the
    /// middle owners (#736). Each strip keeps at least one row. Work that
    /// does not cover the grid, or holds none, gives [`Self::regions`].
    #[must_use]
    pub fn regions_balanced(
        operator: &MeasurementOperator,
        owners: usize,
        row_work: &[u64],
    ) -> Self {
        let [_, ny] = operator.geometry().grid_shape();
        let total = row_work.iter().map(|&work| u128::from(work)).sum::<u128>();
        if row_work.len() != ny || total == 0 {
            return Self::regions(operator, owners);
        }
        let owners = owners.clamp(1, ny);
        // `before[r]`: the work of the rows above row `r`.
        let before = std::iter::once(0)
            .chain(row_work.iter().scan(0_u128, |sum, &work| {
                *sum += u128::from(work);
                Some(*sum)
            }))
            .collect::<Vec<_>>();
        let mut bounds = Vec::with_capacity(owners + 1);
        bounds.push(0);
        for owner in 1..owners {
            let share = total * owner as u128 / owners as u128;
            let row = before.partition_point(|&work| work < share);
            let previous = *bounds.last().expect("the first bound");
            bounds.push(row.clamp(previous + 1, ny - (owners - owner)));
        }
        bounds.push(ny);
        Self::from_bounds(operator, &bounds)
    }

    /// Strips between consecutive `bounds` (rows `bounds[k]..bounds[k+1]`
    /// for owner `k`), with the kernel set's largest half support as halo.
    fn from_bounds(operator: &MeasurementOperator, bounds: &[usize]) -> Self {
        let [nx, ny] = operator.geometry().grid_shape();
        let halo = usize::from(operator.cf().max_half_support()[1]);
        Self::Regions {
            regions: bounds
                .windows(2)
                .map(|pair| {
                    let rows = pair[0]..pair[1];
                    let first = rows.start.saturating_sub(halo);
                    let last = (rows.end + halo).min(ny);
                    Region {
                        tile: Tile {
                            origin: [0, first],
                            shape: [nx, last - first],
                        },
                        rows,
                    }
                })
                .collect(),
            halo,
        }
    }

    /// Number of owners.
    #[must_use]
    pub fn owners(&self) -> usize {
        match self {
            Self::Planes { owners } => *owners,
            Self::Regions { regions, .. } => regions.len(),
        }
    }
}

/// How many planes a pass holds at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Residency {
    /// Every plane in one traversal.
    All,
    /// Consecutive waves of at most `planes_per_wave` planes, one source
    /// traversal each.
    Waves {
        /// Planes per wave; positive.
        planes_per_wave: u32,
    },
}

/// What one wave of a pass holds in memory, for [`Residency::plan`].
#[derive(Clone, Copy)]
pub struct WaveDemand<'a> {
    /// The pass's image domains; they share one plane axis.
    pub domains: &'a [PassDomain<'a>],
    /// Modes accumulated.
    pub modes: ModeSet,
    /// Whether the pass subtracts a model.
    pub with_model: bool,
    /// The widest spacing between adjacent selected native channels, which
    /// sizes a wave's model halo
    /// ([`casa_imaging_operator::SpectralResampler::model_halo`]).
    pub native_spacing_hz: f64,
    /// Workers transforming planes at once.
    pub workers: usize,
    /// Where the owners grid.
    pub backend: BackendChoice,
    /// The layout of the source's rows ([`super::BoundedSource::maximum_block`]).
    pub block: BlockShape,
    /// Placements a row chunk takes at once ([`super::CHUNK_PLACEMENTS`],
    /// or [`Self::plan_chunk_placements`]).
    pub chunk_placements: usize,
}

impl WaveDemand<'_> {
    /// Planes on each image domain's axis.
    #[must_use]
    pub fn planes(&self) -> u32 {
        self.domains
            .first()
            .map_or(0, |domain| domain.operator.basis().planes())
    }

    /// Bytes a wave of `planes` planes holds. Per image domain and plane:
    /// its accumulator holding `modes`; the operator's `f32` images; and, for
    /// a basis whose normal state is resident, that state's copy at its
    /// stored size ([`casa_imaging_deconvolution::resident_bytes_per_cell`]):
    /// a channel-local state is paged, under its cube cache's charge. Per
    /// region-partitioned domain, its gridding work by anchor row. With a
    /// model, the prepared grids of the wave's planes and of its model halo on
    /// each side. Per worker: one
    /// transform plane and one image per grid polarization of the largest
    /// domain. On Metal the accumulators are device memory and cost nothing
    /// more; each owner adds its ring ([`casa_imaging_metal::ring_bytes`]),
    /// and a pass with a model adds the device copy of the model grids,
    /// which the device degrids. Per pass, whatever the wave: the row chunks
    /// it places [`Self::block`]'s rows with, and planning one grid
    /// transform ([`grid_planning_bytes`]), which the process does one at a
    /// time. What grows with the source's block is the source's
    /// ([`super::source_block_bytes`]).
    #[must_use]
    pub fn bytes(&self, planes: u32) -> u64 {
        let total = self.planes();
        let one = PlaneRange::single(0);
        let mut bytes = 0_u64;
        let mut per_worker = 0_u64;
        let mut planning = 0_u64;
        for domain in self.domains {
            let operator = domain.operator;
            let precision = operator.precision();
            let mut per_plane = operator
                .accumulator_layout(one, None, self.modes)
                .bytes(precision) as u64;
            let [width, height] = operator.geometry().image().shape;
            let image_cells = (width * height) as u64;
            let basis = operator.basis();
            let pols = operator.polarization().requested().len() as u64;
            // The weight image has the PSF's terms (`AccumulatorLayout`).
            let images = (basis.data_terms()
                + if self.modes.psf { basis.psf_terms() } else { 0 }
                + if self.modes.weight {
                    basis.psf_terms()
                } else {
                    0
                }) as u64;
            per_plane += images * pols * image_cells * size_of::<f32>() as u64;
            if !matches!(basis, Basis::ChannelLocal { .. }) {
                per_plane += pols
                    * image_cells
                    * resident_bytes_per_cell(
                        basis.data_terms(),
                        basis.psf_terms(),
                        self.modes.psf,
                    );
            }
            bytes += per_plane * u64::from(planes);
            // A region-partitioned domain's gridding work by anchor row,
            // which the pass hands to the next one's partition.
            if matches!(domain.partition, Partition::Regions { .. }) {
                bytes += (operator.geometry().grid_shape()[1] * size_of::<u64>()) as u64;
            }
            if self.with_model {
                let model_planes = planes
                    .saturating_add(2 * domain.resampler.model_halo(self.native_spacing_hz))
                    .min(total);
                // Metal degrids the model on the device, fused into the
                // residual or in the chunks' prediction batches.
                let copies = if self.backend == BackendChoice::Metal {
                    2
                } else {
                    1
                };
                bytes += copies
                    * operator
                        .accumulator_layout(one, None, ModeSet::DATA)
                        .bytes(precision) as u64
                    * u64::from(model_planes);
            }
            if self.backend == BackendChoice::Metal {
                let npol = operator.polarization().correlations().len();
                bytes += domain.partition.owners() as u64 * casa_imaging_metal::ring_bytes(npol);
            }
            let grid_cells = operator.geometry().cells() as u64;
            let gpols = operator.polarization().grid_pols() as u64;
            per_worker = per_worker.max(grid_cells * 16 + gpols * image_cells * 16);
            planning = planning.max(grid_planning_bytes(operator.geometry().cells()));
        }
        bytes
            + planning
            + self.workers as u64 * per_worker
            + super::wave::chunk_bytes(
                self.domains,
                self.backend,
                self.block,
                self.workers,
                self.modes.weight,
                self.with_model,
                self.chunk_placements,
            )
    }

    /// Placements a row chunk of a wave of `planes` planes takes: when a
    /// domain is split among several region owners, the largest of
    /// [`super::REGION_CHUNK_PLACEMENTS`] whose [`Self::bytes`] fit `budget`,
    /// since the owners share larger slices more evenly (#736); otherwise,
    /// or when none fits, [`super::CHUNK_PLACEMENTS`]. A pass that fits with
    /// the default chunks therefore never fails for want of larger ones.
    #[must_use]
    pub fn plan_chunk_placements(&self, planes: u32, budget: u64) -> usize {
        let split = self.domains.iter().any(|domain| {
            matches!(domain.partition, Partition::Regions { .. }) && domain.partition.owners() > 1
        });
        if !split {
            return super::CHUNK_PLACEMENTS;
        }
        super::REGION_CHUNK_PLACEMENTS
            .into_iter()
            .find(|&chunk_placements| {
                WaveDemand {
                    chunk_placements,
                    ..*self
                }
                .bytes(planes)
                    <= budget
            })
            .unwrap_or(super::CHUNK_PLACEMENTS)
    }
}

impl Residency {
    /// The fewest waves whose [`WaveDemand::bytes`] fit `budget`: every
    /// plane when they fit, otherwise the largest wave that does and at
    /// least one plane. Whether a wave fits at all is the admission of its
    /// bytes ([`crate::admit`]).
    #[must_use]
    pub fn plan(demand: &WaveDemand<'_>, budget: u64) -> Self {
        let planes = demand.planes();
        if planes <= 1 || demand.bytes(planes) <= budget {
            return Self::All;
        }
        if demand.bytes(1) > budget {
            return Self::Waves { planes_per_wave: 1 };
        }
        // `bytes` grows with the wave: the largest fitting wave lies in
        // `[fits, too_many)`.
        let (mut fits, mut too_many) = (1, planes);
        while too_many - fits > 1 {
            let middle = fits + (too_many - fits) / 2;
            if demand.bytes(middle) <= budget {
                fits = middle;
            } else {
                too_many = middle;
            }
        }
        Self::Waves {
            planes_per_wave: fits,
        }
    }

    /// The plane ranges of the waves over `planes` planes.
    #[must_use]
    pub fn waves(self, planes: u32) -> Vec<PlaneRange> {
        let step = match self {
            Self::All => planes.max(1),
            Self::Waves { planes_per_wave } => {
                assert!(planes_per_wave > 0, "a wave holds at least one plane");
                planes_per_wave
            }
        };
        (0..planes)
            .step_by(step as usize)
            .map(|start| PlaneRange::new(start, (start + step).min(planes)))
            .collect()
    }
}

/// The owner of each placement within one wave.
pub(super) enum Router {
    /// `starts[k]` is owner `k`'s first plane; owner ranges are contiguous.
    Planes { starts: Vec<u32> },
    /// `owner_of_row[y]` owns anchor row `y`; tiles extend `halo` rows.
    Regions { owner_of_row: Vec<u16>, halo: usize },
}

impl Router {
    pub(super) fn new(partition: &Partition, wave: PlaneRange) -> Self {
        match partition {
            Partition::Planes { owners } => {
                let planes = wave.len();
                let owners = (*owners).clamp(1, planes.max(1));
                Self::Planes {
                    starts: (0..owners)
                        .map(|owner| wave.start + (owner * planes / owners) as u32)
                        .collect(),
                }
            }
            Partition::Regions { regions, halo } => {
                let rows = regions.last().map_or(0, |region| region.rows.end);
                let mut owner_of_row = vec![0; rows];
                for (owner, region) in regions.iter().enumerate() {
                    owner_of_row[region.rows.clone()].fill(owner as u16);
                }
                Self::Regions {
                    owner_of_row,
                    halo: *halo,
                }
            }
        }
    }

    /// Number of owners in this wave.
    pub(super) fn owners(&self) -> usize {
        match self {
            Self::Planes { starts } => starts.len(),
            Self::Regions { owner_of_row, .. } => {
                owner_of_row.last().map_or(1, |owner| *owner as usize + 1)
            }
        }
    }

    /// The one owner that grids the weight image when the grid is split
    /// into regions: the owner of the centre row, where `Mode::Weight`
    /// places every sample's `FT[PB²]` taps. `None` when every owner holds
    /// whole planes and grids its own samples' weights.
    pub(super) fn weight_owner(&self) -> Option<usize> {
        match self {
            Self::Planes { .. } => None,
            Self::Regions { owner_of_row, .. } => {
                Some(usize::from(owner_of_row[owner_of_row.len() / 2]))
            }
        }
    }

    /// Plane range and tile owner `owner` accumulates in `wave`.
    pub(super) fn target(
        &self,
        partition: &Partition,
        wave: PlaneRange,
        owner: usize,
    ) -> (PlaneRange, Option<Tile>) {
        match (self, partition) {
            (Self::Planes { starts }, _) => {
                let end = starts.get(owner + 1).copied().unwrap_or(wave.end);
                (PlaneRange::new(starts[owner], end), None)
            }
            (Self::Regions { .. }, Partition::Regions { regions, .. }) => {
                (wave, Some(regions[owner].tile))
            }
            (Self::Regions { .. }, Partition::Planes { .. }) => {
                unreachable!("router built from this partition")
            }
        }
    }

    /// Owner of one placement and, with region owners, its anchor row and
    /// gridding work, the kernel's taps `(2·h_x + 1)(2·h_y + 1)`, which
    /// [`Partition::regions_balanced`] balances; the placement lies on the
    /// wave and fits the grid.
    pub(super) fn owner_and_work(
        &self,
        operator: &MeasurementOperator,
        placement: &Placement,
    ) -> (usize, Option<(usize, u64)>) {
        match self {
            Self::Planes { starts } => (
                starts.partition_point(|start| *start <= placement.plane) - 1,
                None,
            ),
            Self::Regions { owner_of_row, halo } => {
                let mut hold = CellHold::new();
                let taps = operator.cf().taps(placement.cf, &mut hold);
                let [hx, hy] = taps.half_support();
                debug_assert!(
                    usize::from(hy) <= *halo,
                    "a kernel cell's support exceeds the regions' halo"
                );
                let anchor =
                    operator
                        .geometry()
                        .locate(placement.u, placement.v, taps.oversampling());
                let row = anchor.y as usize;
                let work = (2 * u64::from(hx) + 1) * (2 * u64::from(hy) + 1);
                (usize::from(owner_of_row[row]), Some((row, work)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
    use casa_imaging_operator::{
        Basis, GridGeometry, GridPadding, GridPrecision, ImageExtent, PolarizationRouting,
        Spheroidal,
    };

    fn spheroidal_operator() -> MeasurementOperator {
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [64, 64],
                increment_rad: [-1.0e-5, 1.0e-5],
                reference_pixel: [32, 32],
            },
            GridPadding::CasaComposite,
        )
        .expect("geometry");
        let polarization = PolarizationRouting::compile(
            &[CorrelationType::LinearXx, CorrelationType::LinearYy],
            &[PolarizationCoordinate::StokesI],
        )
        .expect("routing");
        let cf = Spheroidal::new(&geometry, &polarization);
        MeasurementOperator::new(
            geometry,
            Basis::Constant,
            polarization,
            Box::new(cf),
            GridPrecision::F64,
        )
    }

    fn rows_of(partition: &Partition) -> Vec<Range<usize>> {
        let Partition::Regions { regions, .. } = partition else {
            panic!("regions");
        };
        regions.iter().map(|region| region.rows.clone()).collect()
    }

    /// Work gathered at the grid's centre, as uv samples are: equal-height
    /// strips give the middle owner nearly all of it, balanced strips give
    /// each owner its share to within one row's work. The strips still
    /// cover the grid once, each with at least one row and the kernel halo.
    #[test]
    fn balanced_regions_share_centre_heavy_work() {
        let operator = spheroidal_operator();
        let ny = operator.geometry().grid_shape()[1];
        let centre = ny as f64 / 2.0;
        let row_work = (0..ny)
            .map(|row| {
                let offset = (row as f64 - centre) / (ny as f64 / 16.0);
                (1.0e6 * (-offset * offset).exp()) as u64
            })
            .collect::<Vec<_>>();
        let total = row_work.iter().sum::<u64>();
        let largest = *row_work.iter().max().expect("rows");
        let share = |rows: &Range<usize>| row_work[rows.clone()].iter().sum::<u64>();
        let equal = rows_of(&Partition::regions(&operator, 3));
        assert!(
            share(&equal[1]) > total * 9 / 10,
            "the middle strip holds the work"
        );
        let balanced = Partition::regions_balanced(&operator, 3, &row_work);
        let rows = rows_of(&balanced);
        assert_eq!(rows.first().expect("a strip").start, 0);
        assert_eq!(rows.last().expect("a strip").end, ny);
        for pair in rows.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
        for strip in &rows {
            assert!(!strip.is_empty());
            assert!(
                share(strip).abs_diff(total / 3) <= largest,
                "strip {strip:?} holds {} of {total}",
                share(strip)
            );
        }
        let Partition::Regions { regions, halo } = balanced else {
            panic!("regions");
        };
        assert_eq!(halo, 3);
        for region in &regions {
            assert_eq!(
                region.tile.origin[1],
                region.rows.start.saturating_sub(halo)
            );
            assert_eq!(
                region.tile.origin[1] + region.tile.shape[1],
                (region.rows.end + halo).min(ny)
            );
        }
    }

    /// Without usable work, or with work on one row, the strips stay valid:
    /// equal strips for no work or the wrong length, and one row at least
    /// per strip when all the work is on one row.
    #[test]
    fn balanced_regions_fall_back_and_keep_every_strip() {
        let operator = spheroidal_operator();
        let ny = operator.geometry().grid_shape()[1];
        let equal = rows_of(&Partition::regions(&operator, 4));
        assert_eq!(
            rows_of(&Partition::regions_balanced(&operator, 4, &[])),
            equal
        );
        assert_eq!(
            rows_of(&Partition::regions_balanced(&operator, 4, &vec![0; ny])),
            equal
        );
        let mut one_row = vec![0; ny];
        one_row[ny / 2] = 100;
        let rows = rows_of(&Partition::regions_balanced(&operator, 4, &one_row));
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|strip| !strip.is_empty()));
        assert_eq!(rows.last().expect("a strip").end, ny);
        let mut last_row = vec![0; ny];
        last_row[ny - 1] = 100;
        let rows = rows_of(&Partition::regions_balanced(&operator, 4, &last_row));
        assert!(rows.iter().all(|strip| !strip.is_empty()));
    }

    #[test]
    fn regions_cover_the_grid_once_with_the_kernel_halo() {
        let operator = spheroidal_operator();
        let Partition::Regions { regions, halo } = Partition::regions(&operator, 3) else {
            panic!("regions");
        };
        assert_eq!(halo, 3, "the spheroidal kernel's half support");
        let ny = operator.geometry().grid_shape()[1];
        assert_eq!(regions.first().unwrap().rows.start, 0);
        assert_eq!(regions.last().unwrap().rows.end, ny);
        for pair in regions.windows(2) {
            assert_eq!(pair[0].rows.end, pair[1].rows.start);
        }
        for region in &regions {
            assert!(region.tile.origin[1] + halo <= region.rows.start.max(halo));
            assert!(region.tile.origin[1] + region.tile.shape[1] <= ny);
            assert!(region.tile.origin[1] + region.tile.shape[1] >= region.rows.end.min(ny - halo));
        }
    }

    #[test]
    fn waves_cover_the_planes_in_order() {
        assert_eq!(
            Residency::Waves { planes_per_wave: 3 }.waves(7),
            vec![
                PlaneRange::new(0, 3),
                PlaneRange::new(3, 6),
                PlaneRange::new(6, 7)
            ]
        );
        assert_eq!(Residency::All.waves(5), vec![PlaneRange::new(0, 5)]);
    }
}
