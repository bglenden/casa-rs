// SPDX-License-Identifier: LGPL-3.0-or-later
//! How a pass splits accumulation across workers and planes across waves.

use std::ops::Range;

use casa_imaging_deconvolution::resident_bytes_per_cell;
use casa_imaging_operator::{
    Basis, MeasurementOperator, ModeSet, Placement, PlaneRange, Tile, grid_planning_bytes,
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

    /// `owners` strips of `operator`'s grid whose tiles hold the most rows
    /// of any cut into that many strips, each with [`Self::regions`]' halo.
    /// A cut's tiles hold the grid's rows and, at each boundary between
    /// strips, the halo on both sides as far as the grid's edges allow; so
    /// the boundaries here are the ones nearest the grid's centre. A charge
    /// made at these strips bounds a pass on the strips
    /// [`Self::regions_balanced`] later cuts for as many owners, from work
    /// not yet measured (#737).
    #[must_use]
    pub fn regions_widest(operator: &MeasurementOperator, owners: usize) -> Self {
        let [_, ny] = operator.geometry().grid_shape();
        let inner = owners.clamp(1, ny) - 1;
        let first = (ny - inner).div_ceil(2);
        let bounds = std::iter::once(0)
            .chain(first..first + inner)
            .chain(std::iter::once(ny))
            .collect::<Vec<_>>();
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

    /// Whether the grid's rows are split among several region owners: the
    /// one case that measures its gridding work by anchor row
    /// ([`super::PassSummary::row_work`], [`super::run_row_work_pass`]) and
    /// takes larger chunks ([`WaveDemand::plan_chunk_placements`]). A lone
    /// region owner and plane owners do neither (#736).
    #[must_use]
    pub fn splits_rows(&self) -> bool {
        matches!(self, Self::Regions { regions, .. } if regions.len() > 1)
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

    /// Bytes a wave of `planes` planes holds: the largest of its stages
    /// (#738), beside what it holds throughout.
    ///
    /// Throughout the wave, each domain's prepared model grids, with a
    /// model, for the wave's planes and its model halo on each side; on
    /// Metal, their device copy, which the device degrids, and each owner's
    /// ring ([`casa_imaging_metal::ring_bytes`]); and a region-partitioned
    /// domain's gridding work by anchor row (#736). The stages, summed over
    /// image domains, since a domain finishes and appends while the others
    /// hold their tiles or images:
    /// - gridding: the owners' tiles of the wave's planes, beside the row
    ///   chunks the wave places [`Self::block`]'s rows with, of
    ///   [`Self::chunk_placements`] placements each;
    /// - finishing, beside the chunks: a region pass's tiles and the merged
    ///   accumulator they are added into, which one owner's whole-grid tile
    ///   needs none of; then the accumulator beside its `f32` images; and
    ///   for the transforms, one transform plane and one image per grid
    ///   polarization for each transform that runs at once, and planning
    ///   one grid transform ([`grid_planning_bytes`]), which the process
    ///   does one at a time. Domains finish one after another; a plane
    ///   pass's owners finish together on up to the workers, a region
    ///   pass's merged accumulator alone (#745 review);
    /// - appending: the images, the copy the normal state takes of them and,
    ///   for a basis whose normal state is resident, that state at its
    ///   stored size ([`casa_imaging_deconvolution::resident_bytes_per_cell`]);
    ///   a channel-local state is paged, under its cube cache's charge.
    ///
    /// On Metal the accumulators are device memory and cost what a host
    /// one does. What grows with the source's block is the source's
    /// ([`super::source_block_bytes`]).
    #[must_use]
    pub fn bytes(&self, planes: u32) -> u64 {
        let mut stages = DomainStages::default();
        let mut transforming = 0_u64;
        let mut planning = 0_u64;
        for domain in self.domains {
            stages.add(self.domain_stages(domain, planes));
            let operator = domain.operator;
            let [width, height] = operator.geometry().image().shape;
            let image_cells = (width * height) as u64;
            let grid_cells = operator.geometry().cells() as u64;
            let gpols = operator.polarization().grid_pols() as u64;
            let at_once = match &domain.partition {
                Partition::Planes { owners } => (*owners).min(self.workers),
                Partition::Regions { .. } => 1,
            } as u64;
            transforming = transforming.max(at_once * (grid_cells * 16 + gpols * image_cells * 16));
            planning = planning.max(grid_planning_bytes(operator.geometry().cells()));
        }
        let chunks = super::wave::chunk_bytes(
            self.domains,
            self.backend,
            self.block,
            self.workers,
            self.modes.weight,
            self.with_model,
            self.chunk_placements,
        );
        let transforms = planning + transforming;
        stages.held
            + (stages.gridding + chunks)
                .max(stages.finishing + chunks + transforms)
                .max(stages.appending)
    }

    /// Placements a row chunk of a wave of `planes` planes takes: when a
    /// domain is split among several region owners, the largest of
    /// [`super::REGION_CHUNK_PLACEMENTS`] whose [`Self::bytes`] fit `budget`,
    /// since the owners share larger slices more evenly (#736); otherwise,
    /// or when none fits, [`super::CHUNK_PLACEMENTS`]. A pass that fits with
    /// the default chunks therefore never fails for want of larger ones.
    #[must_use]
    pub fn plan_chunk_placements(&self, planes: u32, budget: u64) -> usize {
        if !self
            .domains
            .iter()
            .any(|domain| domain.partition.splits_rows())
        {
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

    /// What `domain` holds at each stage of a wave of `planes` planes
    /// ([`Self::bytes`]).
    fn domain_stages(&self, domain: &PassDomain<'_>, planes: u32) -> DomainStages {
        let operator = domain.operator;
        let precision = operator.precision();
        let one = PlaneRange::single(0);
        let planes_u64 = u64::from(planes);
        let layout = |tile: Option<Tile>, modes: ModeSet| {
            operator
                .accumulator_layout(one, tile, modes)
                .bytes(precision) as u64
        };
        let accumulator = layout(None, self.modes) * planes_u64;
        let (tiles, merging) = match &domain.partition {
            Partition::Planes { .. } => (accumulator, accumulator),
            Partition::Regions { regions, .. } => {
                let tiles = regions
                    .iter()
                    .map(|region| layout(Some(region.tile), self.modes))
                    .sum::<u64>()
                    * planes_u64;
                // One owner's tile is the whole grid, which the wave
                // finishes as it is: both key on the owner count.
                let merged = if regions.len() > 1 { accumulator } else { 0 };
                (tiles, tiles + merged)
            }
        };
        let [width, height] = operator.geometry().image().shape;
        let image_cells = (width * height) as u64;
        let basis = operator.basis();
        let pols = operator.polarization().requested().len() as u64;
        // The weight image has the PSF's terms (`AccumulatorLayout`).
        let terms = (basis.data_terms()
            + if self.modes.psf { basis.psf_terms() } else { 0 }
            + if self.modes.weight {
                basis.psf_terms()
            } else {
                0
            }) as u64;
        let images = terms * pols * image_cells * size_of::<f32>() as u64 * planes_u64;
        let normal = normal_bytes(operator, self.modes.psf) * planes_u64;
        let mut held = 0;
        if self.with_model {
            let model_planes = planes
                .saturating_add(2 * domain.resampler.model_halo(self.native_spacing_hz))
                .min(self.planes());
            // Metal degrids the model on the device, fused into the
            // residual or in the chunks' prediction batches.
            let copies = if self.backend == BackendChoice::Metal {
                2
            } else {
                1
            };
            held += copies * layout(None, ModeSet::DATA) * u64::from(model_planes);
        }
        if self.backend == BackendChoice::Metal {
            let npol = operator.polarization().correlations().len();
            held += domain.partition.owners() as u64 * casa_imaging_metal::ring_bytes(npol);
        }
        // A domain split among region owners measures its gridding work by
        // anchor row, which the pass hands to the next one's partition
        // (#736).
        if domain.partition.splits_rows() {
            held += (operator.geometry().grid_shape()[1] * size_of::<u64>()) as u64;
        }
        DomainStages {
            held,
            gridding: tiles,
            finishing: merging.max(accumulator + images),
            appending: 2 * images + normal,
        }
    }

    /// Bytes the normal state a run keeps resident between its passes
    /// holds, over every image domain: one plane's state with the PSF and
    /// sensitivity of the first pass, which a refresh keeps ([`normal_bytes`]).
    #[must_use]
    pub fn resident_normal_bytes(&self) -> u64 {
        self.domains
            .iter()
            .map(|domain| normal_bytes(domain.operator, true))
            .sum()
    }
}

/// Bytes one plane of `operator`'s normal state holds resident, with the
/// PSF's moments and sensitivity when `psf`, at its stored size
/// ([`resident_bytes_per_cell`]); none for a channel-local state, which is
/// paged under its cube cache's charge.
fn normal_bytes(operator: &MeasurementOperator, psf: bool) -> u64 {
    let basis = operator.basis();
    if matches!(basis, Basis::ChannelLocal { .. }) {
        return 0;
    }
    let [width, height] = operator.geometry().image().shape;
    operator.polarization().requested().len() as u64
        * (width * height) as u64
        * resident_bytes_per_cell(basis.data_terms(), basis.psf_terms(), psf)
}

/// What a wave holds at each of its stages ([`WaveDemand::bytes`]), summed
/// over image domains: a domain finishes and appends while the others
/// hold their tiles or images, so the sums bound every moment.
#[derive(Clone, Copy, Default)]
struct DomainStages {
    /// Held through every stage.
    held: u64,
    /// The owners' tiles.
    gridding: u64,
    /// The merge, then the accumulator beside its images.
    finishing: u64,
    /// The images, their copy and the normal state.
    appending: u64,
}

impl DomainStages {
    fn add(&mut self, other: Self) {
        self.held += other.held;
        self.gridding += other.gridding;
        self.finishing += other.finishing;
        self.appending += other.appending;
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
    /// wave and fits the grid. The kernel's support and oversampling come
    /// from the set's metadata, so routing lends no taps and takes no lock
    /// (#744).
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
                let cf = operator.cf();
                let [hx, hy] = cf.gridding_half_support(placement.cf);
                debug_assert!(
                    usize::from(hy) <= *halo,
                    "a kernel cell's support exceeds the regions' halo"
                );
                let anchor = operator.geometry().locate(
                    placement.u,
                    placement.v,
                    cf.gridding_oversampling(placement.cf),
                );
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

    /// #737: the widest strips hold the most tile rows of any cut into as
    /// many strips, so a charge at them bounds the balanced strips a pass
    /// cuts later. Strips shorter than the halo lose rows at the grid's
    /// edges: there equal strips hold fewer than a balanced cut of
    /// centre-heavy work.
    #[test]
    fn the_widest_regions_hold_the_most_rows_of_any_cut() {
        let operator = spheroidal_operator();
        let ny = operator.geometry().grid_shape()[1];
        let rows = |partition: &Partition| {
            let Partition::Regions { regions, .. } = partition else {
                panic!("regions");
            };
            regions
                .iter()
                .map(|region| region.tile.shape[1])
                .sum::<usize>()
        };
        let most = (1..ny)
            .flat_map(|a| (a + 1..ny).map(move |b| [0, a, b, ny]))
            .map(|bounds| rows(&Partition::from_bounds(&operator, &bounds)))
            .max();
        assert_eq!(Some(rows(&Partition::regions_widest(&operator, 3))), most);

        let owners = ny / 2;
        let widest = rows(&Partition::regions_widest(&operator, owners));
        let centre = ny as f64 / 2.0;
        let centre_heavy = (0..ny)
            .map(|row| {
                let offset = (row as f64 - centre) / (ny as f64 / 16.0);
                (1.0e6 * (-offset * offset).exp()) as u64
            })
            .collect::<Vec<_>>();
        let balanced = rows(&Partition::regions_balanced(
            &operator,
            owners,
            &centre_heavy,
        ));
        assert!(rows(&Partition::regions(&operator, owners)) < balanced);
        assert!(balanced <= widest);
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for _ in 0..64 {
            let work = (0..ny)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    state >> 40
                })
                .collect::<Vec<_>>();
            for owners in [1, 2, 3, 7, owners, ny] {
                let balanced = rows(&Partition::regions_balanced(&operator, owners, &work));
                assert!(balanced <= rows(&Partition::regions_widest(&operator, owners)));
            }
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
