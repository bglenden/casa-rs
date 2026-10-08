// SPDX-License-Identifier: LGPL-3.0-or-later
//! How a pass splits accumulation across workers and planes across waves.

use std::ops::Range;

use casa_imaging_operator::{CellHold, MeasurementOperator, ModeSet, Placement, PlaneRange, Tile};

use super::{PassDomain, PassError};

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
        let [nx, ny] = operator.geometry().grid_shape();
        let halo = usize::from(operator.cf().max_half_support()[1]);
        let owners = owners.clamp(1, ny);
        Self::Regions {
            regions: (0..owners)
                .map(|owner| {
                    let rows = owner * ny / owners..(owner + 1) * ny / owners;
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
    /// its accumulator holding `modes` and its images twice (the operator's
    /// and the normal state's copy); with a model, the prepared grids of the
    /// wave's planes and of its model halo on each side. Per worker: one
    /// transform plane and one image per grid polarization of the largest
    /// domain.
    #[must_use]
    pub fn bytes(&self, planes: u32) -> u64 {
        let total = self.planes();
        let one = PlaneRange::single(0);
        let mut bytes = 0_u64;
        let mut per_worker = 0_u64;
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
            let images =
                (basis.data_terms() + if self.modes.psf { basis.psf_terms() } else { 0 }) as u64;
            per_plane += 2 * images * pols * image_cells * 4;
            bytes += per_plane * u64::from(planes);
            if self.with_model {
                let model_planes = planes
                    .saturating_add(2 * domain.resampler.model_halo(self.native_spacing_hz))
                    .min(total);
                bytes += operator
                    .accumulator_layout(one, None, ModeSet::DATA)
                    .bytes(precision) as u64
                    * u64::from(model_planes);
            }
            let grid_cells = operator.geometry().cells() as u64;
            let gpols = operator.polarization().grid_pols() as u64;
            per_worker = per_worker.max(grid_cells * 16 + gpols * image_cells * 16);
        }
        bytes + self.workers as u64 * per_worker
    }
}

impl Residency {
    /// The fewest waves whose [`WaveDemand::bytes`] fit `budget`: every
    /// plane when they fit, otherwise the largest wave that does.
    pub fn plan(demand: &WaveDemand<'_>, budget: u64) -> Result<Self, PassError> {
        let planes = demand.planes();
        let one = demand.bytes(1);
        if one > budget {
            return Err(PassError::Memory {
                required: one,
                available: budget,
            });
        }
        if demand.bytes(planes) <= budget {
            return Ok(Self::All);
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
        Ok(Self::Waves {
            planes_per_wave: fits,
        })
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

    /// Owner of one placement; the placement lies on the wave and fits the grid.
    pub(super) fn owner(&self, operator: &MeasurementOperator, placement: &Placement) -> usize {
        match self {
            Self::Planes { starts } => {
                starts.partition_point(|start| *start <= placement.plane) - 1
            }
            Self::Regions { owner_of_row, halo } => {
                let mut hold = CellHold::new();
                let taps = operator.cf().taps(placement.cf, &mut hold);
                debug_assert!(
                    usize::from(taps.half_support()[1]) <= *halo,
                    "a kernel cell's support exceeds the regions' halo"
                );
                let anchor =
                    operator
                        .geometry()
                        .locate(placement.u, placement.v, taps.oversampling());
                usize::from(owner_of_row[anchor.y as usize])
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

    #[test]
    fn regions_cover_the_grid_once_with_the_kernel_halo() {
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
        let operator = MeasurementOperator::new(
            geometry,
            Basis::Constant,
            polarization,
            Box::new(cf),
            GridPrecision::F64,
        );
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
