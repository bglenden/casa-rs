// SPDX-License-Identifier: LGPL-3.0-or-later
//! How a pass splits accumulation across workers and planes across waves.

use std::ops::Range;

use casa_imaging_operator::{
    GridGeometry, MeasurementOperator, ModeSet, Placement, PlaneRange, Tile,
};

/// How one pass divides its grid accumulation among owners (one per worker).
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
    /// region, on a tile of the region plus the kernel halo; tiles are added
    /// in region order before the transform.
    Regions(Vec<Region>),
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
    /// `owners` horizontal strips of near-equal height, each with `halo`
    /// rows on both sides; `halo` is the largest kernel half support of the
    /// kernel set, so every routed sample's support lies in its tile.
    #[must_use]
    pub fn regions(geometry: &GridGeometry, owners: usize, halo: usize) -> Self {
        let [nx, ny] = geometry.grid_shape();
        let owners = owners.clamp(1, ny);
        Self::Regions(
            (0..owners)
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
        )
    }

    /// Number of owners.
    #[must_use]
    pub fn owners(&self) -> usize {
        match self {
            Self::Planes { owners } => *owners,
            Self::Regions(regions) => regions.len(),
        }
    }
}

/// How many planes a pass holds at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Residency {
    /// Every plane in one traversal.
    All,
    /// Consecutive waves of at most `planes_per_wave` planes, one source
    /// traversal each, restricted to the native channels feeding the wave.
    Waves {
        /// Planes per wave; positive.
        planes_per_wave: u32,
    },
}

impl Residency {
    /// The fewest waves whose planes fit `budget` bytes with `workers`
    /// workers: per plane, its accumulator holding `modes`, its prepared
    /// model grids when `with_model`, and its images twice (the operator's and
    /// the normal state's copy); per worker, one transform plane and one image
    /// per grid polarization.
    pub fn plan(
        operator: &MeasurementOperator,
        modes: ModeSet,
        with_model: bool,
        workers: usize,
        budget: u64,
    ) -> Result<Self, super::PassError> {
        let precision = operator.precision();
        let one = PlaneRange::single(0);
        let mut per_plane = operator
            .accumulator_layout(one, None, modes)
            .bytes(precision) as u64;
        if with_model {
            per_plane += operator
                .accumulator_layout(one, None, ModeSet::DATA)
                .bytes(precision) as u64;
        }
        let [width, height] = operator.geometry().image().shape;
        let image_cells = (width * height) as u64;
        let basis = operator.basis();
        let pols = operator.polarization().requested().len() as u64;
        let images = (basis.data_terms() + if modes.psf { basis.psf_terms() } else { 0 }) as u64;
        per_plane += 2 * images * pols * image_cells * 4;
        let grid_cells = operator.geometry().cells() as u64;
        let gpols = operator.polarization().grid_pols() as u64;
        let fixed = workers as u64 * (grid_cells * 16 + gpols * image_cells * 16);
        let planes = basis.planes();
        let available = budget.saturating_sub(fixed);
        if per_plane > available {
            return Err(super::PassError::Memory {
                required: per_plane + fixed,
                available: budget,
            });
        }
        let fit = (available / per_plane).min(u64::from(planes)) as u32;
        Ok(if fit >= planes {
            Self::All
        } else {
            Self::Waves {
                planes_per_wave: fit,
            }
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
    /// `owner_of_row[y]` owns anchor row `y`.
    Regions { owner_of_row: Vec<u16> },
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
            Partition::Regions(regions) => {
                let rows = regions.last().map_or(0, |region| region.rows.end);
                let mut owner_of_row = vec![0; rows];
                for (owner, region) in regions.iter().enumerate() {
                    owner_of_row[region.rows.clone()].fill(owner as u16);
                }
                Self::Regions { owner_of_row }
            }
        }
    }

    /// Number of owners in this wave.
    pub(super) fn owners(&self) -> usize {
        match self {
            Self::Planes { starts } => starts.len(),
            Self::Regions { owner_of_row } => {
                owner_of_row.last().map_or(1, |owner| *owner as usize + 1)
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
            (Self::Regions { .. }, Partition::Regions(regions)) => {
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
            Self::Regions { owner_of_row } => {
                let oversampling = operator.cf().taps(placement.cf).oversampling();
                let anchor = operator
                    .geometry()
                    .locate(placement.u, placement.v, oversampling);
                usize::from(owner_of_row[anchor.y as usize])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_operator::{GridPadding, ImageExtent};

    #[test]
    fn regions_cover_the_grid_once_with_clipped_halos() {
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [64, 64],
                increment_rad: [-1.0e-5, 1.0e-5],
                reference_pixel: [32, 32],
            },
            GridPadding::CasaComposite,
        )
        .expect("geometry");
        let Partition::Regions(regions) = Partition::regions(&geometry, 3, 3) else {
            panic!("regions");
        };
        let ny = geometry.grid_shape()[1];
        assert_eq!(regions.first().unwrap().rows.start, 0);
        assert_eq!(regions.last().unwrap().rows.end, ny);
        for pair in regions.windows(2) {
            assert_eq!(pair[0].rows.end, pair[1].rows.start);
        }
        for region in &regions {
            assert!(region.tile.origin[1] + 3 <= region.rows.start.max(3));
            assert!(region.tile.origin[1] + region.tile.shape[1] <= ny);
            assert!(region.tile.origin[1] + region.tile.shape[1] >= region.rows.end.min(ny - 3));
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
