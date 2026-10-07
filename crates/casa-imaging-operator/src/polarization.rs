// SPDX-License-Identifier: LGPL-3.0-or-later
//! Correlation-to-grid polarization routing and the image-domain Stokes
//! basis, pinned to CASA (`FTMachine::initMaps`, `StokesImageUtil`,
//! `CStokesVector`).

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use num_complex::Complex64;

use crate::error::OperatorError;

/// Feed basis of the selected correlations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedBasis {
    /// Values are already Stokes parameters.
    Stokes,
    /// Orthogonal X/Y receptors.
    Linear,
    /// Orthogonal R/L receptors.
    Circular,
}

/// One grid polarization plane in CASA's complex-image representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GridPolarization {
    /// One correlation (or one Stokes parameter when the data are Stokes).
    Correlation(CorrelationType),
    /// Stokes I gridded directly: both parallel hands feed one plane and
    /// their weights both count in its `sumwt` (CASA `isIOnly`).
    StokesI,
}

/// Correlation routing in both directions of the operator.
///
/// The adjoint grids each selected correlation into the grid plane
/// [`PolarizationRouting::pol_map`] names and converts the complex image
/// planes to the requested coordinates with
/// [`PolarizationRouting::to_requested`] (CASA `StokesImageUtil::To`,
/// real part taken). The forward transform expands requested model planes
/// to grid planes with [`PolarizationRouting::from_requested`]
/// (`StokesImageUtil::From`). `sumwt` of a requested Stokes plane is that of
/// grid plane 0 (`StokesImageUtil::ToStokesSumWt`).
#[derive(Clone, Debug, PartialEq)]
pub struct PolarizationRouting {
    correlations: Vec<CorrelationType>,
    requested: Vec<PolarizationCoordinate>,
    grid: Vec<GridPolarization>,
    pol_map: Vec<Option<u8>>,
    feed: FeedBasis,
    to_requested: Vec<Complex64>,
    from_requested: Vec<Complex64>,
    sumwt_source: Vec<usize>,
}

impl PolarizationRouting {
    /// Route the selected `correlations` (block order) to the `requested`
    /// image coordinates.
    ///
    /// Requested coordinates are all Stokes or all correlations, the
    /// selected correlations share one feed basis, every grid plane CASA's
    /// representation needs receives at least one selected correlation, and
    /// requested correlations must be selected.
    pub fn compile(
        correlations: &[CorrelationType],
        requested: &[PolarizationCoordinate],
    ) -> Result<Self, OperatorError> {
        let feed = feed_basis(correlations)?;
        if requested.is_empty() {
            return Err(OperatorError::Polarization {
                reason: "no polarization coordinate requested",
            });
        }
        if requested
            .iter()
            .enumerate()
            .any(|(index, coordinate)| requested[..index].contains(coordinate))
        {
            return Err(OperatorError::Polarization {
                reason: "requested polarization coordinates repeat",
            });
        }
        let stokes_requested = requested.iter().all(|coordinate| is_stokes(*coordinate));
        if !stokes_requested && requested.iter().any(|coordinate| is_stokes(*coordinate)) {
            return Err(OperatorError::Polarization {
                reason: "requested coordinates mix Stokes parameters and correlations",
            });
        }
        let grid = if !stokes_requested {
            requested
                .iter()
                .map(|coordinate| {
                    let correlation = correlation_of(*coordinate);
                    correlations
                        .contains(&correlation)
                        .then_some(GridPolarization::Correlation(correlation))
                        .ok_or(OperatorError::Polarization {
                            reason: "requested correlation is not selected",
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else if feed == FeedBasis::Stokes {
            requested
                .iter()
                .map(|coordinate| {
                    let correlation = correlation_of(*coordinate);
                    correlations
                        .contains(&correlation)
                        .then_some(GridPolarization::Correlation(correlation))
                        .ok_or(OperatorError::Polarization {
                            reason: "requested Stokes parameter is not selected",
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            casa_correlation_representation(feed, requested)
        };
        let pol_map = correlations
            .iter()
            .map(|correlation| {
                grid.iter()
                    .position(|plane| match plane {
                        GridPolarization::Correlation(target) => target == correlation,
                        GridPolarization::StokesI => is_parallel_hand(*correlation),
                    })
                    .map(|index| index as u8)
            })
            .collect::<Vec<_>>();
        if (0..grid.len()).any(|gpol| !pol_map.contains(&Some(gpol as u8))) {
            return Err(OperatorError::Polarization {
                reason: "selected correlations do not feed every grid polarization",
            });
        }
        let to_requested = requested
            .iter()
            .flat_map(|coordinate| {
                grid.iter()
                    .map(|plane| to_coefficient(feed, *coordinate, *plane))
            })
            .collect();
        let from_requested = grid
            .iter()
            .flat_map(|plane| {
                requested
                    .iter()
                    .map(|coordinate| from_coefficient(feed, *plane, *coordinate))
            })
            .collect();
        let sumwt_source = requested
            .iter()
            .map(|coordinate| {
                if stokes_requested {
                    0
                } else {
                    grid.iter()
                        .position(|plane| {
                            *plane == GridPolarization::Correlation(correlation_of(*coordinate))
                        })
                        .expect("requested correlations are grid planes")
                }
            })
            .collect();
        Ok(Self {
            correlations: correlations.to_vec(),
            requested: requested.to_vec(),
            grid,
            pol_map,
            feed,
            to_requested,
            from_requested,
            sumwt_source,
        })
    }

    /// Selected correlations in block order.
    #[must_use]
    pub fn correlations(&self) -> &[CorrelationType] {
        &self.correlations
    }

    /// Requested image coordinates in output order.
    #[must_use]
    pub fn requested(&self) -> &[PolarizationCoordinate] {
        &self.requested
    }

    /// Grid polarization planes.
    #[must_use]
    pub fn grid(&self) -> &[GridPolarization] {
        &self.grid
    }

    /// Number of grid polarization planes.
    #[must_use]
    pub fn grid_pols(&self) -> usize {
        self.grid.len()
    }

    /// Grid plane fed by each selected correlation (`None`: not gridded).
    #[must_use]
    pub fn pol_map(&self) -> &[Option<u8>] {
        &self.pol_map
    }

    /// Feed basis of the selected correlations.
    #[must_use]
    pub const fn feed(&self) -> FeedBasis {
        self.feed
    }

    /// Coefficient of grid plane `gpol` in requested plane `requested`; the
    /// requested image is the real part of the sum.
    #[must_use]
    pub fn to_requested(&self, requested: usize, gpol: usize) -> Complex64 {
        self.to_requested[requested * self.grid.len() + gpol]
    }

    /// Coefficient of requested model plane `requested` in grid plane `gpol`.
    #[must_use]
    pub fn from_requested(&self, gpol: usize, requested: usize) -> Complex64 {
        self.from_requested[gpol * self.requested.len() + requested]
    }

    /// Grid plane whose `sumwt` normalises requested plane `requested`.
    #[must_use]
    pub fn sumwt_source(&self, requested: usize) -> usize {
        self.sumwt_source[requested]
    }
}

fn feed_basis(correlations: &[CorrelationType]) -> Result<FeedBasis, OperatorError> {
    let Some(first) = correlations.first() else {
        return Err(OperatorError::Polarization {
            reason: "no correlation selected",
        });
    };
    let feed = basis_of(*first)?;
    for (index, correlation) in correlations.iter().enumerate() {
        if basis_of(*correlation)? != feed {
            return Err(OperatorError::Polarization {
                reason: "selected correlations mix feed bases",
            });
        }
        if correlations[..index].contains(correlation) {
            return Err(OperatorError::Polarization {
                reason: "selected correlations repeat",
            });
        }
    }
    Ok(feed)
}

fn basis_of(correlation: CorrelationType) -> Result<FeedBasis, OperatorError> {
    match correlation {
        CorrelationType::StokesI
        | CorrelationType::StokesQ
        | CorrelationType::StokesU
        | CorrelationType::StokesV => Ok(FeedBasis::Stokes),
        CorrelationType::LinearXx
        | CorrelationType::LinearXy
        | CorrelationType::LinearYx
        | CorrelationType::LinearYy => Ok(FeedBasis::Linear),
        CorrelationType::CircularRr
        | CorrelationType::CircularRl
        | CorrelationType::CircularLr
        | CorrelationType::CircularLl => Ok(FeedBasis::Circular),
        _ => Err(OperatorError::Polarization {
            reason: "selected correlation is not a Stokes, linear or circular product",
        }),
    }
}

const fn is_stokes(coordinate: PolarizationCoordinate) -> bool {
    matches!(
        coordinate,
        PolarizationCoordinate::StokesI
            | PolarizationCoordinate::StokesQ
            | PolarizationCoordinate::StokesU
            | PolarizationCoordinate::StokesV
    )
}

const fn is_parallel_hand(correlation: CorrelationType) -> bool {
    matches!(
        correlation,
        CorrelationType::LinearXx
            | CorrelationType::LinearYy
            | CorrelationType::CircularRr
            | CorrelationType::CircularLl
    )
}

const fn correlation_of(coordinate: PolarizationCoordinate) -> CorrelationType {
    match coordinate {
        PolarizationCoordinate::StokesI => CorrelationType::StokesI,
        PolarizationCoordinate::StokesQ => CorrelationType::StokesQ,
        PolarizationCoordinate::StokesU => CorrelationType::StokesU,
        PolarizationCoordinate::StokesV => CorrelationType::StokesV,
        PolarizationCoordinate::LinearXx => CorrelationType::LinearXx,
        PolarizationCoordinate::LinearXy => CorrelationType::LinearXy,
        PolarizationCoordinate::LinearYx => CorrelationType::LinearYx,
        PolarizationCoordinate::LinearYy => CorrelationType::LinearYy,
        PolarizationCoordinate::CircularRr => CorrelationType::CircularRr,
        PolarizationCoordinate::CircularRl => CorrelationType::CircularRl,
        PolarizationCoordinate::CircularLr => CorrelationType::CircularLr,
        PolarizationCoordinate::CircularLl => CorrelationType::CircularLl,
    }
}

/// CASA `StokesImageUtil::changeLabelsStokesToCorrStokes`: the complex
/// grid planes that serve a Stokes request on linear or circular feeds.
fn casa_correlation_representation(
    feed: FeedBasis,
    requested: &[PolarizationCoordinate],
) -> Vec<GridPolarization> {
    use CorrelationType::{
        CircularLl, CircularLr, CircularRl, CircularRr, LinearXx, LinearXy, LinearYx, LinearYy,
    };
    use PolarizationCoordinate::{StokesI, StokesQ, StokesU, StokesV};
    let planes = |list: &[CorrelationType]| {
        list.iter()
            .map(|correlation| GridPolarization::Correlation(*correlation))
            .collect::<Vec<_>>()
    };
    let all = match feed {
        FeedBasis::Linear => planes(&[LinearXx, LinearXy, LinearYx, LinearYy]),
        FeedBasis::Circular | FeedBasis::Stokes => {
            planes(&[CircularRr, CircularRl, CircularLr, CircularLl])
        }
    };
    match (feed, requested) {
        (_, [StokesI]) => vec![GridPolarization::StokesI],
        (FeedBasis::Circular, [StokesQ] | [StokesU] | [StokesQ, StokesU]) => {
            planes(&[CircularRl, CircularLr])
        }
        (FeedBasis::Circular, [StokesV] | [StokesI, StokesV]) => planes(&[CircularRr, CircularLl]),
        (FeedBasis::Linear, [StokesQ] | [StokesI, StokesQ]) => planes(&[LinearXx, LinearYy]),
        (FeedBasis::Linear, [StokesU] | [StokesV] | [StokesU, StokesV]) => {
            planes(&[LinearXy, LinearYx])
        }
        _ => all,
    }
}

/// `StokesImageUtil::To` (`CStokesVector::applySlinInv` / `applyScircInv`):
/// the coefficient of one grid plane in one requested plane.
fn to_coefficient(
    feed: FeedBasis,
    requested: PolarizationCoordinate,
    plane: GridPolarization,
) -> Complex64 {
    use CorrelationType::{
        CircularLl, CircularLr, CircularRl, CircularRr, LinearXx, LinearXy, LinearYx, LinearYy,
    };
    use PolarizationCoordinate::{StokesI, StokesQ, StokesU, StokesV};
    let one = Complex64::new(1.0, 0.0);
    let half = Complex64::new(0.5, 0.0);
    let half_i = Complex64::new(0.0, 0.5);
    let correlation = match plane {
        GridPolarization::StokesI => {
            return if requested == StokesI {
                one
            } else {
                Complex64::default()
            };
        }
        GridPolarization::Correlation(correlation) => correlation,
    };
    if !is_stokes(requested) || feed == FeedBasis::Stokes {
        return if correlation_of(requested) == correlation {
            one
        } else {
            Complex64::default()
        };
    }
    match (feed, requested, correlation) {
        (FeedBasis::Linear, StokesI, LinearXx | LinearYy)
        | (FeedBasis::Linear, StokesQ, LinearXx)
        | (FeedBasis::Linear, StokesU, LinearXy | LinearYx)
        | (FeedBasis::Circular, StokesI, CircularRr | CircularLl)
        | (FeedBasis::Circular, StokesQ, CircularRl | CircularLr)
        | (FeedBasis::Circular, StokesV, CircularRr) => half,
        (FeedBasis::Linear, StokesQ, LinearYy) | (FeedBasis::Circular, StokesV, CircularLl) => {
            -half
        }
        (FeedBasis::Linear, StokesV, LinearYx) | (FeedBasis::Circular, StokesU, CircularLr) => {
            half_i
        }
        (FeedBasis::Linear, StokesV, LinearXy) | (FeedBasis::Circular, StokesU, CircularRl) => {
            -half_i
        }
        _ => Complex64::default(),
    }
}

/// `StokesImageUtil::From` (`CStokesVector::applySlin` / `applyScirc`): the
/// coefficient of one requested model plane in one grid plane.
fn from_coefficient(
    feed: FeedBasis,
    plane: GridPolarization,
    requested: PolarizationCoordinate,
) -> Complex64 {
    use CorrelationType::{
        CircularLl, CircularLr, CircularRl, CircularRr, LinearXx, LinearXy, LinearYx, LinearYy,
    };
    use PolarizationCoordinate::{StokesI, StokesQ, StokesU, StokesV};
    let one = Complex64::new(1.0, 0.0);
    let i = Complex64::new(0.0, 1.0);
    let correlation = match plane {
        GridPolarization::StokesI => {
            return if requested == StokesI {
                one
            } else {
                Complex64::default()
            };
        }
        GridPolarization::Correlation(correlation) => correlation,
    };
    if !is_stokes(requested) || feed == FeedBasis::Stokes {
        return if correlation_of(requested) == correlation {
            one
        } else {
            Complex64::default()
        };
    }
    match (feed, correlation, requested) {
        (FeedBasis::Linear, LinearXx, StokesI | StokesQ)
        | (FeedBasis::Linear, LinearYy, StokesI)
        | (FeedBasis::Linear, LinearXy | LinearYx, StokesU)
        | (FeedBasis::Circular, CircularRr, StokesI | StokesV)
        | (FeedBasis::Circular, CircularLl, StokesI)
        | (FeedBasis::Circular, CircularRl | CircularLr, StokesQ) => one,
        (FeedBasis::Linear, LinearYy, StokesQ) | (FeedBasis::Circular, CircularLl, StokesV) => -one,
        (FeedBasis::Linear, LinearXy, StokesV) | (FeedBasis::Circular, CircularRl, StokesU) => i,
        (FeedBasis::Linear, LinearYx, StokesV) | (FeedBasis::Circular, CircularLr, StokesU) => -i,
        _ => Complex64::default(),
    }
}
