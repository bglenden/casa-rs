// SPDX-License-Identifier: LGPL-3.0-or-later
//! The geometry of one integration: uvw, elevation and shadowing.
//!
//! As `NewMSSimulator::observe` does it, at the integration's centre with
//! the stored TIME read as UT1 (CASA's convention, so stored uvw agree with
//! CASA's at equal TIME):
//! - each antenna's uvw is that of the baseline from the observatory to the
//!   antenna (`UvwFrame`), and a row's is the second antenna's minus the
//!   first's;
//! - an antenna is flagged when the field is below the elevation limit at
//!   its position, or when another dish shadows it. Shadowing is charged to
//!   the antenna behind, where CASA's simulator charges the one in front
//!   (recorded in `docs/CASA (C++) bugs.md`);
//! - for `tsys-manual` noise, each antenna's airmass at the phase centre
//!   (`crate::corrupt::airmass`), as CASA's `VisIter::azel` gives the
//!   corruptor.

use std::sync::Arc;

use casa_ms::shadowing::{ProjectedBaseline, ShadowedAntenna, shadowed_antennas};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame, MeasuresProvider, UvwFrame};

use crate::corrupt::{RowNoise, airmass};
use crate::session::array::Antenna;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::observe::PlannedScan;

/// The array's fixed geometry for a session.
pub(crate) struct ArrayGeometry<'a> {
    antennas: &'a [Antenna],
    positions: Vec<MPosition>,
    observatory: MPosition,
    measures: Arc<dyn MeasuresProvider>,
    /// Whether rows need their antennas' airmasses (`tsys-manual` noise).
    airmass: bool,
}

/// One MAIN row's geometry and weights.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct RowGeometry {
    pub(crate) antenna1: usize,
    pub(crate) antenna2: usize,
    pub(crate) uvw_m: [f64; 3],
    pub(crate) flagged: bool,
    pub(crate) sigma: f32,
    pub(crate) weight: f32,
    /// The airmasses of ANTENNA1 and ANTENNA2 at the phase centre, when the
    /// geometry computes them ([`ArrayGeometry::with_airmass`]); NaN
    /// otherwise.
    pub(crate) airmass: [f32; 2],
    /// EXPOSURE, seconds.
    pub(crate) exposure_s: f64,
}

impl RowGeometry {
    /// What the row gives a noise term's σ, its antennas' dishes from
    /// `antennas`.
    pub(crate) fn noise(&self, antennas: &[Antenna]) -> RowNoise {
        RowNoise {
            diameters_m: [
                antennas[self.antenna1].diameter_m,
                antennas[self.antenna2].diameter_m,
            ],
            airmass: self.airmass,
            exposure_s: self.exposure_s,
        }
    }
}

impl<'a> ArrayGeometry<'a> {
    /// The geometry of `antennas`, with uvw referred to `telescope`'s
    /// observatory, or to the first antenna for an unknown telescope.
    pub(crate) fn new(
        telescope: &str,
        antennas: &'a [Antenna],
        measures: Arc<dyn MeasuresProvider>,
    ) -> SimulatorResult<Self> {
        let positions = antennas
            .iter()
            .map(|antenna| {
                let [x, y, z] = antenna.position_itrf_m;
                MPosition::new_itrf(x, y, z)
            })
            .collect::<Vec<_>>();
        let observatory = MPosition::from_observatory_name(telescope, measures.as_ref())
            .ok()
            .flatten()
            .or_else(|| positions.first().cloned())
            .ok_or_else(|| SimulatorError::raised("no antennas"))?;
        Ok(Self {
            antennas,
            positions,
            observatory,
            measures,
            airmass: false,
        })
    }

    /// The geometry, computing each row's airmasses when `airmass`.
    pub(crate) fn with_airmass(self, airmass: bool) -> Self {
        Self { airmass, ..self }
    }

    /// The rows of one integration of `scan` at stored TIME `time_s`, in
    /// CASA's order: antenna 1 ascending, then antenna 2 from antenna 1
    /// (autocorrelations written) or the next. `rows` holds the scan's
    /// [`PlannedRows::baseline_count`](crate::session::observe::PlannedRows::baseline_count)
    /// rows.
    ///
    /// The result depends only on the arguments, so integrations can be
    /// computed on any thread in any order.
    pub(crate) fn integration_rows(
        &self,
        scan: PlannedScan<'_>,
        time_s: f64,
        rows: &mut [RowGeometry],
    ) -> SimulatorResult<()> {
        let planned = scan.rows;
        debug_assert_eq!(rows.len(), planned.baseline_count());
        let count = planned.antenna_count;
        let antennas = &self.antennas[..count];
        let epoch = MEpoch::from_mjd(time_s / 86_400.0, EpochRef::UT1);
        // At the pointing direction, as `NewMSSimulator` computes a moved
        // pointing's UVW, though its DATA is phased on the field (#884).
        let frame = UvwFrame::new(
            scan.scan.pointing_rad,
            epoch.clone(),
            &self.observatory,
            self.measures.clone(),
        )
        .map_err(|error| SimulatorError::raised(format!("uvw at {time_s} s: {error}")))?;
        let observatory = self.observatory.as_itrf();
        let antenna_uvw = antennas
            .iter()
            .map(|antenna| {
                let position = antenna.position_itrf_m;
                frame.baseline_uvw([
                    observatory[0] - position[0],
                    observatory[1] - position[1],
                    observatory[2] - position[2],
                ])
            })
            .collect::<Vec<_>>();
        let largest_diameter = antennas
            .iter()
            .map(|antenna| antenna.diameter_m)
            .fold(0.0_f64, f64::max);
        let with_autocorrelations = planned.autocorrelation_weight > 0.0;
        let baselines = (0..count).flat_map(|antenna1| {
            let first = if with_autocorrelations {
                antenna1
            } else {
                antenna1 + 1
            };
            (first..count).map(move |antenna2| (antenna1, antenna2))
        });
        for (row, (antenna1, antenna2)) in rows.iter_mut().zip(baselines) {
            let uvw_m = [
                antenna_uvw[antenna2][0] - antenna_uvw[antenna1][0],
                antenna_uvw[antenna2][1] - antenna_uvw[antenna1][1],
                antenna_uvw[antenna2][2] - antenna_uvw[antenna1][2],
            ];
            let diameters = antennas[antenna1].diameter_m * antennas[antenna2].diameter_m;
            let sigma = (largest_diameter * largest_diameter / diameters) as f32;
            let mut weight = 1.0 / (sigma * sigma);
            if antenna1 == antenna2 {
                weight *= planned.autocorrelation_weight;
            }
            *row = RowGeometry {
                antenna1,
                antenna2,
                uvw_m,
                flagged: false,
                sigma,
                weight,
                airmass: [f32::NAN; 2],
                exposure_s: planned.integration_s,
            };
        }
        let shadowed = shadowed_antennas(
            count,
            rows.iter()
                .filter(|row| row.antenna1 != row.antenna2)
                .map(|row| ProjectedBaseline {
                    antenna1: row.antenna1,
                    antenna2: row.antenna2,
                    uvw_m: row.uvw_m,
                    diameter1_m: antennas[row.antenna1].diameter_m,
                    diameter2_m: antennas[row.antenna2].diameter_m,
                }),
            planned.shadow_fraction_limit,
            ShadowedAntenna::Behind,
        );
        // The elevation limit is applied where the antennas point
        // (`NewMSSimulator`'s per-antenna `azel`).
        let pointing = self.elevations(scan.scan.pointing_rad, &epoch, count)?;
        let limit = scan.rows.elevation_limit_rad;
        for row in rows.iter_mut() {
            row.flagged = shadowed[row.antenna1]
                || shadowed[row.antenna2]
                || pointing[row.antenna1] < limit
                || pointing[row.antenna2] < limit;
        }
        if self.airmass {
            // The corruptor's airmass is at the phase centre, which a moved
            // pointing leaves (#884).
            let airmasses: Vec<f32> = if scan.scan.phase_center_rad == scan.scan.pointing_rad {
                pointing
                    .iter()
                    .map(|&elevation| airmass(elevation))
                    .collect()
            } else {
                self.airmasses(scan.scan.phase_center_rad, time_s, count)?
            };
            for row in rows.iter_mut() {
                row.airmass = [airmasses[row.antenna1], airmasses[row.antenna2]];
            }
        }
        Ok(())
    }

    /// The airmass ([`airmass`]) of J2000 `direction_rad` at each of the
    /// first `count` antennas at stored TIME `time_s`, as `VisIter::azel`
    /// gives `AtmosCorruptor::simPar` its elevations.
    pub(crate) fn airmasses(
        &self,
        direction_rad: [f64; 2],
        time_s: f64,
        count: usize,
    ) -> SimulatorResult<Vec<f32>> {
        let epoch = MEpoch::from_mjd(time_s / 86_400.0, EpochRef::UT1);
        Ok(self
            .elevations(direction_rad, &epoch, count)?
            .into_iter()
            .map(airmass)
            .collect())
    }

    /// The elevation (AZEL) of J2000 `direction_rad` at each of the first
    /// `count` antennas' positions at `epoch`, radians.
    fn elevations(
        &self,
        direction_rad: [f64; 2],
        epoch: &MEpoch,
        count: usize,
    ) -> SimulatorResult<Vec<f64>> {
        let [ra, dec] = direction_rad;
        let direction = MDirection::from_angles(ra, dec, DirectionRef::J2000);
        self.positions[..count]
            .iter()
            .map(|position| {
                let frame = MeasFrame::new()
                    .with_epoch(epoch.clone())
                    .with_position(position.clone())
                    .with_measures(self.measures.clone());
                let azel = direction
                    .convert_to(DirectionRef::AZEL, &frame)
                    .map_err(|error| SimulatorError::raised(format!("elevation: {error}")))?;
                Ok(azel.as_angles().1)
            })
            .collect()
    }
}
