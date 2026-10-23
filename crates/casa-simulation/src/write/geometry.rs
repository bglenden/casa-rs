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
//!   (recorded in `docs/CASA (C++) bugs.md`).

use std::sync::Arc;

use casa_ms::shadowing::{ProjectedBaseline, ShadowedAntenna, shadowed_antennas};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame, MeasuresProvider, UvwFrame};

use crate::session::array::Antenna;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::observe::PlannedScan;

/// The array's fixed geometry for a session.
pub(crate) struct ArrayGeometry<'a> {
    antennas: &'a [Antenna],
    positions: Vec<MPosition>,
    observatory: MPosition,
    measures: Arc<dyn MeasuresProvider>,
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
        })
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
        let frame = UvwFrame::new(
            scan.scan.phase_center_rad,
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
        let low = self.below_elevation_limit(scan, &epoch)?;
        for row in rows.iter_mut() {
            row.flagged = shadowed[row.antenna1]
                || shadowed[row.antenna2]
                || low[row.antenna1]
                || low[row.antenna2];
        }
        Ok(())
    }

    /// For each antenna, whether the field is below `scan`'s elevation limit
    /// at its position (`NewMSSimulator`'s per-antenna `azel`).
    fn below_elevation_limit(
        &self,
        scan: PlannedScan<'_>,
        epoch: &MEpoch,
    ) -> SimulatorResult<Vec<bool>> {
        let [ra, dec] = scan.scan.phase_center_rad;
        let direction = MDirection::from_angles(ra, dec, DirectionRef::J2000);
        self.positions[..scan.rows.antenna_count]
            .iter()
            .map(|position| {
                let frame = MeasFrame::new()
                    .with_epoch(epoch.clone())
                    .with_position(position.clone())
                    .with_measures(self.measures.clone());
                let azel = direction
                    .convert_to(DirectionRef::AZEL, &frame)
                    .map_err(|error| SimulatorError::raised(format!("elevation: {error}")))?;
                Ok(azel.as_angles().1 < scan.rows.elevation_limit_rad)
            })
            .collect()
    }
}
