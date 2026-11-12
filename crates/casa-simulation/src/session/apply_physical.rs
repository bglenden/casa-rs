// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical Jones factors from interpolated, signed FPARAM values.
use super::*;
use casa_calibration::ComplexSolution;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame};

pub(super) struct Geometry {
    sine_elevation: Vec<f64>,
}

impl Geometry {
    pub(super) fn new(
        sm: &Simulator,
        kind: AppliedKind,
        field: usize,
        time: f64,
    ) -> SimulatorResult<Self> {
        let mut result = Self {
            sine_elevation: Vec::new(),
        };
        if kind != AppliedKind::Opacity {
            return Ok(result);
        }
        let [ra, dec] = sm.fields[field].direction_rad;
        let direction = MDirection::from_angles(ra, dec, DirectionRef::J2000);
        let epoch = MEpoch::from_mjd_seconds(time, EpochRef::UTC);
        for (index, antenna) in sm.antennas.iter().enumerate() {
            let [x, y, z] = antenna.position_itrf_m;
            let frame = MeasFrame::new()
                .with_epoch(epoch.clone())
                .with_position(MPosition::new_itrf(x, y, z))
                .with_measures(sm.options.measures.clone());
            debug_assert_eq!(index, result.sine_elevation.len());
            let elevation = direction
                .convert_to(DirectionRef::AZEL, &frame)
                .map_err(raised)?
                .as_angles()
                .1;
            // VisBuffer::azel holds single-precision angles.
            result
                .sine_elevation
                .push(f64::from(elevation as f32).sin());
        }
        Ok(result)
    }

    pub(super) fn values(
        &self,
        kind: AppliedKind,
        solution: &ComplexSolution,
        antenna: usize,
        frequency: f64,
        reference: f64,
    ) -> Option<[Complex32; 2]> {
        let one = Complex32::new(1., 0.);
        match kind {
            AppliedKind::Delay => Some(std::array::from_fn(|receptor| {
                if solution.flags[receptor] {
                    return one;
                }
                let phase = std::f64::consts::TAU
                    * f64::from(solution.value(receptor, 0).re)
                    * ((frequency - reference) / 1e9);
                Complex32::new(phase.cos() as f32, phase.sin() as f32)
            })),
            AppliedKind::AntennaPosition => {
                // collapseForSim's pivot is B. KAntPos lies below it and
                // corrects the zeroed DATA, before the corrupted model is
                // added. Therefore supplied offsets do not change DATA.
                Some([one; 2])
            }
            AppliedKind::Opacity => {
                let sine = self.sine_elevation[antenna];
                let gain = if solution.flags[0] || sine <= 0. {
                    1.
                } else {
                    (-f64::from(solution.value(0, 0).re) / (2. * sine)).exp() as f32
                };
                Some([Complex32::new(gain, 0.); 2])
            }
            _ => None,
        }
    }
}
