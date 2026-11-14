// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical Jones factors from interpolated, signed FPARAM values.
use super::*;
use casa_calibration::ComplexSolution;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame};

pub(super) struct Geometry {
    elevations: Vec<f64>,
    magnetic_gauss: Vec<f64>,
    band_hz: [f64; 2],
}

impl Geometry {
    pub(super) fn new(
        sm: &Simulator,
        kind: AppliedKind,
        field: usize,
        time: f64,
        ionosphere_height_m: f64,
        band_hz: [f64; 2],
    ) -> SimulatorResult<Self> {
        let mut result = Self {
            elevations: Vec::new(),
            magnetic_gauss: Vec::new(),
            band_hz,
        };
        if !matches!(
            kind,
            AppliedKind::Opacity
                | AppliedKind::GainCurve
                | AppliedKind::PowerCurve
                | AppliedKind::IonosphereLinear
        ) {
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
            if matches!(kind, AppliedKind::IonosphereLinear) {
                let field = casa_types::measures::earth_magnetic::calculate_igrf(
                    ionosphere_height_m,
                    &direction,
                    &frame,
                )
                .map_err(raised)?;
                result.magnetic_gauss.push(field.los_field_nt * 1e-5);
                continue;
            }
            debug_assert_eq!(index, result.elevations.len());
            let elevation = direction
                .convert_to(DirectionRef::AZEL, &frame)
                .map_err(raised)?
                .as_angles()
                .1;
            result.elevations.push(elevation);
        }
        Ok(result)
    }

    pub(super) fn values(
        &self,
        kind: AppliedKind,
        solution: &ComplexSolution,
        antenna: usize,
        (channel, frequency): (usize, f64),
        reference: f64,
    ) -> Option<[Complex32; 2]> {
        let one = Complex32::new(1., 0.);
        match kind {
            AppliedKind::SwitchedPower => Some(std::array::from_fn(|receptor| {
                let parameter = receptor * 2;
                if solution.flags[parameter * solution.channels] {
                    one
                } else {
                    Complex32::new(solution.value(parameter, 0).re, 0.)
                }
            })),
            AppliedKind::Fringe => Some(std::array::from_fn(|receptor| {
                let parameter = receptor * 4;
                if solution.flags[parameter] {
                    return one;
                }
                let value = |index| f64::from(solution.value(parameter + index, 0).re);
                let f = frequency / 1e9;
                let lo = self.band_hz[0].min(self.band_hz[1]) / 1e9;
                let hi = self.band_hz[0].max(self.band_hz[1]) / 1e9;
                let dispersion =
                    1e-3 * std::f64::consts::TAU * (1. / f + (f - lo - hi) / (lo * hi));
                let phase = value(0)
                    + std::f64::consts::TAU * value(1) * ((frequency - reference) / 1e9)
                    + value(3) * dispersion;
                Complex32::from_polar(1., phase as f32)
            })),
            AppliedKind::CrossPhase => Some(if solution.flags[channel] {
                [one; 2]
            } else {
                [solution.value(0, channel), one]
            }),
            AppliedKind::Delay | AppliedKind::MultiBandDelay => {
                Some(std::array::from_fn(|receptor| {
                    if solution.flags[receptor] {
                        return one;
                    }
                    let reference = if kind == AppliedKind::MultiBandDelay {
                        0.
                    } else {
                        reference
                    };
                    let phase = std::f64::consts::TAU
                        * f64::from(solution.value(receptor, 0).re)
                        * ((frequency - reference) / 1e9);
                    Complex32::new(phase.cos() as f32, phase.sin() as f32)
                }))
            }
            AppliedKind::AntennaPosition => {
                // collapseForSim's pivot is B. KAntPos lies below it and
                // corrects the zeroed DATA, before the corrupted model is
                // added. Therefore supplied offsets do not change DATA.
                Some([one; 2])
            }
            AppliedKind::Opacity => {
                let sine = self.elevations[antenna].sin();
                let gain = if solution.flags[channel] || sine <= 0. {
                    1.
                } else {
                    (-f64::from(solution.value(0, channel).re) / (2. * sine)).exp() as f32
                };
                Some([Complex32::new(gain, 0.); 2])
            }
            AppliedKind::GainCurve | AppliedKind::PowerCurve => {
                let zenith = 90. - self.elevations[antenna].to_degrees();
                Some(std::array::from_fn(|receptor| {
                    let coefficients = if kind == AppliedKind::PowerCurve {
                        8
                    } else {
                        4
                    };
                    let gain = (0..coefficients).rev().fold(0., |gain, coefficient| {
                        gain * zenith
                            + f64::from(solution.value(coefficients * receptor + coefficient, 0).re)
                    });
                    if kind == AppliedKind::PowerCurve {
                        if gain < 0. {
                            one
                        } else {
                            Complex32::new(gain.sqrt() as f32, 0.)
                        }
                    } else {
                        Complex32::new(gain as f32, 0.)
                    }
                }))
            }
            AppliedKind::SystemTemperature => Some(std::array::from_fn(|receptor| {
                let value = solution.value(receptor, channel).re;
                if value <= 0. || solution.flags[receptor * solution.channels + channel] {
                    one
                } else {
                    // StandardTsys::syncJones reverses the inversion sense.
                    Complex32::new(1. / value.sqrt(), 0.)
                }
            })),
            _ => None,
        }
    }

    fn ionosphere(&self, solution: &ComplexSolution, antenna: usize, frequency: f64) -> (f64, f64) {
        if solution.flags[0] {
            return (0., 0.);
        }
        let tec = f64::from(solution.value(0, 0).re);
        (
            -8.4483e-7 * tec / frequency,
            2.3649 * tec * self.magnetic_gauss[antenna] / frequency / frequency,
        )
    }

    pub(super) fn ionosphere_matrix(
        &self,
        solution: &ComplexSolution,
        antenna: usize,
        frequency: f64,
        circular: bool,
    ) -> [Complex32; 4] {
        let (delay, rotation) = self.ionosphere(solution, antenna, frequency);
        if circular {
            return [
                Complex32::from_polar(1., (delay + rotation) as f32),
                Complex32::new(0., 0.),
                Complex32::new(0., 0.),
                Complex32::from_polar(1., (delay - rotation) as f32),
            ];
        }
        let phase = Complex32::from_polar(1., delay as f32);
        let diagonal = phase * (-rotation).cos() as f32;
        let off_diagonal = phase * (-rotation).sin() as f32;
        [diagonal, off_diagonal, -off_diagonal, diagonal]
    }
}
