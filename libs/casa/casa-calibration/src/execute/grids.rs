// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(crate) struct LoadedCalibrationTable {
    pub(crate) path: PathBuf,
    pub(crate) baseline: bool,
    pub(super) callibrary: Option<crate::callib::CallibraryOptions>,
    pub(super) time_plans: HashMap<i32, crate::lookup::TimePlan>,
    pub(super) missing_grids: HashMap<i32, Arc<CalibrationGrid>>,
    pub(super) time_parameter_family: Option<TimeParameterFamily>,
    pub(super) phase_delay: bool,
    pub(super) interp: ApplyInterpolationMode,
    pub(super) supports_calwt: bool,
    pub(super) engine: Option<Arc<MsCalEngine>>,
    pub(super) antpos_tropo: Option<TropoDelay>,
    pub(super) ionosphere: Option<ionosphere::Ionosphere>,
    pub(super) sky_exposures: Option<sky::SkyExposures>,
    pub(super) spline: Option<gspline::SplineTable>,
    pub(crate) solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>>,
}

#[derive(Clone, Copy)]
pub(super) enum TimeParameterFamily {
    Delay,
    Antpos,
    GainCurve { power: bool },
    Opacity,
    SpectralOpacity,
    Tsys,
    Sky,
    PositionAngle,
    Tec,
    Fringe,
    SwitchedPower,
}

impl TimeParameterFamily {
    pub(super) fn from_subtype(subtype: &str) -> Option<Self> {
        match subtype {
            "K Jones" | "KMBD Jones" | "Kcross Jones" => Some(Self::Delay),
            "KAntPos Jones" => Some(Self::Antpos),
            "EGainCurve" => Some(Self::GainCurve { power: false }),
            "EPowerCurve" => Some(Self::GainCurve { power: true }),
            "TOpac" => Some(Self::Opacity),
            "TfOpac" => Some(Self::SpectralOpacity),
            "B TSYS" => Some(Self::Tsys),
            "PosAng Jones" => Some(Self::PositionAngle),
            "F Jones" => Some(Self::Tec),
            "Fringe Jones" => Some(Self::Fringe),
            "G EVLASWPOW" => Some(Self::SwitchedPower),
            subtype if crate::sd_solver::is_sky_subtype(subtype) => Some(Self::Sky),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct LegacyCalDescEntry {
    pub(super) spw_id: i32,
    pub(super) receptor_count: usize,
}

pub(crate) struct CalibrationSolution {
    pub(crate) time_seconds: f64,
    pub(crate) grid: Arc<CalibrationGrid>,
}

#[derive(Clone)]
pub(crate) enum CalibrationGrid {
    Complex(GainGrid),
    Delay(DelayGrid),
    Fringe(GainGrid),
    Antpos(AntposGrid),
    GainCurve(GainCurveGrid),
    Opacity(OpacityGrid),
    SpectralOpacity(GainGrid),
    SwitchedPower(switched_power::SwitchedPowerGrid),
}

#[derive(Clone)]
pub(crate) struct GainGrid {
    pub(crate) receptor_count: usize,
    pub(crate) channel_count: usize,
    pub(crate) values: ArrayD<Complex32>,
    pub(crate) flags: ArrayD<bool>,
}

#[derive(Clone, Copy)]
pub(super) struct GainSample {
    pub(super) value: Complex32,
    pub(super) flagged: bool,
}

pub(super) struct CalibrationSamplingContext<'a> {
    pub(super) data_channels: DataChannelGrid<'a>,
    pub(super) cal_frequencies_hz: &'a [f64],
    pub(super) cal_ref_frequency_hz: f64,
    pub(super) interp: ApplyInterpolationMode,
    pub(super) path: &'a Path,
    pub(super) engine: Option<&'a MsCalEngine>,
    pub(super) antpos_tropo: Option<&'a TropoDelay>,
}

#[derive(Clone)]
pub(crate) struct DelayGrid {
    pub(crate) receptor_count: usize,
    pub(crate) channel_count: usize,
    pub(crate) values_ns: ArrayD<f32>,
    pub(crate) flags: ArrayD<bool>,
}

#[derive(Clone)]
pub(crate) struct AntposGrid {
    pub(super) offsets_m: [f32; 3],
    pub(super) flagged: bool,
}

#[derive(Clone)]
pub(crate) struct GainCurveGrid {
    pub(super) receptor_count: usize,
    pub(super) power: bool,
    pub(super) coefficients: ArrayD<f32>,
    pub(super) flags: ArrayD<bool>,
}

#[derive(Clone)]
pub(crate) struct OpacityGrid {
    pub(super) tau: f32,
    pub(super) flagged: bool,
}

impl CalibrationGrid {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn sample(
        &self,
        receptor: usize,
        data_chan_index: usize,
        field_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<GainSample, ApplyExecutionError> {
        match self {
            Self::Complex(grid) => grid.sample(
                receptor,
                data_chan_index,
                context.data_channels,
                context.cal_frequencies_hz,
                context.interp,
                context.path,
            ),
            Self::Delay(grid) => grid.sample(
                receptor,
                data_chan_index,
                context.data_channels.frequencies_hz,
                context.cal_ref_frequency_hz,
                context.path,
            ),
            Self::Fringe(grid) => fringe::sample(grid, receptor, data_chan_index, context),
            Self::Antpos(grid) => {
                grid.sample(data_chan_index, field_id, antenna_id, time_seconds, context)
            }
            Self::GainCurve(grid) => {
                grid.sample(receptor, field_id, antenna_id, time_seconds, context)
            }
            Self::Opacity(grid) => grid.sample(field_id, antenna_id, time_seconds, context),
            Self::SpectralOpacity(grid) => {
                let sample = grid.sample(
                    0,
                    data_chan_index,
                    context.data_channels,
                    context.cal_frequencies_hz,
                    context.interp,
                    context.path,
                )?;
                let elevation = opacity_elevation(
                    context.engine,
                    field_id,
                    antenna_id,
                    time_seconds,
                    context.path,
                    None,
                )?;
                Ok(GainSample {
                    value: Complex32::new(opacity_gain(sample.value.re, elevation), 0.),
                    flagged: sample.flagged,
                })
            }
            Self::SwitchedPower(grid) => Ok(grid.sample(receptor)),
        }
    }
}

impl GainGrid {
    pub(super) fn cross_hand_diagonal(self, path: &Path) -> Result<Self, ApplyExecutionError> {
        if self.receptor_count != 1 {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "cross-hand phase requires one CPARAM receptor, found {:?}",
                    self.values.shape()
                ),
            });
        }
        let channel_count = self.channel_count;
        let shape = IxDyn(&[2, channel_count]).f();
        let values = ArrayD::from_shape_fn(shape.clone(), |index| {
            if index[0] == 0 {
                self.value_at(0, index[1])
            } else {
                Complex32::ONE
            }
        });
        let flags = ArrayD::from_shape_fn(shape, |index| self.flag_at(0, index[1]));
        Ok(Self {
            receptor_count: 2,
            channel_count,
            values,
            flags,
        })
    }

    pub(crate) fn from_arrays(
        path: &Path,
        gains: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Self, ApplyExecutionError> {
        let ArrayValue::Complex32(values) = gains else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: gains.shape().to_vec(),
            });
        };
        let ArrayValue::Bool(flags) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };

        if values.shape() != flags.shape() || values.is_empty() {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: "parameter and FLAG arrays must have matching, nonempty shapes".into(),
            });
        }

        match values.shape() {
            [receptor_count] => Ok(Self {
                receptor_count: *receptor_count,
                channel_count: 1,
                values: values.clone(),
                flags: flags.clone(),
            }),
            [receptor_count, channel_count] => Ok(Self {
                receptor_count: *receptor_count,
                channel_count: *channel_count,
                values: values.clone(),
                flags: flags.clone(),
            }),
            shape => Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: shape.to_vec(),
            }),
        }
    }

    pub(super) fn sample(
        &self,
        receptor: usize,
        data_chan_index: usize,
        data_channels: DataChannelGrid<'_>,
        cal_frequencies_hz: &[f64],
        interp: ApplyInterpolationMode,
        path: &Path,
    ) -> Result<GainSample, ApplyExecutionError> {
        let receptor = receptor.min(self.receptor_count.saturating_sub(1));
        if self.channel_count <= 1 {
            return Ok(GainSample {
                value: self.value_at(receptor, 0),
                flagged: self.flag_at(receptor, 0),
            });
        }

        match interp {
            ApplyInterpolationMode::Nearest
            | ApplyInterpolationMode::Linear
            | ApplyInterpolationMode::Casa { .. }
            | ApplyInterpolationMode::Invalid => {
                let chan = data_channels
                    .original_index(data_chan_index)
                    .min(self.channel_count.saturating_sub(1));
                Ok(GainSample {
                    value: self.value_at(receptor, chan),
                    flagged: self.flag_at(receptor, chan),
                })
            }
            ApplyInterpolationMode::NearestLinear => {
                if cal_frequencies_hz.len() != self.channel_count {
                    return Err(ApplyExecutionError::UnsupportedInterpolation {
                        path: path.display().to_string(),
                        interp,
                        reason: "calibration frequency grid does not match CPARAM channel axis"
                            .to_string(),
                    });
                }
                let target_frequency = *data_channels
                    .frequencies_hz
                    .get(data_chan_index)
                    .ok_or_else(|| ApplyExecutionError::UnsupportedInterpolation {
                        path: path.display().to_string(),
                        interp,
                        reason: "data channel index is outside the MeasurementSet SPW grid"
                            .to_string(),
                    })?;
                Ok(self.sample_frequency_linear(receptor, target_frequency, cal_frequencies_hz))
            }
        }
    }

    fn sample_frequency_linear(
        &self,
        receptor: usize,
        target_frequency_hz: f64,
        cal_frequencies_hz: &[f64],
    ) -> GainSample {
        if target_frequency_hz <= cal_frequencies_hz[0] {
            return GainSample {
                value: self.value_at(receptor, 0),
                flagged: self.flag_at(receptor, 0),
            };
        }
        if target_frequency_hz >= cal_frequencies_hz[self.channel_count - 1] {
            let last = self.channel_count - 1;
            return GainSample {
                value: self.value_at(receptor, last),
                flagged: self.flag_at(receptor, last),
            };
        }

        for upper in 1..self.channel_count {
            let lower = upper - 1;
            let low_freq = cal_frequencies_hz[lower];
            let high_freq = cal_frequencies_hz[upper];
            if target_frequency_hz <= high_freq {
                let fraction = (target_frequency_hz - low_freq) / (high_freq - low_freq);
                let low = self.value_at(receptor, lower);
                let high = self.value_at(receptor, upper);
                return GainSample {
                    value: low + (high - low) * fraction as f32,
                    flagged: self.flag_at(receptor, lower) || self.flag_at(receptor, upper),
                };
            }
        }

        let last = self.channel_count - 1;
        GainSample {
            value: self.value_at(receptor, last),
            flagged: self.flag_at(receptor, last),
        }
    }

    pub(super) fn value_at(&self, receptor: usize, channel: usize) -> Complex32 {
        match self.values.ndim() {
            1 => self.values[[receptor]],
            2 => self.values[[receptor, channel]],
            _ => unreachable!("validated during construction"),
        }
    }

    pub(super) fn flag_at(&self, receptor: usize, channel: usize) -> bool {
        match self.flags.ndim() {
            1 => self.flags[[receptor]],
            2 => self.flags[[receptor, channel]],
            _ => unreachable!("validated during construction"),
        }
    }
}

impl DelayGrid {
    pub(super) fn from_arrays(
        path: &Path,
        delays: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Self, ApplyExecutionError> {
        let values_ns = match delays {
            ArrayValue::Float32(values) => values.clone(),
            ArrayValue::Float64(values) => values.mapv(|value| value as f32),
            other => {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: path.display().to_string(),
                    shape: other.shape().to_vec(),
                });
            }
        };
        let ArrayValue::Bool(flags) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };

        match values_ns.shape() {
            [receptor_count] => Ok(Self {
                receptor_count: *receptor_count,
                channel_count: 1,
                values_ns,
                flags: flags.clone(),
            }),
            [receptor_count, channel_count] => Ok(Self {
                receptor_count: *receptor_count,
                channel_count: *channel_count,
                values_ns,
                flags: flags.clone(),
            }),
            shape => Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: shape.to_vec(),
            }),
        }
    }

    fn sample(
        &self,
        receptor: usize,
        data_chan_index: usize,
        data_frequencies_hz: &[f64],
        cal_ref_frequency_hz: f64,
        path: &Path,
    ) -> Result<GainSample, ApplyExecutionError> {
        let target_frequency_hz = *data_frequencies_hz.get(data_chan_index).ok_or_else(|| {
            ApplyExecutionError::UnsupportedInterpolation {
                path: path.display().to_string(),
                interp: ApplyInterpolationMode::Nearest,
                reason: "data channel index is outside the MeasurementSet SPW grid".to_string(),
            }
        })?;
        let receptor = receptor.min(self.receptor_count.saturating_sub(1));
        let delay_ns = self.value_at(receptor, 0);
        let flagged = self.flag_at(receptor, 0);
        let phase_rad = 2.0_f64
            * std::f64::consts::PI
            * f64::from(delay_ns)
            * ((target_frequency_hz - cal_ref_frequency_hz) / 1.0e9_f64);
        Ok(GainSample {
            value: Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32),
            flagged,
        })
    }

    fn value_at(&self, receptor: usize, channel: usize) -> f32 {
        match self.values_ns.ndim() {
            1 => self.values_ns[[receptor]],
            2 => self.values_ns[[receptor, channel.min(self.channel_count.saturating_sub(1))]],
            _ => unreachable!("validated during construction"),
        }
    }

    fn flag_at(&self, receptor: usize, channel: usize) -> bool {
        match self.flags.ndim() {
            1 => self.flags[[receptor]],
            2 => self.flags[[receptor, channel.min(self.channel_count.saturating_sub(1))]],
            _ => unreachable!("validated during construction"),
        }
    }
}

impl AntposGrid {
    pub(super) fn from_arrays(
        path: &Path,
        offsets: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Self, ApplyExecutionError> {
        let values = match offsets {
            ArrayValue::Float32(values) => values.clone(),
            ArrayValue::Float64(values) => values.mapv(|value| value as f32),
            other => {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: path.display().to_string(),
                    shape: other.shape().to_vec(),
                });
            }
        };
        let ArrayValue::Bool(_) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };
        if values.len() < 3 {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: values.shape().to_vec(),
            });
        }
        Ok(Self {
            offsets_m: [values[[0, 0]], values[[1, 0]], values[[2, 0]]],
            // KAntPosJones::calcAllJones marks the projected Jones term valid
            // regardless of the FPARAM flags. Missing rows are handled separately.
            flagged: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn sample(
        &self,
        data_chan_index: usize,
        field_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<GainSample, ApplyExecutionError> {
        let path = context.path;
        let data_frequencies_hz = context.data_channels.frequencies_hz;
        let frequency_hz = *data_frequencies_hz.get(data_chan_index).ok_or_else(|| {
            ApplyExecutionError::UnsupportedInterpolation {
                path: path.display().to_string(),
                interp: ApplyInterpolationMode::Nearest,
                reason: "data channel index is outside the MeasurementSet SPW grid".to_string(),
            }
        })?;
        let Some(engine) = context.engine else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: "KAntPos Jones apply requires MeasurementSet geometry".to_string(),
            });
        };
        let delay_ns = antpos_delay_ns(
            engine,
            field_id,
            antenna_id,
            time_seconds,
            self.offsets_m.map(f64::from),
            path,
            context.antpos_tropo,
            None,
        )?;
        let phase_rad = 2.0 * std::f64::consts::PI * delay_ns * (frequency_hz / 1.0e9);
        Ok(GainSample {
            value: Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32),
            flagged: self.flagged,
        })
    }
}

impl GainCurveGrid {
    pub(super) fn from_arrays(
        path: &Path,
        coefficients: &ArrayValue,
        flags: &ArrayValue,
        power: bool,
    ) -> Result<Self, ApplyExecutionError> {
        let coefficients = match coefficients {
            ArrayValue::Float32(values) => values.clone(),
            ArrayValue::Float64(values) => values.mapv(|value| value as f32),
            other => {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: path.display().to_string(),
                    shape: other.shape().to_vec(),
                });
            }
        };
        let ArrayValue::Bool(_) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };
        let terms = if power {
            crate::gencal::POWER_CURVE_TERMS
        } else {
            4
        };
        if coefficients.ndim() != 2
            || coefficients.shape()[0] % terms != 0
            || (power && coefficients.shape()[0] != 2 * terms)
        {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: coefficients.shape().to_vec(),
            });
        }
        Ok(Self {
            receptor_count: coefficients.shape()[0] / terms,
            power,
            // EGainCurve::calcAllJones evaluates the coefficients and marks
            // each Jones term valid regardless of the input flags.
            flags: ArrayD::from_elem(IxDyn(coefficients.shape()), false),
            coefficients,
        })
    }

    fn sample(
        &self,
        receptor: usize,
        field_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<GainSample, ApplyExecutionError> {
        let Some(engine) = context.engine else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: context.path.display().to_string(),
                reason: "EGainCurve apply requires MeasurementSet geometry".to_string(),
            });
        };
        let (_az, elevation) = engine
            .azel(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: context.path.display().to_string(),
                source,
            })?;
        let za_degrees = (std::f64::consts::FRAC_PI_2 - elevation).to_degrees();
        let receptor = receptor.min(self.receptor_count.saturating_sub(1));
        let (gain, flagged) = self.evaluate(receptor, za_degrees);
        Ok(GainSample {
            value: Complex32::new(gain, 0.0),
            flagged,
        })
    }

    pub(super) fn evaluate(&self, receptor: usize, za_degrees: f64) -> (f32, bool) {
        if self.power {
            let terms = crate::gencal::POWER_CURVE_TERMS;
            let base = receptor * terms;
            let mut loss = f64::from(self.coefficients[[base, 0]]);
            let mut angle_power = 1.0;
            for index in 1..terms {
                angle_power *= za_degrees;
                loss += f64::from(self.coefficients[[base + index, 0]]) * angle_power;
            }
            let missing = self.flags[[base, 0]];
            return if !missing && loss >= 0.0 {
                (loss.sqrt() as f32, false)
            } else {
                (0.0, true)
            };
        }
        let za_degrees = za_degrees as f32;
        let base = receptor * 4;
        let mut gain = self.coefficients[[base, 0]];
        let mut angle_power = 1.0_f32;
        for index in 1..4 {
            angle_power *= za_degrees;
            gain += self.coefficients[[base + index, 0]] * angle_power;
        }
        let flagged = (0..4).any(|index| self.flags[[base + index, 0]]);
        (gain, flagged)
    }
}

impl OpacityGrid {
    pub(super) fn from_arrays(
        path: &Path,
        tau: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Self, ApplyExecutionError> {
        let values = match tau {
            ArrayValue::Float32(values) => values.clone(),
            ArrayValue::Float64(values) => values.mapv(|value| value as f32),
            other => {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: path.display().to_string(),
                    shape: other.shape().to_vec(),
                });
            }
        };
        let ArrayValue::Bool(flags) = flags else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: flags.shape().to_vec(),
            });
        };
        if values.is_empty() {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: path.display().to_string(),
                shape: values.shape().to_vec(),
            });
        }
        Ok(Self {
            tau: values[[0, 0]],
            flagged: flags.iter().any(|flag| *flag),
        })
    }

    fn sample(
        &self,
        field_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        context: &CalibrationSamplingContext<'_>,
    ) -> Result<GainSample, ApplyExecutionError> {
        let Some(engine) = context.engine else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: context.path.display().to_string(),
                reason: "TOpac apply requires MeasurementSet geometry".to_string(),
            });
        };
        let (_az, elevation) = engine
            .azel(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: context.path.display().to_string(),
                source,
            })?;
        let zenith_angle = std::f64::consts::FRAC_PI_2 - elevation;
        let gain = if zenith_angle < std::f64::consts::FRAC_PI_2 {
            (-f64::from(self.tau) / zenith_angle.cos()).exp().sqrt() as f32
        } else {
            1.0
        };
        Ok(GainSample {
            value: Complex32::new(gain, 0.0),
            flagged: self.flagged,
        })
    }
}

impl LoadedCalibrationTable {
    pub(crate) fn interpolation_windows(&self) -> Vec<i32> {
        let mut windows = self.missing_grids.keys().copied().collect::<Vec<_>>();
        windows.sort_unstable();
        windows
    }

    pub(crate) fn complex_solution_for_field(
        &self,
        field: i32,
        query: crate::lookup::TimeQuery,
    ) -> Result<Option<crate::table::ComplexSolution>, ApplyExecutionError> {
        self.time_plans
            .get(&field)
            .map(|plan| plan.solution(&self.path, query))
            .transpose()
            .map(Option::flatten)
    }

    pub(crate) fn lookup(
        &self,
        field_id: i32,
        spw_id: i32,
        antenna_id: i32,
        time_seconds: f64,
        interp: ApplyInterpolationMode,
    ) -> Result<Option<Arc<CalibrationGrid>>, ApplyExecutionError> {
        if let Some(spline) = &self.spline {
            return Ok(gspline::grid(
                spline,
                field_id,
                antenna_id,
                time_seconds,
                None,
            ));
        }
        if self.interp != interp {
            return Err(ApplyExecutionError::UnsupportedInterpolation {
                path: self.path.display().to_string(),
                interp,
                reason: "loaded-table interpolation state diverged from the plan".to_string(),
            });
        }
        let candidates = self
            .solutions
            .get(&(field_id, spw_id, antenna_id))
            .or_else(|| self.solutions.get(&(field_id, spw_id, -1)))
            .map(Vec::as_slice)
            .or_else(|| {
                // CASA applies a sole-field table across selected MS fields by
                // default when no explicit gainfield override is required.
                self.lookup_single_field_candidates(spw_id, antenna_id)
            });

        let Some(candidates) = candidates else {
            return Ok(None);
        };

        match interp {
            ApplyInterpolationMode::Nearest | ApplyInterpolationMode::NearestLinear => Ok(Some(
                candidates
                    .iter()
                    .min_by(|a, b| {
                        (a.time_seconds - time_seconds)
                            .abs()
                            .total_cmp(&(b.time_seconds - time_seconds).abs())
                    })
                    .expect("non-empty candidates")
                    .grid
                    .clone(),
            )),
            ApplyInterpolationMode::Linear => {
                interpolate_time_linear(&self.path, candidates, time_seconds).map(Some)
            }
            other => Err(ApplyExecutionError::UnsupportedInterpolation {
                path: self.path.display().to_string(),
                interp: other,
                reason: "contextual interpolation requires the common apply time engine (#1518)"
                    .to_string(),
            }),
        }
    }

    fn lookup_single_field_candidates(
        &self,
        spw_id: i32,
        antenna_id: i32,
    ) -> Option<&[CalibrationSolution]> {
        let matching_fields = self
            .solutions
            .keys()
            .filter(|(_, key_spw_id, key_antenna_id)| {
                *key_spw_id == spw_id && (*key_antenna_id == antenna_id || *key_antenna_id == -1)
            })
            .map(|(key_field_id, _, _)| *key_field_id)
            .collect::<BTreeSet<_>>();
        let sole_field_id = match matching_fields.len() {
            1 => *matching_fields.first().expect("one matching field"),
            _ => return None,
        };
        self.solutions
            .get(&(sole_field_id, spw_id, antenna_id))
            .or_else(|| self.solutions.get(&(sole_field_id, spw_id, -1)))
            .map(Vec::as_slice)
    }
}
