// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, SpectralCoordinate};
use casa_core::measures::{
    doppler::{DopplerRef, MDoppler},
    frame::MeasFrame,
    frequency::FrequencyRef,
};
use casa_core::quanta::{Quantity, Unit};

use crate::ImageError;

use super::invalid;

const SPEED_OF_LIGHT: f64 = 299_792_458.;

/// Frequency frame and observing conditions for a profile axis conversion.
pub struct ProfileFrequencyFrame {
    /// Frequency reference requested by the caller.
    pub reference: FrequencyRef,
    /// Observing direction, epoch, position and measures loaded from the image context.
    pub context: MeasFrame,
}

#[derive(Clone, Copy)]
pub(super) enum SpectralDisplay {
    Default,
    Radio,
    Optical,
    Relativistic,
    Wavelength,
    AirWavelength,
}

impl SpectralDisplay {
    pub(super) fn parse(name: &str) -> Result<Self, ImageError> {
        let value = name.to_ascii_lowercase();
        if value == "r" {
            return Err(invalid(
                "Ambiguous spectral type, could be either radio or relativistic",
            ));
        }
        if value.is_empty() {
            return Ok(Self::Default);
        }
        for (name, kind) in [
            ("default", Self::Default),
            ("optical velocity", Self::Optical),
            ("radio velocity", Self::Radio),
            ("relativistic velocity", Self::Relativistic),
            ("beta", Self::Relativistic),
            ("wavelength", Self::Wavelength),
            ("air wavelength", Self::AirWavelength),
        ] {
            if name.starts_with(&value) {
                return Ok(kind);
            }
        }
        Err(invalid(format!("{name} does not identify a spectral type")))
    }
}

pub(super) struct ProfileCoordinate<'a> {
    input: &'a CoordinateSystem,
    output: &'a CoordinateSystem,
    axis: usize,
    world_axis: usize,
    axis_name: String,
    input_spectral: Option<&'a SpectralCoordinate>,
    output_spectral: Option<&'a SpectralCoordinate>,
    native_unit: String,
    world_unit: String,
}

impl<'a> ProfileCoordinate<'a> {
    pub(super) fn new(
        input: &'a CoordinateSystem,
        output: &'a CoordinateSystem,
        axis: usize,
    ) -> Result<Self, ImageError> {
        let world_axis = output
            .pixel_axis_to_world_axis(axis)
            .ok_or_else(|| invalid("Profile pixel axis has no corresponding world axis"))?;
        let (owner, local) = output
            .world_axis_owner(world_axis)
            .ok_or_else(|| invalid("Profile world axis has no coordinate"))?;
        let model = output.coordinate(owner);
        let world_unit = model.axis_units()[local].clone();
        let native_unit = if let CoordinateModel::Direction(direction) = model {
            direction.visible_world_units()[local].clone()
        } else {
            world_unit.clone()
        };
        let output_spectral = match model {
            CoordinateModel::Spectral(spectral) => Some(spectral.as_ref()),
            _ => None,
        };
        let input_spectral = input.pixel_axis_owner(axis).and_then(|(index, _)| {
            if let CoordinateModel::Spectral(spectral) = input.coordinate(index) {
                Some(spectral.as_ref())
            } else {
                None
            }
        });
        let axis_name = model.axis_names()[local].clone();
        Ok(Self {
            input,
            output,
            axis,
            world_axis,
            axis_name,
            input_spectral,
            output_spectral,
            native_unit,
            world_unit,
        })
    }

    pub(super) fn evaluate(
        &self,
        planes: &[f64],
        unit: &str,
        display: SpectralDisplay,
        rest_freq: Option<&Quantity>,
        frame: Option<ProfileFrequencyFrame>,
    ) -> Result<(Vec<f64>, String, String), ImageError> {
        if unit.to_ascii_lowercase().starts_with("pix") {
            return Ok((planes.to_vec(), "pixel".to_owned(), self.axis_name.clone()));
        }
        let unit = if unit.is_empty() {
            &self.native_unit
        } else {
            unit
        };
        let output_unit = Unit::new(unit).map_err(|error| invalid(error.to_string()))?;
        let world_unit = Unit::new(&self.world_unit).map_err(|error| invalid(error.to_string()))?;
        let convert = Conversion::new(
            self.output_spectral,
            &world_unit,
            &output_unit,
            display,
            rest_freq,
        )?;
        let coords = if let (Some(base), Some(frame)) = (self.input_spectral, frame) {
            let converted = base.clone().with_conversion(frame.reference, frame.context);
            let frequency_unit = Unit::new("Hz").expect("hertz is a built-in unit");
            let to_world_unit = Quantity::with_unit(1., frequency_unit)
                .get_value_in(&world_unit)
                .map_err(|error| invalid(error.to_string()))?;
            planes
                .iter()
                .map(|&pixel| -> Result<_, ImageError> {
                    Ok(convert.apply(converted.world_frequency_hz(pixel)? * to_world_unit))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            self.output_coordinates(planes.len(), &convert)?
        };
        let name = if matches!(convert, Conversion::Velocity { .. }) {
            "Velocity".to_owned()
        } else {
            self.axis_name.clone()
        };
        Ok((coords, unit.to_owned(), name))
    }

    fn output_coordinates(
        &self,
        length: usize,
        convert: &Conversion,
    ) -> Result<Vec<f64>, ImageError> {
        let mut pixels = vec![0.; self.input.n_pixel_axes()];
        (0..length)
            .map(|index| {
                pixels[self.axis] = index as f64;
                self.output
                    .to_world(&pixels)
                    .map(|coordinates| convert.apply(coordinates[self.world_axis]))
                    .map_err(ImageError::from)
            })
            .collect()
    }
}

enum Conversion {
    Units(f64),
    Velocity {
        convention: DopplerRef,
        frequency_unit: f64,
        rest_hz: f64,
        scale: f64,
    },
    Wavelength {
        frequency_unit: f64,
        scale: f64,
        air: bool,
    },
}

impl Conversion {
    fn new(
        spectral: Option<&SpectralCoordinate>,
        source: &Unit,
        target: &Unit,
        display: SpectralDisplay,
        rest_freq: Option<&Quantity>,
    ) -> Result<Self, ImageError> {
        if let Ok(scale) = Quantity::with_unit(1., source.clone()).get_value_in(target) {
            return Ok(Self::Units(scale));
        }
        let Some(spectral) = spectral else {
            return Err(invalid(format!(
                "Unit {} does not conform to corresponding axis unit {}",
                target.name(),
                source.name()
            )));
        };
        let explicit_rest = rest_freq.map(rest_frequency_hz).transpose()?;
        let frequency_unit = source.val().factor;
        if target
            .val()
            .conformant(Unit::new("m/s").expect("velocity unit").val())
        {
            let convention = match display {
                SpectralDisplay::Default | SpectralDisplay::Relativistic => DopplerRef::BETA,
                SpectralDisplay::Radio => DopplerRef::RADIO,
                SpectralDisplay::Optical => DopplerRef::Z,
                _ => {
                    return Err(invalid(
                        "Inconsistent spectral type used for velocity units",
                    ));
                }
            };
            let rest_hz = if let Some(rest) = explicit_rest {
                rest
            } else {
                spectral.rest_frequency_hz()?
            };
            Ok(Self::Velocity {
                convention,
                frequency_unit,
                rest_hz,
                scale: SPEED_OF_LIGHT / target.val().factor,
            })
        } else if target
            .val()
            .conformant(Unit::new("m").expect("length unit").val())
        {
            if matches!(
                display,
                SpectralDisplay::Radio | SpectralDisplay::Optical | SpectralDisplay::Relativistic
            ) {
                return Err(invalid(
                    "Inconsistent spectral type used for wavelength units",
                ));
            }
            Ok(Self::Wavelength {
                frequency_unit,
                scale: SPEED_OF_LIGHT / target.val().factor,
                air: matches!(display, SpectralDisplay::AirWavelength),
            })
        } else {
            Err(invalid(format!(
                "Invalid spectral conversion unit {}",
                target.name()
            )))
        }
    }

    fn apply(&self, value: f64) -> f64 {
        match self {
            Self::Units(scale) => value * scale,
            Self::Velocity {
                convention,
                frequency_unit,
                rest_hz,
                scale,
            } => {
                if *rest_hz == 0. {
                    return f64::NAN;
                }
                let ratio = value.abs() * frequency_unit / rest_hz;
                MDoppler::new(ratio, DopplerRef::RATIO)
                    .convert_to(*convention, &MeasFrame::default())
                    .map_or(f64::NAN, |velocity| velocity.value() * scale)
            }
            Self::Wavelength {
                frequency_unit,
                scale,
                air,
            } => {
                let frequency = value * frequency_unit;
                if frequency <= 0. {
                    return f64::INFINITY;
                }
                let wavelength = scale / frequency;
                if *air {
                    let inverse_microns_sq = (frequency / (1e6 * SPEED_OF_LIGHT)).powi(2);
                    let index = 1.
                        + 1e-6
                            * (287.6155
                                + inverse_microns_sq * (1.62887 + 0.01360 * inverse_microns_sq));
                    wavelength / index
                } else {
                    wavelength
                }
            }
        }
    }
}

fn rest_frequency_hz(rest: &Quantity) -> Result<f64, ImageError> {
    let unit = rest.unit().val();
    let hertz = Unit::new("Hz").expect("hertz is a built-in unit");
    let meters = Unit::new("m").expect("meters are a built-in unit");
    let value = if unit.dim.is_dimensionless() {
        rest.get_si_value()
    } else if unit.conformant(meters.val()) {
        SPEED_OF_LIGHT / rest.get_si_value()
    } else {
        rest.get_value_in(&hertz)
            .map_err(|error| invalid(error.to_string()))?
    };
    if value.is_finite() && value >= 0. {
        Ok(value)
    } else {
        Err(invalid("A rest frequency must be finite and nonnegative"))
    }
}
