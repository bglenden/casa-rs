// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert the legacy imager's Python image-definition arguments.

use casa_calibration::ft::{ImagerDefinitionRequest, ImagerPhaseCentre, ImagerSpectralAxis};
use casa_coordinates::{ProjectionType, StokesType};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::doppler::DopplerRef;
use casa_core::measures::frequency::FrequencyRef;
use casa_ms::{CubeAxisConfig, CubeAxisValue};
use pyo3::prelude::*;

use crate::convert::measures::{direction, frequency, frequency_reference};
use crate::convert::{Variant, quantity_value};

use super::runtime_error;

pub(super) struct ImageArguments<'a> {
    pub(super) size: [i64; 2],
    pub(super) cell: [&'a Variant; 2],
    pub(super) stokes: &'a str,
    pub(super) centre: &'a Variant,
    pub(super) mode: &'a str,
    pub(super) nchan: i64,
    pub(super) start: &'a Variant,
    pub(super) step: &'a Variant,
    pub(super) windows: Vec<i64>,
    pub(super) rest: &'a Variant,
    pub(super) frame: &'a str,
    pub(super) velocity: &'a str,
    pub(super) facets: i64,
    pub(super) moving: &'a Variant,
    pub(super) distance: &'a Variant,
    pub(super) projection: &'a str,
}

impl ImageArguments<'_> {
    pub(super) fn request(self) -> PyResult<ImagerDefinitionRequest> {
        let [nx, ny] = self.size;
        let shape = [nx, if ny < 1 { nx } else { ny }]
            .map(|size| usize::try_from(size).ok().filter(|&n| n > 0));
        let [Some(nx), Some(ny)] = shape else {
            return Err(runtime_error("image axes must have at least one pixel"));
        };
        let x = quantity_value(self.cell[0], "rad")?;
        let x = if x == 0. {
            std::f64::consts::PI / 648_000.
        } else {
            x
        };
        let y = if empty(self.cell[1]) {
            x
        } else {
            quantity_value(self.cell[1], "rad")?
        };
        if (nx as f64 * x).abs() > std::f64::consts::PI
            || (ny as f64 * y).abs() > std::f64::consts::PI
        {
            return Err(runtime_error(
                "Cannot image the extent requested for this image",
            ));
        }

        let projection = ProjectionType::from_name(self.projection)
            .filter(|projection| {
                matches!(
                    projection,
                    ProjectionType::SIN
                        | ProjectionType::CAR
                        | ProjectionType::TAN
                        | ProjectionType::SFL
                )
            })
            .ok_or_else(|| {
                runtime_error(format!(
                    "Projection {} is currently not supported.",
                    self.projection
                ))
            })?;
        let stokes = stokes_planes(self.stokes)?;
        let centre = match self.centre {
            Variant::Number(_) | Variant::Numbers(_) | Variant::Empty => {
                let index = channel_number(self.centre);
                if index < 0 {
                    ImagerPhaseCentre::Direction(MDirection::from_angles(
                        0.,
                        std::f64::consts::FRAC_PI_2,
                        DirectionRef::J2000,
                    ))
                } else {
                    ImagerPhaseCentre::Field(index as usize)
                }
            }
            value if empty(value) => ImagerPhaseCentre::Field(0),
            value => ImagerPhaseCentre::Direction(
                direction(
                    value,
                    &casa_ms::open_measures_runtime().map_err(runtime_error)?,
                )?
                .ok_or_else(|| runtime_error("Could not interprete phasecenter parameter"))?,
            ),
        };
        let moving_source = if empty(self.moving) {
            None
        } else {
            Some(
                direction(
                    self.moving,
                    &casa_ms::open_measures_runtime().map_err(runtime_error)?,
                )?
                .ok_or_else(|| runtime_error("Could not interprete phasecenter parameter"))?,
            )
        };
        let frame = (!self.frame.is_empty())
            .then(|| frequency_reference(self.frame))
            .transpose()?;
        let windows = if self.windows == [-1] {
            Vec::new()
        } else {
            self.windows
                .into_iter()
                .map(usize::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(runtime_error)?
        };
        let rest = quantity_value(self.rest, "Hz")?;
        let _ = quantity_value(self.distance, "m")?;
        let spectral = spectral_axis(
            self.mode,
            self.nchan,
            self.start,
            self.step,
            frame,
            self.velocity,
        )?;
        Ok(ImagerDefinitionRequest {
            shape: [nx, ny],
            facets: usize::try_from(self.facets.max(1)).map_err(runtime_error)?,
            cell_rad: [-x, y],
            stokes,
            centre,
            moving_source,
            projection,
            windows,
            rest_frequency_hz: (rest > 0.).then_some(rest),
            spectral,
        })
    }
}

fn spectral_axis(
    mode: &str,
    channels: i64,
    start: &Variant,
    step: &Variant,
    outframe: Option<FrequencyRef>,
    velocity: &str,
) -> PyResult<ImagerSpectralAxis> {
    let mode = mode.to_ascii_uppercase();
    if mode == "MFS" {
        return Ok(ImagerSpectralAxis::Continuum(outframe));
    }
    let mut config = CubeAxisConfig {
        outframe: outframe.unwrap_or(FrequencyRef::LSRK),
        ..CubeAxisConfig::default()
    };
    let mut frame = outframe;
    if mode == "CHANNEL" {
        config.start = Some(CubeAxisValue::Channel(channel_number(start).max(0)));
        let step = channel_number(step);
        config.width = Some(CubeAxisValue::Channel(if step == 0 { 1 } else { step }));
    } else if mode.contains("FREQ") {
        let (start, _) = frequency(start)?;
        let start = start.ok_or_else(|| runtime_error("Could not interpret frequency value"))?;
        config.start = Some(CubeAxisValue::FrequencyHz {
            hz: start.hz(),
            frame: None,
        });
        frame = Some(start.refer());
        config.width = Some(CubeAxisValue::FrequencyHz {
            hz: quantity_value(step, "Hz")?,
            frame: None,
        });
    } else if mode.contains("VEL") || mode.contains("OPTICAL") || mode.contains("TRUE") {
        let velocity = if mode.contains("VELO") {
            velocity.to_ascii_uppercase()
        } else {
            mode
        };
        config.veltype = if velocity.contains("OPTICAL") {
            DopplerRef::Z
        } else if velocity.contains("TRUE")
            || velocity.contains("RELATI")
            || velocity.contains("BETA")
        {
            DopplerRef::BETA
        } else {
            DopplerRef::RADIO
        };
        let (start, start_frame) = radial_velocity(start)?;
        if config.veltype == DopplerRef::RADIO {
            frame = Some(start_frame.unwrap_or(FrequencyRef::LSRK));
        }
        config.start = Some(CubeAxisValue::VelocityMs {
            ms: start,
            frame: None,
        });
        config.width = Some(CubeAxisValue::VelocityMs {
            ms: quantity_value(step, "m/s")?,
            frame: None,
        });
    } else {
        return Err(runtime_error(format!("Unknown mode {mode}")));
    }
    Ok(ImagerSpectralAxis::Cube {
        channels: usize::try_from(channels.max(1)).map_err(runtime_error)?,
        frame,
        native_output_frame: outframe.is_none(),
        config,
    })
}

fn radial_velocity(value: &Variant) -> PyResult<(f64, Option<FrequencyRef>)> {
    if let Variant::Record(record) = value
        && let Some(value) = record.get("m0")
    {
        let frame = record
            .get("refer")
            .map(|reference| frequency_reference(&reference.describe()))
            .transpose()?;
        return Ok((quantity_value(value, "m/s")?, frame));
    }
    if let Variant::Text(text) = value
        && let Some((reference, amount)) = text.trim().split_once(char::is_whitespace)
        && let Ok(reference) = frequency_reference(reference)
    {
        return Ok((
            quantity_value(&Variant::Text(amount.trim().to_owned()), "m/s")?,
            Some(reference),
        ));
    }
    Ok((quantity_value(value, "m/s")?, None))
}

fn stokes_planes(value: &str) -> PyResult<Vec<StokesType>> {
    let value = value.to_ascii_uppercase();
    if let Some(stokes) = StokesType::from_name(&value) {
        return Ok(vec![stokes]);
    }
    let tokens = if value.contains(',') || value.contains(' ') {
        value
            .split([',', ' '])
            .filter(|plane| !plane.is_empty())
            .collect::<Vec<_>>()
    } else if value.starts_with('R')
        || value.starts_with('L')
        || value.starts_with('X')
        || value.starts_with('Y')
    {
        value
            .as_bytes()
            .chunks(2)
            .map(|plane| std::str::from_utf8(plane).unwrap_or(""))
            .collect()
    } else {
        value
            .char_indices()
            .map(|(index, ch)| &value[index..index + ch.len_utf8()])
            .collect()
    };
    let planes = tokens
        .into_iter()
        .map(StokesType::from_name)
        .collect::<Option<Vec<_>>>();
    match planes {
        Some(planes) if !planes.is_empty() && planes.len() <= 4 => Ok(planes),
        _ => Err(runtime_error(format!(
            "Stokes selection {value} is currently not supported."
        ))),
    }
}

fn channel_number(value: &Variant) -> i32 {
    match value {
        Variant::Number(number) => *number as i32,
        Variant::Numbers(numbers) => numbers.first().copied().unwrap_or(0.) as i32,
        _ => 0,
    }
}

fn empty(value: &Variant) -> bool {
    value.is_empty_sequence() || matches!(value, Variant::Text(text) if text.is_empty())
}
