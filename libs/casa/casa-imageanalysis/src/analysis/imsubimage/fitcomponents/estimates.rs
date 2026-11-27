// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::Path;

use casa_core::quanta::Quantity;

use casa_images::ImageError;

#[derive(Clone)]
pub(super) struct Estimate {
    pub(super) peak: f64,
    pub(super) center: [f64; 2],
    pub(super) major: Quantity,
    pub(super) minor: Quantity,
    pub(super) angle: Quantity,
    pub(super) fixed: [bool; 6],
}

pub(super) struct Estimates {
    pub(super) components: Vec<Estimate>,
    pub(super) text: String,
}

impl Estimates {
    pub(super) fn read(path: &Path) -> Result<Self, ImageError> {
        let text = fs::read_to_string(path).map_err(|error| {
            ImageError::CasaSelection(format!(
                "Could not read estimates file {}: {error}",
                path.display()
            ))
        })?;
        let mut components = Vec::new();
        for (index, line) in text.lines().enumerate() {
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
            components.push(parse_row(&fields).map_err(|error| {
                ImageError::CasaSelection(format!(
                    "Estimates file {}, line {}: {error}",
                    path.display(),
                    index + 1
                ))
            })?);
        }
        if components.is_empty() {
            return Err(ImageError::CasaSelection(format!(
                "No valid estimates in {}",
                path.display()
            )));
        }
        Ok(Self { components, text })
    }
}

fn parse_row(fields: &[&str]) -> Result<Estimate, String> {
    let [peak, x, y, major, minor, angle, optional @ ..] = fields else {
        return Err("expected peak, x, y, major axis, minor axis and position angle".into());
    };
    if optional.len() > 1 {
        return Err("expected at most one fixed-parameter field".into());
    }
    let number = |value: &str, name| {
        value
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
            .ok_or_else(|| format!("{name} {value:?} is not numeric"))
    };
    let quantity = |value: &str, name| {
        value
            .parse::<Quantity>()
            .map_err(|error| format!("{name} {value:?} is not a quantity: {error}"))
    };
    let mut fixed = [false; 6];
    if let Some(flags) = optional.first() {
        for flag in flags.bytes() {
            let Some(index) = b"fxyabp".iter().position(|&key| key == flag) else {
                return Err(format!(
                    "fixed parameter ID {:?} is not recognized",
                    char::from(flag)
                ));
            };
            fixed[index] = true;
        }
    }
    Ok(Estimate {
        peak: number(peak, "peak value")?,
        center: [number(x, "x position")?, number(y, "y position")?],
        major: quantity(major, "major axis")?,
        minor: quantity(minor, "minor axis")?,
        angle: quantity(angle, "position angle")?,
        fixed,
    })
}
