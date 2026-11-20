// SPDX-License-Identifier: LGPL-3.0-or-later
//! World coordinates and integration scale of a collapsed image axis.

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_provider_contracts::ParameterValue;
use casa_types::quanta::Unit;

use super::super::SPEED_OF_LIGHT_KM_S;
use crate::{ImageError, ImagePixel, PagedImage};

pub(super) fn resolve<T: ImagePixel>(
    image: &PagedImage<T>,
    parameter: Option<&ParameterValue>,
    selected_shape: &[usize],
) -> Result<usize, ImageError> {
    let invalid = || ImageError::CasaSelection("Illegal moment axis; out of range".into());
    let spectral = || {
        image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
            ImageError::InvalidMetadata(
                "There is no spectral axis in this image -- specify the axis".into(),
            )
        })
    };
    match parameter {
        None => spectral(),
        Some(ParameterValue::Integer(index)) if *index < 0 => {
            let axis = spectral()?;
            if selected_shape[axis] == 1 {
                return Err(ImageError::CasaSelection(
                    "Illegal moment axis; it has only 1 pixel".into(),
                ));
            }
            Ok(axis)
        }
        Some(ParameterValue::Integer(index)) => usize::try_from(*index)
            .ok()
            .filter(|&index| index < image.ndim())
            .ok_or_else(invalid),
        Some(ParameterValue::String(name)) => {
            let name = name.trim().to_ascii_lowercase();
            let candidates: Vec<_> = image
                .axis_names()
                .iter()
                .enumerate()
                .filter_map(|(i, item)| {
                    let item = item.to_ascii_lowercase();
                    let alias = match item.as_str() {
                        "right ascension" => "ra",
                        "declination" => "dec",
                        "longitude" => "long",
                        "latitude" => "lat",
                        "frequency" | "velocity" => "spectral",
                        _ => &item,
                    };
                    (item.starts_with(&name) || alias.starts_with(&name)).then_some(i)
                })
                .collect();
            match candidates.as_slice() {
                [index] => Ok(*index),
                [] => Err(ImageError::CasaSelection(format!(
                    "No axis matches requested axis {name}"
                ))),
                _ => Err(ImageError::CasaSelection(format!(
                    "Multiple axes match requested axis {name}"
                ))),
            }
        }
        _ => Err(ImageError::CasaSelection(
            "axis must be an index or axis name".into(),
        )),
    }
}

pub(super) struct MomentAxis<'a> {
    pub(super) index: usize,
    pub(super) unit: String,
    coordinate: &'a CoordinateModel,
    offset: usize,
    local: usize,
    angle_scale: f64,
    rest: Option<f64>,
}

impl<'a> MomentAxis<'a> {
    pub(super) fn new(system: &'a CoordinateSystem, index: usize) -> Result<Self, ImageError> {
        let mut offset = 0;
        let mut coordinate = None;
        for i in 0..system.n_coordinates() {
            let model = system.coordinate(i);
            if index < offset + model.n_pixel_axes() {
                coordinate = Some(model);
                break;
            }
            offset += model.n_pixel_axes();
        }
        let coordinate = coordinate
            .ok_or_else(|| ImageError::CasaSelection("No coordinate for moment axis".into()))?;
        let local = index - offset;
        let rest = match coordinate {
            CoordinateModel::Spectral(spectral) if spectral.rest_frequency() > 0.0 => {
                Some(spectral.rest_frequency())
            }
            _ => None,
        };
        let (unit, angle_scale) = if let CoordinateModel::Direction(direction) = coordinate {
            let unit = direction.visible_world_units()[local].clone();
            let conversion =
                Unit::new(&unit).map_err(|e| ImageError::InvalidMetadata(e.to_string()))?;
            (unit, conversion.val().factor.recip())
        } else if rest.is_some() {
            ("km/s".into(), 1.0)
        } else {
            (coordinate.axis_units()[local].clone(), 1.0)
        };
        Ok(Self {
            index,
            unit,
            coordinate,
            offset,
            local,
            angle_scale,
            rest,
        })
    }

    pub(super) fn integrated_scale(&self) -> Result<f64, ImageError> {
        if self.rest.is_some() {
            let reference = self.coordinate.reference_pixel()[0];
            Ok((self.position(&[reference + 0.5])? - self.position(&[reference - 0.5])?).abs())
        } else {
            let increment = self.coordinate.increment()[self.local] * self.angle_scale;
            Ok(increment * increment)
        }
    }

    fn position(&self, pixel: &[f64]) -> Result<f64, ImageError> {
        let world = self.coordinate.to_world(pixel)?[self.local];
        Ok(match self.rest {
            Some(rest) => SPEED_OF_LIGHT_KM_S * (1.0 - world / rest),
            None => world * self.angle_scale,
        })
    }

    pub(super) fn profiles(&self, shape: &[usize]) -> Result<ProfileCoordinates, ImageError> {
        let rank = self.coordinate.n_pixel_axes();
        let coupled: Vec<_> = (self.offset..self.offset + rank)
            .filter(|&axis| axis != self.index)
            .map(|axis| (axis, shape[axis]))
            .collect();
        let mut pixels = vec![0.0; rank];
        let len = shape[self.index];
        let mut profiles = Vec::new();
        for number in 0..coupled.iter().map(|&(_, len)| len).product() {
            let mut offset = number;
            for &(axis, length) in coupled.iter().rev() {
                pixels[axis - self.offset] = (offset % length) as f64;
                offset /= length;
            }
            let mut profile = Vec::with_capacity(len);
            for sample in 0..len {
                pixels[self.local] = sample as f64;
                profile.push(self.position(&pixels)?);
            }
            profiles.push(profile);
        }
        let axes = coupled
            .into_iter()
            .map(|(axis, len)| (axis - usize::from(axis > self.index), len))
            .collect();
        Ok(ProfileCoordinates { profiles, axes })
    }
}

pub(in crate::analysis) struct ProfileCoordinates {
    profiles: Vec<Vec<f64>>,
    axes: Vec<(usize, usize)>,
}

impl ProfileCoordinates {
    #[cfg(test)]
    pub(in crate::analysis) fn fixed(profile: &[f64]) -> Self {
        Self {
            profiles: vec![profile.to_vec()],
            axes: vec![],
        }
    }

    pub(super) fn at(&self, index: &[usize]) -> &[f64] {
        let offset = self
            .axes
            .iter()
            .fold(0, |offset, &(axis, len)| offset * len + index[axis]);
        &self.profiles[offset]
    }
}
