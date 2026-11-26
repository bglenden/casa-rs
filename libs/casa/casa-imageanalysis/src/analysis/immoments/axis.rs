// SPDX-License-Identifier: LGPL-3.0-or-later
//! World coordinates and integration scale of a collapsed image axis.

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_core::measures::{
    MeasFrame,
    doppler::{DopplerRef, MDoppler},
};
use casa_core::quanta::Unit;

use super::super::{MomentAxis as MomentAxisParam, SPEED_OF_LIGHT_KM_S};
use casa_images::{ImageError, ImageInterface, ImagePixel};

pub(super) fn resolve<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    parameter: Option<&MomentAxisParam>,
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
        Some(MomentAxisParam::Index(index)) if *index < 0 => {
            let axis = spectral()?;
            if selected_shape[axis] == 1 {
                return Err(ImageError::CasaSelection(
                    "Illegal moment axis; it has only 1 pixel".into(),
                ));
            }
            Ok(axis)
        }
        Some(MomentAxisParam::Index(index)) => usize::try_from(*index)
            .ok()
            .filter(|&index| index < image.ndim())
            .ok_or_else(invalid),
        Some(MomentAxisParam::Name(name)) => {
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
    }
}

pub(super) struct MomentAxis<'a> {
    pub(super) index: usize,
    pub(super) unit: String,
    coordinate: &'a CoordinateModel,
    pixel_axes: Vec<Option<usize>>,
    reference_pixel: Vec<f64>,
    local: usize,
    angle_scale: f64,
    rest: Option<f64>,
    doppler: DopplerRef,
}

impl<'a> MomentAxis<'a> {
    pub(super) fn new(system: &'a CoordinateSystem, index: usize) -> Result<Self, ImageError> {
        Self::with_doppler(system, index, DopplerRef::RADIO)
    }

    pub(super) fn with_doppler(
        system: &'a CoordinateSystem,
        index: usize,
        doppler: DopplerRef,
    ) -> Result<Self, ImageError> {
        let (model, local) = system
            .pixel_axis_owner(index)
            .ok_or_else(|| ImageError::CasaSelection("No coordinate for moment axis".into()))?;
        if system.pixel_axis_to_world_axis(index).is_none() {
            return Err(ImageError::CasaSelection(
                "Moment pixel axis has no visible world axis".into(),
            ));
        }
        let coordinate = system.coordinate(model);
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
            pixel_axes: system
                .coordinate_pixel_axes(model)
                .expect("existing coordinate"),
            reference_pixel: system.local_reference_pixel(model),
            local,
            angle_scale,
            rest,
            doppler,
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
            Some(rest) if self.doppler == DopplerRef::RADIO => {
                SPEED_OF_LIGHT_KM_S * (1.0 - world / rest)
            }
            Some(rest) => {
                let doppler = MDoppler::new(world / rest, DopplerRef::RATIO)
                    .convert_to(self.doppler, &MeasFrame::new())
                    .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
                SPEED_OF_LIGHT_KM_S * doppler.value()
            }
            None => world * self.angle_scale,
        })
    }

    pub(super) fn profile_is_separable(&self) -> bool {
        self.pixel_axes
            .iter()
            .flatten()
            .all(|&axis| axis == self.index)
    }

    pub(super) fn profile_pixel(&self, image_pixel: &[usize]) -> Vec<f64> {
        let mut local = self.reference_pixel.clone();
        for (pixel, &axis) in local.iter_mut().zip(&self.pixel_axes) {
            if let Some(axis) = axis {
                *pixel = image_pixel[axis] as f64;
            }
        }
        local
    }

    pub(super) fn sample_world(&self, local: &mut [f64], pixel: f64) -> Result<f64, ImageError> {
        local[self.local] = pixel;
        self.position(local)
    }

    pub(super) fn profiles(&self, shape: &[usize]) -> Result<ProfileCoordinates, ImageError> {
        let coupled: Vec<_> = self
            .pixel_axes
            .iter()
            .enumerate()
            .filter_map(|(local, &axis)| {
                axis.filter(|&axis| axis != self.index)
                    .map(|axis| (local, axis, shape[axis]))
            })
            .collect();
        let mut pixels = self.reference_pixel.clone();
        let len = shape[self.index];
        let mut profiles = Vec::new();
        for number in 0..coupled.iter().map(|&(_, _, len)| len).product() {
            let mut offset = number;
            for &(local, _, length) in coupled.iter().rev() {
                pixels[local] = (offset % length) as f64;
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
            .map(|(_, axis, len)| (axis - usize::from(axis > self.index), len))
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
