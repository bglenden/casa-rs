// SPDX-License-Identifier: LGPL-3.0-or-later
//! The model `simobserve` makes from a component list when it is given no
//! sky image (`task_simobserve.py`'s component-only branch), and the
//! `.compskymodel` image it draws it on.

use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::quanta::Quantity;
use casa_types::quanta::casacore::{casacore_unit, tos};

use super::modify::check_increments;
use super::{SkyModelError, add_components, flat_image, shape_obs_info, write_image, zeros};
use crate::simutil::{SimutilError, direction, qa};

/// The smallest half-size `simobserve` gives a component-only model, deg
/// (about 5 arcsec).
const MINIMUM_OFFSET_DEG: f64 = 0.0014;

/// What `simobserve` takes from a component list for a model with no image:
/// the list's average direction, the first component's reference frequency,
/// the channels it asks for, and a size three times the farthest component's
/// offset.
#[derive(Debug, Clone, PartialEq)]
pub struct ComponentModel {
    /// The components' average direction, as `simutil` writes directions.
    pub refdir: String,
    /// The first component's reference frequency, in GHz as the component
    /// list's spectrum record gives it.
    pub specrefval: Quantity,
    /// The channel width: `compwidth` (2 GHz when unset) over `comp_nchan`.
    pub width: Quantity,
    /// The number of channels, `comp_nchan`.
    pub nchan: usize,
    /// The reference channel, the middle one: `(comp_nchan − 1)/2`.
    pub specrefpix: f64,
    /// The Stokes planes, `"I"`.
    pub stokes: String,
    /// The size, `"%fdeg"` of three times the farthest component's offset
    /// (at least 0.0014°) along either axis, for both axes.
    pub size: [String; 2],
    /// The whole bandwidth, `compwidth` or `"2GHz"`, which the
    /// `.compskymodel` takes as its spectral increment.
    pub bandwidth: String,
}

impl ComponentModel {
    /// The model `simobserve` makes of `list` with `compwidth` and
    /// `comp_nchan`.
    ///
    /// # Errors
    ///
    /// An empty list, or a component direction or frequency the `simutil`
    /// helpers cannot write.
    pub fn from_list(
        list: &ComponentList,
        compwidth: &str,
        comp_nchan: usize,
    ) -> Result<Self, SkyModelError> {
        let first = list.get(0).ok_or_else(|| {
            SkyModelError::Model("the component list holds no components".to_string())
        })?;
        let directions = list
            .iter()
            .map(|component| {
                let reference = component.shape().reference_direction();
                let [x, y, z] = reference.cosines();
                let longitude = if x != 0.0 || y != 0.0 {
                    y.atan2(x)
                } else {
                    0.0
                };
                let radians = |value: f64| Quantity::with_unit(value, unit("rad"));
                direction::format_direction(
                    reference.refer().as_str(),
                    &radians(longitude),
                    &radians(z.asin()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let centred = direction::average_direction(&directions)?;
        let hz = Quantity::with_unit(first.spectrum().reference_frequency().hz(), unit("Hz"));
        let specrefval = qa::convert(&hz, "GHz")?;
        let bandwidth = if qa::is_quantity(compwidth) {
            tracing::info!("compwidth set: setting model bandwidth to input");
            compwidth.to_string()
        } else {
            tracing::warn!("compwidth unset: setting bandwidth to 2GHz");
            "2GHz".to_string()
        };
        let width = qa::div(&qa::read(&bandwidth)?, &qa::number(comp_nchan as f64))?;
        let farthest = centred
            .offsets
            .iter()
            .flat_map(|offset| offset.map(f64::abs))
            .fold(MINIMUM_OFFSET_DEG, f64::max);
        let size = format!("{:.6}deg", 3.0 * farthest);
        Ok(Self {
            refdir: centred.centre,
            specrefval,
            width,
            nchan: comp_nchan,
            specrefpix: 0.5 * (comp_nchan as f64 - 1.0),
            stokes: "I".to_string(),
            size: [size.clone(), size],
            bandwidth,
        })
    }

    /// The `.compskymodel` image's size in pixels for `cell`:
    /// `int(size / cell)`, both in arcsec.
    ///
    /// # Errors
    ///
    /// A size or cell that is not an angle.
    pub fn image_size(&self, cell: &Quantity) -> Result<usize, SkyModelError> {
        let size = qa::value_in(&qa::read(&self.size[0])?, "arcsec")?;
        let cell = qa::value_in(cell, "arcsec")?;
        Ok((size / cell).trunc() as usize)
    }

    /// Write the `.compskymodel` at `path`, replacing what is there: a
    /// square image of [`Self::image_size`] pixels of `cell` centred on
    /// [`Self::refdir`], one channel and Stokes I, in Jy/pixel, with the
    /// components of `list` added; then its flat image, `<path>.flat`, whose
    /// path is returned.
    ///
    /// The direction is set as `simobserve` sets it, from `qa.tos` of the
    /// split direction (nine decimals) read back; the spectral axis is
    /// LSRK at [`Self::specrefval`], channel [`Self::specrefpix`], with the
    /// whole bandwidth as its increment.
    ///
    /// # Errors
    ///
    /// Those of [`add_components`] and [`flat_image`], and values the
    /// `simutil` helpers cannot convert.
    pub fn write(
        &self,
        list: &ComponentList,
        cell: &[Quantity; 2],
        path: &Path,
    ) -> Result<PathBuf, SkyModelError> {
        let size = self.image_size(&cell[0])?;
        let split = direction::split_direction(&self.refdir)?;
        let refer = split
            .epoch
            .trim()
            .parse()
            .unwrap_or(casa_types::measures::direction::DirectionRef::J2000);
        let reread = |value: &Quantity| -> Result<f64, SimutilError> {
            qa::value_in(&qa::read(&tos(value, 9))?, "rad")
        };
        let crval = [reread(&split.longitude)?, reread(&split.latitude)?];
        let cdelt = [
            -qa::value_in(&cell[0], "rad")?,
            qa::value_in(&cell[1], "rad")?,
        ];
        let centre = size as f64 / 2.0;
        let hz = |value: &Quantity| qa::value_in(value, "Hz");
        let bandwidth = hz(&qa::read(&self.bandwidth)?)?;
        check_increments(&[cdelt[0], cdelt[1], bandwidth])?;
        let mut coordinates = CoordinateSystem::new().with_obs_info(shape_obs_info());
        coordinates.add_coordinate(DirectionCoordinate::new(
            refer,
            Projection::with_parameters(ProjectionType::SIN, vec![0.0, 0.0]),
            crval,
            cdelt,
            [centre, centre],
        ));
        coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            hz(&self.specrefval)?,
            bandwidth,
            self.specrefpix,
            hz(&self.specrefval)?,
        ));
        write_image(path, &zeros(&[size, size, 1, 1]), coordinates, "Jy/pixel")?;
        add_components(path, list)?;
        flat_image(path, None)
    }
}

/// casacore's unit `name`, which is known.
fn unit(name: &str) -> casa_types::quanta::Unit {
    casacore_unit(name).expect("a unit casacore knows")
}
