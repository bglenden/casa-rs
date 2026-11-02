// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simutil.flatimage`: a sky model collapsed to one Stokes I plane, the
//! `.flat` image `simobserve` draws and `simanalyze` compares against.
//!
//! CASA makes it in steps whose result is one plane, `[direction,
//! direction, Stokes I]`:
//! - more than one channel: the mean over the channels
//!   (`ia.moments(moments=[-1])`, which drops the spectral axis);
//! - one channel: the degenerate axes dropped (`dropdeg=True`) and, with
//!   one Stokes plane, a Stokes I axis added back (`ia.adddegaxes`);
//! - more than one Stokes plane: then the I plane (`po.stokesi`).
//!
//! casa-rs forms that plane directly: the mean of the I plane over the
//! channels, in double precision as CASA's moments sums, or the plane
//! itself for one channel. A masked sample does not count: the mean is over
//! the unmasked channels, and a pixel with none is masked (and zero), as
//! CASA's moments leaves it; with one channel, the plane's mask is kept. The
//! image keeps the model's direction coordinate, observation information,
//! brightness unit and restoring beam (the common beam of a beam per plane,
//! as casa-rs's `immoments` takes it). Components, when given, are then
//! added to it ([`add_components`]).

use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, StokesCoordinate, StokesType,
};
use casa_images::{ImageBeamSet, ImageInfo, PagedImage};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::{SkyModelError, add_components, pixel_axes, remove_existing, zeros};

/// `simutil.flatimage(image, complist)`: write `<image>.flat`, replacing
/// what is there, with `complist`'s components added when it is given, and
/// return its path.
///
/// # Errors
///
/// An image with no direction, spectral or Stokes axis, or no Stokes I
/// plane; a component list that is missing or cannot be read; or an image
/// that cannot be read or written.
pub fn flat_image(image_path: &Path, complist: Option<&Path>) -> Result<PathBuf, SkyModelError> {
    let image = PagedImage::<f32>::open(image_path)?;
    let coordinates = image.coordinates();
    let shape = image.shape().to_vec();
    let axis = |kind: CoordinateType, what: &str| {
        pixel_axes(coordinates, kind).ok_or_else(|| {
            SkyModelError::Model(format!(
                "{}: flatimage needs {what} axis",
                image_path.display()
            ))
        })
    };
    let (direction_index, direction_axes) = axis(CoordinateType::Direction, "a direction")?;
    let (_, spectral_axes) = axis(CoordinateType::Spectral, "a spectral")?;
    let (stokes_index, stokes_axes) = axis(CoordinateType::Stokes, "a Stokes")?;
    let CoordinateModel::Stokes(stokes) = coordinates.coordinate(stokes_index) else {
        unreachable!("the coordinate found is a Stokes coordinate");
    };
    let i_plane = if shape[stokes_axes[0]] > 1 {
        stokes
            .stokes()
            .iter()
            .position(|stokes| *stokes == StokesType::I)
            .ok_or_else(|| {
                SkyModelError::Model(format!(
                    "{}: no Stokes I plane to flatten",
                    image_path.display()
                ))
            })?
    } else {
        0
    };
    let pixels = image.get()?;
    let mask = image.get_mask()?;
    let valid = |position: &[usize]| mask.as_ref().is_none_or(|mask| mask[IxDyn(position)]);
    let [nx, ny] = [shape[direction_axes[0]], shape[direction_axes[1]]];
    let nchan = shape[spectral_axes[0]];
    let mut flat = zeros(&[nx, ny, 1]);
    let mut flat_mask = ArrayD::from_elem(IxDyn(&[nx, ny, 1]).f(), true);
    let mut position = vec![0; shape.len()];
    position[stokes_axes[0]] = i_plane;
    for y in 0..ny {
        for x in 0..nx {
            position[direction_axes[0]] = x;
            position[direction_axes[1]] = y;
            if nchan == 1 {
                flat[[x, y, 0]] = pixels[IxDyn(&position)];
                flat_mask[[x, y, 0]] = valid(&position);
                continue;
            }
            let (mut sum, mut count) = (0.0, 0_usize);
            for channel in 0..nchan {
                position[spectral_axes[0]] = channel;
                if valid(&position) {
                    sum += f64::from(pixels[IxDyn(&position)]);
                    count += 1;
                }
            }
            if count == 0 {
                flat_mask[[x, y, 0]] = false;
            } else {
                flat[[x, y, 0]] = (sum / count as f64) as f32;
            }
        }
    }
    let mut flat_coordinates =
        CoordinateSystem::new().with_obs_info(coordinates.obs_info().clone());
    let CoordinateModel::Direction(direction) = coordinates.coordinate(direction_index) else {
        unreachable!("the coordinate found is a direction coordinate");
    };
    flat_coordinates.add_coordinate(direction.clone());
    flat_coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    let units = image.units().to_string();
    let mut info = image.image_info()?;
    if info.beam_set.is_multi() {
        info.beam_set = ImageBeamSet::new(info.beam_set.common_beam()?);
    }
    drop(image);
    let flat_path = PathBuf::from(format!("{}.flat", image_path.display()));
    let masked = flat_mask.iter().any(|valid| !valid);
    write_flat(
        &flat_path,
        &flat,
        flat_coordinates,
        &units,
        &info,
        masked.then_some(&flat_mask),
    )?;
    if let Some(complist) = complist {
        if !complist.exists() {
            return Err(SkyModelError::Model(format!(
                "sky component list {} not found in flatimage",
                complist.display()
            )));
        }
        add_components(&flat_path, &ComponentList::read_table(complist)?)?;
    }
    Ok(flat_path)
}

/// Write the flat image, replacing what is at `path`, with the model's
/// image information and, when some pixels are masked, the mask as the
/// default `mask0`.
fn write_flat(
    path: &Path,
    pixels: &ArrayD<f32>,
    coordinates: CoordinateSystem,
    units: &str,
    info: &ImageInfo,
    mask: Option<&ArrayD<bool>>,
) -> Result<(), SkyModelError> {
    remove_existing(path)?;
    let shape = pixels.shape().to_vec();
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path)?;
    image.put_slice(pixels, &vec![0; shape.len()])?;
    image.set_units(units)?;
    image.set_image_info(info)?;
    if let Some(mask) = mask {
        image.put_mask("mask0", mask)?;
        image.set_default_mask("mask0")?;
    }
    image.save()?;
    Ok(())
}
