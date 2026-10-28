// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's `startmodel` (#841 Phase 6b): the image the model starts from
//! (`SIImageStore::setModelImageOne`). As CASA takes it:
//! - it is in Jy/pixel (`SynthesisImagerVi2::defineImage` refuses others);
//! - its masked pixels are zero;
//! - it is regridded linearly onto the main image's grid and scaled by the
//!   ratio of the pixel areas, so its flux is kept (`regridToModelImage`:
//!   `ImageRegrid` with `Interpolate2D::LINEAR`). CASA copies a start model
//!   already on that grid; casa-rs regrids it too, which changes it only by
//!   rounding;
//! - its Stokes and spectral axes are the image's.
//!
//! It is the model's first generation, so the first major cycle subtracts
//! its prediction and the restored image includes it. Compile reads its
//! header only; its pixels are read and regridded once, when the initial
//! pass seeds the model ([`StartModel::main_samples`]).

use std::path::{Path, PathBuf};

use casa_images::{
    AnyPagedImage, ImageError, ImageInterface, ImagePixel, RegridMethod, RegridTemplate, TempImage,
    TempStoragePolicy, check_regrid, regrid,
};
use ndarray::{ArrayD, Zip};

use super::PrepareError;
use super::domains::PreparedImageDomain;

/// A start model checked against the main image domain.
#[derive(Clone, Debug)]
pub(crate) struct StartModel {
    path: PathBuf,
    input_shape: Vec<usize>,
    template: RegridTemplate,
}

impl StartModel {
    /// Check the image at `path` as the start model of `main`, an image of
    /// `polarizations` Stokes planes and `planes` spectral planes.
    pub(super) fn prepare(
        path: &Path,
        main: &PreparedImageDomain,
        polarizations: usize,
        planes: usize,
    ) -> Result<Self, PrepareError> {
        let [nx, ny] = main.image_size;
        Self::onto(
            path,
            RegridTemplate {
                coordinates: main.coordinates.clone(),
                shape: vec![nx, ny, polarizations, planes],
            },
        )
    }

    /// Check the image at `path` as a start model of `template`'s image.
    pub(crate) fn onto(path: &Path, template: RegridTemplate) -> Result<Self, PrepareError> {
        let refuse = |reason: String| PrepareError::StartModel {
            path: path.to_path_buf(),
            reason,
        };
        let opened = AnyPagedImage::open(path).map_err(|error| refuse(error.to_string()))?;
        let (units, coordinates, shape) = match &opened {
            AnyPagedImage::Float32(image) => header(image),
            AnyPagedImage::Float64(image) => header(image),
            AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
                return Err(refuse("a start model is a real image".into()));
            }
        };
        if units != "Jy/pixel" {
            return Err(refuse(format!(
                "its unit is {units:?}; a start model is in Jy/pixel, as CASA's tclean requires"
            )));
        }
        check_regrid(&coordinates, &shape, &template).map_err(|error| {
            refuse(format!(
                "{error}; its Stokes and spectral axes must be the image's"
            ))
        })?;
        Ok(Self {
            path: path.to_path_buf(),
            input_shape: shape,
            template,
        })
    }

    /// Bytes [`Self::main_samples`] holds at most besides the model: the
    /// image read (up to 8 bytes a pixel) with its mask, its copy with the
    /// masked pixels zero, `regrid`'s own read of that copy, the regridded
    /// image with its mask and the samples copied out of it, and `regrid`'s
    /// planes: an input plane of `f64` with its mask, and an output plane of
    /// `f64`, a flag and an input position each. The direction axes are the
    /// first two of both images, as `check_regrid` requires.
    pub(crate) fn seeding_bytes(&self) -> u64 {
        let input: usize = self.input_shape.iter().product();
        let output: usize = self.template.shape.iter().product();
        let input_plane = self.input_shape[0] * self.input_shape[1];
        let output_plane = self.template.shape[0] * self.template.shape[1];
        let position = size_of::<Option<[f64; 2]>>();
        (input * (8 + 1 + 4 + 4)
            + output * (4 + 1 + 4)
            + input_plane * (8 + 1)
            + output_plane * (8 + 1 + position)) as u64
    }

    /// The main domain's model samples in the model's canonical order
    /// (spectral plane, polarization, y, x, x fastest): the regridded
    /// image's Fortran order.
    pub(crate) fn main_samples(&self) -> Result<Vec<f32>, ImageError> {
        let zeroed = match AnyPagedImage::open(&self.path)? {
            AnyPagedImage::Float32(image) => zeroed_copy(&image)?,
            AnyPagedImage::Float64(image) => zeroed_copy(&image)?,
            AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
                return Err(ImageError::InvalidMetadata(
                    "a start model is a real image".into(),
                ));
            }
        };
        let regridded = regrid(&zeroed, &self.template, RegridMethod::Linear)?;
        Ok(regridded
            .data
            .as_slice_memory_order()
            .expect("regrid writes a contiguous Fortran-order image")
            .to_vec())
    }
}

/// The unit, coordinates and shape [`StartModel::prepare`] reads.
fn header<T: ImagePixel>(
    image: &casa_images::PagedImage<T>,
) -> (String, casa_coordinates::CoordinateSystem, Vec<usize>) {
    (
        image.units().to_string(),
        image.coordinates().clone(),
        image.shape().to_vec(),
    )
}

/// `image` in memory, in single precision, with its masked pixels zero and
/// no mask, as `setModelImageOne` copies it before regridding.
fn zeroed_copy<T>(image: &casa_images::PagedImage<T>) -> Result<TempImage<f32>, ImageError>
where
    T: ImagePixel + Into<f64> + Copy,
{
    let mut values: ArrayD<f32> = image.get()?.mapv(|value| Into::<f64>::into(value) as f32);
    if let Some(mask) = image.default_mask()? {
        Zip::from(&mut values).and(&mask).for_each(|value, &valid| {
            if !valid {
                *value = 0.0;
            }
        });
    }
    let mut copy = TempImage::<f32>::new(
        image.shape().to_vec(),
        image.coordinates().clone(),
        TempStoragePolicy::Memory,
    )?;
    copy.set_units(image.units())?;
    copy.put_slice(&values, &vec![0; values.ndim()])?;
    Ok(copy)
}
