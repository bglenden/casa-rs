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
//! - its direction frame is the image's: casa-rs does not convert frames;
//! - its Stokes planes are copied by position, as CASA leaves that axis
//!   out of the regrid. Different labels produce a warning, not a refusal
//!   or a polarization conversion (owner decision, #980, 2026-10-31);
//! - a cube's start model has the image's spectral axis. An MFS image's has
//!   one plane, at any frequency: CASA's regrid replicates a degenerate
//!   spectral axis, and an MFS model holds one plane whatever frequency it
//!   is stated at.
//!
//! It is the model's first generation, so the first major cycle subtracts
//! its prediction and the restored image includes it. Compile reads its
//! header only; its pixels are read and regridded once, when the initial
//! pass seeds the model ([`StartModel::main_samples`]).

use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, StokesType};
use casa_images::{
    AnyPagedImage, ImageError, ImageInterface, ImagePixel, RegridMethod, RegridTemplate, TempImage,
    TempStoragePolicy, check_regrid, regrid,
};
use ndarray::{ArrayD, Zip};

use super::PrepareError;
use super::domains::PreparedImageDomain;

/// An explicit start model or same-grid restart model checked against the main domain.
#[derive(Clone, Debug)]
pub(crate) struct StartModel {
    path: PathBuf,
    input_shape: Vec<usize>,
    template: RegridTemplate,
    direct: bool,
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
    pub(crate) fn onto(path: &Path, mut template: RegridTemplate) -> Result<Self, PrepareError> {
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
        let (ours, theirs) = (stokes(&coordinates), stokes(&template.coordinates));
        // SIImageStore::regridToModelImage regrids only direction and
        // spectral axes. Match its positional Stokes copying even when
        // the labels differ. This template is private to seeding pixels;
        // published coordinates still come from the compiled image domain.
        if ours != theirs
            && let (Some(input), Some(output)) = (
                coordinates.find_coordinate(CoordinateType::Stokes),
                template.coordinates.find_coordinate(CoordinateType::Stokes),
            )
        {
            template
                .coordinates
                .replace_coordinate(output, coordinates.coordinate(input).clone());
        }
        check_regrid(&coordinates, &shape, &template).map_err(|error| {
            refuse(format!(
                "{error}; the non-regridded axis shapes and spectral coordinates must match"
            ))
        })?;
        if ours != theirs {
            tracing::warn!(startmodel = ?path, input_stokes = ?ours, image_stokes = ?theirs,
                "startmodel Stokes labels differ: copying planes by position as CASA does; no polarization conversion is performed");
        }
        Ok(Self {
            path: path.to_path_buf(),
            input_shape: shape,
            template,
            direct: false,
        })
    }

    /// Reuse an existing model on precisely the requested grid, without regridding.
    pub(super) fn restart(path: &Path, main: &PreparedImageDomain) -> Result<Self, PrepareError> {
        let template = RegridTemplate {
            coordinates: main.coordinates.clone(),
            shape: vec![main.image_size[0], main.image_size[1], 1, 1],
        };
        crate::restart::check_grid(path, &template)?;
        Ok(Self {
            path: path.into(),
            input_shape: template.shape.clone(),
            template,
            direct: true,
        })
    }

    /// Bytes [`Self::main_samples`] holds at most besides the model: the
    /// image read (up to 8 bytes a pixel) with its mask, its copy with the
    /// masked pixels zero, `regrid`'s own read of that copy, the regridded
    /// image with its mask and the samples copied out of it, and `regrid`'s
    /// planes: an input plane of `f64` with its mask, and an output plane of
    /// `f64`, a flag and an input position each. The direction axes are the
    /// first two of both images, as `check_regrid` requires. Direct restart
    /// holds only the input image and the emitted samples, within this charge.
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

    /// The image's pixels along x and y.
    pub(crate) fn image_size(&self) -> [usize; 2] {
        [self.template.shape[0], self.template.shape[1]]
    }

    /// The image's polarizations.
    pub(crate) fn polarizations(&self) -> usize {
        self.template.shape[2]
    }

    /// Bytes making the samples physical holds besides
    /// [`Self::seeding_bytes`]: the physical samples, and one plane's
    /// sensitivity read in `f64`.
    pub(crate) fn conversion_bytes(&self) -> u64 {
        let output: usize = self.template.shape.iter().product();
        let output_plane = self.template.shape[0] * self.template.shape[1];
        (output * 4 + output_plane * 8) as u64
    }

    /// The main domain's model samples in the model's canonical order
    /// (spectral plane, polarization, y, x, x fastest), either read directly
    /// for restart or regridded for an explicit start model.
    pub(crate) fn main_samples(&self) -> Result<Vec<f32>, ImageError> {
        if self.direct {
            let image = casa_images::PagedImage::<f32>::open(&self.path)?;
            // CASA's restart prediction uses StokesImageUtil::From's raw lattice
            // iterator; unlike explicit startmodel, it does not zero a pixel mask.
            let values = image.get()?;
            // Iterate logical axes in model order, independent of image storage order.
            return Ok(values.reversed_axes().iter().copied().collect());
        }
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

/// The Stokes of `coordinates`' Stokes coordinate, in plane order; `None`
/// without one.
fn stokes(coordinates: &CoordinateSystem) -> Option<Vec<StokesType>> {
    (0..coordinates.n_coordinates()).find_map(|index| match coordinates.coordinate(index) {
        CoordinateModel::Stokes(stokes) => Some(stokes.stokes().to_vec()),
        _ => None,
    })
}

/// The unit, coordinates and shape [`StartModel::prepare`] reads.
fn header<T: ImagePixel>(
    image: &casa_images::PagedImage<T>,
) -> (String, CoordinateSystem, Vec<usize>) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use casa_coordinates::{
        DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate, StokesCoordinate,
    };
    use casa_types::measures::{direction::DirectionRef, frequency::FrequencyRef};
    use ndarray::IxDyn;

    #[test]
    fn direct_restart_reads_raw_x_fastest_pixels_while_startmodel_zeros_the_mask() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("existing.model");
        let shape = vec![4, 3, 1, 1];
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.5],
            [-1e-5, 1e-5],
            [2.0, 1.0],
        ));
        coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1e9,
            1e6,
            0.0,
            1e9,
        ));
        let mut image =
            casa_images::PagedImage::<f32>::create(shape.clone(), coordinates.clone(), &path)
                .unwrap();
        image.set_units("Jy/pixel").unwrap();
        let pixels =
            ArrayD::from_shape_fn(IxDyn(&shape), |index| (index[0] + 10 * index[1]) as f32);
        image.put_slice(&pixels, &[0; 4]).unwrap();
        image.make_mask("external", true, false).unwrap();
        image.save().unwrap();
        drop(image);
        let template = RegridTemplate {
            coordinates,
            shape: shape.clone(),
        };
        let explicit = StartModel::onto(&path, template.clone()).unwrap();
        assert!(
            explicit
                .main_samples()
                .unwrap()
                .iter()
                .all(|&value| value == 0.0)
        );
        // The task gate still refuses masked-model continuation until publication
        // can preserve the mask; the seed reader must nevertheless match CASA.
        let direct = StartModel {
            path,
            input_shape: shape,
            template,
            direct: true,
        };
        assert_eq!(
            direct.main_samples().unwrap(),
            vec![
                0.0, 1.0, 2.0, 3.0, 10.0, 11.0, 12.0, 13.0, 20.0, 21.0, 22.0, 23.0
            ]
        );
    }
}
