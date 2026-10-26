// SPDX-License-Identifier: LGPL-3.0-or-later
//! The sky model `simobserve` observes (simulation Phase 5b, #841): CASA
//! 6.7.6's `simutil.modifymodel` and `simutil.flatimage`, `ia.modify`
//! adding a component list to an image, and the component-only model
//! `simobserve` makes when it has no image.
//!
//! | CASA | casa-rs |
//! |---|---|
//! | `simutil.modifymodel` | [`modify_model`] |
//! | `simutil.flatimage` | [`flat_image`] |
//! | `ia.modify(cl.torecord(), subtract=False)` (`ComponentListImage`) | [`add_components`] |
//! | `simobserve`'s `.compskymodel` | [`ComponentModel`] |
//!
//! The images written are what CASA writes: their shape, brightness unit,
//! coordinate values, Stokes and spectral axes, and pixels. Their
//! coordinate systems carry CASA's `ia.fromshape` observation information
//! (ALMA, "Karl Jansky", 2000-01-01), and the spectral axis is LSRK with no
//! conversion layer. The tests compare a grid of CASA's outputs
//! (`tests/data/skymodel_casa.json.gz`, from
//! `tools/simulation/skymodel_oracle.py`): strings exactly, coordinate values
//! to 1e-12, pixels to 1e-6 of the peak.
//!
//! **Where casa-rs differs**, each recorded in `docs/CASA (C++) bugs.md`:
//! - **Axis order.** `modifymodel` reorders the axes by swapping each into
//!   place, which leaves a cyclic order (spectral axis first) in the wrong
//!   order, and CASA then raises "internal error: I'm confused about the
//!   shape if your model data cube"; casa-rs permutes the axes.
//! - **Stokes labels.** CASA's new image takes its Stokes axis from
//!   `ia.fromshape`, which labels planes by their count (I, IQ, IQU, IQUV):
//!   an I,V model comes out labelled I,Q, and three unlabelled planes come
//!   out IQU though `modifymodel` reports and warns IQV. casa-rs keeps the
//!   model's labels, and labels unlabelled planes as it reports them.
//! - **A cell per axis.** `modifymodel` takes `incell` as a list of the two
//!   axes' cells, but under Python 3 indexes the `map` it makes of them and
//!   raises `TypeError`; casa-rs takes the two cells, as CASA's Python 2
//!   code did.
//! - **An unset `compwidth`.** The component-only model's spectral
//!   increment is `compwidth` as given; with none, CASA's
//!   `coordsys.setincrement` puts a meaningless value there (the model's
//!   right ascension, in radians, as hertz). casa-rs uses the 2 GHz
//!   `simobserve` observes with.
//! - **Frequency frames.** A component whose spectrum is in another frame
//!   than the image's is sampled at the image's frequencies taken in the
//!   spectrum's frame ([`casa_components`]' sampling).

mod add;
mod component_model;
mod flat;
mod modify;

pub use add::add_components;
pub use component_model::ComponentModel;
pub use flat::flat_image;
pub use modify::{ModelOverrides, ModelSummary, ModelValue, modify_model};

use std::path::{Path, PathBuf};

use casa_components::{ComponentError, ComponentListError};
use casa_coordinates::{
    CoordinateError, CoordinateSystem, CoordinateType, ObsInfo, Projection, ProjectionType,
};
use casa_images::{AnyPagedImage, ImageError, PagedImage};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use thiserror::Error;

use super::SimutilError;

/// What the sky-model functions refuse.
#[derive(Debug, Error)]
pub enum SkyModelError {
    /// A refusal CASA makes with its own message (`simutil.msg` at error
    /// priority, which raises).
    #[error("{0}")]
    Model(String),
    /// More than one `indirection`, for which CASA logs the message and
    /// `modifymodel` returns `False`.
    #[error("error parsing indirection {0} -- should be a single direction string")]
    Indirections(String),
    /// A quantity or direction the `simutil` helpers refuse.
    #[error(transparent)]
    Simutil(#[from] SimutilError),
    /// An image that cannot be read or written.
    #[error(transparent)]
    Image(#[from] ImageError),
    /// A coordinate system casa-coordinates refuses.
    #[error(transparent)]
    Coordinates(#[from] CoordinateError),
    /// A component casa-components does not sample.
    #[error(transparent)]
    Component(#[from] ComponentError),
    /// A component list that cannot be read.
    #[error(transparent)]
    ComponentList(#[from] ComponentListError),
    /// A file-system operation that failed.
    #[error("{path}: {source}")]
    Io {
        /// The path.
        path: PathBuf,
        /// The failure.
        source: std::io::Error,
    },
}

/// An image as `ia.newimagefromfile` opens it and `ia.getchunk` reads it:
/// its pixels in double precision and its coordinate system.
pub(crate) struct InputImage {
    pub(crate) pixels: ArrayD<f64>,
    pub(crate) coordinates: CoordinateSystem,
}

impl InputImage {
    /// Read the CASA image (a directory) or FITS file (a file) at `path`.
    pub(crate) fn read(path: &Path) -> Result<Self, SkyModelError> {
        if path.is_file() {
            let fits = casa_images::read_fits_image(path)?;
            return Ok(Self {
                pixels: fits.pixels.mapv(f64::from),
                coordinates: fits.coordinates,
            });
        }
        match AnyPagedImage::open(path)? {
            AnyPagedImage::Float32(image) => Ok(Self {
                pixels: image.get()?.mapv(f64::from),
                coordinates: image.coordinates().clone(),
            }),
            AnyPagedImage::Float64(image) => Ok(Self {
                pixels: image.get()?,
                coordinates: image.coordinates().clone(),
            }),
            AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(SkyModelError::Model(
                format!("{}: a complex image is not a sky model", path.display()),
            )),
        }
    }
}

/// The coordinate and pixel axes of the first coordinate of `kind`.
pub(crate) fn pixel_axes(
    coordinates: &CoordinateSystem,
    kind: CoordinateType,
) -> Option<(usize, Vec<usize>)> {
    let mut first = 0;
    for index in 0..coordinates.n_coordinates() {
        let coordinate = coordinates.coordinate(index);
        let count = coordinate.n_pixel_axes();
        if coordinate.coordinate_type() == kind {
            return Some((index, (first..first + count).collect()));
        }
        first += count;
    }
    None
}

/// The observation information of an image CASA makes from a shape
/// (`CoordinateUtil::makeCoordinateSystem`): ALMA, observed by "Karl
/// Jansky" at 2000-01-01T00:00:00.0001 UTC.
pub(crate) fn shape_obs_info() -> ObsInfo {
    ObsInfo::new("ALMA")
        .with_observer("Karl Jansky")
        .with_date(MEpoch::from_mjd(
            51_544.0 + 1.0e-4 / 86_400.0,
            EpochRef::UTC,
        ))
}

/// `projection` as casacore holds it: a SIN projection given no parameters
/// has its two, zero (`Projection(Projection::SIN)`).
pub(crate) fn casacore_projection(projection: &Projection) -> Projection {
    if projection.projection_type() == ProjectionType::SIN && projection.parameters().is_empty() {
        Projection::with_parameters(ProjectionType::SIN, vec![0.0, 0.0])
    } else {
        projection.clone()
    }
}

/// Remove whatever is at `path`, as `ia.fromshape(..., overwrite=True)`
/// replaces it.
pub(crate) fn remove_existing(path: &Path) -> Result<(), SkyModelError> {
    let removed = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else if path.exists() {
        std::fs::remove_file(path)
    } else {
        return Ok(());
    };
    removed.map_err(|source| SkyModelError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Write `pixels` as a new single-precision CASA image at `path`, replacing
/// what is there.
pub(crate) fn write_image(
    path: &Path,
    pixels: &ArrayD<f32>,
    coordinates: CoordinateSystem,
    units: &str,
) -> Result<(), SkyModelError> {
    remove_existing(path)?;
    let shape = pixels.shape().to_vec();
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path)?;
    image.put_slice(pixels, &vec![0; shape.len()])?;
    image.set_units(units)?;
    image.save()?;
    Ok(())
}

/// A zeroed array of `shape` in Fortran order, as images hold their pixels.
pub(crate) fn zeros(shape: &[usize]) -> ArrayD<f32> {
    ArrayD::zeros(IxDyn(shape).f())
}
