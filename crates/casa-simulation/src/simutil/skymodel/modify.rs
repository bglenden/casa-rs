// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simutil.modifymodel`: a model image fitted into the four axes
//! `simobserve` predicts from, `[direction, direction, Stokes, spectral]`,
//! with its brightness, direction, cell, centre frequency and channel width
//! overridden as asked.
//!
//! The model is read whole, as CASA reads it (`ia.getchunk`), from a CASA
//! image or a FITS file. Its pixel axes are found by their coordinates: the
//! direction's two, the spectral and the Stokes axis. An image of two or
//! three axes gains degenerate ones; a missing spectral or Stokes axis is
//! the first axis no coordinate claims. The new image is LSRK, in
//! Jy/pixel, with the model's NaNs as zeros and its pixels scaled to
//! `inbright` at the peak.
//!
//! Its direction is the model's at pixel `(nx/2, ny/2)`, written as
//! `simutil` writes directions (precision 5) and read back, or
//! `indirection`, with the reference pixel at `(nx/2, ny/2)` and the
//! model's projection (SIN for `indirection`). Its cell is the model's
//! increment through its linear transform, or `incell`; its spectral axis
//! the model's, or `incenter` at channel `⌊nchan/2⌋` (also the rest
//! frequency) and `inwidth`.

use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate, Projection,
    ProjectionType, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::quanta::Quantity;
use ndarray::{ArrayD, Axis, IxDyn, ShapeBuilder};

use super::{
    InputImage, SkyModelError, casacore_projection, pixel_axes, shape_obs_info, write_image,
};
use crate::simutil::{SimutilError, direction, python_float, qa};

/// `modifymodel`'s overrides, CASA's strings as `simobserve` passes them.
/// An empty string keeps the model's own value, as `"unchanged"` does for
/// `inbright`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelOverrides {
    /// The peak brightness the model is scaled to, read as Jy/pixel
    /// (`inbright`).
    pub inbright: String,
    /// The direction of the model's centre (`indirection`, one string or a
    /// list of one).
    pub indirection: Vec<String>,
    /// The cell (`incell`): one cell for both axes, the longitude's made
    /// negative, or the two axes' cells as given.
    pub incell: Vec<String>,
    /// The frequency of the model's centre channel (`incenter`).
    pub incenter: String,
    /// The channel width (`inwidth`).
    pub inwidth: String,
}

/// A value `modifymodel` returns either as the override given or as the
/// model's own quantity.
#[derive(Debug, Clone, PartialEq)]
pub enum ModelValue {
    /// The override string, as CASA returns it.
    Given(String),
    /// The model's own value.
    Model(Quantity),
}

impl ModelValue {
    /// The value as a quantity (`qa.quantity` of it).
    ///
    /// # Errors
    ///
    /// [`SimutilError::Quantity`] for an override casacore cannot read.
    pub fn quantity(&self) -> Result<Quantity, SimutilError> {
        match self {
            Self::Given(text) => qa::read(text),
            Self::Model(value) => Ok(value.clone()),
        }
    }
}

/// What `modifymodel` returns, in its order.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelSummary {
    /// The direction of the reference pixel, as `simutil` writes it.
    pub refdir: String,
    /// The cell, both axes positive.
    pub cell: [Quantity; 2],
    /// The image's size, pixels times cell.
    pub size: [Quantity; 2],
    /// The number of channels.
    pub nchan: usize,
    /// The frequency of the reference channel.
    pub specrefval: ModelValue,
    /// The reference channel.
    pub specrefpix: f64,
    /// The channel width.
    pub width: ModelValue,
    /// The Stokes planes' names, `"I"` to `"IQUV"`.
    pub stokes: String,
}

/// The new image's role for each of the model's four axes, and the axis
/// holding each role (`modifymodel`'s `axassigned` and `axmap`): 0 and 1
/// the direction's, 2 Stokes, 3 spectral.
#[derive(Debug, Default)]
struct AxisMap {
    axis_of: [Option<usize>; 4],
    role_of: [Option<usize>; 4],
}

impl AxisMap {
    fn assign(&mut self, role: usize, axis: usize) {
        self.axis_of[role] = Some(axis);
        self.role_of[axis] = Some(role);
    }

    /// The first axis no coordinate claims, for `role`.
    fn assign_unused(&mut self, role: usize, what: &str) -> Result<usize, SkyModelError> {
        let Some(axis) = self.role_of.iter().position(Option::is_none) else {
            let roles = self
                .role_of
                .map(|role| role.map_or(-1, |role| role as i64))
                .map(|role| role.to_string())
                .join(" ");
            return Err(SkyModelError::Model(format!(
                "I can't find an unused axis to make {what} [{roles}] "
            )));
        };
        self.assign(role, axis);
        Ok(axis)
    }

    fn axis(&self, role: usize) -> usize {
        self.axis_of[role].expect("every role is assigned before the image is built")
    }
}

/// The model's direction, cell and projection.
struct ModelDirection {
    refdir: String,
    projection: Projection,
    refpix: [f64; 2],
}

/// `simutil.modifymodel(inimage, outimage, inbright, indirection, incell,
/// incenter, inwidth, innchan, flatimage=False)`: write the model at
/// `input` (a CASA image or a FITS file) as a new image at `output`
/// (replacing what is there) and return what it holds (see the module
/// documentation). `innchan` is unused in CASA.
///
/// # Errors
///
/// [`SkyModelError::Model`] with CASA's message where CASA raises, and
/// [`SkyModelError::Indirections`] where it returns `False`; a model that
/// cannot be read or written, or `output` the same as `input` (CASA then
/// only reports on the model).
pub fn modify_model(
    input: &Path,
    output: &Path,
    overrides: &ModelOverrides,
) -> Result<ModelSummary, SkyModelError> {
    if input == output {
        return Err(SkyModelError::Model(format!(
            "modifymodel writes a new image: {} is both the input and the output",
            input.display()
        )));
    }
    let model = InputImage::read(input)?;
    let scale = brightness_scale(&overrides.inbright, &model.pixels)?;
    let pixels = four_axes(model.pixels)?;
    let shape: [usize; 4] = std::array::from_fn(|axis| pixels.shape()[axis]);
    let incell = cell_override(&overrides.incell)?;
    let coordinates = &model.coordinates;
    let mut axes = AxisMap::default();
    let (direction, image_cell) = image_direction(coordinates, &shape, &mut axes)?;
    let direction = direction_override(&overrides.indirection, direction, &shape, &axes)?;
    let cell = match incell {
        Some(cell) if qa::conforms(&cell[0], "arcsec") => cell,
        _ => match image_cell {
            Some(cell) if qa::conforms(&cell[0], "arcsec") => cell,
            _ => {
                return Err(SkyModelError::Model(
                    "Unable to determine model cell size.  Valid 'incell' parameter must be provided."
                        .to_string(),
                ));
            }
        },
    };
    let spectral = spectral_axis(coordinates, &shape, &mut axes, overrides)?;
    let (stokes, stokes_types) = stokes_axis(coordinates, &shape, &mut axes)?;
    let model_shape = [
        shape[axes.axis(0)],
        shape[axes.axis(1)],
        stokes_types.len(),
        spectral.nchan,
    ];
    let new_coordinates = model_coordinates(&direction, &cell, &spectral, stokes_types)?;
    // The model's axes in the new image's order, NaN as zero, scaled.
    let order: Vec<usize> = (0..4).map(|role| axes.axis(role)).collect();
    let arranged = pixels.permuted_axes(IxDyn(&order));
    let mut written = ArrayD::<f32>::zeros(IxDyn(&model_shape).f());
    written.zip_mut_with(&arranged, |out, &value| {
        let value = if value.is_nan() { 0.0 } else { value };
        *out = (value * scale) as f32;
    });
    write_image(output, &written, new_coordinates, "Jy/pixel")?;
    let cell = cell.map(|cell| Quantity::with_unit(cell.value().abs(), cell.unit().clone()));
    let size = [0, 1].map(|axis| {
        qa::mul(&qa::number(model_shape[axis] as f64), &cell[axis]).expect("a number times a cell")
    });
    Ok(ModelSummary {
        refdir: direction.refdir,
        cell,
        size,
        nchan: spectral.nchan,
        specrefval: spectral.refval,
        specrefpix: spectral.refpix,
        width: spectral.width,
        stokes,
    })
}

/// `scalefactor`: `inbright` (read as Jy/pixel when it has a unit) over
/// the model's peak, ignoring NaNs; 1 when unset or `"unchanged"`.
fn brightness_scale(inbright: &str, pixels: &ArrayD<f64>) -> Result<f64, SkyModelError> {
    if inbright.is_empty() || inbright == "unchanged" {
        return Ok(1.0);
    }
    let brightness = if qa::is_quantity(inbright) {
        let value = qa::read(inbright)?;
        if value.unit().name().is_empty() {
            value.value()
        } else {
            // casacore reads `pixel` but not every brightness unit; CASA
            // then takes the number.
            qa::value_in(&value, "Jy/pixel").unwrap_or_else(|_| {
                tracing::warn!(
                    "assuming inbright={inbright} means {} Jy/pixel",
                    python_float(value.value())
                );
                value.value()
            })
        }
    } else {
        inbright.trim().parse::<f64>().map_err(|_| {
            SkyModelError::Model(format!("could not convert string to float: '{inbright}'"))
        })?
    };
    let peak = pixels
        .iter()
        .filter(|value| !value.is_nan())
        .fold(f64::NEG_INFINITY, |peak, value| peak.max(*value));
    let peak = if peak == f64::NEG_INFINITY {
        f64::NAN
    } else {
        peak
    };
    Ok(brightness / peak)
}

/// The model with degenerate axes added to make four.
fn four_axes(pixels: ArrayD<f64>) -> Result<ArrayD<f64>, SkyModelError> {
    match pixels.ndim() {
        0 | 1 => Err(SkyModelError::Model(
            "Your input model has fewer than 2 dimensions.  Can't proceed".to_string(),
        )),
        2..=4 => {
            let mut pixels = pixels;
            while pixels.ndim() < 4 {
                pixels.insert_axis_inplace(Axis(pixels.ndim()));
            }
            Ok(pixels)
        }
        _ => Err(SkyModelError::Model(
            "model image has more than 4 dimensions.  Not sure how to proceed".to_string(),
        )),
    }
}

/// `incell` as `modifymodel` makes it: one cell converted to arcsec, made
/// positive, for `[−cell, cell]`; or two cells converted to arcsec as given.
/// `None` when unset. A value that is not an angle is kept, and later
/// passed over, as casacore's conversion keeps it.
fn cell_override(incell: &[String]) -> Result<Option<[Quantity; 2]>, SkyModelError> {
    let read = |text: &str| -> Result<Quantity, SkyModelError> {
        let value = if text.is_empty() {
            qa::number(0.0)
        } else {
            qa::read(text)?
        };
        Ok(qa::convert(&value, "arcsec")?)
    };
    match incell {
        [] => Ok(None),
        [cell] => {
            let cell = read(cell)?;
            let cell = Quantity::with_unit(cell.value().abs(), cell.unit().clone());
            let negative = qa::mul(&cell, &qa::number(-1.0))?;
            Ok(Some([negative, cell]))
        }
        [x, y] => Ok(Some([read(x)?, read(y)?])),
        _ => Err(SkyModelError::Model(format!(
            "incell takes one cell or two, got {}",
            incell.len()
        ))),
    }
}

/// The model's direction axes, its direction at pixel `(nx/2, ny/2)` and
/// its cell; the first two axes when it has no direction coordinate.
fn image_direction(
    coordinates: &CoordinateSystem,
    shape: &[usize; 4],
    axes: &mut AxisMap,
) -> Result<(Option<ModelDirection>, Option<[Quantity; 2]>), SkyModelError> {
    let Some((index, pixel_axes)) = pixel_axes(coordinates, CoordinateType::Direction) else {
        axes.assign(0, 0);
        axes.assign(1, 1);
        return Ok((None, None));
    };
    let CoordinateModel::Direction(coordinate) = coordinates.coordinate(index) else {
        unreachable!("the coordinate found is a direction coordinate");
    };
    axes.assign(0, pixel_axes[0]);
    axes.assign(1, pixel_axes[1]);
    let refpix = [
        0.5 * shape[pixel_axes[0]] as f64,
        0.5 * shape[pixel_axes[1]] as f64,
    ];
    let world = coordinate.to_world(&refpix)?;
    let radians = |value: f64| Quantity::with_unit(value, unit("rad"));
    let refdir = format!(
        "{} {} {}",
        coordinate.direction_ref().as_str(),
        qa::hms(&radians(world[0]))?,
        qa::dms(&radians(world[1]))?
    );
    let pc = coordinate.pc_matrix();
    if pc[[0, 1]].abs().max(pc[[1, 0]].abs()) > 1e-4 {
        return Err(SkyModelError::Model(
            "Your image is rotated with respect to Lat/Lon.  I can't cope with that yet"
                .to_string(),
        ));
    }
    let increment = coordinate.increment();
    let cell = [0, 1].map(|axis| {
        qa::mul(&radians(increment[axis]), &qa::number(pc[[axis, axis]]))
            .and_then(|cell| qa::convert(&cell, "arcsec"))
    });
    let [x, y] = cell;
    Ok((
        Some(ModelDirection {
            refdir,
            projection: casacore_projection(coordinate.projection()),
            refpix,
        }),
        Some([x?, y?]),
    ))
}

/// The direction `indirection` sets, with a SIN projection, or the model's.
fn direction_override(
    indirection: &[String],
    model: Option<ModelDirection>,
    shape: &[usize; 4],
    axes: &AxisMap,
) -> Result<ModelDirection, SkyModelError> {
    let indirection = match indirection {
        [] => "",
        [one] => one.as_str(),
        many => {
            let listed = many
                .iter()
                .map(|direction| format!("'{direction}'"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(SkyModelError::Indirections(format!("[{listed}]")));
        }
    };
    if direction::is_direction(indirection) {
        let split = direction::split_direction(indirection)?;
        return Ok(ModelDirection {
            refdir: format!(
                "{}{} {}",
                split.epoch,
                qa::hms(&split.longitude)?,
                qa::dms(&split.latitude)?
            ),
            projection: Projection::with_parameters(ProjectionType::SIN, vec![0.0, 0.0]),
            refpix: [
                0.5 * shape[axes.axis(0)] as f64,
                0.5 * shape[axes.axis(1)] as f64,
            ],
        });
    }
    match model {
        Some(model) if direction::is_direction(&model.refdir) => Ok(model),
        _ => Err(SkyModelError::Model(
            "Cannot determine reference direction in model image.  Valid 'indirection' parameter must be provided."
                .to_string(),
        )),
    }
}

/// The new image's spectral axis.
struct ModelSpectrum {
    nchan: usize,
    refval: ModelValue,
    refpix: f64,
    width: ModelValue,
    rest_frequency: ModelValue,
}

/// The model's spectral axis, or the first unclaimed axis, with `incenter`
/// and `inwidth` applied where they are frequencies and not negative.
fn spectral_axis(
    coordinates: &CoordinateSystem,
    shape: &[usize; 4],
    axes: &mut AxisMap,
    overrides: &ModelOverrides,
) -> Result<ModelSpectrum, SkyModelError> {
    let mut spectrum = match pixel_axes(coordinates, CoordinateType::Spectral) {
        Some((index, pixel_axes)) => {
            let CoordinateModel::Spectral(coordinate) = coordinates.coordinate(index) else {
                unreachable!("the coordinate found is a spectral coordinate");
            };
            axes.assign(3, pixel_axes[0]);
            let axis_unit = unit(&coordinate.axis_units()[0]);
            let in_unit = |value: f64| Quantity::with_unit(value, axis_unit.clone());
            ModelSpectrum {
                nchan: shape[pixel_axes[0]],
                refval: ModelValue::Model(in_unit(coordinate.reference_value()[0])),
                refpix: coordinate.reference_pixel()[0],
                width: ModelValue::Model(in_unit(coordinate.increment()[0])),
                rest_frequency: ModelValue::Model(Quantity::with_unit(
                    coordinate.rest_frequency(),
                    unit("Hz"),
                )),
            }
        }
        None => {
            let axis = axes.assign_unused(3, "Spectral")?;
            let unset = ModelValue::Given(String::new());
            ModelSpectrum {
                nchan: shape[axis],
                refval: unset.clone(),
                refpix: 0.0,
                width: unset.clone(),
                rest_frequency: unset,
            }
        }
    };
    let frequency = |text: &str| {
        qa::is_quantity(text)
            && qa::read(text).is_ok_and(|value| qa::conforms(&value, "Hz") && value.value() >= 0.0)
    };
    let valid = |value: &ModelValue| match value {
        ModelValue::Model(value) => qa::conforms(value, "Hz"),
        ModelValue::Given(text) => qa::is_quantity(text) && frequency(text),
    };
    if frequency(&overrides.incenter) {
        spectrum.refval = ModelValue::Given(overrides.incenter.clone());
        spectrum.refpix = (spectrum.nchan as f64 * 0.5).floor();
        spectrum.rest_frequency = ModelValue::Given(overrides.incenter.clone());
    } else if !valid(&spectrum.refval) {
        return Err(SkyModelError::Model(
            "Unable to determine model frequency.  Valid 'incenter' parameter must be provided."
                .to_string(),
        ));
    }
    if frequency(&overrides.inwidth) {
        spectrum.width = ModelValue::Given(overrides.inwidth.clone());
    } else if !valid(&spectrum.width) {
        return Err(SkyModelError::Model(
            "Unable to determine model channel or bandwidth.  Valid 'inwidth' parameter must be provided."
                .to_string(),
        ));
    }
    Ok(spectrum)
}

/// The model's Stokes axis and its planes, or the first unclaimed axis,
/// labelled by its length: `IQUV`, `IQV`, `IQ` or `I`.
fn stokes_axis(
    coordinates: &CoordinateSystem,
    shape: &[usize; 4],
    axes: &mut AxisMap,
) -> Result<(String, Vec<StokesType>), SkyModelError> {
    if let Some((index, pixel_axes)) = pixel_axes(coordinates, CoordinateType::Stokes) {
        let CoordinateModel::Stokes(coordinate) = coordinates.coordinate(index) else {
            unreachable!("the coordinate found is a Stokes coordinate");
        };
        axes.assign(2, pixel_axes[0]);
        if shape[pixel_axes[0]] > 4 {
            return Err(SkyModelError::Model(
                "you appear to have more than 4 Stokes components - please edit your header and/or parameters"
                    .to_string(),
            ));
        }
        let types = coordinate.stokes().to_vec();
        let names = types.iter().map(|stokes| stokes.name()).collect();
        return Ok((names, types));
    }
    let axis = axes.assign_unused(2, "Stokes")?;
    let planes = shape[axis];
    if planes > 4 {
        return Err(SkyModelError::Model(format!(
            "you have {planes} Stokes parameters in your potential Stokes axis {axis}.  something is wrong."
        )));
    }
    let names = match planes {
        4 => "IQUV",
        3 => "IQV",
        2 => "IQ",
        _ => "I",
    };
    if planes == 2 || planes == 3 {
        tracing::warn!(
            "setting {names} Stokes parameters from the 4th axis of you model.  If that's not what you want, then edit the header"
        );
    }
    let types = names
        .chars()
        .map(|name| StokesType::from_name(&name.to_string()).expect("I, Q, U and V are Stokes"))
        .collect();
    Ok((names.to_string(), types))
}

/// The new image's coordinate system: the direction read back from its
/// string, the cell in radians, the Stokes planes, and an LSRK spectral
/// axis.
fn model_coordinates(
    direction: &ModelDirection,
    cell: &[Quantity; 2],
    spectrum: &ModelSpectrum,
    stokes: Vec<StokesType>,
) -> Result<CoordinateSystem, SkyModelError> {
    let (refer, crval) = direction::direction_measure(&direction.refdir)?;
    let cdelt = [
        qa::value_in(&cell[0], "rad")?,
        qa::value_in(&cell[1], "rad")?,
    ];
    let hz = |value: &ModelValue| -> Result<f64, SkyModelError> {
        Ok(qa::value_in(&value.quantity()?, "Hz")?)
    };
    let mut coordinates = CoordinateSystem::new().with_obs_info(shape_obs_info());
    coordinates.add_coordinate(DirectionCoordinate::new(
        refer,
        direction.projection.clone(),
        crval,
        cdelt,
        direction.refpix,
    ));
    coordinates.add_coordinate(StokesCoordinate::new(stokes));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        hz(&spectrum.refval)?,
        hz(&spectrum.width)?,
        spectrum.refpix,
        hz(&spectrum.rest_frequency)?,
    ));
    Ok(coordinates)
}

/// casacore's unit `name`.
fn unit(name: &str) -> casa_types::quanta::Unit {
    casa_types::quanta::casacore::casacore_unit(name)
        .unwrap_or_else(|_| casa_types::quanta::Unit::dimensionless())
}
