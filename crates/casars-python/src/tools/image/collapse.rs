// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_images::{
    ImageInterface,
    analysis::{
        CollapseFunction, CollapsePixel, ImageCollapse, ImageCollapseOptions, SubimageRegion,
        ensure_outfile_does_not_alias_input,
    },
    image::{ImageHistoryRow, image_history_rows},
};
use casa_types::RecordValue;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyList, PyString, PyTuple};

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::create::publish_masked_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::subimage::{RegionArgument, require_bool};

enum Axes {
    Missing,
    Numbers(Vec<i64>),
    Names(Vec<String>),
    Unsupported,
}

impl Axes {
    fn capture(value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let Some(value) = value.filter(|value| !value.is_none()) else {
            return Ok(Self::Missing);
        };
        if let Ok(value) = value.cast::<PyString>() {
            return Ok(Self::Names(vec![value.to_str()?.to_owned()]));
        }
        if let Some(index) = integer(value) {
            return Ok(Self::Numbers(vec![index]));
        }
        let numpy_kind = dtype_kind(value);
        if value.is_instance_of::<PyList>()
            || value.is_instance_of::<PyTuple>()
            || numpy_kind.is_some()
        {
            let Ok(items) = value.try_iter() else {
                return Ok(Self::Unsupported);
            };
            let items = items.collect::<PyResult<Vec<_>>>()?;
            if items.is_empty() {
                return Ok(if matches!(numpy_kind.as_deref(), Some("i" | "u")) {
                    Self::Numbers(Vec::new())
                } else {
                    Self::Missing
                });
            }
            if items.iter().all(is_bool) {
                return Ok(Self::Missing);
            }
            if let Some(values) = items.iter().map(integer).collect() {
                return Ok(Self::Numbers(values));
            }
            if let Ok(values) = items
                .iter()
                .map(|item| item.extract::<String>())
                .collect::<PyResult<Vec<_>>>()
            {
                return Ok(Self::Names(values));
            }
        }
        Ok(Self::Unsupported)
    }

    fn history(&self, input: Option<&Bound<'_, PyAny>>) -> String {
        match self {
            Self::Numbers(values) if input.is_some_and(|value| integer(value).is_some()) => {
                values[0].to_string()
            }
            Self::Numbers(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Names(values)
                if input.is_some_and(|value| value.is_instance_of::<PyString>()) =>
            {
                format!("'{}'", values[0])
            }
            Self::Names(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|value| format!("{value:?}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            _ => "[]".into(),
        }
    }

    fn resolve(&self, coordinates: &CoordinateSystem) -> PyResult<Vec<usize>> {
        match self {
            Self::Missing => Err(raised("axes must be specified")),
            Self::Unsupported => Err(raised("Unsupported type for parameter axes")),
            Self::Numbers(indices) => indices
                .iter()
                .map(|&axis| {
                    usize::try_from(axis).map_err(|_| raised("Collapse axes must be nonnegative"))
                })
                .collect(),
            Self::Names(names) => {
                let mut available = vec![String::new(); coordinates.n_world_axes()];
                for index in 0..coordinates.n_coordinates() {
                    let model = coordinates.coordinate(index);
                    for (axis, name) in coordinates
                        .coordinate_world_axes(index)
                        .expect("existing coordinate")
                        .into_iter()
                        .zip(model.axis_names())
                    {
                        if let Some(axis) = axis {
                            available[axis] = name.to_ascii_lowercase();
                        }
                    }
                }
                names
                    .iter()
                    .map(|name| {
                        let name = name.to_ascii_lowercase();
                        let matches: Vec<_> = available
                            .iter()
                            .enumerate()
                            .filter(|(_, axis)| axis.starts_with(&name))
                            .collect();
                        match matches.as_slice() {
                            [(axis, _)] => {
                                coordinates.world_axis_to_pixel_axis(*axis).ok_or_else(|| {
                                    raised(format!("World axis {name} has no image pixel axis"))
                                })
                            }
                            [] => Err(raised(format!(
                                "No axis matches requested axis {name}. Image axis names are [{}]",
                                available.join(", ")
                            ))),
                            _ => Err(raised(format!(
                                "Multiple axes [{}] match requested axis {name}",
                                matches
                                    .iter()
                                    .map(|(_, name)| name.as_str())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            ))),
                        }
                    })
                    .collect()
            }
        }
    }
}

fn dtype_kind(value: &Bound<'_, PyAny>) -> Option<String> {
    value
        .getattr("dtype")
        .ok()?
        .getattr("kind")
        .ok()?
        .extract()
        .ok()
}

fn is_bool(value: &Bound<'_, PyAny>) -> bool {
    value.is_instance_of::<PyBool>() || dtype_kind(value).as_deref() == Some("b")
}

fn integer(value: &Bound<'_, PyAny>) -> Option<i64> {
    if is_bool(value) {
        return None;
    }
    value.extract().ok()
}

struct Request<'a> {
    function: &'a str,
    axes: Axes,
    axes_history: String,
    outfile: &'a str,
    region: Option<RegionArgument>,
    box_selection: &'a str,
    chans: &'a str,
    stokes: &'a str,
    mask: &'a str,
    overwrite: bool,
    stretch: bool,
}

impl Request<'_> {
    fn copy<T: CollapsePixel>(
        &self,
        source: &dyn ImageInterface<T>,
        record: RecordValue,
        beams: &RecordValue,
        path: Option<&Path>,
    ) -> PyResult<(Storage<T>, Option<RecordValue>)> {
        let axes = self.axes.resolve(source.coordinates())?;
        let options = ImageCollapseOptions {
            function: CollapseFunction::parse(self.function).map_err(raised)?,
            axes: &axes,
            region: self.region.as_ref().and_then(RegionArgument::selection),
            spatial: (!self.box_selection.is_empty()).then_some(self.box_selection),
            chans: (!self.chans.is_empty()).then_some(self.chans),
            stokes: (!self.stokes.is_empty()).then_some(self.stokes),
            mask: (!self.mask.is_empty()).then_some(self.mask),
            stretch: self.stretch,
            destination: (!self.outfile.trim().is_empty()).then(|| Path::new(self.outfile.trim())),
        };
        let mut collapse = ImageCollapse::new(source, record, options, beams).map_err(raised)?;
        let mut storage = Storage::create(
            collapse.section.shape.clone(),
            collapse.section.coordinates.clone(),
            path,
            None,
        )
        .map_err(raised)?;
        with_storage!(&mut storage, image => collapse.copy_to(source, image.as_mut()))
            .map_err(raised)?;
        Ok((storage, collapse.section.coordinate_record.take()))
    }

    fn build(
        &self,
        source: &ImageHandle,
        beams: &RecordValue,
        path: Option<&Path>,
        history: bool,
    ) -> PyResult<ImageHandle> {
        if let Some(region) = &self.region {
            region.validate_selectors(self.box_selection, self.chans, self.stokes)?;
        }
        let record = source.coordinate_record();
        let (mut output, coords) = match source {
            ImageHandle::Float(Storage::Temp {
                image,
                fits: Some(fits),
                ..
            }) => {
                let (storage, coords) = self.copy(&fits.image_view(image), record, beams, path)?;
                (ImageHandle::Float(storage), coords)
            }
            ImageHandle::Float(image) => {
                let (storage, coords) =
                    with_storage!(image, image => self.copy(image.as_ref(), record, beams, path))?;
                (ImageHandle::Float(storage), coords)
            }
            ImageHandle::Complex(image) => {
                let (storage, coords) =
                    with_storage!(image, image => self.copy(image.as_ref(), record, beams, path))?;
                (ImageHandle::Complex(storage), coords)
            }
            _ => unreachable!("checked single-precision image"),
        };
        if let Some(coords) = coords {
            output.set_coordinate_record(coords)?;
        }
        let entries: Vec<ImageHistoryRow> = with_pixels!(source, storage => match storage {
            Storage::Paged(image) => image_history_rows(image.name().expect("native source")).map_err(raised)?,
            Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
        });
        with_pixels!(&mut output, storage => with_storage!(storage, image => image.append_history_entries(&entries))).map_err(raised)?;
        if history {
            let Self {
                function,
                axes_history,
                outfile,
                region,
                box_selection,
                chans,
                stokes,
                mask,
                overwrite,
                stretch,
                ..
            } = self;
            let region = match region.as_ref().and_then(RegionArgument::selection) {
                Some(SubimageRegion::Text(text)) => format!("'{text}'"),
                Some(SubimageRegion::Record(_)) => "{...}".into(),
                None => region.as_ref().map_or_else(
                    || "''".into(),
                    |region| {
                        let history = region.history();
                        if history == "\"\"" {
                            "''".into()
                        } else {
                            history
                        }
                    },
                ),
            };
            let name = source.path().map_or_else(
                || "Temporary_Image".into(),
                |path| path.display().to_string(),
            );
            output.append_history_at("::image::collapse", &[
                format!("Ran ia.collapse on {name}"),
                format!("ia.collapse(function='{function}', axes={axes_history}, outfile='{outfile}', region={region}, box='{box_selection}', chans='{chans}', stokes='{stokes}', mask='{mask}', overwrite={overwrite}, stretch={stretch})"),
            ])?;
        }
        output.save().map_err(raised)?;
        Ok(output)
    }
}

#[pymethods]
impl PyImageTool {
    /// Reduce each selected image section to a statistic along the specified axes.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (function="", axes=None, outfile="", region=None, r#box="", chans="", stokes="", mask="", overwrite=false, stretch=false), text_signature = "($self, function='', axes=[], outfile='', region='', box='', chans='', stokes='', mask='', overwrite=False, stretch=False)")]
    fn collapse(
        &self,
        py: Python<'_>,
        function: &str,
        axes: Option<&Bound<'_, PyAny>>,
        outfile: &str,
        region: Option<RegionArgument>,
        r#box: &str,
        chans: &str,
        stokes: &str,
        mask: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let captured = Axes::capture(axes)?;
        let axes_history = captured.history(axes);
        let request = Request {
            function,
            axes: captured,
            axes_history,
            outfile,
            region,
            box_selection: r#box,
            chans,
            stokes,
            mask,
            overwrite,
            stretch,
        };
        let product = py.detach(|| {
            let state = self.state();
            let Some(attached) = state.image.as_ref() else {
                return Ok(None);
            };
            let mut live_coordinates = None;
            let outfile = outfile.trim();
            let mut product = publish_masked_image(outfile, overwrite, Some(mask), |path| {
                let mut image = attached.lock();
                image.check_single_precision("collapse")?;
                if !outfile.is_empty() {
                    for source in image
                        .path()
                        .into_iter()
                        .chain(attached.operand_paths().iter().map(|path| path.as_path()))
                    {
                        ensure_outfile_does_not_alias_input(Path::new(outfile), source)
                            .map_err(raised)?;
                    }
                }
                attached.require_pixel_read_lock(&mut image)?;
                let beams = attached.beam_view().clone();
                let staged = request.build(&image, beams.record(), path, state.history);
                let released = attached.release_requested_lock(&mut image);
                let staged = staged?;
                released?;
                live_coordinates = AttachedImage::capture_live_coordinates(&staged)?;
                Ok(staged)
            })?;
            if let Some((record, system)) = live_coordinates {
                product.keep_live_coordinates(record, system);
            }
            Ok::<_, PyErr>(Some(product))
        })?;
        Py::new(py, product.map_or_else(Self::new, Self::attached))
    }
}
