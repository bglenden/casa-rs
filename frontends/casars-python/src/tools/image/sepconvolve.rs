// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_core::{RecordValue, quanta::Quantity};
use casa_imageanalysis::analysis::{
    ImageSepConvolution, SepConvolutionOptions, SubimageRegion, ensure_artifacts_do_not_overlap,
    ensure_outfile_does_not_alias_input,
};
use casa_images::{ImageInterface, image_history_rows};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};

use crate::convert::{IntList, Variant, read_quantity};

use super::PyImageTool;
use super::attachment::read_locked;
use super::beam_view::BeamView;
use super::beams::subset_beam_record;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::records::{numeric_axis_vector, string_vector};
use super::subimage::{RegionArgument, mask_argument, require_bool};

fn parse_widths(value: Option<&Bound<'_, PyAny>>) -> PyResult<(Option<Vec<Quantity>>, String)> {
    let variant = value
        .map(width_variant)
        .transpose()?
        .unwrap_or(Variant::Empty);
    let history = variant.describe();
    let quantities = match variant {
        Variant::Numbers(values) => values
            .into_iter()
            .map(|value| Quantity::new(value, "pix").map_err(raised))
            .collect::<PyResult<_>>()?,
        Variant::Text(text) => {
            let tokens: Vec<_> = text
                .split(|byte: char| byte.is_whitespace() || byte == ',')
                .filter(|token| !token.is_empty())
                .collect();
            if tokens.is_empty() {
                vec![Quantity::dimensionless(0.0)]
            } else {
                tokens
                    .into_iter()
                    .map(width_text)
                    .collect::<PyResult<_>>()?
            }
        }
        Variant::Texts(texts) => texts
            .iter()
            .map(|text| width_text(text))
            .collect::<PyResult<_>>()?,
        Variant::Other(_) => {
            let Some(value) = value.filter(|value| {
                value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>()
            }) else {
                return Ok((None, history));
            };
            let items = value.try_iter()?.collect::<PyResult<Vec<_>>>()?;
            if !items.iter().any(|item| item.is_instance_of::<PyString>()) {
                return Ok((None, history));
            }
            items
                .iter()
                .map(|item| width_text(item.str()?.to_str()?))
                .collect::<PyResult<_>>()?
        }
        _ => return Ok((None, history)),
    };
    Ok((Some(quantities), history))
}

fn width_variant(value: &Bound<'_, PyAny>) -> PyResult<Variant> {
    let Ok(array) = value.cast::<PyUntypedArray>() else {
        return value.extract();
    };
    let dtype = array.dtype();
    match (dtype.kind(), dtype.char()) {
        (b'i' | b'u', _) | (b'f', b'f' | b'd' | b'g') => {
            let flat = value
                .call_method1("ravel", ("F",))?
                .call_method0("tolist")?;
            let values: Vec<f64> = flat.extract()?;
            Ok(if values.is_empty() {
                Variant::Empty
            } else {
                Variant::Numbers(values)
            })
        }
        (b'S' | b'U', _) => {
            if array.ndim() == 0 {
                return Ok(Variant::Texts(Vec::new()));
            }
            if array.shape().iter().filter(|&&length| length > 1).count() > 1 {
                return Err(raised(
                    "Cannot read convolution widths from a multidimensional string array",
                ));
            }
            let flat = value
                .call_method1("ravel", ("F",))?
                .call_method0("tolist")?;
            let texts = flat
                .try_iter()?
                .map(|item| match item?.extract()? {
                    Variant::Text(text) => Ok(text),
                    _ => Err(raised("Convolution width must be a quantity string")),
                })
                .collect::<PyResult<_>>()?;
            Ok(Variant::Texts(texts))
        }
        _ => Ok(Variant::Empty),
    }
}

fn width_text(text: &str) -> PyResult<Quantity> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(Quantity::dimensionless(0.0));
    }
    // A string-vector entry contains one quantity. CASA consumes its first
    // numeric value and attached unit even if another quantity follows it.
    let context = " in converting convolution width";
    if let Some((first, rest)) = text.split_once(|c: char| c.is_whitespace() || c == ',')
        && rest
            .trim_start()
            .starts_with(|c: char| c.is_ascii_digit() || "+-.".contains(c))
        && let Ok(quantity) = read_quantity(first, context)
        && !quantity.unit().name().is_empty()
    {
        return Ok(quantity);
    }
    read_quantity(text, context).map_err(raised)
}

fn make_product(
    image: &dyn ImageInterface<f32>,
    record: RecordValue,
    beams: &BeamView,
    options: SepConvolutionOptions<'_>,
    stage: Option<&Path>,
) -> PyResult<ImageHandle> {
    let mut plan =
        ImageSepConvolution::new(image, record, beams.record(), options).map_err(raised)?;
    let mut output = Storage::create(
        plan.section.shape.clone(),
        plan.section.coordinates.clone(),
        stage,
        None,
    )
    .map_err(raised)?;
    with_storage!(&mut output, product => plan.copy_to(image, product.as_mut())).map_err(raised)?;
    let mut info = plan.section.image_info().to_record();
    let beam_set = beams.beam_set().map_err(raised)?;
    if beam_set.is_multi() {
        let channels = plan.section.beam_planes(
            image.find_axis(CoordinateType::Spectral),
            beam_set.n_channels(),
        );
        let polarizations = plan
            .section
            .beam_planes(image.find_axis(CoordinateType::Stokes), beam_set.n_stokes());
        subset_beam_record(
            &mut info,
            Some(beams.record()),
            &beam_set,
            &channels,
            &polarizations,
        )
        .map_err(raised)?;
    } else {
        beams.apply_to(&mut info);
    }
    with_storage!(&mut output, product => product.set_image_info_record(info)).map_err(raised)?;
    if let Some(record) = plan.section.coordinate_record.take() {
        match &mut output {
            Storage::Paged(image) => image.set_coordinate_record(record).map_err(raised)?,
            Storage::Temp {
                record: current, ..
            } => *current = record,
        }
    }
    Ok(ImageHandle::Float(output))
}

#[pymethods]
impl PyImageTool {
    /// Convolve a selected Float image with one-dimensional kernels.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", axes=IntList::of(&[-1]), types=None, widths=None, scale=-1.0, region=RegionArgument::default(), mask=None, overwrite=false, stretch=false), text_signature = "($self, outfile='', axes=[-1], types=[''], widths=[], scale=-1.0, region={}, mask='', overwrite=False, stretch=False)")]
    fn sepconvolve(
        &self,
        py: Python<'_>,
        outfile: &str,
        #[pyo3(from_py_with = numeric_axis_vector)] axes: IntList,
        types: Option<&Bound<'_, PyAny>>,
        widths: Option<&Bound<'_, PyAny>>,
        scale: f64,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let mut types = string_vector(types, "types")?;
        if types.is_empty() {
            types.push(String::new());
        }
        let widths = parse_widths(widths);
        let mask = mask_argument(mask);
        let attachment = py.detach(|| {
            let state = self.state();
            let input = state.image.as_ref().ok_or_else(|| raised("Unable to create image"))?;
            let mask = mask?;
            let (widths, widths_history) = widths?;
            let Some(widths) = widths else {
                input.lock().check_single_precision("sepconvolve")?;
                return Ok(None);
            };
            let destination = (!outfile.is_empty()).then(|| Path::new(outfile));
            let options = SepConvolutionOptions {
                axes: &axes.0, types: &types, widths: &widths, scale,
                region: region.selection(), mask: (!mask.is_empty()).then_some(mask.as_str()),
                stretch, destination,
            };
            publish_image(outfile, overwrite, |stage| read_locked(input, |handle| {
                handle.check_single_precision("sepconvolve")?;
                let ImageHandle::Float(source) = handle else {
                    return Err(raised("Exception: image cannot be NULL."));
                };
                if let Some(destination) = destination {
                    for source in handle.path().into_iter().chain(input.operand_paths().iter().map(|path| path.as_path())) {
                        ensure_outfile_does_not_alias_input(destination, source).map_err(raised)?;
                    }
                    if let Some(region) = options.region.and_then(SubimageRegion::external_path) {
                        ensure_artifacts_do_not_overlap(destination, region).map_err(raised)?;
                    }
                }
                let mut output = source.with_float_view(|image| {
                    make_product(image, handle.coordinate_record(), &input.beam_view(), options, stage)
                })?;
                let history = match source {
                    Storage::Paged(image) => image_history_rows(image.name().expect("native image path")).map_err(raised)?,
                    Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
                };
                let ImageHandle::Float(product) = &mut output else { unreachable!() };
                with_storage!(product, image => image.append_history_entries(&history)).map_err(raised)?;
                if state.history {
                    let region = region.history();
                    output.record_history("sepconvolve", format!(
                        "ia.sepconvolve(outfile={outfile:?}, axes={:?}, types={:?}, widths={widths_history}, scale={scale}, region={region}, mask={mask:?}, overwrite={overwrite}, stretch={stretch})",
                        axes.0, types,
                    ))?;
                }
                output.save().map_err(raised)?;
                Ok(output)
            })).map(Some)
        })?;
        Py::new(py, attachment.map_or_else(Self::new, Self::attached))
    }
}
