// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.sepconvolve` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::sepconvolve_image`].

use std::path::Path;

use casa_core::quanta::Quantity;
use casa_imageanalysis::analysis::SepConvolutionOptions;
use casa_imageanalysis::tool::ops_transform::sepconvolve_image;
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};

use crate::convert::{IntList, Variant, read_quantity};

use super::PyImageTool;
use super::handle::{external_open_check, raised};
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
        let attachment = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let input = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Unable to create image"))?;
            let mask = mask?;
            let (widths, widths_history) = widths?;
            let Some(widths) = widths else {
                input
                    .lock()
                    .check_single_precision("sepconvolve")
                    .map_err(raised)?;
                return Ok(None);
            };
            let destination = (!outfile.is_empty()).then(|| Path::new(outfile));
            let options = SepConvolutionOptions {
                axes: &axes.0,
                types: &types,
                widths: &widths,
                scale,
                region: region.selection(),
                mask: (!mask.is_empty()).then_some(mask.as_str()),
                stretch,
                destination,
            };
            let history_command = state.history.then(|| {
                let region_hist = region.history();
                format!(
                    "ia.sepconvolve(outfile={outfile:?}, axes={:?}, types={:?}, widths={widths_history}, scale={scale}, region={region_hist}, mask={mask:?}, overwrite={overwrite}, stretch={stretch})",
                    axes.0, types,
                )
            });
            sepconvolve_image(
                input,
                outfile,
                overwrite,
                options,
                history_command,
                external_open_check(),
            )
            .map(Some)
            .map_err(raised)
        })?;
        Py::new(py, attachment.map_or_else(Self::new, Self::attached))
    }
}
