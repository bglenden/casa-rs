// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.deviation` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::deviation_image`].

use std::path::Path;

use casa_core::quanta::Quantity;
use casa_imageanalysis::analysis::{DeviationOptions, SubimageRegion};
use casa_imageanalysis::tool::ops_transform::deviation_image;
use casa_images::resample::RegridMethod;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyString};

use crate::convert::{IntList, Variant, quantity};

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{RegionArgument, mask_argument, require_bool};

struct LengthArgument {
    value: Variant,
    pixels: Option<i64>,
    flags: Option<String>,
}

impl Default for LengthArgument {
    fn default() -> Self {
        Self {
            value: Variant::Text("1pix".into()),
            pixels: None,
            flags: None,
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for LengthArgument {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let pixels = if value.is_instance_of::<PyBool>() {
            None
        } else {
            value.extract::<i64>().ok()
        };
        Ok(Self {
            value: value.extract()?,
            pixels,
            flags: if value.is_instance_of::<PyString>() {
                None
            } else {
                sequence_items(&value)?.as_deref().and_then(bool_history)
            },
        })
    }
}

impl LengthArgument {
    fn parse(&self, name: &str) -> PyResult<Quantity> {
        if let Some(pixels) = self.pixels {
            return Quantity::new(pixels as f64, "pix").map_err(raised);
        }
        match &self.value {
            Variant::Text(_) | Variant::Number(_) => quantity(&self.value)
                .map_err(|error| raised(format!("{name} is not a valid quantity: {error}"))),
            _ => Err(raised(format!("{name} is not a valid quantity"))),
        }
    }

    fn is_empty(&self) -> bool {
        self.flags.is_some()
            || self.value.is_empty_sequence()
            || matches!(&self.value, Variant::Text(text) if text.is_empty())
    }

    fn history(&self) -> String {
        self.flags.as_ref().map_or_else(
            || history_variant(&self.value),
            |flags| history_value(flags).to_owned(),
        )
    }
}

fn sequence_items<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Vec<Bound<'py, PyAny>>>> {
    value
        .try_iter()
        .ok()
        .map(|items| items.collect())
        .transpose()
}

fn bool_history(items: &[Bound<'_, PyAny>]) -> Option<String> {
    let flags = items
        .iter()
        .map(|item| {
            item.cast::<PyBool>()
                .ok()
                .map(|flag| if flag.is_true() { "true" } else { "false" })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(format!("[{}]", flags.join(",")))
}

fn history_value(value: &str) -> &str {
    if value.len() > 300 { "..." } else { value }
}

fn history_string(value: &str) -> String {
    format!("\"{}\"", history_value(value))
}

fn history_variant(value: &Variant) -> String {
    match value {
        Variant::Text(text) => history_string(text),
        value => history_value(&value.describe()).to_owned(),
    }
}

struct AnchorArgument {
    history: String,
    selection: Result<Option<[i64; 2]>, String>,
}

impl Default for AnchorArgument {
    fn default() -> Self {
        Self {
            history: r#""ref""#.into(),
            selection: Ok(None),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for AnchorArgument {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(token) = value.extract::<String>() {
            return Ok(Self {
                history: history_string(&token),
                selection: if token == "ref" {
                    Ok(None)
                } else {
                    Err(format!("Unsupported value for anchor: {token}"))
                },
            });
        }
        let variant: Variant = value.extract()?;
        if variant.is_empty_sequence() {
            return Ok(Self {
                history: "[]".into(),
                selection: Ok(None),
            });
        }
        let items = sequence_items(&value)?;
        if let Some(flags) = items.as_deref().and_then(bool_history) {
            return Ok(Self {
                history: history_value(&flags).into(),
                selection: Ok(None),
            });
        }
        let integers = items.and_then(|items| {
            items
                .iter()
                .map(|item| item.extract::<i64>().ok())
                .collect::<Option<Vec<_>>>()
        });
        let selection: Result<Option<[i64; 2]>, String> = match (&variant, integers) {
            (Variant::Numbers(_), Some(values)) => values
                .try_into()
                .map(Some)
                .map_err(|_| "anchor must have exactly two integer values".to_owned()),
            _ => Err("Unsupported type for anchor".to_owned()),
        };
        let history = match selection.as_ref() {
            Ok(Some([x, y])) => format!("[{x},{y}]"),
            _ => history_variant(&variant),
        };
        Ok(Self { history, selection })
    }
}

#[pymethods]
impl PyImageTool {
    /// Create an image of local statistics sampled across each direction plane.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", region=RegionArgument::empty_text(), mask=None, overwrite=false, stretch=false, grid=IntList::of(&[1, 1]), anchor=AnchorArgument::default(), xlength=LengthArgument::default(), ylength=LengthArgument::default(), interp="cubic", stattype="sigma", statalg="classic", zscore=-1.0, maxiter=-1), text_signature = "($self, outfile='', region='', mask='', overwrite=False, stretch=False, grid=[1, 1], anchor='ref', xlength='1pix', ylength='1pix', interp='cubic', stattype='sigma', statalg='classic', zscore=-1.0, maxiter=-1)")]
    fn deviation(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        grid: IntList,
        anchor: AnchorArgument,
        xlength: LengthArgument,
        ylength: LengthArgument,
        interp: &str,
        stattype: &str,
        statalg: &str,
        zscore: f64,
        maxiter: i64,
    ) -> PyResult<Py<Self>> {
        let mask = mask_argument(mask)?;
        let output = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(None);
            };
            let grid_arr: [i64; 2] = grid
                .0
                .as_slice()
                .try_into()
                .map_err(|_| raised("grid must have exactly two positive integer values"))?;
            if grid_arr.iter().any(|&entry| entry <= 0) {
                return Err(raised("Both grid value(s) must be positive"));
            }
            let interpolation = match interp.to_ascii_lowercase().as_bytes().first() {
                Some(b'c') => RegridMethod::Cubic,
                Some(b'l') => RegridMethod::Linear,
                Some(b'n') => RegridMethod::Nearest,
                _ => {
                    return Err(raised(format!(
                        "Interpolation algorithm {interp} is not supported."
                    )));
                }
            };
            let options = DeviationOptions {
                region: region.selection(),
                mask: Some(&mask),
                stretch,
                destination: (!outfile.is_empty()).then(|| Path::new(outfile)),
                grid: grid_arr.map(|entry| entry as usize),
                anchor: anchor.selection.as_ref().copied().map_err(raised)?,
                x_length: xlength.parse("xlength")?,
                y_length: (!ylength.is_empty())
                    .then(|| ylength.parse("ylength"))
                    .transpose()?,
                interpolation,
                statistic: stattype,
                algorithm: statalg,
                zscore,
                maxiter,
            };
            let history_command = state.history.then(|| {
                let region_hist = match region.selection() {
                    Some(SubimageRegion::Text(text)) => history_string(text),
                    _ => region.history(),
                };
                let grid_str = grid
                    .0
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "ia.deviation(outfile={}, region={region_hist}, mask={}, overwrite={overwrite}, stretch={stretch}, grid=[{grid_str}], anchor={}, xlength={}, ylength={}, interp={}, stattype={}, statalg={}, zscore={zscore:.6}, maxiter={maxiter})",
                    history_string(outfile),
                    history_string(&mask),
                    anchor.history,
                    xlength.history(),
                    ylength.history(),
                    history_string(interp),
                    history_string(stattype),
                    history_string(statalg),
                )
            });
            deviation_image(
                attachment,
                outfile,
                overwrite,
                options,
                history_command,
                external_open_check(),
            )
            .map(Some)
            .map_err(raised)
        })?;
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}
