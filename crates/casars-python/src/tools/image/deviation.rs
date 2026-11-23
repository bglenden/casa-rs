// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::{
    ImageInterface,
    analysis::{
        DeviationOptions, ImageDeviation, SubimageRegion, ensure_outfile_does_not_alias_input,
    },
    regrid::RegridMethod,
};
use casa_types::quanta::Quantity;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyString};

use crate::convert::{IntList, Variant, quantity};

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};
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

struct Request<'a> {
    outfile: &'a str,
    region: RegionArgument,
    mask: String,
    overwrite: bool,
    stretch: bool,
    grid: IntList,
    anchor: AnchorArgument,
    xlength: LengthArgument,
    ylength: LengthArgument,
    interp: &'a str,
    stattype: &'a str,
    statalg: &'a str,
    zscore: f64,
    maxiter: i64,
    history: bool,
}

impl Request<'_> {
    fn build(
        &self,
        attachment: &AttachedImage,
        source: &ImageHandle,
        path: Option<&Path>,
    ) -> PyResult<ImageHandle> {
        let ImageHandle::Float(storage) = source else {
            return Err(raised("This method only supports Float valued images"));
        };
        if !self.outfile.is_empty() {
            for input in source
                .path()
                .into_iter()
                .chain(attachment.operand_paths().iter().map(|path| path.as_path()))
            {
                ensure_outfile_does_not_alias_input(Path::new(self.outfile), input)
                    .map_err(raised)?;
            }
        }
        let grid: [i64; 2] = self
            .grid
            .0
            .as_slice()
            .try_into()
            .map_err(|_| raised("grid must have exactly two positive integer values"))?;
        if grid.iter().any(|&entry| entry <= 0) {
            return Err(raised("Both grid value(s) must be positive"));
        }
        let interpolation = match self.interp.to_ascii_lowercase().as_bytes().first() {
            Some(b'c') => RegridMethod::Cubic,
            Some(b'l') => RegridMethod::Linear,
            Some(b'n') => RegridMethod::Nearest,
            _ => {
                return Err(raised(format!(
                    "Interpolation algorithm {} is not supported.",
                    self.interp
                )));
            }
        };
        let options = DeviationOptions {
            region: self.region.selection(),
            mask: Some(&self.mask),
            stretch: self.stretch,
            destination: (!self.outfile.is_empty()).then(|| Path::new(self.outfile)),
            grid: grid.map(|entry| entry as usize),
            anchor: self.anchor.selection.as_ref().copied().map_err(raised)?,
            x_length: self.xlength.parse("xlength")?,
            y_length: (!self.ylength.is_empty())
                .then(|| self.ylength.parse("ylength"))
                .transpose()?,
            interpolation,
            statistic: self.stattype,
            algorithm: self.statalg,
            zscore: self.zscore,
            maxiter: self.maxiter,
        };
        let compute = |image: &dyn ImageInterface<f32>| -> PyResult<ImageHandle> {
            let plan = ImageDeviation::new(
                image,
                source.coordinate_record(),
                attachment.beam_view().record(),
                options,
            )
            .map_err(raised)?;
            let section = plan.section();
            let mut result = Storage::create(
                section.shape.clone(),
                section.coordinates.clone(),
                path,
                None,
            )
            .map_err(raised)?;
            with_storage!(&mut result, output => plan.copy_to(image, output.as_mut()))
                .map_err(raised)?;
            let mut output = ImageHandle::Float(result);
            if let Some(record) = &section.coordinate_record {
                output.set_coordinate_record(record.clone())?;
            }
            Ok(output)
        };
        let mut output = match storage {
            Storage::Temp {
                image,
                fits: Some(fits),
                ..
            } => compute(&fits.image_view(image)),
            _ => with_storage!(storage, image => compute(image.as_ref())),
        }?;
        output.append_history_entries(&source.history_entries()?)?;
        if self.history {
            output.record_history("deviation", self.history_call())?;
        }
        output.save().map_err(raised)?;
        Ok(output)
    }

    fn history_call(&self) -> String {
        let Self {
            outfile,
            region,
            mask,
            overwrite,
            stretch,
            grid,
            anchor,
            xlength,
            ylength,
            interp,
            stattype,
            statalg,
            zscore,
            maxiter,
            ..
        } = self;
        let region = match region.selection() {
            Some(SubimageRegion::Text(text)) => history_string(text),
            _ => region.history(),
        };
        let grid = grid
            .0
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let anchor = &anchor.history;
        let xlength = xlength.history();
        let ylength = ylength.history();
        let outfile = history_string(outfile);
        let mask = history_string(mask);
        let interp = history_string(interp);
        let stattype = history_string(stattype);
        let statalg = history_string(statalg);
        format!(
            "ia.deviation(outfile={outfile}, region={region}, mask={mask}, overwrite={overwrite}, stretch={stretch}, grid=[{grid}], anchor={anchor}, xlength={xlength}, ylength={ylength}, interp={interp}, stattype={stattype}, statalg={statalg}, zscore={zscore:.6}, maxiter={maxiter})"
        )
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
        let output = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(None);
            };
            let request = Request {
                outfile,
                region,
                mask,
                overwrite,
                stretch,
                grid,
                anchor,
                xlength,
                ylength,
                interp,
                stattype,
                statalg,
                zscore,
                maxiter,
                history: state.history,
            };
            let mut live_coordinates = None;
            let mut product = publish_image(outfile, overwrite, |path| {
                let staged =
                    read_locked(attachment, |source| request.build(attachment, source, path))?;
                live_coordinates = AttachedImage::capture_live_coordinates(&staged)?;
                Ok(staged)
            })?;
            if let Some((record, system)) = live_coordinates {
                product.keep_live_coordinates(record, system);
            }
            Ok::<_, PyErr>(Some(product))
        })?;
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}
