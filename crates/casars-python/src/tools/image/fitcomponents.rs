// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use casa_images::{
    ImageInterface, MutableImageInterface, PagedImage,
    analysis::{
        ComponentFitPixel, ComponentFitQuantity, FitComponentsOptions, ImageComponentFit,
        ensure_artifacts_do_not_overlap, fit_image_components,
    },
};
use casa_lattices::TiledShape;
use casa_types::RecordValue;
use casa_types::quanta::Quantity;
use pyo3::prelude::*;
use pyo3::types::PyBool;

use crate::convert::{DoubleList, Variant, quantity};

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::beam_edit::input_history;
use super::create::publish_image_set;
use super::data_compat::record_to_py;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::{RegionArgument, mask_argument, require_bool};

enum MeasureArgument {
    Numeric(f64),
    Quantity(Quantity),
}

impl MeasureArgument {
    fn value(&self) -> ComponentFitQuantity<'_> {
        match self {
            Self::Numeric(value) => ComponentFitQuantity::Numeric(*value),
            Self::Quantity(value) => ComponentFitQuantity::Quantity(value),
        }
    }
}

struct FitRequest<'a> {
    pixel_box: &'a str,
    region: RegionArgument,
    channels: String,
    channels_history: String,
    stokes: &'a str,
    mask: String,
    include: Vec<f64>,
    exclude: Vec<f64>,
    residual: &'a str,
    model: &'a str,
    estimates: &'a str,
    logfile: &'a str,
    append: bool,
    newestimates: &'a str,
    complist: &'a str,
    overwrite: bool,
    dooff: bool,
    offset: f64,
    fixoffset: bool,
    stretch: bool,
    rms: Option<MeasureArgument>,
    rms_history: String,
    noise: Option<MeasureArgument>,
    noise_history: String,
    summary: &'a str,
}

#[pymethods]
impl PyImageTool {
    /// Fit Gaussian sky components and write optional model and residual images.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (r#box="", region=None, chans=None, stokes="", mask=None, includepix=DoubleList(vec![-1.]), excludepix=DoubleList(vec![-1.]), residual="", model="", estimates="", logfile="", append=true, newestimates="", complist="", overwrite=false, dooff=false, offset=0., fixoffset=false, stretch=false, rms=None, noisefwhm=None, summary=""), text_signature = "($self, box='', region=[], chans=[], stokes='', mask='', includepix=[-1.0], excludepix=[-1.0], residual='', model='', estimates='', logfile='', append=True, newestimates='', complist='', overwrite=False, dooff=False, offset=0.0, fixoffset=False, stretch=False, rms='', noisefwhm='', summary='')")]
    fn fitcomponents<'py>(
        &self,
        py: Python<'py>,
        r#box: &str,
        region: Option<&Bound<'py, PyAny>>,
        chans: Option<&Bound<'py, PyAny>>,
        stokes: &str,
        mask: Option<&Bound<'py, PyAny>>,
        includepix: DoubleList,
        excludepix: DoubleList,
        residual: &str,
        model: &str,
        estimates: &str,
        logfile: &str,
        #[pyo3(from_py_with = require_bool)] append: bool,
        newestimates: &str,
        complist: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] dooff: bool,
        offset: f64,
        #[pyo3(from_py_with = require_bool)] fixoffset: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        rms: Option<&Bound<'py, PyAny>>,
        noisefwhm: Option<&Bound<'py, PyAny>>,
        summary: &str,
    ) -> PyResult<Py<PyAny>> {
        let real = py.detach(|| {
            self.state().image.as_ref().map(|image| {
                matches!(
                    *image.lock(),
                    ImageHandle::Float(_) | ImageHandle::Double(_)
                )
            })
        });
        match real {
            None => return record_to_py(py, RecordValue::default()),
            Some(false) => return Err(raised("This method only supports real valued images")),
            Some(true) => {}
        }
        let (channels, channels_history) = channel_argument(chans)?;
        let (rms, rms_history) = measure_argument(rms, "rms")?;
        let (noise, noise_history) = measure_argument(noisefwhm, "noisefwhm")?;
        let request = FitRequest {
            pixel_box: r#box,
            region: region.map_or_else(
                || Ok(RegionArgument::default()),
                |argument| argument.extract(),
            )?,
            channels,
            channels_history,
            stokes,
            mask: mask_argument(mask)?,
            include: includepix.0,
            exclude: excludepix.0,
            residual,
            model,
            estimates,
            logfile,
            append,
            newestimates,
            complist,
            overwrite,
            dooff,
            offset,
            fixoffset,
            stretch,
            rms,
            rms_history,
            noise,
            noise_history,
            summary,
        };
        let record = py.detach(|| {
            let state = self.state();
            match &state.image {
                Some(source) => request.run(source, state.history),
                None => Ok(RecordValue::default()),
            }
        })?;
        record_to_py(py, record)
    }
}

impl FitRequest<'_> {
    fn run(&self, attachment: &AttachedImage, history: bool) -> PyResult<RecordValue> {
        let paths = [
            self.model,
            self.residual,
            self.complist,
            self.logfile,
            self.newestimates,
            self.summary,
        ]
        .into_iter()
        .filter(|path| !path.is_empty())
        .map(Path::new)
        .collect::<Vec<_>>();
        for (index, path) in paths.iter().enumerate() {
            for peer in &paths[..index] {
                ensure_artifacts_do_not_overlap(path, peer).map_err(raised)?;
            }
        }
        let fit = casa_images::analysis::with_mask_output_paths(paths.iter().copied(), || {
            publish_image_set([self.model, self.residual], true, |staged| {
                read_locked(attachment, |source| {
                    for target in &paths {
                        for input in source
                            .path()
                            .into_iter()
                            .chain(attachment.operand_paths().iter().map(|path| path.as_path()))
                        {
                            ensure_artifacts_do_not_overlap(target, input).map_err(raised)?;
                        }
                    }
                    match source {
                        ImageHandle::Float(Storage::Temp {
                            image,
                            fits: Some(fits),
                            ..
                        }) => self.compute(
                            attachment,
                            source,
                            &fits.image_view(image),
                            &paths,
                            staged,
                            history,
                        ),
                        ImageHandle::Float(storage) => with_storage!(storage, image =>
                            self.compute(attachment, source, image.as_ref(), &paths, staged, history)
                        ),
                        ImageHandle::Double(storage) => with_storage!(storage, image =>
                            self.compute(attachment, source, image.as_ref(), &paths, staged, history)
                        ),
                        _ => Err(raised("This method only supports real valued images")),
                    }
                })
            })
        })?;
        self.write_artifacts(&fit)?;
        Ok(fit.record)
    }

    fn compute<T: ComponentFitPixel>(
        &self,
        attachment: &AttachedImage,
        source: &ImageHandle,
        image: &dyn ImageInterface<T>,
        paths: &[&Path],
        staged: [Option<&Path>; 2],
        history: bool,
    ) -> PyResult<ImageComponentFit> {
        let beam_view = attachment.beam_view();
        let beams = beam_view.beam_set().map_err(raised)?;
        let fit = fit_image_components(
            image,
            FitComponentsOptions {
                pixel_box: nonempty(self.pixel_box),
                region: self.region.selection(),
                channels: nonempty(&self.channels),
                stokes: nonempty(self.stokes),
                mask: Some(&self.mask),
                destinations: paths,
                stretch: self.stretch,
                include_pixels: &self.include,
                exclude_pixels: &self.exclude,
                estimates: nonempty(self.estimates).map(Path::new),
                zero_level: self.dooff.then_some((self.offset, self.fixoffset)),
                beams: Some(&beams),
                rms: self.rms.as_ref().map(MeasureArgument::value),
                report_log: !self.logfile.is_empty(),
                noise_fwhm: self.noise.as_ref().map(MeasureArgument::value),
            },
        )
        .map_err(raised)?;
        let [model, residual] = staged.map(|path| {
            path.map(|path| {
                let shape = image.shape().to_vec();
                let tiled =
                    TiledShape::new(shape.clone(), std::mem::size_of::<f32>()).map_err(raised)?;
                PagedImage::<f32>::create_with_tile_shape_and_cache(
                    shape,
                    tiled.tile_shape().to_vec(),
                    image.coordinates().clone(),
                    path,
                    16 * 1024 * 1024,
                )
                .map_err(raised)
            })
            .transpose()
        });
        let mut model = model?;
        let mut residual = residual?;
        fit.write_images(
            image,
            model
                .as_mut()
                .map(|image| image as &mut dyn MutableImageInterface<f32>),
            residual
                .as_mut()
                .map(|image| image as &mut dyn MutableImageInterface<f32>),
        )
        .map_err(raised)?;
        if model.is_some() || residual.is_some() {
            let coordinate_record = source.coordinate_record();
            let mut image_info = image.image_info().map_err(raised)?.to_record();
            beam_view.apply_to(&mut image_info);
            let source_history = source.history_entries()?;
            let call = history.then(|| self.history_call());
            for mut output in [model, residual].into_iter().flatten() {
                output
                    .set_coordinate_record(coordinate_record.clone())
                    .map_err(raised)?;
                output
                    .set_image_info_record(image_info.clone())
                    .map_err(raised)?;
                let mut output = ImageHandle::Float(Storage::Paged(Box::new(output)));
                output.append_history_entries(&source_history)?;
                if let Some(call) = &call {
                    output.record_history("fitcomponents", call.clone())?;
                }
                output.save().map_err(raised)?;
            }
        }
        Ok(fit)
    }

    fn write_artifacts(&self, fit: &ImageComponentFit) -> PyResult<()> {
        if !fit.components.is_empty() {
            if !self.complist.is_empty() && (self.overwrite || !Path::new(self.complist).exists()) {
                publish_image_set([self.complist], self.overwrite, |[path]| {
                    fit.components
                        .write_table(path.expect("component list destination"))
                        .map_err(raised)
                })?;
            }
            if !self.newestimates.is_empty() {
                write_text(self.newestimates, &fit.estimates_text(), false)?;
            }
            if !self.summary.is_empty() {
                write_text(self.summary, &fit.summary_text(), false)?;
            }
        }
        if !self.logfile.is_empty() {
            let text = format!(
                "{}\n{} fitted components\n{}",
                self.history_call(),
                fit.components.len(),
                fit.log_text()
            );
            write_text(self.logfile, &text, self.append)?;
        }
        Ok(())
    }

    fn history_call(&self) -> String {
        let range = |values: &[f64]| {
            format!(
                "[{}]",
                values
                    .iter()
                    .map(|value| format!("{value:.6}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let region = if self.region.selection().is_none() {
            "[]".to_owned()
        } else {
            self.region.history()
        };
        format!(
            "ia.fitcomponents(box={:?}, region={}, chans={}, stokes={:?}, mask={:?}, includepix={}, excludepix={}, residual={:?}, model={:?}, estimates={:?}, logfile={:?}, append={}, newestimates={:?}, complist={:?}, dooff={}, offset={:.6}, fixoffset={}, stretch={}, rms={}, noisefwhm={})",
            self.pixel_box,
            region,
            self.channels_history,
            self.stokes,
            self.mask,
            range(&self.include),
            range(&self.exclude),
            self.residual,
            self.model,
            self.estimates,
            self.logfile,
            self.append,
            self.newestimates,
            self.complist,
            self.dooff,
            self.offset,
            self.fixoffset,
            self.stretch,
            self.rms_history,
            self.noise_history,
        )
    }
}

fn nonempty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn channel_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<(String, String)> {
    let Some(value) = value else {
        return Ok((String::new(), "[]".into()));
    };
    if let Ok(text) = value.extract::<String>() {
        let history = format!("{text:?}");
        return Ok((text, history));
    }
    if !value.is_instance_of::<PyBool>()
        && let Ok(channel) = value.extract::<i64>()
    {
        let text = channel.to_string();
        return Ok((text.clone(), text));
    }
    if value.extract::<Variant>()?.is_empty_sequence()
        || value.try_iter().is_ok_and(|mut values| {
            values.all(|item| item.is_ok_and(|item| item.is_instance_of::<PyBool>()))
        })
    {
        return Ok((String::new(), "[]".into()));
    }
    Err(raised(
        "Unsupported type for chans. chans must be either an integer or a string",
    ))
}

fn measure_argument(
    value: Option<&Bound<'_, PyAny>>,
    name: &str,
) -> PyResult<(Option<MeasureArgument>, String)> {
    let Some(value) = value else {
        return Ok((None, r#""""#.into()));
    };
    let value = value.extract::<Variant>()?;
    let history = input_history(&value);
    let parsed = match &value {
        Variant::Empty => None,
        Variant::Text(text) if text.is_empty() => None,
        Variant::Number(value) => Some(MeasureArgument::Numeric(*value)),
        Variant::Text(_) | Variant::Record(_) => {
            Some(MeasureArgument::Quantity(quantity(&value).map_err(raised)?))
        }
        _ => return Err(raised(format!("Unsupported data type for {name}"))),
    };
    Ok((parsed, history))
}

fn write_text(path: &str, value: &str, append: bool) -> PyResult<()> {
    let mut output = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)
        .map_err(raised)?;
    output.write_all(value.as_bytes()).map_err(raised)
}
