// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::analysis::{
    ImageProfileFit, ImageProfileFitResult, ProfileDivisor, ProfileEstimates, ProfileFitOptions,
    ProfileOutputNames, ProfilePeaks, ProfileRegion, ProfileSigma, SubimageRegion,
    ensure_artifacts_do_not_overlap,
};
use casa_images::{Image, ImageInterface};
use casa_types::{ArrayD, ArrayValue, RecordValue, Value};
use ndarray::{Dimension, IxDyn, ShapeBuilder};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{DoubleList, IntList, StringList, Variant, quantity};
use crate::tools::{LogLine, emit, logger};

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::create::NamedImageStages;
use super::data_compat::record_to_py;
use super::handle::{ImageHandle, raised};
use super::subimage::{RegionArgument, mask_argument, require_bool};
use super::text_output::StagedTextOutput;

struct FitReport {
    record: RecordValue,
    logs: Vec<LogLine>,
}

struct ProfileRequest<'a> {
    selection: [&'a str; 3],
    region: RegionArgument,
    mask: String,
    axis: i64,
    ngauss: i64,
    poly: i64,
    min_points: i64,
    estimates_file: &'a str,
    multifit: bool,
    stretch: bool,
    estimates: ProfileEstimates<'a>,
    ranges: [Option<[f64; 2]>; 3],
    planes: &'a [i64],
    divisor: ProfileDivisor,
    sigma: SigmaArgument,
    outputs: ProfileOutputNames<'a>,
    logfile: &'a str,
    append: bool,
    logresults: bool,
}

#[pymethods]
impl PyImageTool {
    /// Fit Gaussian, Lorentzian and continuum models along an image axis.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (r#box="", region=None, chans="", stokes="", axis=-1, mask=None, ngauss=1, poly=-1, estimates="", minpts=1, multifit=false, model="", residual="", amp="", amperr="", center="", centererr="", fwhm="", fwhmerr="", integral="", integralerr="", stretch=false, logresults=true, pampest=DoubleList::of(&[]), pcenterest=DoubleList::of(&[]), pfwhmest=DoubleList::of(&[]), pfix=StringList::of(&[]), gmncomps=IntList::of(&[]), gmampcon=DoubleList::of(&[]), gmcentercon=DoubleList::of(&[]), gmfwhmcon=DoubleList::of(&[]), gmampest=DoubleList::of(&[0.]), gmcenterest=DoubleList::of(&[0.]), gmfwhmest=DoubleList::of(&[0.]), gmfix=StringList::of(&[]), spxtype="", spxest=DoubleList::of(&[]), spxfix=None, div=None, spxsol="", spxerr="", logfile="", append=true, pfunc=StringList::of(&[]), goodamprange=DoubleList::of(&[0.]), goodcenterrange=DoubleList::of(&[0.]), goodfwhmrange=DoubleList::of(&[0.]), sigma=None, outsigma="", planes=IntList::of(&[])), text_signature = "($self, box='', region='', chans='', stokes='', axis=-1, mask='', ngauss=1, poly=-1, estimates='', minpts=1, multifit=False, model='', residual='', amp='', amperr='', center='', centererr='', fwhm='', fwhmerr='', integral='', integralerr='', stretch=False, logresults=True, pampest=[], pcenterest=[], pfwhmest=[], pfix='', gmncomps=0, gmampcon=[], gmcentercon=[], gmfwhmcon=[], gmampest=[0.0], gmcenterest=[0.0], gmfwhmest=[0.0], gmfix='', spxtype='', spxest=[], spxfix=[], div=[], spxsol='', spxerr='', logfile='', append=True, pfunc='', goodamprange=[0.0], goodcenterrange=[0.0], goodfwhmrange=[0.0], sigma='', outsigma='', planes=[])")]
    fn fitprofile<'py>(
        &self,
        py: Python<'py>,
        r#box: &str,
        region: Option<&Bound<'py, PyAny>>,
        chans: &str,
        stokes: &str,
        axis: i64,
        mask: Option<&Bound<'py, PyAny>>,
        ngauss: i64,
        poly: i64,
        estimates: &str,
        minpts: i64,
        #[pyo3(from_py_with = require_bool)] multifit: bool,
        model: &str,
        residual: &str,
        amp: &str,
        amperr: &str,
        center: &str,
        centererr: &str,
        fwhm: &str,
        fwhmerr: &str,
        integral: &str,
        integralerr: &str,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        #[pyo3(from_py_with = require_bool)] logresults: bool,
        pampest: DoubleList,
        pcenterest: DoubleList,
        pfwhmest: DoubleList,
        pfix: StringList,
        gmncomps: IntList,
        gmampcon: DoubleList,
        gmcentercon: DoubleList,
        gmfwhmcon: DoubleList,
        gmampest: DoubleList,
        gmcenterest: DoubleList,
        gmfwhmest: DoubleList,
        gmfix: StringList,
        spxtype: &str,
        spxest: DoubleList,
        spxfix: Option<Vec<bool>>,
        div: Option<Variant>,
        spxsol: &str,
        spxerr: &str,
        logfile: &str,
        #[pyo3(from_py_with = require_bool)] append: bool,
        pfunc: StringList,
        goodamprange: DoubleList,
        goodcenterrange: DoubleList,
        goodfwhmrange: DoubleList,
        sigma: Option<Variant>,
        outsigma: &str,
        planes: IntList,
    ) -> PyResult<Py<PyAny>> {
        let image_type = py.detach(|| {
            self.state()
                .image
                .as_ref()
                .map(|image| matches!(*image.lock(), ImageHandle::Float(_)))
        });
        match image_type {
            None => return record_to_py(py, RecordValue::default()),
            Some(false) => return Err(raised("This method only supports Float valued images")),
            Some(true) => {}
        }
        let Some(sigma) = sigma_argument(sigma)? else {
            return record_to_py(py, RecordValue::default());
        };
        let request = ProfileRequest {
            selection: [r#box, chans, stokes],
            region: region.map_or_else(
                || Ok(RegionArgument::default()),
                |argument| argument.extract(),
            )?,
            mask: mask_argument(mask)?,
            axis,
            ngauss,
            poly,
            min_points: minpts,
            estimates_file: estimates,
            multifit,
            stretch,
            estimates: ProfileEstimates {
                singlets: peaks(&pampest, &pcenterest, &pfwhmest, &pfix),
                functions: list_or_empty(&pfunc),
                multiplets: peaks(&gmampest, &gmcenterest, &gmfwhmest, &gmfix),
                multiplet_sizes: if gmncomps.0 == [0] { &[] } else { &gmncomps.0 },
                multiplet_ties: [&gmampcon.0, &gmcentercon.0, &gmfwhmcon.0],
                spectral_kind: spxtype,
                spectral_values: &spxest.0,
                spectral_fixed: spxfix.as_deref().unwrap_or_default(),
            },
            ranges: [
                range("goodamprange", &goodamprange)?,
                range("goodcenterrange", &goodcenterrange)?,
                range("goodfwhmrange", &goodfwhmrange)?,
            ],
            planes: &planes.0,
            divisor: if !spxtype.is_empty() && !spxest.0.is_empty() {
                divisor_argument(div)?
            } else {
                ProfileDivisor::Automatic
            },
            sigma,
            outputs: ProfileOutputNames {
                spectra: [model, residual],
                lines: [
                    amp,
                    amperr,
                    center,
                    centererr,
                    fwhm,
                    fwhmerr,
                    integral,
                    integralerr,
                ],
                coefficients: [spxsol, spxerr],
                sigma: outsigma,
            },
            logfile,
            append,
            logresults: logresults
                && logger(py, "casars.tools.image")?
                    .bind(py)
                    .call_method1("isEnabledFor", (20,))?
                    .extract()?,
        };
        let result = py.detach(|| {
            let state = self.state();
            match &state.image {
                Some(source) => request.run(source, state.history),
                None => Ok(FitReport {
                    record: RecordValue::default(),
                    logs: Vec::new(),
                }),
            }
        })?;
        emit(py, "casars.tools.image", &result.logs)?;
        profile_record(py, result.record)
    }
}

impl ProfileRequest<'_> {
    fn run(self, attachment: &AttachedImage, history: bool) -> PyResult<FitReport> {
        let sigma = match &self.sigma {
            SigmaArgument::Path(path) => Some(Image::open(path).map_err(raised)?),
            _ => None,
        };
        validate_log(self.logfile)?;
        let (stages, fit) = read_locked(attachment, |source| match source {
            ImageHandle::Float(storage) => storage.with_float_view(|image| {
                self.compute(attachment, source, image, sigma.as_ref(), history)
            }),
            _ => Err(raised("This method only supports Float valued images")),
        })?;
        let mut logs: Vec<_> = fit.summary.iter().cloned().map(LogLine::info).collect();
        if self.ngauss < 0 {
            logs.push(LogLine::warning(
                "ngauss < 0 is meaningless. Setting ngauss = 0",
            ));
        }
        if !self.outputs.sigma.is_empty() && matches!(self.sigma, SigmaArgument::None) {
            logs.push(LogLine::warning(
                "Ignoring outsigma because no sigma image or array was specified",
            ));
        }
        let details = (self.logresults || !self.logfile.is_empty()).then(|| self.log_text(&fit));
        let staged_log = if let Some(details) = &details
            && !self.logfile.is_empty()
        {
            let text = format!("{}\n\n{details}", fit.summary.join("\n"));
            Some(StagedTextOutput::new(
                Path::new(self.logfile),
                &text,
                self.append,
            )?)
        } else {
            None
        };
        stages.publish_with(true, || {
            staged_log.map_or(Ok(()), StagedTextOutput::install)
        })?;
        if let Some(details) = details
            && self.logresults
        {
            logs.push(LogLine::info(details));
        }
        Ok(FitReport {
            record: fit.record,
            logs,
        })
    }

    fn compute(
        &self,
        attachment: &AttachedImage,
        source: &ImageHandle,
        image: &dyn ImageInterface<f32>,
        sigma: Option<&Image>,
        history: bool,
    ) -> PyResult<(NamedImageStages, ImageProfileFitResult)> {
        let mut beam_record = image.image_info().map_err(raised)?.to_record();
        attachment.beam_view().apply_to(&mut beam_record);
        let [pixel_box, channels, stokes] = self.selection;
        let region = self.region.selection();
        let region_file = region.and_then(SubimageRegion::external_path);
        let fit = ImageProfileFit::new(
            image,
            source.coordinate_record(),
            ProfileFitOptions {
                selection: ProfileRegion {
                    pixel_box: nonempty(pixel_box),
                    region,
                    chans: nonempty(channels),
                    stokes: nonempty(stokes),
                },
                axis: self.axis,
                mask: nonempty(&self.mask),
                stretch: self.stretch,
                multifit: self.multifit,
                min_points: self.min_points.max(0) as usize,
                gaussians: self.ngauss.max(0) as usize,
                polynomial: usize::try_from(self.poly).ok(),
                estimates_file: nonempty(self.estimates_file).map(Path::new),
                estimates: self.estimates,
                good_ranges: self.ranges,
                planes: self.planes,
                divisor: self.divisor.clone(),
                sigma: match &self.sigma {
                    SigmaArgument::Values(values) => Some(ProfileSigma::Values(values)),
                    SigmaArgument::Path(_) => sigma.map(|sigma| ProfileSigma::Image(sigma)),
                    SigmaArgument::None => None,
                },
                other_artifact: nonempty(self.logfile).map(Path::new),
                beam_record: Some(&beam_record),
                log_polynomials: self.logresults || !self.logfile.is_empty(),
            },
            self.outputs,
        )
        .map_err(raised)?;
        validate_paths(
            fit.destinations().chain(nonempty(self.logfile)),
            source
                .path()
                .into_iter()
                .chain(attachment.operand_paths().iter().map(|path| path.as_path()))
                .chain(sigma.and_then(ImageInterface::name))
                .chain(nonempty(self.estimates_file).map(Path::new))
                .chain(region_file),
        )?;
        casa_images::analysis::protect_composed_mask_outputs(
            &self.mask,
            fit.destinations()
                .chain(nonempty(self.logfile))
                .map(Path::new),
        )
        .map_err(raised)?;
        let output_is_spectrum: Vec<_> = fit
            .destinations()
            .map(|name| self.outputs.spectra.contains(&name) || self.outputs.sigma == name)
            .collect();
        let stages = NamedImageStages::with_replacements(
            fit.destinations()
                .map(|name| (name, name == self.outputs.sigma)),
        )?;
        let paths = stages.paths();
        let fit = fit.fit(&paths).map_err(raised)?;
        {
            let mut paths = paths
                .into_iter()
                .zip(output_is_spectrum)
                .filter_map(|(path, with_history)| path.filter(|_| with_history))
                .peekable();
            let prior = if paths.peek().is_some() {
                source.history_entries()?
            } else {
                Vec::new()
            };
            for path in paths {
                let mut image = Image::open(path).map_err(raised)?;
                if image.history().map_err(raised)?.is_empty() {
                    image.append_history_entries(&prior).map_err(raised)?;
                }
                if history {
                    image
                        .append_history_at(
                            "::image::fitprofile",
                            &["Ran ia.fitprofile".into(), self.history_call()],
                        )
                        .map_err(raised)?;
                }
                image
                    .append_history_at("::ImageProfileFitter::fit", &fit.summary)
                    .map_err(raised)?;
                image.save().map_err(raised)?;
            }
        }
        Ok((stages, fit))
    }

    fn history_call(&self) -> String {
        format!(
            "ia.fitprofile(box={:?}, region={}, chans={:?}, stokes={:?}, axis={}, mask={:?}, ngauss={}, poly={}, estimates={:?}, minpts={}, multifit={}, model={:?}, residual={:?})",
            self.selection[0],
            self.region.history(),
            self.selection[1],
            self.selection[2],
            self.axis,
            self.mask,
            self.ngauss,
            self.poly,
            self.estimates_file,
            self.min_points,
            self.multifit,
            self.outputs.spectra[0],
            self.outputs.spectra[1],
        )
    }

    fn log_text(&self, fit: &ImageProfileFitResult) -> String {
        use std::fmt::Write as _;

        let mut text = format!("Input parameters ---\n{}\n", self.history_call());
        let record = &fit.record;
        let Some(Value::Array(ArrayValue::Bool(valid))) = record.get("valid") else {
            return text;
        };
        for (pixel, &ok) in valid.indexed_iter() {
            let position = pixel.slice();
            let _ = writeln!(
                text,
                "Fit at pixel {position:?}\n    Valid: {}",
                if ok { "YES" } else { "NO" }
            );
            if !ok {
                continue;
            }
            if let Some(polynomial) = fit.polynomials.get(position) {
                let _ = writeln!(text, "    Type: POLYNOMIAL");
                for (unit, terms) in [("pixel", &polynomial.pixel), ("world", &polynomial.world)] {
                    let _ = writeln!(text, "        Coefficients in {unit} coordinates");
                    for (index, &[value, error]) in terms.iter().enumerate() {
                        let _ = writeln!(text, "        c{index} : {value:.12e} +/- {error:.8e}");
                    }
                }
            }
            for field in record.fields() {
                let Value::Record(model) = &field.value else {
                    continue;
                };
                let _ = writeln!(text, "    {}", field.name);
                for field in model.fields() {
                    let Value::Array(ArrayValue::Float64(values)) = &field.value else {
                        continue;
                    };
                    let mut index = position.to_vec();
                    index.push(0);
                    let count = values.shape()[values.ndim() - 1];
                    let _ = write!(text, "        {}", field.name);
                    for component in 0..count {
                        index[position.len()] = component;
                        let _ = write!(text, "  {:.8e}", values[IxDyn(&index)]);
                    }
                    text.push('\n');
                }
            }
        }
        text
    }
}

fn peaks<'a>(
    amp: &'a DoubleList,
    center: &'a DoubleList,
    width: &'a DoubleList,
    fixed: &'a StringList,
) -> ProfilePeaks<'a> {
    ProfilePeaks {
        amplitudes: &amp.0,
        centers: &center.0,
        widths: &width.0,
        fixed: list_or_empty(fixed),
    }
}

fn list_or_empty(values: &StringList) -> &[String] {
    if values.0.len() == 1 && values.0[0].is_empty() {
        &[]
    } else {
        &values.0
    }
}

fn range(name: &str, values: &DoubleList) -> PyResult<Option<[f64; 2]>> {
    match values.0.as_slice() {
        [first, last] => Ok(Some([*first, *last])),
        [] | [_] => Ok(None),
        _ => Err(raised(format!("Too many elements in {name}"))),
    }
}

enum SigmaArgument {
    None,
    Values(ArrayD<f32>),
    Path(String),
}

fn sigma_argument(value: Option<Variant>) -> PyResult<Option<SigmaArgument>> {
    let (shape, values) = match value.unwrap_or(Variant::Empty) {
        Variant::Empty => return Ok(Some(SigmaArgument::None)),
        Variant::Text(path) if path.is_empty() => return Ok(Some(SigmaArgument::None)),
        Variant::Text(path) => return Ok(Some(SigmaArgument::Path(path))),
        Variant::Numbers(values) => (vec![values.len()], values),
        Variant::Array { shape, values } => (shape, values),
        _ => return Ok(None),
    };
    let values = values.into_iter().map(|value| value as f32).collect();
    let array = ArrayD::from_shape_vec(IxDyn(&shape).f(), values).map_err(raised)?;
    Ok(Some(SigmaArgument::Values(array)))
}

fn divisor_argument(value: Option<Variant>) -> PyResult<ProfileDivisor> {
    match value.unwrap_or(Variant::Empty) {
        Variant::Empty => Ok(ProfileDivisor::Automatic),
        Variant::Number(value) => Ok(ProfileDivisor::Number(value)),
        value @ (Variant::Text(_) | Variant::Record(_)) => {
            Ok(ProfileDivisor::Quantity(quantity(&value).map_err(raised)?))
        }
        value => Err(raised(format!(
            "Unsupported type for div: {}",
            value.describe()
        ))),
    }
}

fn profile_record(py: Python<'_>, record: RecordValue) -> PyResult<Py<PyAny>> {
    let record = record_to_py(py, record)?;
    let dict = record.bind(py).cast::<PyDict>()?;
    for key in ["direction", "type"] {
        if let Some(value) = dict.get_item(key)? {
            let kwargs = PyDict::new(py);
            kwargs.set_item("dtype", "str")?;
            let array = py
                .import("numpy")?
                .call_method("asarray", (value,), Some(&kwargs))?;
            dict.set_item(key, array)?;
        }
    }
    Ok(record)
}

fn validate_paths<'a, 'b>(
    outputs: impl IntoIterator<Item = &'a str>,
    inputs: impl IntoIterator<Item = &'b Path>,
) -> PyResult<()> {
    let outputs: Vec<_> = outputs.into_iter().map(Path::new).collect();
    let inputs: Vec<_> = inputs.into_iter().collect();
    for (index, path) in outputs.iter().enumerate() {
        for other in inputs
            .iter()
            .copied()
            .chain(outputs[..index].iter().copied())
        {
            ensure_artifacts_do_not_overlap(path, other).map_err(raised)?;
        }
    }
    Ok(())
}

fn validate_log(path: &str) -> PyResult<()> {
    if let Some(path) = nonempty(path).map(Path::new) {
        if path.is_dir() {
            return Err(raised(format!(
                "Log destination {} is a directory",
                path.display()
            )));
        }
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
            && !parent.is_dir()
        {
            return Err(raised(format!(
                "Log directory {} does not exist",
                parent.display()
            )));
        }
    }
    Ok(())
}

fn nonempty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}
