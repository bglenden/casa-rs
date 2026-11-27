// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.fitprofile` wrapper delegating to [`casa_imageanalysis::tool::ops_fit::fit_profile`].

use casa_core::{ArrayD, RecordValue};
use casa_imageanalysis::analysis::{
    ProfileDivisor, ProfileEstimates, ProfileOutputNames, ProfilePeaks,
};
use casa_imageanalysis::tool::ops_fit::{FitProfileRequest, ProfileSigmaInput, fit_profile};
use ndarray::{IxDyn, ShapeBuilder};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{DoubleList, IntList, StringList, Variant, quantity};
use crate::tools::logger;

use super::PyImageTool;
use super::data_compat::record_to_py;
use super::handle::{emit_tool_logs, external_open_check, raised};
use super::subimage::{RegionArgument, mask_argument, require_bool};

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
        if py.detach(|| self.state().image.is_none()) {
            return record_to_py(py, RecordValue::default());
        }
        let Some(sigma) = sigma_argument(sigma)? else {
            return record_to_py(py, RecordValue::default());
        };
        let region_arg: RegionArgument =
            region.map_or_else(|| Ok(RegionArgument::default()), |arg| arg.extract())?;
        let mask = mask_argument(mask)?;
        let good_ranges = [
            range("goodamprange", &goodamprange)?,
            range("goodcenterrange", &goodcenterrange)?,
            range("goodfwhmrange", &goodfwhmrange)?,
        ];
        let divisor = if !spxtype.is_empty() && !spxest.0.is_empty() {
            divisor_argument(div)?
        } else {
            ProfileDivisor::Automatic
        };
        let log_details = logresults
            && logger(py, "casars.tools.image")?
                .bind(py)
                .call_method1("isEnabledFor", (20,))?
                .extract()?;
        let history_call = format!(
            "ia.fitprofile(box={:?}, region={}, chans={:?}, stokes={:?}, axis={}, mask={:?}, ngauss={}, poly={}, estimates={:?}, minpts={}, multifit={}, model={:?}, residual={:?})",
            r#box,
            region_arg.history(),
            chans,
            stokes,
            axis,
            mask,
            ngauss,
            poly,
            estimates,
            minpts,
            multifit,
            model,
            residual,
        );
        let (record, logs) = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(source) = &state.image else {
                return Ok((RecordValue::default(), Vec::new()));
            };
            let request = FitProfileRequest {
                selection: [r#box, chans, stokes],
                region: region_arg.selection(),
                mask: &mask,
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
                good_ranges,
                planes: &planes.0,
                divisor,
                sigma: match &sigma {
                    SigmaArgument::None => ProfileSigmaInput::None,
                    SigmaArgument::Values(v) => ProfileSigmaInput::Values(v),
                    SigmaArgument::Path(p) => ProfileSigmaInput::Path(p),
                },
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
                log_details,
                history_call,
                record_history: state.history,
            };
            fit_profile(source, request, external_open_check()).map_err(raised)
        })?;
        emit_tool_logs(py, &logs)?;
        profile_record(py, record)
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
