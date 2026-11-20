// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python adapter for legacy, calibration-facing imager operations.

mod geometry;
mod options;
pub(crate) mod surface;

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use casa_calibration::{
    AttachmentError, AttachmentOptions, ImagerAttachments, ImagerChannelSelection,
    ImagerPrediction, ImagerPredictionModel, ImagerVoltagePattern,
};
use casa_ms::MsSelection;
use pyo3::{
    Borrowed,
    exceptions::{PyRuntimeError, PySystemError, PyTypeError},
    prelude::*,
    types::{PyFloat, PyInt, PyList, PyString},
};

use crate::convert::{Variant, quantity_value};
use crate::tools::{LogLine, emit, not_yet_implemented_in};

const LOGGER: &str = "casars.tools.imager";

#[derive(Default)]
struct ImagerState {
    attachments: ImagerAttachments,
    prediction: ImagerPrediction,
    valid: bool,
    uses_scratch: bool,
    model_in_sky_equation: bool,
}

/// CASA's calibration-facing legacy imager.
#[pyclass(name = "imager", module = "casars.tools", frozen)]
pub(crate) struct PyImager {
    state: Mutex<ImagerState>,
}

impl PyImager {
    fn with_state<T: Send>(
        &self,
        py: Python<'_>,
        operation: impl FnOnce(&mut ImagerState) -> PyResult<T> + Send,
    ) -> PyResult<T> {
        py.detach(|| {
            let mut state = self.state.lock().map_err(runtime_error)?;
            operation(&mut state)
        })
    }

    fn attached(
        &self,
        py: Python<'_>,
        operation: impl FnOnce(&mut ImagerState) -> PyResult<bool> + Send,
    ) -> PyResult<bool> {
        let result = self.with_state(py, |state| {
            state.valid.then(|| operation(state)).transpose()
        })?;
        if let Some(result) = result {
            return Ok(result);
        }
        emit(
            py,
            LOGGER,
            &[LogLine::error(
                "No MeasurementSet has been assigned, please run open.",
            )],
        )?;
        Ok(false)
    }
}

#[pymethods]
impl PyImager {
    #[new]
    fn new() -> Self {
        Self {
            state: Mutex::default(),
        }
    }

    #[pyo3(signature = (thems = "", compress = false, usescratch = false), text_signature = "($self, thems='', compress=False, usescratch=False)")]
    fn open(
        &self,
        py: Python<'_>,
        thems: &str,
        compress: bool,
        usescratch: bool,
    ) -> PyResult<bool> {
        // For this legacy tool both compression choices attach the same MODEL_DATA iterator.
        let _ = compress;
        self.with_state(py, |state| {
            *state = ImagerState::default();
            let path = if thems.is_empty() {
                std::env::current_dir().map_err(runtime_error)?
            } else {
                PathBuf::from(thems)
            };
            let write_access = std::fs::OpenOptions::new()
                .write(true)
                .open(path.join("table.dat"))
                .is_ok();
            state
                .attachments
                .open(
                    &path,
                    AttachmentOptions {
                        write_access,
                        use_scratch: usescratch,
                    },
                )
                .map_err(runtime_error)?;
            state.valid = true;
            state.uses_scratch = usescratch;
            Ok(true)
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<bool> {
        self.with_state(py, |state| {
            *state = ImagerState::default();
            Ok(true)
        })
    }

    fn done(&self, py: Python<'_>) -> PyResult<bool> {
        self.close(py)
    }

    #[pyo3(signature = (nx = 128, ny = -1, cellx = Variant::Empty, celly = Variant::Empty, stokes = "I", phasecenter = Variant::Empty, mode = "mfs", nchan = -1, start = Variant::Empty, step = Variant::Empty, spw = ChannelVector::Default, restfreq = Variant::Empty, outframe = "LSRK", veltype = "radio", facets = 1, movingsource = Variant::Empty, distance = Variant::Empty, projection = "SIN"), text_signature = "($self, nx=128, ny=-1, cellx=[], celly=[], stokes='I', phasecenter=[], mode='mfs', nchan=-1, start=[], step=[], spw=[0], restfreq=[], outframe='LSRK', veltype='radio', facets=1, movingsource=[], distance=[], projection='SIN')")]
    #[allow(clippy::too_many_arguments)]
    fn defineimage(
        &self,
        py: Python<'_>,
        nx: i64,
        ny: i64,
        cellx: Variant,
        celly: Variant,
        stokes: &str,
        phasecenter: Variant,
        mode: &str,
        nchan: i64,
        start: Variant,
        step: Variant,
        spw: ChannelVector,
        restfreq: Variant,
        outframe: &str,
        veltype: &str,
        facets: i64,
        movingsource: Variant,
        distance: Variant,
        projection: &str,
    ) -> PyResult<bool> {
        self.attached(py, |state| {
            let request = geometry::ImageArguments {
                size: [nx, ny],
                cell: [&cellx, &celly],
                stokes,
                centre: &phasecenter,
                mode,
                nchan,
                start: &start,
                step: &step,
                windows: spw.values("spw", 0)?,
                rest: &restfreq,
                frame: outframe,
                velocity: veltype,
                facets,
                moving: &movingsource,
                distance: &distance,
                projection,
            }
            .request()?;
            state
                .prediction
                .define_model_geometry(request)
                .map_err(runtime_error)?;
            state.model_in_sky_equation = false;
            Ok(true)
        })
    }

    #[pyo3(signature = (ftmachine = "ft", cache = -1, tile = 16, gridfunction = "SF", location = Variant::Empty, padding = 1.0, freqinterp = "nearest", wprojplanes = -1, epjtablename = "", applypointingoffsets = false, dopbgriddingcorrections = true, cfcachedirname = "", rotpastep = 5.0, pastep = 360.0, pblimit = 0.05, imagetilevol = 0, singleprecisiononly = false, numthreads = -1, psterm = true, aterm = true, mterm = true, wbawp = false, conjbeams = true), text_signature = "($self, ftmachine='ft', cache=-1, tile=16, gridfunction='SF', location=[], padding=1.0, freqinterp='nearest', wprojplanes=-1, epjtablename='', applypointingoffsets=False, dopbgriddingcorrections=True, cfcachedirname='', rotpastep=5.0, pastep=360.0, pblimit=0.05, imagetilevol=0, singleprecisiononly=False, numthreads=-1, psterm=True, aterm=True, mterm=True, wbawp=False, conjbeams=True)")]
    #[allow(clippy::too_many_arguments)]
    fn setoptions(
        &self,
        py: Python<'_>,
        ftmachine: &str,
        cache: i64,
        tile: i32,
        gridfunction: &str,
        location: Variant,
        padding: f64,
        freqinterp: &str,
        wprojplanes: i32,
        epjtablename: &str,
        applypointingoffsets: bool,
        dopbgriddingcorrections: bool,
        cfcachedirname: &str,
        rotpastep: f64,
        pastep: f64,
        pblimit: f64,
        imagetilevol: i64,
        singleprecisiononly: bool,
        numthreads: i32,
        psterm: bool,
        aterm: bool,
        mterm: bool,
        wbawp: bool,
        conjbeams: bool,
    ) -> PyResult<bool> {
        // The A/W controls are consumed by their Fourier machines. Ordinary
        // GridFT and WProjectFT do not use them.
        let _ = (
            epjtablename,
            applypointingoffsets,
            dopbgriddingcorrections,
            cfcachedirname,
            rotpastep,
            pastep,
            pblimit,
            imagetilevol,
            psterm,
            aterm,
            mterm,
            wbawp,
            conjbeams,
        );
        self.attached(py, |state| {
            if !state.prediction.has_image_definition() {
                return Ok(false);
            }
            let settings = options::TransformArguments {
                machine: ftmachine,
                cache,
                tile,
                convolution: gridfunction,
                location: &location,
                padding,
                interpolation: freqinterp,
                planes: wprojplanes,
                single_precision: singleprecisiononly,
                threads: numthreads,
            }
            .options(state.prediction.transform_options())?;
            state
                .prediction
                .configure_transform(settings)
                .map_err(runtime_error)?;
            Ok(true)
        })
    }

    #[pyo3(signature = (dovp = false, usedefaultvp = true, vptable = "", dosquint = false, parangleinc = Variant::Empty, skyposthreshold = Variant::Empty, telescope = "", verbose = true), text_signature = "($self, dovp=False, usedefaultvp=True, vptable='', dosquint=False, parangleinc=[], skyposthreshold=[], telescope='', verbose=True)")]
    #[allow(clippy::too_many_arguments)]
    fn setvp(
        &self,
        py: Python<'_>,
        dovp: bool,
        usedefaultvp: bool,
        vptable: &str,
        dosquint: bool,
        parangleinc: Variant,
        skyposthreshold: Variant,
        telescope: &str,
        verbose: bool,
    ) -> PyResult<bool> {
        let angle = |value: &Variant, default| {
            if value.is_empty_sequence() || matches!(value, Variant::Text(text) if text.is_empty())
            {
                Ok(default)
            } else {
                quantity_value(value, "deg")
            }
        };
        let parallactic = angle(&parangleinc, 360.)?;
        let threshold = angle(&skyposthreshold, 180.)?;
        if dovp && (dosquint || (parallactic, threshold) != (360., 180.)) {
            return not_yet_implemented_in(
                "imager",
                "setvp for varying or squinted voltage patterns",
                1520,
            );
        }
        // SkyEquation chooses its default pattern from the attached observation.
        let _ = telescope;
        self.with_state(py, |state| {
            let pattern = dovp.then(|| ImagerVoltagePattern {
                table: (!usedefaultvp).then(|| PathBuf::from(vptable)),
            });
            state
                .prediction
                .configure_voltage_pattern(pattern)
                .map_err(runtime_error)?;
            state.model_in_sky_equation = false;
            Ok(())
        })?;
        let mut lines = vec![LogLine::info("Setting voltage pattern parameters")];
        if verbose {
            lines.push(LogLine::info(format!(
                "Sky position tolerance is {threshold} degrees"
            )));
            lines.push(LogLine::info(if usedefaultvp {
                "Using system default voltage patterns for each telescope".to_owned()
            } else {
                format!("Using user defined voltage patterns in Table {vptable}")
            }));
        }
        emit(py, LOGGER, &lines)?;
        Ok(true)
    }

    #[pyo3(signature = (vis = "", nchan = ChannelVector::Default, start = ChannelVector::Default, step = ChannelVector::Default, spw = Variant::Empty, field = Variant::Empty, baseline = Variant::Empty, time = Variant::Empty, scan = Variant::Empty, intent = "", observation = Variant::Empty, uvrange = Variant::Empty, taql = "", usescratch = false, datainmemory = false, writeaccess = true), text_signature = "($self, vis='', nchan=[-1], start=[0], step=[1], spw=[], field=[], baseline=[], time=[], scan=[], intent='', observation=[], uvrange=[], taql='', usescratch=False, datainmemory=False, writeaccess=True)")]
    #[allow(clippy::too_many_arguments)]
    fn selectvis(
        &self,
        py: Python<'_>,
        vis: &str,
        nchan: ChannelVector,
        start: ChannelVector,
        step: ChannelVector,
        spw: Variant,
        field: Variant,
        baseline: Variant,
        time: Variant,
        scan: Variant,
        intent: &str,
        observation: Variant,
        uvrange: Variant,
        taql: &str,
        usescratch: bool,
        datainmemory: bool,
        writeaccess: bool,
    ) -> PyResult<bool> {
        let selection = MsSelection {
            spw: wildcard_selector(&spw),
            field: wildcard_selector(&field),
            antenna: wildcard_selector(&baseline),
            timerange: super::calibrater::selector(&time),
            scan: super::calibrater::selector(&scan),
            intent: nonempty(intent),
            observation: super::calibrater::selector(&observation),
            uvrange: super::calibrater::selector(&uvrange),
            msselect: nonempty(taql),
            ..MsSelection::default()
        };
        let channels = ImagerChannelSelection {
            count: nchan.values("nchan", -1)?,
            start: start.values("start", 0)?,
            step: step.values("step", 1)?,
        };
        let result = self.with_state(py, |state| {
            let path = (!vis.is_empty()).then(|| Path::new(vis));
            let explicit_open = state
                .attachments
                .inputs()
                .first()
                .is_some_and(|input| input.mode() == casa_calibration::AttachmentMode::Open);
            if !explicit_open && let Some(path) = path {
                state.uses_scratch = usescratch && !datainmemory;
                if datainmemory {
                    // The pinned Multi-MS memory path fails before attaching.
                    return Err(runtime_error(
                        "NullTable::getColumn - Table object is empty",
                    ));
                }
                if !path.join("table.dat").is_file() {
                    state.valid = false;
                    return Ok(Some(false));
                }
            }
            let options = AttachmentOptions {
                write_access: writeaccess,
                use_scratch: usescratch,
            };
            let selected =
                state
                    .attachments
                    .select_channels(path, options, selection, Some(channels));
            match selected {
                Err(AttachmentError::Unattached) => Ok(None),
                result => {
                    let selected = result.map_err(runtime_error)?;
                    state.valid = selected;
                    if selected {
                        state.model_in_sky_equation = false;
                    }
                    Ok(Some(selected))
                }
            }
        })?;
        if let Some(selected) = result {
            return Ok(selected);
        }
        emit(
            py,
            LOGGER,
            &[LogLine::error(
                "No MeasurementSet has been assigned, please run open or assign an ms here via the vis parameter",
            )],
        )?;
        Ok(false)
    }

    #[pyo3(signature = (uvmin = 0.0, uvmax = 0.0), text_signature = "($self, uvmin=0.0, uvmax=0.0)")]
    fn uvrange(&self, py: Python<'_>, uvmin: f64, uvmax: f64) -> PyResult<bool> {
        self.attached(py, |state| {
            match state.attachments.narrow_uvrange(uvmin, uvmax) {
                Ok(()) => Ok(true),
                Err(AttachmentError::UvRangeRequiresSelection) => Err(runtime_error(
                    "Error in TaQL command: select from $1 where ROWID() IN []",
                )),
                Err(error) => Err(runtime_error(error)),
            }
        })
    }

    #[pyo3(signature = (ntaylorterms = 2, reffreq = 0.0), text_signature = "($self, ntaylorterms=2, reffreq=0.0)")]
    fn settaylorterms(&self, py: Python<'_>, ntaylorterms: i32, reffreq: f64) -> PyResult<bool> {
        self.attached(py, |state| {
            state
                .prediction
                .set_taylor_terms(ntaylorterms.max(1), (reffreq > 0.).then_some(reffreq))
                .map_err(runtime_error)?;
            Ok(true)
        })
    }

    #[pyo3(signature = (model = None, complist = "", incremental = false, phasecentertime = -1.0), text_signature = "($self, model=[], complist='', incremental=False, phasecentertime=-1.0)")]
    fn ft(
        &self,
        py: Python<'_>,
        model: Option<&Bound<'_, PyAny>>,
        complist: &str,
        incremental: bool,
        phasecentertime: f64,
    ) -> PyResult<bool> {
        let images = model_images(model)?;
        let mut preparation_error = None;
        let result = self.attached(py, |state| {
            let has_model = !images.is_empty() || !complist.is_empty();
            // The legacy sky equation rejects a second model addition until
            // the caller rebuilds its data or image selection.
            if state.model_in_sky_equation && has_model {
                return Ok(false);
            }
            let model = ImagerPredictionModel {
                images: &images,
                components: (!complist.is_empty()).then(|| Path::new(complist)),
                incremental,
                phase_centre_time: (phasecentertime >= 0.).then_some(phasecentertime),
            };
            match state
                .prediction
                .predict_attached(&state.attachments, model, state.uses_scratch)
            {
                Ok(true) => {
                    state.model_in_sky_equation |= has_model;
                    Ok(true)
                }
                Err(AttachmentError::PredictionPreparation(error)) => {
                    preparation_error = Some(error.to_string());
                    Ok(false)
                }
                result => result.map_err(runtime_error),
            }
        })?;
        if let Some(error) = preparation_error {
            emit(py, LOGGER, &[LogLine::error(error)])?;
        }
        Ok(result)
    }
}

enum ChannelVector {
    Default,
    Values(Vec<i64>),
    Text,
}

impl ChannelVector {
    fn values(self, name: &str, default: i64) -> PyResult<Vec<i64>> {
        match self {
            Self::Default => Ok(vec![default]),
            Self::Values(values) => Ok(values),
            Self::Text => Err(PyTypeError::new_err(format!(
                "argument _{name} must not be a string"
            ))),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for ChannelVector {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyString>() {
            return Ok(Self::Text);
        }
        if value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>() {
            return Ok(Self::Values(vec![value.extract::<f64>()? as i64]));
        }
        if value.is_instance_of::<PyList>()
            || value
                .getattr("ndim")
                .and_then(|n| n.extract::<usize>())
                .is_ok_and(|n| n > 0)
        {
            let values = value
                .try_iter()?
                .map(|item| Ok(item?.extract::<f64>()? as i64))
                .collect::<PyResult<_>>()?;
            return Ok(Self::Values(values));
        }
        Err(PySystemError::new_err(
            "<built-in function imager_selectvis> returned a result with an exception set",
        ))
    }
}

fn model_images(model: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<PathBuf>> {
    let Some(model) = model else {
        return Ok(Vec::new());
    };
    let images = if let Ok(path) = model.cast::<PyString>() {
        vec![path.to_str()?.to_owned()]
    } else if let Ok(paths) = model.cast::<PyList>() {
        paths
            .iter()
            .map(|path| path.extract::<String>())
            .collect::<PyResult<_>>()?
    } else {
        // CASA's string-vector binding retains the empty default for tuples and arrays.
        Vec::new()
    };
    if matches!(images.as_slice(), [name] if name.is_empty()) {
        return Ok(Vec::new());
    }
    Ok(images.into_iter().map(PathBuf::from).collect())
}

fn wildcard_selector(value: &Variant) -> Option<String> {
    super::calibrater::selector(value).filter(|selector| selector != "-1")
}

fn nonempty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
