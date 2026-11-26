// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python adapter for legacy, calibration-facing imager operations.

mod flux;
mod geometry;
mod options;
pub(crate) mod surface;

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use casa_calibration::{
    AttachmentError, AttachmentOptions, ImagerAttachments, ImagerChannelSelection,
    ImagerLegacyAwOptions, ImagerLegacyPbWOptions, ImagerPrediction, ImagerPredictionModel,
    ImagerSingleDishSetup, ImagerVoltagePattern,
};
use casa_ms::MsSelection;
use pyo3::{
    Borrowed,
    exceptions::{PyRuntimeError, PySystemError, PyTypeError},
    prelude::*,
    types::{PyFloat, PyInt, PyList, PyString},
};

use crate::convert::{Variant, quantity, quantity_value};
use crate::tools::{LogLine, emit};

const LOGGER: &str = "casars.tools.imager";

#[derive(Default)]
struct ImagerState {
    attachments: ImagerAttachments,
    prediction: ImagerPrediction,
    valid: bool,
    uses_scratch: bool,
    sky_model: SkyModelPaths,
}

#[derive(Default)]
struct SkyModelPaths {
    images: Vec<PathBuf>,
    components: Option<PathBuf>,
}

impl ImagerState {
    fn reset_sky_model(&mut self) {
        self.sky_model = SkyModelPaths::default();
        self.prediction.clear_retained_model_sources();
    }
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
            state.reset_sky_model();
            Ok(true)
        })
    }

    #[pyo3(signature = (ftmachine = "ft", cache = -1, tile = 16, gridfunction = "SF", location = Variant::Empty, padding = 1.0, freqinterp = "nearest", wprojplanes = -1, epjtablename = "", applypointingoffsets = false, dopbgriddingcorrections = true, cfcachedirname = "", rotpastep = 5.0, pastep = 360.0, pblimit = 0.05, imagetilevol = options::ImageTileVolume::default(), singleprecisiononly = false, numthreads = -1, psterm = true, aterm = true, mterm = true, wbawp = false, conjbeams = true), text_signature = "($self, ftmachine='ft', cache=-1, tile=16, gridfunction='SF', location=[], padding=1.0, freqinterp='nearest', wprojplanes=-1, epjtablename='', applypointingoffsets=False, dopbgriddingcorrections=True, cfcachedirname='', rotpastep=5.0, pastep=360.0, pblimit=0.05, imagetilevol=0, singleprecisiononly=False, numthreads=-1, psterm=True, aterm=True, mterm=True, wbawp=False, conjbeams=True)")]
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
        imagetilevol: options::ImageTileVolume,
        singleprecisiononly: bool,
        numthreads: i32,
        psterm: bool,
        aterm: bool,
        mterm: bool,
        wbawp: bool,
        conjbeams: bool,
    ) -> PyResult<bool> {
        // In the supported A/W geometry the parallel-hand cell set and
        // prediction agree for either setting of the Mueller term.
        let _ = mterm;
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
            let mosaic = ftmachine.eq_ignore_ascii_case("pbmosaic");
            if mosaic || ftmachine.eq_ignore_ascii_case("pbwproject") {
                state.prediction.configure_legacy_pbw_with_pointing(
                    settings,
                    ImagerLegacyPbWOptions {
                        mosaic,
                        cache_directory: PathBuf::from(cfcachedirname),
                        w_planes: wprojplanes,
                        apply_pointing_offsets: applypointingoffsets,
                        correct_primary_beam: dopbgriddingcorrections,
                        primary_beam_limit: pblimit,
                    },
                    (applypointingoffsets && !epjtablename.is_empty())
                        .then(|| PathBuf::from(epjtablename)),
                )
            } else if matches!(
                ftmachine.to_ascii_lowercase().as_str(),
                "awproject" | "mawproject"
            ) {
                state.prediction.configure_legacy_aw_terms(
                    settings,
                    ImagerLegacyAwOptions {
                        cache_directory: PathBuf::from(cfcachedirname),
                        w_planes: wprojplanes,
                        apply_pointing_offsets: applypointingoffsets,
                        correct_primary_beam: dopbgriddingcorrections,
                        primary_beam_limit: pblimit,
                        pa_step_deg: pastep,
                        rotation_step_deg: rotpastep,
                        conjugate_beams: conjbeams,
                        wideband: wbawp,
                    },
                    [psterm, aterm],
                )
            } else {
                state.prediction.configure_transform(settings)
            }
            .map_err(runtime_error)?;
            state.prediction.set_image_tile_volume(imagetilevol.0);
            Ok(true)
        })
    }

    #[pyo3(signature = (scale = 1.0, weight = 1.0, convsupport = -1, pointingcolumntouse = "DIRECTION", truncate = Variant::Empty, gwidth = Variant::Empty, jwidth = Variant::Empty, minweight = 0.0, clipminmax = false, enablecache = false, convertfirst = "NEVER"), text_signature = "($self, scale=1.0, weight=1.0, convsupport=-1, pointingcolumntouse='DIRECTION', truncate=[], gwidth=[], jwidth=[], minweight=0.0, clipminmax=False, enablecache=False, convertfirst='NEVER')")]
    #[allow(clippy::too_many_arguments)]
    fn setsdoptions(
        &self,
        py: Python<'_>,
        scale: f64,
        weight: f64,
        convsupport: i64,
        pointingcolumntouse: &str,
        truncate: Variant,
        gwidth: Variant,
        jwidth: Variant,
        minweight: f64,
        clipminmax: bool,
        enablecache: bool,
        convertfirst: &str,
    ) -> PyResult<bool> {
        // GridBoth and SDGrid consume these when accumulating data and
        // weights into an image. A forward model prediction samples pixels.
        let _ = (scale, weight, minweight, clipminmax);
        let setup = ImagerSingleDishSetup {
            support: convsupport,
            truncate: quantity(&truncate)?,
            gaussian_width: quantity(&gwidth)?,
            jinc_width: quantity(&jwidth)?,
            pointing_column: pointingcolumntouse.into(),
            cache_pointings: enablecache,
            convert_first: convertfirst.into(),
        };
        self.with_state(py, |state| {
            state
                .prediction
                .configure_single_dish(setup)
                .map_err(runtime_error)?;
            state.reset_sky_model();
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
        // SkyEquation chooses its default pattern from the attached observation.
        let _ = telescope;
        self.with_state(py, |state| {
            let pattern = (dovp || !usedefaultvp).then(|| ImagerVoltagePattern {
                table: (!usedefaultvp).then(|| PathBuf::from(vptable)),
            });
            state
                .prediction
                .configure_voltage_pattern_rotation(
                    pattern,
                    dovp,
                    dosquint,
                    parallactic.to_radians(),
                )
                .map_err(runtime_error)?;
            state.reset_sky_model();
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
            if dosquint {
                lines.push(LogLine::info(
                    "Beam Squint will be included in the VP model",
                ));
                lines.push(LogLine::info(format!(
                    "and the Parallactic Angle increment is {parallactic} degrees"
                )));
            }
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
                        state.prediction.reset_fourier_machine();
                        state.reset_sky_model();
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

    #[pyo3(signature = (field = Variant::Empty, spw = Variant::Empty, modimage = "", fluxdensity = Variant::Numbers(vec![0.; 4]), standard = "SOURCE", scalebychan = false, spix = Variant::Numbers(vec![0.]), reffreq = Variant::Empty, polindex = Variant::Numbers(vec![0.]), polangle = Variant::Numbers(vec![0.]), rotmeas = 0.0, time = "", scan = "", intent = "", observation = "", interpolation = "nearest"), text_signature = "($self, field=[], spw=[], modimage='', fluxdensity=[0.0, 0.0, 0.0, 0.0], standard='SOURCE', scalebychan=False, spix=[0.0], reffreq=[], polindex=[0.0], polangle=[0.0], rotmeas=0.0, time='', scan='', intent='', observation='', interpolation='nearest')")]
    #[allow(clippy::too_many_arguments)]
    fn setjy(
        &self,
        py: Python<'_>,
        field: Variant,
        spw: Variant,
        modimage: &str,
        fluxdensity: Variant,
        standard: &str,
        scalebychan: bool,
        spix: Variant,
        reffreq: Variant,
        polindex: Variant,
        polangle: Variant,
        rotmeas: f64,
        time: &str,
        scan: &str,
        intent: &str,
        observation: &str,
        interpolation: &str,
    ) -> PyResult<Py<PyAny>> {
        use casa_contracts::CasaValue;
        use casa_tasks::calibration::{ImagerSetJyRequest, imager_setjy};

        let request = ImagerSetJyRequest {
            workspace: std::env::current_dir().map_err(runtime_error)?,
            selection: MsSelection {
                field: wildcard_selector(&field),
                spw: wildcard_selector(&spw),
                timerange: nonempty(time),
                scan: nonempty(scan),
                intent: nonempty(intent),
                observation: nonempty(observation),
                ..MsSelection::default()
            },
            image: (!modimage.is_empty()).then(|| PathBuf::from(modimage)),
            flux_jy: flux::flux(fluxdensity)?,
            standard: standard.to_owned(),
            by_channel: scalebychan,
            spectral_index: flux::coefficients(spix, "spix")?,
            reference: flux::reference(&reffreq)?,
            polarization_index: flux::coefficients(polindex, "polindex")?,
            polarization_angle: flux::coefficients(polangle, "polangle")?,
            rotation_measure: rotmeas,
            interpolation: interpolation.to_owned(),
        };
        let answer = self.with_state(py, |state| {
            if !state.valid {
                return Ok(None);
            }
            let answer = imager_setjy(&mut state.attachments, &request).map_err(runtime_error)?;
            if matches!(&answer, CasaValue::Dict { entries } if entries.len() > 1) {
                state.reset_sky_model();
            }
            Ok(Some(answer))
        })?;
        let answer = match answer {
            Some(answer) => answer,
            None => {
                emit(
                    py,
                    LOGGER,
                    &[LogLine::error(
                        "No MeasurementSet has been assigned, please run open.",
                    )],
                )?;
                CasaValue::Dict {
                    entries: Vec::new(),
                }
            }
        };
        crate::convert::reply_value::reply_value(py, &answer)
    }

    #[pyo3(signature = (objname = "", standard = "", epoch = Variant::Empty, freqs = None, pfx = "predictcomp"), text_signature = "($self, objname='', standard='', epoch=[], freqs=[100000000000.0], pfx='predictcomp')")]
    fn predictcomp(
        &self,
        py: Python<'_>,
        objname: &str,
        standard: &str,
        epoch: Variant,
        freqs: Option<Variant>,
        pfx: &str,
    ) -> PyResult<String> {
        use casa_calibration::predict_calibrator_component;
        use casa_core::measures::{EpochRef, MEpoch};

        let epoch = match epoch {
            Variant::Numbers(values) | Variant::Array { values, .. } => Some(MEpoch::from_mjd(
                values.first().copied().unwrap_or_default(),
                EpochRef::UTC,
            )),
            Variant::Bool(flag) => Some(MEpoch::from_mjd(f64::from(flag), EpochRef::UTC)),
            _ => crate::convert::measures::epoch(&epoch)?,
        };
        let Some(epoch) = epoch else {
            return Ok(String::new());
        };
        let frequencies = match freqs {
            Some(Variant::Array { values, .. }) => values,
            Some(freqs) => flux::coefficients(freqs, "freqs")?,
            None => vec![1e11],
        };
        let workspace = std::env::current_dir().map_err(runtime_error)?;
        py.detach(|| {
            let Some(predicted) = predict_calibrator_component(
                &workspace,
                objname,
                standard,
                &epoch,
                &frequencies,
                pfx,
            )
            .map_err(runtime_error)?
            else {
                return Ok(String::new());
            };
            predicted
                .write_if_absent(&workspace)
                .map_err(runtime_error)?;
            Ok(predicted.filename)
        })
    }

    fn ssoflux(&self, py: Python<'_>) -> PyResult<bool> {
        emit(
            py,
            LOGGER,
            &[LogLine::error("ssoflux is no longer supported. Use setjy.")],
        )?;
        Ok(false)
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
        let components = (!complist.is_empty()).then(|| PathBuf::from(complist));
        let mut preparation_error = None;
        let result = self.attached(py, |state| {
            // The sky equation has separate image and component slots.
            // A call can fill either vacant slot and predict both together.
            if (!images.is_empty() && !state.sky_model.images.is_empty())
                || (components.is_some() && state.sky_model.components.is_some())
            {
                return Ok(false);
            }
            let model = ImagerPredictionModel {
                images: if images.is_empty() {
                    &state.sky_model.images
                } else {
                    &images
                },
                components: components
                    .as_deref()
                    .or(state.sky_model.components.as_deref()),
                incremental,
                phase_centre_time: (phasecentertime >= 0.).then_some(phasecentertime),
            };
            let prediction = state.prediction.predict_attached_retaining_sources(
                &state.attachments,
                model,
                state.uses_scratch,
            );
            if matches!(prediction, Ok(true))
                || prediction
                    .as_ref()
                    .is_err_and(ImagerPrediction::retains_prepared_model)
            {
                if !images.is_empty() {
                    state.sky_model.images = images;
                }
                if components.is_some() {
                    state.sky_model.components = components;
                }
            }
            match prediction {
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
