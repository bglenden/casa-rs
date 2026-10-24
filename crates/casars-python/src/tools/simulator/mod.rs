// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars.tools.simulator`: CASA's `simulator` tool (`sm`) over
//! [`casa_simulation::Simulator`].
//!
//! The class declares every method of CASA's installed binding with CASA's
//! names, parameter order and effective defaults ([`surface`]), and each
//! method delegates to a free function: the session's lifecycle in
//! `lifecycle`, the setters and `observe` in `setup`, prediction
//! (`predict`, `setoptions`, `setvp`) in `prediction`, thermal noise
//! (`setseed`, `setnoise`, `corrupt`, `reset`) in `corruption`, and the
//! methods casa-rs does not implement yet, or that CASA disables, in `stubs`.
//!
//! CASA's outcomes are kept: before `open` every method but `type`, `close`
//! and `done` returns `False` (or `''`) without converting its arguments, and
//! `setconfig` also logs "The sm tool must be opened first."; a failure CASA
//! reports by logging and returning `False` is logged at ERROR on the Python
//! logger `casars.tools.simulator`; a failure CASA raises is a
//! `RuntimeError`. What CASA does and casa-rs does not yet (a whole method,
//! or a mode such as `setnoise(mode='tsys-atm')`) raises
//! `NotImplementedError` naming the phase that implements it.
//!
//! Divergence: the MeasurementSet is written at `close` or `done` (and by an
//! `open` that replaces the session), never while observing, so `predict`
//! prepares the model and `corrupt` records the noise, and `close` writes
//! both. A tool dropped with planned
//! scans and no `close` writes nothing, where CASA leaves the partly written
//! MS on disk; the drop emits a `ResourceWarning`.

mod corruption;
mod gains;
mod lifecycle;
mod prediction;
mod setup;
mod stubs;
pub(crate) mod surface;

use std::ffi::CString;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use casa_simulation::{CasaOutcome, SessionResources, Simulator, SimulatorError, SimulatorResult};
use casa_types::measures::MeasuresProvider;
use pyo3::exceptions::{PyNotImplementedError, PyResourceWarning, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;

use crate::convert::{DoubleList, IntList, StringList, Variant};
use crate::tools::{LogLine, emit};

/// The Python logger CASA's `sm` messages go to.
const LOGGER: &str = "casars.tools.simulator";

/// An open session: the simulator and the measures data it was opened with.
pub(crate) struct Session {
    simulator: Simulator,
    measures: Arc<dyn MeasuresProvider>,
}

/// CASA's `simulator` tool.
///
/// `simulator(*, memory_budget=None, backend='auto', workers=None)` takes
/// only casa-rs's controls, keyword-only (ADR-0019 item 3):
/// - `memory_budget` caps, in bytes, what a session holds at once while
///   `close` writes: the MeasurementSet writer gets half of it and the
///   geometry and noise the rest. `None` takes three quarters of the
///   machine's free memory.
/// - `backend` is the device to predict on: `'auto'` (the machine's Metal
///   device when it has one, else the CPU), `'cpu'` or `'metal'`; `'metal'`
///   on a machine without a Metal device raises `RuntimeError`. Noise is
///   computed on the CPU whatever the backend.
/// - `workers` is the number of threads `close` keeps busy, the writer's
///   among them; `None` takes three quarters of the machine's threads, at
///   most its performance cores.
///
/// The MeasurementSet is bitwise the same for any budget or workers, and
/// for any backend when nothing is predicted; a Metal prediction agrees with
/// the CPU's to `1e-4` of the largest visibility.
#[pyclass(name = "simulator", module = "casars.tools", frozen)]
pub(crate) struct PySimulator {
    state: Mutex<Option<Session>>,
    resources: SessionResources,
}

impl PySimulator {
    /// The session lock, taken without the GIL (inside `Python::detach`).
    fn lock(&self) -> MutexGuard<'_, Option<Session>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The session lock, taken with the GIL attached; waiting for it
    /// releases the GIL.
    fn lock_attached(&self, py: Python<'_>) -> MutexGuard<'_, Option<Session>> {
        self.state
            .lock_py_attached(py)
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs `work` on the open session with the GIL released, and logs what
    /// it reports once the GIL is back. Before `open`, returns `None` without
    /// running it.
    ///
    /// The method has converted its Python arguments to Rust values (the
    /// signature's `Variant`s and lists) before it calls this; `work` turns
    /// them into the session's types, which can need the session's measures,
    /// and does the session's work, all without the GIL (ADR-0019 §4). An
    /// error it returns is created lazily and raised once the GIL is back.
    fn on_session<T: Send>(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut Session, &mut Vec<LogLine>) -> PyResult<T> + Send,
    ) -> PyResult<Option<T>> {
        let (result, lines) = py.detach(|| {
            let mut lines = Vec::new();
            let result = match self.lock().as_mut() {
                Some(session) => work(session, &mut lines).map(Some),
                None => Ok(None),
            };
            (result, lines)
        });
        emit(py, LOGGER, &lines)?;
        result
    }

    /// Whether a session is open.
    fn is_open(&self, py: Python<'_>) -> bool {
        self.lock_attached(py).is_some()
    }
}

/// CASA's outcome for a session result: `True`, `False` with the message
/// logged at ERROR, or a `RuntimeError`; or `NotImplementedError` for what
/// casa-rs does not do yet.
fn outcome(result: SimulatorResult<()>, lines: &mut Vec<LogLine>) -> PyResult<bool> {
    match result {
        Ok(()) => Ok(true),
        Err(error) => match error.outcome() {
            CasaOutcome::ReturnsFalse => {
                lines.push(LogLine::error(error.message()));
                Ok(false)
            }
            CasaOutcome::Raises => Err(raised(&error)),
            CasaOutcome::NotYetImplemented => {
                Err(PyNotImplementedError::new_err(error.message().to_string()))
            }
        },
    }
}

/// A session error as the `RuntimeError` CASA raises.
fn raised(error: &SimulatorError) -> PyErr {
    PyRuntimeError::new_err(error.message().to_string())
}

impl Drop for PySimulator {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        let Some(session) = state.take() else {
            return;
        };
        let scans = session.simulator.scan_count();
        if scans == 0 {
            return;
        }
        let message = format!(
            "simulator for {} was dropped with {scans} planned scan(s) and no close(); \
             nothing was written (casa-rs writes the MeasurementSet at close or done)",
            session.simulator.name().display()
        );
        Python::attach(|py| {
            let category = py.get_type::<PyResourceWarning>();
            let message = CString::new(message).unwrap_or_default();
            if let Err(error) = PyErr::warn(py, category.as_any(), &message, 1) {
                error.write_unraisable(py, None);
            }
        });
    }
}

#[pymethods]
impl PySimulator {
    #[new]
    #[pyo3(signature = (*, memory_budget = None, backend = "auto", workers = None))]
    fn new(memory_budget: Option<i64>, backend: &str, workers: Option<i64>) -> PyResult<Self> {
        let controls = lifecycle::execution_controls(memory_budget, backend, workers)?;
        let resources =
            SessionResources::from_controls(&controls).map_err(|error| raised(&error))?;
        Ok(Self {
            state: Mutex::new(None),
            resources,
        })
    }

    /// Start a new MeasurementSet `ms`, closing (writing) any open one.
    #[pyo3(signature = (ms = ""), text_signature = "($self, ms='')")]
    fn open(&self, py: Python<'_>, ms: &str) -> PyResult<bool> {
        lifecycle::open(self, py, ms)
    }

    /// Write the MeasurementSet and end the session.
    fn close(&self, py: Python<'_>) -> PyResult<bool> {
        lifecycle::close(self, py)
    }

    /// Write the MeasurementSet and end the session (as `close`).
    fn done(&self, py: Python<'_>) -> PyResult<bool> {
        lifecycle::close(self, py)
    }

    /// The MeasurementSet's name, or `''` before `open`.
    fn name(&self, py: Python<'_>) -> String {
        lifecycle::name(self, py)
    }

    /// Log a summary of the session; `False` before `open`.
    fn summary(&self, py: Python<'_>) -> PyResult<bool> {
        lifecycle::summary(self, py)
    }

    /// The tool type, `'Simulator'`.
    #[pyo3(name = "type")]
    fn tool_type(&self) -> &'static str {
        "Simulator"
    }

    /// Set the integration time and the reference time of `observe`.
    #[pyo3(
        signature = (integrationtime = Variant::Empty, usehourangle = true, referencetime = Variant::Empty),
        text_signature = "($self, integrationtime=[], usehourangle=True, referencetime=[])"
    )]
    fn settimes(
        &self,
        py: Python<'_>,
        integrationtime: Variant,
        usehourangle: bool,
        referencetime: Variant,
    ) -> PyResult<bool> {
        setup::settimes(self, py, &integrationtime, usehourangle, &referencetime)
    }

    /// Plan a scan of `sourcename` in spectral window `spwname`.
    #[allow(clippy::too_many_arguments, reason = "CASA's observe signature")]
    #[pyo3(
        signature = (
            sourcename = "", spwname = "", starttime = Variant::Empty, stoptime = Variant::Empty,
            add_observation = false, state_sig = true, state_ref = false, state_cal = 0.0,
            state_load = 0.0, state_sub_scan = 0, state_obs_mode = "OBSERVE_TARGET.ON_SOURCE",
            observer = "CASA simulator", project = "CASA simulation"
        ),
        text_signature = "($self, sourcename='', spwname='', starttime=[], stoptime=[], add_observation=False, state_sig=True, state_ref=False, state_cal=0.0, state_load=0.0, state_sub_scan=0, state_obs_mode='OBSERVE_TARGET.ON_SOURCE', observer='CASA simulator', project='CASA simulation')"
    )]
    fn observe(
        &self,
        py: Python<'_>,
        sourcename: &str,
        spwname: &str,
        starttime: Variant,
        stoptime: Variant,
        add_observation: bool,
        state_sig: bool,
        state_ref: bool,
        state_cal: f64,
        state_load: f64,
        state_sub_scan: i64,
        state_obs_mode: &str,
        observer: &str,
        project: &str,
    ) -> PyResult<bool> {
        let request = setup::ObserveRequest {
            source: sourcename,
            spectral_window: spwname,
            start: starttime,
            stop: stoptime,
            add_observation,
            state: setup::StateRequest {
                sig: state_sig,
                reference: state_ref,
                cal: state_cal,
                load: state_load,
                sub_scan: state_sub_scan,
                obs_mode: state_obs_mode,
            },
            observer,
            project,
        };
        setup::observe(self, py, &request)
    }

    /// Set the shadowing and elevation limits of `observe`.
    #[pyo3(
        signature = (shadowlimit = 1e-6, elevationlimit = Variant::Empty),
        text_signature = "($self, shadowlimit=1e-06, elevationlimit=[])"
    )]
    fn setlimits(
        &self,
        py: Python<'_>,
        shadowlimit: f64,
        elevationlimit: Variant,
    ) -> PyResult<bool> {
        setup::setlimits(self, py, shadowlimit, &elevationlimit)
    }

    /// Set the autocorrelation weight; autocorrelations are written when it
    /// is positive.
    #[pyo3(signature = (autocorrwt = 0.0), text_signature = "($self, autocorrwt=0.0)")]
    fn setauto(&self, py: Python<'_>, autocorrwt: f64) -> PyResult<bool> {
        setup::setauto(self, py, autocorrwt)
    }

    /// Add antennas.
    #[allow(clippy::too_many_arguments, reason = "CASA's setconfig signature")]
    #[pyo3(
        signature = (
            telescopename = "VLA", x = DoubleList::of(&[0.0]), y = DoubleList::of(&[0.0]),
            z = DoubleList::of(&[0.0]), dishdiameter = DoubleList::of(&[0.0]),
            offset = DoubleList::of(&[0.0]), mount = StringList::of(&["ALT-AZ"]),
            antname = StringList::of(&["A"]), padname = StringList::of(&["P"]),
            coordsystem = "global", referencelocation = Variant::Empty
        ),
        text_signature = "($self, telescopename='VLA', x=[0.0], y=[0.0], z=[0.0], dishdiameter=[0.0], offset=[0.0], mount=['ALT-AZ'], antname=['A'], padname=['P'], coordsystem='global', referencelocation=[])"
    )]
    fn setconfig(
        &self,
        py: Python<'_>,
        telescopename: &str,
        x: DoubleList,
        y: DoubleList,
        z: DoubleList,
        dishdiameter: DoubleList,
        offset: DoubleList,
        mount: StringList,
        antname: StringList,
        padname: StringList,
        coordsystem: &str,
        referencelocation: Variant,
    ) -> PyResult<bool> {
        let request = setup::ConfigRequest {
            telescope: telescopename,
            x: x.0,
            y: y.0,
            z: z.0,
            dish_diameter: dishdiameter.0,
            offset: offset.0,
            mount: mount.0,
            antenna_names: antname.0,
            pad_names: padname.0,
            coordinate_system: coordsystem,
            reference_location: referencelocation,
        };
        setup::setconfig(self, py, request)
    }

    /// Add one perfect feed per antenna.
    #[pyo3(
        signature = (mode = "", x = DoubleList::of(&[0.0]), y = DoubleList::of(&[0.0]), pol = StringList::of(&["R"])),
        text_signature = "($self, mode='', x=[0.0], y=[0.0], pol=['R'])"
    )]
    fn setfeed(
        &self,
        py: Python<'_>,
        mode: &str,
        x: DoubleList,
        y: DoubleList,
        pol: StringList,
    ) -> PyResult<bool> {
        setup::setfeed(self, py, mode, &x.0, &y.0, &pol.0)
    }

    /// Add a field and its source.
    #[pyo3(
        signature = (sourcename = "SOURCE", sourcedirection = Variant::Empty, calcode = "", distance = Variant::Empty),
        text_signature = "($self, sourcename='SOURCE', sourcedirection=[], calcode='', distance=[])"
    )]
    fn setfield(
        &self,
        py: Python<'_>,
        sourcename: &str,
        sourcedirection: Variant,
        calcode: &str,
        distance: Variant,
    ) -> PyResult<bool> {
        setup::setfield(self, py, sourcename, &sourcedirection, calcode, &distance)
    }

    /// Add a spectral window and its polarization setup.
    #[allow(clippy::too_many_arguments, reason = "CASA's setspwindow signature")]
    #[pyo3(
        signature = (
            spwname = "XBAND", freq = Variant::Empty, deltafreq = Variant::Empty,
            freqresolution = Variant::Empty, refcode = "TOPO", nchannels = 1, stokes = "RR LL"
        ),
        text_signature = "($self, spwname='XBAND', freq=[], deltafreq=[], freqresolution=[], refcode='TOPO', nchannels=1, stokes='RR LL')"
    )]
    fn setspwindow(
        &self,
        py: Python<'_>,
        spwname: &str,
        freq: Variant,
        deltafreq: Variant,
        freqresolution: Variant,
        refcode: &str,
        nchannels: i64,
        stokes: &str,
    ) -> PyResult<bool> {
        let request = setup::WindowRequest {
            name: spwname,
            frequency: freq,
            width: deltafreq,
            resolution: freqresolution,
            frame: refcode,
            channels: nchannels,
            stokes,
        };
        setup::setspwindow(self, py, &request)
    }

    /// Open the existing MeasurementSet `ms`, closing (writing) any open
    /// one; `predict` and `corrupt` then rewrite its rows at `close`.
    #[pyo3(signature = (ms = ""), text_signature = "($self, ms='')")]
    fn openfromms(&self, py: Python<'_>, ms: &str) -> PyResult<bool> {
        lifecycle::openfromms(self, py, ms)
    }

    /// Plan several pointings: a scan for each run of pointings of one
    /// field, each pointing's phase centre its direction where one is given.
    #[allow(clippy::too_many_arguments, reason = "CASA's observemany signature")]
    #[pyo3(
        signature = (
            sourcenames = StringList::of(&[]), spwname = "", starttimes = StringList::of(&["0s"]),
            stoptimes = StringList::of(&["3600s"]), directions = StringList::of(&[""]),
            add_observation = false, state_sig = true, state_ref = false, state_cal = 0.0,
            state_load = 0.0, state_sub_scan = 0, state_obs_mode = "OBSERVE_TARGET#ON_SOURCE",
            observer = "CASA simulator", project = "CASA simulation"
        ),
        text_signature = "($self, sourcenames=[], spwname='', starttimes=['0s'], stoptimes=['3600s'], directions=[''], add_observation=False, state_sig=True, state_ref=False, state_cal=0.0, state_load=0.0, state_sub_scan=0, state_obs_mode='OBSERVE_TARGET#ON_SOURCE', observer='CASA simulator', project='CASA simulation')"
    )]
    fn observemany(
        &self,
        py: Python<'_>,
        sourcenames: StringList,
        spwname: &str,
        starttimes: StringList,
        stoptimes: StringList,
        directions: StringList,
        add_observation: bool,
        state_sig: bool,
        state_ref: bool,
        state_cal: f64,
        state_load: f64,
        state_sub_scan: i64,
        state_obs_mode: &str,
        observer: &str,
        project: &str,
    ) -> PyResult<bool> {
        let request = setup::ObserveManyRequest {
            sources: sourcenames.0,
            spectral_window: spwname,
            starts: starttimes.0,
            stops: stoptimes.0,
            directions: directions.0,
            add_observation,
            state: setup::StateRequest {
                sig: state_sig,
                reference: state_ref,
                cal: state_cal,
                load: state_load,
                sub_scan: state_sub_scan,
                obs_mode: state_obs_mode,
            },
            observer,
            project,
        };
        setup::observemany(self, py, &request)
    }

    /// Add an `xmosp × ymosp` grid of fields, `mosspacing` apart, centred on
    /// `fieldcenter`; casa-rs centres the grid where CASA's sits off its
    /// centre (`docs/CASA (C++) bugs.md`).
    #[allow(clippy::too_many_arguments, reason = "CASA's setmosaicfield signature")]
    #[pyo3(
        signature = (
            sourcename = "SOURCE", calcode = "", fieldcenter = Variant::Empty, xmosp = 1,
            ymosp = 1, mosspacing = Variant::Empty, distance = Variant::Empty
        ),
        text_signature = "($self, sourcename='SOURCE', calcode='', fieldcenter=[], xmosp=1, ymosp=1, mosspacing=[], distance=[])"
    )]
    fn setmosaicfield(
        &self,
        py: Python<'_>,
        sourcename: &str,
        calcode: &str,
        fieldcenter: Variant,
        xmosp: i64,
        ymosp: i64,
        mosspacing: Variant,
        distance: Variant,
    ) -> PyResult<bool> {
        let request = setup::MosaicRequest {
            name: sourcename,
            calcode,
            centre: fieldcenter,
            counts: (xmosp, ymosp),
            spacing: mosspacing,
            distance,
        };
        setup::setmosaicfield(self, py, &request)
    }

    /// Select the rows the next `predict` and `corrupt` act on, until the
    /// next `observe`. casa-rs takes a selection that keeps every row
    /// observed so far (`simobserve`'s); one that keeps fewer, or a TaQL
    /// `msselect`, raises `NotImplementedError`.
    #[pyo3(
        signature = (spwid = IntList::of(&[0]), fieldid = IntList::of(&[0]), msselect = ""),
        text_signature = "($self, spwid=[0], fieldid=[0], msselect='')"
    )]
    fn setdata(
        &self,
        py: Python<'_>,
        spwid: IntList,
        fieldid: IntList,
        msselect: &str,
    ) -> PyResult<bool> {
        let result = self.on_session(py, |session, lines| {
            outcome(
                session.simulator.set_data(&spwid.0, &fieldid.0, msselect),
                lines,
            )
        })?;
        Ok(result.unwrap_or(false))
    }

    /// Predict the model image and the component list into the rows observed
    /// so far, replacing their DATA or, with `incremental`, adding to it;
    /// written at `close`. One CASA image (Stokes I, J2000) and a CASA
    /// component list of J2000 points, Gaussians and disks are implemented;
    /// several images, a limb-darkened disk or another frame raise
    /// `NotImplementedError`.
    #[pyo3(
        signature = (imagename = StringList::of(&[]), complist = "", incremental = false),
        text_signature = "($self, imagename=[], complist='', incremental=False)"
    )]
    fn predict(
        &self,
        py: Python<'_>,
        imagename: StringList,
        complist: &str,
        incremental: bool,
    ) -> PyResult<bool> {
        prediction::predict(self, py, imagename.0, complist, incremental)
    }

    /// Set how `predict` grids: `GridFT` (`ftmachine='ft'`) with the
    /// spheroidal `gridfunction='SF'` and its `padding`, `WProjectFT`
    /// (`ftmachine='wproject'`) with `wprojplanes` planes, which CASA pads
    /// by 1 whatever `padding` says, and total power through `SDGrid`
    /// (`ftmachine='sd'`) with `gridfunction` `'PB'`, `'BOX'` or `'SF'`, are
    /// implemented. `WProjectFT` and `SDGrid` predict on the CPU. Other
    /// FTMachines and grid functions raise `NotImplementedError`.
    #[allow(unused_variables, clippy::too_many_arguments)]
    #[pyo3(
        signature = (
            ftmachine = "ft", cache = 0, tile = 16, gridfunction = "SF",
            location = Variant::Empty, padding = 1.3, facets = 1, maxdata = 2000.0,
            wprojplanes = 1
        ),
        text_signature = "($self, ftmachine='ft', cache=0, tile=16, gridfunction='SF', location=[], padding=1.3, facets=1, maxdata=2000.0, wprojplanes=1)"
    )]
    fn setoptions(
        &self,
        py: Python<'_>,
        ftmachine: &str,
        cache: i64,
        tile: i64,
        gridfunction: &str,
        location: Variant,
        padding: f64,
        facets: i64,
        maxdata: f64,
        wprojplanes: i64,
    ) -> PyResult<bool> {
        let request = prediction::OptionsRequest {
            ftmachine,
            grid_function: gridfunction,
            location,
            padding,
            w_planes: wprojplanes,
        };
        prediction::setoptions(self, py, &request)
    }

    /// Set whether `predict` applies the primary beam (`dovp`): the
    /// telescope's default voltage pattern, implemented for ALMA, ACA, the
    /// VLA (by band) and the EVLA.
    /// A voltage-pattern table raises `NotImplementedError`.
    #[allow(unused_variables, clippy::too_many_arguments)]
    #[pyo3(
        signature = (
            dovp = true, usedefaultvp = true, vptable = "", dosquint = true,
            parangleinc = Variant::Empty, skyposthreshold = Variant::Empty, pblimit = 0.01
        ),
        text_signature = "($self, dovp=True, usedefaultvp=True, vptable='', dosquint=True, parangleinc=[], skyposthreshold=[], pblimit=0.01)"
    )]
    fn setvp(
        &self,
        py: Python<'_>,
        dovp: bool,
        usedefaultvp: bool,
        vptable: &str,
        dosquint: bool,
        parangleinc: Variant,
        skyposthreshold: Variant,
        pblimit: f64,
    ) -> PyResult<bool> {
        let request = prediction::VoltagePatternRequest {
            dovp,
            use_default_vp: usedefaultvp,
            vp_table: vptable,
            do_squint: dosquint,
            parallactic_angle_increment: parangleinc,
            sky_position_threshold: skyposthreshold,
        };
        prediction::setvp(self, py, &request)
    }

    /// Apply every noise term to the rows observed so far; the noise is
    /// written at `close`.
    fn corrupt(&self, py: Python<'_>) -> PyResult<bool> {
        corruption::corrupt(self, py)
    }

    /// Remove every noise, gain and leakage term; what `corrupt` already
    /// applied stays.
    fn reset(&self, py: Python<'_>) -> PyResult<bool> {
        corruption::reset(self, py)
    }

    /// Simulate antenna gains over the rows observed so far, as fractional
    /// Brownian motion of `amplitude` (`mode='fbm'`) or drawn afresh each
    /// integration about one (`mode='random'`), for `corrupt` to apply.
    /// Writing `table` raises `NotImplementedError`.
    #[pyo3(
        signature = (
            mode = "fbm", table = "", interval = Variant::Empty,
            amplitude = DoubleList::of(&[0.01])
        ),
        text_signature = "($self, mode='fbm', table='', interval=[], amplitude=[0.01])"
    )]
    fn setgain(
        &self,
        py: Python<'_>,
        mode: &str,
        table: &str,
        interval: Variant,
        amplitude: DoubleList,
    ) -> PyResult<bool> {
        let request = gains::GainRequest {
            mode,
            table,
            interval,
            amplitude: amplitude.0,
        };
        gains::setgain(self, py, request)
    }

    /// Simulate polarization leakages over the rows observed so far, of
    /// `amplitude` about `offset`, for `corrupt` to apply. Writing `table`
    /// raises `NotImplementedError`.
    #[allow(unused_variables)]
    #[pyo3(
        signature = (
            mode = "constant", table = "", amplitude = DoubleList::of(&[0.01]),
            offset = DoubleList::of(&[0.0])
        ),
        text_signature = "($self, mode='constant', table='', amplitude=[0.01], offset=[0.0])"
    )]
    fn setleakage(
        &self,
        py: Python<'_>,
        mode: &str,
        table: &str,
        amplitude: DoubleList,
        offset: DoubleList,
    ) -> PyResult<bool> {
        gains::setleakage(self, py, (table, amplitude.0, offset.0))
    }

    /// Disabled in CASA: logs CASA's error and returns `False`.
    #[allow(unused_variables)]
    #[pyo3(
        signature = (mode = None, table = None, interval = None, amplitude = None),
        text_signature = "($self, mode='calculate', table='', interval=[], amplitude=[0.0])"
    )]
    fn setbandpass(
        &self,
        py: Python<'_>,
        mode: Option<&Bound<'_, PyAny>>,
        table: Option<&Bound<'_, PyAny>>,
        interval: Option<&Bound<'_, PyAny>>,
        amplitude: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        stubs::disabled_in_casa(self, py)
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables, clippy::too_many_arguments)]
    #[pyo3(
        signature = (
            table = None, r#type = None, t = None, field = None, interp = None, calwt = None,
            spwmap = None, opacity = None
        ),
        text_signature = "($self, table='', type='', t=0.0, field=[], interp='linear', calwt=False, spwmap=[-1], opacity=0.0)"
    )]
    fn setapply(
        &self,
        table: Option<&Bound<'_, PyAny>>,
        r#type: Option<&Bound<'_, PyAny>>,
        t: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        interp: Option<&Bound<'_, PyAny>>,
        calwt: Option<&Bound<'_, PyAny>>,
        spwmap: Option<&Bound<'_, PyAny>>,
        opacity: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        stubs::not_yet_implemented("setapply")
    }

    /// Not yet implemented in casa-rs.
    #[allow(unused_variables, clippy::too_many_arguments)]
    #[pyo3(
        signature = (
            mode = None, table = None, pwv = None, deltapwv = None, beta = None,
            windspeed = None, simint = None
        ),
        text_signature = "($self, mode='screen', table='', pwv=3.0, deltapwv=0.15, beta=1.1, windspeed=7.0, simint=-1.0)"
    )]
    fn settrop(
        &self,
        mode: Option<&Bound<'_, PyAny>>,
        table: Option<&Bound<'_, PyAny>>,
        pwv: Option<&Bound<'_, PyAny>>,
        deltapwv: Option<&Bound<'_, PyAny>>,
        beta: Option<&Bound<'_, PyAny>>,
        windspeed: Option<&Bound<'_, PyAny>>,
        simint: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        stubs::not_yet_implemented("settrop")
    }

    /// Disabled in CASA: logs CASA's error and returns `False`.
    #[allow(unused_variables)]
    #[pyo3(
        signature = (epjtablename = None, applypointingoffsets = None, dopbcorrection = None),
        text_signature = "($self, epjtablename='', applypointingoffsets=False, dopbcorrection=False)"
    )]
    fn setpointingerror(
        &self,
        py: Python<'_>,
        epjtablename: Option<&Bound<'_, PyAny>>,
        applypointingoffsets: Option<&Bound<'_, PyAny>>,
        dopbcorrection: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        stubs::disabled_in_casa(self, py)
    }

    /// Deprecated in CASA; casa-rs provides `setnoise` instead.
    #[allow(unused_variables, clippy::too_many_arguments)]
    #[pyo3(
        signature = (
            mode = None, table = None, simplenoise = None, antefficiency = None,
            correfficiency = None, spillefficiency = None, tau = None, trx = None, tatmos = None,
            tcmb = None
        ),
        text_signature = "($self, mode='calculate', table='', simplenoise=[], antefficiency=0.8, correfficiency=0.85, spillefficiency=0.85, tau=0.1, trx=50.0, tatmos=230.0, tcmb=2.7)"
    )]
    fn oldsetnoise(
        &self,
        mode: Option<&Bound<'_, PyAny>>,
        table: Option<&Bound<'_, PyAny>>,
        simplenoise: Option<&Bound<'_, PyAny>>,
        antefficiency: Option<&Bound<'_, PyAny>>,
        correfficiency: Option<&Bound<'_, PyAny>>,
        spillefficiency: Option<&Bound<'_, PyAny>>,
        tau: Option<&Bound<'_, PyAny>>,
        trx: Option<&Bound<'_, PyAny>>,
        tatmos: Option<&Bound<'_, PyAny>>,
        tcmb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        stubs::deprecated_oldsetnoise()
    }

    /// Add a noise term with the current seed. `mode='simplenoise'` adds
    /// σ = `simplenoise` (Jy) to each real and imaginary part;
    /// `mode='tsys-manual'` adds the σ of each row's system temperature
    /// (`tau`, `tatmos`, the efficiencies, `trx`, `tground`, `tcmb`,
    /// `senscoeff`, `rxtype`, the row's dishes, elevations, channel width
    /// and integration time). `tsys-atm`, `OTF=False`, and `tsys-manual`
    /// with a `table`, raise `NotImplementedError`.
    #[allow(unused_variables, non_snake_case, clippy::too_many_arguments)]
    #[pyo3(
        signature = (
            mode = "simplenoise", table = "", simplenoise = Variant::Empty,
            pground = Variant::Empty, relhum = 20.0, altitude = Variant::Empty,
            waterheight = Variant::Empty, pwv = Variant::Empty, tatmos = 250.0, tau = 0.1,
            antefficiency = 0.8, spillefficiency = 0.85, correfficiency = 0.88, trx = 50.0,
            tground = 270.0, tcmb = 2.73, OTF = true, senscoeff = 0.0, rxtype = 0
        ),
        text_signature = "($self, mode='simplenoise', table='', simplenoise=[], pground=[], relhum=20.0, altitude=[], waterheight=[], pwv=[], tatmos=250.0, tau=0.1, antefficiency=0.8, spillefficiency=0.85, correfficiency=0.88, trx=50.0, tground=270.0, tcmb=2.73, OTF=True, senscoeff=0.0, rxtype=0)"
    )]
    fn setnoise(
        &self,
        py: Python<'_>,
        mode: &str,
        table: &str,
        simplenoise: Variant,
        pground: Variant,
        relhum: f64,
        altitude: Variant,
        waterheight: Variant,
        pwv: Variant,
        tatmos: f64,
        tau: f64,
        antefficiency: f64,
        spillefficiency: f64,
        correfficiency: f64,
        trx: f64,
        tground: f64,
        tcmb: f64,
        OTF: bool,
        senscoeff: f64,
        rxtype: i64,
    ) -> PyResult<bool> {
        let request = corruption::NoiseRequest {
            mode,
            table,
            simple_noise: simplenoise,
            ground_pressure: pground,
            altitude,
            water_height: waterheight,
            water_vapour: pwv,
            tsys: casa_simulation::TsysParameters {
                tau,
                atmosphere_k: tatmos,
                antenna_efficiency: antefficiency,
                spillover_efficiency: spillefficiency,
                correlator_efficiency: correfficiency,
                receiver_k: trx,
                ground_k: tground,
                cmb_k: tcmb,
                sensitivity: senscoeff,
                receiver_type: rxtype,
            },
            on_the_fly: OTF,
        };
        corruption::setnoise(self, py, &request)
    }

    /// Disabled in CASA: logs CASA's error and returns `False`.
    #[allow(unused_variables)]
    #[pyo3(
        signature = (mode = None, table = None, interval = None),
        text_signature = "($self, mode='calculate', table='', interval=[])"
    )]
    fn setpa(
        &self,
        py: Python<'_>,
        mode: Option<&Bound<'_, PyAny>>,
        table: Option<&Bound<'_, PyAny>>,
        interval: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        stubs::disabled_in_casa(self, py)
    }

    /// Set the seed of the noise terms set from now on (CASA's is 11111
    /// until this is called).
    #[pyo3(signature = (seed = 185_349_251), text_signature = "($self, seed=185349251)")]
    fn setseed(&self, py: Python<'_>, seed: i64) -> PyResult<bool> {
        corruption::setseed(self, py, seed)
    }
}
