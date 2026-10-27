// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars.simutil.simutil`: the parts of CASA's `simutil` helper class
//! (`casatasks.private.simutil`) that a `sm` script or `simobserve` uses,
//! over [`casa_simulation::readantenna`] and [`casa_simulation::simutil`].
//!
//! `simutil` is a Python class of CASA's tasks, not a CASA tool, so it is
//! not under `casars.tools`; `casars.simutil` is its public import (CASA's
//! is `casatasks.private.simutil`). ADR-0018 puts its computation in Rust,
//! so the class is declared here like the tools, with CASA's constructor
//! and method signatures, and its state: `direction`, `verbose`,
//! `centerfreq`, `bandwidth` and `totaltime` from the constructor, the
//! `pointings` that `calc_pointings2` and `read_pointings` set, and the
//! `telescopename` that `noisetemp` reads when it is given no telescope:
//! `None` until `readantenna` (or `baselineLengths` or `approxBeam`, which
//! read a configuration with it) sets it, or a caller does; CASA's class
//! has no such attribute until then.
//!
//! `direction` is a string or a list of them, and the methods keep the two
//! apart as CASA does: a list, even of one direction, is split at its mean,
//! which CASA writes at precision 5 and reads back
//! ([`casa_simulation::simutil::direction::Directions`]).
//!
//! Declared so far (simulation Phases 5a and 5b): `readantenna`; the
//! direction helpers `direction_splitter`, `average_direction`,
//! `median_direction`, `dir_m2s`, `wrapang`, `isquantity` and
//! `isdirection`; the pointings `calc_pointings2`, `read_pointings` and
//! `write_pointings`; `noisetemp`, `bandname` and `polsettings`;
//! `itrf2loc`, `baselineLengths` and `approxBeam`; and the sky-model
//! helpers `modifymodel` and `flatimage`
//! ([`casa_simulation::simutil::skymodel`]).
//!
//! Not being a CASA tool, the class is not in the object-surface bundle
//! (whose CASA tools are [`crate::tools::TOOLS`]); its Python tests compare
//! its signatures with CASA's installed class
//! (`python/tests/data/casa_simutil_signatures.json`).
//!
//! Results take CASA's shapes: lists of `str`, tuples, quantity records
//! (`{'unit': 'deg', 'value': ...}`) and NumPy arrays where CASA returns
//! them (the direction offsets, `itrf2loc`'s and `baselineLengths`'
//! values). A refusal CASA raises through `simutil.msg(...,
//! priority='error')` (an `Exception` with CASA's message) is a
//! `RuntimeError` here, and a file that cannot be opened an `OSError`.
//!
//! `readantenna(antab)` returns CASA's tuple `(x, y, z, d, padnames,
//! antnames, telescopename, posobs)`: ITRF x, y and z as lists of `float`,
//! the dish diameters as a NumPy array, the pad and antenna names as lists
//! of `str`, the telescope name and the observatory's WGS84 position
//! record. [`casa_simulation::readantenna`] records where its reading
//! differs from CASA's; also, CASA's LOC files give the positions as NumPy
//! `float64` scalars, which here are `float`s.

mod ops;

use std::path::PathBuf;

use casa_simulation::readantenna::{AntennaConfigFile, ReadAntennaError, read_antenna_config};
use casa_simulation::simutil::baselines::{approximate_beam, baseline_lengths, itrf_to_local};
use casa_simulation::simutil::direction::{
    Directions, average_direction, format_direction, median_direction, wrap_angle,
};
use casa_simulation::simutil::pointings::{calc_pointings, format_pointings, parse_pointings};
use casa_simulation::simutil::receivers::{band_name, noise_temperature, polarization_settings};
use casa_simulation::simutil::skymodel::{ModelOverrides, SkyModelError, flat_image, modify_model};
use pyo3::exceptions::{
    PyAttributeError, PyFileNotFoundError, PyOSError, PyRuntimeError, PyTypeError,
};
use pyo3::prelude::*;
use pyo3::types::PyString;

use crate::convert::holder::{CasaMeasure, Measure};
use crate::convert::value::CasaValue;
use crate::convert::{DoubleList, StringList, Variant};
use crate::tools::{LogLine, emit};

/// The Python logger `simutil`'s messages go to.
const LOGGER: &str = "casars.simutil";

/// CASA's `simutil` helper class.
#[pyclass(name = "simutil", module = "casars.simutil")]
pub(crate) struct PySimutil {
    /// The direction string, or list of them, the methods default to.
    #[pyo3(get, set)]
    direction: Py<PyAny>,
    #[pyo3(get, set)]
    verbose: bool,
    /// The centre frequency `noisetemp` defaults to.
    #[pyo3(get, set)]
    centerfreq: Py<PyAny>,
    #[pyo3(get, set)]
    bandwidth: Py<PyAny>,
    #[pyo3(get, set)]
    totaltime: Py<PyAny>,
    /// The last pointings `calc_pointings2` or `read_pointings` gave.
    #[pyo3(get, set)]
    pointings: Option<Py<PyAny>>,
    /// The telescope `noisetemp` defaults to.
    #[pyo3(get, set)]
    telescopename: Option<Py<PyAny>>,
}

/// A configuration-file error as the Python exception CASA's raises.
fn raised(error: &ReadAntennaError) -> PyErr {
    match error {
        ReadAntennaError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound => {
            PyFileNotFoundError::new_err(error.to_string())
        }
        ReadAntennaError::Io { .. } => PyOSError::new_err(error.to_string()),
        _ => PyRuntimeError::new_err(error.to_string()),
    }
}

/// A file error as Python's `open` raises it.
fn file_error(path: &std::path::Path, error: &std::io::Error) -> PyErr {
    let message = format!("{error}: '{}'", path.display());
    if error.kind() == std::io::ErrorKind::NotFound {
        PyFileNotFoundError::new_err(message)
    } else {
        PyOSError::new_err(message)
    }
}

/// An antenna configuration read with casa-rs's measures data.
fn configuration(path: &std::path::Path) -> PyResult<AntennaConfigFile> {
    let measures = casa_ms::open_measures_runtime()
        .map_err(|error| PyRuntimeError::new_err(format!("measures data: {error}")))?;
    read_antenna_config(path, &*measures).map_err(|error| raised(&error))
}

#[pymethods]
impl PySimutil {
    #[new]
    #[pyo3(
        signature = (
            direction = None,
            centerfreq = None,
            bandwidth = None,
            totaltime = None,
            verbose = false
        ),
        text_signature = "(direction='', centerfreq={'unit': 'GHz', 'value': 245.0}, bandwidth={'unit': 'GHz', 'value': 1.0}, totaltime={'unit': 'h', 'value': 0.0}, verbose=False)"
    )]
    fn new(
        py: Python<'_>,
        direction: Option<Py<PyAny>>,
        centerfreq: Option<Py<PyAny>>,
        bandwidth: Option<Py<PyAny>>,
        totaltime: Option<Py<PyAny>>,
        verbose: bool,
    ) -> PyResult<Self> {
        let or_quantity = |given: Option<Py<PyAny>>, value: f64, unit: &str| match given {
            Some(given) => Ok(given),
            None => CasaValue::quantity(value, unit).into_python(py),
        };
        Ok(Self {
            direction: direction.unwrap_or_else(|| PyString::new(py, "").into_any().unbind()),
            verbose,
            centerfreq: or_quantity(centerfreq, 245.0, "GHz")?,
            bandwidth: or_quantity(bandwidth, 1.0, "GHz")?,
            totaltime: or_quantity(totaltime, 0.0, "h")?,
            pointings: None,
            telescopename: None,
        })
    }

    /// Read an antenna configuration file: `(x, y, z, d, padnames,
    /// antnames, telescopename, posobs)`. The telescope is kept as
    /// `self.telescopename`, as in CASA.
    #[pyo3(signature = (antab = None), text_signature = "($self, antab=None)")]
    fn readantenna(&mut self, py: Python<'_>, antab: Option<PathBuf>) -> PyResult<Py<PyAny>> {
        let Some(path) = antab else {
            return Err(PyTypeError::new_err(
                "readantenna needs the antenna configuration file (antab)",
            ));
        };
        emit(
            py,
            LOGGER,
            &[LogLine::info(format!(
                "Reading antenna positions from '{}'",
                path.display()
            ))],
        )?;
        // The file is read and CASA's tuple built without the GIL; only the
        // Python objects are made with it.
        let (telescope, value) = py.detach(move || -> PyResult<(String, CasaValue)> {
            let config = configuration(&path)?;
            Ok((config.telescope.clone(), readantenna_value(config)))
        })?;
        self.keep_telescope(py, telescope);
        value.into_python(py)
    }

    /// Pointings for a mosaic of `size` with pointings `spacing` apart about
    /// `direction` (else `self.direction`), from the top row. The direction
    /// is checked first, as CASA's `isdirection` checks it; a list of more
    /// than one is then returned as it is.
    #[pyo3(
        signature = (spacing, size, maptype = "hex".to_string(), direction = None, relmargin = 0.5, beam = 0.0),
        text_signature = "($self, spacing, size, maptype='hex', direction=None, relmargin=0.5, beam=0.0)"
    )]
    #[allow(clippy::too_many_arguments)]
    fn calc_pointings2(
        &mut self,
        py: Python<'_>,
        spacing: Variant,
        size: &Bound<'_, PyAny>,
        maptype: String,
        direction: Option<Py<PyAny>>,
        relmargin: f64,
        beam: f64,
    ) -> PyResult<Py<PyAny>> {
        let sizes = ops::sizes(size)?;
        if let Some(direction) = direction {
            self.direction = direction;
        }
        let directions = ops::directions(self.direction.bind(py))?;
        if py.detach(|| directions.split()).is_err() {
            return Err(PyRuntimeError::new_err(format!(
                "can't interpret '{}' as a direction",
                self.direction.bind(py).str()?
            )));
        }
        if matches!(&directions, Directions::List(listed) if listed.len() > 1) {
            return Ok(self.direction.clone_ref(py));
        }
        let spacing = ops::quantity(&spacing)?;
        let pointings = py
            .detach(|| {
                calc_pointings(
                    &spacing,
                    [&sizes[0], &sizes[1]],
                    &maptype,
                    &directions,
                    relmargin,
                    beam,
                )
            })
            .map_err(ops::raised)?;
        emit(
            py,
            LOGGER,
            &[LogLine::info(format!(
                "using {} generated pointing(s)",
                pointings.len()
            ))],
        )?;
        let value = ops::texts(pointings).into_python(py)?;
        self.pointings = Some(value.clone_ref(py));
        Ok(value)
    }

    /// Read a pointing file: `(count, pointings, times)`.
    #[pyo3(signature = (filename), text_signature = "($self, filename)")]
    fn read_pointings(&mut self, py: Python<'_>, filename: PathBuf) -> PyResult<Py<PyAny>> {
        let file = py.detach(|| -> PyResult<_> {
            let text = std::fs::read_to_string(&filename)
                .map_err(|error| file_error(&filename, &error))?;
            parse_pointings(&text).map_err(ops::raised)
        })?;
        emit(
            py,
            LOGGER,
            &[LogLine::info(format!(
                "read in {} pointing(s) from file",
                file.pointings.len()
            ))],
        )?;
        let value = CasaValue::Tuple(vec![
            CasaValue::Int(file.pointings.len() as i64),
            ops::texts(file.pointings),
            CasaValue::List(file.times.into_iter().map(CasaValue::Float).collect()),
        ])
        .into_python(py)?;
        self.pointings = Some(value.bind(py).get_item(1)?.unbind());
        Ok(value)
    }

    /// Write a pointing file: each pointing, then its time (one time for
    /// every pointing, or one each), as Python writes them.
    #[pyo3(signature = (filename, pointings, time = None), text_signature = "($self, filename, pointings, time=1.0)")]
    fn write_pointings(
        &self,
        py: Python<'_>,
        filename: PathBuf,
        pointings: StringList,
        time: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let times = match time {
            Some(time) => ops::times(time)?,
            None => vec!["1.0".to_string()],
        };
        let text = format_pointings(&pointings.0, &times).map_err(ops::raised)?;
        py.detach(|| std::fs::write(&filename, text).map_err(|error| file_error(&filename, &error)))
    }

    /// The mean of `directions` (else `self.direction`): `(direction,
    /// offsets)`, the offsets in degrees on the sky, shape (2, n).
    #[pyo3(signature = (directions = None), text_signature = "($self, directions=None)")]
    fn average_direction(
        &self,
        py: Python<'_>,
        directions: Option<StringList>,
    ) -> PyResult<Py<PyAny>> {
        let directions = match directions {
            Some(directions) => directions.0,
            None => ops::listed(ops::directions(self.direction.bind(py))?),
        };
        let centred = py
            .detach(|| average_direction(&directions))
            .map_err(ops::raised)?;
        ops::centred(centred).into_python(py)
    }

    /// The median of `directions` (else `self.direction`), as
    /// [`Self::average_direction`].
    #[pyo3(signature = (directions = None), text_signature = "($self, directions=None)")]
    fn median_direction(
        &self,
        py: Python<'_>,
        directions: Option<StringList>,
    ) -> PyResult<Py<PyAny>> {
        let directions = match directions {
            Some(directions) => directions.0,
            None => ops::listed(ops::directions(self.direction.bind(py))?),
        };
        let centred = py
            .detach(|| median_direction(&directions))
            .map_err(ops::raised)?;
        ops::centred(centred).into_python(py)
    }

    /// A direction's `(epoch, longitude, latitude)`: the epoch with a space
    /// after it (or `''`), the angles as quantities in degrees. A list of
    /// directions, even of one, is split at its mean.
    #[pyo3(signature = (direction = None), text_signature = "($self, direction=None)")]
    fn direction_splitter(
        &self,
        py: Python<'_>,
        direction: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let directions = ops::directions(direction.as_ref().unwrap_or(self.direction.bind(py)))?;
        let split = py.detach(|| directions.split()).map_err(ops::raised)?;
        let angle = |value: &casa_types::quanta::Quantity| {
            CasaValue::quantity(value.value(), value.unit().name())
        };
        CasaValue::Tuple(vec![
            CasaValue::Text(split.epoch),
            angle(&split.longitude),
            angle(&split.latitude),
        ])
        .into_python(py)
    }

    /// A direction measure as a direction string.
    #[pyo3(signature = (dir), text_signature = "($self, dir)")]
    fn dir_m2s(&self, py: Python<'_>, dir: Variant) -> PyResult<Py<PyAny>> {
        let Variant::Record(record) = &dir else {
            return Err(PyRuntimeError::new_err("converting direction measure"));
        };
        if record.get("type") != Some(&Variant::Text("direction".to_string())) {
            return Err(PyRuntimeError::new_err("converting direction measure"));
        }
        let field = |key: &str| {
            record.get(key).ok_or_else(|| {
                PyRuntimeError::new_err(format!("the direction measure has no {key}"))
            })
        };
        let Variant::Text(refer) = field("refer")? else {
            return Err(PyRuntimeError::new_err(
                "the direction measure's refer is not a string",
            ));
        };
        let (longitude, latitude) = (ops::quantity(field("m0")?)?, ops::quantity(field("m1")?)?);
        let text = format_direction(refer, &longitude, &latitude).map_err(ops::raised)?;
        CasaValue::Text(text).into_python(py)
    }

    /// `ang` moved by whole periods to within half a period of `target`.
    #[pyo3(signature = (ang, target, period = 360.0), text_signature = "($self, ang, target, period=360.0)")]
    fn wrapang(&self, ang: f64, target: f64, period: f64) -> f64 {
        wrap_angle(ang, target, period)
    }

    /// Antenna efficiencies and receiver temperature: `(eta_p, eta_s,
    /// eta_b, eta_t, eta_q, t_rx)`. The telescope defaults to
    /// `self.telescopename`, the frequency to `self.centerfreq`.
    #[pyo3(
        signature = (telescope = None, freq = None, diam = None, epsilon = None, correlator = None),
        text_signature = "($self, telescope=None, freq=None, diam=None, epsilon=None, correlator=None)"
    )]
    fn noisetemp(
        &self,
        py: Python<'_>,
        telescope: Option<String>,
        freq: Option<Variant>,
        diam: Option<f64>,
        epsilon: Option<f64>,
        correlator: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let telescope = match telescope {
            Some(telescope) => telescope,
            None => self
                .telescopename
                .as_ref()
                .ok_or_else(|| {
                    PyAttributeError::new_err("'simutil' object has no attribute 'telescopename'")
                })?
                .extract::<String>(py)?,
        };
        let freq = match freq {
            Some(freq) => freq,
            None => self.centerfreq.extract::<Variant>(py)?,
        };
        let frequency = ops::quantity(&freq)?;
        let receivers = py
            .detach(|| {
                noise_temperature(&telescope, &frequency, diam, epsilon, correlator.as_deref())
            })
            .map_err(ops::raised)?;
        CasaValue::Tuple(
            [
                receivers.phase,
                receivers.spillover,
                receivers.blockage,
                receivers.taper,
                receivers.correlator,
                receivers.receiver_temperature_k,
            ]
            .into_iter()
            .map(CasaValue::Float)
            .collect(),
        )
        .into_python(py)
    }

    /// The band name of a frequency in GHz.
    #[pyo3(signature = (freq), text_signature = "($self, freq)")]
    fn bandname(&self, freq: f64) -> String {
        band_name(freq)
    }

    /// `(stokes, feed mode)` for `sm.setspwindow` and `sm.setfeed`, or with
    /// `listall` every polarization type's.
    #[pyo3(signature = (telescope, poltype = None, listall = false), text_signature = "($self, telescope, poltype=None, listall=False)")]
    fn polsettings(
        &self,
        py: Python<'_>,
        telescope: String,
        poltype: Option<String>,
        listall: bool,
    ) -> PyResult<Py<PyAny>> {
        let pair = |(stokes, feed): (&str, &str)| {
            CasaValue::Tuple(vec![
                CasaValue::Text(stokes.into()),
                CasaValue::Text(feed.into()),
            ])
        };
        if listall {
            return CasaValue::record(
                ["circular", "linear", "RR"]
                    .map(|kind| (kind, pair(polarization_settings(&telescope, Some(kind))))),
            )
            .into_python(py);
        }
        pair(polarization_settings(&telescope, poltype.as_deref())).into_python(py)
    }

    /// Positions relative to `(cx, cy, cz)` in the local tangent plane:
    /// `(lat, lon, el)` arrays, which are east, north and up, metres.
    #[pyo3(signature = (x, y, z, cx, cy, cz), text_signature = "($self, x, y, z, cx, cy, cz)")]
    #[allow(clippy::too_many_arguments)]
    fn itrf2loc(
        &self,
        py: Python<'_>,
        x: DoubleList,
        y: DoubleList,
        z: DoubleList,
        cx: f64,
        cy: f64,
        cz: f64,
    ) -> PyResult<Py<PyAny>> {
        if y.0.len() != x.0.len() || z.0.len() != x.0.len() {
            return Err(PyRuntimeError::new_err("x, y and z have different lengths"));
        }
        let positions: Vec<[f64; 3]> = (0..x.0.len()).map(|i| [x.0[i], y.0[i], z.0[i]]).collect();
        let local = py
            .detach(|| itrf_to_local(&positions, [cx, cy, cz]))
            .map_err(ops::raised)?;
        let axis = |k: usize| CasaValue::floats(local.iter().map(|p| p[k]).collect());
        CasaValue::Tuple(vec![axis(0), axis(1), axis(2)]).into_python(py)
    }

    /// Every antenna pair's distance in a configuration file, metres. The
    /// file is read as `readantenna` reads it, keeping its telescope.
    #[pyo3(name = "baselineLengths", signature = (configfile), text_signature = "($self, configfile)")]
    fn baseline_lengths(&mut self, py: Python<'_>, configfile: PathBuf) -> PyResult<Py<PyAny>> {
        let (telescope, lengths) = py.detach(|| -> PyResult<(String, Vec<f64>)> {
            let config = configuration(&configfile)?;
            let lengths = baseline_lengths(&config).map_err(ops::raised)?;
            Ok((config.telescope, lengths))
        })?;
        self.keep_telescope(py, telescope);
        CasaValue::floats(lengths).into_python(py)
    }

    /// λ over the 90th-percentile baseline of a configuration, arcsec, at
    /// `freq` GHz. The file is read as `readantenna` reads it, keeping its
    /// telescope.
    #[pyo3(name = "approxBeam", signature = (configfile, freq), text_signature = "($self, configfile, freq)")]
    fn approx_beam(&mut self, py: Python<'_>, configfile: PathBuf, freq: f64) -> PyResult<f64> {
        let (telescope, beam) = py.detach(|| -> PyResult<(String, f64)> {
            let config = configuration(&configfile)?;
            let beam = approximate_beam(&config, freq).map_err(ops::raised)?;
            Ok((config.telescope, beam))
        })?;
        self.keep_telescope(py, telescope);
        Ok(beam)
    }

    /// The model image `inimage` (a CASA image or a FITS file) fitted into
    /// `simobserve`'s four axes as the new image `outimage`, with its
    /// brightness, direction, cell, centre frequency and channel width
    /// overridden where given; with `flatimage`, its flat image too.
    /// Returns CASA's tuple `(model_refdir, model_cell, model_size,
    /// model_nchan, model_specrefval, model_specrefpix, model_width,
    /// model_stokes)`, or `False` for more than one `indirection`.
    /// `innchan` is unused, as in CASA.
    #[pyo3(
        signature = (inimage, outimage, inbright, indirection, incell, incenter, inwidth, innchan, flatimage = false),
        text_signature = "($self, inimage, outimage, inbright, indirection, incell, incenter, inwidth, innchan, flatimage=False)"
    )]
    #[allow(clippy::too_many_arguments, unused_variables)]
    fn modifymodel(
        &self,
        py: Python<'_>,
        inimage: PathBuf,
        outimage: PathBuf,
        inbright: String,
        indirection: StringList,
        incell: StringList,
        incenter: String,
        inwidth: String,
        innchan: &Bound<'_, PyAny>,
        flatimage: bool,
    ) -> PyResult<Py<PyAny>> {
        let overrides = ModelOverrides {
            inbright,
            indirection: indirection.0,
            incell: incell.0,
            incenter,
            inwidth,
        };
        let summary = py.detach(|| {
            let summary = modify_model(&inimage, &outimage, &overrides)?;
            if flatimage {
                flat_image(&outimage, None)?;
            }
            Ok(summary)
        });
        match summary {
            Ok(summary) => ops::model_summary(summary).into_python(py),
            Err(SkyModelError::Indirections(message)) => {
                emit(
                    py,
                    LOGGER,
                    &[LogLine::info(
                        SkyModelError::Indirections(message).to_string(),
                    )],
                )?;
                Ok(false.into_pyobject(py)?.to_owned().into_any().unbind())
            }
            Err(error) => Err(ops::sky_model_raised(error)),
        }
    }

    /// `image` collapsed to one Stokes I plane, as `<image>.flat`, with the
    /// components of `complist` added when it is given.
    #[pyo3(signature = (image, complist = "", verbose = false), text_signature = "($self, image, complist='', verbose=False)")]
    #[allow(unused_variables)]
    fn flatimage(
        &self,
        py: Python<'_>,
        image: PathBuf,
        complist: &str,
        verbose: bool,
    ) -> PyResult<()> {
        let complist = (!complist.is_empty()).then(|| PathBuf::from(complist));
        py.detach(|| flat_image(&image, complist.as_deref()))
            .map(|_| ())
            .map_err(ops::sky_model_raised)
    }

    /// Whether `s` (a value or a list) holds quantities casacore reads; with
    /// `halt`, a value that does not raises.
    #[pyo3(signature = (s, halt = true), text_signature = "($self, s, halt=True)")]
    fn isquantity(&self, s: &Bound<'_, PyAny>, halt: bool) -> PyResult<bool> {
        let items: Vec<Bound<'_, PyAny>> = if s.is_instance_of::<pyo3::types::PyList>() {
            s.try_iter()?.collect::<PyResult<_>>()?
        } else {
            vec![s.clone()]
        };
        for item in items {
            if !ops::is_quantity(&item) {
                if halt {
                    return Err(PyRuntimeError::new_err(format!(
                        "can't interpret '{}' as a CASA quantity",
                        item.str()?
                    )));
                }
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether `s` (a string, or a list split at its mean) is a direction;
    /// with `halt`, one that is not raises.
    #[pyo3(signature = (s, halt = true), text_signature = "($self, s, halt=True)")]
    fn isdirection(&self, py: Python<'_>, s: &Bound<'_, PyAny>, halt: bool) -> PyResult<bool> {
        let valid =
            ops::directions(s).is_ok_and(|directions| py.detach(|| directions.split().is_ok()));
        if !valid && halt {
            return Err(PyRuntimeError::new_err(format!(
                "can't interpret '{}' as a direction",
                s.str()?
            )));
        }
        Ok(valid)
    }
}

impl PySimutil {
    /// Keep the telescope of a configuration just read, as CASA's
    /// `readantenna` sets `self.telescopename`.
    fn keep_telescope(&mut self, py: Python<'_>, telescope: String) {
        self.telescopename = Some(PyString::new(py, &telescope).into_any().unbind());
    }
}

/// CASA's `readantenna` tuple, made from the configuration's own vectors.
fn readantenna_value(config: AntennaConfigFile) -> CasaValue {
    let floats =
        |values: Vec<f64>| CasaValue::List(values.into_iter().map(CasaValue::Float).collect());
    let texts =
        |values: Vec<String>| CasaValue::List(values.into_iter().map(CasaValue::Text).collect());
    let observatory = Measure::from_casa(&CasaMeasure::Position(config.observatory)).to_value();
    CasaValue::Tuple(vec![
        floats(config.x),
        floats(config.y),
        floats(config.z),
        CasaValue::floats(config.dish_diameter),
        texts(config.pad_names),
        texts(config.antenna_names),
        CasaValue::Text(config.telescope),
        observatory,
    ])
}
