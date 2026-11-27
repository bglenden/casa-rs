// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's calibration-table flagging session.

mod arguments;
mod parsers;
pub(crate) mod surface;
mod versions;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use casa_contracts::{CasaFloat, CasaValue, FloatDtype, ParameterValue};
use casa_flagging::{FlagDataReport, FlagDataRequest};
use casa_flagging::{
    flagdata_calibration_agent_rows, flagdata_calibration_agents,
    validate_calibration_flag_averaging,
};
use casa_ms::{MeasurementSet, MsSelection};
use casa_tasks::ms::flagdata_named_summary_record;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::reply_value::reply_value;
use crate::parameters::parameter_value;
use crate::tools::{LogLine, emit};

use self::arguments::{Agent, Selectors, agent, selection};

const LOGGER: &str = "casars.tools.agentflagger";

type Parameters = BTreeMap<String, ParameterValue>;

#[derive(Default)]
struct Session {
    attached: Option<Attachment>,
    pending: Vec<Agent>,
    initialized: Vec<Agent>,
}

struct Attachment {
    path: PathBuf,
    selection: Option<MsSelection>,
    column_names: Vec<String>,
    time_chunk: Option<f64>,
}

#[pyclass(name = "agentflagger", module = "casars.tools", frozen)]
pub(crate) struct PyAgentFlagger {
    session: Mutex<Session>,
}

impl PyAgentFlagger {
    fn with_session<T: Send>(
        &self,
        py: Python<'_>,
        operation: impl FnOnce(&mut Session) -> PyResult<T> + Send,
    ) -> PyResult<T> {
        py.detach(|| {
            let mut session = self.session.lock().map_err(runtime_error)?;
            operation(&mut session)
        })
    }

    fn queue(&self, py: Python<'_>, parameters: &Parameters, specialized: bool) -> PyResult<bool> {
        self.with_session(py, |session| {
            let Some(table) = &session.attached else {
                return Ok(true);
            };
            let Some(mut agent) = agent(parameters, table.time_chunk) else {
                return Ok(false);
            };
            agent.check_column(&table.column_names);
            if !specialized && !agent.configurable {
                return Ok(false);
            }
            session.pending.push(agent);
            Ok(true)
        })
    }
}

#[pymethods]
impl PyAgentFlagger {
    #[new]
    fn new() -> Self {
        Self {
            session: Mutex::default(),
        }
    }

    #[pyo3(signature = (msname = "", ntime = 0.0), text_signature = "($self, msname='', ntime=0.0)")]
    fn open(&self, py: Python<'_>, msname: &str, ntime: f64) -> PyResult<bool> {
        if msname.is_empty() {
            return Ok(false);
        }
        let path = absolute_path(Path::new(msname))?;
        self.with_session(py, |session| {
            let mut table = open_table(&path)?;
            if table.main_table().info().table_type != "Calibration" {
                return Err(PyRuntimeError::new_err(
                    "agentflagger requires a calibration table (#1527); MeasurementSet support is tracked in #978",
                ));
            }
            let main = table.main_table();
            if main.schema().is_some_and(|schema| !schema.contains_column("OBSERVATION_ID"))
                || main.keywords().get("OBSERVATION").is_none()
            {
                // CASA's writable NewCalTable open upgrades legacy tables.
                drop(table);
                casa_calibration::update_calibration_table(&path).map_err(runtime_error)?;
                table = open_table(&path)?;
            }
            let column_names = table.main_table().schema().into_iter()
                .flat_map(casa_tables::TableSchema::columns)
                .map(|column| column.name().to_owned())
                .collect();
            *session = Session {
                attached: Some(Attachment {
                    path, selection: None, column_names,
                    time_chunk: (ntime > 0.0 && ntime.is_finite()).then_some(ntime),
                }),
                ..Session::default()
            };
            Ok(true)
        })
    }

    fn done(&self, py: Python<'_>) -> PyResult<bool> {
        self.with_session(py, |session| {
            *session = Session::default();
            Ok(true)
        })
    }

    #[pyo3(signature = (config = None, field = "", spw = "", array = "", feed = "", scan = "", antenna = "", uvrange = "", timerange = "", correlation = "", intent = "", observation = ""), text_signature = "($self, config={}, field='', spw='', array='', feed='', scan='', antenna='', uvrange='', timerange='', correlation='', intent='', observation='')")]
    #[allow(clippy::too_many_arguments)]
    fn selectdata(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = supplied)] config: Option<Bound<'_, PyAny>>,
        field: &str,
        spw: &str,
        array: &str,
        feed: &str,
        scan: &str,
        antenna: &str,
        uvrange: &str,
        timerange: &str,
        correlation: &str,
        intent: &str,
        observation: &str,
    ) -> PyResult<bool> {
        let config = config
            .map(|value| record(&value, "config"))
            .transpose()?
            .unwrap_or_default();
        let requested = if config.is_empty() {
            Selectors {
                field,
                spw,
                array,
                feed,
                scan,
                antenna,
                uvrange,
                timerange,
                correlation,
                intent,
                observation,
            }
            .selection()
        } else {
            selection(&config)?
        };
        self.with_session(py, |session| {
            if let Some(attached) = &mut session.attached {
                let table = open_table(&attached.path)?;
                flagdata_calibration_agent_rows(&table, &requested).map_err(runtime_error)?;
                attached.selection = Some(requested);
            }
            Ok(true)
        })
    }

    fn parseagentparameters(&self, py: Python<'_>, aparams: &Bound<'_, PyAny>) -> PyResult<bool> {
        self.queue(py, &record(aparams, "aparams")?, false)
    }

    fn init(&self, py: Python<'_>) -> PyResult<bool> {
        self.with_session(py, |session| {
            if session
                .attached
                .as_ref()
                .and_then(|attached| attached.selection.as_ref())
                .is_none()
            {
                return Ok(false);
            }
            let configs = session
                .initialized
                .iter()
                .chain(&session.pending)
                .filter(|agent| agent.configurable)
                .map(|agent| (agent.mode.as_str(), agent.channelavg, agent.timeavg));
            validate_calibration_flag_averaging(configs).map_err(runtime_error)?;
            let initialized = session.initialized.len();
            session
                .initialized
                .extend(session.pending.drain(..).filter(Agent::can_init));
            Ok(initialized != session.initialized.len())
        })
    }

    #[pyo3(signature = (writeflags = true, sequential = true), text_signature = "($self, writeflags=True, sequential=True)")]
    fn run(&self, py: Python<'_>, writeflags: bool, sequential: bool) -> PyResult<Py<PyAny>> {
        let value = self.with_session(py, |session| {
            let agents = std::mem::take(&mut session.initialized);
            let Some(attached) = &session.attached else {
                return Ok(empty_record());
            };
            let Some(selection) = &attached.selection else {
                return Ok(empty_record());
            };
            if agents.is_empty() {
                return Ok(empty_record());
            }
            let table = open_table(&attached.path)?;
            let rows = flagdata_calibration_agent_rows(&table, selection).map_err(runtime_error)?;
            let requests: Vec<FlagDataRequest> =
                agents.iter().map(|agent| agent.request.clone()).collect();
            let reports =
                flagdata_calibration_agents(table, &requests, &rows, writeflags, sequential)
                    .map_err(runtime_error)?;
            Ok(program_record(&reports, &agents))
        })?;
        reply_value(py, &value)
    }

    fn printflagselection(&self, py: Python<'_>) -> PyResult<bool> {
        let line = self.with_session(py, |session| {
            Ok(session
                .attached
                .as_ref()
                .and_then(|target| target.selection.as_ref())
                .map_or_else(
                    || LogLine::info("No flag selection configured."),
                    |selection| LogLine::info(format!("{selection:?}")),
                ))
        })?;
        emit(py, LOGGER, &[line])?;
        Ok(true)
    }
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn absolute_path(path: &Path) -> PyResult<PathBuf> {
    std::path::absolute(path).map_err(runtime_error)
}

fn open_table(path: &Path) -> PyResult<MeasurementSet> {
    if !path.exists() {
        return Err(PyRuntimeError::new_err(format!(
            "Table {} does not exist",
            path.display()
        )));
    }
    MeasurementSet::open(path).map_err(runtime_error)
}

fn record(value: &Bound<'_, PyAny>, name: &str) -> PyResult<Parameters> {
    let dict = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err(format!("_{name} is not a dictionary")))?;
    dict.iter()
        .map(|(key, value)| Ok((key.extract::<String>()?, parameter_value(&value)?)))
        .collect()
}

#[allow(clippy::unnecessary_wraps)] // PyO3's extractor distinguishes a default from explicit None.
fn supplied<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(Some(value.clone()))
}

fn empty_record() -> CasaValue {
    CasaValue::dict(std::iter::empty::<(&str, CasaValue)>())
}

fn program_record(reports: &[Option<FlagDataReport>], agents: &[Agent]) -> CasaValue {
    if reports.iter().all(Option::is_none) {
        return empty_record();
    }
    let array = |rows: &[[f64; 3]]| CasaValue::FloatArray {
        dtype: FloatDtype::Float64,
        shape: vec![rows.len(), 3],
        data: (0..3)
            .flat_map(|column| rows.iter().map(move |row| CasaFloat(row[column])))
            .collect(),
    };
    let reports = reports
        .iter()
        .zip(agents)
        .filter_map(|(report, agent)| {
            let report = report.as_ref()?;
            if let Some(summary) = &report.summary {
                Some(flagdata_named_summary_record(summary, &agent.name))
            } else {
                let thresholds = report.rflag_report.as_ref()?;
                if thresholds.timedev.is_empty() && thresholds.freqdev.is_empty() {
                    return None;
                }
                Some(CasaValue::dict([
                    ("name", CasaValue::string(&agent.name)),
                    ("type", CasaValue::string("rflag")),
                    ("timedev", array(&thresholds.timedev)),
                    ("freqdev", array(&thresholds.freqdev)),
                ]))
            }
        })
        .collect::<Vec<_>>();
    let count = reports.len() as i64;
    let mut record = BTreeMap::from([
        ("type".to_owned(), CasaValue::string("list")),
        ("nreport".to_owned(), CasaValue::Int { value: count }),
    ]);
    record.extend(
        reports
            .into_iter()
            .enumerate()
            .map(|(i, report)| (format!("report{i}"), report)),
    );
    CasaValue::dict(record)
}
