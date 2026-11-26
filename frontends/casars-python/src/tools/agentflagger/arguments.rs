// SPDX-License-Identifier: LGPL-3.0-or-later
//! Translate tool records and convenience-method arguments into shared agents.

use std::collections::BTreeMap;

use casa_contracts::ParameterValue;
use casa_flagging::FlagDataRequest;
use casa_flagging::{
    FlagDataAction, FlagDataColumn, FlagDataMode, QuackMode, TfcropFit, TfcropOptions,
    TfcropPasses, TfcropWindowStats,
};
use casa_ms::MsSelection;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::Parameters;

pub(super) struct Selectors<'a> {
    pub(super) field: &'a str,
    pub(super) spw: &'a str,
    pub(super) array: &'a str,
    pub(super) feed: &'a str,
    pub(super) scan: &'a str,
    pub(super) antenna: &'a str,
    pub(super) uvrange: &'a str,
    pub(super) timerange: &'a str,
    pub(super) correlation: &'a str,
    pub(super) intent: &'a str,
    pub(super) observation: &'a str,
}

impl Selectors<'_> {
    fn entries(&self) -> [(&'static str, &str); 11] {
        [
            ("field", self.field),
            ("spw", self.spw),
            ("array", self.array),
            ("feed", self.feed),
            ("scan", self.scan),
            ("antenna", self.antenna),
            ("uvrange", self.uvrange),
            ("timerange", self.timerange),
            ("correlation", self.correlation),
            ("intent", self.intent),
            ("observation", self.observation),
        ]
    }

    pub(super) fn parameters(&self, mode: &str) -> Parameters {
        let mut parameters: Parameters = self
            .entries()
            .into_iter()
            .filter(|(_, value)| !value.is_empty())
            .map(|(name, value)| (name.to_owned(), ParameterValue::String(value.to_owned())))
            .collect();
        parameters.insert("mode".to_owned(), ParameterValue::String(mode.to_owned()));
        parameters
    }

    pub(super) fn selection(&self) -> MsSelection {
        let nonempty = |text: &str| (!text.is_empty()).then(|| text.to_owned());
        MsSelection {
            field: nonempty(self.field),
            spw: nonempty(self.spw),
            array: nonempty(self.array),
            feed: nonempty(self.feed),
            scan: nonempty(self.scan),
            antenna: nonempty(self.antenna),
            uvrange: nonempty(self.uvrange),
            timerange: nonempty(self.timerange),
            correlation: nonempty(self.correlation),
            intent: nonempty(self.intent),
            observation: nonempty(self.observation),
            ..MsSelection::default()
        }
    }
}

pub(super) fn selection(record: &Parameters) -> PyResult<MsSelection> {
    let text = |name: &str| match record.get(name) {
        None => Ok(""),
        Some(ParameterValue::String(text)) => Ok(text.as_str()),
        Some(ParameterValue::Integer(_)) => Err(PyRuntimeError::new_err(format!(
            "RecordRep::get_pointer - incorrect data type String   used for field {name} with type Int     "
        ))),
        Some(_) => Err(PyRuntimeError::new_err(
            "RecordInterface::asString - invalid data type",
        )),
    };
    Ok(Selectors {
        field: text("field")?,
        spw: text("spw")?,
        array: text("array")?,
        feed: text("feed")?,
        scan: text("scan")?,
        antenna: text("antenna")?,
        uvrange: text("uvrange")?,
        timerange: text("timerange")?,
        correlation: text("correlation")?,
        intent: text("intent")?,
        observation: text("observation")?,
    }
    .selection())
}

pub(super) struct Agent {
    pub(super) request: FlagDataRequest,
    pub(super) name: String,
    pub(super) configurable: bool,
    valid_record: bool,
    pub(super) mode: String,
    pub(super) channelavg: bool,
    pub(super) timeavg: bool,
}

impl Agent {
    pub(super) fn check_column(&mut self, names: &[String]) {
        if matches!(
            self.request.mode,
            FlagDataMode::Clip | FlagDataMode::Tfcrop | FlagDataMode::Rflag | FlagDataMode::Antint
        ) {
            self.configurable &= matches!(
                self.request.data_column,
                FlagDataColumn::Cparam | FlagDataColumn::Fparam | FlagDataColumn::Snr
            ) && names
                .iter()
                .any(|name| name == self.request.data_column.name());
        }
    }

    pub(super) fn can_init(&self) -> bool {
        self.configurable
            && self.valid_record
            && (self.request.correlation.is_none()
                || matches!(
                    self.request.mode,
                    FlagDataMode::Clip | FlagDataMode::Tfcrop | FlagDataMode::Rflag
                ))
            && (self.request.mode != FlagDataMode::Quack
                || self.request.quackmode != QuackMode::Disabled)
            && (self.request.mode != FlagDataMode::Tfcrop
                || self.request.tfcrop.is_some()
                || self.request.extendflags)
            && (self.request.mode != FlagDataMode::Rflag
                || self.request.rflag_window.is_some()
                || self.request.extendflags)
    }
}

pub(super) fn agent(values: &Parameters, tool_ntime: Option<f64>) -> Option<Agent> {
    let record = Options(values);
    let mode = match record.text("mode", "") {
        "manual" | "manualflag" | "unflag" => FlagDataMode::Manual,
        "clip" => FlagDataMode::Clip,
        "quack" => FlagDataMode::Quack,
        "elevation" => FlagDataMode::Elevation,
        "tfcrop" => FlagDataMode::Tfcrop,
        "rflag" => FlagDataMode::Rflag,
        "antint" => FlagDataMode::Antint,
        "extend" => FlagDataMode::Extend,
        "summary" => FlagDataMode::Summary,
        "shadow" => FlagDataMode::Shadow,
        _ => return None,
    };
    let parsed_selection = selection(values);
    let valid_record = parsed_selection.is_ok() && record.valid_switches();
    let mut selection = parsed_selection.unwrap_or_default();
    if mode == FlagDataMode::Tfcrop
        && matches!(values.get("correlation"), Some(ParameterValue::String(value)) if value.is_empty())
    {
        selection.correlation = Some("SOL1".to_owned());
    }
    selection.observation = None;
    selection.array = None;
    selection.uvrange = None;
    selection.intent = None;
    let data_column = match record
        .text("datacolumn", "DATA")
        .to_ascii_uppercase()
        .as_str()
    {
        "CPARAM" => FlagDataColumn::Cparam,
        "FPARAM" => FlagDataColumn::Fparam,
        "SNR" => FlagDataColumn::Snr,
        _ => FlagDataColumn::Data,
    };
    let flag = record.text("mode", "") != "unflag" && record.flag("apply", true);
    let time_thresholds = record.thresholds("timedev");
    let freq_thresholds = record.thresholds("freqdev");
    let thresholds_valid = time_thresholds.is_some() && freq_thresholds.is_some();
    let mut request = FlagDataRequest {
        spw: selection.spw.clone(),
        correlation: selection.correlation.clone(),
        selection,
        casa_row_selection: true,
        flagbackup: false,
        mode,
        data_column,
        action: if flag {
            FlagDataAction::Flag
        } else {
            FlagDataAction::Unflag
        },
        autocorr: record.flag("autocorr", false),
        clipoutside: Some(record.flag("clipoutside", true)),
        clipzeros: record.flag("clipzeros", false),
        time_chunk: record
            .numeric("ntime")
            .or(tool_ntime)
            .filter(|time| *time > 0.0 && time.is_finite()),
        combinescans: record.flag("combinescans", false),
        quackinterval: record.number("quackinterval", 1.0),
        quackincrement: record.flag("quackincrement", false),
        quackmode: match record.text("quackmode", "beg") {
            "beg" => QuackMode::Beg,
            "endb" => QuackMode::End,
            "end" => QuackMode::BeforeEnd,
            "tail" => QuackMode::Tail,
            _ => QuackMode::Disabled,
        },
        elevation_limits_deg: [
            record.number("lowerlimit", 0.0),
            record.number("upperlimit", 90.0),
        ],
        timecutoff: record.number("timecutoff", 4.0),
        freqcutoff: record.number("freqcutoff", 3.0),
        timedevscale: record.number("timedevscale", 5.0),
        freqdevscale: record.number("freqdevscale", 5.0),
        spectralmax: record.number("spectralmax", 1e6),
        spectralmin: record.number("spectralmin", 0.0),
        rflag_window: record
            .width("winsize", 3)
            .filter(|&value| value > 0 && thresholds_valid),
        rflag_calculate: (mode == FlagDataMode::Rflag).then(|| !record.flag("writeflags", true)),
        timedev: record.numeric("timedev"),
        freqdev: record.numeric("freqdev"),
        timedev_by_field_spw: time_thresholds.unwrap_or_default(),
        freqdev_by_field_spw: freq_thresholds.unwrap_or_default(),
        extendflags: record.flag("extendflags", true),
        extendpols: record.flag("extendpols", true),
        growtime: record.number("growtime", 50.0),
        growfreq: record.number("growfreq", 50.0),
        growaround: record.flag("growaround", false),
        flagneartime: record.flag("flagneartime", false),
        flagnearfreq: record.flag("flagnearfreq", false),
        ..FlagDataRequest::default()
    };
    request.antint.reference_antenna = record.text("antint_ref_antenna", "").to_owned();
    request.antint.min_channel_fraction = record.number("minchanfrac", 0.6);
    request.antint.verbose = record.flag("verbose", false);
    request.summary_options.spwchan = record.flag("spwchan", false);
    request.summary_options.spwcorr = record.flag("spwcorr", false);
    request.summary_options.basecnt = record.flag("basecnt", false);
    request.summary_options.fieldcnt = record.flag("fieldcnt", false);
    request.clipminmax = record.array("clipminmax").and_then(|range| {
        let [min, max] = range else { return None };
        Some([numeric(min)?, numeric(max)?])
    });
    request.clip_channel_bins = record
        .flag("channelavg", false)
        .then(|| {
            record
                .width("chanbin", 1)
                .and_then(|value| u32::try_from(value).ok())
        })
        .flatten()
        .map(|value| vec![value]);
    request.clip_time_bin = record
        .flag("timeavg", false)
        .then(|| record.text("timebin", "").to_owned());
    request.tfcrop = record.tfcrop_options().filter(|options| {
        (1..=9).contains(&options.max_pieces) && (1..=3).contains(&options.half_window)
    });
    let name = match mode {
        FlagDataMode::Summary => record.text("name", "Summary"),
        FlagDataMode::Rflag => record.text("agentname", "Rflag"),
        _ => "",
    }
    .to_owned();
    let configurable = !matches!(mode, FlagDataMode::Elevation | FlagDataMode::Shadow);
    Some(Agent {
        request,
        name,
        configurable,
        valid_record,
        mode: record.text("mode", "").to_owned(),
        channelavg: record.flag("channelavg", false),
        timeavg: record.flag("timeavg", false),
    })
}

struct Options<'a>(&'a Parameters);

impl Options<'_> {
    fn text<'a>(&'a self, name: &str, default: &'a str) -> &'a str {
        match self.0.get(name) {
            Some(ParameterValue::String(value)) => value,
            _ => default,
        }
    }

    fn flag(&self, name: &str, default: bool) -> bool {
        match self.0.get(name) {
            Some(ParameterValue::Bool(value)) => *value,
            Some(ParameterValue::Integer(value)) => *value != 0,
            _ => default,
        }
    }

    fn valid_switches(&self) -> bool {
        [
            "apply",
            "autocorr",
            "clipoutside",
            "clipzeros",
            "combinescans",
            "quackincrement",
            "extendflags",
            "extendpols",
            "growaround",
            "flagneartime",
            "flagnearfreq",
            "verbose",
            "spwchan",
            "spwcorr",
            "basecnt",
            "fieldcnt",
            "channelavg",
            "timeavg",
            "writeflags",
        ]
        .iter()
        .all(|name| match self.0.get(*name) {
            None | Some(ParameterValue::Bool(_) | ParameterValue::Integer(_)) => true,
            Some(ParameterValue::NumpyScalar { value, .. }) => {
                matches!(value.as_ref(), ParameterValue::Bool(_))
            }
            _ => false,
        })
    }

    fn number(&self, name: &str, default: f64) -> f64 {
        self.numeric(name).unwrap_or(default)
    }

    fn numeric(&self, name: &str) -> Option<f64> {
        self.0.get(name).and_then(numeric)
    }

    fn width(&self, name: &str, default: usize) -> Option<usize> {
        match self.0.get(name) {
            Some(ParameterValue::Integer(value)) => usize::try_from(*value).ok(),
            Some(_) => None,
            None => Some(default),
        }
    }

    fn array(&self, name: &str) -> Option<&[ParameterValue]> {
        match self.0.get(name) {
            Some(ParameterValue::Array(values)) => Some(values),
            _ => None,
        }
    }

    fn thresholds(&self, name: &str) -> Option<BTreeMap<(i32, i32), f64>> {
        match self.0.get(name) {
            None | Some(ParameterValue::EmptyArray { .. }) => Some(BTreeMap::new()),
            Some(value) if numeric(value).is_some() => Some(BTreeMap::new()),
            Some(ParameterValue::Array(rows)) => rows
                .iter()
                .map(|row| {
                    let ParameterValue::Array(row) = row else {
                        return None;
                    };
                    let [field, window, count] = row.as_slice() else {
                        return None;
                    };
                    Some((
                        (numeric(field)? as i32, numeric(window)? as i32),
                        numeric(count)?,
                    ))
                })
                .collect(),
            _ => None,
        }
    }

    fn tfcrop_options(&self) -> Option<TfcropOptions> {
        let fit = |value: &str| match value {
            "line" => Some(TfcropFit::Line),
            "poly" => Some(TfcropFit::Poly),
            _ => None,
        };
        let passes = match self.text("flagdimension", "freqtime") {
            "freq" => TfcropPasses::Frequency,
            "time" => TfcropPasses::Time,
            "freqtime" => TfcropPasses::FrequencyThenTime,
            "timefreq" => TfcropPasses::TimeThenFrequency,
            _ => return None,
        };
        let window_stats = match self.text("usewindowstats", "none") {
            "none" => TfcropWindowStats::None,
            "sum" => TfcropWindowStats::Sum,
            "std" => TfcropWindowStats::Std,
            "both" => TfcropWindowStats::Both,
            _ => return None,
        };
        Some(TfcropOptions {
            time_fit: fit(self.text("timefit", "line"))?,
            frequency_fit: fit(self.text("freqfit", "poly"))?,
            passes,
            window_stats,
            max_pieces: self.width("maxnpieces", 7)?,
            half_window: self.width("halfwin", 1)?,
        })
    }
}

fn numeric(value: &ParameterValue) -> Option<f64> {
    match value {
        ParameterValue::Integer(value) => Some(*value as f64),
        ParameterValue::Float(value) => Some(*value),
        ParameterValue::NumpyScalar { value, .. } => numeric(value),
        _ => None,
    }
}
