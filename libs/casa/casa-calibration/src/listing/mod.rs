// SPDX-License-Identifier: LGPL-3.0-or-later
//! List calibrated antenna solutions using the MeasurementSet's selectors.

mod format;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::observation_io::NamespaceRows;
use casa_ms::selection::ChannelRange;
use casa_ms::subtables::{MsAntenna, MsField, MsSpectralWindow};
use casa_ms::{MeasurementSet, SubtableId};
use casa_tables::{Table, TableOptions, add_directory};
use serde::Serialize;
use tempfile::NamedTempFile;

use crate::constants::{
    COL_ANTENNA1, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_FPARAM, COL_SPECTRAL_WINDOW_ID, COL_TIME,
    TABLE_INFO_TYPE,
};

use crate::rerefant::{names_agree, same_antenna};

use format::{Line, Listing, Window};

/// One piece of a CASA listing. Pagination pauses at a page boundary and
/// finishes the current integration when the reader chooses to quit.
#[derive(Serialize)]
pub struct ListingPart {
    /// Rendered text in this page fragment.
    pub text: String,
    /// Whether the interactive caller should pause after this fragment.
    pub prompt: bool,
    /// Whether this fragment completes the current integration.
    pub last_in_integration: bool,
    /// Number of antenna solutions represented by the fragment.
    pub solutions: usize,
}

#[derive(Serialize)]
/// Paginated CASA calibration listing for terminal display or export.
pub struct CalibrationListing {
    /// Page fragments in display order.
    pub parts: Vec<ListingPart>,
    /// Total number of antenna solutions included.
    pub solutions: usize,
    /// Diagnostic to report after any partial listing text.
    pub error: Option<String>,
}

/// Text listing prepared for the Python task or tool, including its terminal
/// page boundaries or the output file already published.
#[derive(Serialize)]
pub struct CalibrationListingReport {
    /// Formatted listing for display when no output file was published.
    pub listing: Option<CalibrationListing>,
    /// Path of the listing written by the task, when one was requested.
    pub listfile: Option<String>,
}

/// Publishes the prepared listing text via a staged file in the destination directory. An existing
/// output is preserved and reported as an error.
pub fn publish_listing(path: &Path, text: &str) -> Result<(), String> {
    let fail = |error: std::io::Error| {
        format!("could not create listing file {}: {error}", path.display(),)
    };
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut stage = NamedTempFile::new_in(directory).map_err(fail)?;
    stage.write_all(text.as_bytes()).map_err(fail)?;
    stage
        .persist_noclobber(path)
        .map_err(|error| fail(error.error))?;
    Ok(())
}

impl CalibrationListing {
    /// Concatenates the listing text and solution count without interactive prompts.
    pub fn lines(&self) -> String {
        let mut text = self
            .parts
            .iter()
            .map(|part| part.text.as_str())
            .collect::<String>();
        text.push_str(&format!(
            "\nListed {} antenna solutions.\n\n",
            self.solutions
        ));
        text
    }
}

/// Lists antenna solutions from a compatible calibration table using MeasurementSet field, antenna,
/// and spectral-window selectors. Invalid selectors or table contents return a diagnostic.
pub fn list_calibration(
    ms: &MeasurementSet,
    input: &Path,
    field: &str,
    antenna: &str,
    spw: &str,
    page_rows: i32,
) -> Result<CalibrationListing, String> {
    list_calibration_named(ms, ms.path(), input, field, antenna, spw, page_rows).map_err(display)
}

/// Failure to render a calibration listing from an observation namespace.
#[derive(Debug, thiserror::Error)]
pub enum CalibrationListingError {
    /// The existing selector, calibration table or its solutions could not be read.
    #[error("{0}")]
    Listing(String),
    /// An MSv4 entity's ordinal cannot be used as a calibration subtable row.
    #[error("incompatible MSv4 calibration {axis} identity: {reason}")]
    Identity {
        /// The entity axis being bound to the calibration table.
        axis: &'static str,
        /// Why the selected physical identity cannot be bound.
        reason: String,
    },
}

impl From<String> for CalibrationListingError {
    fn from(message: String) -> Self {
        Self::Listing(message)
    }
}

/// List calibration solutions using an explicit observation path for a
/// metadata namespace that is held in memory.
///
/// # Errors
/// The same selector and calibration table errors as [`list_calibration`].
pub fn list_calibration_named(
    ms: &MeasurementSet,
    observed_path: Option<&Path>,
    input: &Path,
    field: &str,
    antenna: &str,
    spw: &str,
    page_rows: i32,
) -> Result<CalibrationListing, CalibrationListingError> {
    let options = TableOptions::new(input);
    #[cfg(unix)]
    let table = Table::open_with_lock(
        options,
        casa_tables::LockOptions::new(casa_tables::LockMode::AutoLocking).read_only(true),
    );
    #[cfg(not(unix))]
    let table = Table::open(options);
    let table = table.map_err(display)?;
    if table.info().table_type != TABLE_INFO_TYPE {
        return Err(format!("{} is not a calibration table", input.display()).into());
    }
    let subtype = listing_subtype(&table.info().sub_type)?;
    let ms_name = observed_path
        .map(std::path::absolute)
        .transpose()
        .map_err(display)?;
    let fields = ms.resolve_field_selector(field).map_err(display)?;
    let antennas = ms.resolve_specified_antennas(antenna).map_err(display)?;
    let windows = ms
        .resolve_spw_selector(if spw.is_empty() { "*" } else { spw })
        .map_err(display)?;
    let verify_namespace = ms.path().is_none() && observed_path.is_some();
    let (names, spectral) = open_subtables(
        &table,
        input,
        verify_namespace.then_some(SelectedRows {
            observation: ms,
            fields: &fields,
            antennas: &antennas,
        }),
    )?;
    let legacy_channels = (!verify_namespace)
        .then(|| legacy_window_counts(&spectral))
        .transpose()?;
    let antennas = if antennas.is_empty() {
        (0..names.antennas.len()).collect()
    } else {
        antennas
    };
    if antennas.iter().any(|&id| id >= names.antennas.len()) {
        return Err("selected antenna is absent from the calibration table"
            .to_string()
            .into());
    }
    let mut ranges = BTreeMap::new();
    for range in windows.channel_list() {
        // CASA lists the first channel element it encounters for each SPW.
        ranges.entry(range.spw_id).or_insert(*range);
    }
    if ranges.is_empty() {
        ranges.insert(
            0,
            ChannelRange {
                spw_id: 0,
                start: 0,
                stop: 0,
                step: 1,
            },
        );
    }
    let default_windows = matches!(spw.trim(), "" | "*");
    let headers = row_headers(
        &table,
        (!(verify_namespace && default_windows)).then_some(&ranges),
    )?;
    let channels = if let Some(legacy) = legacy_channels {
        WindowChannels::Legacy(legacy)
    } else {
        // The wildcard must check even solutions whose IDs are absent in the
        // observation. Explicit selections bind the requested windows only.
        let required = headers
            .keys()
            .filter(|_| default_windows)
            .chain(ranges.keys().filter(|_| !default_windows))
            .copied();
        WindowChannels::Validated(validate_window_ids(ms, &spectral, required, &ranges)?)
    };
    drop(spectral);
    let complex = table
        .schema()
        .is_some_and(|schema| schema.column(COL_CPARAM).is_some());
    let source = ListingSource {
        table: &table,
        input,
        ms_name: ms_name.as_deref(),
        subtype,
        complex,
        fields: &fields,
        antennas: &antennas,
        names: &names,
        channels: &channels,
    };
    let mut listing = Listing::new(page_rows);
    let result = headers
        .into_iter()
        .try_for_each(|(spw, rows)| source.window(&mut listing, spw, &rows, &ranges[&spw]));
    Ok(listing.finish(result.err()))
}

enum WindowChannels {
    Legacy(Vec<usize>),
    Validated(BTreeMap<usize, usize>),
}

impl WindowChannels {
    fn get(&self, id: usize) -> Option<usize> {
        match self {
            Self::Legacy(counts) => counts.get(id).copied(),
            Self::Validated(counts) => counts.get(&id).copied(),
        }
    }
}

fn validate_window_ids(
    ms: &MeasurementSet,
    calibration: &Table,
    required: impl Iterator<Item = usize>,
    ranges: &BTreeMap<usize, ChannelRange>,
) -> Result<BTreeMap<usize, usize>, CalibrationListingError> {
    const AXIS: &str = "spectral-window";

    let observation = ms
        .subtable(SubtableId::SpectralWindow)
        .ok_or_else(|| invalid_identity(AXIS, "the observation has no spectral-window table"))?;
    let mut required = required.peekable();
    if required.peek().is_none() {
        return Ok(BTreeMap::new());
    }
    let reader = |table| {
        NamespaceRows::spectral_window_reader(table).map_err(|error| invalid_identity(AXIS, error))
    };
    let observed_window = reader(observation)?;
    let calibration_window = reader(calibration)?;
    let spectral = MsSpectralWindow::new(calibration);
    required
        .map(|id| {
            if id >= observation.row_count() || id >= calibration.row_count() {
                return Err(invalid_identity(
                    AXIS,
                    format!("window {id} required for the listing is absent in one input"),
                ));
            }
            let row_error = |error| invalid_identity(AXIS, format!("window {id}: {error}"));
            let window = observed_window(id).map_err(row_error)?;
            let solved = calibration_window(id).map_err(row_error)?;
            let count = spectral
                .num_chan(id)
                .map_err(|error| invalid_identity(AXIS, format!("window {id}: {error}")))?;
            let channels = usize::try_from(count).map_err(|_| {
                invalid_identity(AXIS, format!("window {id} has invalid NUM_CHAN {count}"))
            })?;
            if !ranges.contains_key(&id)
                || channels != solved.frequencies_hz.len()
                || !window.matches_persisted_name(&solved, id)
                || !window.describes_solution_axis(&solved)
            {
                return Err(invalid_identity(
                    AXIS,
                    format!(
                        "window {id} ({}) does not describe calibration window {}",
                        window.name, solved.name
                    ),
                ));
            }
            Ok((id, channels))
        })
        .collect()
}

fn invalid_identity(axis: &'static str, reason: impl std::fmt::Display) -> CalibrationListingError {
    CalibrationListingError::Identity {
        axis,
        reason: reason.to_string(),
    }
}

struct SelectedRows<'a> {
    observation: &'a MeasurementSet,
    fields: &'a [i32],
    antennas: &'a [usize],
}

impl SelectedRows<'_> {
    fn validate(&self, antenna: &Table, field: &Table) -> Result<(), CalibrationListingError> {
        if !self.antennas.is_empty() {
            const AXIS: &str = "antenna";
            let observed = self.subtable(SubtableId::Antenna, AXIS)?;
            for &id in self.antennas {
                let physical = |table| {
                    NamespaceRows::antenna_at(table, id)
                        .map_err(|error| invalid_identity(AXIS, error))
                };
                let expected = physical(observed)?;
                let actual = physical(antenna)?;
                if !same_antenna(&expected, &actual, 0.0) || !names_agree(&expected, &actual) {
                    return Err(invalid_identity(
                        AXIS,
                        format!(
                            "selected antenna {id} ({}) does not describe calibration antenna {}",
                            expected.name, actual.name,
                        ),
                    ));
                }
            }
        }
        if !self.fields.is_empty() {
            const AXIS: &str = "field";
            let observed = self.subtable(SubtableId::Field, AXIS)?;
            for &id in self.fields {
                if !NamespaceRows::same_fixed_field(observed, field, id as usize)
                    .map_err(|error| invalid_identity(AXIS, error))?
                {
                    return Err(invalid_identity(
                        AXIS,
                        format!(
                            "selected field {id} does not describe the calibration field in that row",
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    fn subtable(
        &self,
        id: SubtableId,
        axis: &'static str,
    ) -> Result<&Table, CalibrationListingError> {
        self.observation
            .subtable(id)
            .ok_or_else(|| invalid_identity(axis, format!("the observation has no {id:?} table")))
    }
}

struct ListingSource<'a> {
    table: &'a Table,
    input: &'a Path,
    ms_name: Option<&'a Path>,
    subtype: &'a str,
    complex: bool,
    fields: &'a [i32],
    antennas: &'a [usize],
    names: &'a TableNames,
    channels: &'a WindowChannels,
}

impl ListingSource<'_> {
    fn window(
        &self,
        listing: &mut Listing,
        spw: usize,
        rows: &[Header],
        range: &ChannelRange,
    ) -> Result<(), String> {
        let names = self.names;
        let channels = self
            .channels
            .get(spw)
            .ok_or("missing calibration spectral window")?;
        let selected = channels_in_table(range, channels)?;
        let params = self
            .table
            .column_accessor(if self.complex { COL_CPARAM } else { COL_FPARAM })
            .map_err(display)?;
        let first = params.array_cell(rows[0].row).map_err(display)?;
        let receptors = first
            .shape()
            .first()
            .copied()
            .filter(|&size| size != 0)
            .ok_or("calibration solution has no parameter axis")?;
        let field_width = rows
            .iter()
            .filter_map(|row| field_name(&names.fields, row.field))
            .map(str::len)
            .max()
            .unwrap_or(0)
            .max(5);
        validate_integrations(rows, names.antennas.len())?;
        let window = Window::new(self.subtype, self.complex, receptors, channels, field_width);
        let data = solution_rows(rows, self.fields, self.antennas, &params, self.table)?;
        listing.start_window(&window, spw, rows[0].time, self.input, self.ms_name);
        for group in self.antennas.chunks(window.antennas_per_line()) {
            let mut first_heading = true;
            for integration in rows.chunks_exact(names.antennas.len()) {
                let at = &integration[0];
                if !self.fields.is_empty() && !self.fields.contains(&at.field) {
                    continue;
                }
                let values = group
                    .iter()
                    .map(|antenna| {
                        integration
                            .iter()
                            .find(|entry| entry.antenna == *antenna)
                            .and_then(|entry| data.get(&entry.row))
                    })
                    .collect::<Solutions<'_>>();
                let field = field_name(&names.fields, at.field).unwrap_or_default();
                for &channel in &selected {
                    let line = Line {
                        time: at.time,
                        field,
                        channel,
                        solutions: &values,
                    };
                    listing.row(&window, &names.antennas, group, &mut first_heading, line)?;
                }
                listing.end_integration();
            }
        }
        Ok(())
    }
}

fn listing_subtype(subtype: &str) -> Result<&str, String> {
    let upper = subtype.to_ascii_uppercase();
    let base = upper.strip_suffix(" JONES").unwrap_or(&upper);
    let name = match base {
        "B" => "B Jones",
        "G" => "G Jones",
        "T" | "T NOISE" => "T Jones",
        "TF" | "TF NOISE" => "Tf Jones",
        "J" => "J Jones",
        "JF" => "Jf Jones",
        "XJ" => "X Jones",
        "XF" => "Xf Jones",
        "K" => "K Jones",
        "KCROSS" => "Kcross Jones",
        "KMBD" => "KMBD Jones",
        "POSANG" => "PosAng Jones",
        "GLINXPH" | "XY+QU" => "GlinXph Jones",
        "GLINXPHF" | "XYF+QU" => "GlinXphf Jones",
        "XPARANG" | "XPARANG+QU" => "Xparang Jones",
        "XFPARANG" | "XFPARANG+QU" => "Xfparang Jones",
        "TFOPAC" => "TfOpac",
        "TOPAC" => "TOpac",
        "BPOLY" | "GSPLINE" => {
            return Err("GSPLINE and BPOLY tables cannot currently be listed".into());
        }
        "M" | "A" | "N" | "MF" | "X" | "M MUELLER" | "A MUELLER" | "A NOISE" | "MF MUELLER"
        | "X MUELLER" => {
            return Err(format!(
                "baseline calibration {subtype} does not provide an antenna listing"
            ));
        }
        "EP" | "LJ" => {
            return Err(format!(
                "{subtype} is unavailable to the calibration listing tool"
            ));
        }
        _ => {
            let prefix = base.split('+').next().unwrap_or(base);
            match prefix {
                "D" | "DLIN" => "D Jones",
                "DF" | "DFLIN" => "Df Jones",
                "DLLS" => "DLLS Jones",
                "DFLLS" => "DfLLS Jones",
                "DGEN" | "DGENERAL" => "Dgen Jones",
                "DFGEN" | "DFGENERAL" => "Dfgen Jones",
                _ if upper.contains("EVLAGAIN") => {
                    return Err("regenerate the EVLA switched-power table with gencal".into());
                }
                _ if upper.contains("SDSKY_OTF") => {
                    return Err(format!(
                        "{subtype} is unavailable to the calibration listing tool"
                    ));
                }
                _ if [
                    "KANTPOS",
                    "TSYS",
                    "EVLASWP",
                    "GAINCURVE",
                    "POWERCURVE",
                    "TEC",
                    "F JONES",
                    "FRINGE",
                    "ACCOR",
                    "ZENITHDELAY",
                    "SDSKY_PS",
                    "SDSKY_RASTER",
                    "SDGAIN_OTFD",
                ]
                .iter()
                .any(|token| upper.contains(token)) =>
                {
                    subtype
                }
                _ => return Err(format!("unknown calibration type {subtype:?}")),
            }
        }
    };
    Ok(name)
}

fn validate_integrations(rows: &[Header], antennas: usize) -> Result<(), String> {
    if antennas == 0 || !rows.len().is_multiple_of(antennas) {
        return Err("calibration table has an incomplete antenna integration".into());
    }
    let mut seen = vec![false; antennas];
    for block in rows.chunks_exact(antennas) {
        seen.fill(false);
        let first = block[0];
        for row in block {
            if row.time != first.time
                || row.field != first.field
                || row.antenna >= antennas
                || std::mem::replace(&mut seen[row.antenna], true)
            {
                return Err(format!(
                    "inconsistent antenna integration beginning at row {}",
                    first.row
                ));
            }
        }
    }
    Ok(())
}

struct TableNames {
    antennas: Vec<String>,
    fields: Vec<String>,
}

fn open_subtables(
    table: &Table,
    path: &Path,
    selected: Option<SelectedRows<'_>>,
) -> Result<(TableNames, Table), CalibrationListingError> {
    let subtable = |name: &str| {
        let source = table
            .keywords()
            .get(name)
            .and_then(Value::as_table_ref)
            .map_or_else(|| path.join(name), |stored| add_directory(stored, path));
        Table::open(TableOptions::new(source)).map_err(display)
    };
    let antenna = subtable("ANTENNA")?;
    let field = subtable("FIELD")?;
    if let Some(selected) = &selected {
        selected.validate(&antenna, &field)?;
    }
    let antenna = MsAntenna::new(&antenna);
    let field = MsField::new(&field);
    let spectral = subtable("SPECTRAL_WINDOW")?;
    let antennas = (0..antenna.row_count())
        .map(|row| antenna.name(row))
        .collect::<Result<_, _>>()
        .map_err(display)?;
    let fields = (0..field.row_count())
        .map(|row| field.name(row))
        .collect::<Result<_, _>>()
        .map_err(display)?;
    Ok((TableNames { antennas, fields }, spectral))
}

fn legacy_window_counts(table: &Table) -> Result<Vec<usize>, String> {
    let spectral = MsSpectralWindow::new(table);
    (0..spectral.row_count())
        .map(|row| usize::try_from(spectral.num_chan(row).map_err(display)?).map_err(display))
        .collect()
}

fn channels_in_table(range: &ChannelRange, channels: usize) -> Result<Vec<usize>, String> {
    let start = range.start.max(0) as usize;
    let stop = usize::try_from(range.stop)
        .map_err(display)?
        .min(channels.saturating_sub(1));
    if start > stop || channels == 0 {
        return Err("none of the selected channels are present in the calibration table".into());
    }
    if range.step < 0 {
        return Err("stepping backwards through channels is unsupported".into());
    }
    Ok((start..=stop).step_by(range.step.max(1) as usize).collect())
}

#[derive(Clone, Copy)]
struct Header {
    row: usize,
    time: f64,
    field: i32,
    antenna: usize,
}

fn row_headers(
    table: &Table,
    selected: Option<&BTreeMap<usize, ChannelRange>>,
) -> Result<BTreeMap<usize, Vec<Header>>, String> {
    let mut windows = BTreeMap::<usize, Vec<Header>>::new();
    let access = table.row_accessor();
    let integer = |row, name| match access.scalar_cell(row, name).map_err(display)? {
        ScalarValue::Int32(value) => Ok(*value),
        _ => Err(format!("invalid {name} at row {row}")),
    };
    for row in 0..table.row_count() {
        let window = usize::try_from(integer(row, COL_SPECTRAL_WINDOW_ID)?).map_err(display)?;
        if selected.is_some_and(|ranges| !ranges.contains_key(&window)) {
            continue;
        }
        let ScalarValue::Float64(time) = access.scalar_cell(row, COL_TIME).map_err(display)? else {
            return Err(format!("invalid TIME at row {row}"));
        };
        if !time.is_finite() {
            return Err(format!("non-finite time at row {row}"));
        }
        let field = integer(row, COL_FIELD_ID)?;
        let antenna = usize::try_from(integer(row, COL_ANTENNA1)?).map_err(display)?;
        windows.entry(window).or_default().push(Header {
            row,
            time: *time,
            field,
            antenna,
        });
    }
    Ok(windows)
}

type Solutions<'a> = Vec<Option<&'a (ArrayValue, ArrayValue)>>;

fn solution_rows(
    headers: &[Header],
    fields: &[i32],
    antennas: &[usize],
    params: &casa_tables::TableColumn<'_>,
    table: &Table,
) -> Result<BTreeMap<usize, (ArrayValue, ArrayValue)>, String> {
    let rows = headers
        .iter()
        .filter(|row| {
            (fields.is_empty() || fields.contains(&row.field)) && antennas.contains(&row.antenna)
        })
        .map(|row| row.row)
        .collect::<Vec<_>>();
    let flag = table.column_accessor(COL_FLAG).map_err(display)?;
    let mut solutions = BTreeMap::new();
    for chunk in rows.chunks(4096) {
        let values = params.array_cells_owned_uncached(chunk).map_err(display)?;
        let flags = flag.array_cells_owned_uncached(chunk).map_err(display)?;
        for (&row, (value, flag)) in chunk.iter().zip(values.into_iter().zip(flags)) {
            let (Some(value), Some(flag)) = (value, flag) else {
                return Err(format!("undefined calibration solution at row {row}"));
            };
            solutions.insert(row, (value, flag));
        }
    }
    Ok(solutions)
}

fn field_name(fields: &[String], id: i32) -> Option<&str> {
    usize::try_from(id)
        .ok()
        .and_then(|id| fields.get(id))
        .map(String::as_str)
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
