// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility listings for the public MeasurementSet tool.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Write};

use casa_coordinates::StokesType;
use casa_core::{ArrayValue, RecordValue, Value};

use crate::selection::{ResolvedMsSelection, resolve_casa_selection_with_system_budget};
use crate::tool_iteration::ToolIteration;
use crate::tool_visibility::{VisibilityReadOptions, read_tool_visibility};
use crate::{MeasurementSet, MsError, MsResult, MsSelection};

mod render;

use render::{ListingState, report_chunk};

/// CASA visibility-listing arguments that affect output and MS selection.
pub struct ToolListerOptions {
    /// Visibility column requested by the caller.
    pub data_column: String,
    /// Row/channel selection retained by the listing.
    pub selection: MsSelection,
    /// Correlation names requested in the report.
    pub correlation: String,
    /// Number of printed visibility rows between repeated headers.
    pub page_rows: i32,
}

/// List selected visibilities in the public MS tool report format.
///
/// The reader is used only for interactive display; pass `None` for files.
/// The output writer receives the report in bounded visibility buffers.
pub fn write_visibility_listing<'a>(
    ms: &MeasurementSet,
    options: &ToolListerOptions,
    output: &'a mut dyn Write,
    input: Option<&'a mut dyn BufRead>,
) -> MsResult<()> {
    let columns = ListingColumns::parse(&options.data_column)?;
    let selection = resolve_casa_selection_with_system_budget(ms, &options.selection)?;
    let rows: Vec<_> = selection.row_indices().collect();
    if rows.is_empty() {
        return Err(listing_error("specified selection contains zero rows"));
    }
    let mut iteration = ToolIteration::new(
        ms.main_table(),
        None,
        Some(&rows),
        &["ANTENNA1".to_string(), "ANTENNA2".to_string()],
        300.0,
        0,
        true,
    )?
    .ok_or_else(|| listing_error("cannot group MeasurementSet rows"))?;
    let spectra = ListingSpectra::new(ms, &selection, &options.correlation)?;
    let antenna = ms.antenna()?;
    let antenna_names: Vec<_> = (0..antenna.row_count())
        .map(|row| antenna.name(row))
        .collect::<MsResult<_>>()?;
    let mut state = ListingState::new(output, input, options.page_rows);
    let items = columns.items();
    iteration.rewind();
    loop {
        let record = read_tool_visibility(
            ms.main_table(),
            &[],
            None,
            iteration.rows()?,
            &items,
            Some(ms),
            VisibilityReadOptions::default(),
        )?;
        let chunk = ListingChunk::read(&record, columns)?;
        if !report_chunk(&mut state, &chunk, &spectra, &antenna_names)? || !iteration.advance()? {
            return Ok(());
        }
    }
}

#[derive(Clone, Copy)]
struct ListingColumns {
    amplitude: &'static str,
    phase: Option<&'static str>,
}

impl ListingColumns {
    fn parse(name: &str) -> MsResult<Self> {
        let (amplitude, phase) = match name {
            "" | "data" => ("amplitude", Some("phase")),
            "corrected" => ("corrected_amplitude", Some("corrected_phase")),
            "model" => ("model_amplitude", Some("model_phase")),
            "residual" => ("residual_amplitude", Some("residual_phase")),
            "float_data" => ("float_data", None),
            _ => return Err(listing_error("unrecognized value in parameter datacolumn")),
        };
        Ok(Self { amplitude, phase })
    }

    fn items(self) -> Vec<String> {
        [
            "time",
            "antenna1",
            "antenna2",
            "field_id",
            "data_desc_id",
            "uvw",
            "weight",
            "flag",
            self.amplitude,
        ]
        .into_iter()
        .chain(self.phase)
        .map(str::to_string)
        .collect()
    }
}

struct ListingSpectra {
    windows: BTreeMap<i32, ListingWindow>,
    multiple_channels: bool,
}

struct ListingWindow {
    id: i32,
    channels: Vec<usize>,
    correlations: Vec<(usize, String)>,
    uvdist_scale: f64,
}

impl ListingSpectra {
    fn new(
        ms: &MeasurementSet,
        selection: &ResolvedMsSelection,
        correlation: &str,
    ) -> MsResult<Self> {
        let data_description = ms.data_description()?;
        let spectral_window = ms.spectral_window()?;
        let polarization = ms.polarization()?;
        let mut windows = BTreeMap::new();
        let mut multiple_channels = false;
        let mut polarization_ids = BTreeSet::new();
        let descriptions: BTreeSet<_> = selection
            .selected_rows
            .iter()
            .map(|row| row.data_desc_id)
            .collect();
        for description in descriptions {
            let row = valid_index(description)?;
            let id = data_description.spectral_window_id(row)?;
            let pol_id = data_description.polarization_id(row)?;
            polarization_ids.insert(pol_id);
            let spw_row = valid_index(id)?;
            let count = valid_index(spectral_window.num_chan(spw_row)?)?;
            let channels = match selection.channel_selections.get(&id) {
                Some(selection) => {
                    multiple_channels |= selection
                        .segments
                        .iter()
                        .any(|segment| segment.start != segment.end);
                    selection.indices_with_overlaps(count)?
                }
                None => {
                    multiple_channels |= count > 1;
                    (0..count).collect()
                }
            };
            let correlations =
                listing_correlations(&polarization.corr_type(valid_index(pol_id)?)?, correlation)?;
            let uvdist_scale = spectral_window.ref_frequency(spw_row)? / 299_792_458.0;
            windows.insert(
                description,
                ListingWindow {
                    id,
                    channels,
                    correlations,
                    uvdist_scale,
                },
            );
        }
        if polarization_ids.len() > 1 {
            return Err(listing_error(
                "selection has multiple polarization configurations",
            ));
        }
        Ok(Self {
            windows,
            multiple_channels,
        })
    }
}

fn listing_correlations(codes: &[i32], request: &str) -> MsResult<Vec<(usize, String)>> {
    let available: Vec<_> = codes
        .iter()
        .enumerate()
        .map(|(index, &code)| {
            let name = StokesType::from_code(code)
                .map_or("Undefined", StokesType::name)
                .to_string();
            (index, name)
        })
        .collect();
    if !request.bytes().any(|byte| byte.is_ascii_alphabetic()) {
        return Ok(available);
    }
    request
        .as_bytes()
        .split(|byte| !byte.is_ascii_alphabetic())
        .flat_map(|letters| letters.chunks(2))
        .map(|name| {
            available
                .iter()
                .find(|(_, found)| found.as_bytes().eq_ignore_ascii_case(name))
                .cloned()
                .ok_or_else(|| {
                    listing_error(format!(
                        "unrecognized correlation {}",
                        String::from_utf8_lossy(name)
                    ))
                })
        })
        .collect()
}

struct ListingChunk<'a> {
    times: &'a ndarray::ArrayD<f64>,
    antenna1: &'a ndarray::ArrayD<i32>,
    antenna2: &'a ndarray::ArrayD<i32>,
    fields: &'a ndarray::ArrayD<i32>,
    descriptions: &'a ndarray::ArrayD<i32>,
    uvw: &'a ndarray::ArrayD<f64>,
    weight: &'a ndarray::ArrayD<f32>,
    flags: &'a ndarray::ArrayD<bool>,
    amplitude: &'a ndarray::ArrayD<f32>,
    phase: Option<&'a ndarray::ArrayD<f32>>,
}

impl<'a> ListingChunk<'a> {
    fn read(record: &'a RecordValue, columns: ListingColumns) -> MsResult<Self> {
        let floats = |name| match record.get(name) {
            Some(Value::Array(ArrayValue::Float32(values))) if !values.is_empty() => Ok(values),
            _ => Err(listing_error(format!(
                "visibility product {name} is unavailable"
            ))),
        };
        let doubles = |name| match record.get(name) {
            Some(Value::Array(ArrayValue::Float64(values))) => Ok(values),
            _ => Err(listing_error(format!(
                "visibility product {name} is unavailable"
            ))),
        };
        let integers = |name| match record.get(name) {
            Some(Value::Array(ArrayValue::Int32(values))) => Ok(values),
            _ => Err(listing_error(format!(
                "visibility product {name} is unavailable"
            ))),
        };
        let Some(Value::Array(ArrayValue::Bool(flags))) = record.get("flag") else {
            return Err(listing_error("visibility product flag is unavailable"));
        };
        let chunk = Self {
            times: doubles("time")?,
            antenna1: integers("antenna1")?,
            antenna2: integers("antenna2")?,
            fields: integers("field_id")?,
            descriptions: integers("data_desc_id")?,
            uvw: doubles("uvw")?,
            weight: floats("weight")?,
            flags,
            amplitude: floats(columns.amplitude)?,
            phase: columns.phase.map(floats).transpose()?,
        };
        let row_count = chunk.times.len();
        let cube_shape = chunk.amplitude.shape();
        if cube_shape.len() != 3
            || cube_shape[2] != row_count
            || chunk.times.shape() != [row_count]
            || chunk.antenna1.shape() != [row_count]
            || chunk.antenna2.shape() != [row_count]
            || chunk.fields.shape() != [row_count]
            || chunk.descriptions.shape() != [row_count]
            || chunk.uvw.shape() != [3, row_count]
            || chunk.weight.shape() != [cube_shape[0], row_count]
            || chunk.flags.shape() != cube_shape
            || chunk.phase.is_some_and(|phase| phase.shape() != cube_shape)
        {
            return Err(listing_error("inconsistent visibility listing shape"));
        }
        Ok(chunk)
    }
}

fn listing_error(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(format!("Error in MSLister::list: {}", message.into()))
}

fn valid_index(value: i32) -> MsResult<usize> {
    usize::try_from(value).map_err(|_| listing_error(format!("negative MS index {value}")))
}
