// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frame-only updates for legacy regridspw leave visibility cells in place.

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::path::{Path, PathBuf};

use casa_tables::publication::exchange_directories;
use casa_tables::{RequiredScalarColumnValues, Table, TableOptions, TablePartNames};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Array1;

use crate::subtables::{MsDataDescription, MsSpectralWindow, get_i32};
use crate::{MeasurementSet, SubtableId};

use super::super::{MsRegridOptions, MsTransformError};
use super::{Frame, invalid, ms_error, radial_shift};

const DDID: &str = "DATA_DESC_ID";
const SPW_ID: &str = "SPECTRAL_WINDOW_ID";
const SOURCE_ID: &str = "SOURCE_ID";

/// Transform the spectral metadata of a private writable MeasurementSet.
/// This implements the frame-only branch of CASA 6.7.6.14's `SubMS::regridSpw`.
/// Each changed field/window pair gets its own spectral coordinates at the
/// first integration of the pair. MAIN rows retain their order and arrays.
///
/// # Errors
/// The private stage is malformed, can't resolve its measures, has an
/// externally linked table that would need mutation, or cannot be saved.
pub fn reframe_staged_ms(
    stage: &Path,
    options: &MsRegridOptions,
) -> Result<bool, MsTransformError> {
    let mut ms = MeasurementSet::open(stage).map_err(ms_error)?;
    let paths = local_tables(&ms, stage)?;
    let Some(layout) = Layout::new(&ms, options)? else {
        return Ok(false);
    };
    layout.write(&mut ms, &paths)?;
    Ok(true)
}

struct LocalTables {
    windows: PathBuf,
    descriptions: PathBuf,
    source: Option<PathBuf>,
}

fn local_tables(ms: &MeasurementSet, stage: &Path) -> Result<LocalTables, MsTransformError> {
    let root = stage.canonicalize().map_err(ms_error)?;
    if !matches!(
        Table::immediate_part_names(&root, &root).map_err(ms_error)?,
        TablePartNames::Plain(_)
    ) {
        return Err(invalid(
            "Frame-only regridding needs a physical MAIN table.".into(),
        ));
    }
    let path = |id| {
        let linked = crate::ms::linked_subtables::strict_subtable_path(ms.main_table(), &root, id)
            .map_err(ms_error)?;
        linked
            .map(|path| {
                let path = path.canonicalize().map_err(ms_error)?;
                if path == root || !path.starts_with(&root) {
                    return Err(invalid(format!(
                        "{} lies outside the staging directory.",
                        id.name()
                    )));
                }
                if !matches!(
                    Table::immediate_part_names(&path, &path).map_err(ms_error)?,
                    TablePartNames::Plain(_)
                ) {
                    return Err(invalid(format!("{} must be a physical table.", id.name())));
                }
                Ok(path)
            })
            .transpose()
    };
    let required =
        |id: SubtableId| path(id)?.ok_or_else(|| invalid(format!("Missing {}.", id.name())));
    path(SubtableId::History)?;
    Ok(LocalTables {
        windows: required(SubtableId::SpectralWindow)?,
        descriptions: required(SubtableId::DataDescription)?,
        source: path(SubtableId::Source)?,
    })
}

struct Layout {
    windows: Vec<RecordValue>,
    descriptions: Vec<RecordValue>,
    sources: Vec<(i32, i32, i32)>,
    row_descriptions: Vec<i32>,
}

impl Layout {
    fn new(
        ms: &MeasurementSet,
        options: &MsRegridOptions,
    ) -> Result<Option<Self>, MsTransformError> {
        let source_frame = options.outframe.eq_ignore_ascii_case("SOURCE");
        let target = if source_frame {
            FrequencyRef::GEO
        } else if let Ok(reference) = options.outframe.parse::<FrequencyRef>() {
            reference
        } else if options.outframe.is_empty() {
            return Ok(None);
        } else {
            return Err(super::invalid_choice("outframe", &options.outframe));
        };
        let main = ms.main_table();
        let rows = (0..ms.row_count()).collect::<Vec<_>>();
        let mut scalars = main
            .required_scalar_columns_owned_for_rows(&[DDID, "FIELD_ID", "TIME"], &rows)
            .map_err(ms_error)?;
        let (
            Some(RequiredScalarColumnValues::Int32(descriptions)),
            Some(RequiredScalarColumnValues::Int32(fields)),
            Some(RequiredScalarColumnValues::Float64(times)),
        ) = (
            scalars.remove(DDID),
            scalars.remove("FIELD_ID"),
            scalars.remove("TIME"),
        )
        else {
            return Err(invalid(
                "Invalid time, field or description column for regridding.".into(),
            ));
        };
        let desc = ms
            .subtable(SubtableId::DataDescription)
            .ok_or_else(|| invalid("Missing DATA_DESCRIPTION.".into()))?;
        let windows = ms
            .subtable(SubtableId::SpectralWindow)
            .ok_or_else(|| invalid("Missing SPECTRAL_WINDOW.".into()))?;
        let spectral = MsSpectralWindow::new(windows);
        let descriptions_to_windows = (0..desc.row_count())
            .map(|row| MsDataDescription::new(desc).spectral_window_id(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ms_error)?;
        let references = (0..spectral.row_count())
            .map(|row| {
                let code = spectral.meas_freq_ref(row).map_err(ms_error)?;
                FrequencyRef::from_casacore_code(code)
                    .ok_or_else(|| invalid(format!("Invalid MEAS_FREQ_REF {code}.")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut layout = Self {
            windows: Vec::new(),
            descriptions: Vec::new(),
            sources: Vec::new(),
            row_descriptions: vec![0; rows.len()],
        };
        let mut window_ids = BTreeMap::new();
        let mut description_ids = BTreeMap::new();
        let mut sorted = rows;
        sorted.sort_by(|&left, &right| times[left].total_cmp(&times[right]));
        let mut changed = false;
        let mut source_rows = BTreeSet::new();
        let field_table = ms.field().map_err(ms_error)?;
        let has_source = ms.subtable(SubtableId::Source).is_some();
        for row in sorted {
            let ddid = descriptions[row] as usize;
            let window = *descriptions_to_windows
                .get(ddid)
                .ok_or_else(|| invalid(format!("Invalid MAIN DATA_DESC_ID {ddid}.")))?
                as usize;
            let reference = *references
                .get(window)
                .ok_or_else(|| invalid(format!("Invalid DATA_DESCRIPTION {SPW_ID} {window}.")))?;
            let convert = reference != target || source_frame;
            changed |= convert;
            let field = fields[row];
            let key = (window, convert.then_some(field));
            let output_spw = match window_ids.entry(key) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let mut record = windows
                        .row_accessor()
                        .row(window)
                        .map_err(ms_error)?
                        .clone();
                    if convert {
                        let mut options = options.clone();
                        options.phasecenter = field.to_string();
                        let frame = Frame::new(ms, &options, row)?.ok_or_else(|| {
                            invalid("A target frequency frame is required.".into())
                        })?;
                        update_window(&mut record, &spectral, window, reference, &frame)?;
                    }
                    let output = i32::try_from(layout.windows.len()).map_err(ms_error)?;
                    layout.windows.push(record);
                    *entry.insert(output)
                }
            };
            let output_ddid = match description_ids.entry((ddid, output_spw)) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let mut record = desc.row_accessor().row(ddid).map_err(ms_error)?.clone();
                    set_i32(&mut record, SPW_ID, output_spw);
                    let output = i32::try_from(layout.descriptions.len()).map_err(ms_error)?;
                    layout.descriptions.push(record);
                    *entry.insert(output)
                }
            };
            if has_source && source_rows.insert((field, output_spw)) {
                let source = field_table.source_id(field as usize).map_err(ms_error)?;
                if source >= 0 {
                    layout.sources.push((source, window as i32, output_spw));
                }
            }
            layout.row_descriptions[row] = output_ddid;
        }
        Ok(changed.then_some(layout))
    }

    fn write(self, ms: &mut MeasurementSet, paths: &LocalTables) -> Result<(), MsTransformError> {
        replace_rows(&paths.windows, self.windows)?;
        replace_rows(&paths.descriptions, self.descriptions)?;
        if let Some(source) = &paths.source {
            rewrite_sources(source, &self.sources)?;
        }
        let main = ms.main_table_mut();
        {
            let mut column = main.column_accessor_mut(DDID).map_err(ms_error)?;
            for (row, value) in self.row_descriptions.into_iter().enumerate() {
                column
                    .set(row, Value::Scalar(ScalarValue::Int32(value)))
                    .map_err(ms_error)?;
            }
        }
        main.prepare_write()
            .save_selected_columns(&[DDID])
            .map_err(ms_error)
    }
}

fn update_window(
    record: &mut RecordValue,
    spectral: &MsSpectralWindow<'_>,
    window: usize,
    reference: FrequencyRef,
    frame: &Frame,
) -> Result<(), MsTransformError> {
    let convert = frame.prepare(reference, &frame.target)?;
    let shift = radial_shift(-frame.source_velocity.unwrap_or(0.));
    let apply = |frequency| shift.shift_frequency_hz(convert.convert_hz(frequency));
    let input = spectral.chan_freq(window).map_err(ms_error)?;
    let interval = |widths: Vec<f64>| {
        if widths.len() != input.len() {
            return Err(invalid(
                "Spectral metadata axes have different lengths.".into(),
            ));
        }
        Ok(input
            .iter()
            .zip(widths)
            .map(|(&centre, width)| {
                shift.shift_frequency_hz(
                    convert.convert_hz(centre + width / 2.)
                        - convert.convert_hz(centre - width / 2.),
                )
            })
            .collect::<Vec<_>>())
    };
    let centres = input.iter().copied().map(apply).collect::<Vec<_>>();
    let widths = interval(
        spectral
            .chan_width(window)
            .map_err(ms_error)?
            .into_iter()
            .map(f64::abs)
            .collect(),
    )?;
    let resolution = interval(spectral.resolution(window).map_err(ms_error)?)?;
    let Some((&first, &last, &first_width, &last_width)) = centres
        .first()
        .zip(centres.last())
        .zip(widths.first().zip(widths.last()))
        .map(|((first, last), (first_width, last_width))| (first, last, first_width, last_width))
    else {
        return Err(invalid("The spectral window has no channels.".into()));
    };
    let reference_hz = apply(spectral.ref_frequency(window).map_err(ms_error)?);
    let bandwidth = (last - first).abs() + (first_width.abs() + last_width.abs()) / 2.;
    for (column, values) in [
        ("CHAN_FREQ", centres),
        ("CHAN_WIDTH", widths),
        ("RESOLUTION", resolution),
    ] {
        record.upsert(
            column,
            Value::Array(ArrayValue::Float64(Array1::from_vec(values).into_dyn())),
        );
    }
    record.upsert(
        "REF_FREQUENCY",
        Value::Scalar(ScalarValue::Float64(reference_hz)),
    );
    record.upsert(
        "TOTAL_BANDWIDTH",
        Value::Scalar(ScalarValue::Float64(bandwidth)),
    );
    set_i32(
        record,
        "MEAS_FREQ_REF",
        frame.metadata_reference.casacore_code(),
    );
    Ok(())
}

fn set_i32(record: &mut RecordValue, column: &str, value: i32) {
    record.upsert(column, Value::Scalar(ScalarValue::Int32(value)));
}

fn replace_rows(path: &Path, records: Vec<RecordValue>) -> Result<(), MsTransformError> {
    let original = Table::open(TableOptions::new(path)).map_err(ms_error)?;
    let schema = original
        .schema()
        .ok_or_else(|| invalid("Missing table schema".into()))?;
    let mut table = Table::with_schema(schema.clone());
    table.copy_info(&original);
    *table.keywords_mut() = original.keywords().clone();
    for column in schema.columns() {
        if let Some(keywords) = original.column_keywords(column.name()) {
            table.set_column_keywords(column.name(), keywords.clone());
        }
    }
    drop(original);
    for record in records {
        table.add_row(record).map_err(ms_error)?;
    }
    let workspace =
        tempfile::tempdir_in(path.parent().unwrap_or_else(|| Path::new("."))).map_err(ms_error)?;
    let staged = workspace.path().join("subtable");
    table
        .prepare_write()
        .save(TableOptions::new(&staged))
        .map_err(ms_error)?;
    exchange_directories(&staged, path).map_err(ms_error)
}

fn rewrite_sources(path: &Path, links: &[(i32, i32, i32)]) -> Result<(), MsTransformError> {
    let table = Table::open(TableOptions::new(path)).map_err(ms_error)?;
    if table.row_count() == 0 || links.is_empty() {
        return Ok(());
    }
    let pairs = (0..table.row_count())
        .map(|row| {
            Ok((
                get_i32(&table, row, SOURCE_ID)?,
                get_i32(&table, row, SPW_ID)?,
            ))
        })
        .collect::<crate::MsResult<Vec<_>>>()
        .map_err(ms_error)?;
    let replaced = links
        .iter()
        .map(|&(source, _, window)| (source, window))
        .collect::<BTreeSet<_>>();
    let mut retained = BTreeSet::new();
    let mut records = Vec::new();
    for (row, &pair) in pairs.iter().enumerate() {
        if !replaced.contains(&pair) && retained.insert(pair) {
            records.push(table.row_accessor().row(row).map_err(ms_error)?.clone());
        }
    }
    for &(source, previous, output) in links {
        let row = pairs
            .iter()
            .position(|&(id, window)| id == source && (window == previous || window == -1))
            .ok_or_else(|| {
                invalid(format!(
                    "No SOURCE row for source {source} and SPW {previous}."
                ))
            })?;
        let mut record = table.row_accessor().row(row).map_err(ms_error)?.clone();
        set_i32(&mut record, SPW_ID, output);
        records.push(record);
    }
    drop(table);
    replace_rows(path, records)
}
