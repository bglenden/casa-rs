// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frame-only updates for legacy regridspw leave visibility cells in place.

use std::collections::{BTreeMap, BTreeSet, HashMap, btree_map::Entry};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use casa_tables::publication::copy_locked_table_tree_with_shared_files;
use casa_tables::{
    RequiredScalarColumnValues, Table, TableOptions, TablePartNames, TableWriteLock,
};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Array1;

use crate::subtables::{MsDataDescription, MsSpectralWindow, get_i32};
use crate::{MeasurementSet, SubtableId};

use super::super::{MsRegridOptions, MsTransformError, maybe_log_transform_progress};
use super::{Frame, FrameGeometry, invalid, ms_error, radial_shift};

const DDID: &str = "DATA_DESC_ID";
const SPW_ID: &str = "SPECTRAL_WINDOW_ID";
const SOURCE_ID: &str = "SOURCE_ID";

/// Stage a locked MS for a frame-only `regridspw` and retain the unchanged
/// storage payloads outside the DATA_DESC_ID manager. The source locks must
/// be held until the caller publishes or removes the staged table.
///
/// # Errors
/// Returns an error if the input descriptor or any copied file is unreadable.
pub fn stage_frame_only_ms(source: &Path, stage: &Path) -> Result<(), MsTransformError> {
    let main = Table::open_metadata_only(TableOptions::new(source)).map_err(ms_error)?;
    let reusable = main
        .data_manager_info()
        .iter()
        .filter(|manager| !manager.columns.iter().any(|name| name == DDID))
        .map(|manager| manager.seq_nr)
        .collect::<BTreeSet<_>>();
    copy_locked_table_tree_with_shared_files(source, stage, |name| {
        let Some(file) = name.to_str().and_then(|name| name.strip_prefix("table.f")) else {
            return false;
        };
        let manager = if let Some((manager, cube)) = file.split_once("_TSM") {
            if cube.parse::<u32>().is_err() {
                return false;
            }
            manager
        } else {
            file.strip_suffix('i').unwrap_or(file)
        };
        manager
            .parse::<u32>()
            .is_ok_and(|manager| reusable.contains(&manager))
    })
    .map_err(ms_error)
}

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
    let started = Instant::now();
    let (mut ms, paths) = open_frame_tables(stage)?;
    let Some(layout) = Layout::new(&ms, options, None)? else {
        return Ok(false);
    };
    maybe_log_transform_progress("frame_only_plan", started.elapsed(), started.elapsed());
    let metadata_started = Instant::now();
    let windows = ms
        .subtable(SubtableId::SpectralWindow)
        .ok_or_else(|| invalid("Missing SPECTRAL_WINDOW.".into()))?;
    let spectral = MsSpectralWindow::new(windows);
    let geometry = FrameGeometry::open(&ms, options, true)?;
    let records = layout.window_records(windows, |_, origin, record| {
        if let Some(frame) = origin.frame_at_reference(options, &geometry)? {
            update_window(
                record,
                &spectral,
                origin.spw as usize,
                origin.reference,
                &frame,
            )?;
        }
        Ok(())
    })?;
    let descriptions = ms
        .subtable(SubtableId::DataDescription)
        .ok_or_else(|| invalid("Missing DATA_DESCRIPTION.".into()))?;
    layout.write_subtables(&paths, windows, descriptions, records)?;
    maybe_log_transform_progress(
        "frame_only_subtables",
        metadata_started.elapsed(),
        started.elapsed(),
    );

    let descriptions_started = Instant::now();
    write_main_descriptions(
        ms.main_table_mut(),
        &layout.original_descriptions,
        &layout.row_descriptions,
    )?;
    maybe_log_transform_progress(
        "frame_only_main_descriptions",
        descriptions_started.elapsed(),
        started.elapsed(),
    );
    Ok(true)
}

fn open_frame_tables(stage: &Path) -> Result<(MeasurementSet, LocalTables), MsTransformError> {
    let main = Table::open(TableOptions::new(stage)).map_err(ms_error)?;
    let paths = local_tables(&main, stage)?;
    let read_links = [
        SubtableId::Antenna,
        SubtableId::Field,
        SubtableId::Observation,
    ];
    let mut links = HashMap::from([
        (SubtableId::SpectralWindow, paths.windows.clone()),
        (SubtableId::DataDescription, paths.descriptions.clone()),
    ]);
    if let Some(source) = &paths.source {
        links.insert(SubtableId::Source, source.clone());
    }
    for id in read_links {
        if let Some(path) =
            crate::ms::linked_subtables::strict_subtable_path(&main, stage, id).map_err(ms_error)?
        {
            links.insert(id, path);
        }
    }
    let ms = MeasurementSet::with_open_main(main, stage.to_owned(), &links).map_err(ms_error)?;
    Ok((ms, paths))
}

fn write_main_descriptions(
    main: &mut Table,
    original: &[i32],
    ddids: &[i32],
) -> Result<(), MsTransformError> {
    if original == ddids {
        return Ok(());
    }
    if main.data_manager_info().iter().any(|manager| {
        manager.dm_type == "StandardStMan" && manager.columns.iter().any(|column| column == DDID)
    }) {
        let replacements = BTreeMap::from([(
            DDID.to_owned(),
            ddids
                .iter()
                .map(|&value| Some(Value::Scalar(ScalarValue::Int32(value))))
                .collect(),
        )]);
        let mut lock = TableWriteLock::acquire_to_reread(
            main.path().expect("a staged MAIN has a table path"),
            1,
        )
        .map_err(ms_error)?;
        let mut edit = main
            .prepare_column_replacement(&replacements, None, &mut lock)
            .map_err(ms_error)?;
        edit.publish().map_err(ms_error)?;
        return edit.synchronize().map_err(ms_error);
    }
    let mut changed = Vec::new();
    {
        let mut writer = main.row_accessor_mut().prepare(&[DDID]).map_err(ms_error)?;
        for (row, (&previous, &value)) in original.iter().zip(ddids).enumerate() {
            if previous == value {
                continue;
            }
            writer.seek(row).map_err(ms_error)?;
            writer
                .set_value_at(0, Value::Scalar(ScalarValue::Int32(value)))
                .map_err(ms_error)?;
            changed.push(row);
        }
    }
    if !changed.is_empty() {
        main.prepare_write()
            .save_selected_rows(&[DDID], &changed)
            .map_err(ms_error)?;
    }
    Ok(())
}

struct LocalTables {
    windows: PathBuf,
    descriptions: PathBuf,
    source: Option<PathBuf>,
}

fn local_tables(main: &Table, stage: &Path) -> Result<LocalTables, MsTransformError> {
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
        let linked =
            crate::ms::linked_subtables::strict_subtable_path(main, &root, id).map_err(ms_error)?;
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

pub(super) struct WindowOrigin {
    pub(super) spw: i32,
    pub(super) reference: FrequencyRef,
    field: Option<i32>,
    reference_time: f64,
}

impl WindowOrigin {
    pub(super) fn frame_at_reference(
        &self,
        options: &MsRegridOptions,
        geometry: &Arc<FrameGeometry>,
    ) -> Result<Option<Frame>, MsTransformError> {
        let Some(field) = self.field else {
            return Ok(None);
        };
        let mut options = options.clone();
        options.phasecenter = field.to_string();
        Frame::new(&options, self.reference_time, field as usize, geometry).map(Some)
    }
}

pub(super) struct Layout {
    pub(super) windows: Vec<WindowOrigin>,
    descriptions: Vec<RecordValue>,
    description_windows: Vec<i32>,
    sources: Vec<(i32, i32, i32)>,
    row_descriptions: Vec<i32>,
    original_descriptions: Vec<i32>,
}

impl Layout {
    pub(super) fn new(
        ms: &MeasurementSet,
        options: &MsRegridOptions,
        rows: Option<&[usize]>,
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
        if !source_frame && references.iter().all(|&reference| target == reference) {
            return Ok(None);
        }
        let (descriptions, fields, times) = layout_columns(ms.main_table(), rows)?;
        let mut layout = Self {
            windows: Vec::new(),
            descriptions: Vec::new(),
            description_windows: Vec::new(),
            sources: Vec::new(),
            row_descriptions: vec![-1; ms.row_count()],
            original_descriptions: Vec::new(),
        };
        let mut window_ids = BTreeMap::new();
        let mut description_ids = BTreeMap::new();
        let mut sorted = (0..times.len()).collect::<Vec<_>>();
        sorted.sort_by(|&left, &right| times[left].total_cmp(&times[right]));
        let mut changed = false;
        let mut source_rows = BTreeSet::new();
        let field_table = ms.field().map_err(ms_error)?;
        let has_source = ms.subtable(SubtableId::Source).is_some();
        let mut previous_pair = None;
        for slot in sorted {
            let row = rows.map_or(slot, |rows| rows[slot]);
            let ddid = descriptions[slot] as usize;
            let field = fields[slot];
            if let Some((previous_ddid, previous_field, output)) = previous_pair
                && ddid == previous_ddid
                && field == previous_field
            {
                layout.row_descriptions[row] = output;
                continue;
            }
            let window = *descriptions_to_windows
                .get(ddid)
                .ok_or_else(|| invalid(format!("Invalid MAIN DATA_DESC_ID {ddid}.")))?
                as usize;
            let reference = *references
                .get(window)
                .ok_or_else(|| invalid(format!("Invalid DATA_DESCRIPTION {SPW_ID} {window}.")))?;
            let convert = reference != target || source_frame;
            changed |= convert;
            let key = (window, convert.then_some(field));
            let output_spw = match window_ids.entry(key) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let output = i32::try_from(layout.windows.len()).map_err(ms_error)?;
                    layout.windows.push(WindowOrigin {
                        spw: window as i32,
                        reference,
                        field: key.1,
                        reference_time: times[slot],
                    });
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
                    layout.description_windows.push(output_spw);
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
            previous_pair = Some((ddid, field, output_ddid));
        }
        if rows.is_none() {
            layout.original_descriptions = descriptions;
        }
        Ok(changed.then_some(layout))
    }

    pub(super) fn description(&self, row: usize) -> Option<i32> {
        self.row_descriptions
            .get(row)
            .copied()
            .filter(|&id| id >= 0)
    }

    pub(super) fn window(&self, row: usize) -> Option<i32> {
        self.description(row)
            .map(|id| self.description_windows[id as usize])
    }

    fn window_records(
        &self,
        windows: &Table,
        mut populate: impl FnMut(usize, &WindowOrigin, &mut RecordValue) -> Result<(), MsTransformError>,
    ) -> Result<Vec<RecordValue>, MsTransformError> {
        let descriptor = windows.row_accessor();
        self.windows
            .iter()
            .enumerate()
            .map(|(id, origin)| {
                let mut record = descriptor
                    .row(origin.spw as usize)
                    .map_err(ms_error)?
                    .clone();
                populate(id, origin, &mut record)?;
                Ok(record)
            })
            .collect()
    }

    pub(super) fn write_resampled(
        &self,
        stage: &Path,
        mut populate: impl FnMut(usize, &WindowOrigin, &mut RecordValue) -> Result<(), MsTransformError>,
    ) -> Result<(), MsTransformError> {
        let main = Table::open(TableOptions::new(stage)).map_err(ms_error)?;
        let paths = local_tables(&main, stage)?;
        let windows = Table::open(TableOptions::new(&paths.windows)).map_err(ms_error)?;
        let descriptions = Table::open(TableOptions::new(&paths.descriptions)).map_err(ms_error)?;
        let records = self.window_records(&windows, &mut populate)?;
        self.write_subtables(&paths, &windows, &descriptions, records)
    }

    fn write_subtables(
        &self,
        paths: &LocalTables,
        original_windows: &Table,
        original_descriptions: &Table,
        windows: Vec<RecordValue>,
    ) -> Result<(), MsTransformError> {
        replace_staged_rows(&paths.windows, original_windows, windows)?;
        replace_staged_rows(
            &paths.descriptions,
            original_descriptions,
            self.descriptions.clone(),
        )?;
        if let Some(source) = &paths.source {
            rewrite_sources(source, &self.sources)?;
        }
        Ok(())
    }
}

type LayoutColumns = (Vec<i32>, Vec<i32>, Vec<f64>);

fn layout_columns(main: &Table, rows: Option<&[usize]>) -> Result<LayoutColumns, MsTransformError> {
    let columns = [DDID, "FIELD_ID", "TIME"];
    let mut scalars = match rows {
        Some(rows) => main.required_scalar_columns_owned_for_rows(&columns, rows),
        None => main.required_scalar_columns_owned(&columns),
    }
    .map_err(ms_error)?;
    match (
        scalars.remove(DDID),
        scalars.remove("FIELD_ID"),
        scalars.remove("TIME"),
    ) {
        (
            Some(RequiredScalarColumnValues::Int32(descriptions)),
            Some(RequiredScalarColumnValues::Int32(fields)),
            Some(RequiredScalarColumnValues::Float64(times)),
        ) => Ok((descriptions, fields, times)),
        _ => Err(invalid(
            "Invalid time, field or description column for regridding.".into(),
        )),
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

fn replace_staged_rows(
    path: &Path,
    original: &Table,
    records: Vec<RecordValue>,
) -> Result<(), MsTransformError> {
    let schema = original
        .schema()
        .ok_or_else(|| invalid("Missing table schema".into()))?;
    let mut table = Table::with_schema(schema.clone());
    table.copy_info(original);
    *table.keywords_mut() = original.keywords().clone();
    for column in schema.columns() {
        if let Some(keywords) = original.column_keywords(column.name()) {
            table.set_column_keywords(column.name(), keywords.clone());
        }
    }
    for record in records {
        table.add_row(record).map_err(ms_error)?;
    }
    // LocalTables checked that this physical subtable is inside the private
    // stage. The outer transaction owns cleanup if saving it fails.
    fs::remove_dir_all(path).map_err(ms_error)?;
    table
        .prepare_write()
        .save(TableOptions::new(path))
        .map_err(ms_error)
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
    replace_staged_rows(path, &table, records)
}
