// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write a casacore MeasurementSet from UVFITS visibility groups.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_core::{ScalarValue, Value};
use casa_tables::streamed_tiled::StreamedTiledShapeValueType;
use casa_tables::{
    ColumnOverrides, GeneratedArrayColumn, GeneratedScalarColumn, TableInfo, TableOptions,
};
use fitsio::FitsFile;

use super::groups::{GroupMetadata, GroupVisibility, VisibilityGroups, open_file};
use super::invalid;
use super::metadata::{Extensions, Header};
use super::uvfits_ms::write_metadata;
use crate::schema::main_table::OptionalMainColumn;
use crate::subtable_rows::string_array;
use crate::write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetCreateTarget, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, standard_main_scalar_column_plans,
};
use crate::{MeasurementSet, MeasurementSetBuilder, MsResult, SubtableId};

/// Options that change a UVFITS import's persisted MeasurementSet layout.
#[derive(Clone, Copy, Debug, Default)]
pub struct UvFitsImportOptions {
    /// Zero uses the ordinary spectral-line tiles. Other values use the
    /// short-row tiles used for mosaics.
    pub observation_type: i32,
    /// Use the telescope-specific antenna name scheme when defined.
    pub new_antenna_names: bool,
}

/// Convert UVFITS visibilities, flags, weights and AIPS subtables to MSv2.
/// The named MeasurementSet is created only after the conversion completes.
///
/// # Errors
/// The input cannot be read or has incompatible visibility metadata, the
/// output already exists, or the converted MeasurementSet cannot be saved.
pub fn import_uvfits(input: &Path, output: &Path, options: UvFitsImportOptions) -> MsResult<()> {
    let destination = MeasurementSetCreateTarget::prepare(output, false).map_err(invalid)?;
    let mut fits = open_file(input)?;
    let mut groups = VisibilityGroups::open(&mut fits)?;
    let header = Header::read(&mut fits, groups.binary_hdu())?;
    let extensions = Extensions::read(&mut fits, &header, groups.layout())?;
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let payload = tempfile::tempdir_in(parent).map_err(invalid)?;
    let staging = payload.path().join("visibility.ms");
    std::fs::create_dir(&staging).map_err(invalid)?;
    let mut ms = MeasurementSet::create_staged(
        &staging,
        MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::WeightSpectrum)
            .with_optional_subtable(SubtableId::Source),
    )?;
    let table = ms.main_table_mut();
    table.set_info(TableInfo {
        table_type: "MeasurementSet".to_owned(),
        sub_type: "UVFITS".to_owned(),
        readme: vec![
            "This is a measurement set Table holding astronomical observations".to_owned(),
        ],
    });
    let mut flag_keywords = table
        .column_keywords("FLAG_CATEGORY")
        .cloned()
        .unwrap_or_default();
    flag_keywords.upsert(
        "CATEGORY",
        string_array(&["FLAG_CMD", "ORIGINAL", "USER"], vec![3]),
    );
    table.set_column_keywords("FLAG_CATEGORY", flag_keywords);
    let mut columns = ImportColumns::start(&staging, &header, &groups, options)?;
    let usage = write_groups(&mut columns.writer, &mut fits, &mut groups, &extensions)?;
    write_metadata(
        &mut ms,
        &header,
        groups.layout(),
        &extensions,
        &usage,
        options,
    )?;
    let system_power = staging.join("SYSPOWER");
    if extensions.system_power.is_some() {
        ms.main_table_mut().keywords_mut().upsert(
            "SYSPOWER",
            Value::TableRef(casa_tables::strip_directory(&system_power, &staging)),
        );
    }
    columns.finish(&mut ms)?;
    if let Some(table) = &extensions.system_power {
        table.save(TableOptions::new(&system_power))?;
    }
    std::fs::rename(staging, destination.staging_path()).map_err(invalid)?;
    destination.commit().map_err(invalid)
}

struct ImportColumns {
    writer: MeasurementSetWriteSession,
    rows: usize,
}

impl ImportColumns {
    fn start(
        path: &Path,
        header: &Header,
        groups: &VisibilityGroups,
        options: UvFitsImportOptions,
    ) -> MsResult<Self> {
        use MeasurementSetColumnStorage::{TiledColumn, TiledShape};
        use StreamedTiledShapeValueType::{Bool, Complex32, Float32, Float64};

        let layout = groups.layout();
        let rows = groups
            .count()
            .checked_mul(layout.windows)
            .ok_or_else(|| invalid("UVFITS row count overflow"))?;
        let tile = import_tiles(header, groups, options);
        let visibility = [layout.corr.len(), layout.frequency.length];
        let weight = [layout.corr.len()];
        let shape = |name: &str, value_type, cell: &[usize], tile_shape, storage_manager| {
            MeasurementSetArrayColumnPlan {
                name: name.into(),
                value_type,
                shapes: vec![MeasurementSetArrayShapePlan {
                    cell_shape: cell.to_vec(),
                    row_count: rows,
                    tile_shape,
                }],
                storage_manager,
            }
        };
        let weight_tile = crate::ms::casa_weight_tile_shape(&tile);
        let arrays = vec![
            shape("DATA", Complex32, &visibility, tile.clone(), TiledShape),
            shape("FLAG", Bool, &visibility, tile.clone(), TiledShape),
            shape(
                "WEIGHT_SPECTRUM",
                Float32,
                &visibility,
                tile.clone(),
                TiledShape,
            ),
            shape("WEIGHT", Float32, &weight, weight_tile.clone(), TiledShape),
            shape("SIGMA", Float32, &weight, weight_tile, TiledShape),
            shape(
                "UVW",
                Float64,
                &[3],
                crate::ms::casa_uvw_tile_shape(&tile),
                TiledColumn,
            ),
        ];
        let mut scalars = standard_main_scalar_column_plans();
        scalars.retain(|column| {
            !CONSTANT_COLUMNS
                .iter()
                .any(|(name, _)| *name == column.name)
        });
        let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(invalid)?;
        let plan =
            MeasurementSetWritePlan::variable_array_creation(rows, arrays, scalars, resources)
                .map_err(invalid)?;
        let writer = MeasurementSetWriteSession::start(path, plan).map_err(invalid)?;
        Ok(Self { writer, rows })
    }

    fn finish(self, ms: &mut MeasurementSet) -> MsResult<()> {
        let mut overrides = ColumnOverrides::for_row_count(self.rows);
        for name in ["DATA", "FLAG", "WEIGHT", "SIGMA", "WEIGHT_SPECTRUM", "UVW"] {
            overrides.insert_deferred(name);
        }
        overrides.insert_generated_array(
            "FLAG_CATEGORY",
            GeneratedArrayColumn::constant(self.rows, None),
        );
        for (name, value) in CONSTANT_COLUMNS {
            overrides.insert_generated_scalar(
                name,
                GeneratedScalarColumn::constant(self.rows, Some(ScalarValue::Int32(value))),
            );
        }
        self.writer
            .save_and_finish(ms, &overrides)
            .map_err(invalid)?;
        Ok(())
    }
}

const CONSTANT_COLUMNS: [(&str, i32); 5] = [
    ("FEED1", 0),
    ("FEED2", 0),
    ("OBSERVATION_ID", 0),
    ("PROCESSOR_ID", -1),
    ("STATE_ID", -1),
];

fn import_tiles(
    header: &Header,
    groups: &VisibilityGroups,
    options: UvFitsImportOptions,
) -> Vec<usize> {
    let correlations = groups.layout().corr.len();
    let channels = groups.layout().frequency.length;
    if options.observation_type == 0 {
        return crate::ms::casa_visibility_tile_shape(correlations, channels, &header.telescope);
    }
    let rows = crate::ms::casa_interferometer_count(&header.telescope);
    let channel_tile = (131_072 / correlations / rows).clamp(1, channels);
    vec![correlations, channel_tile, rows]
}

#[derive(Default)]
pub(super) struct Usage {
    pub(super) windows: usize,
    pub(super) occurrences: Vec<(i32, i32, f64)>,
}

#[derive(Default)]
struct Scan {
    number: i32,
    last_field: Option<i32>,
    last_frequency: Option<i32>,
}

impl Scan {
    fn record(&mut self, group: &GroupMetadata, gap: f64) {
        if self.last_field != Some(group.field)
            || self.last_frequency != Some(group.frequency)
            || gap > 300.0
        {
            self.number += 1;
            self.last_field = Some(group.field);
            self.last_frequency = Some(group.frequency);
        }
    }
}

fn write_groups(
    writer: &mut MeasurementSetWriteSession,
    fits: &mut FitsFile,
    groups: &mut VisibilityGroups,
    extensions: &Extensions,
) -> MsResult<Usage> {
    let mut scans = BTreeMap::<i32, Scan>::new();
    let mut used = Usage::default();
    let mut seen = BTreeSet::new();
    let mut previous_time = 0.0;
    let mut inferred = f64::MAX;
    let mut inferred_rows = 0;
    for index in 0..groups.count() {
        let (group, layout, samples) = groups.read(fits, index)?;
        if group
            .antennas
            .iter()
            .any(|antenna| !extensions.array.antennas.contains_key(antenna))
        {
            return Err(invalid(
                "A UVFITS visibility antenna is absent from its AN table",
            ));
        }
        let scan = scans.entry(group.array).or_default();
        let gap = group.time - previous_time;
        scan.record(&group, gap);
        previous_time = group.time;
        if gap > 0.01 {
            inferred = inferred.min(gap);
        }
        let field = if let Some(sources) = &extensions.sources {
            if !sources.contains_key(&group.field) {
                return Err(invalid(format!(
                    "UVFITS source {} is absent from its SU table",
                    group.field + 1
                )));
            }
            group.field
        } else {
            0
        };
        let first_window = (group.frequency as usize)
            .checked_mul(layout.windows)
            .ok_or_else(|| invalid("UVFITS frequency selection overflow"))?;
        for (window, band) in (0..layout.windows).zip(first_window..) {
            if extensions
                .spectral
                .as_ref()
                .map_or(band != 0, |windows| band >= windows.len())
            {
                return Err(invalid(format!(
                    "UVFITS spectral window {band} is missing its FQ metadata"
                )));
            }
            used.windows = used.windows.max(band + 1);
            let band = i32::try_from(band).map_err(invalid)?;
            if seen.insert((field, band)) {
                used.occurrences.push((field, band, group.time));
            }
            write_main_row(
                writer,
                &group,
                field,
                band,
                scan.number,
                layout.visibility(samples, window),
            )?;
            match group.interval {
                Some(interval) => {
                    writer
                        .push_scalar_row("INTERVAL", Some(ScalarValue::Float64(interval)))
                        .map_err(invalid)?;
                    writer
                        .push_scalar_row("EXPOSURE", Some(ScalarValue::Float64(interval)))
                        .map_err(invalid)?;
                }
                None => inferred_rows += 1,
            }
        }
    }
    if inferred_rows > 0 {
        let interval = (100.0 * inferred + 0.5).floor() / 100.0;
        for name in ["INTERVAL", "EXPOSURE"] {
            let id = writer.scalar_column(name).map_err(invalid)?;
            writer
                .push_repeated_scalar(id, Some(ScalarValue::Float64(interval)), inferred_rows)
                .map_err(invalid)?;
        }
    }
    Ok(used)
}

fn write_main_row(
    writer: &mut MeasurementSetWriteSession,
    group: &GroupMetadata,
    field: i32,
    window: i32,
    scan: i32,
    visibility: GroupVisibility,
) -> MsResult<()> {
    let row_flag = visibility.flag.iter().all(|&value| value);
    use ScalarValue::{Bool, Float64, Int32};
    for (name, value) in [
        ("ANTENNA1", Int32(group.antennas[0])),
        ("ANTENNA2", Int32(group.antennas[1])),
        ("ARRAY_ID", Int32(group.array)),
        ("DATA_DESC_ID", Int32(window)),
        ("FIELD_ID", Int32(field)),
        ("FLAG_ROW", Bool(row_flag)),
        ("SCAN_NUMBER", Int32(scan)),
        ("TIME", Float64(group.time)),
        ("TIME_CENTROID", Float64(group.time)),
    ] {
        writer.push_scalar_row(name, Some(value)).map_err(invalid)?;
    }
    let data = &visibility.data;
    writer
        .push_complex32_row(
            "DATA",
            data.shape(),
            data.as_slice_memory_order()
                .expect("contiguous FITS visibilities"),
        )
        .map_err(invalid)?;
    let flag = &visibility.flag;
    writer
        .push_bool_row(
            "FLAG",
            flag.shape(),
            flag.as_slice_memory_order().expect("contiguous FITS flags"),
        )
        .map_err(invalid)?;
    let weight = &visibility.weight_spectrum;
    writer
        .push_f32_row(
            "WEIGHT_SPECTRUM",
            weight.shape(),
            weight
                .as_slice_memory_order()
                .expect("contiguous FITS weights"),
        )
        .map_err(invalid)?;
    for (name, values) in [("WEIGHT", visibility.weight), ("SIGMA", visibility.sigma)] {
        writer
            .push_f32_row(
                name,
                values.shape(),
                values.as_slice().expect("contiguous correlation vector"),
            )
            .map_err(invalid)?;
    }
    writer
        .push_f64_row("UVW", &[3], &group.uvw)
        .map_err(invalid)
}
