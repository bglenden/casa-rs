// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write a casacore MeasurementSet from UVFITS visibility groups.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_core::{ArrayValue, Value};
use casa_tables::{Table, TableInfo, TableOptions};
use fitsio::FitsFile;

use super::groups::{GroupMetadata, GroupVisibility, VisibilityGroups, open_file};
use super::invalid;
use super::metadata::{Extensions, Header};
use super::uvfits_ms::write_metadata;
use crate::schema::main_table::{OptionalMainColumn, REQUIRED_COLUMNS};
use crate::subtable_rows::{b, f, f64_array, i, row_from_defs, string_array};
use crate::write_session::MeasurementSetCreateTarget;
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
    let mut ms = MeasurementSet::create_memory(
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
    let usage = write_groups(table, &mut fits, &mut groups, &extensions)?;
    write_metadata(
        &mut ms,
        &header,
        groups.layout(),
        &extensions,
        &usage,
        options,
    )?;
    let payload = tempfile::tempdir_in(
        output
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
    .map_err(invalid)?;
    let staging = payload.path().join("visibility.ms");
    let system_power = staging.join("SYSPOWER");
    if extensions.system_power.is_some() {
        ms.main_table_mut().keywords_mut().upsert(
            "SYSPOWER",
            Value::TableRef(casa_tables::strip_directory(&system_power, &staging)),
        );
    }
    ms.save_as_with_visibility_tiles(&staging, import_tiles(&header, &groups, options))?;
    if let Some(table) = &extensions.system_power {
        table.save(TableOptions::new(&system_power))?;
    }
    drop(ms);
    std::fs::rename(staging, destination.staging_path()).map_err(invalid)?;
    destination.commit().map_err(invalid)
}

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
    table: &mut Table,
    fits: &mut FitsFile,
    groups: &mut VisibilityGroups,
    extensions: &Extensions,
) -> MsResult<Usage> {
    let mut scans = BTreeMap::<i32, Scan>::new();
    let mut used = Usage::default();
    let mut seen = BTreeSet::new();
    let mut previous_time = 0.0;
    let mut inferred = f64::MAX;
    let mut explicit = false;
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
        explicit = group.interval.is_some();
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
                table,
                &group,
                field,
                band,
                scan.number,
                layout.visibility(samples, window),
            )?;
        }
    }
    if !explicit {
        let interval = (100.0 * inferred + 0.5).floor() / 100.0;
        for row in 0..table.row_count() {
            table.cell_accessor_mut(row, "INTERVAL")?.set(f(interval))?;
            table.cell_accessor_mut(row, "EXPOSURE")?.set(f(interval))?;
        }
    }
    Ok(used)
}

fn write_main_row(
    table: &mut Table,
    group: &GroupMetadata,
    field: i32,
    window: i32,
    scan: i32,
    visibility: GroupVisibility,
) -> MsResult<()> {
    let row_flag = visibility.flag.iter().all(|&value| value);
    let interval = group.interval.unwrap_or_default();
    let mut row = row_from_defs(
        REQUIRED_COLUMNS,
        &[
            ("ANTENNA1", i(group.antennas[0])),
            ("ANTENNA2", i(group.antennas[1])),
            ("ARRAY_ID", i(group.array)),
            ("DATA_DESC_ID", i(window)),
            ("FIELD_ID", i(field)),
            ("EXPOSURE", f(interval)),
            ("INTERVAL", f(interval)),
            ("FLAG_ROW", b(row_flag)),
            ("PROCESSOR_ID", i(-1)),
            ("STATE_ID", i(-1)),
            ("SCAN_NUMBER", i(scan)),
            ("TIME", f(group.time)),
            ("TIME_CENTROID", f(group.time)),
            ("UVW", f64_array(&group.uvw, vec![3])),
        ],
    );
    row.remove("FLAG_CATEGORY");
    for (name, value) in [
        ("DATA", ArrayValue::Complex32(visibility.data.into_dyn())),
        ("FLAG", ArrayValue::Bool(visibility.flag.into_dyn())),
        ("WEIGHT", ArrayValue::Float32(visibility.weight.into_dyn())),
        ("SIGMA", ArrayValue::Float32(visibility.sigma.into_dyn())),
        (
            "WEIGHT_SPECTRUM",
            ArrayValue::Float32(visibility.weight_spectrum.into_dyn()),
        ),
    ] {
        row.upsert(name, Value::Array(value));
    }
    table.add_row(row)?;
    Ok(())
}
