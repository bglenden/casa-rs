// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write a casacore MeasurementSet from UVFITS visibility groups.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_core::{ScalarValue, Value};
use casa_tables::publication::rename_without_replacing;
use casa_tables::{TableInfo, TableOptions};
use fitsio::FitsFile;

use super::groups::{GroupMetadata, VisibilityGroups, open_file};
use super::import_columns::{ImportColumns, MainRow, MainSamples, Spectra};
use super::invalid;
use super::metadata::{Extensions, Header};
use super::uvfits_ms::write_metadata;
use crate::schema::main_table::OptionalMainColumn;
use crate::subtable_rows::string_array;
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
    if output.try_exists().map_err(invalid)? {
        return Err(invalid(format!(
            "UVFITS output already exists: {}",
            output.display()
        )));
    }
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
    let layout = groups.layout();
    let rows = groups
        .count()
        .checked_mul(layout.windows)
        .ok_or_else(|| invalid("UVFITS row count overflow"))?;
    let visibility = [layout.corr.len(), layout.frequency.length];
    let tile = import_tiles(&header, &groups, options);
    let categories = defines_flag_category(rows, visibility, groups.binary_hdu().is_some());
    let mut columns = ImportColumns::start(
        &staging,
        rows,
        visibility,
        tile,
        categories,
        Spectra::Weight,
    )?;
    let usage = write_groups(&mut columns, &mut fits, &mut groups, &extensions)?;
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
    columns.finish(&mut ms, &staging)?;
    if let Some(table) = &extensions.system_power {
        table.save(TableOptions::new(&system_power))?;
    }
    rename_without_replacing(&staging, output).map_err(invalid)
}

fn defines_flag_category(rows: usize, visibility: [usize; 2], binary: bool) -> bool {
    // Casacore populates FLAG_CATEGORY in the binary and row-wise readers.
    // The random-groups reader switches to row-wise on large MAIN payloads.
    let estimate_kib = (rows / 1024)
        .saturating_mul(visibility[0])
        .saturating_mul(visibility[1])
        .saturating_mul(16);
    let host_kib =
        crate::selection::system_physical_memory_bytes().map_or(usize::MAX, |n| n / 1024);
    binary || estimate_kib >= host_kib.min(1_000_000)
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
    columns: &mut ImportColumns,
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
            let values = layout.visibility(samples, window);
            columns.append(
                MainRow {
                    antennas: group.antennas,
                    uvw: group.uvw,
                    time: group.time,
                    centroid: group.time,
                    interval: group.interval,
                    array: group.array,
                    field,
                    window: band,
                    scan: scan.number,
                },
                MainSamples {
                    data: &values.data,
                    flags: &values.flag,
                    weight: &values.weight,
                    sigma: &values.sigma,
                    weight_spectrum: Some(&values.weight_spectrum),
                    sigma_spectrum: None,
                },
            )?;
            if group.interval.is_none() {
                inferred_rows += 1;
            }
        }
    }
    if inferred_rows > 0 {
        let interval = (100.0 * inferred + 0.5).floor() / 100.0;
        for name in ["INTERVAL", "EXPOSURE"] {
            let id = columns.writer.scalar_column(name).map_err(invalid)?;
            columns
                .writer
                .push_repeated_scalar(id, Some(ScalarValue::Float64(interval)), inferred_rows)
                .map_err(invalid)?;
        }
    }
    Ok(used)
}
