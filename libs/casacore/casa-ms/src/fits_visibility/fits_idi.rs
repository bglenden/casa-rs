// SPDX-License-Identifier: LGPL-3.0-or-later
//! Import standard FITS Interferometry Data Interchange MeasurementSets.

mod auxiliary;
mod calibration;
mod digital;
mod metadata;
mod rows;

use std::collections::BTreeMap;
use std::path::Path;

use casa_core::Value;
use casa_tables::{Table, TableInfo, TableOptions};
use fitsio::FitsFile;
use fitsio::hdu::FitsHdu;

use super::groups::{open_file, optional_key};
use super::invalid;
use super::uvfits_ms::{configure_references, fits_date, insert, write_polarization};
use crate::schema::main_table::OptionalMainColumn;
use crate::subtable_rows::{f, i, s, string_array};
use crate::write_session::MeasurementSetCreateTarget;
use crate::{MeasurementSet, MeasurementSetBuilder, MsResult, SubtableId};
use metadata::{write_feeds, write_fields, write_frequency, write_geometry, write_observation};
use rows::{DataLayout, SpectralInput, TimeRange, append_hdu};

/// Layout and replacement policy for FITS-IDI imports.
#[derive(Clone, Copy, Debug, Default)]
pub struct FitsIdiOptions {
    /// Nonzero selects mosaic-style visibility tiling.
    pub observation_type: i32,
    /// Replace an existing writable MeasurementSet after the import succeeds.
    pub overwrite: bool,
}

/// Convert the FITS-IDI visibility buffers and extension metadata to MSv2.
///
/// # Errors
/// Returns an error for an unreadable source, inconsistent visibility axes
/// or linked identifiers, and when the destination cannot be created.
pub fn import_fits_idi(input: &Path, output: &Path, options: FitsIdiOptions) -> MsResult<()> {
    let mut fits = open_file(input)?;
    let destination =
        MeasurementSetCreateTarget::prepare(output, options.overwrite).map_err(invalid)?;
    let catalog = Catalog::read(&mut fits)?;
    let first = fits.hdu(catalog.first("UV_DATA")?).map_err(invalid)?;
    let layout = DataLayout::read(&mut fits, &first)?;
    let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    let builder = if layout.components == 3 {
        builder
            .with_main_column(OptionalMainColumn::WeightSpectrum)
            .with_main_column(OptionalMainColumn::SigmaSpectrum)
    } else {
        builder
    };
    let builder = if catalog.extensions.contains_key("SYSTEM_TEMPERATURE") {
        builder.with_optional_subtable(SubtableId::SysCal)
    } else {
        builder
    };
    let builder = if catalog.extensions.contains_key("WEATHER") {
        builder.with_optional_subtable(SubtableId::Weather)
    } else {
        builder
    };
    let mut ms = MeasurementSet::create_memory(builder)?;
    configure_main(&mut ms);
    let geometry_hdu = fits
        .hdu(catalog.first("ARRAY_GEOMETRY")?)
        .map_err(invalid)?;
    let frequency_hdu = fits.hdu(catalog.first("FREQUENCY")?).map_err(invalid)?;
    let source_hdu = fits.hdu(catalog.first("SOURCE")?).map_err(invalid)?;
    let feed_hdu = fits.hdu(catalog.first("ANTENNA")?).map_err(invalid)?;
    let mut geometry = write_geometry(&mut ms, &mut fits, &geometry_hdu)?;
    let frame = write_fields(&mut ms, &mut fits, &source_hdu)?;
    let resolutions = write_frequency(&mut ms, &mut fits, &frequency_hdu)?;
    write_feeds(&mut ms, &mut fits, &feed_hdu, &mut geometry)?;
    write_polarization(&mut ms, &layout.correlations)?;
    configure_references(&mut ms, frame, &geometry.epoch)?;
    let spectral = SpectralInput {
        resolutions: &resolutions,
        correlator: &catalog.correlator,
        correlator_version: catalog.correlator_version,
        van_vleck: catalog.van_vleck,
    };
    let mut times = TimeRange::default();
    for (scan, &index) in catalog.extensions["UV_DATA"].iter().enumerate() {
        let uv = fits.hdu(index).map_err(invalid)?;
        let actual = append_hdu(
            &mut fits,
            &uv,
            ms.main_table_mut(),
            &geometry,
            &spectral,
            scan as i32,
            &mut times,
        )?;
        if actual.channels != layout.channels
            || actual.bands != layout.bands
            || actual.correlations != layout.correlations
            || actual.components != layout.components
        {
            return Err(invalid(
                "FITS-IDI visibility layout changes between UV_DATA extensions",
            ));
        }
        write_history(&mut ms, &mut fits, &uv)?;
    }
    write_observation(&mut ms, &mut fits, &first, &geometry, &times)?;
    write_optional_subtables(&mut ms, &mut fits, &catalog, &geometry, resolutions.len())?;
    let tile = crate::ms::casa_visibility_tile_shape(
        layout.correlations.len(),
        layout.channels,
        &geometry.name,
    );
    let tile = if options.observation_type == 0 {
        tile
    } else {
        let rows = crate::ms::casa_interferometer_count(&geometry.name);
        let channels = (131_072 / layout.correlations.len() / rows).clamp(1, layout.channels);
        vec![layout.correlations.len(), channels, rows]
    };
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let payload = tempfile::tempdir_in(parent).map_err(invalid)?;
    let staging = payload.path().join("visibility.ms");
    let calibration = calibration_subtables(&mut ms, &mut fits, &catalog, &geometry, &times)?;
    ms.save_as_with_visibility_tiles(&staging, tile)?;
    for (name, table) in calibration {
        table.save(TableOptions::new(staging.join(name)))?;
    }
    drop(ms);
    std::fs::rename(staging, destination.staging_path()).map_err(invalid)?;
    destination.commit().map_err(invalid)
}

fn calibration_subtables(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    catalog: &Catalog,
    geometry: &metadata::Geometry,
    times: &TimeRange,
) -> MsResult<Vec<(&'static str, Table)>> {
    let mut tables = Vec::new();
    for (input, output) in [
        ("CALC", "EARTH_ORIENTATION"),
        ("PHASE-CAL", "PHASE_CAL"),
        ("GAIN_CURVE", "GAIN_CURVE"),
    ] {
        let Ok(index) = catalog.first(input) else {
            continue;
        };
        let hdu = fits.hdu(index).map_err(invalid)?;
        let table = match input {
            "CALC" => calibration::earth_orientation(fits, &hdu, geometry)?,
            "PHASE-CAL" => calibration::phase_calibration(fits, &hdu, geometry)?,
            _ => calibration::gain_curve(fits, &hdu, geometry, times)?,
        };
        ms.main_table_mut()
            .keywords_mut()
            .upsert(output, Value::table_ref(output));
        tables.push((output, table));
    }
    Ok(tables)
}

fn write_optional_subtables(
    ms: &mut MeasurementSet,
    fits: &mut FitsFile,
    catalog: &Catalog,
    geometry: &metadata::Geometry,
    spectral_windows: usize,
) -> MsResult<()> {
    for (name, extensions) in &catalog.extensions {
        for &index in extensions {
            match name.as_str() {
                "SYSTEM_TEMPERATURE" => {
                    let hdu = fits.hdu(index).map_err(invalid)?;
                    auxiliary::system_temperatures(ms, fits, &hdu, geometry)?;
                }
                "FLAG" => {
                    let hdu = fits.hdu(index).map_err(invalid)?;
                    auxiliary::flag_commands(ms, fits, &hdu, geometry, spectral_windows)?;
                }
                "WEATHER" => {
                    let hdu = fits.hdu(index).map_err(invalid)?;
                    auxiliary::weather(ms, fits, &hdu, geometry)?;
                }
                _ => (),
            }
        }
    }
    Ok(())
}

fn configure_main(ms: &mut MeasurementSet) {
    let table = ms.main_table_mut();
    table.set_info(TableInfo {
        table_type: "MeasurementSet".to_owned(),
        sub_type: "FITS-IDI".to_owned(),
        readme: vec![
            "This is a measurement set Table holding astronomical observations".to_owned(),
        ],
    });
    let mut keywords = table
        .column_keywords("FLAG_CATEGORY")
        .cloned()
        .unwrap_or_default();
    keywords.upsert(
        "CATEGORY",
        string_array(&["FLAG_CMD", "ORIGINAL", "USER"], vec![3]),
    );
    table.set_column_keywords("FLAG_CATEGORY", keywords);
}

struct Catalog {
    extensions: BTreeMap<String, Vec<usize>>,
    correlator: String,
    correlator_version: f64,
    van_vleck: f64,
}

impl Catalog {
    fn read(fits: &mut FitsFile) -> MsResult<Self> {
        let primary = fits.primary_hdu().map_err(invalid)?;
        let axes: i32 = primary.read_key(fits, "NAXIS").map_err(invalid)?;
        if axes > 0 {
            let first_axis: i64 = primary.read_key(fits, "NAXIS1").map_err(invalid)?;
            if first_axis == 0 || (axes == 2 && first_axis == 777_777_701) {
                return Err(invalid("FITS-IDI expects a primary array HDU"));
            }
        }
        let correlator = optional_key::<String>(&primary, fits, "CORRELAT")?
            .unwrap_or_default()
            .trim()
            .to_owned();
        let correlator_version = optional_key::<String>(&primary, fits, "FXCORVER")?
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or_default();
        let van_vleck = optional_key::<f64>(&primary, fits, "VANVLECK")?.unwrap_or_default();
        let mut extensions = BTreeMap::<String, Vec<usize>>::new();
        for index in 1..fits.num_hdus().map_err(invalid)? {
            let hdu = fits.hdu(index).map_err(invalid)?;
            let name = hdu.name(fits).map_err(invalid)?;
            extensions
                .entry(name.trim_end().to_owned())
                .or_default()
                .push(index);
        }
        Ok(Self {
            extensions,
            correlator,
            correlator_version,
            van_vleck,
        })
    }

    fn first(&self, extension: &str) -> MsResult<usize> {
        self.extensions
            .get(extension)
            .and_then(|items| items.first())
            .copied()
            .ok_or_else(|| invalid(format!("FITS-IDI has no {extension} extension")))
    }
}

fn write_history(ms: &mut MeasurementSet, fits: &mut FitsFile, hdu: &FitsHdu) -> MsResult<()> {
    let date = optional_key::<String>(hdu, fits, "DATE-OBS")?
        .or(optional_key(hdu, fits, "DATE")?)
        .unwrap_or_else(|| "2000-01-01".to_owned());
    let time = fits_date(&date)?;
    for item in super::metadata::history(fits)?
        .iter()
        .take_while(|item| !item.is_empty())
    {
        insert(
            ms,
            SubtableId::History,
            &[
                ("OBSERVATION_ID", i(0)),
                ("TIME", f(time)),
                ("PRIORITY", s("NORMAL")),
                ("ORIGIN", s("FITSIDItoMS1::fillObsTables")),
                ("APPLICATION", s("ms")),
                ("MESSAGE", s(item)),
            ],
        )?;
    }
    Ok(())
}
