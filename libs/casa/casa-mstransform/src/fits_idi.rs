// SPDX-License-Identifier: LGPL-3.0-or-later
//! Import standard FITS Interferometry Data Interchange MeasurementSets.

mod auxiliary;
mod calibration;
mod digital;
mod metadata;
mod rows;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_core::Value;
use casa_tables::{Table, TableInfo, TableOptions};
use fitsio::FitsFile;
use fitsio::hdu::{FitsHdu, HduInfo};

use casa_ms::fits_visibility::reader_support::{
    ImportColumns, Spectra, binary_extension, binary_extension_name, configure_references, f,
    fits_date, history, i, insert, invalid, open_file, optional_key, s, string_array,
    write_polarization,
};
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::{MeasurementSet, MeasurementSetBuilder, MsResult, SubtableId};
use metadata::{write_feeds, write_fields, write_frequency, write_geometry, write_observation};
use rows::{DataLayout, TimeRange, VisibilityInput, append_hdu};

/// Layout and replacement policy for FITS-IDI imports.
#[derive(Clone, Copy, Debug, Default)]
pub struct FitsIdiOptions {
    /// Nonzero selects mosaic-style visibility tiling.
    pub observation_type: i32,
    /// Replace an existing writable MeasurementSet after the import succeeds.
    pub overwrite: bool,
}

/// An MSv2 conversion held in a private sibling directory until publication.
pub struct StagedFitsIdi {
    directory: tempfile::TempDir,
    output: PathBuf,
    overwrite: bool,
    diagnostics: Vec<String>,
}

impl StagedFitsIdi {
    /// Publish the completed MS and return its correlator diagnostics.
    ///
    /// # Errors
    /// Returns an error if the destination cannot be created or replaced.
    pub fn publish(self) -> MsResult<Vec<String>> {
        publish_import(
            &self.directory.path().join("visibility.ms"),
            &self.output,
            self.overwrite,
        )?;
        Ok(self.diagnostics)
    }
}

/// Convert FITS-IDI visibility and extension metadata into a private MSv2.
///
/// Hold the result until the caller is ready to publish the completed table.
///
/// # Errors
/// Returns an error for an unreadable source, inconsistent visibility axes
/// or linked identifiers, and when the destination cannot be created.
pub fn stage_fits_idi(
    input: &Path,
    output: &Path,
    options: FitsIdiOptions,
) -> MsResult<StagedFitsIdi> {
    let mut fits = open_file(input)?;
    if output.file_name().is_none() {
        return Err(invalid("FITS-IDI output filename is missing"));
    }
    if output.try_exists().map_err(invalid)? {
        if !options.overwrite {
            return Err(invalid(format!(
                "FITS-IDI output already exists: {}",
                output.display()
            )));
        }
        check_replacement_target(output)?;
    }
    let catalog = Catalog::read(&mut fits)?;
    let (layout, visibility, row_count) = catalog.visibility(&mut fits)?;
    let first = &visibility[0];
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let payload = tempfile::Builder::new()
        .prefix(".fits-idi-")
        .tempdir_in(parent)
        .map_err(invalid)?;
    let staging = payload.path().join("visibility.ms");
    std::fs::create_dir(&staging).map_err(invalid)?;
    let mut ms = MeasurementSet::create_staged(&staging, idi_builder(&layout, &catalog))?;
    configure_main(&mut ms);
    let geometry_hdu = read_extension(&mut fits, catalog.first("ARRAY_GEOMETRY")?)?;
    let frequency_hdu = read_extension(&mut fits, catalog.first("FREQUENCY")?)?;
    let source_hdu = read_extension(&mut fits, catalog.first("SOURCE")?)?;
    let feed_hdu = read_extension(&mut fits, catalog.first("ANTENNA")?)?;
    let mut geometry = write_geometry(&mut ms, &mut fits, &geometry_hdu)?;
    let (frame, field_count) = write_fields(&mut ms, &mut fits, &source_hdu)?;
    let resolutions = write_frequency(
        &mut ms,
        &mut fits,
        &frequency_hdu,
        layout.bands,
        layout.channels,
    )?;
    write_feeds(&mut ms, &mut fits, &feed_hdu, &mut geometry)?;
    write_polarization(&mut ms, &layout.correlations)?;
    configure_references(&mut ms, frame, &geometry.epoch, false)?;
    let dimensions = [layout.correlations.len(), layout.channels];
    let tile = visibility_tile(dimensions, &geometry.name, options.observation_type);
    let spectra = if layout.components == 3 {
        Spectra::WeightAndSigma
    } else {
        Spectra::None
    };
    let mut columns = ImportColumns::start(&staging, row_count, dimensions, tile, true, spectra)?;
    let correlator = if catalog.correlator.is_empty() && geometry.name == "VLBA" {
        "VLBA"
    } else {
        &catalog.correlator
    };
    let input = VisibilityInput {
        resolutions: &resolutions,
        field_count,
        correlator,
        correlator_version: catalog.correlator_version,
        van_vleck: catalog.van_vleck,
    };
    let mut times = TimeRange::default();
    for (scan, uv) in visibility.iter().enumerate() {
        append_hdu(
            &mut fits,
            uv,
            &mut columns,
            &geometry,
            &input,
            scan as i32,
            &mut times,
        )?;
        write_history(&mut ms, &mut fits, uv)?;
    }
    write_observation(&mut ms, &mut fits, first, &geometry, &times)?;
    let diagnostics =
        write_optional_subtables(&mut ms, &mut fits, &catalog, &geometry, resolutions.len())?;
    let calibration = calibration_subtables(&mut ms, &mut fits, &catalog, &geometry, &times)?;
    columns.finish(&mut ms, &staging)?;
    for (name, table) in calibration {
        table.save(TableOptions::new(staging.join(name)))?;
    }
    Ok(StagedFitsIdi {
        directory: payload,
        output: output.to_owned(),
        overwrite: options.overwrite,
        diagnostics,
    })
}

fn idi_builder(layout: &DataLayout, catalog: &Catalog) -> MeasurementSetBuilder {
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
    if catalog.extensions.contains_key("WEATHER") {
        builder.with_optional_subtable(SubtableId::Weather)
    } else {
        builder
    }
}

fn visibility_tile(
    [hands, channels]: [usize; 2],
    telescope: &str,
    observation_type: i32,
) -> Vec<usize> {
    if observation_type == 0 {
        casa_ms::casa_visibility_tile_shape(hands, channels, telescope)
    } else {
        let rows = casa_ms::casa_interferometer_count(telescope);
        vec![hands, (131_072 / hands / rows).clamp(1, channels), rows]
    }
}

fn publish_import(staging: &Path, output: &Path, overwrite: bool) -> MsResult<()> {
    use casa_tables::publication::rename_without_replacing;
    match rename_without_replacing(staging, output) {
        Ok(()) => Ok(()),
        Err(error) if overwrite && error.kind() == std::io::ErrorKind::AlreadyExists => {
            replace_import(staging, output)
        }
        Err(error) => Err(invalid(error)),
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn check_replacement_target(output: &Path) -> MsResult<()> {
    if !output.symlink_metadata().map_err(invalid)?.is_dir() || !output.join("table.dat").is_file()
    {
        return Err(invalid(
            "FITS-IDI overwrite requires an existing casacore table directory",
        ));
    }
    casa_tables::publication::check_write_access(output).map_err(invalid)?;
    Table::open(TableOptions::new(output))?;
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn replace_import(staging: &Path, output: &Path) -> MsResult<()> {
    use casa_tables::publication::{exchange_directories, lock_table_tree_for_replacement};
    check_replacement_target(output)?;
    let guards = lock_table_tree_for_replacement(output)?;
    check_replacement_target(output)?;
    if !guards
        .first()
        .is_some_and(|guard| guard.matches_table_path(output))
    {
        return Err(invalid("FITS-IDI output changed during replacement"));
    }
    exchange_directories(staging, output).map_err(invalid)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn check_replacement_target(_output: &Path) -> MsResult<()> {
    Err(invalid(
        "Atomic MeasurementSet replacement is unavailable on this platform",
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn replace_import(_staging: &Path, output: &Path) -> MsResult<()> {
    check_replacement_target(output)
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
        let hdu = read_extension(fits, index)?;
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
) -> MsResult<Vec<String>> {
    let mut diagnostics = Vec::new();
    for (name, extensions) in &catalog.extensions {
        for &index in extensions {
            match name.as_str() {
                "SYSTEM_TEMPERATURE" => {
                    let hdu = read_extension(fits, index)?;
                    auxiliary::system_temperatures(ms, fits, &hdu, geometry)?;
                }
                "FLAG" => {
                    let hdu = read_extension(fits, index)?;
                    auxiliary::flag_commands(ms, fits, &hdu, geometry, spectral_windows)?;
                }
                "WEATHER" => {
                    let hdu = read_extension(fits, index)?;
                    auxiliary::weather(ms, fits, &hdu, geometry)?;
                }
                "MODEL_COMPS" if geometry.name == "VLBA" => {
                    let hdu = read_extension(fits, index)?;
                    let fft = optional_key::<i32>(&hdu, fits, "FFT_TWID")?.unwrap_or_default();
                    let taper = optional_key::<String>(&hdu, fits, "TAPER_FN")?.unwrap_or_default();
                    if fft != 1 {
                        diagnostics.push(
                            "Data needs FFT artifact corrections; this is not yet implemented."
                                .to_owned(),
                        );
                    }
                    if taper.trim() != "UNIFORM" {
                        diagnostics.push(format!(
                            "Data was correlated with {} taper; support for this taper is not yet implemented.",
                            taper.trim(),
                        ));
                    }
                }
                _ => (),
            }
        }
    }
    Ok(diagnostics)
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
            .and_then(|value| value.trim().parse::<f32>().ok())
            .map(f64::from)
            .unwrap_or_default();
        let van_vleck = optional_key::<f64>(&primary, fits, "VANVLECK")?.unwrap_or_default();
        let mut extensions = BTreeMap::<String, Vec<usize>>::new();
        for index in 1..fits.num_hdus().map_err(invalid)? {
            if let Some(name) = binary_extension_name(fits, index)? {
                extensions.entry(name).or_default().push(index);
            }
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

    fn visibility(&self, fits: &mut FitsFile) -> MsResult<(DataLayout, Vec<FitsHdu>, usize)> {
        let mut visibility = Vec::new();
        let mut layout: Option<DataLayout> = None;
        let mut total = 0_usize;
        for &index in self.extensions.get("UV_DATA").into_iter().flatten() {
            let hdu = read_extension(fits, index)?;
            let actual = DataLayout::read(fits, &hdu)?;
            if let Some(expected) = &layout
                && !actual.same_output_shape(expected)
            {
                return Err(invalid(
                    "FITS-IDI visibility layout changes between UV_DATA extensions",
                ));
            }
            let HduInfo::TableInfo { num_rows, .. } = hdu.info else {
                return Err(invalid("UV_DATA is not a FITS binary table"));
            };
            total = num_rows
                .checked_mul(actual.bands)
                .and_then(|rows| total.checked_add(rows))
                .ok_or_else(|| invalid("FITS-IDI row count overflow"))?;
            layout.get_or_insert(actual);
            visibility.push(hdu);
        }
        let layout = layout
            .filter(|_| total > 0)
            .ok_or_else(|| invalid("FITS-IDI contains no visibility rows"))?;
        Ok((layout, visibility, total))
    }
}

fn read_extension(fits: &mut FitsFile, index: usize) -> MsResult<FitsHdu> {
    if binary_extension_name(fits, index)?.is_none() {
        return Err(invalid("FITS-IDI requires named binary extensions"));
    }
    binary_extension(fits, index)
}

fn write_history(ms: &mut MeasurementSet, fits: &mut FitsFile, hdu: &FitsHdu) -> MsResult<()> {
    let date = optional_key::<String>(hdu, fits, "DATE-OBS")?
        .or(optional_key(hdu, fits, "DATE")?)
        .unwrap_or_else(|| "2000-01-01".to_owned());
    let time = fits_date(&date)?;
    for item in history(fits)?.iter().take_while(|item| !item.is_empty()) {
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
