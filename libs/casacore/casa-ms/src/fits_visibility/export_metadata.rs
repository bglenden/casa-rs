// SPDX-License-Identifier: LGPL-3.0-or-later
//! AIPS frequency, antenna and source extension tables for UVFITS output.

use std::collections::BTreeMap;
use std::sync::Arc;

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::epoch::EpochRef;
use casa_core::measures::{
    MEpoch, MPosition, MeasFrame, MeasuresProvider, MjdHighPrec, PositionRef,
};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, ScalarValue, Value};
use casa_tables::Table;
use fitsio::FitsFile;
use fitsio::hdu::FitsHdu;
use fitsio::tables::{ColumnDataType as Type, ColumnDescription, WritesCol};

use super::export_calibration::{write_system_calibration, write_weather};
use super::export_layout::{OutputLayout, f64_cell, has_column, i32_cell, subtable};
use super::invalid;
use super::uvfits_export::{UvFitsExportOptions, fits_date};
use crate::{MeasurementSet, MsResult, SubtableId};

pub(super) struct ExportHeader {
    pub(super) telescope: String,
    pub(super) observer: String,
    pub(super) object: String,
    pub(super) epoch: f64,
    pub(super) epoch_reference: EpochRef,
    pub(super) direction_deg: [f64; 2],
    pub(super) antenna_numbers: Vec<i32>,
    pub(super) time: f64,
    pub(super) rest_frequency: f64,
    pub(super) measures: Option<Arc<dyn MeasuresProvider>>,
}

impl ExportHeader {
    pub(super) fn read(ms: &MeasurementSet, layout: &OutputLayout) -> MsResult<Self> {
        let obs = subtable(ms, SubtableId::Observation)?;
        let telescope = string_cell(obs, 0, "TELESCOPE_NAME")?.to_owned();
        let antenna = subtable(ms, SubtableId::Antenna)?;
        let numbers = (0..antenna.row_count())
            .map(|row| {
                let name = string_cell(antenna, row, "NAME")?;
                let parse = if telescope.contains("VLA") {
                    name.trim_start_matches(|c: char| !c.is_ascii_digit())
                } else {
                    name
                };
                Ok(parse.parse::<i32>().ok())
            })
            .collect::<MsResult<Option<Vec<_>>>>();
        let mut antenna_numbers = match numbers {
            Ok(Some(numbers))
                if numbers.iter().all(|&number| number >= 1)
                    && numbers
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == numbers.len() =>
            {
                numbers
            }
            Ok(_) => (1..=antenna.row_count() as i32).collect(),
            Err(error) => return Err(error),
        };
        antenna_numbers.shrink_to_fit();
        let field = subtable(ms, SubtableId::Field)?;
        let direction_reference = measure_ref(field, "PHASE_DIR")
            .parse::<DirectionRef>()
            .unwrap_or(DirectionRef::J2000);
        let epoch = if direction_reference.as_str().contains("1950") {
            1950.0
        } else {
            2000.0
        };
        let (object, direction_deg) = if layout.multi {
            ("MULTI".to_owned(), [0.0; 2])
        } else {
            let row = layout.field as usize;
            let mut angles = direction_degrees(field, row)?;
            angles[0] = angles[0].rem_euclid(360.0);
            (string_cell(field, row, "NAME")?.to_owned(), angles)
        };
        let rest_frequency = ms
            .subtable(SubtableId::Source)
            .filter(|source| source.row_count() > 0)
            .map_or(0.0, |source| {
                optional_array_first(source, 0, "REST_FREQUENCY")
            });
        let first_row = layout
            .rows
            .iter()
            .map(|row| row.index)
            .min()
            .expect("export has rows");
        let epoch_reference = measure_ref(ms.main_table(), "TIME")
            .parse::<EpochRef>()
            .unwrap_or(EpochRef::UTC);
        Ok(Self {
            telescope,
            observer: string_cell(obs, 0, "OBSERVER")?.to_owned(),
            object,
            epoch,
            epoch_reference,
            direction_deg,
            antenna_numbers,
            time: f64_cell(ms.main_table(), first_row, "TIME")?,
            rest_frequency,
            measures: fits_measures(),
        })
    }

    pub(super) fn frame(&self, time: f64, reference: EpochRef) -> MsResult<MeasFrame> {
        let mjd = time / 86_400.0;
        let mut frame =
            MeasFrame::new().with_epoch(MEpoch::new(MjdHighPrec::from_mjd(mjd), reference));
        if let Some(measures) = &self.measures {
            frame = frame.with_measures(Arc::clone(measures));
        }
        if frame.dut1_for_mjd(mjd)?.is_none() {
            frame = frame.with_dut1(0.0);
        }
        Ok(frame)
    }

    pub(super) fn separate_baselines(&self) -> bool {
        self.antenna_numbers.iter().any(|&number| number >= 256)
    }
}

#[cfg(not(test))]
pub(super) fn fits_measures() -> Option<Arc<dyn MeasuresProvider>> {
    use casa_measures::MeasuresRuntime;
    use std::sync::Mutex;

    // Catalogs are immutable for the lifetime of a runtime. Keep one provider
    // across exports, and follow a newly selected data root on the next call.
    static RUNTIME: Mutex<Option<Arc<MeasuresRuntime>>> = Mutex::new(None);
    let root = MeasuresRuntime::discover_candidates()
        .ok()?
        .into_iter()
        .find(|candidate| candidate.complete)?
        .path
        .canonicalize()
        .ok()?;
    let mut current = RUNTIME.lock().ok()?;
    if let Some(runtime) = current.as_ref()
        && runtime.root() == root
    {
        return Some(runtime.clone());
    }
    let runtime = Arc::new(MeasuresRuntime::open(root, Default::default()).ok()?);
    *current = Some(runtime.clone());
    Some(runtime)
}

#[cfg(test)]
pub(super) fn fits_measures() -> Option<Arc<dyn MeasuresProvider>> {
    crate::open_measures_runtime().ok()
}

pub(super) fn write_extensions(
    fits: &mut FitsFile,
    ms: &MeasurementSet,
    layout: &OutputLayout,
    metadata: &ExportHeader,
    options: &UvFitsExportOptions<'_>,
) -> MsResult<()> {
    write_frequency(fits, layout, &metadata.telescope)?;
    write_antenna(fits, ms, layout, metadata, options.writestation)?;
    if layout.multi || layout.astron_bin_width.is_some() {
        write_source(fits, ms, layout, metadata)?;
    }
    if options.writesyscal {
        write_system_calibration(fits, ms, layout, metadata)?;
    }
    write_weather(fits, ms, metadata)
}

fn write_frequency(fits: &mut FitsFile, layout: &OutputLayout, telescope: &str) -> MsResult<()> {
    let count = if layout.combine {
        layout.windows.len()
    } else {
        1
    };
    let hdu = create_table(
        fits,
        "AIPS FQ",
        &[
            ("FRQSEL", Type::Int, 1, ""),
            ("IF FREQ", Type::Double, count, "HZ"),
            ("CH WIDTH", Type::Float, count, "HZ"),
            ("TOTAL BANDWIDTH", Type::Float, count, "HZ"),
            ("SIDEBAND", Type::Int, count, ""),
        ],
    )?;
    hdu.write_key(fits, "EXTVER", 1).map_err(invalid)?;
    hdu.write_key(fits, "NO_IF", count as i32)
        .map_err(invalid)?;
    let ids = if layout.combine {
        vec![1]
    } else {
        (1..=layout.windows.len() as i32).collect()
    };
    let offsets = layout
        .windows
        .iter()
        .map(|window| {
            if matches!(telescope, "IRAM PDB" | "IRAM_PDB") {
                0.0
            } else {
                window.frequency - layout.reference
            }
        })
        .collect::<Vec<_>>();
    let increments = layout
        .windows
        .iter()
        .map(|window| {
            (if telescope == "ALMA" {
                layout.step.abs().copysign(window.increment)
            } else {
                layout.step
            }) as f32
        })
        .collect::<Vec<_>>();
    let bandwidths = layout
        .windows
        .iter()
        .map(|window| window.bandwidth as f32)
        .collect::<Vec<_>>();
    let sidebands = layout
        .windows
        .iter()
        .map(|window| {
            if telescope == "ALMA" {
                if window.increment < 0.0 { -1 } else { 1 }
            } else {
                window.sideband
            }
        })
        .collect::<Vec<_>>();
    put(&hdu, fits, "FRQSEL", &ids)?;
    put(&hdu, fits, "IF FREQ", &offsets)?;
    put(&hdu, fits, "CH WIDTH", &increments)?;
    put(&hdu, fits, "TOTAL BANDWIDTH", &bandwidths)?;
    put(&hdu, fits, "SIDEBAND", &sidebands)
}

fn write_antenna(
    fits: &mut FitsFile,
    ms: &MeasurementSet,
    layout: &OutputLayout,
    meta: &ExportHeader,
    station: bool,
) -> MsResult<()> {
    let antenna = subtable(ms, SubtableId::Antenna)?;
    let obs = subtable(ms, SubtableId::Observation)?;
    let names = fingerprints(antenna, "NAME")?;
    let stations = fingerprints(antenna, "STATION")?;
    let names = if (station || all_equal(&names)) && !all_equal(&stations) {
        stations
    } else {
        names
    };
    let frame = measure_ref(antenna, "POSITION")
        .parse::<PositionRef>()
        .unwrap_or(PositionRef::ITRF);
    let feeds = relevant_feeds(ms, layout)?;
    let cells = antenna_cells(antenna, frame, &feeds)?;
    for array in 0..obs.row_count() {
        let telescope = string_cell(obs, array, "TELESCOPE_NAME")?;
        let hdu = create_table(
            fits,
            "AIPS AN",
            &[
                ("ANNAME", Type::String, 8, ""),
                ("STABXYZ", Type::Double, 3, "METERS"),
                ("ORBPARM", Type::Double, 0, ""),
                ("NOSTA", Type::Int, 1, ""),
                ("MNTSTA", Type::Int, 1, ""),
                ("STAXOF", Type::Float, 1, "METERS"),
                ("POLTYA", Type::String, 1, ""),
                ("POLAA", Type::Float, 1, "DEGREES"),
                ("POLCALA", Type::Float, 1, ""),
                ("POLTYB", Type::String, 1, ""),
                ("POLAB", Type::Float, 1, "DEGREES"),
                ("POLCALB", Type::Float, 1, ""),
                ("DIAMETER", Type::Float, 1, ""),
            ],
        )?;
        hdu.write_key(fits, "EXTVER", (array + 1) as i32)
            .map_err(invalid)?;
        write_antenna_keywords(fits, &hdu, telescope, layout.reference, meta)?;
        put(&hdu, fits, "ANNAME", &names)?;
        put(&hdu, fits, "STABXYZ", &cells.positions)?;
        put(&hdu, fits, "NOSTA", &meta.antenna_numbers)?;
        put(&hdu, fits, "MNTSTA", &cells.mounts)?;
        put(&hdu, fits, "STAXOF", &cells.offsets)?;
        put(&hdu, fits, "DIAMETER", &cells.diameters)?;
        for (index, suffix) in ["A", "B"].into_iter().enumerate() {
            put(
                &hdu,
                fits,
                &format!("POLTY{suffix}"),
                &cells.polarizations[index],
            )?;
            put(&hdu, fits, &format!("POLA{suffix}"), &cells.angles[index])?;
            put(
                &hdu,
                fits,
                &format!("POLCAL{suffix}"),
                &vec![0.0f32; names.len()],
            )?;
        }
    }
    Ok(())
}

fn write_antenna_keywords(
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    telescope: &str,
    reference: f64,
    meta: &ExportHeader,
) -> MsResult<()> {
    let time = MjdHighPrec::from_mjd(meta.time / 86_400.0);
    let epoch = MEpoch::new(time, meta.epoch_reference);
    let frame = meta.frame(meta.time, meta.epoch_reference)?;
    let utc = epoch.convert_to(EpochRef::UTC, &frame).map_err(invalid)?;
    let iat_utc = epoch
        .convert_to(EpochRef::TAI, &frame)
        .map(|tai| (tai.value().as_mjd() - utc.value().as_mjd()) * 86_400.0)
        .unwrap_or(0.0);
    let utc_mjd = utc.value().as_mjd();
    let start = (utc_mjd + iat_utc / 86_400.0).floor();
    let (x, y) = frame
        .polar_motion_for_mjd(utc_mjd.floor())?
        .unwrap_or((0.0, 0.0));
    let polar_m = |arcseconds: f64| 6_356_752.31 * (arcseconds / 3600.0).to_radians();
    let dut1 = frame.dut1_for_mjd(utc_mjd)?.unwrap_or(0.0);
    let gst = [start, start + 1.0].map(|day| {
        MEpoch::new(MjdHighPrec::from_mjd(day), EpochRef::UTC)
            .convert_to(EpochRef::GMST1, &frame)
            .map(|value| value.value().as_mjd())
    });
    let [gst, next] = gst;
    let gst = gst.map_err(invalid)?;
    let degrees = 360.0 * (next.map_err(invalid)? - gst);
    for (key, value) in [
        ("ARRAYX", 0.0),
        ("ARRAYY", 0.0),
        ("ARRAYZ", 0.0),
        ("GSTIA0", 360.0 * gst.fract()),
        ("DEGPDY", degrees),
        ("FREQ", reference),
        ("POLARX", polar_m(x)),
        ("POLARY", polar_m(y)),
        ("UT1UTC", dut1),
        ("IATUTC", iat_utc),
    ] {
        hdu.write_key(fits, key, value).map_err(invalid)?;
    }
    for (key, value) in [("NUMORB", 0), ("NOPCAL", 0)] {
        hdu.write_key(fits, key, value).map_err(invalid)?;
    }
    let date = fits_date(meta.time, false);
    let reference = meta.epoch_reference.to_string();
    for (key, value) in [
        ("RDATE", date.as_str()),
        ("TIMSYS", reference.as_str()),
        ("ARRNAM", telescope),
        ("POLTYPE", " "),
        ("XYZHAND", "RIGHT"),
        ("FRAME", "ITRF"),
    ] {
        hdu.write_key(fits, key, value).map_err(invalid)?;
    }
    Ok(())
}

type Feed = [(String, f32); 2];

fn relevant_feeds(ms: &MeasurementSet, layout: &OutputLayout) -> MsResult<BTreeMap<i32, Feed>> {
    let table = subtable(ms, SubtableId::Feed)?;
    let mut feeds = BTreeMap::new();
    for row in 0..table.row_count() {
        let id = i32_cell(table, row, "ANTENNA_ID")?;
        let spw = i32_cell(table, row, "SPECTRAL_WINDOW_ID")?;
        if feeds.contains_key(&id)
            || (spw >= 0 && !layout.windows.iter().any(|window| window.id == spw))
        {
            continue;
        }
        let ArrayValue::String(types) = table.cell_accessor(row, "POLARIZATION_TYPE")?.array()?
        else {
            return Err(invalid("Invalid feed polarization type"));
        };
        let angles = numeric_array(table, row, "RECEPTOR_ANGLE")?;
        let mut hands = std::array::from_fn(|_| (" ".to_owned(), 0.0));
        for ((hand, pol), angle) in hands.iter_mut().zip(types.iter()).zip(angles) {
            *hand = (pol.as_str().to_owned(), angle.to_degrees() as f32);
        }
        feeds.insert(id, hands);
    }
    Ok(feeds)
}

#[derive(Default)]
struct AntennaCells {
    positions: Vec<f64>,
    mounts: Vec<i32>,
    offsets: Vec<f32>,
    diameters: Vec<f32>,
    polarizations: [Vec<String>; 2],
    angles: [Vec<f32>; 2],
}

fn antenna_cells(
    table: &Table,
    frame: PositionRef,
    feeds: &BTreeMap<i32, Feed>,
) -> MsResult<AntennaCells> {
    let mut cells = AntennaCells::default();
    for row in 0..table.row_count() {
        let position = numeric_array(table, row, "POSITION")?;
        let [x, y, z]: [f64; 3] = position
            .try_into()
            .map_err(|_| invalid("Invalid antenna position shape"))?;
        let position = match frame {
            PositionRef::ITRF => MPosition::new_itrf(x, y, z),
            PositionRef::WGS84 => MPosition::new_wgs84(x, y, z),
        };
        cells.positions.extend(
            position
                .convert_to(PositionRef::ITRF)
                .map_err(invalid)?
                .values(),
        );
        let mount = string_cell(table, row, "MOUNT")?.to_uppercase();
        let names = [
            "ALT-AZ+NASMYTH-R",
            "ALT-AZ+NASMYTH-L",
            "ALT-AZ+BWG-R",
            "ALT-AZ+BWG-L",
            "ALT-AZ",
            "EQUATORIAL",
            "ORBIT",
            "X-Y",
            "BIZARRE",
        ];
        cells.mounts.push(
            names
                .iter()
                .position(|name| mount.contains(name))
                .map_or(9, |index| [4, 5, 6, 7, 0, 1, 2, 3, 8][index]),
        );
        cells.offsets.push(
            numeric_array(table, row, "OFFSET")?
                .first()
                .copied()
                .unwrap_or_default() as f32,
        );
        cells
            .diameters
            .push(f64_cell(table, row, "DISH_DIAMETER")? as f32);
        for index in 0..2 {
            let feed = feeds.get(&(row as i32)).map(|hands| &hands[index]);
            cells.polarizations[index]
                .push(feed.map_or_else(|| " ".to_owned(), |value| value.0.clone()));
            cells.angles[index].push(feed.map_or(0.0, |value| value.1));
        }
    }
    Ok(cells)
}

fn write_source(
    fits: &mut FitsFile,
    ms: &MeasurementSet,
    layout: &OutputLayout,
    meta: &ExportHeader,
) -> MsResult<()> {
    let field = subtable(ms, SubtableId::Field)?;
    let fields = fingerprints(field, "NAME")?;
    let width = layout.windows.len();
    let hdu = create_table(
        fits,
        "AIPS SU",
        &[
            ("ID. NO.", Type::Int, 1, ""),
            ("SOURCE", Type::String, 20, ""),
            ("QUAL", Type::Int, 1, ""),
            ("CALCODE", Type::String, 4, ""),
            ("IFLUX", Type::Float, width, "JY"),
            ("QFLUX", Type::Float, width, "JY"),
            ("UFLUX", Type::Float, width, "JY"),
            ("VFLUX", Type::Float, width, "JY"),
            ("FREQOFF", Type::Double, width, "HZ"),
            ("BANDWIDTH", Type::Double, 1, "HZ"),
            ("RAEPO", Type::Double, 1, "DEGREES"),
            ("DECEPO", Type::Double, 1, "DEGREES"),
            ("EPOCH", Type::Double, 1, "YEARS"),
            ("RAAPP", Type::Double, 1, "DEGREES"),
            ("DECAPP", Type::Double, 1, "DEGREES"),
            ("LSRVEL", Type::Double, width, "M/SEC"),
            ("RESTFREQ", Type::Double, width, "HZ"),
            ("PMRA", Type::Double, 1, "DEG/DAY"),
            ("PMDEC", Type::Double, 1, "DEG/DAY"),
        ],
    )?;
    hdu.write_key(fits, "EXTVER", 1).map_err(invalid)?;
    hdu.write_key(fits, "NO_IF", width as i32)
        .map_err(invalid)?;
    hdu.write_key(fits, "FREQID", 1).map_err(invalid)?;
    if layout.astron_bin_width.is_some() {
        let spectral = subtable(ms, SubtableId::SpectralWindow)?;
        for (keyword, column) in [
            ("VELTYP", "NFRA_VELOCDEFINITION"),
            ("VELDEF", "NFRA_CONVERSIONTYPE"),
        ] {
            if has_column(spectral, column) && spectral.is_cell_defined(0, column)? {
                hdu.write_key(
                    fits,
                    keyword,
                    string_cell(spectral, 0, column)?.to_ascii_uppercase(),
                )
                .map_err(invalid)?;
            }
        }
    }
    let sources = ms.subtable(SubtableId::Source);
    let mut cells = SourceCells::default();
    for (&id, &number) in &layout.fields {
        let row = id as usize;
        let name = fields.get(row).ok_or_else(|| invalid("Invalid field ID"))?;
        cells.ids.push(number);
        cells.names.push(name.clone());
        cells.qualifiers.push(
            if fields.iter().filter(|other| other == &name).count() > 1 {
                id
            } else {
                0
            },
        );
        cells.directions.push(direction_degrees(field, row)?);
        cells.apparent.push(apparent_directions(field, row, meta)?);
        let source_id = i32_cell(field, row, "SOURCE_ID")?;
        let source = sources.and_then(|table| {
            (0..table.row_count())
                .find(|&index| i32_cell(table, index, "SOURCE_ID").ok() == Some(source_id))
                .map(|index| (table, index))
        });
        cells.record_source(source, width)?;
    }
    cells.write(fits, &hdu, width, layout.windows[0].bandwidth, meta.epoch)
}

#[derive(Default)]
struct SourceCells {
    ids: Vec<i32>,
    names: Vec<String>,
    qualifiers: Vec<i32>,
    codes: Vec<String>,
    directions: Vec<[f64; 2]>,
    apparent: Vec<[f64; 2]>,
    frequencies: Vec<f64>,
    velocities: Vec<f64>,
    motion: Vec<[f64; 2]>,
}

impl SourceCells {
    fn record_source(&mut self, source: Option<(&Table, usize)>, width: usize) -> MsResult<()> {
        if let Some((table, row)) = source {
            self.codes.push(string_cell(table, row, "CODE")?.to_owned());
            self.motion.push(proper_motion(table, row)?);
            self.frequencies.extend(std::iter::repeat_n(
                optional_array_first(table, row, "REST_FREQUENCY"),
                width,
            ));
            self.velocities.extend(std::iter::repeat_n(
                optional_array_first(table, row, "SYSVEL"),
                width,
            ));
        } else {
            self.codes.push(String::new());
            self.motion.push([0.0; 2]);
            self.frequencies.extend(std::iter::repeat_n(0.0, width));
            self.velocities.extend(std::iter::repeat_n(0.0, width));
        }
        Ok(())
    }

    fn write(
        self,
        fits: &mut FitsFile,
        hdu: &FitsHdu,
        width: usize,
        bandwidth: f64,
        epoch: f64,
    ) -> MsResult<()> {
        let count = self.ids.len();
        put(hdu, fits, "ID. NO.", &self.ids)?;
        put(hdu, fits, "SOURCE", &self.names)?;
        put(hdu, fits, "QUAL", &self.qualifiers)?;
        put(hdu, fits, "CALCODE", &self.codes)?;
        let flux = vec![0.0f32; width * count];
        for name in ["IFLUX", "QFLUX", "UFLUX", "VFLUX"] {
            put(hdu, fits, name, &flux)?;
        }
        put(hdu, fits, "FREQOFF", &vec![0.0f64; width * count])?;
        put(hdu, fits, "BANDWIDTH", &vec![bandwidth; count])?;
        put(hdu, fits, "EPOCH", &vec![epoch; count])?;
        put(hdu, fits, "RESTFREQ", &self.frequencies)?;
        put(hdu, fits, "LSRVEL", &self.velocities)?;
        for (columns, pairs) in [
            (["RAEPO", "DECEPO"], self.directions),
            (["RAAPP", "DECAPP"], self.apparent),
            (["PMRA", "PMDEC"], self.motion),
        ] {
            for (index, name) in columns.into_iter().enumerate() {
                put(
                    hdu,
                    fits,
                    name,
                    &pairs.iter().map(|pair| pair[index]).collect::<Vec<_>>(),
                )?;
            }
        }
        Ok(())
    }
}

fn proper_motion(table: &Table, row: usize) -> MsResult<[f64; 2]> {
    let column = "PROPER_MOTION";
    if !table.is_cell_defined(row, column)? {
        return Ok([0.0; 2]);
    }
    let motion = numeric_array(table, row, column)?;
    let mut values = [0.0; 2];
    let units = match table
        .column_keywords(column)
        .and_then(|record| record.get("QuantumUnits"))
    {
        Some(Value::Array(ArrayValue::String(units))) => units.as_slice(),
        _ => None,
    };
    let degrees_per_day = Unit::new("deg/d").map_err(invalid)?;
    for (axis, (output, input)) in values.iter_mut().zip(motion).enumerate() {
        let unit = units
            .and_then(|names| names.get(axis).or_else(|| names.first()))
            .map_or("rad/s", String::as_str);
        // CASA treats a value with unrecognized or incompatible units as deg/d.
        *output = Quantity::new(input, unit)
            .and_then(|value| value.get_value_in(&degrees_per_day))
            .unwrap_or(input);
    }
    Ok(values)
}

fn direction_degrees(table: &Table, row: usize) -> MsResult<[f64; 2]> {
    let ArrayValue::Float64(value) = table.cell_accessor(row, "PHASE_DIR")?.array()? else {
        return Err(invalid("Invalid phase direction type"));
    };
    if value.ndim() != 2 || value.shape()[0] != 2 || value.shape()[1] == 0 {
        return Err(invalid("Invalid phase direction shape"));
    }
    Ok([value[[0, 0]].to_degrees(), value[[1, 0]].to_degrees()])
}

fn apparent_directions(table: &Table, row: usize, meta: &ExportHeader) -> MsResult<[f64; 2]> {
    let angles = direction_degrees(table, row)?.map(f64::to_radians);
    let reference = measure_ref(table, "PHASE_DIR")
        .parse::<DirectionRef>()
        .unwrap_or(DirectionRef::J2000);
    let direction = MDirection::from_angles(angles[0], angles[1], reference);
    let time = f64_cell(table, row, "TIME")?;
    let reference = measure_ref(table, "TIME")
        .parse::<EpochRef>()
        .unwrap_or(meta.epoch_reference);
    let frame = meta.frame(time, reference)?;
    let direction = direction
        .convert_to(DirectionRef::APP, &frame)
        .map_err(invalid)?;
    let (ra, dec) = direction.as_angles();
    Ok([ra.sin().atan2(ra.cos()).to_degrees(), dec.to_degrees()])
}

fn optional_array_first(table: &Table, row: usize, column: &str) -> f64 {
    if has_column(table, column) {
        numeric_array(table, row, column)
            .ok()
            .and_then(|values| values.first().copied())
            .unwrap_or_default()
    } else {
        0.0
    }
}

fn all_equal(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] == pair[1])
}

fn fingerprints(table: &Table, column: &str) -> MsResult<Vec<String>> {
    (0..table.row_count())
        .map(|row| string_cell(table, row, column).map(str::to_owned))
        .collect()
}

fn numeric_array(table: &Table, row: usize, column: &str) -> MsResult<Vec<f64>> {
    match table.cell_accessor(row, column)?.array()? {
        ArrayValue::Float64(value) => Ok(value.iter().copied().collect()),
        _ => Err(invalid(format!(
            "Invalid floating point array column {column}"
        ))),
    }
}

fn string_cell<'a>(table: &'a Table, row: usize, column: &str) -> MsResult<&'a str> {
    match table.cell_accessor(row, column)?.scalar()? {
        ScalarValue::String(value) => Ok(value),
        _ => Err(invalid(format!("Invalid string column {column}"))),
    }
}

pub(super) fn measure_ref<'a>(table: &'a Table, column: &str) -> &'a str {
    match table
        .column_keywords(column)
        .and_then(|keywords| keywords.get("MEASINFO"))
    {
        Some(Value::Record(info)) => match info.get("Ref") {
            Some(Value::Scalar(ScalarValue::String(reference))) => reference,
            _ => "",
        },
        _ => "",
    }
}

pub(super) fn create_table(
    fits: &mut FitsFile,
    name: &str,
    columns: &[(&str, Type, usize, &str)],
) -> MsResult<FitsHdu> {
    let descriptors = columns
        .iter()
        .map(|&(name, kind, count, _)| {
            ColumnDescription::new(name)
                .with_type(kind)
                .that_repeats(count)
                .create()
                .map_err(invalid)
        })
        .collect::<MsResult<Vec<_>>>()?;
    let hdu = fits.create_table(name, &descriptors).map_err(invalid)?;
    for (index, &(_, _, _, units)) in columns.iter().enumerate() {
        if !units.is_empty() {
            hdu.write_key(fits, &format!("TUNIT{}", index + 1), units)
                .map_err(invalid)?;
        }
    }
    Ok(hdu)
}

pub(super) fn put<T: WritesCol>(
    hdu: &FitsHdu,
    fits: &mut FitsFile,
    column: &str,
    values: &[T],
) -> MsResult<()> {
    hdu.write_col(fits, column, values)
        .map(|_| ())
        .map_err(invalid)
}
