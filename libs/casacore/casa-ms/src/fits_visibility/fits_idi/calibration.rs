// SPDX-License-Identifier: LGPL-3.0-or-later
//! FITS-IDI calibration and Earth-orientation extension tables.

use casa_core::{Complex32, PrimitiveType, RecordValue, Value};
use casa_measures::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::{ColumnSchema, Table, TableSchema};
use fitsio::FitsFile;
use fitsio::hdu::FitsHdu;

use super::super::invalid;
use super::super::uvfits_ms::column_keyword;
use super::metadata::{Geometry, IdiTable};
use super::rows::TimeRange;
use crate::MsResult;
use crate::subtable_rows::{complex_array, f, f32_array, f64_array, i, s, string_array};

const DAY: f64 = 86_400.0;

fn column(name: &str, ty: PrimitiveType) -> ColumnSchema {
    ColumnSchema::scalar(name, ty)
}

fn array(name: &str, ty: PrimitiveType, ndim: usize) -> ColumnSchema {
    ColumnSchema::array_variable(name, ty, Some(ndim))
}

fn standard_calibration_columns() -> Vec<ColumnSchema> {
    use PrimitiveType::{Float64, Int32};
    vec![
        column("ANTENNA_ID", Int32),
        column("FEED_ID", Int32),
        column("SPECTRAL_WINDOW_ID", Int32),
        column("TIME", Float64),
        column("INTERVAL", Float64),
    ]
}

fn new_table(columns: Vec<ColumnSchema>, units: &[(&str, &str)]) -> MsResult<Table> {
    let mut table = Table::with_schema(TableSchema::new(columns)?);
    TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "UTC").write(&mut table)?;
    for &(name, unit) in units {
        column_keyword(
            &mut table,
            name,
            "QuantumUnits",
            string_array(&[unit], vec![1]),
        );
    }
    Ok(table)
}

fn insert(
    table: &mut Table,
    values: impl IntoIterator<Item = (&'static str, Value)>,
) -> MsResult<()> {
    let mut row = RecordValue::default();
    for (name, value) in values {
        row.upsert(name, value);
    }
    table.add_row(row)?;
    Ok(())
}

pub(super) fn earth_orientation(
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    geometry: &Geometry,
) -> MsResult<Table> {
    use PrimitiveType::{Float64, Int32, String as Text};
    let mut table = new_table(
        vec![
            column("TIME", Float64),
            column("OBSERVATION_ID", Int32),
            column("UT1_UTC", Float64),
            array("PM", Float64, 1),
            column("TYPE", Text),
        ],
        &[("TIME", "s"), ("UT1_UTC", "s"), ("PM", "rad")],
    )?;
    let mut fits = IdiTable::new(fits, hdu)?;
    let time = fits.column::<f64>("TIME", 1)?;
    let utc = fits.column::<f64>("UT1-UTC", 1)?;
    let motion = fits.column::<f64>("WOBXY", 2)?;
    let ut1_types = fits.column::<String>("UT1 TYPE", 1)?;
    let motion_types = fits.column::<String>("WOB TYPE", 1)?;
    for row in 0..fits.rows {
        let types = [ut1_types[row].trim_end(), motion_types[row].trim_end()];
        let kind = [("E", "PREDICTED"), ("P", "PRELIMINARY"), ("F", "FINAL")]
            .into_iter()
            .find_map(|(code, name)| types.contains(&code).then_some(name))
            .unwrap_or_default();
        let pm = [motion[row * 2], motion[row * 2 + 1]].map(|value| (value / 3600.0).to_radians());
        insert(
            &mut table,
            [
                ("TIME", f(time[row] * DAY + geometry.reference_time)),
                ("OBSERVATION_ID", i(0)),
                ("UT1_UTC", f(utc[row])),
                ("PM", f64_array(&pm, vec![2])),
                ("TYPE", s(kind)),
            ],
        )?;
    }
    Ok(table)
}

struct ToneData {
    frequency: Vec<f64>,
    real: Vec<f32>,
    imaginary: Vec<f32>,
}

impl ToneData {
    fn read(table: &mut IdiTable<'_>, hand: u8, count: usize) -> MsResult<Self> {
        Ok(Self {
            frequency: table.column_or_replicate(&format!("PC_FREQ_{hand}"), count)?,
            real: table.column_or_replicate(&format!("PC_REAL_{hand}"), count)?,
            imaginary: table.column_or_replicate(&format!("PC_IMAG_{hand}"), count)?,
        })
    }
}

pub(super) fn phase_calibration(
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    geometry: &Geometry,
) -> MsResult<Table> {
    use PrimitiveType::{Complex32 as Complex, Float64, Int32};
    let mut columns = standard_calibration_columns();
    columns.extend([
        column("NUM_TONES", Int32),
        array("TONE_FREQUENCY", Float64, 2),
        array("PHASE_CAL", Complex, 2),
        column("CABLE_CAL", Float64),
    ]);
    let units = [
        ("TIME", "s"),
        ("INTERVAL", "s"),
        ("TONE_FREQUENCY", "Hz"),
        ("CABLE_CAL", "s"),
    ];
    let mut table = new_table(columns, &units)?;
    let mut fits = IdiTable::new(fits, hdu)?;
    let bands = fits.count("NO_BAND")?;
    let tones = fits.count("NO_TONES")?;
    let width = bands
        .checked_mul(tones)
        .ok_or_else(|| invalid("FITS-IDI tone dimensions overflow"))?;
    let antenna = super::auxiliary::antenna_ids(&mut fits, geometry)?;
    let time = fits.column::<f64>("TIME", 1)?;
    let interval = fits.column::<f32>("TIME_INTERVAL", 1)?;
    let cable = fits.column::<f64>("CABLE_CAL", 1)?;
    let mut hands = vec![ToneData::read(&mut fits, 1, width)?];
    if fits.has("PC_FREQ_2") {
        hands.push(ToneData::read(&mut fits, 2, width)?);
    }
    let shape = vec![hands.len(), tones];
    for (row, antenna) in antenna.into_iter().enumerate() {
        for band in 0..bands {
            let start = row * width + band * tones;
            let mut frequencies = Vec::with_capacity(tones * hands.len());
            let mut phases = Vec::with_capacity(tones * hands.len());
            for tone in start..start + tones {
                for hand in &hands {
                    frequencies.push(hand.frequency[tone]);
                    phases.push(Complex32::new(hand.real[tone], hand.imaginary[tone]));
                }
            }
            insert(
                &mut table,
                [
                    ("ANTENNA_ID", i(antenna)),
                    ("FEED_ID", i(-1)),
                    ("SPECTRAL_WINDOW_ID", i(band as i32)),
                    ("TIME", f(time[row] * DAY + geometry.reference_time)),
                    ("INTERVAL", f(f64::from(interval[row]) * DAY)),
                    ("NUM_TONES", i(tones as i32)),
                    ("TONE_FREQUENCY", f64_array(&frequencies, shape.clone())),
                    ("PHASE_CAL", complex_array(&phases, shape.clone())),
                    ("CABLE_CAL", f(cable[row])),
                ],
            )?;
        }
    }
    Ok(table)
}

struct GainData {
    kind: Vec<i32>,
    terms: Vec<i32>,
    axis_x: Vec<i32>,
    axis_y: Vec<i32>,
    sensitivity: Vec<f32>,
    gain: Vec<f32>,
}

impl GainData {
    fn read(
        fits: &mut IdiTable<'_>,
        hand: u8,
        bands: usize,
        coefficients: usize,
    ) -> MsResult<Self> {
        Ok(Self {
            kind: fits.column_or_replicate(&format!("TYPE_{hand}"), bands)?,
            terms: fits.column_or_replicate(&format!("NTERM_{hand}"), bands)?,
            axis_x: fits.column_or_replicate(&format!("X_TYP_{hand}"), bands)?,
            axis_y: fits.column_or_replicate(&format!("Y_TYP_{hand}"), bands)?,
            sensitivity: fits.column_or_replicate(&format!("SENS_{hand}"), bands)?,
            gain: fits.column_or_replicate(&format!("GAIN_{hand}"), coefficients)?,
        })
    }

    fn spec(&self, index: usize) -> [i32; 4] {
        [
            self.kind[index],
            self.terms[index],
            self.axis_x[index],
            self.axis_y[index],
        ]
    }
}

pub(super) fn gain_curve(
    fits: &mut FitsFile,
    hdu: &FitsHdu,
    geometry: &Geometry,
    times: &TimeRange,
) -> MsResult<Table> {
    use PrimitiveType::{Float32, Int32, String as Text};
    let mut columns = standard_calibration_columns();
    columns.extend([
        column("TYPE", Text),
        column("NUM_POLY", Int32),
        array("GAIN", Float32, 2),
        array("SENSITIVITY", Float32, 1),
    ]);
    let mut table = new_table(
        columns,
        &[("TIME", "s"), ("INTERVAL", "s"), ("SENSITIVITY", "K/Jy")],
    )?;
    let mut fits = IdiTable::new(fits, hdu)?;
    let bands = fits.count("NO_BAND")?;
    let tabs = fits.count("NO_TABS")?;
    let width = bands
        .checked_mul(tabs)
        .ok_or_else(|| invalid("FITS-IDI gain dimensions overflow"))?;
    let antennas = super::auxiliary::antenna_ids(&mut fits, geometry)?;
    let mut hands = vec![GainData::read(&mut fits, 1, bands, width)?];
    if fits.has("TYPE_2") {
        hands.push(GainData::read(&mut fits, 2, bands, width)?);
    }
    let start = times.first.unwrap_or_default();
    for (row, antenna) in antennas.into_iter().enumerate() {
        for band in 0..bands {
            let index = row * bands + band;
            let [kind, terms, _, axis] = hands[0].spec(index);
            let selector = match axis {
                1 => "POWER(EL)",
                2 => "POWER(ZA)",
                _ => continue,
            };
            if kind != 2
                || hands
                    .iter()
                    .any(|hand| hand.spec(index) != hands[0].spec(index))
            {
                continue;
            }
            let count = usize::try_from(terms)
                .ok()
                .filter(|&count| count <= tabs)
                .ok_or_else(|| invalid("Invalid FITS-IDI gain polynomial size"))?;
            let offset = row * width + band * tabs;
            let coefficients = (offset..offset + count)
                .flat_map(|term| hands.iter().map(move |hand| hand.gain[term]))
                .collect::<Vec<_>>();
            let sensitivity = hands
                .iter()
                .map(|hand| hand.sensitivity[index])
                .collect::<Vec<_>>();
            insert(
                &mut table,
                [
                    ("ANTENNA_ID", i(antenna)),
                    ("FEED_ID", i(-1)),
                    ("SPECTRAL_WINDOW_ID", i(band as i32)),
                    ("TIME", f((start + times.last_end) / 2.0)),
                    ("INTERVAL", f(times.last_end - start)),
                    ("TYPE", s(selector)),
                    ("NUM_POLY", i(terms)),
                    ("GAIN", f32_array(&coefficients, vec![hands.len(), count])),
                    ("SENSITIVITY", f32_array(&sensitivity, vec![hands.len()])),
                ],
            )?;
        }
    }
    Ok(table)
}
