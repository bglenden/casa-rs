// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read legacy calibration solutions into transient rows for the PlotMS engine.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_core::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use casa_numerics::{legacy_bpoly_chebyshev_value, legacy_cubic_spline};
use casa_tables::{ColumnSchema, Table, TableInfo, TableSchema};
use ndarray::Array2;

use super::axis::{Axis, SampleCoordinates};
use super::selection::Expressions;
use super::table_data::{array, integer, number, subtable, text};
use casa_ms::selection::syntax::parse_numeric_id_selector;
use casa_ms::{MeasurementSet, SubtableId, subtable_path_from_main};

#[derive(Clone, Copy)]
pub(super) struct SplineDisplay {
    field: i32,
    window: i32,
    scan: i32,
    interval: f64,
}

impl SplineDisplay {
    pub(super) fn coordinates(self, point: &mut SampleCoordinates) {
        point.field = self.field;
        point.window = self.window;
        point.scan = self.scan;
        point.interval = self.interval;
    }
}

pub(super) fn materialize(
    source: Table,
    path: &Path,
    selection: &Expressions<'_>,
    averaged: bool,
    polarization: &str,
) -> Result<(Table, Option<SplineDisplay>), String> {
    let subtype = source.info().sub_type.as_str();
    if !matches!(subtype, "BPOLY" | "GSPLINE") {
        return Ok((source, None));
    }
    if averaged {
        return Err(format!(
            "Averaging not supported for cal table type {subtype}"
        ));
    }
    if subtype == "BPOLY" && polarization == "/" {
        return Err("Polarization ratio plots not supported for BPOLY tables.".into());
    }
    let input = LegacySource::open(&source, path)?;
    let mut virtual_table = input.virtual_table(subtype)?;
    let spline = if subtype == "BPOLY" {
        input.bandpass(&source, &mut virtual_table)?;
        None
    } else {
        Some(input.spline(&source, selection, &mut virtual_table)?)
    };
    Ok((virtual_table, spline))
}

pub(super) fn check_spline_axes(axes: impl IntoIterator<Item = Axis>) -> Result<(), String> {
    use Axis::*;
    for axis in axes {
        if !matches!(
            axis,
            Scan | Field
                | Time
                | Window
                | Interval
                | Receptor
                | Antenna1
                | Feed1
                | Flag
                | Amplitude
                | GainAmplitude
                | Phase
                | GainPhase
                | Real
                | GainReal
                | Imaginary
                | GainImaginary
                | Observation
        ) {
            return Err(format!(
                "GSPLINE plotting does not support {} axis",
                axis.label()
            ));
        }
    }
    Ok(())
}

struct LegacySource {
    ms: MeasurementSet,
    path: PathBuf,
    windows: Vec<Vec<i32>>,
}

impl LegacySource {
    fn open(table: &Table, path: &Path) -> Result<Self, String> {
        let description = subtable(table, path, "CAL_DESC")?;
        let name = text(&description, 0, "MS_NAME")
            .filter(|name| !name.is_empty())
            .ok_or("Associated MS is not available, cannot plot solutions.")?;
        let path = path.parent().unwrap_or_else(|| Path::new(".")).join(name);
        let path = std::path::absolute(path).map_err(display)?;
        let ms = MeasurementSet::open(&path)
            .map_err(|_| "Associated MS is not available, cannot plot solutions.")?;
        let windows = (0..description.row_count())
            .map(
                |row| match array(&description, row, "SPECTRAL_WINDOW_ID")? {
                    ArrayValue::Int32(values) if !values.is_empty() => {
                        Ok(values.iter().copied().collect())
                    }
                    ArrayValue::Int32(_) => Err("CAL_DESC has no spectral window".into()),
                    _ => Err("CAL_DESC spectral-window IDs must be integers".into()),
                },
            )
            .collect::<Result<_, String>>()?;
        Ok(Self { ms, path, windows })
    }

    fn virtual_table(&self, subtype: &str) -> Result<Table, String> {
        let columns = ["TIME", "INTERVAL"]
            .into_iter()
            .map(|name| ColumnSchema::scalar(name, PrimitiveType::Float64))
            .chain(
                [
                    "SCAN_NUMBER",
                    "FIELD_ID",
                    "SPECTRAL_WINDOW_ID",
                    "ANTENNA1",
                    "ANTENNA2",
                    "FEED1",
                    "OBSERVATION_ID",
                ]
                .into_iter()
                .map(|name| ColumnSchema::scalar(name, PrimitiveType::Int32)),
            )
            .chain(
                [
                    ("CPARAM", PrimitiveType::Complex32),
                    ("FLAG", PrimitiveType::Bool),
                    ("SNR", PrimitiveType::Float32),
                ]
                .into_iter()
                .map(|(name, kind)| ColumnSchema::array_variable(name, kind, Some(2))),
            )
            .collect();
        let mut result = Table::with_schema(TableSchema::new(columns).map_err(display)?);
        result.set_info(TableInfo {
            table_type: "Calibration".into(),
            sub_type: subtype.into(),
            ..TableInfo::default()
        });
        for id in [
            SubtableId::Antenna,
            SubtableId::Field,
            SubtableId::SpectralWindow,
            SubtableId::Observation,
        ] {
            let path = subtable_path_from_main(self.ms.main_table(), &self.path, id)
                .ok_or_else(|| format!("associated MS has no {} table", id.name()))?;
            result.keywords_mut().push(RecordField::new(
                id.name(),
                Value::TableRef(path.to_string_lossy().into_owned()),
            ));
        }
        result.keywords_mut().push(RecordField::new(
            "MSName",
            Value::Scalar(ScalarValue::String(
                self.path.to_string_lossy().into_owned(),
            )),
        ));
        Ok(result)
    }

    fn bandpass(&self, source: &Table, output: &mut Table) -> Result<(), String> {
        let spectral_window = self.ms.spectral_window().map_err(display)?;
        let frequencies = (0..spectral_window.row_count())
            .map(|row| spectral_window.chan_freq(row).map_err(display))
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..source.row_count() {
            let description = integer(source, row, "CAL_DESC_ID")
                .and_then(|id| usize::try_from(id).ok())
                .and_then(|id| self.windows.get(id))
                .map(|ids| ids[0])
                .ok_or("invalid legacy CAL_DESC_ID")?;
            let frequencies = usize::try_from(description)
                .ok()
                .and_then(|id| frequencies.get(id))
                .ok_or("legacy spectral window is missing from the associated MS")?;
            let domain: [f64; 2] = array_values(source, row, "VALID_DOMAIN")?
                .try_into()
                .map_err(|_| "BPOLY needs two domain frequencies")?;
            if !domain[0].is_finite() || !domain[1].is_finite() || domain[0] > domain[1] {
                return Err("invalid BPOLY frequency domain".into());
            }
            let amp = coefficients(source, row, "N_POLY_AMP", "POLY_COEFF_AMP")?;
            let phase = coefficients(source, row, "N_POLY_PHASE", "POLY_COEFF_PHASE")?;
            let scale = gain_scale(source, row)?;
            let active = [0, 1].map(|pol| {
                amp[pol]
                    .iter()
                    .chain(&phase[pol])
                    .any(|&value| value != 0.0)
            });
            let values = Array2::from_shape_fn((2, frequencies.len()), |(pol, channel)| {
                let frequency = frequencies[channel];
                if !active[pol] || frequency < domain[0] || frequency > domain[1] {
                    return Complex32::new(1.0, 0.0);
                }
                let amplitude =
                    legacy_bpoly_chebyshev_value(&amp[pol], domain[0], domain[1], frequency).exp();
                let angle =
                    legacy_bpoly_chebyshev_value(&phase[pol], domain[0], domain[1], frequency);
                scale * Complex32::from_polar(amplitude as f32, angle as f32)
            });
            let flags = Array2::from_shape_fn(values.dim(), |(pol, _)| !active[pol]);
            let meta = RowMetadata::read(source, row, description)?;
            write_row(
                output,
                RowMetadata {
                    observation: meta.observation.max(0),
                    ..meta
                },
                values,
                flags,
            )?;
        }
        Ok(())
    }

    fn selected_spline_rows(
        &self,
        source: &Table,
        selection: &Expressions<'_>,
    ) -> Result<Vec<(usize, RowMetadata)>, String> {
        if !selection.timerange.is_empty() {
            tracing::warn!("Time selection not supported for GSPLINE calibration tables");
        }
        let fields = self
            .ms
            .resolve_field_selector(selection.field)
            .map_err(display)?;
        let mut antennas = BTreeSet::new();
        let mut excluded = BTreeSet::new();
        for clause in selection.antenna.split(';').map(str::trim) {
            let (set, expr) = clause
                .strip_prefix('!')
                .map_or_else(|| (&mut antennas, clause), |expr| (&mut excluded, expr));
            set.extend(self.ms.resolve_reference_antennas(expr).map_err(display)?);
        }
        let windows = self
            .ms
            .resolve_spw_selector(selection.spw)
            .map_err(display)?;
        let scans = parse_numeric_id_selector(selection.scan, "scan").map_err(display)?;
        let observations =
            parse_numeric_id_selector(selection.observation, "observation").map_err(display)?;
        let raw_rows = if selection.taql.is_empty() {
            (0..source.row_count()).collect::<Vec<_>>()
        } else {
            source
                .query(&format!("SELECT * WHERE ({})", selection.taql))
                .map_err(display)?
                .row_numbers()
                .to_vec()
        };
        let mut matched = Vec::new();
        for row in raw_rows {
            let index = integer(source, row, "CAL_DESC_ID")
                .and_then(|id| usize::try_from(id).ok())
                .ok_or("invalid GSPLINE CAL_DESC_ID")?;
            let description = self
                .windows
                .get(index)
                .ok_or("missing GSPLINE CAL_DESC row")?;
            if !windows.selects_all()
                && !description
                    .iter()
                    .any(|&id| usize::try_from(id).is_ok_and(|id| windows.spw_ids().contains(&id)))
            {
                continue;
            }
            let metadata = RowMetadata::read(source, row, description[0])?;
            let antenna = metadata.antenna1 as usize;
            if (fields.is_empty() || fields.contains(&metadata.field))
                && (scans.is_empty() || scans.contains(&metadata.scan))
                && (observations.is_empty() || observations.contains(&metadata.observation))
                && (antennas.is_empty() || antennas.contains(&antenna))
                && !excluded.contains(&antenna)
            {
                matched.push((row, metadata));
            }
        }
        if matched.is_empty() {
            return Err("calibration selection contains no rows".into());
        }
        Ok(matched)
    }

    fn spline(
        &self,
        source: &Table,
        selection: &Expressions<'_>,
        output: &mut Table,
    ) -> Result<SplineDisplay, String> {
        let matched = self.selected_spline_rows(source, selection)?;
        let selected_fields: BTreeSet<_> =
            matched.iter().map(|(_, metadata)| metadata.field).collect();
        let field = *selected_fields.first().expect("matched rows");
        let window = matched[0].1.window;
        let display_window = common_value(self.windows.iter().flatten().copied(), -1);
        let scan = common_value(matched.iter().map(|(_, meta)| meta.scan), -1);
        let interval = common_value(matched.iter().map(|(_, meta)| meta.interval), -1.0);
        let mut signals = BTreeMap::new();
        for (row, meta) in matched
            .into_iter()
            .filter(|(_, metadata)| metadata.field == field)
        {
            if let std::collections::btree_map::Entry::Vacant(slot) = signals.entry(meta.antenna1) {
                slot.insert((meta, Spline::read(source, row)?));
            }
        }
        let first = &signals
            .first_key_value()
            .ok_or("GSPLINE has no solutions")?
            .1
            .1;
        let (start, stop) = first.range()?;
        let reference_frequency = first.reference_frequency;
        let step = (stop - start) / 1000.0;
        for index in 0..1000 {
            let time = start + 0.5 * step + f64::from(index) * step;
            for (meta, spline) in signals.values() {
                let [first, second] = spline.evaluate(time, reference_frequency)?;
                let values =
                    Array2::from_shape_vec((2, 1), vec![first, second]).expect("two receptors");
                let flags = Array2::from_elem((2, 1), false);
                write_row(
                    output,
                    RowMetadata {
                        time,
                        window: window.max(0),
                        ..*meta
                    },
                    values,
                    flags,
                )?;
            }
        }
        Ok(SplineDisplay {
            field: if selected_fields.len() == 1 {
                field
            } else {
                -1
            },
            window: display_window,
            scan,
            interval,
        })
    }
}

#[derive(Clone, Copy)]
struct RowMetadata {
    time: f64,
    interval: f64,
    scan: i32,
    field: i32,
    window: i32,
    antenna1: i32,
    antenna2: i32,
    feed1: i32,
    observation: i32,
}

impl RowMetadata {
    fn read(source: &Table, row: usize, window: i32) -> Result<Self, String> {
        let antenna = integer(source, row, "ANTENNA1")
            .filter(|&id| id >= 0)
            .ok_or("legacy calibration row has no valid antenna")?;
        let reference = array(source, row, "REF_ANT")
            .ok()
            .and_then(|values| match values {
                ArrayValue::Int32(values) => values.iter().copied().next(),
                _ => None,
            })
            .unwrap_or(-1);
        Ok(Self {
            time: number(source, row, "TIME").ok_or("legacy calibration row has no TIME")?,
            interval: number(source, row, "INTERVAL").unwrap_or_default(),
            scan: integer(source, row, "SCAN_NUMBER").unwrap_or(-1),
            field: integer(source, row, "FIELD_ID").unwrap_or(-1),
            window,
            antenna1: antenna,
            antenna2: reference,
            feed1: integer(source, row, "FEED1").unwrap_or(-1),
            observation: integer(source, row, "OBSERVATION_ID").unwrap_or(-1),
        })
    }
}

fn write_row(
    output: &mut Table,
    row: RowMetadata,
    values: Array2<Complex32>,
    flags: Array2<bool>,
) -> Result<(), String> {
    let scalar = |name, value| RecordField::new(name, Value::Scalar(value));
    let array = |name, value| RecordField::new(name, Value::Array(value));
    let snr = Array2::from_elem(values.dim(), 1.0_f32);
    output
        .add_row(RecordValue::new(vec![
            scalar("TIME", ScalarValue::Float64(row.time)),
            scalar("INTERVAL", ScalarValue::Float64(row.interval)),
            scalar("SCAN_NUMBER", ScalarValue::Int32(row.scan)),
            scalar("FIELD_ID", ScalarValue::Int32(row.field)),
            scalar("SPECTRAL_WINDOW_ID", ScalarValue::Int32(row.window)),
            scalar("ANTENNA1", ScalarValue::Int32(row.antenna1)),
            scalar("ANTENNA2", ScalarValue::Int32(row.antenna2)),
            scalar("FEED1", ScalarValue::Int32(row.feed1)),
            scalar("OBSERVATION_ID", ScalarValue::Int32(row.observation)),
            array("CPARAM", ArrayValue::Complex32(values.into_dyn())),
            array("FLAG", ArrayValue::Bool(flags.into_dyn())),
            array("SNR", ArrayValue::Float32(snr.into_dyn())),
        ]))
        .map_err(display)
}

fn common_value<T: Copy + PartialEq>(mut values: impl Iterator<Item = T>, ambiguous: T) -> T {
    let Some(first) = values.next() else {
        return ambiguous;
    };
    if values.all(|value| value == first) {
        first
    } else {
        ambiguous
    }
}

struct Spline {
    amplitude: Option<SplineComponent>,
    phase: Option<SplineComponent>,
    scale: Complex32,
    reference_frequency: f64,
}

impl Spline {
    fn read(table: &Table, row: usize) -> Result<Self, String> {
        let mode = text(table, row, "POLY_MODE").ok_or("GSPLINE row has no POLY_MODE")?;
        let amplitude = (mode.contains("AMP") || mode.contains("A&P"))
            .then(|| SplineComponent::read(table, row, "AMP"))
            .transpose()?;
        let phase = (mode.contains("PHAS") || mode.contains("A&P"))
            .then(|| SplineComponent::read(table, row, "PHASE"))
            .transpose()?;
        let reference_frequency = array_values(table, row, "REF_FREQUENCY")?
            .first()
            .copied()
            .ok_or("GSPLINE row has no reference frequency")?;
        Ok(Self {
            amplitude,
            phase,
            scale: gain_scale(table, row)?,
            reference_frequency,
        })
    }

    fn range(&self) -> Result<(f64, f64), String> {
        let knots = &self
            .amplitude
            .as_ref()
            .or(self.phase.as_ref())
            .ok_or("GSPLINE has no active coefficients")?
            .knots;
        Ok((knots[0], *knots.last().expect("valid knots")))
    }

    fn evaluate(&self, time: f64, frequency: f64) -> Result<[Complex32; 2], String> {
        let ratio = if self.reference_frequency == 0.0 {
            1.0
        } else {
            frequency / self.reference_frequency
        };
        let value = |pol| -> Result<Complex32, String> {
            let amp = self
                .amplitude
                .as_ref()
                .map_or(Ok(0.0), |part| part.value(time, pol))?;
            let phase = self
                .phase
                .as_ref()
                .map_or(Ok(0.0), |part| part.value(time, pol))?;
            Ok(self.scale * Complex32::from_polar(amp.exp() as f32, (-phase * ratio) as f32))
        };
        Ok([value(0)?, value(1)?])
    }
}

struct SplineComponent {
    knots: Vec<f64>,
    coefficients: [Vec<f64>; 2],
}

impl SplineComponent {
    fn read(table: &Table, row: usize, component: &str) -> Result<Self, String> {
        let count = integer(table, row, &format!("N_KNOTS_{component}"))
            .and_then(|count| usize::try_from(count).ok())
            .ok_or("invalid GSPLINE knot count")?;
        let knots = array_values(table, row, &format!("SPLINE_KNOTS_{component}"))?;
        let knots = knots
            .get(..count)
            .ok_or("GSPLINE knot count exceeds its array length")?
            .to_vec();
        if knots.len() < 8
            || knots.iter().any(|value| !value.is_finite())
            || knots.windows(2).any(|pair| pair[0] > pair[1])
            || knots[0] >= knots[knots.len() - 1]
        {
            return Err("invalid GSPLINE knot vector".into());
        }
        let coefficients = coefficients(
            table,
            row,
            &format!("N_POLY_{component}"),
            &format!("POLY_COEFF_{component}"),
        )?;
        Ok(Self {
            knots,
            coefficients,
        })
    }

    fn value(&self, time: f64, pol: usize) -> Result<f64, String> {
        legacy_cubic_spline(&self.knots, &self.coefficients[pol], time)
            .ok_or_else(|| "cannot evaluate legacy cubic spline".into())
    }
}

fn coefficients(
    table: &Table,
    row: usize,
    count: &str,
    name: &str,
) -> Result<[Vec<f64>; 2], String> {
    let count = integer(table, row, count)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| format!("invalid legacy coefficient count for {name}"))?;
    if count == 0 {
        return Ok([Vec::new(), Vec::new()]);
    }
    let values = array_values(table, row, name)?;
    let first = values
        .get(..count)
        .ok_or_else(|| format!("truncated legacy coefficients in {name}"))?;
    let second = values
        .get(count..2 * count)
        .ok_or_else(|| format!("missing second receptor coefficients in {name}"))?;
    Ok([first.to_vec(), second.to_vec()])
}

fn array_values(table: &Table, row: usize, name: &str) -> Result<Vec<f64>, String> {
    match array(table, row, name)? {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        ArrayValue::Float32(values) => Ok(values.iter().copied().map(f64::from).collect()),
        _ => Err(format!(
            "legacy calibration column {name} is not a floating array"
        )),
    }
}

fn gain_scale(table: &Table, row: usize) -> Result<Complex32, String> {
    match table
        .cell_accessor(row, "SCALE_FACTOR")
        .and_then(|cell| cell.scalar())
        .map_err(display)?
    {
        ScalarValue::Complex32(value) => Ok(*value),
        ScalarValue::Complex64(value) => Ok(Complex32::new(value.re as f32, value.im as f32)),
        _ => Err("legacy calibration SCALE_FACTOR is not complex".into()),
    }
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
