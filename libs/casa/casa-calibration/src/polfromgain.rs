// SPDX-License-Identifier: LGPL-3.0-or-later
//! Estimate fractional linear polarization from receptor gain amplitude ratios.

use std::collections::{BTreeMap, BTreeSet};
use std::f64::consts::{PI, TAU};
use std::path::Path;

use casa_core::{
    ArrayValue, Complex32, Value,
    measures::{EpochRef, MEpoch},
};
use casa_ms::{MeasurementSet, derived::engine::MsCalEngine, subtables::MsField};
use casa_tables::{RequiredScalarColumnValues, Table, TableOptions};

use crate::output::CalibrationOutputMode;
use crate::paths::{overlap, subtable_owner};
use crate::solution_edit::{error_text, stage_calibration_table_edit_with_mode};

type Stokes = [f64; 4];
pub type FractionalPolarization = BTreeMap<String, BTreeMap<String, Stokes>>;

pub fn polarization_from_gain(
    ms_path: &Path,
    input: &Path,
    output: Option<&Path>,
    angle_offset_degrees: f64,
    coverage_degrees: f64,
) -> Result<FractionalPolarization, String> {
    let ms = MeasurementSet::open(ms_path).map_err(error_text)?;
    let geometry = FeedGeometry::new(&ms, angle_offset_degrees * PI / 180.0)?;
    let coverage = coverage_degrees * PI / 180.0;
    let mut result = BTreeMap::new();
    if let Some(output) = output {
        if overlap(ms_path, output).map_err(error_text)? {
            return Err("the output calibration table overlaps the MeasurementSet".into());
        }
        stage_calibration_table_edit_with_mode(
            input,
            output,
            CalibrationOutputMode::Create,
            |table, stage| {
                result = estimate(table, stage, &geometry, coverage, true)?;
                table
                    .save_metadata_only(TableOptions::new(stage))
                    .map_err(error_text)
            },
        )?;
    } else {
        let mut table = Table::open(TableOptions::new(input)).map_err(error_text)?;
        result = estimate(&mut table, input, &geometry, coverage, false)?;
    }
    Ok(result)
}

struct FeedGeometry {
    measures: MsCalEngine,
    fields: Vec<String>,
    angles: Vec<Vec<f64>>,
    window_count: usize,
    offset: f64,
}

impl FeedGeometry {
    fn new(ms: &MeasurementSet, offset: f64) -> Result<Self, String> {
        let fields = ms.field().map_err(error_text)?;
        let fields = (0..fields.row_count())
            .map(|id| fields.name(id).map_err(error_text))
            .collect::<Result<Vec<_>, _>>()?;
        let count = ms.spectral_window().map_err(error_text)?.row_count();
        let measures = MsCalEngine::for_calibration(ms).map_err(error_text)?;
        let mut angles = vec![vec![0.0; count]; measures.num_antennas()];
        let feed = ms.feed().map_err(error_text)?;
        for row in 0..feed.row_count() {
            let antenna = feed.i32(row, "ANTENNA_ID").map_err(error_text)?;
            let window = feed.i32(row, "SPECTRAL_WINDOW_ID").map_err(error_text)?;
            // CASA indexes the feed array directly, making -1 the last slot.
            let window = if window == -1 {
                count.saturating_sub(1)
            } else {
                window as usize
            };
            let ArrayValue::Float64(values) =
                feed.array(row, "RECEPTOR_ANGLE").map_err(error_text)?
            else {
                return Err("FEED RECEPTOR_ANGLE must contain doubles".into());
            };
            let angle = values.get([0]).ok_or("FEED RECEPTOR_ANGLE is empty")?;
            let cell = angles
                .get_mut(antenna as usize)
                .and_then(|windows| windows.get_mut(window))
                .ok_or("FEED contains an unknown antenna or spectral window")?;
            *cell = *angle;
        }
        Ok(Self {
            measures,
            fields,
            angles,
            window_count: count,
            offset,
        })
    }

    fn angle(
        &self,
        time: f64,
        antenna: usize,
        window: usize,
        direction: [f64; 2],
    ) -> Result<f64, String> {
        let [right_ascension, declination] = direction;
        let frame = self
            .measures
            .antenna_frame(time, antenna)
            .map_err(error_text)?;
        let latitude = frame
            .position()
            .ok_or("antenna frame lacks a position")?
            .latitude_rad();
        let local_time = MEpoch::from_mjd_seconds(time, EpochRef::UTC)
            .convert_to(EpochRef::LAST, &frame)
            .map_err(error_text)?
            .value()
            .as_mjd();
        let hour_angle = local_time.rem_euclid(1.0) * TAU - right_ascension;
        let (sine, cosine) = latitude.sin_cos();
        let mut angle = (cosine * hour_angle.sin())
            .atan2(sine * declination.cos() - cosine * declination.sin() * hour_angle.cos());
        if latitude < declination && angle < 0.0 {
            angle += TAU;
        }
        Ok(angle + self.angles[antenna][window] + self.offset)
    }
}

type RowsByAntenna = BTreeMap<usize, Vec<(usize, f64)>>;

struct SolutionRows {
    fields: BTreeSet<usize>,
    windows: BTreeSet<usize>,
    groups: BTreeMap<(usize, usize), RowsByAntenna>,
}

impl SolutionRows {
    fn read(table: &Table, geometry: &FeedGeometry) -> Result<Self, String> {
        let mut columns = table
            .required_scalar_columns_owned(&["FIELD_ID", "SPECTRAL_WINDOW_ID", "ANTENNA1", "TIME"])
            .map_err(error_text)?;
        let Some(RequiredScalarColumnValues::Float64(times)) = columns.remove("TIME") else {
            return Err("invalid calibration TIME column".into());
        };
        let mut integer = |name| match columns.remove(name) {
            Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
            _ => Err(format!("invalid calibration {name} column")),
        };
        let fields = integer("FIELD_ID")?;
        let windows = integer("SPECTRAL_WINDOW_ID")?;
        let antennas = integer("ANTENNA1")?;
        let mut rows = Self {
            fields: BTreeSet::new(),
            windows: BTreeSet::new(),
            groups: BTreeMap::new(),
        };
        for (row, time) in times.into_iter().enumerate() {
            let (field, window, antenna) = (
                fields[row] as usize,
                windows[row] as usize,
                antennas[row] as usize,
            );
            if field >= geometry.fields.len()
                || window >= geometry.window_count
                || antenna >= geometry.angles.len()
            {
                return Err(
                    "calibration solution refers to an unknown field, window or antenna".into(),
                );
            }
            rows.fields.insert(field);
            rows.windows.insert(window);
            rows.groups
                .entry((field, window))
                .or_default()
                .entry(antenna)
                .or_default()
                .push((row, time));
        }
        Ok(rows)
    }
}

fn estimate(
    table: &mut Table,
    path: &Path,
    geometry: &FeedGeometry,
    coverage: f64,
    correct: bool,
) -> Result<FractionalPolarization, String> {
    let rows = SolutionRows::read(table, geometry)?;
    let owner = subtable_owner(table, path).map_err(error_text)?;
    let field_path = table
        .keywords()
        .get("FIELD")
        .and_then(Value::as_table_ref)
        .map_or_else(
            || owner.join("FIELD"),
            |link| casa_tables::add_directory(link, &owner),
        );
    let field_table = Table::open(TableOptions::new(field_path)).map_err(error_text)?;
    let fields = MsField::new(&field_table);
    let mut result = BTreeMap::new();
    let mut changed_rows = Vec::new();
    for &field in &rows.fields {
        let direction = delay_direction(&fields, field)?;
        let mut spectrum = BTreeMap::new();
        let mut good_windows = Vec::new();
        for &window in &rows.windows {
            let mut series = Vec::new();
            if let Some(antennas) = rows.groups.get(&(field, window)) {
                for (&antenna, times) in antennas {
                    let samples = times
                        .iter()
                        .map(|&(row, epoch)| {
                            let angle = geometry.angle(epoch, antenna, window, direction)?;
                            read_sample(table, row, angle)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let coefficients = fit_antenna(&samples, coverage)?;
                    series.push((samples, coefficients));
                }
            }
            let average = mean_polarization(series.iter().filter_map(|(_, fit)| *fit));
            if let Some([q, u]) = average {
                good_windows.push([q, u]);
                spectrum.insert(format!("Spw{window}"), [1.0, q, u, 0.0]);
            }
            if correct {
                for (samples, fit) in series {
                    if let Some(polarization) = fit.or(average) {
                        correct_series(
                            table,
                            &samples,
                            polarization,
                            fit.is_some(),
                            &mut changed_rows,
                        )?;
                    }
                }
            }
        }
        if let Some([q, u]) = mean_polarization(good_windows.into_iter()) {
            spectrum.insert("SpwAve".into(), [1.0, q, u, 0.0]);
        }
        result.insert(geometry.fields[field].clone(), spectrum);
    }
    if !changed_rows.is_empty() {
        changed_rows.sort_unstable();
        table
            .prepare_write()
            .save_selected_rows(&["CPARAM"], &changed_rows)
            .map_err(error_text)?;
    }
    Ok(result)
}

fn delay_direction(fields: &MsField<'_>, field: usize) -> Result<[f64; 2], String> {
    let ArrayValue::Float64(direction) = fields.delay_dir(field).map_err(error_text)? else {
        return Err("calibration FIELD DELAY_DIR must contain doubles".into());
    };
    let value = |axis| {
        direction
            .get([axis, 0])
            .copied()
            .ok_or_else(|| "missing calibration FIELD direction".to_string())
    };
    Ok([value(0)?, value(1)?])
}

struct GainSample {
    row: usize,
    angle: f64,
    sine: f64,
    cosine: f64,
    squared_amplitude_ratio: Option<f64>,
}

fn read_sample(table: &Table, row: usize, angle: f64) -> Result<GainSample, String> {
    let ArrayValue::Complex32(gains) = table
        .cell_accessor(row, "CPARAM")
        .and_then(|cell| cell.array())
        .map_err(error_text)?
    else {
        return Err("calibration CPARAM must contain complex gains".into());
    };
    let ArrayValue::Bool(flags) = table
        .cell_accessor(row, "FLAG")
        .and_then(|cell| cell.array())
        .map_err(error_text)?
    else {
        return Err("calibration FLAG must contain booleans".into());
    };
    if flags.shape() != gains.shape()
        || gains.ndim() != 2
        || gains.shape()[0] < 2
        || gains.shape()[1] == 0
    {
        return Err("polfromgain requires two gain receptors with matching flags".into());
    }
    let unflagged = !flags[[0, 0]] && !flags[[1, 0]];
    let squared_amplitude_ratio = unflagged.then(|| {
        let replace_zero = |gain: Complex32| {
            let amplitude = f64::from(gain.re).hypot(f64::from(gain.im));
            if amplitude == 0.0 { 1.0 } else { amplitude }
        };
        (replace_zero(gains[[0, 0]]) / replace_zero(gains[[1, 0]])).powi(2)
    });
    let (sine, cosine) = (2.0 * angle).sin_cos();
    Ok(GainSample {
        row,
        angle,
        sine,
        cosine,
        squared_amplitude_ratio,
    })
}

fn fit_antenna(samples: &[GainSample], coverage: f64) -> Result<Option<[f64; 2]>, String> {
    let mut bounds = [f64::INFINITY, f64::NEG_INFINITY];
    let mut good = 0;
    let points: Vec<_> = samples
        .iter()
        .map(|sample| {
            let Some(observed) = sample.squared_amplitude_ratio else {
                return ([0.0; 3], 0.0);
            };
            bounds[0] = bounds[0].min(sample.angle);
            bounds[1] = bounds[1].max(sample.angle);
            good += 1;
            ([1.0, sample.cosine, sample.sine], observed)
        })
        .collect();
    if good < 3 {
        return Ok(None);
    }
    if points.iter().any(|(basis, _)| !basis[1].is_finite()) {
        return Err("SVD did not converge in Linear Least Squares".into());
    }
    if bounds[1] - bounds[0] < coverage {
        return Ok(None);
    }
    if points.iter().any(|(_, observed)| !observed.is_finite()) {
        return Ok(Some([f64::NAN; 2]));
    }
    let [intercept, cosine, sine] = casa_numerics::solve_least_squares(&points)
        .ok_or("SVD did not converge in Linear Least Squares")?;
    Ok((intercept >= 0.0).then(|| [cosine / intercept / 2.0, sine / intercept / 2.0]))
}

fn mean_polarization(fits: impl Iterator<Item = [f64; 2]>) -> Option<[f64; 2]> {
    let (sum, count) = fits.fold(([0.0, 0.0], 0), |(sum, count), [q, u]| {
        ([sum[0] + q, sum[1] + u], count + 1)
    });
    (count > 0).then(|| sum.map(|value| value / f64::from(count)))
}

fn correct_series(
    table: &mut Table,
    samples: &[GainSample],
    [q, u]: [f64; 2],
    fitted: bool,
    changed_rows: &mut Vec<usize>,
) -> Result<(), String> {
    let fraction = q.hypot(u);
    if !fraction.is_finite() || fraction >= 1.0 {
        return if fitted {
            Err("Spurious fractional polarization!".into())
        } else {
            Ok(())
        };
    }
    let mut writer = table
        .row_accessor_mut()
        .prepare(&["CPARAM"])
        .map_err(error_text)?;
    for sample in samples {
        let Some(Value::Array(ArrayValue::Complex32(gains))) =
            writer.load(sample.row).map_err(error_text)?.get("CPARAM")
        else {
            return Err("calibration CPARAM must contain complex gains".into());
        };
        let mut gains = gains.clone();
        let polarized = q * sample.cosine + u * sample.sine;
        for (receptor, power) in [(0, 1.0 + polarized), (1, 1.0 - polarized)] {
            let gain = &mut gains[[receptor, 0]];
            let amplitude = power.sqrt();
            *gain = Complex32::new(
                (f64::from(gain.re) / amplitude) as f32,
                (f64::from(gain.im) / amplitude) as f32,
            );
        }
        writer
            .set_value_at(0, Value::Array(ArrayValue::Complex32(gains)))
            .map_err(error_text)?;
        changed_rows.push(sample.row);
    }
    writer.flush().map_err(error_text)
}
