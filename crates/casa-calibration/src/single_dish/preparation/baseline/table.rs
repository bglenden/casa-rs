// SPDX-License-Identifier: LGPL-3.0-or-later
//! The CASA single-dish baseline table, including its per-polarization masks.

use std::collections::BTreeMap;
use std::path::Path;

use casa_ms::{MeasurementSet, SubtableId};
use casa_tables::{ColumnSchema, Table, TableInfo, TableOptions, TableSchema, strip_directory};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{Array2, Axis};

use crate::writer::{set_fixed_unit_keyword, set_measinfo_keyword};

use super::polynomial::Polynomial;

pub(super) struct BaselineRow {
    pub(super) scan: u32,
    pub(super) beam: u32,
    pub(super) antenna: u32,
    pub(super) window: u32,
    pub(super) time: f64,
    pub(super) channels: usize,
    pub(super) models: Vec<Option<SpectrumModel>>,
}

pub(super) struct SpectrumModel {
    pub(super) family: Polynomial,
    pub(super) coefficients: Vec<f64>,
    pub(super) mask: Vec<u32>,
    pub(super) rms: f32,
    pub(super) clip_threshold: f32,
    pub(super) clip_iterations: u32,
}

#[derive(Clone, Copy)]
struct IndexedRow {
    time: f64,
    scan: u32,
    index: usize,
}

type BaselineGroup = (u32, u32, u32);

pub(super) struct BaselineTable {
    table: Table,
    rows: BTreeMap<BaselineGroup, Vec<IndexedRow>>,
}

pub(super) struct AppliedPolynomial {
    pub(super) family: Polynomial,
    pub(super) coefficients: Vec<f64>,
    pub(super) included: Vec<bool>,
}

impl BaselineTable {
    pub(super) fn open(path: &Path) -> Result<Self, String> {
        let table = Table::open(TableOptions::new(path)).map_err(error_text)?;
        let mut rows = BTreeMap::<_, Vec<IndexedRow>>::new();
        for index in 0..table.row_count() {
            let window = scalar_uint(&table, index, "IFNO")?;
            let antenna = scalar_uint(&table, index, "ANTNO")?;
            let beam = scalar_uint(&table, index, "BEAMNO")?;
            let scan = scalar_uint(&table, index, "SCANNO")?;
            let cell = table
                .cell_accessor(index, "TIME")
                .and_then(|cell| cell.scalar())
                .map_err(error_text)?;
            let ScalarValue::Float64(time) = cell else {
                return Err("Expected a baseline TIME with type Double.".into());
            };
            if !time.is_finite() {
                return Err("Nonfinite baseline TIME.".into());
            }
            rows.entry((window, antenna, beam))
                .or_default()
                .push(IndexedRow {
                    time: *time,
                    scan,
                    index,
                });
        }
        for group in rows.values_mut() {
            group.sort_by(|left, right| left.time.total_cmp(&right.time));
            group.dedup_by(|later, earlier| {
                if later.time == earlier.time {
                    *earlier = *later;
                    true
                } else {
                    false
                }
            });
        }
        Ok(Self { table, rows })
    }

    pub(super) fn matching_row(
        &self,
        window: u32,
        antenna: u32,
        beam: u32,
        scan: u32,
        time: f64,
        interval: f64,
    ) -> Option<usize> {
        let rows = self.rows.get(&(window, antenna, beam))?;
        if time < rows.first()?.time || time > rows.last()?.time {
            return None;
        }
        let later = rows.partition_point(|row| row.time < time);
        let matching = if rows[later].time == time {
            rows[later]
        } else {
            // CASA tests the earlier bracketing integration first.
            *rows
                .get(later.saturating_sub(1)..=later)?
                .iter()
                .find(|row| (row.time - time).abs() < interval / 2.)?
        };
        (matching.scan == scan).then_some(matching.index)
    }

    pub(super) fn model(
        &self,
        row: usize,
        receptor: usize,
    ) -> Result<Option<AppliedPolynomial>, String> {
        let cell = |column| {
            self.table
                .cell_accessor(row, column)
                .and_then(|cell| cell.array())
                .map_err(error_text)
        };
        let ArrayValue::Bool(apply) = cell("APPLY")? else {
            return Err("Expected a Boolean baseline APPLY cell.".into());
        };
        if !baseline_scalar(apply, receptor).copied().unwrap_or(false) {
            return Ok(None);
        }
        let ArrayValue::UInt32(families) = cell("FUNC_TYPE")? else {
            return Err("Expected an unsigned FUNC_TYPE cell.".into());
        };
        let family = match baseline_scalar(families, receptor) {
            Some(0) => Polynomial::Monomial,
            Some(1) => Polynomial::Chebyshev,
            _ => return Err("Unsupported baseline type.".into()),
        };
        let ArrayValue::Int32(degrees) = cell("FUNC_PARAM")? else {
            return Err("Expected an integer FUNC_PARAM cell.".into());
        };
        let degree = baseline_scalar(degrees, receptor)
            .and_then(|&value| usize::try_from(value).ok())
            .ok_or("Invalid baseline order.")?;
        let ArrayValue::Float32(result) = cell("RESULT")? else {
            return Err("Expected a floating-point RESULT cell.".into());
        };
        let mut coefficients = Vec::with_capacity(degree + 1);
        for term in 0..=degree {
            coefficients.push(f64::from(
                *result
                    .get([receptor, term])
                    .ok_or("Baseline order exceeds the stored coefficient count.")?,
            ));
        }
        let ArrayValue::UInt32(mask) = cell("MASKLIST")? else {
            return Err("Expected an unsigned MASKLIST cell.".into());
        };
        if mask.ndim() != 2 || receptor >= mask.shape()[0] {
            return Err("Invalid polarization axis in baseline MASKLIST.".into());
        }
        let ranges = mask
            .index_axis(Axis(0), receptor)
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let channels = scalar_uint(&self.table, row, "NCHAN")? as usize;
        let included = decode_ranges(&ranges, channels)?;
        Ok(Some(AppliedPolynomial {
            family,
            coefficients,
            included,
        }))
    }
}

fn baseline_scalar<T>(values: &ndarray::ArrayD<T>, receptor: usize) -> Option<&T> {
    match values.ndim() {
        1 => values.get([receptor]),
        2 => values.get([receptor, 0]),
        _ => None,
    }
}

fn decode_ranges(ranges: &[u32], channels: usize) -> Result<Vec<bool>, String> {
    let (pairs, tail) = ranges.as_chunks::<2>();
    if !tail.is_empty() {
        return Err("masklist must have even number of elements.".into());
    }
    let mut included = vec![false; channels];
    for &[start, end] in pairs {
        let section = included
            .get_mut(start as usize..=end as usize)
            .ok_or("Baseline mask range is outside the spectral window.")?;
        section.fill(true);
    }
    Ok(included)
}

fn scalar_uint(table: &Table, row: usize, column: &str) -> Result<u32, String> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar())
        .map_err(error_text)?
    {
        ScalarValue::UInt32(value) => Ok(*value),
        _ => Err(format!(
            "Expected unsigned integer baseline column {column}."
        )),
    }
}

impl BaselineRow {
    pub(super) fn record(&self) -> RecordValue {
        let receptors = self.models.len();
        let widest = |size: fn(&SpectrumModel) -> usize| {
            self.models
                .iter()
                .flatten()
                .map(size)
                .max()
                .unwrap_or_default()
        };
        let coefficients = widest(|model| model.coefficients.len());
        let ranges = widest(|model| model.mask.len());
        let mut apply = Array2::from_elem((receptors, 1), false);
        let mut family = Array2::<u32>::zeros((receptors, 1));
        let mut degree = Array2::<i32>::zeros((receptors, 1));
        let mut result = Array2::<f32>::zeros((receptors, coefficients));
        let mut mask = Array2::<u32>::zeros((receptors, ranges));
        let mut rms = Array2::<f32>::zeros((receptors, 1));
        let mut clip = Array2::<f32>::zeros((receptors, 1));
        let mut iterations = Array2::<u32>::zeros((receptors, 1));
        for (receptor, model) in self.models.iter().enumerate() {
            let Some(model) = model else { continue };
            apply[[receptor, 0]] = true;
            family[[receptor, 0]] = match model.family {
                Polynomial::Monomial => 0,
                Polynomial::Chebyshev => 1,
            };
            degree[[receptor, 0]] = model.coefficients.len().saturating_sub(1) as i32;
            for (index, &value) in model.coefficients.iter().enumerate() {
                result[[receptor, index]] = value as f32;
            }
            for (index, slot) in mask.row_mut(receptor).iter_mut().enumerate() {
                // A shorter mask repeats its last valid range to fill the
                // rectangular table cell without adding new fitted channels.
                let previous = model.mask.len().saturating_sub(2) + index % 2;
                *slot = model
                    .mask
                    .get(index)
                    .or_else(|| model.mask.get(previous))
                    .copied()
                    .unwrap_or_default();
            }
            rms[[receptor, 0]] = model.rms;
            clip[[receptor, 0]] = model.clip_threshold;
            iterations[[receptor, 0]] = model.clip_iterations;
        }
        let uint = |name, value| RecordField::new(name, Value::Scalar(ScalarValue::UInt32(value)));
        let array = |name, value| RecordField::new(name, Value::Array(value));
        RecordValue::new(vec![
            uint("SCANNO", self.scan),
            uint("BEAMNO", self.beam),
            uint("ANTNO", self.antenna),
            uint("IFNO", self.window),
            uint("FREQ_ID", 0),
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(self.time))),
            array("APPLY", ArrayValue::Bool(apply.into_dyn())),
            array("FUNC_TYPE", ArrayValue::UInt32(family.into_dyn())),
            array("FUNC_PARAM", ArrayValue::Int32(degree.into_dyn())),
            array(
                "FUNC_FPARAM",
                ArrayValue::Float32(Array2::zeros((receptors, 0)).into_dyn()),
            ),
            array("MASKLIST", ArrayValue::UInt32(mask.into_dyn())),
            array("RESULT", ArrayValue::Float32(result.into_dyn())),
            array("RMS", ArrayValue::Float32(rms.into_dyn())),
            uint("NCHAN", self.channels as u32),
            array("CLIP_THRESHOLD", ArrayValue::Float32(clip.into_dyn())),
            array("CLIP_ITERATION", ArrayValue::UInt32(iterations.into_dyn())),
            array(
                "USE_LF",
                ArrayValue::Bool(Array2::from_elem((receptors, 1), false).into_dyn()),
            ),
            array(
                "LF_THRESHOLD",
                ArrayValue::Float32(Array2::zeros((receptors, 1)).into_dyn()),
            ),
            array(
                "LF_AVERAGE",
                ArrayValue::UInt32(Array2::zeros((receptors, 1)).into_dyn()),
            ),
            array(
                "LF_EDGE",
                ArrayValue::UInt32(Array2::zeros((receptors, 2)).into_dyn()),
            ),
        ])
    }
}

pub(super) fn mask_ranges(mask: &[bool]) -> Vec<u32> {
    let mut result = Vec::new();
    let mut first = None;
    for (channel, included) in mask.iter().copied().chain([false]).enumerate() {
        if included {
            first.get_or_insert(channel);
        } else if let Some(start) = first.take() {
            result.extend([start as u32, (channel - 1) as u32]);
        }
    }
    result
}

pub(super) fn write(
    output: &Path,
    input: &MeasurementSet,
    rows: impl IntoIterator<Item = RecordValue>,
) -> Result<(), String> {
    let mut table = Table::with_schema(schema());
    table.set_info(TableInfo {
        table_type: "ApplyTable".into(),
        sub_type: String::new(),
        readme: Vec::new(),
    });
    let source = input
        .path()
        .map(|path| path.display().to_string())
        .unwrap_or_default();
    for (key, value) in [
        (
            "ApplyType",
            Value::Scalar(ScalarValue::String("BASELINE".into())),
        ),
        ("VERSION", Value::Scalar(ScalarValue::Int32(1))),
        ("MSName", Value::Scalar(ScalarValue::String(source))),
        (
            "FREQUENCIES",
            Value::table_ref(strip_directory(&output.join("SPECTRAL_WINDOW"), output)),
        ),
    ] {
        table.keywords_mut().upsert(key, value);
    }
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "TIME", &["d"]);
    table.set_column_keywords(
        "FUNC_TYPE",
        RecordValue::new(
            ["Polynomial", "Chebyshev", "Cubic Spline", "Sinusoid"]
                .into_iter()
                .enumerate()
                .map(|(id, label)| {
                    RecordField::new(label, Value::Scalar(ScalarValue::Int32(id as i32)))
                })
                .collect(),
        ),
    );
    for row in rows {
        table.add_row(row).map_err(error_text)?;
    }
    table.save(TableOptions::new(output)).map_err(error_text)?;
    input
        .subtable(SubtableId::SpectralWindow)
        .ok_or("No SPECTRAL_WINDOW subtable for the baseline table.")?
        .deep_copy(TableOptions::new(output.join("SPECTRAL_WINDOW")))
        .map_err(error_text)?;
    Ok(())
}

fn schema() -> TableSchema {
    use PrimitiveType::{Bool, Float32, Float64, Int32, UInt32};
    let array = |name, primitive| ColumnSchema::array_variable(name, primitive, Some(2));
    TableSchema::new(vec![
        ColumnSchema::scalar("SCANNO", UInt32),
        ColumnSchema::scalar("BEAMNO", UInt32),
        ColumnSchema::scalar("ANTNO", UInt32),
        ColumnSchema::scalar("IFNO", UInt32),
        ColumnSchema::scalar("FREQ_ID", UInt32),
        ColumnSchema::scalar("TIME", Float64),
        array("APPLY", Bool),
        array("FUNC_TYPE", UInt32),
        array("FUNC_PARAM", Int32),
        array("FUNC_FPARAM", Float32),
        array("MASKLIST", UInt32),
        array("RESULT", Float32),
        array("RMS", Float32),
        ColumnSchema::scalar("NCHAN", UInt32),
        array("CLIP_THRESHOLD", Float32),
        array("CLIP_ITERATION", UInt32),
        array("USE_LF", Bool),
        array("LF_THRESHOLD", Float32),
        array("LF_AVERAGE", UInt32),
        array("LF_EDGE", UInt32),
    ])
    .expect("valid baseline table schema")
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
