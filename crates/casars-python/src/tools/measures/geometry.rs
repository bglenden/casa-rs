// SPDX-License-Identifier: LGPL-3.0-or-later
//! Baseline/uvw construction, projection and pairwise expansion.
use super::{State, record::Measures};
use crate::convert::holder::{Measure, MeasureKind, MeasureValue};
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant};
use crate::tools::LogLine;
use pyo3::PyResult;

fn quantity_vector(values: Vec<f64>, unit: &str) -> CasaValue {
    CasaValue::record([
        ("unit", CasaValue::Text(unit.into())),
        ("value", CasaValue::Floats { shape: vec![values.len()], values }),
    ])
}
fn failed_tuple(lines: &mut Vec<LogLine>, message: impl Into<String>, n: usize) -> CasaValue {
    lines.push(LogLine::error(message));
    CasaValue::Tuple(vec![CasaValue::empty_record(); n])
}
fn cartesian(value: Measure) -> Result<[f64; 3], String> {
    match value.value {
        MeasureValue::Baseline(xyz) | MeasureValue::Uvw(xyz) | MeasureValue::Position(xyz) => Ok(xyz),
        _ => Err("Trying to convert non-baseline to uvw\n".into()),
    }
}

/// Expand each ordered i<j pair to position[j]-position[i], retaining metadata.
/// CASA forces the input to uvw for decoding and restores its original labels.
pub(super) fn expand(input: &RecordArg, lines: &mut Vec<LogLine>) -> CasaValue {
    let mut fields = input.0.clone();
    let labels = match (fields.get("type"), fields.get("refer")) {
        (Some(Variant::Text(kind)), Some(Variant::Text(refer))) => (kind.clone(), refer.clone()),
        _ => return failed_tuple(lines, "Non-measure input to expand()\nCan only expand baselines, positions, or uvw\n", 2),
    };
    fields.insert("type".into(), Variant::Text("uvw".into()));
    fields.insert("refer".into(), Variant::Text("J2000".into()));
    let source = match Measures::read(&RecordArg(fields)) {
        Ok(source) => source,
        Err(error) => return failed_tuple(lines, error.message(), 2),
    };
    let positions: Vec<_> = source.values.iter().map(|&v| cartesian(v).expect("decoded uvw")).collect();
    let mut vectors = Vec::new();
    if positions.len() < 2 {
        vectors.push(positions[0]);
    } else {
        for (i, first) in positions.iter().enumerate() {
            for second in &positions[i+1..] {
                vectors.push(std::array::from_fn(|axis| second[axis]-first[axis]));
            }
        }
    }
    let mut result = Measures::scalar(Measure { refer: "J2000", value: MeasureValue::Uvw(vectors[0]) });
    result.vector = source.values.len() > 1;
    result.values = vectors.iter().map(|&v| Measure { refer: "J2000", value: MeasureValue::Uvw(v) }).collect();
    let mut record = result.to_value();
    if let CasaValue::Record(fields) = &mut record {
        for (key, value) in fields {
            match key.as_str() {
                "type" => *value = CasaValue::Text(labels.0.clone()),
                "refer" => *value = CasaValue::Text(labels.1.clone()),
                _ => {}
            }
        }
    }
    CasaValue::Tuple(vec![record, quantity_vector(vectors.into_iter().flatten().collect(), "m")])
}

/// Project absolute J2000 baselines on the explicit frame direction, and
/// provide casacore's sidereal-rate uvw derivative in flattened XYZ order.
pub(super) fn touvw(state: &mut State, input: &RecordArg, lines: &mut Vec<LogLine>) -> PyResult<CasaValue> {
    let source = match Measures::read(input) {
        Ok(source) if source.values[0].kind() == MeasureKind::Baseline => source,
        Ok(_) => return Ok(failed_tuple(lines, "Trying to convert non-baseline to uvw\n", 3)),
        Err(error) => return Ok(failed_tuple(lines, error.message(), 3)),
    };
    let source = match source.resolve(state, "J2000")? {
        Ok(source) => source,
        Err(error) => return Ok(failed_tuple(lines, error, 3)),
    };
    let Some(direction) = state.frame.direction else {
        return Ok(failed_tuple(lines, "No direction in frame for uvw calculation\n", 3));
    };
    let direction = match super::frame::convert(state, &direction, "J2000")? {
        Ok(direction) => direction,
        Err(error) => return Ok(failed_tuple(lines, error, 3)),
    };
    let crate::convert::holder::CasaMeasure::Direction(direction) = direction.to_casa().map_err(pyo3::exceptions::PyRuntimeError::new_err)? else { unreachable!("frame direction") };
    let dec = direction.latitude_rad();
    let (sd, cd) = dec.sin_cos();
    let rate = std::f64::consts::PI / 180.0 / 240.0 / 1.002_737_909_350_795;
    let mut xyz = Vec::with_capacity(3*source.values.len());
    let mut dot = Vec::with_capacity(xyz.capacity());
    let result = source.map(|value| {
        let vector = casa_types::measures::baseline::j2000_baseline_uvw(cartesian(value)?, &direction);
        xyz.extend(vector);
        dot.extend([(-sd*vector[1]+cd*vector[2])*rate, sd*vector[0]*rate, -cd*vector[0]*rate]);
        Ok(Measure { refer: "J2000", value: MeasureValue::Uvw(vector) })
    }).expect("baseline kind checked");
    Ok(CasaValue::Tuple(vec![result.to_value(), quantity_vector(xyz, "m"), quantity_vector(dot, "m/s")]))
}
