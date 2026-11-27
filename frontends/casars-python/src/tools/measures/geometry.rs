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
        (
            "value",
            CasaValue::Floats {
                shape: vec![values.len()],
                values,
            },
        ),
    ])
}
fn failed_tuple(lines: &mut Vec<LogLine>, message: impl Into<String>, n: usize) -> CasaValue {
    lines.push(LogLine::error(message));
    CasaValue::Tuple(vec![CasaValue::empty_record(); n])
}
fn cartesian(value: Measure) -> Result<[f64; 3], String> {
    match value.value {
        MeasureValue::Baseline(xyz) | MeasureValue::Uvw(xyz) | MeasureValue::Position(xyz) => {
            Ok(xyz)
        }
        _ => Err("Trying to convert non-baseline to uvw\n".into()),
    }
}

/// Expand each ordered `i < j` pair to `position[j] - position[i]`, retaining metadata.
/// CASA forces the input to uvw for decoding and restores its original labels.
pub(super) fn expand(input: &RecordArg, lines: &mut Vec<LogLine>) -> CasaValue {
    let mut fields = input.0.clone();
    let labels = match (fields.get("type"), fields.get("refer")) {
        (Some(Variant::Text(kind)), Some(Variant::Text(refer))) => (kind.clone(), refer.clone()),
        _ => {
            return failed_tuple(
                lines,
                "Non-measure input to expand()\nCan only expand baselines, positions, or uvw\n",
                2,
            );
        }
    };
    if !matches!(
        labels.0.to_ascii_lowercase().as_str(),
        "baseline" | "position" | "uvw"
    ) {
        return failed_tuple(lines, "Can only expand baselines, positions, or uvw", 2);
    }
    fields.insert("type".into(), Variant::Text("uvw".into()));
    fields.insert("refer".into(), Variant::Text("J2000".into()));
    let source = match Measures::read(&RecordArg(fields)) {
        Ok(source) => source,
        Err(error) => return failed_tuple(lines, error.message(), 2),
    };
    let positions: Vec<_> = source
        .values
        .iter()
        .map(|&v| cartesian(v).expect("decoded uvw"))
        .collect();
    let mut vectors = Vec::new();
    if positions.len() < 2 {
        vectors.push(positions[0]);
    } else {
        for (i, first) in positions.iter().enumerate() {
            for second in &positions[i + 1..] {
                vectors.push(std::array::from_fn(|axis| second[axis] - first[axis]));
            }
        }
    }
    let mut result = Measures::scalar(Measure {
        refer: "J2000",
        value: MeasureValue::Uvw(vectors[0]),
    });
    result.vector = source.values.len() > 1;
    result.values = vectors
        .iter()
        .map(|&v| Measure {
            refer: "J2000",
            value: MeasureValue::Uvw(v),
        })
        .collect();
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
    CasaValue::Tuple(vec![
        record,
        quantity_vector(vectors.into_iter().flatten().collect(), "m"),
    ])
}

/// Project absolute J2000 baselines on the explicit frame direction, and
/// provide casacore's sidereal-rate uvw derivative in flattened XYZ order.
pub(super) fn touvw(
    state: &mut State,
    input: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    let source = match Measures::read(input) {
        Ok(source) if source.values[0].kind() == MeasureKind::Baseline => source,
        Ok(_) => {
            return Ok(failed_tuple(
                lines,
                "Trying to convert non-baseline to uvw\n",
                3,
            ));
        }
        Err(error) => return Ok(failed_tuple(lines, error.message(), 3)),
    };
    let source = match source.resolve(state, "J2000")? {
        Ok(source) => source,
        Err(error) => return Ok(failed_tuple(lines, error, 3)),
    };
    let Some(direction) = state.frame.direction.as_ref() else {
        return Ok(failed_tuple(
            lines,
            "No direction in frame for uvw calculation\n",
            3,
        ));
    };
    let direction = direction.absolute;
    let result = super::frame::convert(state, &direction, "J2000")?;
    let direction = match result {
        Ok(direction) => direction,
        Err(error) => return Ok(failed_tuple(lines, error, 3)),
    };
    let crate::convert::holder::CasaMeasure::Direction(direction) = direction
        .to_casa()
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?
    else {
        unreachable!("frame direction")
    };
    let dec = direction.latitude_rad();
    let (sd, cd) = dec.sin_cos();
    let rate = std::f64::consts::PI / 180.0 / 240.0 / 1.002_737_909_350_795;
    let mut xyz = Vec::with_capacity(3 * source.values.len());
    let mut dot = Vec::with_capacity(xyz.capacity());
    let result = source
        .map(|value| {
            let vector =
                casa_core::measures::baseline::j2000_baseline_uvw(cartesian(value)?, &direction);
            xyz.extend(vector);
            dot.extend([
                (-sd * vector[1] + cd * vector[2]) * rate,
                sd * vector[0] * rate,
                -cd * vector[0] * rate,
            ]);
            Ok(Measure {
                refer: "J2000",
                value: MeasureValue::Uvw(vector),
            })
        })
        .expect("baseline kind checked");
    Ok(CasaValue::Tuple(vec![
        result.to_value(),
        quantity_vector(xyz, "m"),
        quantity_vector(dot, "m/s"),
    ]))
}

use casa_core::measures::MeasFrame;
use casa_core::measures::baseline::baseline_rotation;
use casa_core::measures::direction::DirectionRef;
use casa_core::measures::earth_magnetic::{EarthMagneticRef, MEarthMagnetic};

enum Operator {
    Matrix([[f64; 3]; 3]),
    Magnetic(Box<MeasFrame>, EarthMagneticRef),
}
/// One operator for every element of a measure vector, compiled once per call.
pub(super) struct Conversion {
    operator: Operator,
    refer: &'static str,
}
impl Conversion {
    pub(super) fn new(
        state: &mut State,
        input: Measure,
        target: &'static str,
    ) -> PyResult<Result<Self, String>> {
        let frame = match super::frame::native_frame(state)? {
            Ok(frame) => super::frame::with_resolved_direction(state, frame),
            Err(error) => return Ok(Err(error)),
        };
        Ok((|| {
            let operator = if input.kind() == MeasureKind::EarthMagnetic {
                Operator::Magnetic(
                    Box::new(frame),
                    target
                        .parse()
                        .map_err(|e: casa_core::measures::MeasureError| e.to_string())?,
                )
            } else {
                let from = input
                    .refer
                    .parse::<DirectionRef>()
                    .map_err(|e| e.to_string())?;
                let to = target.parse::<DirectionRef>().map_err(|e| e.to_string())?;
                let matrix = baseline_rotation(from, to, &frame, input.kind() == MeasureKind::Uvw)
                    .map_err(|error| error.to_string())?;
                Operator::Matrix(matrix)
            };
            Ok(Self {
                operator,
                refer: target,
            })
        })())
    }
    pub(super) fn apply(&self, input: Measure) -> Result<Measure, String> {
        let value = match (&self.operator, input.value) {
            (Operator::Matrix(m), MeasureValue::Baseline(v)) => {
                MeasureValue::Baseline(rotate(*m, v))
            }
            (Operator::Matrix(m), MeasureValue::Uvw(v)) => MeasureValue::Uvw(rotate(*m, v)),
            (Operator::Magnetic(frame, target), MeasureValue::EarthMagnetic(v)) => {
                let from = input
                    .refer
                    .parse()
                    .map_err(|e: casa_core::measures::MeasureError| e.to_string())?;
                let value = MEarthMagnetic::from_xyz_nt(v[0], v[1], v[2], from)
                    .convert_to(*target, frame)
                    .map_err(|e| e.to_string())?;
                MeasureValue::EarthMagnetic(value.xyz_nt())
            }
            _ => return Err("Non-conforming geometry operator".into()),
        };
        Ok(Measure {
            refer: self.refer,
            value,
        })
    }
}
pub(super) fn convert(
    state: &mut State,
    input: &Measure,
    target: &'static str,
) -> PyResult<Result<Measure, String>> {
    Ok(match Conversion::new(state, *input, target)? {
        Ok(plan) => plan.apply(*input),
        Err(error) => Err(error),
    })
}
fn rotate(m: [[f64; 3]; 3], v: [f64; 3]) -> [f64; 3] {
    std::array::from_fn(|i| (0..3).map(|j| m[i][j] * v[j]).sum())
}
use crate::convert::quantum::quantum_holder;
use casa_core::quanta::Quantity;

fn failure(lines: &mut Vec<LogLine>, error: impl Into<String>) -> CasaValue {
    lines.push(LogLine::error(error));
    CasaValue::empty_record()
}
pub(super) fn define_three(
    state: &mut State,
    kind: MeasureKind,
    rf: &str,
    args: [&Variant; 3],
    off: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    let array = quantum_holder(args[0])
        .ok()
        .flatten()
        .filter(|q| q.is_array() && !q.values.is_empty());
    let mut triples = Vec::new();
    if kind != MeasureKind::EarthMagnetic
        && let Some(first) = array
    {
        let wrong_length = if kind == MeasureKind::Position {
            "Exception Reports: The length of the 3 quantities are not conformant \n"
        } else {
            "The length of the 3 quantities are not conformant \n"
        };
        let mut quantities = vec![first];
        for arg in &args[1..] {
            let Some(q) = quantum_holder(arg).ok().flatten() else {
                return Ok(failure(lines, wrong_length));
            };
            if q.values.len() != quantities[0].values.len() {
                return Ok(failure(lines, wrong_length));
            }
            quantities.push(q);
        }
        for i in 0..quantities[0].values.len() {
            triples.push(
                quantities
                    .iter()
                    .map(|q| Quantity::with_unit(q.values[i], q.unit.clone()))
                    .collect::<Vec<_>>(),
            );
        }
    } else {
        let Some(mut q) = args
            .iter()
            .map(|v| super::define::quantity_of(v, lines))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(CasaValue::empty_record());
        };
        if q.iter()
            .all(|q| q.value() == 0. && q.unit().name().is_empty())
        {
            q = if kind == MeasureKind::EarthMagnetic {
                vec![
                    Quantity::new(0., "G").expect("G"),
                    Quantity::new(0., "deg").expect("deg"),
                    Quantity::new(90., "deg").expect("deg"),
                ]
            } else {
                vec![
                    Quantity::new(0., "deg").expect("deg"),
                    Quantity::new(0., "deg").expect("deg"),
                    Quantity::new(0., "m").expect("m"),
                ]
            };
        }
        triples.push(q);
    }
    if kind == MeasureKind::EarthMagnetic {
        use casa_core::quanta::{Dimension, UnitDim};
        let angle = UnitDim::basis(Dimension::Angle);
        if triples[0][1..].iter().any(|q| q.unit().val().dim != angle) {
            return Ok(failure(
                lines,
                "earthmagnetic requires a field strength and two angles",
            ));
        }
    }
    let refer = if kind == MeasureKind::Position {
        super::define::position_reference(rf, lines)
    } else {
        kind.codes().resolve(rf).unwrap_or_else(|| {
            lines.push(LogLine::warning("Illegal reference frame string."));
            kind.codes().default_code()
        })
    };
    let values = match triples
        .iter()
        .map(|q| Measure::from_values(kind, refer, q))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(values) => values,
        Err(error) => return Ok(failure(lines, error.message())),
    };
    let mut result = Measures::scalar(values[0]);
    result.vector = values.len() > 1;
    result.values = values;
    if !off.0.is_empty() {
        match Measures::read(off) {
            Ok(origin) if origin.values[0].kind() == kind => {
                if kind == MeasureKind::EarthMagnetic {
                    result.offset = Some(Box::new(origin.into_reference_origin()));
                } else {
                    result = match result.relative_to(origin, state)? {
                        Ok(value) => value,
                        Err(error) => return Ok(failure(lines, error)),
                    };
                }
            }
            Err(_) if kind == MeasureKind::Position => {}
            Err(_) => lines.push(LogLine::warning(
                "Illegal offset type specified, not used\n",
            )),
            _ if kind == MeasureKind::Baseline => lines.push(LogLine::warning(
                "Illegal offset type specified, not used\n",
            )),
            _ => return Ok(failure(lines, "Non-conforming offset measure type")),
        }
    }
    Ok(result.to_value())
}
fn asbaseline(
    state: &mut State,
    input: &RecordArg,
    lines: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    let source = match Measures::read(input) {
        Ok(source) => source,
        Err(error) => return Ok(failure(lines, error.message())),
    };
    if source.values[0].kind() == MeasureKind::Baseline {
        return Ok(source.to_value());
    }
    if source.values[0].kind() != MeasureKind::Position {
        return Ok(failure(lines, "Non-position type for asbaseline input"));
    }
    let source = match source.resolve(state, "ITRF")? {
        Ok(source) => source,
        Err(error) => return Ok(failure(lines, error)),
    };
    let source = source
        .map(|value| {
            Ok(Measure {
                refer: "ITRF",
                value: MeasureValue::Baseline(cartesian(value)?),
            })
        })
        .expect("position kind");
    match source.resolve(state, "J2000")? {
        Ok(value) => Ok(value.to_value()),
        Err(error) => Ok(failure(lines, error)),
    }
}

use super::PyMeasures;
use pyo3::prelude::*;
#[pymethods]
impl PyMeasures {
    /// Define one or more baseline vectors, with an optional reference origin.
    #[pyo3(signature=(rf=None,v0=Variant::Empty,v1=Variant::Empty,v2=Variant::Empty,off=None),text_signature="($self, rf='ITRF', v0=[], v1=[], v2=[], off={})")]
    fn baseline(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=super::arguments::supplied)] rf: Option<Bound<'_, PyAny>>,
        v0: Variant,
        v1: Variant,
        v2: Variant,
        #[pyo3(from_py_with=super::arguments::supplied)] off: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let rf = super::arguments::reference(rf.as_ref(), "ITRF")?;
        let off = super::arguments::record(off.as_ref(), "off")?;
        self.call(py, |state, lines| {
            define_three(
                state,
                MeasureKind::Baseline,
                &rf,
                [&v0, &v1, &v2],
                &off,
                lines,
            )
        })
    }
    /// Define one or more uvw vectors, with an optional reference origin.
    #[pyo3(signature=(rf=None,v0=Variant::Empty,v1=Variant::Empty,v2=Variant::Empty,off=None),text_signature="($self, rf='ITRF', v0=[], v1=[], v2=[], off={})")]
    fn uvw(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=super::arguments::supplied)] rf: Option<Bound<'_, PyAny>>,
        v0: Variant,
        v1: Variant,
        v2: Variant,
        #[pyo3(from_py_with=super::arguments::supplied)] off: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let rf = super::arguments::reference(rf.as_ref(), "ITRF")?;
        let off = super::arguments::record(off.as_ref(), "off")?;
        self.call(py, |state, lines| {
            define_three(state, MeasureKind::Uvw, &rf, [&v0, &v1, &v2], &off, lines)
        })
    }
    /// Define a magnetic field using its strength, longitude and latitude.
    #[pyo3(signature=(rf=None,v0=Variant::Empty,v1=Variant::Empty,v2=Variant::Empty,off=None),text_signature="($self, rf='IGRF', v0=[], v1=[], v2=[], off={})")]
    fn earthmagnetic(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=super::arguments::supplied)] rf: Option<Bound<'_, PyAny>>,
        v0: Variant,
        v1: Variant,
        v2: Variant,
        #[pyo3(from_py_with=super::arguments::supplied)] off: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let rf = super::arguments::reference(rf.as_ref(), "IGRF")?;
        let off = super::arguments::record(off.as_ref(), "off")?;
        self.call(py, |state, lines| {
            define_three(
                state,
                MeasureKind::EarthMagnetic,
                &rf,
                [&v0, &v1, &v2],
                &off,
                lines,
            )
        })
    }
    /// Convert positions into J2000 baselines, retaining every input element.
    #[pyo3(signature=(pos),text_signature="($self, pos)")]
    fn asbaseline(&self, py: Python<'_>, pos: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let pos = super::arguments::record(Some(pos), "pos")?;
        self.call(py, |state, lines| asbaseline(state, &pos, lines))
    }
    /// Expand antenna coordinates into every pairwise baseline.
    #[pyo3(signature=(v=None),text_signature="($self, v={})")]
    fn expand(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=super::arguments::supplied)] v: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let v = super::arguments::record(v.as_ref(), "v")?;
        self.call(py, |_, lines| Ok(expand(&v, lines)))
    }
    /// Project baselines into uvw and uvw time derivatives.
    #[pyo3(signature=(v=None),text_signature="($self, v={})")]
    fn touvw(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=super::arguments::supplied)] v: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let v = super::arguments::record(v.as_ref(), "v")?;
        self.call(py, |state, lines| touvw(state, &v, lines))
    }
}
