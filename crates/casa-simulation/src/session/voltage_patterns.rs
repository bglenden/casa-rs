// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulator::getVPRecord and VPManager saved-table selection.
use std::{path::Path, sync::Arc};

use casa_imaging_runtime::{Admitted, Demand, Reservation, admit};
use casa_ms::voltage_pattern::{VoltagePatternTable, radial_pattern};
use casa_types::{
    RecordValue, ScalarValue, Value,
    quanta::{Quantity, Unit},
};

use super::{
    Simulator, VoltagePatternSpec,
    error::{SimulatorError, SimulatorResult},
};
use crate::predict::{BeamPattern, PrimaryBeam};

mod images;

#[derive(Clone)]
pub(crate) enum BeamSource {
    Configured(PrimaryBeam),
    Tabulated(Arc<Admitted<BeamPattern>>),
    Image(Arc<crate::predict::jones::JonesImage>),
}

impl std::fmt::Debug for BeamSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configured(beam) => std::fmt::Debug::fmt(beam, f),
            Self::Tabulated(_) => f.write_str("saved radial voltage curve"),
            Self::Image(image) => std::fmt::Debug::fmt(image, f),
        }
    }
}

impl BeamSource {
    pub(crate) fn configured(&self) -> Option<PrimaryBeam> {
        match self {
            Self::Configured(beam) => Some(*beam),
            Self::Tabulated(_) | Self::Image(_) => None,
        }
    }
    pub(crate) fn pattern(&self) -> SimulatorResult<BeamPattern> {
        Ok(match self {
            Self::Configured(beam) => beam.pattern(),
            Self::Tabulated(pattern) => pattern.value.clone(),
            Self::Image(_) => {
                return Err(SimulatorError::not_yet_implemented(
                    "saved Jones image consumption through this prediction path (#1297)",
                ));
            }
        })
    }
    pub(crate) fn image(&self) -> Option<&Arc<crate::predict::jones::JonesImage>> {
        match self {
            Self::Image(image) => Some(image),
            _ => None,
        }
    }
}

fn raised(error: impl ToString) -> SimulatorError {
    SimulatorError::raised(format!("voltage-pattern table: {}", error.to_string()))
}

pub(super) fn capture_path(
    session: &Simulator,
    spec: &VoltagePatternSpec,
) -> SimulatorResult<Option<Arc<Admitted<String>>>> {
    if spec.use_default_vp || spec.vp_table.is_empty() {
        return Ok(None);
    }
    let reservation = reserve(session, spec.vp_table.len() as u64 + 128)?;
    Ok(Some(Arc::new(Admitted {
        value: spec.vp_table.clone(),
        reservation,
    })))
}

fn reserve(session: &Simulator, memory: u64) -> SimulatorResult<Reservation> {
    let resources = &session.options.resources;
    admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "the simulator's saved voltage pattern",
            memory,
        },
    )
    .map_err(SimulatorError::admission)
}

/// Preserve how VPManager resolved the record: standard VLA patterns carry
/// band squint, while custom records choose common-beam mosaic kernels.
#[derive(Clone)]
pub(super) struct ResolvedBeam {
    pub(super) pattern: BeamSource,
    pub(super) standard: bool,
    /// Saved AZEL tangent offset in radians, and its reference frequency Hz.
    pub(super) squint: Option<([f64; 2], f64)>,
}

/// MSDerivedValues::parAngle leaves equatorial feeds fixed. The component
/// cache currently shares one rotation across antennas; reject other or
/// mixed mounting geometries instead of silently applying alt-az rotation.
pub(super) fn squint_mount(session: &Simulator) -> SimulatorResult<bool> {
    let kind = |name: &str| {
        if name.is_empty()
            || name.eq_ignore_ascii_case("alt-az")
            || name.eq_ignore_ascii_case("alt-az+rotator")
        {
            Ok(true)
        } else if name.eq_ignore_ascii_case("equatorial") {
            Ok(false)
        } else {
            Err(SimulatorError::not_yet_implemented(format!(
                "squinted component prediction with antenna mount {name} (#1297)"
            )))
        }
    };
    let first = kind(&session.antennas[0].mount)?;
    for antenna in &session.antennas {
        if kind(&antenna.mount)? != first {
            return Err(SimulatorError::not_yet_implemented(
                "squinted component prediction with mixed antenna mount classes (#1297)",
            ));
        }
    }
    Ok(first)
}

pub(super) fn read(session: &Simulator, path: &Path) -> SimulatorResult<ResolvedBeam> {
    let _root_memory = reserve(
        session,
        VoltagePatternTable::reading_bytes(path).map_err(raised)?,
    )?;
    let table = VoltagePatternTable::open(path).map_err(raised)?;
    let _defaults_memory = reserve(session, table.defaults_reading_bytes().map_err(raised)?)?;
    let telescope = session.observing_telescope();
    let Some(record) = table.default_record(telescope).map_err(raised)? else {
        return PrimaryBeam::for_telescope(telescope, session.reference_frequency_hz())
            .map(|pattern| ResolvedBeam {
                pattern: BeamSource::Configured(pattern),
                standard: true,
                squint: None,
            })
            .ok_or_else(|| {
                SimulatorError::not_yet_implemented(format!(
                    "default simulation beam for {telescope} (#1297)"
                ))
            });
    };
    let squint = if session.squint
        && let Some(Value::Record(direction)) = record.get("squintdir")
    {
        let offset =
            casa_types::measures::record::direction_from_record(direction).map_err(raised)?;
        if offset.as_angles() != (0., 0.) {
            let reference = quantity(&record, "squintreffreq", "Hz")?;
            // A squint is a signed tangent offset. MVDirection::getAngle
            // uses atan2's signed longitude; a sky longitude in [0,2π)
            // would turn a small negative offset into almost a full turn.
            let [cx, cy, _] = offset.cosines();
            let (x, y) = (cy.atan2(cx), offset.latitude_rad());
            if !x.is_finite() || !y.is_finite() || !reference.is_finite() || reference <= 0. {
                return Err(raised(
                    "squint offset and positive reference frequency must be finite",
                ));
            }
            Some(([x, y], reference))
        } else {
            None
        }
    } else {
        None
    };
    let name = text(&record, "name")?;
    let pattern = if name == "IMAGE" {
        BeamSource::Image(images::load(session, &record)?)
    } else if matches!(name, "POLY" | "IPOLY" | "COSPOLY" | "NUMERIC") {
        saved_curve(session, &record)?
    } else {
        BeamSource::Configured(from_record(&record)?)
    };
    Ok(ResolvedBeam {
        pattern,
        standard: false,
        squint,
    })
}

fn numbers(record: &RecordValue, name: &str) -> SimulatorResult<Vec<f64>> {
    match record.get(name) {
        Some(Value::Array(casa_types::ArrayValue::Float64(values))) if values.ndim() == 1 => {
            Ok(values.iter().copied().collect())
        }
        _ => Err(raised(format!("missing numeric vector {name}"))),
    }
}

fn saved_curve(session: &Simulator, record: &RecordValue) -> SimulatorResult<BeamSource> {
    let ghz = quantity(record, "reffreq", "GHz")?;
    let radius = quantity(record, "maxrad", "arcmin")? * ghz;
    if !ghz.is_finite() || ghz <= 0. || !radius.is_finite() || radius <= 0. {
        return Err(raised(
            "reference frequency and support must be finite and positive",
        ));
    }
    let voltage = boolean(record, "isthisvp")?;
    if text(record, "name")? == "IPOLY" && record.get("fitfreqs").is_some() {
        return saved_frequency_curve(session, record, radius, voltage);
    }
    let mut reservation = reserve(session, 2 * BeamPattern::BYTES + 256)?;
    let pattern = match text(record, "name")? {
        "POLY" | "IPOLY" => BeamPattern::polynomial(
            radius,
            &numbers(record, "coeff")?,
            text(record, "name")? == "IPOLY",
            voltage,
        ),
        "COSPOLY" => BeamPattern::cosine_polynomial(
            radius,
            &numbers(record, "coeff")?,
            &numbers(record, "scale")?
                .into_iter()
                .map(|scale| scale / ghz)
                .collect::<Vec<_>>(),
            voltage,
        ),
        "NUMERIC" => BeamPattern::numeric(
            radius,
            &numbers(record, "vect")?
                .into_iter()
                .map(|v| v as f32)
                .collect::<Vec<_>>(),
            voltage,
        ),
        _ => unreachable!("the selected radial curve form"),
    }
    .ok_or_else(|| raised("invalid support, coefficients or voltage samples"))?;
    reservation.retain(BeamPattern::BYTES + 128);
    Ok(BeamSource::Tabulated(Arc::new(Admitted {
        value: pattern,
        reservation,
    })))
}

fn saved_frequency_curve(
    session: &Simulator,
    record: &RecordValue,
    radius: f64,
    voltage: bool,
) -> SimulatorResult<BeamSource> {
    let Some(Value::Array(casa_types::ArrayValue::Float64(coefficients))) = record.get("coeff")
    else {
        return Err(raised("missing coefficient matrix"));
    };
    let Some(Value::Array(casa_types::ArrayValue::Float64(frequencies))) = record.get("fitfreqs")
    else {
        return Err(raised("missing fit frequency vector"));
    };
    if coefficients.ndim() != 2
        || frequencies.ndim() != 1
        || coefficients.shape()[1] != frequencies.len()
    {
        return Err(raised(
            "coefficient columns and fit frequencies do not match",
        ));
    }
    let bytes = ((coefficients.len() + frequencies.len()) * size_of::<f64>()) as u64;
    let mut reservation = reserve(session, 2 * bytes + 256)?;
    let mut columns = Vec::with_capacity(coefficients.len());
    for fit in 0..frequencies.len() {
        for power in 0..coefficients.shape()[0] {
            columns.push(coefficients[[power, fit]]);
        }
    }
    let pattern = BeamPattern::frequency_inverse_polynomial(
        radius,
        columns,
        frequencies.iter().copied().collect(),
        voltage,
    )
    .ok_or_else(|| raised("invalid inverse-polynomial fit matrix or frequencies"))?;
    reservation.retain(bytes + 256);
    Ok(BeamSource::Tabulated(Arc::new(Admitted {
        value: pattern,
        reservation,
    })))
}

fn text<'a>(record: &'a RecordValue, field: &str) -> SimulatorResult<&'a str> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value),
        _ => Err(raised(format!("missing string {field}"))),
    }
}

fn boolean(record: &RecordValue, field: &str) -> SimulatorResult<bool> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::Bool(value))) => Ok(*value),
        _ => Err(raised(format!("missing boolean {field}"))),
    }
}

fn quantity(record: &RecordValue, field: &str, unit: &str) -> SimulatorResult<f64> {
    let Some(Value::Record(value)) = record.get(field) else {
        return Err(raised(format!("missing quantity {field}")));
    };
    let Some(Value::Scalar(ScalarValue::Float64(number))) = value.get("value") else {
        return Err(raised(format!("non-scalar quantity {field}")));
    };
    Quantity::new(*number, text(value, "unit")?)
        .and_then(|q| q.get_value_in(&Unit::new(unit)?))
        .map_err(raised)
}

fn from_record(record: &RecordValue) -> SimulatorResult<PrimaryBeam> {
    let name = text(record, "name")?;
    if !matches!(name, "AIRY" | "GAUSS") {
        return Err(SimulatorError::not_yet_implemented(format!(
            "simulation voltage-pattern table shape {name} (#1297)"
        )));
    }
    radial_pattern(record).map_err(raised)
}
