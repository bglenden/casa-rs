// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulator::getVPRecord and VPManager saved-table selection.
use std::{path::Path, sync::Arc};

use casa_imaging_runtime::{Admitted, Demand, Reservation, admit};
use casa_tables::{Table, TableOptions};
use casa_types::{
    RecordValue, ScalarValue, Value,
    quanta::{Quantity, Unit},
};

use super::{
    Simulator, VoltagePatternSpec,
    error::{SimulatorError, SimulatorResult},
};
use crate::predict::{BeamPattern, PrimaryBeam};

#[derive(Clone)]
pub(crate) enum BeamSource {
    Configured(PrimaryBeam),
    Tabulated(Arc<Admitted<BeamPattern>>),
}

impl std::fmt::Debug for BeamSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configured(beam) => std::fmt::Debug::fmt(beam, f),
            Self::Tabulated(_) => f.write_str("saved radial voltage curve"),
        }
    }
}

impl BeamSource {
    pub(crate) fn configured(&self) -> Option<PrimaryBeam> {
        match self {
            Self::Configured(beam) => Some(*beam),
            Self::Tabulated(_) => None,
        }
    }
    pub(crate) fn pattern(&self) -> BeamPattern {
        match self {
            Self::Configured(beam) => beam.pattern(),
            Self::Tabulated(pattern) => pattern.value.clone(),
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

fn table_bytes(path: &Path) -> SimulatorResult<u64> {
    let rows = Table::rows_on_disk(path).map_err(raised)? as u64;
    let mut bytes = rows.saturating_mul(4096).saturating_add(64 << 10);
    // Record fields and decoded scalars require more space than their wire
    // representation. Only the two small VPManager tables are inspected.
    for entry in std::fs::read_dir(path).map_err(raised)? {
        let metadata = entry.map_err(raised)?.metadata().map_err(raised)?;
        if metadata.is_file() {
            bytes = bytes.saturating_add(metadata.len().saturating_mul(4));
        }
    }
    Ok(bytes)
}

/// Preserve how VPManager resolved the record: standard VLA patterns carry
/// band squint, while custom records choose common-beam mosaic kernels.
#[derive(Clone)]
pub(super) struct ResolvedBeam {
    pub(super) pattern: BeamSource,
    pub(super) standard: bool,
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
    let _root_memory = reserve(session, table_bytes(path)?)?;
    let table = Table::open(TableOptions::new(path)).map_err(raised)?;
    let Some(Value::TableRef(reference)) = table.keywords().get("VPLIST_DEFAULTS") else {
        return Err(raised("missing VPLIST_DEFAULTS subtable"));
    };
    let defaults_path = path.join(reference);
    let _defaults_memory = reserve(session, table_bytes(&defaults_path)?)?;
    let defaults = Table::open(TableOptions::new(defaults_path)).map_err(raised)?;
    let telescope = session.observing_telescope();
    let mut selected = None;
    for row in 0..defaults.row_count() {
        let name = defaults
            .cell_accessor(row, "tel_and_anttype")
            .and_then(|cell| cell.scalar())
            .map_err(raised)?;
        if matches!(name, ScalarValue::String(name) if name == telescope) {
            let ScalarValue::Int32(index) = defaults
                .cell_accessor(row, "vplistnum")
                .and_then(|cell| cell.scalar())
                .map_err(raised)?
            else {
                return Err(raised("VPLIST_DEFAULTS.vplistnum is not an integer"));
            };
            selected = Some(*index);
        }
    }
    let selected =
        selected.ok_or_else(|| raised(format!("no default for telescope {telescope}")))?;
    if selected == -1 {
        return PrimaryBeam::for_telescope(telescope, session.reference_frequency_hz())
            .map(|pattern| ResolvedBeam {
                pattern: BeamSource::Configured(pattern),
                standard: true,
            })
            .ok_or_else(|| {
                SimulatorError::not_yet_implemented(format!(
                    "default simulation beam for {telescope} (#1297)"
                ))
            });
    }
    let index = usize::try_from(selected).map_err(raised)?;
    if index >= table.row_count() {
        return Err(raised("VPLIST_DEFAULTS selects an absent beam"));
    }
    let record = table
        .cell_accessor(index, "pbdescription")
        .and_then(|cell| cell.record())
        .map_err(raised)?;
    if session.squint
        && let Some(Value::Record(direction)) = record.get("squintdir")
    {
        let offset =
            casa_types::measures::record::direction_from_record(direction).map_err(raised)?;
        if offset.as_angles() != (0., 0.) {
            return Err(SimulatorError::not_yet_implemented(
                "squinted custom simulation voltage patterns (#1297)",
            ));
        }
    }
    let name = text(&record, "name")?;
    let pattern = if matches!(name, "POLY" | "IPOLY" | "COSPOLY" | "NUMERIC") {
        saved_curve(session, &record)?
    } else {
        BeamSource::Configured(from_record(&record)?)
    };
    Ok(ResolvedBeam {
        pattern,
        standard: false,
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
    let mut reservation = reserve(session, 2 * BeamPattern::BYTES + 256)?;
    let pattern = match text(record, "name")? {
        "POLY" | "IPOLY" => {
            if record.get("fitfreqs").is_some() {
                return Err(SimulatorError::not_yet_implemented(
                    "saved frequency-dependent inverse-polynomial beam records (#1297)",
                ));
            }
            BeamPattern::polynomial(
                radius,
                &numbers(record, "coeff")?,
                text(record, "name")? == "IPOLY",
                voltage,
            )
        }
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
    let ghz = quantity(record, "reffreq", "GHz")?;
    let radius = quantity(record, "maxrad", "arcmin")? * ghz;
    if !ghz.is_finite() || ghz <= 0. || !radius.is_finite() || radius <= 0. {
        return Err(raised(
            "reference frequency and support must be finite and positive",
        ));
    }
    match name {
        "AIRY" => {
            let diameter = quantity(record, "dishdiam", "m")?;
            let blockage = quantity(record, "blockagediam", "m")?;
            if !diameter.is_finite()
                || diameter <= 0.
                || !blockage.is_finite()
                || blockage < 0.
                || blockage >= diameter
            {
                return Err(raised("invalid dish or blockage diameter"));
            }
            Ok(PrimaryBeam::Airy {
                dish_diameter_m: diameter,
                blockage_diameter_m: blockage,
                maximum_radius_arcmin_ghz: radius,
            })
        }
        "GAUSS" => {
            let width = quantity(record, "halfwidth", "arcmin")? * ghz;
            if !width.is_finite() || width <= 0. {
                return Err(raised("Gaussian HWHM must be finite and positive"));
            }
            Ok(PrimaryBeam::Gaussian {
                half_width_arcmin_ghz: width,
                maximum_radius_arcmin_ghz: radius,
                is_voltage: boolean(record, "isthisvp")?,
            })
        }
        name => Err(SimulatorError::not_yet_implemented(format!(
            "simulation voltage-pattern table shape {name} (#1297)"
        ))),
    }
}
