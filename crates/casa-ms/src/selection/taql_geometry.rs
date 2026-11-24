// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observer and antenna geometry used by the MSCAL TaQL predicates.

use std::f64::consts::{PI, TAU};
use std::sync::{Arc, Mutex};

use casa_measures_data::PlanetaryEphemeris;
use casa_tables::taql::ast::Expr;
use casa_tables::taql::eval::{ArrayValue, EvalContext, ExprValue, eval_constant};
use casa_tables::taql::{BoundRowFunction, IndexStyle, TaqlError, UvwWavelengthScale};
use casa_types::measures::baseline::{baseline_rotation, j2000_baseline_uvw};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::EpochRef;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::position::MPosition;
use casa_types::measures::{MeasureError, MeasuresProvider};
use casa_types::quanta::{Quantity, Unit, constants};
use casa_types::{ScalarValue, Value};

use super::taql_selection::taql_error;
use crate::subtables::SubTable;
use crate::subtables::measured_columns::{self as measured, TimeColumn};
use crate::{MeasurementSet, MsError, MsResult};

pub(super) struct GeometryBinder<'a> {
    ms: &'a MeasurementSet,
    geometry: Option<Arc<Geometry>>,
}

impl<'a> GeometryBinder<'a> {
    pub(super) fn new(ms: &'a MeasurementSet) -> Self {
        Self { ms, geometry: None }
    }

    pub(super) fn bind(
        &mut self,
        name: &str,
        args: &[Expr],
        style: IndexStyle,
    ) -> MsResult<Option<Expr>> {
        let lower = name.to_ascii_lowercase();
        let Some(function) = lower
            .strip_prefix("mscal.")
            .or_else(|| lower.strip_prefix("derivedmscal."))
        else {
            return Ok(None);
        };
        let Some((kind, site)) = Kind::parse(function) else {
            return Ok(None);
        };
        if args.len() > 1 {
            return Err(taql_error(TaqlError::ArgumentCount {
                name: name.to_owned(),
                expected: "zero or one".into(),
                got: args.len(),
            }));
        }
        let geometry = match &self.geometry {
            Some(geometry) => Arc::clone(geometry),
            None => {
                let geometry = Arc::new(Geometry::read(self.ms)?);
                self.geometry = Some(Arc::clone(&geometry));
                geometry
            }
        };
        let unit = Unit::new(match kind {
            Kind::Delay => "s",
            Kind::ProjectedUvw { .. } => "m",
            _ => "rad",
        })
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let argument = args
            .first()
            .map(|arg| eval_constant(arg, name, style))
            .transpose()
            .map_err(taql_error)?;
        let source = Source::read(self.ms, argument, kind, &geometry.measures)?;
        let scale = match kind {
            Kind::ProjectedUvw {
                wavelengths: Some(per_channel),
                ..
            } => Some(
                UvwWavelengthScale::read(self.ms.main_table(), per_channel).map_err(taql_error)?,
            ),
            _ => None,
        };
        let displayed = args.first().map(ToString::to_string).unwrap_or_default();
        let bound = BoundRowFunction::new(format!("{name}({displayed})"), move |context| {
            geometry.evaluate(context, kind, site, &source, &unit, scale.as_ref())
        });
        Ok(Some(Expr::BoundRowFunction(bound)))
    }
}

#[derive(Clone, Copy)]
enum Site {
    Array,
    First,
    Second,
}

impl Site {
    fn antenna(self, context: &EvalContext<'_>) -> Result<Option<usize>, TaqlError> {
        let column = match self {
            Self::Array => return Ok(None),
            Self::First => "ANTENNA1",
            Self::Second => "ANTENNA2",
        };
        row_index(context, column).map(Some)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    HourAngle,
    HourAngleDeclination,
    AzimuthElevation,
    ParallacticAngle,
    SiderealTime,
    TerrestrialDirection,
    Delay,
    ProjectedUvw {
        apparent: bool,
        wavelengths: Option<bool>,
    },
}

impl Kind {
    fn parse(function: &str) -> Option<(Self, Site)> {
        for (prefix, apparent) in [("uvwj2000", false), ("uvwapp", true)] {
            if let Some(suffix) = function.strip_prefix(prefix) {
                let wavelengths = match suffix {
                    "" => None,
                    "wvl" => Some(false),
                    "wvls" => Some(true),
                    _ => return None,
                };
                return Some((
                    Self::ProjectedUvw {
                        apparent,
                        wavelengths,
                    },
                    Site::Array,
                ));
            }
        }
        let (name, site) = if let Some(name) = function.strip_suffix('1') {
            (name, Site::First)
        } else if let Some(name) = function.strip_suffix('2') {
            (name, Site::Second)
        } else {
            (function, Site::Array)
        };
        let kind = match name {
            "ha" => Self::HourAngle,
            "hadec" => Self::HourAngleDeclination,
            "azel" => Self::AzimuthElevation,
            "last" => Self::SiderealTime,
            "delay" => Self::Delay,
            "pa" if !matches!(site, Site::Array) => Self::ParallacticAngle,
            "itrf" if matches!(site, Site::Array) => Self::TerrestrialDirection,
            _ => return None,
        };
        Some((kind, site))
    }
}

struct Geometry {
    antennas: Vec<MPosition>,
    alt_az: Vec<bool>,
    observatory: MPosition,
    time: TimeColumn,
    projections: [Mutex<Option<Projection>>; 2],
    measures: Arc<dyn MeasuresProvider>,
}

impl Geometry {
    fn read(ms: &MeasurementSet) -> MsResult<Self> {
        let measures = crate::open_measures_runtime()?;
        let antenna = ms.antenna()?;
        let antennas = measured::antenna_positions(antenna.table())?;
        let mut alt_az = Vec::with_capacity(antenna.row_count());
        for row in 0..antenna.row_count() {
            alt_az.push(
                antenna
                    .mount(row)?
                    .to_ascii_lowercase()
                    .starts_with("alt-az"),
            );
        }
        let telescope = ms
            .observation()
            .ok()
            .and_then(|obs| obs.string(0, "TELESCOPE_NAME").ok());
        let keyword = match ms.main_table().keywords().get("TELESCOPE_NAME") {
            Some(Value::Scalar(ScalarValue::String(name))) => Some(name.as_str()),
            _ => None,
        };
        let observatory = [telescope.as_deref(), keyword]
            .into_iter()
            .flatten()
            .find_map(|name| MPosition::casa_observatory(name, measures.as_ref()))
            .or_else(|| antennas.get(antennas.len() / 2).cloned())
            .ok_or_else(|| {
                MsError::InvalidInput("MSCAL could not resolve the array position".into())
            })?;
        Ok(Self {
            antennas,
            alt_az,
            observatory,
            measures,
            time: TimeColumn::read(ms.main_table())?,
            projections: Default::default(),
        })
    }

    fn evaluate(
        &self,
        context: &EvalContext<'_>,
        kind: Kind,
        site: Site,
        source: &Source,
        unit: &Unit,
        scale: Option<&UvwWavelengthScale>,
    ) -> Result<ExprValue, TaqlError> {
        let epoch = self.time.epoch(context.row)?;
        let antenna = site.antenna(context)?;
        let position = if kind == Kind::Delay {
            &self.observatory
        } else {
            antenna
                .map(|id| self.antenna(id))
                .transpose()?
                .unwrap_or(&self.observatory)
        };
        let frame = MeasFrame::new()
            .with_epoch(epoch.clone())
            .with_position(position.clone())
            .with_measures(Arc::clone(&self.measures));
        let direction = source.direction(context, &frame)?;
        let frame = frame.with_direction(direction.clone());
        let scalar = |value| ExprValue::Quantity(Quantity::with_unit(value, unit.clone()));
        let direction_angles = |reference| -> Result<[f64; 2], TaqlError> {
            let (lon, lat) = direction
                .convert_to(reference, &frame)
                .map_err(measure_error)?
                .as_angles();
            Ok([if lon > PI { lon - TAU } else { lon }, lat])
        };
        let pair = |values: [f64; 2]| {
            ExprValue::Array(ArrayValue {
                shape: vec![2],
                data: values.into_iter().map(scalar).collect(),
            })
        };
        match kind {
            Kind::HourAngle => Ok(scalar(direction_angles(DirectionRef::HADEC)?[0])),
            Kind::HourAngleDeclination => Ok(pair(direction_angles(DirectionRef::HADEC)?)),
            Kind::AzimuthElevation => Ok(pair(direction_angles(DirectionRef::AZEL)?)),
            Kind::TerrestrialDirection => Ok(pair(direction_angles(DirectionRef::ITRF)?)),
            Kind::ParallacticAngle => {
                let rotating = antenna.is_some_and(|id| self.alt_az[id]);
                Ok(scalar(if rotating {
                    direction.parallactic_angle(&frame).map_err(measure_error)?
                } else {
                    0.0
                }))
            }
            // C++ reports the full LAST epoch in days, with TaQL metadata of rad.
            Kind::SiderealTime => Ok(scalar(
                epoch
                    .convert_to(EpochRef::LAST, &frame)
                    .map_err(measure_error)?
                    .value()
                    .as_mjd(),
            )),
            Kind::Delay => Ok(scalar(self.delay(context, antenna, direction, &frame)?)),
            Kind::ProjectedUvw { apparent, .. } => {
                let value = self.projected_uvw(context, &direction, &frame, apparent)?;
                match scale {
                    Some(scale) => scale.apply(context, value),
                    None => Ok(ExprValue::Array(ArrayValue {
                        shape: vec![3],
                        data: value.into_iter().map(scalar).collect(),
                    })),
                }
            }
        }
    }

    fn projected_uvw(
        &self,
        context: &EvalContext<'_>,
        direction: &MDirection,
        frame: &MeasFrame,
        apparent: bool,
    ) -> Result<[f64; 3], TaqlError> {
        let first = self.antenna(row_index(context, "ANTENNA1")?)?.as_itrf();
        let second = self.antenna(row_index(context, "ANTENNA2")?)?.as_itrf();
        let baseline = std::array::from_fn(|axis| second[axis] - first[axis]);
        if baseline == [0.0; 3] {
            return Ok(baseline);
        }
        let epoch = frame
            .epoch()
            .expect("MSCAL projection requires its row epoch");
        let key = ProjectionKey {
            epoch_ref: epoch.refer(),
            day: epoch.value().day(),
            fraction: epoch.value().frac(),
            direction: direction.cosines(),
        };
        let mut cached = self.projections[usize::from(apparent)]
            .lock()
            .map_err(|err| TaqlError::Table(err.to_string()))?;
        if let Some(projection) = cached.as_ref()
            && projection.key == key
        {
            return Ok(projection.apply(baseline, direction));
        }
        let projection = Projection::read(key, frame, apparent)?;
        let uvw = projection.apply(baseline, direction);
        *cached = Some(projection);
        Ok(uvw)
    }

    fn antenna(&self, id: usize) -> Result<&MPosition, TaqlError> {
        self.antennas
            .get(id)
            .ok_or_else(|| TaqlError::Table(format!("MSCAL antenna ID {id} is out of range")))
    }

    fn delay(
        &self,
        context: &EvalContext<'_>,
        antenna: Option<usize>,
        direction: MDirection,
        frame: &MeasFrame,
    ) -> Result<f64, TaqlError> {
        let direction = direction
            .convert_to(DirectionRef::ITRF, frame)
            .map_err(measure_error)?
            .cosines();
        let origin = self.observatory.as_itrf();
        let delay_from_origin = |id| -> Result<f64, TaqlError> {
            let position = self.antenna(id)?.as_itrf();
            Ok((0..3)
                .map(|axis| direction[axis] * (position[axis] - origin[axis]))
                .sum())
        };
        let metres = match antenna {
            Some(id) => delay_from_origin(id)?,
            None => {
                delay_from_origin(row_index(context, "ANTENNA1")?)?
                    - delay_from_origin(row_index(context, "ANTENNA2")?)?
            }
        };
        Ok(metres / constants::c().value())
    }
}

/// The position is the query's fixed observatory. Keep one entry per
/// projection so a comparison of J2000 and APP can reuse both rotations.
#[derive(PartialEq)]
struct ProjectionKey {
    epoch_ref: EpochRef,
    day: f64,
    fraction: f64,
    direction: [f64; 3],
}

struct Projection {
    key: ProjectionKey,
    to_j2000: [[f64; 3]; 3],
    to_apparent: Option<[[f64; 3]; 3]>,
}

impl Projection {
    fn read(key: ProjectionKey, frame: &MeasFrame, apparent: bool) -> Result<Self, TaqlError> {
        let to_j2000 = baseline_rotation(DirectionRef::ITRF, DirectionRef::J2000, frame, false)
            .map_err(measure_error)?;
        let to_apparent = apparent
            .then(|| baseline_rotation(DirectionRef::J2000, DirectionRef::APP, frame, true))
            .transpose()
            .map_err(measure_error)?;
        Ok(Self {
            key,
            to_j2000,
            to_apparent,
        })
    }

    fn apply(&self, baseline: [f64; 3], direction: &MDirection) -> [f64; 3] {
        let uvw = j2000_baseline_uvw(rotate(self.to_j2000, baseline), direction);
        self.to_apparent.map_or(uvw, |matrix| rotate(matrix, uvw))
    }
}

enum Source {
    Field(Vec<MDirection>),
    Fixed(MDirection),
    Named(String),
    Planet(Box<PlanetaryEphemeris>),
}

impl Source {
    fn read(
        ms: &MeasurementSet,
        argument: Option<ExprValue>,
        kind: Kind,
        measures: &Arc<dyn MeasuresProvider>,
    ) -> MsResult<Self> {
        match argument {
            None => Self::field(
                ms,
                if kind == Kind::Delay {
                    "DELAY_DIR"
                } else {
                    "PHASE_DIR"
                },
            ),
            Some(ExprValue::String(name)) => {
                if let Some(column) = name.strip_prefix('\\') {
                    return Self::field(ms, column);
                }
                if name.eq_ignore_ascii_case("PLUTO") {
                    let planet = PlanetaryEphemeris::open_discovered(&name)
                        .map_err(|error| MsError::MeasuresRuntime(error.to_string()))?;
                    return Ok(Self::Planet(Box::new(planet)));
                }
                let frame = MeasFrame::new().with_measures(Arc::clone(measures));
                match MDirection::check_source_name(&name, &frame) {
                    Ok(()) => Ok(Self::Named(name)),
                    Err(MeasureError::UnknownSourceName { .. }) => Self::field(ms, &name),
                    Err(error) => Err(MsError::InvalidInput(error.to_string())),
                }
            }
            Some(ExprValue::Array(values)) if values.data.len() == 2 => {
                let radians =
                    Unit::new("rad").map_err(|error| MsError::InvalidInput(error.to_string()))?;
                let angle = |value: &ExprValue| match value {
                    ExprValue::Float(angle) => Ok(*angle),
                    ExprValue::Quantity(angle) => angle
                        .get_value_in(&radians)
                        .map_err(|error| MsError::InvalidInput(error.to_string())),
                    _ => Err(MsError::InvalidInput(
                        "MSCAL direction requires two floating-point angles".into(),
                    )),
                };
                Ok(Self::Fixed(MDirection::from_angles(
                    angle(&values.data[0])?,
                    angle(&values.data[1])?,
                    DirectionRef::J2000,
                )))
            }
            _ => Err(MsError::InvalidInput(
                "MSCAL direction requires a FIELD column, source name, or two angles".into(),
            )),
        }
    }

    fn field(ms: &MeasurementSet, column: &str) -> MsResult<Self> {
        let field = ms.field()?;
        Ok(Self::Field(measured::directions(field.table(), column)?))
    }

    fn direction(
        &self,
        context: &EvalContext<'_>,
        frame: &MeasFrame,
    ) -> Result<MDirection, TaqlError> {
        let direction = match self {
            Self::Fixed(direction) => direction.clone(),
            Self::Planet(planet) => planet
                .direction(frame)
                .map_err(|error| TaqlError::Table(error.to_string()))?,
            Self::Named(name) => {
                MDirection::from_source_name(name, frame).map_err(measure_error)?
            }
            Self::Field(directions) => {
                let id = row_index(context, "FIELD_ID")?;
                directions
                    .get(id)
                    .ok_or_else(|| {
                        TaqlError::Table(format!("MSCAL field ID {id} is out of range"))
                    })?
                    .clone()
            }
        };
        direction
            .convert_to(DirectionRef::J2000, frame)
            .map_err(measure_error)
    }
}

fn row_index(context: &EvalContext<'_>, column: &str) -> Result<usize, TaqlError> {
    match context.row.get(column) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => usize::try_from(*value)
            .map_err(|_| TaqlError::Table(format!("MSCAL has negative {column} {value}"))),
        _ => Err(TaqlError::Table(format!(
            "MSCAL requires an integer {column} column"
        ))),
    }
}

fn measure_error(error: MeasureError) -> TaqlError {
    TaqlError::Table(error.to_string())
}

fn rotate(matrix: [[f64; 3]; 3], vector: [f64; 3]) -> [f64; 3] {
    matrix.map(|row| row.into_iter().zip(vector).map(|(a, b)| a * b).sum())
}
