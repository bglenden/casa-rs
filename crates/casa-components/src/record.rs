// SPDX-License-Identifier: LGPL-3.0-or-later
//! A component's and a list's records, as CASA's `toRecord` forms them
//! (`SkyCompRep::toRecord`, `ComponentList::toRecord`): what the `cl` tool's
//! `getcomponent` and `torecord` return.
//!
//! | record | fields |
//! |---|---|
//! | list | `nelements`, then `component0`, `component1`, … |
//! | component | `flux`, `shape`, `spectrum`, `optionalParameters` (when there are any), `label` |
//! | flux | `value` and `unit` (a `QuantumHolder` of the four values: real for Stokes, complex otherwise), `polarisation`, `error` (real for Stokes, complex otherwise) |
//! | shape | `type`, `direction` (a `MeasureHolder` direction record with an `error` record of `longitude` and `latitude` quantities); a two-sided shape adds `majoraxis`, `minoraxis`, `positionangle` and the three `…error`s |
//! | spectrum | the `Spectral_Record` form ([`SpectralModel::to_record`]) |
//!
//! CASA records a shape's axes in the units it holds them in: those given
//! for a component it built in memory, and for one read from a table arcmin
//! (axes) and degrees (position angle), with the errors in radians. casa-rs
//! holds radians and records every component as CASA records one read from
//! a table; the quantities are the same.

use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use num_complex::Complex64;

use crate::component::SkyComponent;
use crate::component_type::Polarisation;
use crate::error::ComponentError;
use crate::flux::Flux;
use crate::list::ComponentList;
use crate::shape::ComponentShape;
use crate::spectrum::SpectralModel;

/// Radians in an arcminute.
const ARCMIN_RAD: f64 = std::f64::consts::PI / 10_800.0;

/// Radians in a degree.
const DEGREE_RAD: f64 = std::f64::consts::PI / 180.0;

impl ComponentList {
    /// The list's record (`ComponentList::toRecord`): `nelements` and each
    /// component's record as `component<i>`.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Spectrum`] for a tabular spectrum CASA could not
    /// write (see [`SpectralModel::to_record`]).
    pub fn to_record(&self) -> Result<RecordValue, ComponentError> {
        let mut fields = vec![field(
            "nelements",
            Value::Scalar(ScalarValue::Int32(
                i32::try_from(self.len()).unwrap_or(i32::MAX),
            )),
        )];
        for (index, component) in self.iter().enumerate() {
            fields.push(field(
                &format!("component{index}"),
                Value::Record(component.to_record()?),
            ));
        }
        Ok(RecordValue::new(fields))
    }
}

impl SkyComponent {
    /// The component's record (`SkyCompRep::toRecord`).
    ///
    /// # Errors
    ///
    /// [`ComponentError::Spectrum`] for a tabular spectrum CASA could not
    /// write (see [`SpectralModel::to_record`]).
    pub fn to_record(&self) -> Result<RecordValue, ComponentError> {
        let mut fields = vec![
            field("flux", Value::Record(self.flux().to_record())),
            field("shape", Value::Record(self.shape().to_record())),
            field("spectrum", Value::Record(self.spectrum().to_record()?)),
        ];
        if !self.optional_parameters().is_empty() {
            fields.push(field(
                "optionalParameters",
                reals(self.optional_parameters().to_vec()),
            ));
        }
        fields.push(field("label", string(self.label())));
        Ok(RecordValue::new(fields))
    }
}

impl Flux {
    /// The flux's record (`FluxRep::toRecord`): the values and errors are
    /// real for Stokes and complex in a circular or linear basis.
    pub fn to_record(&self) -> RecordValue {
        let stokes = self.polarisation() == Polarisation::Stokes;
        let array = |values: &[Complex64; 4]| {
            if stokes {
                reals(values.iter().map(|value| value.re).collect())
            } else {
                Value::Array(ArrayValue::from_complex64_vec(values.to_vec()))
            }
        };
        RecordValue::new(vec![
            field("value", array(self.values())),
            field("unit", string(self.unit().name())),
            field("polarisation", string(self.polarisation().casa_name())),
            field("error", array(self.errors())),
        ])
    }
}

impl ComponentShape {
    /// The shape's record (`ComponentShape::toRecord`, and
    /// `TwoSidedShape::toRecord` for the shapes with axes), in the units CASA
    /// holds a shape read from a table in (see the module documentation).
    pub fn to_record(&self) -> RecordValue {
        let error = self.direction_error();
        let [longitude, latitude] = crate::table::casacore_angles(self.reference_direction());
        let direction = RecordValue::new(vec![
            field("type", string("direction")),
            field("refer", string(self.reference_direction().refer().as_str())),
            field("m0", Value::Record(quantity(longitude, "rad"))),
            field("m1", Value::Record(quantity(latitude, "rad"))),
            field(
                "error",
                Value::Record(RecordValue::new(vec![
                    field("longitude", Value::Record(quantity_of(error.longitude()))),
                    field("latitude", Value::Record(quantity_of(error.latitude()))),
                ])),
            ),
        ]);
        let mut fields = vec![
            field("type", string(self.shape_type().casa_name())),
            field("direction", Value::Record(direction)),
        ];
        if let Some((axes, errors)) = self.kind().axes() {
            fields.extend([
                field(
                    "majoraxis",
                    Value::Record(quantity(axes.major_axis() / ARCMIN_RAD, "arcmin")),
                ),
                field(
                    "minoraxis",
                    Value::Record(quantity(axes.minor_axis() / ARCMIN_RAD, "arcmin")),
                ),
                field(
                    "positionangle",
                    Value::Record(quantity(axes.position_angle() / DEGREE_RAD, "deg")),
                ),
                field(
                    "majoraxiserror",
                    Value::Record(quantity(errors.major_axis, "rad")),
                ),
                field(
                    "minoraxiserror",
                    Value::Record(quantity(errors.minor_axis, "rad")),
                ),
                field(
                    "positionangleerror",
                    Value::Record(quantity(errors.position_angle, "rad")),
                ),
            ]);
        }
        RecordValue::new(fields)
    }
}

impl SpectralModel {
    /// The model's record (`SpectralModel::toRecord`), as a component list's
    /// `Spectral_Record` column holds it.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Spectrum`] for a tabular spectrum whose reference
    /// frequency CASA could not express in its tabulation frame.
    pub fn to_record(&self) -> Result<RecordValue, ComponentError> {
        crate::table::spectral_record(self)
    }
}

/// A `QuantumHolder` record.
fn quantity(value: f64, unit: &str) -> RecordValue {
    RecordValue::new(vec![
        field("value", real(value)),
        field("unit", string(unit)),
    ])
}

fn quantity_of(value: &casa_types::quanta::Quantity) -> RecordValue {
    quantity(value.value(), value.unit().name())
}

fn field(name: &str, value: Value) -> RecordField {
    RecordField::new(name, value)
}

fn string(text: &str) -> Value {
    Value::Scalar(ScalarValue::String(text.to_string()))
}

fn real(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn reals(values: Vec<f64>) -> Value {
    Value::Array(ArrayValue::from_f64_vec(values))
}
