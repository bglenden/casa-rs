// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's single-component fit to selected corrected visibilities.

mod samples;

use std::f64::consts::{FRAC_PI_2, LN_2, PI, TAU};

use casa_components::{
    ComponentShape, Ellipse, EllipseErrors, Flux, ShapeKind, ShapeTransform, SkyComponent,
    SpectralModel,
};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::ephemeris::offset_true_angle;
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::quanta::Unit;
use casa_core::{Complex64, ScalarValue};
use casa_ms::columns::direction_columns::DirectionColumn;
use casa_ms::derived::engine::raw_field_phase_direction;
use casa_ms::{MeasurementSet, MsError, MsResult, ResolvedMsSelection, SubTable};

const ARCSECOND_RAD: f64 = PI / 648_000.0;
const LIGHT_SPEED: f64 = 299_792_458.0;
const PARAMETERS: usize = 6;

/// Single-component shape supported by CASA visibility fitting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VisibilityFitShape {
    /// Unresolved point source.
    Point,
    /// Elliptical Gaussian.
    Gaussian,
    /// Uniform elliptical disk.
    Disk,
}

impl VisibilityFitShape {
    pub fn parse(name: &str) -> MsResult<Self> {
        match name {
            "P" => Ok(Self::Point),
            "G" => Ok(Self::Gaussian),
            "D" => Ok(Self::Disk),
            _ => Err(invalid("unrecognized visibility component type")),
        }
    }

    fn count(self) -> usize {
        match self {
            Self::Point => 3,
            Self::Gaussian | Self::Disk => PARAMETERS,
        }
    }
}

/// Inputs shared by `uvmodelfit` and `calibrater.modelfit`.
pub struct VisibilityFitRequest<'a> {
    /// Fitted component shape.
    pub shape: VisibilityFitShape,
    /// Stokes I in Jy, east and north offsets in arcseconds, then for resolved
    /// shapes the major axis in arcseconds, minor-to-major ratio and PA in degrees.
    pub initial: &'a [f64],
    /// False entries hold parameters fixed. Omitted entries can vary.
    pub vary: &'a [bool],
    /// Maximum number of accepted fit iterations.
    pub iterations: i64,
}

/// The parameters in the task's units, and the sky component for its table.
pub struct VisibilityComponentFit {
    /// Fitted values in the order and units of [`VisibilityFitRequest::initial`].
    pub parameters: Vec<f64>,
    /// Formal errors from the inverse normal matrix, in the same units.
    pub formal_errors: Vec<f64>,
    /// The fitted component placed on the sky relative to the selected field.
    pub component: SkyComponent,
}

/// Fit one component to the selected visibilities of an open MeasurementSet.
///
/// The fit reads `CORRECTED_DATA` and leaves the selected data unchanged. A
/// negative iteration limit returns the initial component without reading samples.
///
/// # Errors
///
/// Returns an error on unreadable data, multiple selected fields, invalid
/// component parameters or a selection with no data of positive weight.
pub fn fit_visibility_component(
    ms: &MeasurementSet,
    selection: &ResolvedMsSelection,
    request: VisibilityFitRequest<'_>,
) -> MsResult<VisibilityComponentFit> {
    let field = selection
        .selected_rows
        .first()
        .ok_or_else(|| invalid("no rows in the visibility fit selection"))?
        .field_id;
    if selection
        .selected_rows
        .iter()
        .any(|row| row.field_id != field)
    {
        return Err(invalid("visibility model fitting requires a single field"));
    }
    let centre = fit_phase_centre(ms, selection, field as usize)?;
    let count = request.shape.count();
    if request.initial.len() != count || request.initial.iter().any(|number| !number.is_finite()) {
        return Err(invalid(format!(
            "the component needs {count} finite parameter values"
        )));
    }
    if request
        .vary
        .get(count..)
        .is_some_and(|extra| extra.contains(&false))
    {
        return Err(invalid("varypar selects a parameter outside the component"));
    }
    let mut values = [0.0; PARAMETERS];
    values[..count].copy_from_slice(request.initial);
    values[5] = values[5].to_radians();
    let vary =
        std::array::from_fn(|index| index < count && request.vary.get(index) != Some(&false));
    let model = FitModel {
        shape: request.shape,
        values,
    };
    let (fitted, equations) = if request.iterations < 0 {
        // With a negative limit UVMod exports the initial component without reading samples.
        (model, Equations::default())
    } else {
        let input = samples::FitSamples::new(ms, selection)?;
        solve_model(&input, model, vary, request.iterations as usize)?
    };
    let mut parameters = fitted.values[..count].to_vec();
    let mut formal_errors = equations.formal_errors(&vary[..count]);
    if count == PARAMETERS {
        parameters[5] = parameters[5].to_degrees();
        formal_errors[5] = formal_errors[5].to_degrees();
    }
    Ok(VisibilityComponentFit {
        parameters,
        formal_errors,
        component: fitted.component(&centre)?,
    })
}

fn fit_phase_centre(
    ms: &MeasurementSet,
    selection: &ResolvedMsSelection,
    field_id: usize,
) -> MsResult<MDirection> {
    let direction = raw_field_phase_direction(ms, field_id)?;
    let field = ms.field()?;
    if field.num_poly(field_id)? == 0 {
        return Ok(direction);
    }
    // UVMod reads the phase centre of the first buffer in array, field,
    // data-description and time order. Its input has already been reduced to one field.
    let row_ids: Vec<_> = selection.row_indices().collect();
    let arrays = ms
        .main_table()
        .column_accessor("ARRAY_ID")?
        .scalar_cells_owned_for_rows(&row_ids)?;
    let mut first: Option<((i32, i32), f64)> = None;
    for (row, array) in selection.selected_rows.iter().zip(arrays) {
        let Some(ScalarValue::Int32(array)) = array else {
            return Err(invalid("ARRAY_ID is not an Int array identifier"));
        };
        let keys = (array, row.data_desc_id);
        let time = row.time_mjd_seconds;
        if first.is_none_or(|(earliest_keys, earliest_time)| {
            keys < earliest_keys || (keys == earliest_keys && time < earliest_time)
        }) {
            first = Some((keys, time));
        }
    }
    let epoch = first.expect("the fit has selected rows").1;
    let mut measured = DirectionColumn::phase_dir(field.table())
        .get_direction_at(field_id, epoch - field.time(field_id)?)?;
    measured.set_reference(direction.refer());
    Ok(measured)
}

fn solve_model(
    input: &samples::FitSamples<'_>,
    mut model: FitModel,
    vary: [bool; PARAMETERS],
    iterations: usize,
) -> MsResult<(FitModel, Equations)> {
    let mut previous = input.equations(model, &vary)?;
    let mut damping = 0.001;
    let mut accepted = 0;
    // UVMod limits retries as well as accepted steps.
    for _ in 0..101 {
        if accepted >= iterations {
            break;
        }
        let mut hessian = previous.symmetric_hessian(PARAMETERS);
        for index in (0..hessian.len()).step_by(PARAMETERS + 1) {
            hessian[index] *= 1.0 + damping;
        }
        let Some(delta) =
            casa_numerics::solve_symmetric_positive_definite(&hessian, &previous.gradient)
        else {
            damping *= 10.0;
            continue;
        };
        let trial = model.update(delta.try_into().expect("one step per parameter"));
        let attempted = input.equations(trial, &vary)?;
        // UVMod accepts improvements and tolerates rounding at the same chi-square.
        let increase = attempted.chi_square - previous.chi_square;
        if increase < f64::EPSILON {
            model = trial;
            previous = attempted;
            damping *= 0.5;
            accepted += 1;
        } else {
            damping *= 10.0;
        }
    }
    Ok((model, previous))
}

#[derive(Clone, Copy)]
struct FitModel {
    shape: VisibilityFitShape,
    values: [f64; PARAMETERS],
}

impl FitModel {
    fn shape_at(self, direction: MDirection) -> MsResult<ComponentShape> {
        if self.shape == VisibilityFitShape::Point {
            return Ok(ComponentShape::point(direction));
        }
        let major = self.values[3] * ARCSECOND_RAD;
        let axes = Ellipse::new(major, major * self.values[4], self.values[5]).map_err(invalid)?;
        let errors = EllipseErrors::default();
        let kind = match self.shape {
            VisibilityFitShape::Gaussian => ShapeKind::Gaussian { axes, errors },
            VisibilityFitShape::Disk => ShapeKind::Disk { axes, errors },
            VisibilityFitShape::Point => unreachable!("handled above"),
        };
        ComponentShape::new(direction, kind).map_err(invalid)
    }

    fn transform(self) -> MsResult<ShapeTransform> {
        self.shape_at(MDirection::from_angles(0.0, 0.0, DirectionRef::J2000))?
            .transform()
            .map_err(invalid)
    }

    fn component(self, center: &MDirection) -> MsResult<SkyComponent> {
        let direction = offset_true_angle(
            center,
            [
                self.values[1] * ARCSECOND_RAD,
                self.values[2] * ARCSECOND_RAD,
            ],
        )
        .map_err(invalid)?;
        let shape = self.shape_at(direction)?;
        let mut spectrum = SpectralModel::constant(MFrequency::new(1.0e9, FrequencyRef::LSRK));
        spectrum
            .set_frequency_unit(Unit::new("GHz").expect("GHz is a unit"))
            .map_err(invalid)?;
        Ok(SkyComponent::new(
            Flux::stokes_jy(self.values[0], 0.0, 0.0, 0.0),
            shape,
            spectrum,
        ))
    }

    fn update(self, mut delta: [f64; PARAMETERS]) -> Self {
        if self.shape != VisibilityFitShape::Point {
            for index in [3, 4] {
                if delta[index] < -self.values[index] {
                    delta[index] = -0.9 * self.values[index];
                }
            }
        }
        let mut values = std::array::from_fn(|index| self.values[index] + delta[index]);
        if self.shape != VisibilityFitShape::Point {
            values[4] = values[4].min(1.0);
            if values[5] < -FRAC_PI_2 || values[5] > FRAC_PI_2 {
                values[5] = (values[5] + FRAC_PI_2).rem_euclid(PI) - FRAC_PI_2;
            }
        }
        Self { values, ..self }
    }

    fn derivatives(
        self,
        coordinates: [f64; 3],
        frequency: f64,
        shape: f64,
    ) -> (Complex64, [Complex64; PARAMETERS]) {
        let scale = ARCSECOND_RAD * (TAU / LIGHT_SPEED) * frequency;
        let dx = coordinates[0] * scale;
        let dy = coordinates[1] * scale;
        let unit_flux = Complex64::from_polar(shape, dx * self.values[1] + dy * self.values[2]);
        let predicted = unit_flux * self.values[0];
        let mut slopes = [Complex64::new(0.0, 0.0); PARAMETERS];
        slopes[0] = unit_flux;
        slopes[1] = Complex64::new(0.0, dx) * predicted;
        slopes[2] = Complex64::new(0.0, dy) * predicted;
        if self.shape != VisibilityFitShape::Point {
            self.shape_derivatives(dx / TAU, dy / TAU, predicted, &mut slopes);
        }
        (predicted, slopes)
    }

    fn shape_derivatives(
        self,
        u: f64,
        v: f64,
        predicted: Complex64,
        slopes: &mut [Complex64; PARAMETERS],
    ) {
        let (sin, cos) = self.values[5].sin_cos();
        let x = u * cos - v * sin;
        let y = u * sin + v * cos;
        let ratio = self.values[4];
        let d_major = PI * (x * ratio).hypot(y);
        let radius = self.values[3] * d_major;
        if radius == 0.0 {
            return;
        }
        let slope = match self.shape {
            VisibilityFitShape::Gaussian => -radius / (2.0 * LN_2),
            // UVMod multiplies its disk derivative by the current model.
            VisibilityFitShape::Disk => -2.0 * libm::jn(2, radius) / radius,
            VisibilityFitShape::Point => unreachable!("point components have no axes"),
        };
        let square = (PI * self.values[3]).powi(2);
        let derivative = predicted * slope;
        slopes[3] = derivative * d_major;
        slopes[4] = derivative * (square * ratio * x * x / radius);
        slopes[5] = derivative * (square * (1.0 - ratio * ratio) * x * y / radius);
    }
}

#[derive(Default)]
struct Equations {
    chi_square: f64,
    sum_weight: f64,
    gradient: [f64; PARAMETERS],
    hessian: [[f64; PARAMETERS]; PARAMETERS],
}

impl Equations {
    fn symmetric_hessian(&self, size: usize) -> Vec<f64> {
        let mut matrix = Vec::with_capacity(size * size);
        for row in 0..size {
            for column in 0..size {
                let entry = self.hessian[row.min(column)][row.max(column)];
                matrix.push(if row == column && entry == 0.0 {
                    1.0
                } else {
                    entry
                });
            }
        }
        matrix
    }

    fn formal_errors(&self, vary: &[bool]) -> Vec<f64> {
        let size = vary.len();
        let matrix = self.symmetric_hessian(size);
        let inverse = casa_numerics::invert_symmetric_positive_definite(&matrix, size);
        vary.iter()
            .enumerate()
            .map(|(index, &enabled)| {
                if enabled {
                    inverse
                        .as_ref()
                        .map_or(f64::NAN, |data| data[index * (size + 1)].sqrt())
                } else {
                    0.0
                }
            })
            .collect()
    }

    fn add(
        &mut self,
        residual: Complex64,
        slopes: &[Complex64; PARAMETERS],
        weight: f64,
        vary: &[bool; PARAMETERS],
    ) {
        self.chi_square += weight * residual.norm_sqr();
        self.sum_weight += weight;
        for (parameter, &enabled) in vary.iter().enumerate() {
            if enabled {
                let slope = slopes[parameter];
                self.gradient[parameter] += weight * (residual * slope.conj()).re;
                for column in parameter..PARAMETERS {
                    if vary[column] {
                        self.hessian[parameter][column] +=
                            weight * (slope * slopes[column].conj()).re;
                    }
                }
            }
        }
    }
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
