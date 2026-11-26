// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateType, Projection, ProjectionType};
use casa_core::Value;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::DoubleList;

use super::conversion_frames::FrameName;
use super::frame_metadata::{PROJECTIONS, missing};
use super::inspect::numbered_coordinates;
use super::replace::replace_at;
use super::{CoordinateState, PyCoordsys};

fn raised(message: impl ToString) -> PyErr {
    PyRuntimeError::new_err(message.to_string())
}

pub(super) fn direction_projection(name: &str, mut parameters: Vec<f64>) -> PyResult<Projection> {
    let name = if name == "GLS" { "SFL" } else { name };
    let (_, count) = PROJECTIONS
        .iter()
        .find(|(known, _)| *known == name)
        .ok_or_else(|| raised("Projection::nParameters() - unknown projection"))?;
    if parameters.is_empty() && matches!(name, "COP" | "COD" | "COE" | "COO" | "BON") {
        return Err(raised(
            "Projection::validate() - there are missingobligatory parameters",
        ));
    }
    let kind = ProjectionType::from_name(name)
        .ok_or_else(|| raised("Projection::nParameters() - unknown projection"))?;
    if parameters.len() < *count as usize {
        // Casacore resizes this vector without copying the supplied prefix,
        // then fills the missing suffix with the projection's defaults.
        let supplied = parameters.len();
        let mut defaults = if name == "SIN" {
            vec![0.0; 2]
        } else {
            kind.fits_parameter_defaults()[..*count as usize].to_vec()
        };
        defaults[..supplied].fill(0.0);
        parameters = defaults;
    }
    Ok(Projection::with_parameters(kind, parameters))
}

fn apply_projection(
    state: &mut CoordinateState,
    name: &str,
    parameters: Vec<f64>,
) -> PyResult<bool> {
    let (index, _, placement) = numbered_coordinates(state)?
        .into_iter()
        .find(|(_, model, _)| matches!(model, CoordinateModel::Direction(_)))
        .ok_or_else(|| missing(CoordinateType::Direction))?;
    let CoordinateModel::Direction(direction) = placement.model(&state.record).map_err(raised)?
    else {
        return Err(missing(CoordinateType::Direction));
    };
    let parameters = if parameters == [-1.0] {
        Vec::new()
    } else {
        parameters
    };
    let projection = direction_projection(name, parameters)?;
    let changed = CoordinateModel::from(
        direction
            .with_rebuilt_projection(projection)
            .map_err(raised)?,
    );
    let record = Value::Record(changed.to_record());
    replace_at(state, index as i32, changed, record)?;
    Ok(true)
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (r#type, parameters=DoubleList::of(&[-1.0])), text_signature = "($self, type, parameters=[-1.0])")]
    fn setprojection(
        &self,
        py: Python<'_>,
        r#type: FrameName,
        parameters: DoubleList,
    ) -> PyResult<bool> {
        let name = r#type.into_name("type")?;
        py.detach(|| apply_projection(&mut self.state(), &name, parameters.0))
    }
}
