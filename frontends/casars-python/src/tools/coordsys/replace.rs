// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_types::Value;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::tools::table::casa_record;

use super::construction::restore;
use super::inspect::numbered_coordinates;
use super::{CoordinateState, PyCoordsys, index_argument};

fn raised(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn found_index(index: i32, count: usize, argument: &str) -> PyResult<usize> {
    usize::try_from(index)
        .ok()
        .filter(|&index| index < count)
        .ok_or_else(|| raised(format!("Illegal index {index} for {argument} coordinate")))
}

fn n_axes(model: &CoordinateModel) -> (usize, usize) {
    (model.n_pixel_axes(), model.n_world_axes())
}

fn replace_coordinate(
    target: &mut CoordinateState,
    source: &CoordinateState,
    input: i32,
    output: i32,
) -> PyResult<bool> {
    let inputs = numbered_coordinates(source)?;
    let input = found_index(input, inputs.len(), "input")?;
    let source_field = &inputs[input].2;
    let replacement = source_field.model(&source.record).map_err(raised)?;
    let source_record = source
        .record
        .get(&source_field.field)
        .expect("source coordinate record");
    replace_at(target, output, replacement, source_record.clone())?;
    Ok(true)
}

pub(super) fn replace_at(
    target: &mut CoordinateState,
    output: i32,
    replacement: CoordinateModel,
    source_record: Value,
) -> PyResult<()> {
    let outputs = numbered_coordinates(target)?;
    let output = found_index(output, outputs.len(), "output")?;
    let destination_field = &outputs[output].2;
    if n_axes(&replacement)
        != (
            destination_field.pixel_axes.len(),
            destination_field.world_axes.len(),
        )
    {
        return Err(raised("Coordinates must have the same number of axes"));
    }

    // Replace the complete local coordinate, retaining either signed axis map.
    // Restore full models even if the reader compacted a hidden linear axis.
    let mut coordinates = CoordinateSystem::from_mapped_record(&target.record).map_err(raised)?;
    for (index, (_, live, _)) in outputs.iter().enumerate() {
        if index != output && n_axes(live) == n_axes(coordinates.coordinate(index)) {
            coordinates.replace_coordinate(index, (*live).clone());
        }
    }
    let prefix = replacement.coordinate_type().record_prefix();
    coordinates.replace_coordinate(output, replacement);

    let mut record = target.record.clone();
    record.remove(&destination_field.field);
    record.upsert(format!("{prefix}{output}"), source_record);
    let field = format!("worldreplace{output}");
    let mapped = coordinates.to_record();
    let Some(Value::Array(replacements)) = mapped.get(&field) else {
        return Err(raised("Missing world replacement values"));
    };
    record.upsert(field, Value::Array(replacements.clone()));
    target.coordinates = coordinates;
    target.record = record;
    Ok(())
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (csys, whichin, whichout), text_signature = "($self, csys, whichin, whichout)")]
    fn replace(
        &self,
        py: Python<'_>,
        csys: &Bound<'_, PyAny>,
        whichin: &Bound<'_, PyAny>,
        whichout: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let csys = csys
            .cast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("_csys is not a dictionary"))?;
        let input = index_argument(whichin, "whichin")?;
        let output = index_argument(whichout, "whichout")?;
        let record = casa_record(py, Some(csys))?;
        py.detach(|| {
            let source = restore(record, "unknown".into())?;
            replace_coordinate(&mut self.state(), &source, input, output)
        })
    }
}
