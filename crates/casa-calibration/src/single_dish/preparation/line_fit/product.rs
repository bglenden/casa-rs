// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polarization metadata of the empty fit-line MeasurementSet.

use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use casa_ms::{MsDataDescription, MsDataDescriptionMut, MsPolarization};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Correlation, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{Array1, Array2};

use super::error_text;

pub(super) fn write_polarization(path: &Path) -> Result<(), String> {
    let polarization = path.join("POLARIZATION");
    let descriptions = path.join("DATA_DESCRIPTION");
    let mut pol = Table::open(TableOptions::new(&polarization)).map_err(error_text)?;
    let mut dd = Table::open(TableOptions::new(&descriptions)).map_err(error_text)?;
    let rows = polarization_rows(&MsPolarization::new(&pol))?;
    let stokes_i = Correlation::I.casacore_code();
    let stokes = if let Some(id) = rows
        .iter()
        .position(|(flagged, codes)| !flagged && codes == &[stokes_i])
    {
        id
    } else {
        let id = pol.row_count();
        let field = |name, value| RecordField::new(name, value);
        pol.add_row(RecordValue::new(vec![
            field("NUM_CORR", Value::Scalar(ScalarValue::Int32(1))),
            field(
                "CORR_TYPE",
                Value::Array(ArrayValue::Int32(
                    Array1::from_vec(vec![stokes_i]).into_dyn(),
                )),
            ),
            field(
                "CORR_PRODUCT",
                Value::Array(ArrayValue::Int32(Array2::zeros((2, 1)).into_dyn())),
            ),
            field("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        ]))
        .map_err(error_text)?;
        pol.save(TableOptions::new(&polarization))
            .map_err(error_text)?;
        drop(pol);
        // The copied lock advertises the previous row count to casacore.
        // This private staged subtable has no other readers.
        if let Err(error) = fs::remove_file(polarization.join("table.lock"))
            && error.kind() != ErrorKind::NotFound
        {
            return Err(error_text(error));
        }
        id
    };
    let associations = MsDataDescription::new(&dd);
    let mapped = (0..associations.row_count())
        .map(|row| associations.polarization_id(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let mut changed = false;
    for (row, id) in mapped.into_iter().enumerate() {
        if let Some((flagged, codes)) = rows.get(id as usize)
            && !flagged
            && contains_parallel_pair(codes)
        {
            MsDataDescriptionMut::new(&mut dd)
                .set_i32(row, "POLARIZATION_ID", stokes as i32)
                .map_err(error_text)?;
            changed = true;
        }
    }
    if changed {
        dd.save(TableOptions::new(descriptions))
            .map_err(error_text)?;
    }
    Ok(())
}

fn polarization_rows(pol: &MsPolarization<'_>) -> Result<Vec<(bool, Vec<i32>)>, String> {
    (0..pol.row_count())
        .map(|i| {
            Ok((
                pol.flag_row(i).map_err(error_text)?,
                pol.corr_type(i).map_err(error_text)?,
            ))
        })
        .collect()
}

fn contains_parallel_pair(codes: &[i32]) -> bool {
    [
        [Correlation::XX, Correlation::YY],
        [Correlation::RR, Correlation::LL],
    ]
    .into_iter()
    .any(|pair| {
        pair.into_iter()
            .all(|hand| codes.contains(&hand.casacore_code()))
    })
}
