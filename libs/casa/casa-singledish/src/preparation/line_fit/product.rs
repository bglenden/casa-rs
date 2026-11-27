// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polarization metadata of the empty fit-line MeasurementSet.

use std::path::Path;

use casa_core::Correlation;
use casa_ms::{MsDataDescription, MsDataDescriptionMut, MsPolarization};
use casa_mstransform::ensure_stokes_i_polarization;
use casa_tables::{Table, TableOptions};

use super::error_text;

pub(super) fn write_polarization(path: &Path) -> Result<(), String> {
    let polarization = path.join("POLARIZATION");
    let descriptions = path.join("DATA_DESCRIPTION");
    let pol = Table::open(TableOptions::new(&polarization)).map_err(error_text)?;
    let mut dd = Table::open(TableOptions::new(&descriptions)).map_err(error_text)?;
    let rows = polarization_rows(&MsPolarization::new(&pol))?;
    drop(pol);
    let stokes = ensure_stokes_i_polarization(&polarization).map_err(error_text)?;
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
