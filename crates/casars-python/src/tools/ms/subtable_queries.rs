// SPDX-License-Identifier: LGPL-3.0-or-later
//! FIELD and spectral-window metadata on the current MS view.

use std::collections::BTreeSet;

use casa_ms::subtables::{
    FieldDirectionColumn, MetadataDirection, MsDataDescription, MsField, MsPolarization,
    MsSpectralWindow,
};
use casa_ms::{MeasurementSet, MsError, MsResult, SubtableId};
use casa_tables::{Table, TableOptions};
use casa_types::ScalarValue;
use pyo3::prelude::*;

use super::{AttachedMs, PyMsTool, runtime_error};
use crate::convert::value::CasaValue;
use crate::tools::msmetadata::direction_record;
use crate::tools::{LogLine, emit};

impl AttachedMs {
    fn field_direction(&self, column: &str, field: i64, time: f64) -> MsResult<MetadataDirection> {
        let column = match column.to_ascii_uppercase().as_str() {
            "DELAY_DIR" => FieldDirectionColumn::Delay,
            "PHASE_DIR" => FieldDirectionColumn::Phase,
            "REFERENCE_DIR" => FieldDirectionColumn::Reference,
            "EPHEMERIS_DIR" => FieldDirectionColumn::Ephemeris,
            _ => {
                return Err(MsError::InvalidInput(format!(
                    "Illegal FIELD direction column name: {column}"
                )));
            }
        };
        let field = usize::try_from(field)
            .map_err(|_| MsError::InvalidInput(format!("Invalid FIELD row {field}")))?;
        let table = MeasurementSet::open_linked_subtable(&self.main, SubtableId::Field)?;
        MsField::new(&table).metadata_direction_at_seconds(field, column, time)
    }

    fn spectral_window_info(&self) -> MsResult<CasaValue> {
        let dd = MeasurementSet::open_linked_subtable(&self.main, SubtableId::DataDescription)?;
        let spw = MeasurementSet::open_linked_subtable(&self.main, SubtableId::SpectralWindow)?;
        let pol = MeasurementSet::open_linked_subtable(&self.main, SubtableId::Polarization)?;
        let (dd, spw, pol) = (
            MsDataDescription::new(&dd),
            MsSpectralWindow::new(&spw),
            MsPolarization::new(&pol),
        );
        if dd.row_count() == 0 || spw.row_count() == 0 {
            return Ok(CasaValue::empty_record());
        }

        let mut records = Vec::new();
        for id in self.used_data_descriptions()? {
            let spw_id = nonnegative_id(dd.spectral_window_id(id)?, "SPECTRAL_WINDOW_ID")?;
            let pol_id = nonnegative_id(dd.polarization_id(id)?, "POLARIZATION_ID")?;
            let chan_freq = spw.chan_freq(spw_id)?;
            let chan_width = spw.chan_width(spw_id)?;
            let (Some(&freq), Some(&width)) = (chan_freq.first(), chan_width.first()) else {
                return Err(MsError::InvalidInput(format!(
                    "Spectral window {spw_id} has no first channel frequency or width"
                )));
            };
            let record = CasaValue::record([
                ("SpectralWindowId", CasaValue::Int(spw_id as i64)),
                ("NumChan", CasaValue::Int(spw.num_chan(spw_id)?.into())),
                (
                    "Frame",
                    CasaValue::Text(spw.reference_frequency_reference(spw_id)?.as_str().into()),
                ),
                ("Chan1Freq", CasaValue::Float(freq)),
                ("ChanWidth", CasaValue::Float(width)),
                ("TotalWidth", CasaValue::Float(spw.total_bandwidth(spw_id)?)),
                ("RefFreq", CasaValue::Float(spw.ref_frequency(spw_id)?)),
                ("PolId", CasaValue::Int(pol_id as i64)),
                ("NumCorr", CasaValue::Int(pol.num_corr(pol_id)?.into())),
            ]);
            records.push((id.to_string(), record));
        }
        Ok(CasaValue::record(records))
    }

    fn used_data_descriptions(&self) -> MsResult<BTreeSet<usize>> {
        const ROWS_PER_BATCH: usize = 128 * 1024;
        // A different tool may have published additional MAIN rows since open.
        let main = Table::open(TableOptions::new(&self.path))?;
        let column = main.column_accessor("DATA_DESC_ID")?;
        let selection = self.selected_rows.as_deref();
        let size = selection.map_or(main.row_count(), <[usize]>::len);
        let mut row_numbers = Vec::with_capacity(size.min(ROWS_PER_BATCH));
        let mut ids = BTreeSet::new();
        for start in (0..size).step_by(ROWS_PER_BATCH) {
            let end = start.saturating_add(ROWS_PER_BATCH).min(size);
            let rows = match selection {
                Some(selection) => &selection[start..end],
                None => {
                    row_numbers.clear();
                    row_numbers.extend(start..end);
                    &row_numbers
                }
            };
            for value in column.scalar_cells_owned_for_rows(rows)? {
                let Some(ScalarValue::Int32(id)) = value else {
                    return Err(MsError::InvalidInput("Invalid MAIN.DATA_DESC_ID".into()));
                };
                ids.insert(nonnegative_id(id, "DATA_DESC_ID")?);
            }
        }
        Ok(ids)
    }
}

fn nonnegative_id(id: i32, name: &str) -> MsResult<usize> {
    usize::try_from(id).map_err(|_| MsError::InvalidInput(format!("Invalid {name} {id}")))
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (dircolname = "PHASE_DIR", fieldid = 0, time = 0.0, format = "measure"), text_signature = "($self, dircolname='PHASE_DIR', fieldid=0, time=0.0, format='measure')")]
    fn getfielddirmeas(
        &self,
        py: Python<'_>,
        dircolname: &str,
        fieldid: i64,
        time: f64,
        format: &str,
    ) -> PyResult<Py<PyAny>> {
        let direction = self.attachment_info(py, |attached| {
            Ok(attached.map(|ms| ms.field_direction(dircolname, fieldid, time)))
        })?;
        let value = match direction {
            Some(Ok(direction)) if format.starts_with(['s', 'S']) => {
                let [x, y, z] = direction.direction.cosines().map(casacore_direction_number);
                CasaValue::Text(format!("Direction: [{x}, {y}, {z}]"))
            }
            Some(Ok(direction)) => direction_record(direction),
            Some(Err(error)) => {
                emit(py, "casars.tools.ms", &[LogLine::error(error.to_string())])?;
                empty_direction()
            }
            None => empty_direction(),
        };
        value.into_python(py)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn getspectralwindowinfo(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.attachment_info(py, |attached| match attached {
            Some(ms) => ms.spectral_window_info().map_err(runtime_error),
            None => Ok(CasaValue::empty_record()),
        })?
        .into_python(py)
    }
}

fn empty_direction() -> CasaValue {
    CasaValue::Bools {
        shape: vec![0],
        values: Vec::new(),
    }
}

// casacore prints direction cosines with default C++ stream formatting,
// which chooses fixed or scientific notation at six significant digits.
fn casacore_direction_number(value: f64) -> String {
    let scientific = format!("{value:.5e}");
    let Some((digits, exponent)) = scientific.split_once('e') else {
        return scientific.to_ascii_lowercase();
    };
    let exponent = exponent
        .parse::<i32>()
        .expect("Rust's finite float exponent");
    if (-4..6).contains(&exponent) {
        let precision = (5 - exponent) as usize;
        let fixed = format!("{value:.precision$}");
        if fixed.contains('.') {
            return fixed.trim_end_matches('0').trim_end_matches('.').to_owned();
        }
        return fixed;
    }
    let digits = digits.trim_end_matches('0').trim_end_matches('.');
    format!("{digits}e{exponent:+03}")
}
