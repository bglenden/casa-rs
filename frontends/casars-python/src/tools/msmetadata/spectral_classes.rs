// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_core::ScalarValue;
use casa_ms::{MeasurementSet, MsError, MsResult, MsSpectralWindow, SubTable};
use casa_tables::{Table, TableOptions};
use numpy::IntoPyArray;
use pyo3::prelude::*;

use super::PyMsMetadata;
use super::names::Indices;
use super::spectral::{spw_row, water_vapor_windows};
use crate::convert::value::CasaValue;

fn is_square_law_detector(name: &str) -> bool {
    name.match_indices("BB_").any(|(start, _)| {
        let suffix = &name.as_bytes()[start + 3..];
        matches!(suffix, [b'0'..=b'9', rest @ ..] if rest.starts_with(b"#SQLD"))
    })
}

fn has_column(windows: &MsSpectralWindow<'_>, column: &str) -> bool {
    windows
        .table()
        .schema()
        .is_some_and(|schema| schema.contains_column(column))
}

fn polarization_ids(ms: &MeasurementSet, count: usize) -> MsResult<Vec<BTreeSet<i32>>> {
    let descriptions = ms.data_description()?;
    let mut polarizations = vec![BTreeSet::new(); count];
    for row in 0..descriptions.row_count() {
        let spw = descriptions.spectral_window_id(row)?;
        let ids = polarizations
            .get_mut(spw as usize)
            .ok_or_else(|| MsError::InvalidInput(format!("Unknown spectral window ID {spw}")))?;
        ids.insert(descriptions.polarization_id(row)?);
    }
    Ok(polarizations)
}

fn selected_windows(
    ms: &MeasurementSet,
    chavg: bool,
    fdm: bool,
    sqld: bool,
    tdm: bool,
    wvr: bool,
    complement: bool,
) -> MsResult<Vec<i64>> {
    let windows = ms.spectral_window()?;
    let has_baseband = has_column(&windows, "BBC_NO");
    let wvr_ids = if wvr {
        water_vapor_windows(&windows)?
    } else {
        Vec::new()
    };
    let mut pol_ids = None;
    let mut chosen = Vec::new();
    for row in 0..windows.row_count() {
        let mut matched = wvr_ids.binary_search(&row).is_ok();
        if !matched && (chavg || fdm || tdm || (sqld && has_baseband)) {
            let name = windows.name(row)?;
            if sqld && has_baseband && is_square_law_detector(&name) {
                matched = true;
            } else if !name.contains("WVR") {
                let channels = windows
                    .table()
                    .cell_accessor(row, "CHAN_WIDTH")?
                    .array()?
                    .len();
                if channels == 1 && !name.contains("FULL_RES") {
                    matched = chavg;
                } else if fdm || tdm {
                    // ALMA classifications include both normal TDM channel counts and
                    // the counts after filtering edge channels.
                    let mut is_fdm = windows.total_bandwidth(row)? < 2.0e9
                        || channels >= 15
                            && ![256, 128, 64, 32, 16, 248, 124, 62, 31].contains(&channels);
                    if !is_fdm {
                        if pol_ids.is_none() {
                            pol_ids = Some(polarization_ids(ms, windows.row_count())?);
                        }
                        let ids = &pol_ids.as_ref().expect("polarizations were initialized")[row];
                        if ids.len() == 1 {
                            let id = *ids.first().expect("one polarization");
                            let ncorrelations = ms.polarization()?.num_corr(id as usize)?;
                            is_fdm = channels * ncorrelations as usize > 256;
                        }
                    }
                    matched = if is_fdm { fdm } else { tdm };
                }
            }
        }
        if matched != complement {
            chosen.push(row as i64);
        }
    }
    Ok(chosen)
}

fn baseband_number(windows: &MsSpectralWindow<'_>, row: usize) -> MsResult<u32> {
    if let ScalarValue::Int32(value) = windows.table().cell_accessor(row, "BBC_NO")?.scalar()? {
        return Ok(*value as u32);
    }
    Err(MsError::InvalidInput(
        "BBC_NO is not an Int32 column".into(),
    ))
}

fn check_baseband(windows: &MsSpectralWindow<'_>) -> MsResult<()> {
    if !has_column(windows, "BBC_NO") {
        return Err(MsError::InvalidInput(
            "This MS's SPECTRAL_WINDOW table does not have a BBC_NO column".into(),
        ));
    }
    Ok(())
}

fn text_cell<'a>(table: &'a Table, row: usize, column: &str) -> MsResult<&'a str> {
    match table.cell_accessor(row, column)?.scalar()? {
        ScalarValue::String(value) => Ok(value),
        _ => Err(MsError::InvalidInput(format!(
            "{column} is not a string column"
        ))),
    }
}

fn integer_suffix<'a>(name: &'a str, prefix: &str, width: Option<usize>) -> Option<&'a str> {
    name.match_indices(prefix).find_map(|(start, _)| {
        let suffix = &name[start + prefix.len()..];
        let digits = suffix.bytes().take_while(u8::is_ascii_digit).count();
        let needed = width.unwrap_or(digits);
        (needed > 0 && digits >= needed).then(|| &suffix[..needed])
    })
}

fn receiver_bands(path: &Path, count: usize) -> MsResult<Vec<i64>> {
    let path = path.join("ASDM_RECEIVER");
    let mut bands = vec![-1; count];
    if path.is_dir() {
        let table = Table::open(TableOptions::new(&path))?;
        if table.row_count() <= count {
            // CASA uses the first receiver-table entry for repeated windows.
            for row in (0..table.row_count()).rev() {
                let band = text_cell(&table, row, "frequencyBand")?;
                if let Some(band) = integer_suffix(band, "RB_", Some(2)) {
                    let name = text_cell(&table, row, "spectralWindowId")?;
                    let target = integer_suffix(name, "SpectralWindow_", None)
                        .and_then(|window| window.parse::<usize>().ok())
                        .and_then(|window| bands.get_mut(window))
                        .ok_or_else(|| {
                            MsError::InvalidInput(format!(
                                "Unable to find spectral window ID for row {row} in {}",
                                path.display()
                            ))
                        })?;
                    *target = band.parse().expect("two decimal digits");
                }
            }
        }
    }
    Ok(bands)
}

fn selected_name_numbers(
    ms: &MeasurementSet,
    selection: Indices,
    prefix: &str,
    use_receiver_table: bool,
) -> MsResult<Vec<i64>> {
    let windows = ms.spectral_window()?;
    let indices = match selection {
        Indices::EmptyString | Indices::BooleanVector => Indices::Empty,
        selection => selection,
    };
    let mut bands = None;
    indices
        .rows(windows.row_count(), "Spectral window ID")?
        .into_iter()
        .map(|row| {
            let name = windows.name(row)?;
            if let Some(digits) = integer_suffix(&name, prefix, Some(2)) {
                Ok(digits.parse().expect("two decimal digits"))
            } else if use_receiver_table {
                if bands.is_none() {
                    let root = ms.path().expect("an attached MS has a path");
                    bands = Some(receiver_bands(root, windows.row_count())?);
                }
                Ok(bands.as_ref().expect("receiver bands were initialized")[row])
            } else {
                Ok(-1)
            }
        })
        .collect()
}

fn int_array(values: Vec<i64>) -> CasaValue {
    CasaValue::Ints {
        shape: vec![values.len()],
        values,
    }
}

#[pymethods]
impl PyMsMetadata {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (chavg = false, fdm = false, sqld = false, tdm = false, wvr = false, complement = false), text_signature = "($self, chavg=False, fdm=False, sqld=False, tdm=False, wvr=False, complement=False)")]
    fn almaspws(
        &self,
        py: Python<'_>,
        chavg: bool,
        fdm: bool,
        sqld: bool,
        tdm: bool,
        wvr: bool,
        complement: bool,
    ) -> PyResult<Py<PyAny>> {
        let selected = self.with_ms(py, |ms| {
            selected_windows(ms, chavg, fdm, sqld, tdm, wvr, complement)
        })?;
        Ok(selected.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn chanavgspws(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.almaspws(py, true, false, false, false, false, false)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn fdmspws(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.almaspws(py, false, true, false, false, false, false)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn tdmspws(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.almaspws(py, false, false, false, true, false, false)
    }

    #[pyo3(signature = (spw), text_signature = "($self, spw)")]
    fn baseband(&self, py: Python<'_>, spw: i64) -> PyResult<u32> {
        self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let row = spw_row(&windows, spw)?;
            check_baseband(&windows)?;
            baseband_number(&windows, row)
        })
    }

    #[pyo3(signature = (baseband = -1, sqldmode = "include"), text_signature = "($self, baseband=-1, sqldmode='include')")]
    fn spwsforbaseband(
        &self,
        py: Python<'_>,
        baseband: i64,
        sqldmode: &str,
    ) -> PyResult<Py<PyAny>> {
        let selected = self.with_ms(py, |ms| {
            let mode = sqldmode.as_bytes().first().map(u8::to_ascii_lowercase);
            if !matches!(mode, Some(b'i' | b'e' | b'o')) {
                return Err(MsError::InvalidInput(format!(
                    "Unsupported sqldmode {sqldmode}. Must be either i(nclude), e(xclude), or o(nly)."
                )));
            }
            let windows = ms.spectral_window()?;
            check_baseband(&windows)?;
            let mut groups = BTreeMap::<u32, Vec<i64>>::new();
            for row in 0..windows.row_count() {
                let group = baseband_number(&windows, row)?;
                if baseband >= 0 && group as i64 != baseband {
                    continue;
                }
                let entries = groups.entry(group).or_default();
                if mode == Some(b'i')
                    || is_square_law_detector(&windows.name(row)?) == (mode == Some(b'o'))
                {
                    entries.push(row as i64);
                }
            }
            if baseband >= 0 {
                Ok(int_array(groups.pop_first().map(|(_, values)| values).unwrap_or_default()))
            } else {
                Ok(CasaValue::record(groups.into_iter().map(|(id, values)| (id.to_string(), int_array(values)))))
            }
        })?;
        selected.into_python(py)
    }

    #[pyo3(signature = (spw = Indices::Scalar(-1)), text_signature = "($self, spw=-1)")]
    fn corrbit(&self, py: Python<'_>, spw: Indices) -> PyResult<Py<PyAny>> {
        let value = self.with_ms(py, |ms| {
            let windows = ms.spectral_window()?;
            let column = "SDM_CORR_BIT";
            let has_bits = has_column(&windows, column);
            let bits = |row| {
                if has_bits {
                    text_cell(windows.table(), row, column).map(str::to_owned)
                } else {
                    Ok("UNKNOWN".to_owned())
                }
            };
            let selected = match spw {
                Indices::Scalar(id) if id >= 0 => {
                    let row = spw_row(&windows, id)?;
                    return bits(row).map(CasaValue::Text);
                }
                Indices::Scalar(_) => Indices::Empty,
                Indices::Vector(ids) if ids.is_empty() => {
                    return Ok(CasaValue::TextArray {
                        shape: vec![0],
                        values: Vec::new(),
                    });
                }
                Indices::Vector(ids) => Indices::Vector(ids),
                _ => {
                    return Err(MsError::InvalidInput(
                        "Parameter spw must either be an integer or a list of integers".into(),
                    ));
                }
            };
            let values = selected
                .rows(windows.row_count(), "Spectral window ID")?
                .into_iter()
                .map(bits)
                .collect::<MsResult<Vec<_>>>()?;
            Ok(CasaValue::TextArray {
                shape: vec![values.len()],
                values,
            })
        })?;
        value.into_python(py)
    }

    #[pyo3(signature = (spwids = Indices::EmptyString), text_signature = "($self, spwids='')")]
    fn rxbands(&self, py: Python<'_>, spwids: Indices) -> PyResult<Py<PyAny>> {
        let selected = self.with_ms(py, |ms| selected_name_numbers(ms, spwids, "RB_", true))?;
        Ok(selected.into_pyarray(py).into_any().unbind())
    }

    #[pyo3(signature = (spwids = Indices::EmptyString), text_signature = "($self, spwids='')")]
    fn subwindows(&self, py: Python<'_>, spwids: Indices) -> PyResult<Py<PyAny>> {
        let selected = self.with_ms(py, |ms| selected_name_numbers(ms, spwids, "SW-", false))?;
        Ok(selected.into_pyarray(py).into_any().unbind())
    }
}
