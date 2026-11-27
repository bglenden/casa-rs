// SPDX-License-Identifier: LGPL-3.0-or-later
//! Initialize the MeasurementSet's visibility weights without loading visibilities.

use casa_core::{ArrayValue, Value};
use casa_ms::{MeasurementSet, MsError, MsResult};
use casa_tables::{RequiredScalarColumnValues, Table, TableWriteLock};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::weight_spectra::{NewSpectra, SIGMA_SPECTRUM, WEIGHT_SPECTRUM, has_column};
use crate::{apply::ApplyCalibrationTableSpec, execute::tsys_weights::TsysPlan};

#[derive(Clone, Copy, PartialEq, Eq)]
/// CASA weight initialization or spectrum removal mode.
pub enum Mode {
    /// Initialize weights from integration time and effective bandwidth, and derive sigma.
    Nyquist,
    /// Compute weights from existing sigma values.
    Sigma,
    /// Initialize channel weights from existing row weights.
    Weight,
    /// Set weight and sigma to one.
    Ones,
    /// Remove the per-channel weight column.
    DeleteWeightSpectrum,
    /// Remove the per-channel sigma column.
    DeleteSigmaSpectrum,
}

impl Mode {
    /// Maps a CASA wtmode token to its initializer. Unknown modes return `None`.
    pub fn from_name(mode: &str) -> Option<Self> {
        Some(match mode {
            "nyq" => Self::Nyquist,
            "sigma" => Self::Sigma,
            "weight" => Self::Weight,
            "ones" => Self::Ones,
            "delwtsp" => Self::DeleteWeightSpectrum,
            "delsigsp" => Self::DeleteSigmaSpectrum,
            _ => return None,
        })
    }

    /// Returns the spectrum column removed before updates in this mode.
    pub fn removed_column(self) -> &'static str {
        match self {
            Self::DeleteWeightSpectrum => WEIGHT_SPECTRUM,
            _ => SIGMA_SPECTRUM,
        }
    }

    fn written_columns(self, spectrum: bool) -> Vec<&'static str> {
        let mut columns = match self {
            Self::Nyquist | Self::Ones => vec!["WEIGHT", "SIGMA"],
            Self::Sigma => vec!["WEIGHT"],
            Self::Weight => vec![],
            Self::DeleteWeightSpectrum | Self::DeleteSigmaSpectrum => unreachable!(),
        };
        if spectrum {
            columns.push(WEIGHT_SPECTRUM);
        }
        columns
    }
}

/// Updates row weights and requested channel spectra under the MAIN-table lock. Invalid mode
/// combinations, missing row metadata, and table failures propagate.
pub fn initialize(
    ms: &mut MeasurementSet,
    mode: Mode,
    create_spectrum: bool,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    if mode == Mode::Weight && !create_spectrum {
        return Err(MsError::InvalidInput(
            "Specified wtmode requires dowtsp=T".into(),
        ));
    }
    if matches!(mode, Mode::DeleteWeightSpectrum | Mode::DeleteSigmaSpectrum) {
        return remove_column(ms.main_table_mut(), mode.removed_column(), lock);
    }
    if ms.main_table().row_count() == 0 {
        return Err(MsError::InvalidInput(
            "weight initialization requires at least one MeasurementSet row".into(),
        ));
    }

    let spectrum = create_spectrum || has_column(ms.main_table(), WEIGHT_SPECTRUM);
    let layout = Layout::read(ms, mode)?;
    let main = ms.main_table_mut();
    remove_column(main, mode.removed_column(), lock)?;
    let columns = mode.written_columns(spectrum);
    let shapes = layout.row_description.iter().map(|&index| {
        let shape = &layout.descriptions[index];
        [shape.correlations, shape.channels]
    });
    let mut spectra = NewSpectra::prepare(main, &columns, shapes)?;
    let bytes_per_row = layout
        .descriptions
        .iter()
        .map(|shape| (if spectrum { shape.channels } else { 0 } + 2) * shape.correlations * 4)
        .max()
        .unwrap_or(1)
        .max(1);
    let batch_rows = batch_size(bytes_per_row);
    let mut start = 0;
    while start < main.row_count() {
        let end = (start + batch_rows).min(main.row_count());
        let rows: Vec<_> = (start..end).collect();
        write_batch(main, &rows, &layout, mode, &columns, &mut spectra, lock)?;
        start = end;
    }
    spectra.publish(main, lock)
}

/// Derives weights and sigma from a system-temperature calibration table. The plan can scale by
/// exposure and publish per-channel spectra under the same write lock.
pub fn initialize_tsys(
    ms: &mut MeasurementSet,
    spec: ApplyCalibrationTableSpec,
    by_exposure: bool,
    create_spectrum: bool,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let spectrum = create_spectrum || has_column(ms.main_table(), WEIGHT_SPECTRUM);
    let plan = TsysPlan::read(ms, spec, by_exposure)
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    let main = ms.main_table_mut();
    let columns = if spectrum {
        &["WEIGHT", "SIGMA", WEIGHT_SPECTRUM, SIGMA_SPECTRUM][..]
    } else {
        remove_column(main, SIGMA_SPECTRUM, lock)?;
        &["WEIGHT", "SIGMA"][..]
    };
    let mut spectra = NewSpectra::prepare(main, columns, plan.row_shapes())?;
    let (correlations, channels) = plan.max_cell_shape();
    let batch_rows = batch_size((1 + if spectrum { channels } else { 0 }) * correlations * 8);
    for start in (0..main.row_count()).step_by(batch_rows) {
        let rows: Vec<_> = (start..(start + batch_rows).min(main.row_count())).collect();
        write_values(main, &rows, columns, &mut spectra, lock, |_, row| {
            let values = plan
                .values(row, spectrum)
                .map_err(|error| MsError::InvalidInput(error.to_string()))?;
            Ok(values.map_or_else(
                || WeightRow::Missing(plan.row_shape(row)),
                WeightRow::Update,
            ))
        })?;
    }
    spectra.publish(main, lock)
}

fn batch_size(bytes_per_row: usize) -> usize {
    (16 * 1024 * 1024 / bytes_per_row.max(1)).clamp(1, 65_536)
}

fn remove_column(table: &mut Table, name: &str, lock: &mut TableWriteLock) -> MsResult<()> {
    if has_column(table, name) {
        lock.record_write();
        table.remove_columns_preserving_managers(&[name])?;
    }
    Ok(())
}

struct Description {
    correlations: usize,
    channels: usize,
    mean_effective_bandwidth: f64,
}

struct NyquistInputs {
    exposures: Vec<f64>,
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
}

struct Layout {
    descriptions: Vec<Description>,
    row_description: Vec<usize>,
    nyquist: Option<NyquistInputs>,
}

impl Layout {
    fn read(ms: &MeasurementSet, mode: Mode) -> MsResult<Self> {
        let spw = ms.spectral_window()?;
        let polarization = ms.polarization()?;
        let description = ms.data_description()?;
        let descriptions = (0..description.row_count())
            .map(|id| {
                let spw_id =
                    nonnegative(description.spectral_window_id(id)?, "SPECTRAL_WINDOW_ID")?;
                let pol_id = nonnegative(description.polarization_id(id)?, "POLARIZATION_ID")?;
                let channels = nonnegative(spw.num_chan(spw_id)?, "NUM_CHAN")?;
                let correlations = nonnegative(polarization.num_corr(pol_id)?, "NUM_CORR")?;
                let mean_effective_bandwidth = if mode == Mode::Nyquist {
                    let widths = spw.effective_bw(spw_id)?;
                    widths.iter().sum::<f64>() / widths.len() as f64
                } else {
                    0.0
                };
                Ok(Description {
                    channels,
                    correlations,
                    mean_effective_bandwidth,
                })
            })
            .collect::<MsResult<Vec<_>>>()?;
        let names = if mode == Mode::Nyquist {
            &["DATA_DESC_ID", "EXPOSURE", "ANTENNA1", "ANTENNA2"][..]
        } else {
            &["DATA_DESC_ID"][..]
        };
        let mut columns = ms.main_table().required_scalar_columns_owned(names)?;
        let row_description = int_column(columns.remove("DATA_DESC_ID"), "DATA_DESC_ID")?
            .into_iter()
            .map(|id| {
                let index = nonnegative(id, "DATA_DESC_ID")?;
                if index >= descriptions.len() {
                    return Err(MsError::InvalidIndex {
                        index,
                        max: descriptions.len(),
                        context: "DATA_DESC_ID".into(),
                    });
                }
                Ok(index)
            })
            .collect::<MsResult<_>>()?;
        let nyquist = if mode == Mode::Nyquist {
            let Some(RequiredScalarColumnValues::Float64(exposures)) = columns.remove("EXPOSURE")
            else {
                return Err(MsError::InvalidInput("EXPOSURE must be float64".into()));
            };
            Some(NyquistInputs {
                exposures,
                antenna1: int_column(columns.remove("ANTENNA1"), "ANTENNA1")?,
                antenna2: int_column(columns.remove("ANTENNA2"), "ANTENNA2")?,
            })
        } else {
            None
        };
        Ok(Self {
            descriptions,
            row_description,
            nyquist,
        })
    }

    fn scalar_weight(&self, row: usize, shape: &Description) -> f32 {
        match &self.nyquist {
            Some(inputs) => {
                let baseline_factor = if inputs.antenna1[row] == inputs.antenna2[row] {
                    1.0
                } else {
                    2.0
                };
                (baseline_factor * shape.mean_effective_bandwidth * inputs.exposures[row]) as f32
            }
            None => 1.0,
        }
    }
}

fn nonnegative(value: i32, column: &str) -> MsResult<usize> {
    usize::try_from(value)
        .map_err(|_| MsError::InvalidInput(format!("{column} cannot be negative ({value})")))
}

fn int_column(value: Option<RequiredScalarColumnValues>, column: &str) -> MsResult<Vec<i32>> {
    match value {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        _ => Err(MsError::InvalidInput(format!("{column} must be int32"))),
    }
}

fn sigma_weight(sigma: f32) -> f32 {
    if sigma == 0.0 {
        f32::EPSILON
    } else {
        (sigma * sigma).recip()
    }
}

fn write_batch(
    main: &mut Table,
    rows: &[usize],
    layout: &Layout,
    mode: Mode,
    columns: &[&'static str],
    spectra: &mut NewSpectra,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let inputs = match mode {
        Mode::Sigma => Some("SIGMA"),
        Mode::Weight => Some("WEIGHT"),
        _ => None,
    }
    .map(|name| main.column_accessor(name)?.array_cells_owned_uncached(rows))
    .transpose()?;
    write_values(main, rows, columns, spectra, lock, |index, row| {
        let shape = &layout.descriptions[layout.row_description[row]];
        let weights = if let Some(inputs) = &inputs {
            let Some(ArrayValue::Float32(cell)) = &inputs[index] else {
                return Err(MsError::InvalidInput(format!(
                    "missing or invalid input weight or sigma at row {row}"
                )));
            };
            if cell.shape() != [shape.correlations] {
                return Err(MsError::InvalidInput(format!(
                    "input weight or sigma shape does not match polarization at row {row}"
                )));
            }
            if mode == Mode::Sigma {
                cell.iter().copied().map(sigma_weight).collect()
            } else {
                cell.iter().copied().collect()
            }
        } else {
            vec![layout.scalar_weight(row, shape); shape.correlations]
        };
        let arrays = columns
            .iter()
            .map(|column| {
                ArrayValue::Float32(match *column {
                    "WEIGHT" => {
                        ArrayD::from_shape_fn(IxDyn(&[shape.correlations]), |i| weights[i[0]])
                    }
                    "SIGMA" => ArrayD::from_shape_fn(IxDyn(&[shape.correlations]), |i| {
                        weights[i[0]].sqrt().recip()
                    }),
                    WEIGHT_SPECTRUM => ArrayD::from_shape_fn(
                        IxDyn(&[shape.correlations, shape.channels]).f(),
                        |i| weights[i[0]],
                    ),
                    _ => unreachable!(),
                })
            })
            .collect();
        Ok(WeightRow::Update(arrays))
    })
}

enum WeightRow {
    Update(Vec<ArrayValue>),
    Missing([usize; 2]),
}

fn write_values(
    main: &mut Table,
    rows: &[usize],
    columns: &[&'static str],
    spectra: &mut NewSpectra,
    lock: &mut TableWriteLock,
    mut values: impl FnMut(usize, usize) -> MsResult<WeightRow>,
) -> MsResult<()> {
    let changed: Vec<_> = columns
        .iter()
        .copied()
        .filter(|name| !spectra.contains(name))
        .collect();
    let mut writer = (!changed.is_empty())
        .then(|| main.row_accessor_mut().prepare(&changed))
        .transpose()?;
    let mut rows_written = Vec::with_capacity(rows.len());
    for (index, &row) in rows.iter().enumerate() {
        let arrays = match values(index, row)? {
            WeightRow::Update(arrays) => arrays,
            WeightRow::Missing(shape) => {
                spectra.append_unity(shape)?;
                continue;
            }
        };
        if let Some(writer) = &mut writer {
            writer.seek(row)?;
            rows_written.push(row);
        }
        for (&name, value) in columns.iter().zip(arrays) {
            if spectra.append(name, &value)? {
                continue;
            }
            let writer = writer.as_mut().expect("output column has a table writer");
            let slot = writer.column_index(name).expect("planned output column");
            writer.set_value_at(slot, Value::Array(value))?;
        }
    }
    if let Some(mut writer) = writer {
        writer.flush()?;
    }
    if !rows_written.is_empty() {
        lock.record_write();
        main.prepare_write()
            .save_selected_rows(&changed, &rows_written)?;
        main.discard_persisted_cell_updates(&changed, &rows_written);
    }
    Ok(())
}
