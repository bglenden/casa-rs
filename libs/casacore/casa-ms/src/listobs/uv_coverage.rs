// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl ListObsUvCoverage {
    /// Build UV coverage from an open [`MeasurementSet`].
    pub fn from_ms(ms: &MeasurementSet) -> MsResult<Self> {
        Self::from_ms_with_options(ms, &ListObsOptions::default())
    }

    /// Build UV coverage from an open [`MeasurementSet`] using explicit
    /// task-style options.
    pub fn from_ms_with_options(ms: &MeasurementSet, options: &ListObsOptions) -> MsResult<Self> {
        options.validate_supported()?;
        let selected_rows = if !options.selectdata || !options.has_selection() {
            None
        } else {
            Some(resolve_row_indices_with_system_budget(
                ms,
                &MsSelection::from_summary_options(options),
            )?)
        };
        let dd = ms.data_description()?;
        let spw = ms.spectral_window()?;
        let mut ddid_to_spw = HashMap::<i32, i32>::with_capacity(dd.row_count());
        for row in 0..dd.row_count() {
            ddid_to_spw.insert(row as i32, dd.spectral_window_id(row)?);
        }

        let mut spw_center_hz = HashMap::<i32, f64>::with_capacity(spw.row_count());
        for row in 0..spw.row_count() {
            let chan_freq = spw.chan_freq(row)?;
            let center_hz = mean_or_zero(&chan_freq);
            let center_hz = if center_hz > 0.0 {
                center_hz
            } else {
                spw.ref_frequency(row)?
            };
            if center_hz <= 0.0 {
                return Err(MsError::VersionError(format!(
                    "spectral window {row} does not expose a positive representative frequency"
                )));
            }
            spw_center_hz.insert(row as i32, center_hz);
        }

        let mut grouped = BTreeMap::<(i32, i32, i32, i32), (f64, Vec<ListObsUvPoint>)>::new();
        let mut sample_count = 0usize;
        let mut max_abs_uv_lambda = 0.0_f64;
        let rows = selected_rows
            .as_deref()
            .map(|rows| rows.to_vec())
            .unwrap_or_else(|| (0..ms.main_table().row_count()).collect());
        let main_table = ms.main_table();
        let antenna1_values = load_i32_rows(main_table, "ANTENNA1", &rows)?;
        let antenna2_values = load_i32_rows(main_table, "ANTENNA2", &rows)?;
        let field_values = load_i32_rows(main_table, "FIELD_ID", &rows)?;
        let data_desc_values = load_i32_rows(main_table, "DATA_DESC_ID", &rows)?;
        let time_values = load_f64_rows(main_table, "TIME", &rows)?;
        let uvw_values = load_uvw_rows(main_table, "UVW", &rows)?;

        for (row_slot, row) in rows.iter().copied().enumerate() {
            let antenna1 = antenna1_values[row_slot];
            let antenna2 = antenna2_values[row_slot];
            let field_id = field_values[row_slot];
            let data_desc_id = data_desc_values[row_slot];
            let time_mjd_seconds = time_values[row_slot];
            let spectral_window_id = *ddid_to_spw.get(&data_desc_id).ok_or_else(|| {
                MsError::VersionError(format!(
                    "MAIN.DATA_DESC_ID {data_desc_id} does not resolve to a DATA_DESCRIPTION row"
                ))
            })?;
            let center_frequency_hz = *spw_center_hz.get(&spectral_window_id).ok_or_else(|| {
                MsError::VersionError(format!(
                    "DATA_DESCRIPTION row {data_desc_id} resolved to unknown SPECTRAL_WINDOW {spectral_window_id}"
                ))
            })?;
            let wavelength_m = SPEED_OF_LIGHT_M_S / center_frequency_hz;
            let [u_m, v_m, w_m] = uvw_values[row_slot];
            let point = ListObsUvPoint {
                row,
                time_mjd_seconds,
                u_lambda: u_m / wavelength_m,
                v_lambda: v_m / wavelength_m,
                w_lambda: w_m / wavelength_m,
            };
            max_abs_uv_lambda = max_abs_uv_lambda
                .max(point.u_lambda.abs())
                .max(point.v_lambda.abs());
            sample_count += 1;
            grouped
                .entry((antenna1, antenna2, field_id, spectral_window_id))
                .or_insert_with(|| (center_frequency_hz, Vec::new()))
                .1
                .push(point);
        }

        let mut tracks = grouped
            .into_iter()
            .map(
                |(
                    (antenna1, antenna2, field_id, spectral_window_id),
                    (center_frequency_hz, mut samples),
                )| {
                    samples.sort_by(|left, right| {
                        left.time_mjd_seconds.total_cmp(&right.time_mjd_seconds)
                    });
                    ListObsUvTrack {
                        antenna1,
                        antenna2,
                        field_id,
                        spectral_window_id,
                        center_frequency_hz,
                        samples,
                    }
                },
            )
            .collect::<Vec<_>>();
        tracks.sort_by_key(|track| {
            (
                track.antenna1,
                track.antenna2,
                track.field_id,
                track.spectral_window_id,
            )
        });

        Ok(Self {
            schema_version: LISTOBS_UV_SCHEMA_VERSION,
            options: options.clone(),
            measurement_set_path: ms.path().map(|path| path.display().to_string()),
            axis_unit: "lambda".to_string(),
            mirrored_display: true,
            sample_count,
            max_abs_uv_lambda,
            tracks,
        })
    }

    /// Open an on-disk MeasurementSet and extract UV coverage.
    pub fn from_path(path: impl AsRef<Path>) -> MsResult<Self> {
        Self::from_path_with_options(path, &ListObsOptions::default())
    }

    /// Open an on-disk MeasurementSet and extract UV coverage using explicit
    /// task-style options.
    pub fn from_path_with_options(
        path: impl AsRef<Path>,
        options: &ListObsOptions,
    ) -> MsResult<Self> {
        let ms = MeasurementSet::open(path)?;
        Self::from_ms_with_options(&ms, options)
    }

    /// Render the UV coverage as pretty-printed JSON.
    pub fn render_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

pub(super) fn load_i32_rows(table: &Table, column: &str, rows: &[usize]) -> MsResult<Vec<i32>> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(ScalarValue::Int32(v)) => Ok(v),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Int32".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

pub(super) fn load_f64_rows(table: &Table, column: &str, rows: &[usize]) -> MsResult<Vec<f64>> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(ScalarValue::Float64(v)) => Ok(v),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Float64".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

pub(super) fn load_uvw_rows(
    table: &Table,
    column: &str,
    rows: &[usize],
) -> MsResult<Vec<[f64; 3]>> {
    table
        .column_accessor(column)?
        .array_cells_owned(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(ArrayValue::Float64(values)) => {
                let slice = values
                    .as_slice()
                    .ok_or_else(|| MsError::ColumnTypeMismatch {
                        column: column.to_string(),
                        table: "MAIN".to_string(),
                        expected: "contiguous f64[3]".to_string(),
                        found: "non-contiguous".to_string(),
                    })?;
                if slice.len() != 3 {
                    return Err(MsError::ColumnTypeMismatch {
                        column: column.to_string(),
                        table: "MAIN".to_string(),
                        expected: "f64[3]".to_string(),
                        found: format!("f64[{}]", slice.len()),
                    });
                }
                Ok([slice[0], slice[1], slice[2]])
            }
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Float64 array".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}
