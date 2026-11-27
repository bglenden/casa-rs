// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn geometry_engine_for_plan(
    ms: &MeasurementSet,
    plan: &ApplyPlan,
) -> Result<Option<Arc<MsCalEngine>>, ApplyExecutionError> {
    let needs_geometry = plan.calibration_tables.iter().any(|table| {
        matches!(
            table.summary.table_subtype.as_str(),
            "KAntPos Jones" | "EGainCurve" | "EPowerCurve" | "TOpac" | "TfOpac" | "F Jones"
        )
    });
    if !needs_geometry {
        return Ok(None);
    }
    let ms_path = display_ms_path(ms);
    MsCalEngine::for_calibration(ms)
        .map(Arc::new)
        .map(Some)
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: ms_path,
            source,
        })
}

pub(crate) fn load_calibration_table(
    table_plan: &ApplyCalibrationTablePlan,
    geometry_engine: Option<&Arc<MsCalEngine>>,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    load_parameters(table_plan, geometry_engine, false, None)
}

pub(crate) fn load_simulator_table(
    table_plan: &ApplyCalibrationTablePlan,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    load_parameters(table_plan, None, true, None)
}

pub(super) fn load_parameters(
    table_plan: &ApplyCalibrationTablePlan,
    geometry_engine: Option<&Arc<MsCalEngine>>,
    simulator: bool,
    ms: Option<&MeasurementSet>,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    match table_plan.summary.table_subtype.as_str() {
        "BPOLY" => return load_bpoly_calibration_table(table_plan),
        "GSPLINE" => return gspline::load(table_plan),
        _ => {}
    }
    let table = Table::open(TableOptions::new(&table_plan.spec.path)).map_err(|source| {
        ApplyExecutionError::OpenMeasurementSet {
            path: table_plan.spec.path.display().to_string(),
            source: MsError::from(source),
        }
    })?;
    let matrix = polarization::JonesMatrix::from_subtype(&table_plan.summary.table_subtype);
    let ionosphere = ionosphere::Ionosphere::for_plan(&table, table_plan)?;
    let cross_hand_phase = polarization::is_cross_hand_phase(&table_plan.summary.table_subtype);
    let sky_term = crate::sd_solver::is_sky_subtype(&table_plan.summary.table_subtype);
    let sky_exposures = if sky_term && table_plan.calwt {
        ms.map(|ms| sky::SkyExposures::read(ms, &table, &table_plan.spec.path))
            .transpose()?
    } else {
        None
    };
    let supports_calwt =
        weights::supports_calwt(&table_plan.summary, matrix.is_some(), cross_hand_phase);
    let antpos_tropo = match (
        table_plan.summary.table_subtype.as_str(),
        ms,
        geometry_engine,
    ) {
        ("KAntPos Jones", Some(ms), Some(engine)) => load_antpos_tropo(&table, ms, engine)?,
        _ => None,
    };

    let baseline = ms.is_some() && baseline::is_subtype(&table_plan.summary.table_subtype);
    let x_mueller = table_plan.summary.table_subtype == "X Mueller";
    let batch_gain_arrays = table_plan.summary.parameter_family
        == CalibrationParameterFamily::Complex
        && matches!(
            table_plan.summary.table_subtype.as_str(),
            "G Jones" | "T Jones"
        )
        && table.row_count() > 1;
    let mut gain_arrays = None;
    let path = &table_plan.spec.path;
    let mut solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>> = HashMap::new();
    for row_index in 0..table.row_count() {
        let field_id = get_i32(&table, row_index, COL_FIELD_ID)?;
        let spw_id = get_i32(&table, row_index, COL_SPECTRAL_WINDOW_ID)?;
        let Some((antenna_id, reversed)) = load_parameter_antenna(
            &table,
            row_index,
            &table_plan.spec.path,
            baseline,
            x_mueller,
        )?
        else {
            continue;
        };
        let time_seconds = get_f64(&table, row_index, COL_TIME)?;
        if batch_gain_arrays && row_index.is_multiple_of(gain_arrays::GainArrayBatch::ROWS) {
            gain_arrays = gain_arrays::GainArrayBatch::read(&table, row_index);
        }
        let (flag_cell, gain_cell) = gain_arrays
            .as_ref()
            .map(|batch| batch.cells(row_index))
            .unwrap_or_default();
        let flags = gain_arrays::load_array(&table, path, row_index, COL_FLAG, flag_cell)?;
        let read_fparam = || gain_arrays::load_array(&table, path, row_index, COL_FPARAM, None);
        let grid = match table_plan.summary.parameter_family {
            CalibrationParameterFamily::Complex => load_complex_parameters(
                gain_arrays::load_array(&table, path, row_index, COL_CPARAM, gain_cell)?,
                row_index,
                path,
                flags,
                match table_plan.summary.table_subtype.as_str() {
                    baseline::NOISE_SUBTYPE => Some((4, "additive noise")),
                    "M Mueller" | "Mf Mueller" if baseline => Some((2, "baseline calibration")),
                    _ => matrix.map(polarization::JonesMatrix::parameter_shape),
                },
                // The simulator constructs [X, 1] itself from raw CPARAM.
                cross_hand_phase && !simulator,
                reversed,
            )?,
            CalibrationParameterFamily::Float
                if simulator || table_plan.summary.table_subtype == "G EVLASWPOW" =>
            {
                let values = read_fparam()?;
                let path = &table_plan.spec.path;
                if (matches!(
                    table_plan.summary.table_subtype.as_str(),
                    "PosAng Jones" | "TfOpac"
                ) || ionosphere.is_some())
                    && values.shape().first() != Some(&1)
                {
                    return Err(ApplyExecutionError::UnsupportedParameterShape {
                        path: path.display().to_string(),
                        shape: values.shape().to_vec(),
                    });
                }
                if table_plan.summary.table_subtype == "Fringe Jones" && values.shape() != [8, 1] {
                    return Err(ApplyExecutionError::UnsupportedParameterShape {
                        path: path.display().to_string(),
                        shape: values.shape().to_vec(),
                    });
                }
                CalibrationGrid::Complex(GainGrid::from_real_arrays(path, values, flags)?)
            }
            CalibrationParameterFamily::Float
                if matches!(
                    table_plan.summary.table_subtype.as_str(),
                    "K Jones" | "KMBD Jones" | "Kcross Jones"
                ) =>
            {
                let delays = read_fparam()?;
                CalibrationGrid::Delay(DelayGrid::from_arrays(
                    &table_plan.spec.path,
                    delays,
                    flags,
                )?)
            }
            CalibrationParameterFamily::Float
                if table_plan.summary.table_subtype.as_str() == "KAntPos Jones" =>
            {
                let offsets = read_fparam()?;
                CalibrationGrid::Antpos(AntposGrid::from_arrays(
                    &table_plan.spec.path,
                    offsets,
                    flags,
                )?)
            }
            CalibrationParameterFamily::Float
                if matches!(
                    table_plan.summary.table_subtype.as_str(),
                    "EGainCurve" | "EPowerCurve"
                ) =>
            {
                let coefficients = read_fparam()?;
                CalibrationGrid::GainCurve(GainCurveGrid::from_arrays(
                    &table_plan.spec.path,
                    coefficients,
                    flags,
                    table_plan.summary.table_subtype == "EPowerCurve",
                )?)
            }
            CalibrationParameterFamily::Float
                if matches!(
                    table_plan.summary.table_subtype.as_str(),
                    "TOpac" | "TfOpac"
                ) =>
            {
                let tau = read_fparam()?;
                let path = &table_plan.spec.path;
                if table_plan.summary.table_subtype == "TfOpac" {
                    CalibrationGrid::SpectralOpacity(GainGrid::from_real_arrays(path, tau, flags)?)
                } else {
                    CalibrationGrid::Opacity(OpacityGrid::from_arrays(path, tau, flags)?)
                }
            }
            _ => {
                return Err(ApplyExecutionError::UnsupportedInterpolation {
                    path: table_plan.spec.path.display().to_string(),
                    interp: table_plan.interp,
                    reason: "unsupported calibration parameter family for executor".to_string(),
                });
            }
        };
        solutions
            .entry((field_id, spw_id, antenna_id))
            .or_default()
            .push(CalibrationSolution {
                time_seconds,
                grid: Arc::new(grid),
            });
    }

    Ok(LoadedCalibrationTable {
        path: table_plan.spec.path.clone(),
        baseline,
        interp: table_plan.interp,
        supports_calwt,
        engine: geometry_engine.cloned(),
        antpos_tropo,
        ionosphere,
        sky_exposures,
        spline: None,
        solutions,
        callibrary: None,
        time_plans: HashMap::new(),
        missing_grids: HashMap::new(),
        time_parameter_family: None,
        phase_delay: false,
    })
}

pub(super) fn load_parameter_antenna(
    table: &Table,
    row_index: usize,
    path: &Path,
    baseline: bool,
    x_mueller: bool,
) -> Result<Option<(i32, bool)>, ApplyExecutionError> {
    let first = get_i32(table, row_index, COL_ANTENNA1)?;
    if x_mueller {
        // XMueller uses the first baseline parameter for every baseline.
        let is_first = first == 0 && get_i32(table, row_index, COL_ANTENNA2)? == 0;
        Ok(is_first.then_some((0, false)))
    } else if baseline {
        let second = get_i32(table, row_index, COL_ANTENNA2)?;
        let index = baseline::require_index(first, second, path)?;
        Ok(Some((index, first > second)))
    } else {
        Ok(Some((first, false)))
    }
}

pub(super) fn load_complex_parameters(
    gains: &ArrayValue,
    row_index: usize,
    path: &Path,
    flags: &ArrayValue,
    parameter_shape: Option<(usize, &str)>,
    cross_hand_phase: bool,
    reversed: bool,
) -> Result<CalibrationGrid, ApplyExecutionError> {
    if let Some((count, family)) = parameter_shape
        && gains.shape().first() != Some(&count)
    {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "{family} requires {count} CPARAM elements, found shape {:?} at row {row_index}",
                gains.shape()
            ),
        });
    }
    let mut grid = GainGrid::from_arrays(path, gains, flags)?;
    if reversed {
        grid.values.mapv_inplace(|value| value.conj());
        if grid.receptor_count == 4 && grid.values.ndim() == 2 {
            for coefficient in 0..grid.channel_count {
                grid.values.swap([1, coefficient], [2, coefficient]);
                grid.flags.swap([1, coefficient], [2, coefficient]);
            }
        }
    }
    if cross_hand_phase {
        grid = grid.cross_hand_diagonal(path)?;
    }
    Ok(CalibrationGrid::Complex(grid))
}

pub(super) fn load_antpos_tropo(
    table: &Table,
    ms: &MeasurementSet,
    engine: &MsCalEngine,
) -> Result<Option<TropoDelay>, ApplyExecutionError> {
    let Some(value) = table.keywords().get(TROPO_KEYWORD) else {
        return Ok(None);
    };
    let failed = |source| ApplyExecutionError::MutateMeasurementSet {
        path: display_ms_path(ms),
        source,
    };
    if !only_vla_observations(ms).map_err(failed)? {
        return Ok(None);
    }
    let scale = match value {
        Value::Scalar(ScalarValue::Float64(scale)) => *scale,
        Value::Scalar(ScalarValue::Float32(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::UInt8(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::Int16(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::UInt32(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::Int32(scale)) => f64::from(*scale),
        Value::Scalar(ScalarValue::Int64(scale)) => *scale as f64,
        _ => {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: table
                    .path()
                    .map_or_else(String::new, |path| path.display().to_string()),
                reason: format!("{TROPO_KEYWORD} must be a numeric keyword"),
            });
        }
    };
    (scale != 0.0)
        .then(|| TropoDelay::new(ms, engine, scale))
        .transpose()
        .map_err(failed)
}

pub(super) fn load_bpoly_calibration_table(
    table_plan: &ApplyCalibrationTablePlan,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    let table = Table::open(TableOptions::new(&table_plan.spec.path)).map_err(|source| {
        ApplyExecutionError::OpenMeasurementSet {
            path: table_plan.spec.path.display().to_string(),
            source: MsError::from(source),
        }
    })?;
    let cal_desc = Table::open(TableOptions::new(
        table_plan.spec.path.join(LEGACY_CAL_DESC_KEYWORD),
    ))
    .map_err(
        |source| ApplyExecutionError::OpenCalibrationAuxiliaryTable {
            path: table_plan.spec.path.display().to_string(),
            subtable: LEGACY_CAL_DESC_KEYWORD,
            source,
        },
    )?;
    let cal_desc_map = load_bpoly_cal_desc_map(&table_plan.spec.path, &cal_desc)?;
    let spw_plans = table_plan
        .calibration_spectral_windows
        .iter()
        .map(|spw| (spw.spw_id, spw))
        .collect::<HashMap<_, _>>();

    let rows = (0..table.row_count())
        .map(|row| {
            Ok((
                get_i32(&table, row, COL_CAL_DESC_ID)?,
                get_f64(&table, row, COL_TIME)?,
            ))
        })
        .collect::<Result<Vec<_>, ApplyExecutionError>>()?;
    let first = rows
        .iter()
        .copied()
        .min_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
    let mut solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>> = HashMap::new();
    for (row_index, (cal_desc_id, time_seconds)) in rows.into_iter().enumerate() {
        let field_id = get_i32(&table, row_index, COL_FIELD_ID)?;
        let antenna_id = get_i32(&table, row_index, COL_ANTENNA1)?;
        let cal_desc_entry = cal_desc_map.get(&cal_desc_id).ok_or_else(|| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: table_plan.spec.path.display().to_string(),
                reason: format!("CAL_DESC_ID {cal_desc_id} was not present in CAL_DESC"),
            }
        })?;
        let Some(spw_plan) = spw_plans.get(&cal_desc_entry.spw_id) else {
            // Only mapped, applicable windows have a target sampling grid.
            // Unavailable rows are skipped or strictly flagged by the caller.
            continue;
        };
        let grid = sample_bpoly_row(
            &table,
            row_index,
            &table_plan.spec.path,
            spw_plan,
            *cal_desc_entry,
            first == Some((cal_desc_id, time_seconds)),
        )?;
        solutions
            .entry((field_id, cal_desc_entry.spw_id, antenna_id))
            .or_default()
            .push(CalibrationSolution {
                time_seconds,
                grid: Arc::new(grid),
            });
    }

    Ok(LoadedCalibrationTable {
        path: table_plan.spec.path.clone(),
        baseline: false,
        interp: table_plan.interp,
        supports_calwt: false,
        engine: None,
        antpos_tropo: None,
        ionosphere: None,
        sky_exposures: None,
        spline: None,
        solutions,
        callibrary: None,
        time_plans: HashMap::new(),
        missing_grids: HashMap::new(),
        time_parameter_family: None,
        phase_delay: false,
    })
}

pub(super) fn load_bpoly_cal_desc_map(
    path: &Path,
    cal_desc: &Table,
) -> Result<HashMap<i32, LegacyCalDescEntry>, ApplyExecutionError> {
    let mut entries = HashMap::new();
    for row_index in 0..cal_desc.row_count() {
        let spw_ids = cal_desc
            .cell_accessor(row_index, COL_SPECTRAL_WINDOW_ID)
            .and_then(|cell| cell.array())
            .map_err(|source| ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "failed to read CAL_DESC SPECTRAL_WINDOW_ID row {row_index}: {source}"
                ),
            })?;
        let ArrayValue::Int32(spw_ids) = spw_ids else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "CAL_DESC SPECTRAL_WINDOW_ID row {row_index} had unexpected type {:?}",
                    spw_ids.primitive_type()
                ),
            });
        };
        let spw_values = spw_ids.iter().copied().collect::<Vec<_>>();
        let [spw_id] = spw_values.as_slice() else {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "CAL_DESC row {row_index} must reference exactly one spectral window"
                ),
            });
        };
        let receptor_count = get_i32(cal_desc, row_index, "NUM_RECEPTORS")?;
        let receptor_count = usize::try_from(receptor_count).map_err(|_| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!(
                    "CAL_DESC row {row_index} had invalid NUM_RECEPTORS value {receptor_count}"
                ),
            }
        })?;
        entries.insert(
            row_index as i32,
            LegacyCalDescEntry {
                spw_id: *spw_id,
                receptor_count,
            },
        );
    }
    Ok(entries)
}

pub(super) fn sample_bpoly_row(
    table: &Table,
    row_index: usize,
    path: &Path,
    spw_plan: &crate::plan::SpectralWindowPlan,
    cal_desc_entry: LegacyCalDescEntry,
    first_interval: bool,
) -> Result<CalibrationGrid, ApplyExecutionError> {
    let scale_factor = get_complex32(table, row_index, COL_SCALE_FACTOR)?;
    let valid_domain = get_f64_array(table, row_index, COL_VALID_DOMAIN, path)?;
    let [domain_start_hz, domain_end_hz] = valid_domain.as_slice() else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!("BPOLY VALID_DOMAIN row {row_index} must contain exactly two values"),
        });
    };
    let amp_coeff_count =
        usize::try_from(get_i32(table, row_index, COL_N_POLY_AMP)?).map_err(|_| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("BPOLY N_POLY_AMP row {row_index} was negative"),
            }
        })?;
    let phase_coeff_count =
        usize::try_from(get_i32(table, row_index, COL_N_POLY_PHASE)?).map_err(|_| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("BPOLY N_POLY_PHASE row {row_index} was negative"),
            }
        })?;
    let amp_values = get_numeric_array(table, row_index, COL_POLY_COEFF_AMP, path)?;
    let phase_values = get_numeric_array(table, row_index, COL_POLY_COEFF_PHASE, path)?;
    let amp_receptor_count = infer_bpoly_receptor_count(
        amp_values.len(),
        cal_desc_entry.receptor_count,
        amp_coeff_count,
        path,
        row_index,
        COL_POLY_COEFF_AMP,
    )?;
    let phase_receptor_count = infer_bpoly_receptor_count(
        phase_values.len(),
        cal_desc_entry.receptor_count,
        phase_coeff_count,
        path,
        row_index,
        COL_POLY_COEFF_PHASE,
    )?;
    if amp_coeff_count > 0 && phase_coeff_count > 0 && amp_receptor_count != phase_receptor_count {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "BPOLY row {row_index} inferred mismatched receptor counts: amp={amp_receptor_count}, phase={phase_receptor_count}"
            ),
        });
    }
    let receptor_count = if amp_coeff_count == 0 {
        phase_receptor_count
    } else {
        amp_receptor_count
    };
    let amp_coefficients = split_bpoly_coefficients(
        amp_values,
        receptor_count,
        amp_coeff_count,
        path,
        row_index,
        COL_POLY_COEFF_AMP,
    )?;
    let phase_coefficients = split_bpoly_coefficients(
        phase_values,
        receptor_count,
        phase_coeff_count,
        path,
        row_index,
        COL_POLY_COEFF_PHASE,
    )?;
    let phase_unit_scale = match get_string(table, row_index, COL_PHASE_UNITS)?
        .to_ascii_uppercase()
        .as_str()
    {
        "RAD" | "RADIAN" | "RADIANS" => 1.0_f64,
        "DEG" | "DEGREE" | "DEGREES" => std::f64::consts::PI / 180.0_f64,
        other => {
            return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("unsupported BPOLY PHASE_UNITS value {other:?}"),
            });
        }
    };

    let channel_count = spw_plan.channel_frequencies_hz.len().max(1);
    let mut values = ArrayD::from_elem(
        IxDyn(&[receptor_count, channel_count]).f(),
        Complex32::new(1.0, 0.0),
    );
    let mut flags = ArrayD::from_elem(IxDyn(&[receptor_count, channel_count]).f(), false);
    let domain_start_hz = *domain_start_hz;
    let domain_end_hz = *domain_end_hz;

    for receptor in 0..receptor_count {
        let amp_coeff = amp_coefficients.get(receptor).ok_or_else(|| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("missing BPOLY amplitude coefficients for receptor {receptor}"),
            }
        })?;
        let phase_coeff = phase_coefficients.get(receptor).ok_or_else(|| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: format!("missing BPOLY phase coefficients for receptor {receptor}"),
            }
        })?;
        // The CASA-layout loader starts with valid unity gains only for the
        // first stored interval. Later intervals flag receptor groups whose
        // coefficients never replace that interval's initial unity value.
        if cal_desc_entry.receptor_count == 0
            && !first_interval
            && amp_coeff
                .iter()
                .chain(phase_coeff)
                .all(|&coefficient| coefficient == 0.0)
        {
            flags.index_axis_mut(ndarray::Axis(0), receptor).fill(true);
            continue;
        }

        for (channel, frequency_hz) in spw_plan.channel_frequencies_hz.iter().copied().enumerate() {
            if frequency_hz < domain_start_hz || frequency_hz > domain_end_hz {
                values[[receptor, channel]] = Complex32::new(1.0, 0.0);
                continue;
            }

            let amp_value = legacy_bpoly_chebyshev_value(
                amp_coeff,
                domain_start_hz,
                domain_end_hz,
                frequency_hz,
            );
            let phase_rad = legacy_bpoly_chebyshev_value(
                phase_coeff,
                domain_start_hz,
                domain_end_hz,
                frequency_hz,
            ) * phase_unit_scale;
            let amp_scale = amp_value.exp() as f32;
            let polynomial_gain =
                Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32) * amp_scale;
            values[[receptor, channel]] = scale_factor * polynomial_gain;
        }
    }

    Ok(CalibrationGrid::Complex(GainGrid {
        receptor_count,
        channel_count,
        values,
        flags,
    }))
}

pub(super) fn split_bpoly_coefficients(
    coefficients: Vec<f64>,
    receptor_count: usize,
    coefficients_per_receptor: usize,
    path: &Path,
    row_index: usize,
    column: &str,
) -> Result<Vec<Vec<f64>>, ApplyExecutionError> {
    let expected = receptor_count.saturating_mul(coefficients_per_receptor);
    if coefficients.len() != expected {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "BPOLY {column} row {row_index} contained {} coefficients but expected {expected}",
                coefficients.len()
            ),
        });
    }
    if coefficients_per_receptor == 0 {
        return Ok(vec![Vec::new(); receptor_count]);
    }
    Ok(coefficients
        .chunks(coefficients_per_receptor)
        .map(|chunk| chunk.to_vec())
        .collect())
}

pub(super) fn infer_bpoly_receptor_count(
    total_coefficients: usize,
    nominal_receptor_count: usize,
    coefficients_per_receptor: usize,
    path: &Path,
    row_index: usize,
    column: &str,
) -> Result<usize, ApplyExecutionError> {
    if coefficients_per_receptor == 0 {
        return Ok(nominal_receptor_count.max(1));
    }
    let nominal_total = nominal_receptor_count.saturating_mul(coefficients_per_receptor);
    if total_coefficients == nominal_total {
        return Ok(nominal_receptor_count.max(1));
    }
    if total_coefficients.is_multiple_of(coefficients_per_receptor) {
        return Ok((total_coefficients / coefficients_per_receptor).max(1));
    }
    Err(ApplyExecutionError::UnsupportedCalibrationTable {
        path: path.display().to_string(),
        reason: format!(
            "BPOLY {column} row {row_index} contained {total_coefficients} coefficients, which is not divisible by the per-receptor coefficient count {coefficients_per_receptor}"
        ),
    })
}

pub(super) fn delay_reference_frequency(
    cal_spw: &crate::plan::SpectralWindowPlan,
    plan: &ApplyCalibrationTablePlan,
    loaded: &LoadedCalibrationTable,
) -> f64 {
    if plan.summary.table_subtype == "KMBD Jones" {
        if loaded.callibrary.is_none() {
            // The traditional KMBD applicator always uses an absolute pivot.
            return 0.;
        }
        // A callibrary instead uses KJones's version-dependent frequency pivot.
        if matches!(
            plan.summary.keywords.casa_version.as_deref(),
            None | Some(
                "Unknown"
                    | "5.3.0-100"
                    | "5.3.0-101"
                    | "5.3.0-102"
                    | "5.3.0-103"
                    | "5.3.0-104"
                    | "5.3.0-105"
                    | "5.3.0-106"
            )
        ) {
            return cal_spw.ref_frequency_hz;
        }
    }
    cal_spw_reference_frequency_hz(cal_spw)
}

pub(super) fn cal_spw_reference_frequency_hz(cal_spw: &crate::plan::SpectralWindowPlan) -> f64 {
    cal_spw
        .channel_frequencies_hz
        .get(cal_spw.channel_frequencies_hz.len() / 2)
        .copied()
        .unwrap_or(cal_spw.ref_frequency_hz)
}
