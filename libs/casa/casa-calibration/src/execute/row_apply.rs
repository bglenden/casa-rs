// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn apply_row_all_channels(
    row: &ApplyRowPlan,
    inputs: ExecutionRowInputs<'_>,
    plan: &ApplyPlan,
    chain: &ApplyChain,
    geometry_cache: Option<&mut RowGeometryCache>,
    compute_profile: Option<&mut ApplyRowComputeProfile>,
    mut activity: Option<&mut ToolApplyActivity>,
) -> Result<ExecutionRowResult, ApplyExecutionError> {
    let mut compute_profile = compute_profile;
    let setup_started_at = compute_profile.as_ref().map(|_| Instant::now());
    let ExecutionRowInputs {
        correlation_types,
        data,
        original_flags,
        original_weight,
        original_weight_spectrum,
        has_weight_spectrum,
        channel_grid,
        forward_model,
        skip_unavailable_terms,
        trial,
        parang_calwt,
        ..
    } = inputs;
    let ArrayValue::Complex32(data) = data else {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: "<measurement-set DATA>".to_string(),
            shape: data.shape().to_vec(),
        });
    };
    let flag_array = match original_flags {
        Some(ArrayValue::Bool(flag_array)) => Some(flag_array),
        Some(other) => {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: "<measurement-set FLAG>".to_string(),
                shape: other.shape().to_vec(),
            });
        }
        None => None,
    };
    if data.ndim() != 2
        || flag_array
            .is_some_and(|flag_array| flag_array.ndim() != 2 || data.shape() != flag_array.shape())
    {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: "<measurement-set row>".to_string(),
            shape: data.shape().to_vec(),
        });
    }
    if data.shape()[0] != correlation_types.len() {
        return Err(ApplyExecutionError::UnsupportedCorrelationLayout {
            data_desc_id: row.data_desc_id,
            correlation_types: correlation_types
                .iter()
                .map(|code| stokes_name(*code).to_string())
                .collect(),
        });
    }

    let mut corrected = data.clone();
    let propagate_flags = plan.apply_mode.writes_flags() || activity.is_some();
    let mut flags = if propagate_flags {
        flag_array
            .ok_or_else(|| ApplyExecutionError::MutateMeasurementSet {
                path: "<measurement-set FLAG>".to_string(),
                source: MsError::from(TableError::ColumnNotFound {
                    row_index: row.row_index,
                    column: "FLAG".to_string(),
                }),
            })?
            .clone()
    } else {
        ArrayD::from_elem(IxDyn(&[0]).f(), false)
    };
    let mut newly_flagged_samples = 0;
    let initial_flags = activity
        .as_ref()
        .map_or(0, |_| flags.iter().filter(|flag| **flag).count());
    if let Some(activity) = activity.as_deref_mut() {
        activity.begin_row(data.len());
    }
    let any_calwt = plan.apply_mode.writes_corrected_data()
        && (parang_calwt
            || plan
                .calibration_tables
                .iter()
                .zip(&chain.tables)
                .any(|(table, loaded)| table.calwt && loaded.supports_calwt));
    let mut weight = match original_weight {
        Some(ArrayValue::Float32(weight)) => Some(weight.clone()),
        Some(other) => {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: other.shape().to_vec(),
            });
        }
        None => None,
    };
    let mut weight_spectrum = None;
    let mut implicit_weight_spectrum =
        (any_calwt && !has_weight_spectrum).then(|| ArrayD::from_elem(data.raw_dim(), 1.0_f32));
    let data_spw = plan
        .measurement_set_spectral_windows
        .iter()
        .find(|spw| spw.spw_id == row.data_spw_id)
        .expect("planner guarantees selected MS spectral windows");
    let data_channels =
        channel_grid.unwrap_or_else(|| DataChannelGrid::all(&data_spw.channel_frequencies_hz));
    let mut geometry_cache = geometry_cache;
    let mut fast_gain_pairs: Vec<FastGainPair> = Vec::new();

    if any_calwt {
        let Some(weight_values) = weight.as_ref() else {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: Vec::new(),
            });
        };
        if weight_values.ndim() != 1 || weight_values.shape()[0] != correlation_types.len() {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: weight_values.shape().to_vec(),
            });
        }
        if has_weight_spectrum {
            weight_spectrum = Some(match original_weight_spectrum {
                Some(ArrayValue::Float32(values)) if values.shape() == data.shape() => {
                    values.clone()
                }
                None => expand_weight_to_spectrum(weight_values, data.shape()[1]),
                Some(other) => {
                    return Err(ApplyExecutionError::UnsupportedParameterShape {
                        path: "<measurement-set SIGMA_SPECTRUM>".into(),
                        shape: other.shape().to_vec(),
                    });
                }
            });
        }
    }
    if let Some(profile) = compute_profile.as_deref_mut() {
        profile.rows += 1;
        ApplyRowComputeProfile::add_elapsed(&mut profile.setup_ns, setup_started_at);
    }

    let mut output = ApplyBuffer {
        data_desc_id: row.data_desc_id,
        correlation_types,
        data_channels,
        corrected: &mut corrected,
        flags: &mut flags,
        propagate_flags,
        newly_flagged_samples: &mut newly_flagged_samples,
    };
    let mut pending_count = None;
    let mut parang_registration = 0;
    for &step in &chain.steps {
        if let Some((term, flags_in)) = pending_count.take() {
            // The fast gain kernel may defer this term's effect. Finish it
            // before measuring the flags that the next term receives.
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            activity.as_deref_mut().expect("tracked term").count_term(
                term,
                data.len(),
                flags_in,
                initial_flags + *output.newly_flagged_samples,
            );
        }
        let ApplyStep::Table(table_index) = step else {
            if let Some(activity) = activity.as_deref_mut() {
                pending_count = Some((
                    activity.parallactic_term(parang_registration),
                    initial_flags + *output.newly_flagged_samples,
                ));
                parang_registration += 1;
            }
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            apply_parallactic_step(
                row,
                chain.parang.as_ref().expect("planned parallactic angle"),
                &mut output,
                forward_model,
                compute_profile.as_deref_mut(),
            )?;
            continue;
        };
        let table_plan = &plan.calibration_tables[table_index];
        let loaded_table = &chain.tables[table_index];
        let legacy_scan = chain
            .legacy_scans
            .get(&(table_index, row.row_index))
            .copied();
        let scan = legacy_scan.unwrap_or(row.scan_number);
        if !table_plan.applies_to_scan(row, scan)
            || ((forward_model || skip_unavailable_terms)
                && !availability::table_available(row, table_plan))
        {
            continue;
        }
        if let Some(activity) = activity.as_deref_mut() {
            pending_count = Some((
                activity.table_term(table_index),
                initial_flags + *output.newly_flagged_samples,
            ));
        }
        let calibrate_weights = !trial
            && plan.apply_mode.writes_corrected_data()
            && table_plan.calwt
            && loaded_table.supports_calwt
            // TfOpac requests sigma reset and writeback with unity scaling.
            && table_plan.summary.table_subtype != "TfOpac";
        if let Some(profile) = compute_profile.as_deref_mut() {
            profile.table_applications += 1;
        }
        let lookup_started_at = compute_profile.as_ref().map(|_| Instant::now());
        let cal_spw_id = table_plan
            .spw_mapping
            .iter()
            .find(|mapping| mapping.data_spw_id == row.data_spw_id)
            .map(|mapping| mapping.calibration_spw_id)
            .expect("planner guarantees spw mapping for selected rows");
        let cal_spw = table_plan
            .calibration_spectral_windows
            .iter()
            .find(|spw| spw.spw_id == cal_spw_id)
            .expect("planner guarantees mapped caltable spectral windows");

        let mut field_id = table_plan
            .resolved_nearest_gainfields
            .iter()
            .find(|mapping| mapping.measurement_set_field_id == row.field_id)
            .map(|mapping| mapping.calibration_field_id)
            .unwrap_or(-1);

        if let Some(library) = table_plan
            .spec
            .callibrary
            .as_ref()
            .filter(|library| !library.field_map.is_empty())
        {
            field_id = usize::try_from(row.field_id)
                .ok()
                .and_then(|id| library.field_map.get(id))
                .copied()
                .unwrap_or(-1);
        }

        let subtype = table_plan.summary.table_subtype.as_str();
        let is_spline = subtype == "GSPLINE";
        let field_id = if is_spline { row.field_id } else { field_id };
        let is_tsys = subtype == "B TSYS";
        let is_sky = crate::sd_solver::is_sky_subtype(subtype);
        let is_switched_power = subtype == "G EVLASWPOW";
        let position_angle = subtype == "PosAng Jones";
        let channelized_polarization = polarization::JonesMatrix::is_channelized(subtype)
            || matches!(subtype, "Xf Jones" | "Xfparang Jones" | "GlinXphf Jones")
            || position_angle;
        let bounded_frequency_cache = is_tsys
            || is_sky
            || channelized_polarization
            || is_spline
            || matches!(subtype, "Mf Mueller" | "TfOpac" | additive::SUBTYPE);
        let evaluation_frequency_hz = if is_spline {
            let frequencies = data_channels.frequencies_hz;
            frequencies.iter().sum::<f64>() / frequencies.len() as f64
        } else {
            cal_spw_reference_frequency_hz(cal_spw)
        };
        let lookup = |antenna, cache: Option<&mut RowGeometryCache>| {
            let mut key = calibration_lookup_key(
                table_index,
                field_id,
                cal_spw_id,
                antenna,
                row.time_seconds,
                table_plan.interp,
            );
            key.observation_id = row.observation_id;
            key.scan_number = scan;
            key.data_spw_id = row.data_spw_id;
            let calculate = || {
                fields::lookup(
                    loaded_table,
                    field_id,
                    cal_spw_id,
                    antenna,
                    (row, scan),
                    data_spw,
                    evaluation_frequency_hz,
                )
            };
            if let Some(cache) = cache {
                cache.calibration_lookup(
                    key,
                    bounded_frequency_cache.then(|| row.time_seconds.to_bits()),
                    calculate,
                )
            } else {
                calculate()
            }
        };
        if subtype == "X Mueller" {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            let solution = lookup(0, geometry_cache.as_deref_mut())?;
            polarization::apply_x_mueller(
                solution.as_ref().map(|solution| solution.grid.as_ref()),
                &loaded_table.path,
                forward_model,
                &mut output,
            )?;
            continue;
        }
        if loaded_table.baseline {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            let antenna1 = fields::mapped_antenna(loaded_table, row.antenna1);
            let antenna2 = fields::mapped_antenna(loaded_table, row.antenna2);
            let solution = match baseline::index(antenna1, antenna2) {
                Some(index) => lookup(index, geometry_cache.as_deref_mut())?,
                None => None,
            };
            if let Some(profile) = compute_profile.as_deref_mut() {
                ApplyRowComputeProfile::add_elapsed(
                    &mut profile.table_lookup_ns,
                    lookup_started_at,
                );
            }
            if subtype == "A Mueller" && cal_spw.num_chan > 1 {
                if !forward_model && (trial || !plan.apply_mode.writes_corrected_data()) {
                    return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                        path: loaded_table.path.display().to_string(),
                        reason: "trial apply is unavailable for additive polynomial tables".into(),
                    });
                }
                additive_polynomial::apply(
                    solution.as_ref().map(|solution| solution.grid.as_ref()),
                    cal_spw,
                    &loaded_table.path,
                    antenna1 > antenna2,
                    forward_model,
                    chain.legacy_corruption,
                    &mut output,
                )?;
                continue;
            }
            let grid = baseline::prepare(
                solution,
                &cal_spw.channel_frequencies_hz,
                data_spw,
                geometry_cache.as_deref_mut(),
                loaded_table,
            )?;
            if subtype == additive::SUBTYPE {
                let add = chain.additive.as_ref().map_or(forward_model, |state| {
                    state.direction(
                        (table_index, row.data_spw_id),
                        additive::Context::new(loaded_table, row, field_id, cal_spw_id),
                        loaded_table,
                        forward_model,
                        geometry_cache
                            .as_mut()
                            .map(|cache| &mut cache.additive_matrices),
                    )
                });
                additive::apply(
                    grid.as_deref(),
                    &loaded_table.path,
                    antenna1 > antenna2,
                    add,
                    forward_model && chain.legacy_corruption,
                    &mut output,
                )?;
            } else if subtype == baseline::NOISE_SUBTYPE {
                baseline::apply_noise(
                    grid.as_deref(),
                    &loaded_table.path,
                    antenna1 > antenna2,
                    forward_model,
                    &mut output,
                )?;
            } else {
                baseline::apply(
                    grid.as_deref(),
                    &loaded_table.path,
                    antenna1 > antenna2,
                    forward_model,
                    &mut output,
                )?;
            }
            continue;
        }
        let ant1 = lookup(row.antenna1, geometry_cache.as_deref_mut())?;
        let ant2 = if is_sky {
            ant1.clone()
        } else {
            lookup(row.antenna2, geometry_cache.as_deref_mut())?
        };
        if let Some(profile) = compute_profile.as_deref_mut() {
            ApplyRowComputeProfile::add_elapsed(&mut profile.table_lookup_ns, lookup_started_at);
        }

        let (Some(ant1), Some(ant2)) = (ant1, ant2) else {
            if propagate_flags {
                flush_fast_gain_pairs(
                    &mut fast_gain_pairs,
                    &mut output,
                    compute_profile.as_deref_mut(),
                )?;
                for corr_index in 0..data.shape()[0] {
                    for chan_index in 0..data.shape()[1] {
                        if !output.flags[[corr_index, chan_index]] {
                            output.flags[[corr_index, chan_index]] = true;
                            *output.newly_flagged_samples += 1;
                        }
                    }
                }
            }
            continue;
        };

        // CASA samples BPOLY on its CAL_DESC window and then interpolates
        // the resulting B grid onto the data window.
        let is_bandpass = is_tsys || matches!(subtype, "B Jones" | "BPOLY");
        let resample_frequency =
            is_bandpass || is_sky || channelized_polarization || subtype == "TfOpac";
        let first_receptor_only = (is_bandpass
            || is_switched_power
            || polarization::is_cross_hand_phase(subtype)
            || matches!(subtype, "GSPLINE" | "GlinXph Jones" | "GlinXphf Jones")
            || (subtype == "G Jones"
                && table_plan.summary.keywords.casa_version.as_deref()
                    != Some(crate::constants::LEGACY_NATIVE_G_VERSION)))
            && correlation_types.len() == 1;
        let (ant1_transient, ant2_transient) = (
            bounded_frequency_cache && ant1.interpolated,
            bounded_frequency_cache && ant2.interpolated,
        );
        let ordered_tsys = is_tsys && chain.single_dish_solve;
        let ordered_gains = ordered_tsys
            || (!forward_model
                && table_plan.spec.callibrary.is_some()
                && matches!(subtype, "B Jones" | "G Jones" | "T Jones"));
        let raw_jones = ordered_gains.then_some([
            (Arc::as_ptr(&ant1.grid), !ant1.interpolated),
            (Arc::as_ptr(&ant2.grid), !ant2.interpolated),
        ]);
        let (ant1, ant2) = (ant1.grid, ant2.grid);
        let bandpass_weights = if is_bandpass && !is_tsys && calibrate_weights {
            let mut scratch = bandpass::PowerCache::default();
            geometry_cache
                .as_deref_mut()
                .map_or(&mut scratch, |cache| &mut cache.bandpass_power)
                .factors(
                    table_index,
                    (row.antenna1, &ant1),
                    (row.antenna2, &ant2),
                    correlation_types,
                )
        } else {
            None
        };
        let (ant1, ant2) = if resample_frequency {
            let ant1 = bandpass::prepare(
                ant1,
                &cal_spw.channel_frequencies_hz,
                data_spw,
                geometry_cache.as_deref_mut(),
                loaded_table,
                ant1_transient,
                ordered_gains,
            )?;
            let ant2 = bandpass::prepare(
                ant2,
                &cal_spw.channel_frequencies_hz,
                data_spw,
                geometry_cache.as_deref_mut(),
                loaded_table,
                ant2_transient,
                ordered_gains,
            )?;
            (ant1, ant2)
        } else {
            (ant1, ant2)
        };
        let bandpass_weights = if is_tsys && calibrate_weights && !has_weight_spectrum {
            bandpass::temperature_weight_factors(
                &ant1,
                &ant2,
                correlation_types,
                data_channels,
                ordered_tsys,
            )
        } else {
            bandpass_weights
        };
        let sampling_context = CalibrationSamplingContext {
            data_channels,
            cal_frequencies_hz: if resample_frequency {
                &data_spw.channel_frequencies_hz
            } else {
                &cal_spw.channel_frequencies_hz
            },
            cal_ref_frequency_hz: delay_reference_frequency(cal_spw, table_plan, loaded_table),
            // Prepared solutions already occupy the full MS grid. Temporal
            // interpolation happened at lookup; sample original bins directly.
            interp: if resample_frequency {
                ApplyInterpolationMode::Nearest
            } else {
                table_plan.interp
            },
            path: &loaded_table.path,
            engine: loaded_table.engine.as_deref(),
            antpos_tropo: loaded_table.antpos_tropo.as_ref(),
        };

        if is_sky {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            sky::apply_grid(&ant1, &loaded_table.path, &mut output, forward_model)?;
            if calibrate_weights && let Some(exposures) = &loaded_table.sky_exposures {
                let factor = exposures.weight_factor(row, cal_spw_id, loaded_table.interp);
                if let Some(spectrum) = &mut weight_spectrum {
                    *spectrum *= factor;
                }
                if let Some(spectrum) = &mut implicit_weight_spectrum {
                    *spectrum *= factor;
                }
            }
            continue;
        }
        if let Some(matrix) = polarization::JonesMatrix::from_subtype(subtype) {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            matrix.apply(
                row,
                [&ant1, &ant2],
                &sampling_context,
                &mut output,
                forward_model,
            )?;
            continue;
        }
        if position_angle {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            polarization::apply_position_angle(
                row,
                [&ant1, &ant2],
                &sampling_context,
                &mut output,
                forward_model,
            )?;
            continue;
        }
        if let Some(ionosphere) = &loaded_table.ionosphere {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            ionosphere.apply(
                row,
                [&ant1, &ant2],
                &sampling_context,
                &mut output,
                forward_model,
            )?;
            continue;
        }

        let materialize_started_at = compute_profile.as_ref().map(|_| Instant::now());
        if !forward_model
            && !ordered_gains
            && !calibrate_weights
            && !first_receptor_only
            && let Some(pair) = build_fast_gain_pair(
                table_index,
                &ant1,
                &ant2,
                row.field_id,
                row.antenna1,
                row.antenna2,
                row.time_seconds,
                loaded_table,
                reborrow_geometry_cache(&mut geometry_cache),
            )?
        {
            if let Some(profile) = compute_profile.as_deref_mut() {
                ApplyRowComputeProfile::add_elapsed(
                    &mut profile.row_dependent_grid_ns,
                    materialize_started_at,
                );
                profile.row_dependent_fast_pairs += 1;
            }
            fast_gain_pairs.push(pair);
            continue;
        }
        let ant1 = materialize_row_dependent_grid(
            ant1,
            row.field_id,
            row.antenna1,
            row.time_seconds,
            data_spw,
            loaded_table,
            geometry_cache.as_deref_mut(),
        )?;
        let ant2 = materialize_row_dependent_grid(
            ant2,
            row.field_id,
            row.antenna2,
            row.time_seconds,
            data_spw,
            loaded_table,
            geometry_cache.as_deref_mut(),
        )?;
        if let Some(profile) = compute_profile.as_deref_mut() {
            ApplyRowComputeProfile::add_elapsed(
                &mut profile.row_dependent_grid_ns,
                materialize_started_at,
            );
            profile.row_dependent_materialized_pairs += 1;
        }

        if let Some([raw_first, raw_second]) = raw_jones
            && matches!(ant1.as_ref(), CalibrationGrid::Complex(_))
            && matches!(ant2.as_ref(), CalibrationGrid::Complex(_))
        {
            flush_fast_gain_pairs(
                &mut fast_gain_pairs,
                &mut output,
                compute_profile.as_deref_mut(),
            )?;
            let mut scratch = ordered_gain::Cache::default();
            let cache = geometry_cache
                .as_deref_mut()
                .map_or(&mut scratch, |cache| &mut cache.ordered_gains);
            let form = |reconstruct| match (ordered_tsys, reconstruct) {
                (true, _) => ordered_gain::JonesForm::Correction,
                (false, true) => ordered_gain::JonesForm::ReconstructPolar,
                (false, false) => ordered_gain::JonesForm::Divisor,
            };
            let antennas = [
                ordered_gain::Antenna {
                    id: row.antenna1,
                    grid: &ant1,
                    form: form(raw_first.1 && raw_first.0 == Arc::as_ptr(&ant1)),
                },
                ordered_gain::Antenna {
                    id: row.antenna2,
                    grid: &ant2,
                    form: form(raw_second.1 && raw_second.0 == Arc::as_ptr(&ant2)),
                },
            ];
            if ordered_gain::apply(
                cache,
                antennas,
                ordered_gain::Options {
                    table: table_index,
                    calibrate_weights,
                    bandpass_weights: bandpass_weights.as_deref(),
                    first_receptor_only,
                    zero_flagged_weights: ordered_tsys && has_weight_spectrum,
                },
                &mut output,
                [weight_spectrum.as_mut(), implicit_weight_spectrum.as_mut()],
            )? {
                continue;
            }
        }

        if !forward_model
            && !first_receptor_only
            && let (CalibrationGrid::Complex(first), CalibrationGrid::Complex(second)) =
                (ant1.as_ref(), ant2.as_ref())
        {
            let scalar_weights =
                !is_bandpass && first.channel_count == 1 && second.channel_count == 1;
            if !calibrate_weights || scalar_weights {
                if calibrate_weights {
                    weights::apply_scalar_pair(
                        first,
                        second,
                        correlation_types,
                        row.data_desc_id,
                        table_plan.summary.table_subtype == "EPowerCurve",
                        weight_spectrum.as_mut(),
                        implicit_weight_spectrum.as_mut(),
                    )?;
                }
                fast_gain_pairs.push(FastGainPair::Complex(ant1, ant2));
                continue;
            }
        }

        flush_fast_gain_pairs(
            &mut fast_gain_pairs,
            &mut output,
            compute_profile.as_deref_mut(),
        )?;

        let generic_apply_started_at = compute_profile.as_ref().map(|_| Instant::now());
        for corr_index in 0..data.shape()[0] {
            let receptors = if first_receptor_only {
                bandpass::receptors(correlation_types, corr_index)
            } else {
                correlation_receptors(correlation_types[corr_index])
            }
            .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
                data_desc_id: row.data_desc_id,
                correlation_types: correlation_types
                    .iter()
                    .map(|code| stokes_name(*code).to_string())
                    .collect(),
            })?;
            let switched_power_weight = match (ant1.as_ref(), ant2.as_ref()) {
                (CalibrationGrid::SwitchedPower(left), CalibrationGrid::SwitchedPower(right)) => {
                    Some(left.weight_scale(receptors.0) * right.weight_scale(receptors.1))
                }
                _ => None,
            };

            for chan_index in 0..data.shape()[1] {
                let gain1 = ant1.sample(
                    receptors.0,
                    chan_index,
                    row.field_id,
                    row.antenna1,
                    row.time_seconds,
                    &sampling_context,
                )?;
                let gain2 = ant2.sample(
                    receptors.1,
                    chan_index,
                    row.field_id,
                    row.antenna2,
                    row.time_seconds,
                    &sampling_context,
                )?;

                let gain = if forward_model {
                    gain_validity::forward_gain
                } else {
                    correcting_gain
                };
                let (value1, flag1) = gain(gain1.value, gain1.flagged);
                let (value2, flag2) = gain(gain2.value, gain2.flagged);
                if (flag1 || flag2) && propagate_flags && !output.flags[[corr_index, chan_index]] {
                    output.flags[[corr_index, chan_index]] = true;
                    *output.newly_flagged_samples += 1;
                }

                let factor = value1 * value2.conj();
                if forward_model {
                    output.corrected[[corr_index, chan_index]] *= factor;
                } else {
                    output.corrected[[corr_index, chan_index]] /= factor;
                }
                if calibrate_weights {
                    let factor = if is_tsys && has_weight_spectrum && (flag1 || flag2) {
                        0.0
                    } else {
                        switched_power_weight
                            .or_else(|| {
                                bandpass_weights.as_ref().map(|factors| factors[corr_index])
                            })
                            .unwrap_or_else(|| value1.norm_sqr() * value2.norm_sqr())
                    };
                    if let Some(weight_spectrum) = weight_spectrum.as_mut() {
                        weight_spectrum[[corr_index, chan_index]] *= factor;
                    }
                    if let Some(implicit_weight_spectrum) = implicit_weight_spectrum.as_mut() {
                        implicit_weight_spectrum[[corr_index, chan_index]] *= factor;
                    }
                }
            }
        }
        if let Some(profile) = compute_profile.as_deref_mut() {
            ApplyRowComputeProfile::add_elapsed(
                &mut profile.generic_sample_apply_ns,
                generic_apply_started_at,
            );
        }
    }

    flush_fast_gain_pairs(
        &mut fast_gain_pairs,
        &mut output,
        compute_profile.as_deref_mut(),
    )?;
    if let Some((term, flags_in)) = pending_count {
        activity.expect("tracked term").count_term(
            term,
            data.len(),
            flags_in,
            initial_flags + *output.newly_flagged_samples,
        );
    }

    if any_calwt {
        let weight_finalize_started_at = compute_profile.as_ref().map(|_| Instant::now());
        let weight_values = weight
            .as_mut()
            .expect("validated WEIGHT availability when calwt is enabled");
        if let Some(weight_spectrum_values) = weight_spectrum.as_ref() {
            for corr_index in 0..weight_values.shape()[0] {
                let samples = (0..weight_spectrum_values.shape()[1])
                    .map(|chan_index| weight_spectrum_values[[corr_index, chan_index]])
                    .collect::<Vec<_>>();
                weight_values[[corr_index]] = weights::lower_median(samples);
            }
        } else if let Some(implicit_weight_spectrum) = implicit_weight_spectrum.as_ref() {
            for corr_index in 0..weight_values.shape()[0] {
                let samples = (0..implicit_weight_spectrum.shape()[1])
                    .map(|chan_index| implicit_weight_spectrum[[corr_index, chan_index]])
                    .collect::<Vec<_>>();
                weight_values[[corr_index]] *= weights::lower_median(samples);
            }
        }
        if let Some(profile) = compute_profile {
            ApplyRowComputeProfile::add_elapsed(
                &mut profile.weight_finalize_ns,
                weight_finalize_started_at,
            );
        }
    }

    if trial && plan.apply_mode.writes_corrected_data() {
        corrected.assign(data);
    }
    let flags_changed = newly_flagged_samples > 0;
    let row_became_fully_flagged = flags_changed && flags.iter().all(|flag| *flag);
    Ok(ExecutionRowResult {
        corrected_data: ArrayValue::Complex32(corrected),
        updated_flags: (plan.apply_mode.writes_flags() && flags_changed)
            .then_some(ArrayValue::Bool(flags)),
        updated_weight: any_calwt.then(|| ArrayValue::Float32(weight.expect("calwt weight"))),
        updated_weight_spectrum: weight_spectrum.map(ArrayValue::Float32),
        newly_flagged_samples,
        row_became_fully_flagged,
    })
}

pub(super) fn flush_fast_gain_pairs(
    pairs: &mut Vec<FastGainPair>,
    output: &mut ApplyBuffer<'_>,
    profile: Option<&mut ApplyRowComputeProfile>,
) -> Result<(), ApplyExecutionError> {
    if !pairs.is_empty() {
        let started = profile.as_ref().map(|_| Instant::now());
        apply_complex_gain_pairs_fast(pairs, output)?;
        if let Some(profile) = profile {
            ApplyRowComputeProfile::add_elapsed(&mut profile.fast_gain_apply_ns, started);
        }
        pairs.clear();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_row_prefetched(
    row: &ApplyRowPlan,
    correlation_types: &[i32],
    inputs: &PrefetchedExecutionRowInputs,
    has_weight_spectrum: bool,
    plan: &ApplyPlan,
    chain: &ApplyChain,
    geometry_cache: Option<&mut RowGeometryCache>,
    compute_profile: Option<&mut ApplyRowComputeProfile>,
    activity: Option<&mut ToolApplyActivity>,
) -> Result<ExecutionRowResult, ApplyExecutionError> {
    apply_row(
        row,
        ExecutionRowInputs {
            correlation_types,
            data: &inputs.data,
            original_flags: inputs.original_flags.as_ref(),
            original_weight: inputs.original_weight.as_ref(),
            original_weight_spectrum: inputs.original_weight_spectrum.as_ref(),
            has_weight_spectrum,
            preserved_corrected_data: inputs.preserved_corrected_data.as_ref(),
            preserved_weight_spectrum: inputs.preserved_weight_spectrum.as_ref(),
            channel_grid: None,
            forward_model: false,
            skip_unavailable_terms: false,
            trial: inputs.trial,
            parang_calwt: inputs.parang_calwt,
        },
        plan,
        chain,
        geometry_cache,
        compute_profile,
        activity,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn materialize_row_dependent_grid(
    grid: Arc<CalibrationGrid>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    data_spw: &crate::plan::SpectralWindowPlan,
    table: &LoadedCalibrationTable,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<Arc<CalibrationGrid>, ApplyExecutionError> {
    let engine = table.engine.as_deref();
    let path = &table.path;
    let mut geometry_cache = geometry_cache;
    let transient = matches!(grid.as_ref(), CalibrationGrid::SpectralOpacity(_));
    let cache_key =
        materialized_grid_key(&grid, field_id, antenna_id, time_seconds, data_spw.spw_id);
    if let (Some(cache), Some(key)) = (&geometry_cache, cache_key)
        && let Some(grid) = cache.materialized_grid(key, transient)
    {
        return Ok(grid);
    }
    let result = match grid.as_ref() {
        CalibrationGrid::Antpos(grid) => {
            let Some(engine) = engine else {
                return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                    path: path.display().to_string(),
                    reason: "KAntPos Jones apply requires MeasurementSet geometry".to_string(),
                });
            };
            let delay_ns = antpos_delay_ns(
                engine,
                field_id,
                antenna_id,
                time_seconds,
                grid.offsets_m.map(f64::from),
                path,
                table.antpos_tropo.as_ref(),
                geometry_cache.as_deref_mut(),
            )?;
            let values = data_spw
                .channel_frequencies_hz
                .iter()
                .map(|frequency_hz| {
                    let phase_rad = 2.0 * std::f64::consts::PI * delay_ns * (*frequency_hz / 1.0e9);
                    Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32)
                })
                .collect::<Vec<_>>();
            let channel_count = values.len();
            Ok(Arc::new(CalibrationGrid::Complex(GainGrid {
                receptor_count: 1,
                channel_count,
                values: ArrayD::from_shape_vec(IxDyn(&[1, channel_count]).f(), values)
                    .expect("antpos materialized grid shape is valid"),
                flags: ArrayD::from_elem(IxDyn(&[1, channel_count]).f(), grid.flagged),
            })))
        }
        CalibrationGrid::GainCurve(grid) => {
            let Some(engine) = engine else {
                return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                    path: path.display().to_string(),
                    reason: "EGainCurve apply requires MeasurementSet geometry".to_string(),
                });
            };
            let elevation = if let Some(cache) = geometry_cache.as_deref_mut() {
                cache.elevation(engine, time_seconds, field_id, antenna_id)
            } else {
                engine
                    .azel(
                        time_seconds,
                        usize::try_from(field_id).unwrap_or(usize::MAX),
                        usize::try_from(antenna_id).unwrap_or(usize::MAX),
                    )
                    .map(|(_az, elevation)| elevation)
            }
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: path.display().to_string(),
                source,
            })?;
            let za_degrees = (std::f64::consts::FRAC_PI_2 - elevation).to_degrees();
            let mut values = Vec::with_capacity(grid.receptor_count);
            let mut flags = Vec::with_capacity(grid.receptor_count);
            for receptor in 0..grid.receptor_count {
                let (gain, flagged) = grid.evaluate(receptor, za_degrees);
                values.push(Complex32::new(gain, 0.0));
                flags.push(flagged);
            }
            Ok(Arc::new(CalibrationGrid::Complex(GainGrid {
                receptor_count: grid.receptor_count,
                channel_count: 1,
                values: ArrayD::from_shape_vec(IxDyn(&[grid.receptor_count]).f(), values)
                    .expect("gaincurve materialized grid shape is valid"),
                flags: ArrayD::from_shape_vec(IxDyn(&[grid.receptor_count]).f(), flags)
                    .expect("gaincurve materialized flag shape is valid"),
            })))
        }
        CalibrationGrid::Opacity(grid) => {
            let gain = fast_opacity_gain(
                grid,
                engine,
                field_id,
                antenna_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            Ok(Arc::new(CalibrationGrid::Complex(GainGrid {
                receptor_count: 1,
                channel_count: 1,
                values: ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![Complex32::new(gain, 0.0)])
                    .expect("opacity materialized grid shape is valid"),
                flags: ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![grid.flagged])
                    .expect("opacity materialized flag shape is valid"),
            })))
        }
        CalibrationGrid::SpectralOpacity(tau) => {
            let elevation = opacity_elevation(
                engine,
                field_id,
                antenna_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            let mut gain = tau.clone();
            gain.values
                .mapv_inplace(|value| Complex32::new(opacity_gain(value.re, elevation), 0.));
            Ok(Arc::new(CalibrationGrid::Complex(gain)))
        }
        _ => Ok(Arc::clone(&grid)),
    };
    if let (Some(cache), Some(key), Ok(materialized)) =
        (&mut geometry_cache, cache_key, result.as_ref())
    {
        cache.insert_materialized_grid(key, Arc::clone(materialized), transient);
    }
    result
}
