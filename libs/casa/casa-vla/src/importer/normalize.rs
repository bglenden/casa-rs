// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn normalize_record(
    record: &LogicalRecord,
    options: &ImportVlaOptions,
    selection: &ArchiveSelection,
    measures: &dyn MeasuresProvider,
    frequency_tolerance: &mut f64,
) -> Result<Option<NormalizedRecord>, VlaError> {
    if !record_is_selected_for_import(record, options, selection)? {
        return Ok(None);
    }

    let rca = record.rca();
    let sda = record
        .sda()
        .map_err(|error| VlaError::import(format!("decode SDA: {error}")))?;
    let project = sda
        .observation_id()
        .map_err(|error| VlaError::import(format!("decode observation id: {error}")))?;
    let source_name = sda
        .source_name()
        .map_err(|error| VlaError::import(format!("decode source name: {error}")))?;

    let direction_epoch = sda
        .direction_epoch()
        .map_err(|error| VlaError::import(format!("decode direction epoch: {error}")))?;
    let source_direction = sda
        .source_direction_radians()
        .map_err(|error| VlaError::import(format!("decode source direction: {error}")))?;
    let source_direction = [
        normalize_longitude_radians(source_direction[0]),
        source_direction[1],
    ];
    let time_seconds = time_from_archive(
        rca.obs_day()
            .map_err(|error| VlaError::import(format!("decode obs day: {error}")))?,
        sda.observation_time_seconds()
            .map_err(|error| VlaError::import(format!("decode observation time: {error}")))?,
    );
    let integration_seconds = sda
        .integration_time_seconds()
        .map_err(|error| VlaError::import(format!("decode integration time: {error}")))?;

    let antennas = normalize_antennas(record, options.antnamescheme, measures)?;
    let groups = normalize_groups(record, options, antennas.len(), frequency_tolerance)?;
    if groups.is_empty() {
        return Ok(None);
    }

    Ok(Some(NormalizedRecord {
        project,
        source_name,
        calibration_code: sda
            .calibration_code()
            .map_err(|error| VlaError::import(format!("decode calibration code: {error}")))?,
        source_num_lines: count_valid_source_lines(record)?,
        source_direction,
        array_id: i32::from(
            sda.subarray_id()
                .map_err(|error| VlaError::import(format!("decode subarray id: {error}")))?,
        ) - 1,
        time_seconds,
        integration_seconds,
        direction_epoch,
        antennas,
        groups,
    }))
}

pub(super) fn record_is_selected_for_import(
    record: &LogicalRecord,
    options: &ImportVlaOptions,
    selection: &ArchiveSelection,
) -> Result<bool, VlaError> {
    if record
        .rca()
        .length_bytes()
        .map_err(|error| VlaError::import(format!("decode RCA length: {error}")))?
        == 0
    {
        return Ok(false);
    }
    let sda = record
        .sda()
        .map_err(|error| VlaError::import(format!("decode SDA: {error}")))?;
    let project = sda
        .observation_id()
        .map_err(|error| VlaError::import(format!("decode observation id: {error}")))?;
    if !selection.matches_project(&project) {
        return Ok(false);
    }
    let observation_mode = sda
        .observation_mode_code()
        .map_err(|error| VlaError::import(format!("decode observation mode: {error}")))?;
    if !is_supported_observation_mode(&observation_mode) {
        return Ok(false);
    }
    let source_name = sda
        .source_name()
        .map_err(|error| VlaError::import(format!("decode source name: {error}")))?;
    if source_name.is_empty() && !options.keepblanks {
        return Ok(false);
    }
    selection.matches_time_and_band(record)
}

pub(super) fn is_supported_observation_mode(code: &str) -> bool {
    matches!(
        code,
        "  " | "H " | "S " | "SP" | "VA" | "VB" | "VL" | "VR" | "VX"
    )
}

pub(super) fn normalize_antennas(
    record: &LogicalRecord,
    naming: AntennaNameScheme,
    measures: &dyn MeasuresProvider,
) -> Result<Vec<NormalizedAntenna>, VlaError> {
    let n_antennas = usize::from(
        record
            .rca()
            .n_antennas()
            .map_err(|error| VlaError::import(format!("decode antenna count: {error}")))?,
    );
    let observatory = MPosition::from_observatory_name(DEFAULT_TELESCOPE_NAME, measures)
        .map_err(|error| VlaError::import(error.to_string()))?
        .ok_or_else(|| {
            VlaError::import("resolve observatory position for VLA from measures catalog")
        })?;
    let [obs_x, obs_y, obs_z] = observatory.as_itrf();
    let longitude = observatory.longitude_rad();
    let (sin_lon, cos_lon) = longitude.sin_cos();

    let mut antennas = Vec::with_capacity(n_antennas);
    for archive_index in 0..n_antennas {
        let ada = record
            .ada(archive_index)
            .map_err(|error| VlaError::import(format!("decode ADA {archive_index}: {error}")))?;
        let bx = ada
            .bx_meters()
            .map_err(|error| VlaError::import(format!("decode ADA bx: {error}")))?;
        let by = ada
            .by_meters()
            .map_err(|error| VlaError::import(format!("decode ADA by: {error}")))?;
        let bz = ada
            .bz_meters()
            .map_err(|error| VlaError::import(format!("decode ADA bz: {error}")))?;
        let dx = cos_lon * bx - sin_lon * by;
        let dy = sin_lon * bx + cos_lon * by;
        let dz = bz;
        let name = ada
            .antenna_name(matches!(naming, AntennaNameScheme::New))
            .map_err(|error| VlaError::import(format!("decode antenna name: {error}")))?;
        antennas.push(NormalizedAntenna {
            antenna_id: ada
                .antenna_id()
                .map_err(|error| VlaError::import(format!("decode antenna id: {error}")))?,
            station: ada
                .pad_name()
                .map_err(|error| VlaError::import(format!("decode antenna pad name: {error}")))?,
            name,
            position_itrf_m: [obs_x + dx, obs_y + dy, obs_z + dz],
        });
    }
    Ok(antennas)
}

pub(super) fn count_valid_source_lines(record: &LogicalRecord) -> Result<i32, VlaError> {
    let mut count = 0_i32;
    let rca = record.rca();
    for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
        if rca
            .cda_offset_bytes(cda_id.index())
            .map_err(|error| VlaError::import(format!("decode CDA offset {:?}: {error}", cda_id)))?
            == 0
        {
            continue;
        }
        let cda = record
            .cda(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA {:?}: {error}", cda_id)))?;
        if cda.is_valid() {
            count += 1;
        }
    }
    Ok(count)
}

pub(super) fn normalize_groups(
    record: &LogicalRecord,
    options: &ImportVlaOptions,
    n_antennas: usize,
    frequency_tolerance: &mut f64,
) -> Result<Vec<NormalizedGroup>, VlaError> {
    let revision = record
        .rca()
        .revision()
        .map_err(|error| VlaError::import(format!("decode revision: {error}")))?;
    let cda_groups = collect_cda_groups(record, frequency_tolerance)?;

    let mut groups = Vec::with_capacity(cda_groups.len());
    for (descriptor, cda_ids) in cda_groups {
        let corr_types = correlation_types_for_group(record, &cda_ids, n_antennas)?;
        let shadowed = compute_shadowed_antennas(record, n_antennas)?;
        let mut baselines = Vec::new();
        let mut cross_index = 0_usize;
        for ant1 in 0..n_antennas {
            for ant2 in ant1..n_antennas {
                let is_cross = ant1 != ant2;
                if !is_cross && !options.autocorr {
                    continue;
                }
                let uvw_m = if is_cross {
                    let ada1 = record.ada(ant1).map_err(|error| {
                        VlaError::import(format!("decode ADA {ant1} for UVW: {error}"))
                    })?;
                    let ada2 = record.ada(ant2).map_err(|error| {
                        VlaError::import(format!("decode ADA {ant2} for UVW: {error}"))
                    })?;
                    [
                        ada1.u_meters()
                            .map_err(|error| VlaError::import(format!("decode u: {error}")))?
                            - ada2
                                .u_meters()
                                .map_err(|error| VlaError::import(format!("decode u: {error}")))?,
                        ada1.v_meters()
                            .map_err(|error| VlaError::import(format!("decode v: {error}")))?
                            - ada2
                                .v_meters()
                                .map_err(|error| VlaError::import(format!("decode v: {error}")))?,
                        ada1.w_meters()
                            .map_err(|error| VlaError::import(format!("decode w: {error}")))?
                            - ada2
                                .w_meters()
                                .map_err(|error| VlaError::import(format!("decode w: {error}")))?,
                    ]
                } else {
                    [0.0, 0.0, 0.0]
                };
                baselines.push(normalize_baseline_row(
                    record,
                    &descriptor,
                    &cda_ids,
                    &corr_types,
                    ant1,
                    ant2,
                    is_cross,
                    cross_index,
                    revision,
                    options.applytsys,
                    uvw_m,
                    &shadowed,
                )?);
                if is_cross {
                    cross_index += 1;
                }
            }
        }
        groups.push(NormalizedGroup {
            descriptor,
            cda_ids,
            baselines,
        });
    }

    Ok(groups)
}

pub(super) fn collect_cda_groups(
    record: &LogicalRecord,
    frequency_tolerance: &mut f64,
) -> Result<Vec<(SpectralDescriptor, Vec<CdaId>)>, VlaError> {
    let rca = record.rca();
    let sda = record
        .sda()
        .map_err(|error| VlaError::import(format!("decode SDA: {error}")))?;
    let mut cda_groups: Vec<(SpectralDescriptor, Vec<CdaId>)> = Vec::new();
    for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
        if rca
            .cda_offset_bytes(cda_id.index())
            .map_err(|error| VlaError::import(format!("decode CDA offset {:?}: {error}", cda_id)))?
            == 0
        {
            continue;
        }
        let cda = record
            .cda(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA {:?}: {error}", cda_id)))?;
        if !cda.is_valid() {
            continue;
        }
        if sda
            .n_polarizations(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA polarization count: {error}")))?
            == 0
        {
            continue;
        }
        let descriptor = SpectralDescriptor {
            edge_frequency_hz: sda.edge_frequency_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode edge frequency for {:?}: {error}", cda_id))
            })?,
            observed_frequency_hz: sda.observed_frequency_hz(cda_id).map_err(|error| {
                VlaError::import(format!(
                    "decode observed frequency for {:?}: {error}",
                    cda_id
                ))
            })?,
            channel_width_hz: sda.channel_width_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode channel width for {:?}: {error}", cda_id))
            })?,
            num_chan: usize::try_from(sda.n_channels(cda_id).map_err(|error| {
                VlaError::import(format!("decode channel count for {:?}: {error}", cda_id))
            })?)
            .map_err(|_| VlaError::import("channel count does not fit in usize"))?,
            if_conv_chain: i32::try_from(sda.electronic_path(cda_id).map_err(|error| {
                VlaError::import(format!("decode IF chain for {:?}: {error}", cda_id))
            })?)
            .map_err(|_| VlaError::import("IF conversion chain does not fit in i32"))?,
            rest_frequency_hz: sda.rest_frequency_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode rest frequency for {:?}: {error}", cda_id))
            })?,
            rest_frame: sda.rest_frame(cda_id).map_err(|error| {
                VlaError::import(format!("decode rest frame for {:?}: {error}", cda_id))
            })?,
            doppler_definition: sda.doppler_definition(cda_id).map_err(|error| {
                VlaError::import(format!(
                    "decode doppler definition for {:?}: {error}",
                    cda_id
                ))
            })?,
            doppler_velocity_mps: sda.radial_velocity_mps(cda_id).map_err(|error| {
                VlaError::import(format!("decode radial velocity for {:?}: {error}", cda_id))
            })?,
            doppler_tracking: sda.doppler_tracking(cda_id).map_err(|error| {
                VlaError::import(format!("decode doppler tracking for {:?}: {error}", cda_id))
            })?,
        };
        if *frequency_tolerance == 0.0 {
            *frequency_tolerance = descriptor.channel_width_hz;
        }
        if let Some((_, cdas)) = cda_groups.iter_mut().find(|(existing, _)| {
            same_spectral_descriptor(existing, &descriptor, *frequency_tolerance)
        }) {
            cdas.push(cda_id);
        } else {
            cda_groups.push((descriptor, vec![cda_id]));
        }
    }
    Ok(cda_groups)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn normalize_baseline_row(
    record: &LogicalRecord,
    descriptor: &SpectralDescriptor,
    cda_ids: &[CdaId],
    corr_types: &[i32],
    ant1: usize,
    ant2: usize,
    is_cross: bool,
    cross_index: usize,
    revision: u16,
    apply_tsys: bool,
    uvw_m: [f64; 3],
    shadowed: &[bool],
) -> Result<NormalizedBaselineRow, VlaError> {
    let sda = record
        .sda()
        .map_err(|error| VlaError::import(format!("decode SDA: {error}")))?;
    let n_corr = corr_types.len();
    let n_chan = descriptor.num_chan;
    let mut values = vec![Complex32::new(0.0, 0.0); n_corr * n_chan];
    let model_values = build_model_data(corr_types, n_chan);
    let mut flags = vec![false; n_corr * n_chan];
    let mut flag_levels = vec![false; n_corr * n_chan * DEFAULT_FLAG_CATEGORIES];
    let mut weights = vec![0.0_f32; n_corr];
    let mut sigmas = vec![0.0_f32; n_corr];

    let mut is_scaled_by_nominal_sensitivity = false;
    for &cda_id in cda_ids {
        for usage in sda.if_usage(cda_id).map_err(|error| {
            VlaError::import(format!("decode IF usage for {:?}: {error}", cda_id))
        })? {
            let a1 = record.ada(ant1).map_err(|error| {
                VlaError::import(format!("decode ADA {ant1} nominal sensitivity: {error}"))
            })?;
            let a2 = record.ada(ant2).map_err(|error| {
                VlaError::import(format!("decode ADA {ant2} nominal sensitivity: {error}"))
            })?;
            is_scaled_by_nominal_sensitivity |= a1
                .nominal_sensitivity_applied(usage.ant1, revision)
                .map_err(|error| {
                    VlaError::import(format!(
                        "decode nominal sensitivity state for antenna {ant1}: {error}"
                    ))
                })?;
            is_scaled_by_nominal_sensitivity |= a2
                .nominal_sensitivity_applied(usage.ant2, revision)
                .map_err(|error| {
                    VlaError::import(format!(
                        "decode nominal sensitivity state for antenna {ant2}: {error}"
                    ))
                })?;
        }
    }

    for &cda_id in cda_ids {
        let cda = record
            .cda(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA {:?}: {error}", cda_id)))?;
        let baseline = if is_cross {
            cda.cross_corr(cross_index)
        } else {
            cda.auto_corr(ant1)
        }
        .map_err(|error| VlaError::import(format!("decode baseline: {error}")))?;
        let mut baseline_data = baseline
            .data()
            .map_err(|error| VlaError::import(format!("decode baseline data: {error}")))?;
        let stokes = record.stokes_products(cda_id, 0, 0).map_err(|error| {
            VlaError::import(format!("derive Stokes products for {:?}: {error}", cda_id))
        })?;
        let if_usage = sda
            .if_usage(cda_id)
            .map_err(|error| VlaError::import(format!("decode IF usage: {error}")))?;

        if baseline_data.len() == stokes.len() && n_chan == 1 {
            for (product_index, (stokes_product, usage)) in
                stokes.iter().zip(if_usage.iter()).enumerate()
            {
                let corr_index = corr_types
                    .iter()
                    .position(|code| *code == stokes_code(*stokes_product))
                    .ok_or_else(|| {
                        VlaError::import("correlation type not found in continuum row")
                    })?;
                let bl_sensitivity =
                    baseline_sensitivity(record, ant1, ant2, *usage, revision).unwrap_or(1.0);
                let mut value = baseline_data[product_index];
                let (weight, sigma) = visibility_weight_sigma(
                    descriptor.channel_width_hz,
                    sda.integration_time_seconds().map_err(|error| {
                        VlaError::import(format!("decode integration time: {error}"))
                    })?,
                    bl_sensitivity,
                    apply_tsys,
                    is_scaled_by_nominal_sensitivity,
                );
                if apply_tsys {
                    if !is_scaled_by_nominal_sensitivity {
                        value *= bl_sensitivity.sqrt();
                    }
                } else if is_scaled_by_nominal_sensitivity && bl_sensitivity > 1.0e-10 {
                    value /= bl_sensitivity.sqrt();
                }
                let (product_flag, product_categories) =
                    product_flagging(record, ant1, ant2, *usage, shadowed)?;
                values[corr_index] = value;
                weights[corr_index] = weight;
                sigmas[corr_index] = sigma;
                flags[corr_index] = product_flag;
                for (category_index, flagged) in product_categories.iter().copied().enumerate() {
                    flag_levels
                        [flag_category_index(corr_index, 0, category_index, n_corr, n_chan)] =
                        flagged;
                }
            }
        } else if stokes.len() == 1 && baseline_data.len() == n_chan {
            let corr_index = corr_types
                .iter()
                .position(|code| *code == stokes_code(stokes[0]))
                .ok_or_else(|| VlaError::import("correlation type not found in spectral row"))?;
            let bl_sensitivity =
                baseline_sensitivity(record, ant1, ant2, if_usage[0], revision).unwrap_or(1.0);
            let (weight, sigma) = visibility_weight_sigma(
                descriptor.channel_width_hz,
                sda.integration_time_seconds().map_err(|error| {
                    VlaError::import(format!("decode integration time: {error}"))
                })?,
                bl_sensitivity,
                apply_tsys,
                is_scaled_by_nominal_sensitivity,
            );
            let (product_flag, product_categories) =
                product_flagging(record, ant1, ant2, if_usage[0], shadowed)?;
            for (chan_index, value) in baseline_data.iter_mut().enumerate() {
                if apply_tsys {
                    if !is_scaled_by_nominal_sensitivity {
                        *value *= bl_sensitivity.sqrt();
                    }
                } else if is_scaled_by_nominal_sensitivity && bl_sensitivity > 1.0e-10 {
                    *value /= bl_sensitivity.sqrt();
                }
                values[corr_index + chan_index * n_corr] = *value;
                flags[corr_index + chan_index * n_corr] = product_flag;
                for (category_index, flagged) in product_categories.iter().copied().enumerate() {
                    flag_levels[flag_category_index(
                        corr_index,
                        chan_index,
                        category_index,
                        n_corr,
                        n_chan,
                    )] = flagged;
                }
            }
            weights[corr_index] = weight;
            sigmas[corr_index] = sigma;
        } else {
            return Err(VlaError::import(format!(
                "unsupported CDA/data layout for {:?}: data={} stokes={} nchan={n_chan}",
                cda_id,
                baseline_data.len(),
                stokes.len()
            )));
        }
    }

    let data = ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), values)
            .map_err(|error| VlaError::import(format!("shape data array: {error}")))?,
    );
    let corrected_data = data.clone();
    let model_data = ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), model_values)
            .map_err(|error| VlaError::import(format!("shape model-data array: {error}")))?,
    );
    let flag = ArrayValue::Bool(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), flags)
            .map_err(|error| VlaError::import(format!("shape flag array: {error}")))?,
    );
    let flag_category = ArrayValue::Bool(
        ArrayD::from_shape_vec(
            IxDyn(&[n_corr, n_chan, DEFAULT_FLAG_CATEGORIES]).f(),
            flag_levels,
        )
        .map_err(|error| VlaError::import(format!("shape flag-category array: {error}")))?,
    );
    let weight = ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr]).f(), weights)
            .map_err(|error| VlaError::import(format!("shape weight array: {error}")))?,
    );
    let sigma = ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr]).f(), sigmas)
            .map_err(|error| VlaError::import(format!("shape sigma array: {error}")))?,
    );
    Ok(NormalizedBaselineRow {
        antenna1_archive: ant1,
        antenna2_archive: ant2,
        corr_types: corr_types.to_vec(),
        uvw_m,
        data,
        corrected_data,
        model_data,
        flag,
        flag_category,
        weight,
        sigma,
    })
}
