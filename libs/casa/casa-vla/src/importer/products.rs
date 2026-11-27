// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn compute_shadowed_antennas(
    record: &LogicalRecord,
    n_antennas: usize,
) -> Result<Vec<bool>, VlaError> {
    let mut shadowed = vec![false; n_antennas];
    for ant1 in 0..n_antennas {
        let ada1 = record.ada(ant1).map_err(|error| {
            VlaError::import(format!("decode ADA {ant1} for shadowing: {error}"))
        })?;
        for ant2 in (ant1 + 1)..n_antennas {
            let ada2 = record.ada(ant2).map_err(|error| {
                VlaError::import(format!("decode ADA {ant2} for shadowing: {error}"))
            })?;
            let du = ada1
                .u_meters()
                .map_err(|error| VlaError::import(format!("decode u: {error}")))?
                - ada2
                    .u_meters()
                    .map_err(|error| VlaError::import(format!("decode u: {error}")))?;
            let dv = ada1
                .v_meters()
                .map_err(|error| VlaError::import(format!("decode v: {error}")))?
                - ada2
                    .v_meters()
                    .map_err(|error| VlaError::import(format!("decode v: {error}")))?;
            if du * du + dv * dv < 625.0 {
                let dw = ada1
                    .w_meters()
                    .map_err(|error| VlaError::import(format!("decode w: {error}")))?
                    - ada2
                        .w_meters()
                        .map_err(|error| VlaError::import(format!("decode w: {error}")))?;
                if dw > 0.0 {
                    shadowed[ant2] = true;
                } else {
                    shadowed[ant1] = true;
                }
            }
        }
    }
    Ok(shadowed)
}

pub(super) fn product_flagging(
    record: &LogicalRecord,
    ant1: usize,
    ant2: usize,
    usage: IfUsage,
    shadowed: &[bool],
) -> Result<(bool, [bool; DEFAULT_FLAG_CATEGORIES]), VlaError> {
    let ada1 = record
        .ada(ant1)
        .map_err(|error| VlaError::import(format!("decode ADA {ant1} for flagging: {error}")))?;
    let ada2 = record
        .ada(ant2)
        .map_err(|error| VlaError::import(format!("decode ADA {ant2} for flagging: {error}")))?;
    let status1 = ada1.if_status(usage.ant1).map_err(|error| {
        VlaError::import(format!("decode IF status for antenna {ant1}: {error}"))
    })?;
    let status2 = ada2.if_status(usage.ant2).map_err(|error| {
        VlaError::import(format!("decode IF status for antenna {ant2}: {error}"))
    })?;
    let shadow = shadowed.get(ant1).copied().unwrap_or(false)
        || shadowed.get(ant2).copied().unwrap_or(false);
    let categories = [
        (status1 & 0x01) != 0 || (status2 & 0x01) != 0,
        (status1 & 0x02) != 0 || (status2 & 0x02) != 0,
        (status1 & 0x04) != 0 || (status2 & 0x04) != 0,
        (status1 & 0x08) != 0 || (status2 & 0x08) != 0,
        shadow,
        false,
    ];
    Ok((categories[2] || categories[3] || shadow, categories))
}

pub(super) fn flag_category_index(
    corr_index: usize,
    chan_index: usize,
    category_index: usize,
    n_corr: usize,
    n_chan: usize,
) -> usize {
    corr_index + chan_index * n_corr + category_index * n_corr * n_chan
}

pub(super) fn stokes_code(stokes: StokesProduct) -> i32 {
    match stokes {
        StokesProduct::Rr => 5,
        StokesProduct::Rl => 6,
        StokesProduct::Lr => 7,
        StokesProduct::Ll => 8,
    }
}

/// The imported rows' `MODEL_DATA`: CASA's initial model
/// ([`casa_ms::initial_model_visibility`]), channel by channel. CASA's
/// `importvla` fills it by position instead (`VLAFiller.cc`: with four
/// products the second and third are 0, otherwise every product is 1).
/// The two agree wherever cross hands come with all four products, which
/// this importer orders `RR RL LR LL`; a cross hand in a list of two would
/// hold 1 there and 0 here.
pub(super) fn build_model_data(corr_types: &[i32], n_chan: usize) -> Vec<Complex32> {
    let mut values = Vec::with_capacity(corr_types.len() * n_chan);
    for _ in 0..n_chan {
        values.extend(
            corr_types
                .iter()
                .map(|code| casa_ms::initial_model_visibility(*code)),
        );
    }
    values
}

pub(super) fn visibility_weight_sigma(
    channel_width_hz: f64,
    integration_seconds: f64,
    baseline_sensitivity: f32,
    apply_tsys: bool,
    _is_scaled_by_nominal_sensitivity: bool,
) -> (f32, f32) {
    let nominal_weight = (integration_seconds * 0.12 / 10_000.0 * channel_width_hz) as f32;
    if nominal_weight <= 0.0 {
        return (0.0, 0.0);
    }
    let mut weight = nominal_weight;
    let mut sigma = 1.0 / nominal_weight.sqrt();
    if apply_tsys && baseline_sensitivity > 1.0e-10 {
        weight /= baseline_sensitivity;
        sigma *= baseline_sensitivity.sqrt();
    }
    (weight, sigma)
}

pub(super) fn baseline_sensitivity(
    record: &LogicalRecord,
    ant1: usize,
    ant2: usize,
    usage: IfUsage,
    revision: u16,
) -> Option<f32> {
    let a1 = record.ada(ant1).ok()?;
    let a2 = record.ada(ant2).ok()?;
    let s1 = nominal_sensitivity_or_default(&a1, usage.ant1);
    let s2 = nominal_sensitivity_or_default(&a2, usage.ant2);
    let _scaled = a1.nominal_sensitivity_applied(usage.ant1, revision).ok()?
        || a2.nominal_sensitivity_applied(usage.ant2, revision).ok()?;
    Some(s1 * s2)
}

pub(super) fn nominal_sensitivity_or_default(
    ada: &crate::record::AntennaDataArea<'_>,
    if_id: IfId,
) -> f32 {
    ada.nominal_sensitivity(if_id)
        .ok()
        .filter(|value| *value > 1.0e-10)
        .unwrap_or(0.333)
}

pub(super) fn time_from_archive(obs_day: u32, obs_seconds: f64) -> f64 {
    f64::from(obs_day) * 86_400.0 + obs_seconds
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub(super) fn reorder_baseline_for_ms(
    antenna1_id: i32,
    antenna2_id: i32,
    data: ArrayValue,
    corrected: ArrayValue,
    model: ArrayValue,
    flag: ArrayValue,
    flag_category: ArrayValue,
    weight: ArrayValue,
    sigma: ArrayValue,
    corr_types: &[i32],
) -> Result<
    (
        i32,
        i32,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        ArrayValue,
        Vec<i32>,
    ),
    VlaError,
> {
    if antenna1_id <= antenna2_id {
        return Ok((
            antenna1_id,
            antenna2_id,
            data,
            corrected,
            model,
            flag,
            flag_category,
            weight,
            sigma,
            corr_types.to_vec(),
        ));
    }

    let swapped_corr_types = swap_rl_lr(corr_types);
    Ok((
        antenna2_id,
        antenna1_id,
        reorder_complex_array(&data, corr_types, &swapped_corr_types)?,
        reorder_complex_array(&corrected, corr_types, &swapped_corr_types)?,
        reorder_complex_array(&model, corr_types, &swapped_corr_types)?,
        reorder_flag_array_for_swapped_baseline(&flag, corr_types, &swapped_corr_types)?,
        flag_category,
        reorder_vector_array(&weight, corr_types, &swapped_corr_types)?,
        reorder_vector_array(&sigma, corr_types, &swapped_corr_types)?,
        swapped_corr_types,
    ))
}

pub(super) fn swap_rl_lr(corr_types: &[i32]) -> Vec<i32> {
    corr_types
        .iter()
        .map(|code| match code {
            6 => 7,
            7 => 6,
            _ => *code,
        })
        .collect()
}

pub(super) fn reorder_complex_array(
    value: &ArrayValue,
    current: &[i32],
    target: &[i32],
) -> Result<ArrayValue, VlaError> {
    let ArrayValue::Complex32(array) = value else {
        return Err(VlaError::import("expected Complex32 data array"));
    };
    let shape = array.shape();
    let n_corr = *shape
        .first()
        .ok_or_else(|| VlaError::import("complex array missing correlation axis"))?;
    let n_chan = *shape
        .get(1)
        .ok_or_else(|| VlaError::import("complex array missing channel axis"))?;
    let source = array
        .as_slice_memory_order()
        .ok_or_else(|| VlaError::import("complex array is not contiguous in memory order"))?;
    let reorder_map = reorder_index_map(current, target, "source array")?;
    let mut reordered = vec![Complex32::new(0.0, 0.0); n_corr * n_chan];
    for (new_index, &old_index) in reorder_map.iter().enumerate() {
        for chan in 0..n_chan {
            reordered[new_index + chan * n_corr] = source[old_index + chan * n_corr].conj();
        }
    }
    Ok(ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), reordered)
            .map_err(|error| VlaError::import(format!("shape reordered complex array: {error}")))?,
    ))
}

pub(super) fn reorder_flag_array_for_swapped_baseline(
    flag: &ArrayValue,
    current: &[i32],
    target: &[i32],
) -> Result<ArrayValue, VlaError> {
    if !(current.contains(&6) && current.contains(&7)) {
        return reorder_bool_array_generic(flag, current, target, "flag");
    }
    let ArrayValue::Bool(flag_array) = flag else {
        return Err(VlaError::import("expected Bool flag array"));
    };
    let shape = flag_array.shape();
    let n_corr = *shape
        .first()
        .ok_or_else(|| VlaError::import("flag array missing correlation axis"))?;
    let n_chan = *shape
        .get(1)
        .ok_or_else(|| VlaError::import("flag array missing channel axis"))?;
    let source = flag_array
        .as_slice_memory_order()
        .ok_or_else(|| VlaError::import("flag array is not contiguous in memory order"))?;
    let old_lr_index = current
        .iter()
        .position(|code| *code == 7)
        .ok_or_else(|| VlaError::import("swapped flag array missing LR correlation"))?;
    let reorder_map = reorder_index_map(current, target, "flag array")?;
    let mut reordered = vec![false; n_corr * n_chan];
    for (new_index, &mapped_index) in reorder_map.iter().enumerate() {
        let old_index = if target[new_index] == 6 {
            old_lr_index
        } else {
            mapped_index
        };
        for chan in 0..n_chan {
            reordered[new_index + chan * n_corr] = source[old_index + chan * n_corr];
        }
    }
    Ok(ArrayValue::Bool(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), reordered)
            .map_err(|error| VlaError::import(format!("shape reordered flag array: {error}")))?,
    ))
}

pub(super) fn reorder_bool_array_generic(
    value: &ArrayValue,
    current: &[i32],
    target: &[i32],
    label: &str,
) -> Result<ArrayValue, VlaError> {
    let ArrayValue::Bool(array) = value else {
        return Err(VlaError::import(format!("expected Bool {label} array")));
    };
    let shape = array.shape();
    let n_corr = *shape
        .first()
        .ok_or_else(|| VlaError::import(format!("{label} array missing correlation axis")))?;
    let n_chan = *shape
        .get(1)
        .ok_or_else(|| VlaError::import(format!("{label} array missing channel axis")))?;
    let source = array.as_slice_memory_order().ok_or_else(|| {
        VlaError::import(format!("{label} array is not contiguous in memory order"))
    })?;
    let reorder_map = reorder_index_map(current, target, label)?;
    let mut reordered = vec![false; n_corr * n_chan];
    for (new_index, &old_index) in reorder_map.iter().enumerate() {
        for chan in 0..n_chan {
            reordered[new_index + chan * n_corr] = source[old_index + chan * n_corr];
        }
    }
    Ok(ArrayValue::Bool(
        ArrayD::from_shape_vec(IxDyn(&[n_corr, n_chan]).f(), reordered)
            .map_err(|error| VlaError::import(format!("shape reordered {label} array: {error}")))?,
    ))
}

pub(super) fn flag_row_value(flag: &ArrayValue) -> Result<bool, VlaError> {
    let ArrayValue::Bool(array) = flag else {
        return Err(VlaError::import("expected Bool flag array"));
    };
    Ok(array.iter().all(|flagged| *flagged))
}

pub(super) fn reorder_vector_array(
    value: &ArrayValue,
    current: &[i32],
    target: &[i32],
) -> Result<ArrayValue, VlaError> {
    let source = match value {
        ArrayValue::Float32(array) => array
            .as_slice_memory_order()
            .ok_or_else(|| VlaError::import("vector array is not contiguous in memory order"))?,
        _ => return Err(VlaError::import("expected Float32 vector array")),
    };
    let reorder_map = reorder_index_map(current, target, "vector")?;
    let mut reordered = vec![0.0_f32; source.len()];
    for (new_index, &old_index) in reorder_map.iter().enumerate() {
        reordered[new_index] = source[old_index];
    }
    Ok(ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[reordered.len()]).f(), reordered)
            .map_err(|error| VlaError::import(format!("shape reordered vector array: {error}")))?,
    ))
}

pub(super) fn reorder_index_map(
    current: &[i32],
    target: &[i32],
    label: &str,
) -> Result<Vec<usize>, VlaError> {
    target
        .iter()
        .map(|code| {
            current
                .iter()
                .position(|existing| existing == code)
                .ok_or_else(|| {
                    VlaError::import(format!(
                        "target correlation code missing from {label} array"
                    ))
                })
        })
        .collect()
}

pub(super) fn corr_product_array(corr_types: &[i32]) -> Result<ArrayValue, VlaError> {
    let mut values = Vec::with_capacity(corr_types.len() * 2);
    for &code in corr_types {
        let (r0, r1) = match code {
            5 => (0, 0),
            6 => (0, 1),
            7 => (1, 0),
            8 => (1, 1),
            _ => return Err(VlaError::import(format!("unsupported Stokes code {code}"))),
        };
        values.push(r0);
        values.push(r1);
    }
    Ok(ArrayValue::Int32(
        ArrayD::from_shape_vec(IxDyn(&[2, corr_types.len()]).f(), values)
            .map_err(|error| VlaError::import(format!("shape CORR_PRODUCT array: {error}")))?,
    ))
}
