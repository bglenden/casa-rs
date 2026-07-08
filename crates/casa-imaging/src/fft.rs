// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal centered 2-D FFT helpers.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

use ndarray::{Array2, Axis};
use num_complex::{Complex32, Complex64};
use rustfft::{Fft, FftPlanner};

use crate::fft_backend::{
    Fft2Spec, FftBackendChoice, FftBackendSelection, FftDirection, FftPrecision, FftTiming,
    FftUseCase, select_fft_backend, transform_f32,
};

type FftKey = (usize, bool);
type FftPlan32 = Arc<dyn Fft<f32>>;
type FftPlan64 = Arc<dyn Fft<f64>>;
type FftCache<T> = LazyLock<Mutex<HashMap<FftKey, T>>>;
type DirtyF32PairWithTiming = (Array2<Complex32>, Array2<Complex32>, FftTiming);
type DirtyF64Pair = (Array2<Complex64>, Array2<Complex64>);

static FFT32_CACHE: FftCache<FftPlan32> = LazyLock::new(|| Mutex::new(HashMap::new()));
static FFT64_CACHE: FftCache<FftPlan64> = LazyLock::new(|| Mutex::new(HashMap::new()));

const IMAGING_FFT_PRECISION_ENV: &str = "CASA_RS_IMAGING_FFT_PRECISION";
const DIRTY_F32_FFT_BATCH_CHUNK_ENV: &str = "CASA_RS_DIRTY_F32_FFT_BATCH_CHUNK";
const DIRTY_F32_FFT_BATCH_TARGET_BYTES_ENV: &str = "CASA_RS_DIRTY_F32_FFT_BATCH_TARGET_BYTES";
const DEFAULT_DIRTY_F32_FFT_BATCH_TARGET_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_DIRTY_F32_FFT_BATCH_MAX_CHUNK: usize = 8;

pub(crate) fn fft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut transformed = input.clone();
    transform_axis(&mut transformed, Axis(0), false);
    transform_axis(&mut transformed, Axis(1), false);
    transformed
}

pub(crate) fn centered_fft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    centered_fft2_timed(input, FftUseCase::ModelDegrid).0
}

pub(crate) fn centered_fft2_f64(input: &Array2<Complex64>) -> Array2<Complex64> {
    centered_fft2_f64_timed(input, FftUseCase::Benchmark).0
}

pub(crate) fn centered_ifft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    centered_ifft2_timed(input, FftUseCase::Restoration).0
}

pub(crate) fn centered_ifft2_f64(input: &Array2<Complex64>) -> Array2<Complex64> {
    centered_ifft2_f64_timed(input, FftUseCase::DirtyPsfResidual).0
}

pub(crate) fn centered_fft2_timed(
    input: &Array2<Complex32>,
    use_case: FftUseCase,
) -> (Array2<Complex32>, FftTiming) {
    centered_fft2_timed_with_backend(input, use_case, FftBackendChoice::Auto)
}

pub(crate) fn centered_fft2_timed_with_backend(
    input: &Array2<Complex32>,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Array2<Complex32>, FftTiming) {
    rustfft_centered_transform_f32(input, false, use_case, backend_choice)
}

#[allow(dead_code)]
pub(crate) fn centered_fft2_batch_f32_timed_with_backend(
    inputs: &[Array2<Complex32>],
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Vec<Array2<Complex32>>, FftTiming) {
    centered_transform_batch_f32(inputs, false, use_case, backend_choice)
}

pub(crate) fn centered_fft2_f64_timed(
    input: &Array2<Complex64>,
    use_case: FftUseCase,
) -> (Array2<Complex64>, FftTiming) {
    centered_fft2_f64_timed_with_backend(input, use_case, FftBackendChoice::Auto)
}

pub(crate) fn centered_fft2_f64_timed_with_backend(
    input: &Array2<Complex64>,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Array2<Complex64>, FftTiming) {
    centered_fft2_f64_timed_with_backend_and_policy(
        input,
        use_case,
        backend_choice,
        imaging_demote_dirty_f64_fft_to_f32(),
    )
}

fn centered_fft2_f64_timed_with_backend_and_policy(
    input: &Array2<Complex64>,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
    demote_to_f32: bool,
) -> (Array2<Complex64>, FftTiming) {
    if demote_to_f32 {
        return rustfft_centered_transform_f64_via_f32(input, false, use_case, backend_choice);
    }
    rustfft_centered_transform_f64(input, false, use_case, backend_choice)
}

pub(crate) fn centered_ifft2_timed(
    input: &Array2<Complex32>,
    use_case: FftUseCase,
) -> (Array2<Complex32>, FftTiming) {
    centered_ifft2_timed_with_backend(input, use_case, FftBackendChoice::Auto)
}

pub(crate) fn centered_ifft2_timed_with_backend(
    input: &Array2<Complex32>,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Array2<Complex32>, FftTiming) {
    rustfft_centered_transform_f32(input, true, use_case, backend_choice)
}

#[allow(dead_code)]
pub(crate) fn centered_ifft2_batch_f32_timed_with_backend(
    inputs: &[Array2<Complex32>],
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Vec<Array2<Complex32>>, FftTiming) {
    centered_transform_batch_f32(inputs, true, use_case, backend_choice)
}

pub(crate) fn centered_ifft2_batch_f64_to_f32_timed_with_backend(
    inputs: &[Array2<Complex64>],
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Vec<Array2<Complex32>>, FftTiming) {
    centered_transform_batch_f64_to_f32(inputs, true, use_case, backend_choice)
}

pub(crate) fn centered_ifft2_f64_timed(
    input: &Array2<Complex64>,
    use_case: FftUseCase,
) -> (Array2<Complex64>, FftTiming) {
    centered_ifft2_f64_timed_with_backend(input, use_case, FftBackendChoice::Auto)
}

pub(crate) fn centered_ifft2_f64_timed_with_backend(
    input: &Array2<Complex64>,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Array2<Complex64>, FftTiming) {
    centered_ifft2_f64_timed_with_backend_and_policy(
        input,
        use_case,
        backend_choice,
        imaging_demote_dirty_f64_fft_to_f32(),
    )
}

fn centered_ifft2_f64_timed_with_backend_and_policy(
    input: &Array2<Complex64>,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
    demote_to_f32: bool,
) -> (Array2<Complex64>, FftTiming) {
    if demote_to_f32 {
        return rustfft_centered_transform_f64_via_f32(input, true, use_case, backend_choice);
    }
    rustfft_centered_transform_f64(input, true, use_case, backend_choice)
}

fn rustfft_centered_transform_f32(
    input: &Array2<Complex32>,
    inverse: bool,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Array2<Complex32>, FftTiming) {
    let direction = if inverse {
        FftDirection::Inverse
    } else {
        FftDirection::Forward
    };
    let spec = Fft2Spec::centered_c2c(
        input.shape()[0],
        input.shape()[1],
        FftPrecision::F32,
        direction,
        use_case,
        backend_choice,
    );
    let selection = select_fft_backend(spec);
    #[cfg(all(target_os = "macos", not(coverage)))]
    if selection.selected_backend == FftBackendChoice::MetalMpsGraph
        && selection.requested_backend_supported
    {
        if let Ok(result) = crate::apple_fft::centered_transform_f32(input, spec, selection) {
            return result;
        }
        let fallback_selection = FftBackendSelection {
            requested_backend: backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: false,
            fallback_used: true,
            reason: "selected_fft_backend_failed_using_rustfft",
        };
        return rustfft_centered_transform_f32_selected(input, inverse, spec, fallback_selection);
    }
    if selection.selected_backend != FftBackendChoice::RustFft
        && selection.requested_backend_supported
    {
        if let Ok(result) = transform_f32(selection.selected_backend, input, direction, use_case) {
            return result;
        }
        let fallback_selection = FftBackendSelection {
            requested_backend: backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: false,
            fallback_used: true,
            reason: "selected_fft_backend_failed_using_rustfft",
        };
        return rustfft_centered_transform_f32_selected(input, inverse, spec, fallback_selection);
    }
    if selection.selected_backend != FftBackendChoice::RustFft {
        let fallback_selection = FftBackendSelection {
            requested_backend: backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: false,
            fallback_used: true,
            reason: selection.reason,
        };
        return rustfft_centered_transform_f32_selected(input, inverse, spec, fallback_selection);
    }
    rustfft_centered_transform_f32_selected(input, inverse, spec, selection)
}

fn centered_transform_batch_f32(
    inputs: &[Array2<Complex32>],
    inverse: bool,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Vec<Array2<Complex32>>, FftTiming) {
    assert!(
        !inputs.is_empty(),
        "batched FFT requires at least one input plane"
    );
    let rows = inputs[0].shape()[0];
    let columns = inputs[0].shape()[1];
    for input in inputs {
        assert_eq!(
            input.shape(),
            &[rows, columns],
            "batched FFT planes must have identical shape"
        );
    }
    let direction = if inverse {
        FftDirection::Inverse
    } else {
        FftDirection::Forward
    };
    let spec = Fft2Spec::centered_c2c_batch(
        rows,
        columns,
        inputs.len(),
        FftPrecision::F32,
        direction,
        use_case,
        backend_choice,
    );
    let selection = select_fft_backend(spec);
    #[cfg(all(target_os = "macos", not(coverage)))]
    if selection.selected_backend == FftBackendChoice::MetalMpsGraph
        && selection.requested_backend_supported
    {
        if let Ok(result) = crate::apple_fft::centered_transform_f32_batch(inputs, spec, selection)
        {
            return result;
        }
        let fallback_selection = FftBackendSelection {
            requested_backend: backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: false,
            fallback_used: true,
            reason: "selected_batch_fft_backend_failed_using_rustfft",
        };
        return centered_transform_batch_f32_loop_selected(
            inputs,
            inverse,
            spec,
            fallback_selection,
        );
    }
    if selection.selected_backend != FftBackendChoice::RustFft
        && !selection.requested_backend_supported
    {
        let fallback_selection = FftBackendSelection {
            requested_backend: backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: false,
            fallback_used: true,
            reason: selection.reason,
        };
        return centered_transform_batch_f32_loop_selected(
            inputs,
            inverse,
            spec,
            fallback_selection,
        );
    }
    centered_transform_batch_f32_loop_selected(inputs, inverse, spec, selection)
}

fn centered_transform_batch_f64_to_f32(
    inputs: &[Array2<Complex64>],
    inverse: bool,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Vec<Array2<Complex32>>, FftTiming) {
    assert!(
        !inputs.is_empty(),
        "batched FFT requires at least one input plane"
    );
    let rows = inputs[0].shape()[0];
    let columns = inputs[0].shape()[1];
    for input in inputs {
        assert_eq!(
            input.shape(),
            &[rows, columns],
            "batched FFT planes must have identical shape"
        );
    }
    let direction = if inverse {
        FftDirection::Inverse
    } else {
        FftDirection::Forward
    };
    let spec = Fft2Spec::centered_c2c_batch(
        rows,
        columns,
        inputs.len(),
        FftPrecision::F32,
        direction,
        use_case,
        backend_choice,
    );
    let selection = select_fft_backend(spec);
    #[cfg(all(target_os = "macos", not(coverage)))]
    if selection.selected_backend == FftBackendChoice::MetalMpsGraph
        && selection.requested_backend_supported
    {
        if let Ok(result) =
            crate::apple_fft::centered_transform_f64_to_f32_batch(inputs, spec, selection)
        {
            return result;
        }
        let fallback_selection = FftBackendSelection {
            requested_backend: backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: false,
            fallback_used: true,
            reason: "selected_f64_to_f32_batch_fft_backend_failed_using_rustfft",
        };
        return centered_transform_batch_f64_to_f32_loop_selected(
            inputs,
            inverse,
            spec,
            fallback_selection,
        );
    }
    if selection.selected_backend != FftBackendChoice::RustFft
        && !selection.requested_backend_supported
    {
        let fallback_selection = FftBackendSelection {
            requested_backend: backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: false,
            fallback_used: true,
            reason: selection.reason,
        };
        return centered_transform_batch_f64_to_f32_loop_selected(
            inputs,
            inverse,
            spec,
            fallback_selection,
        );
    }
    centered_transform_batch_f64_to_f32_loop_selected(inputs, inverse, spec, selection)
}

fn centered_transform_batch_f64_to_f32_loop_selected(
    inputs: &[Array2<Complex64>],
    inverse: bool,
    spec: Fft2Spec,
    selection: FftBackendSelection,
) -> (Vec<Array2<Complex32>>, FftTiming) {
    let total_start = Instant::now();
    let pack_start = Instant::now();
    let f32_inputs = inputs
        .iter()
        .map(|input| input.mapv(|value| Complex32::new(value.re as f32, value.im as f32)))
        .collect::<Vec<_>>();
    let pack_elapsed = pack_start.elapsed();
    let (outputs, mut timing) =
        centered_transform_batch_f32_loop_selected(&f32_inputs, inverse, spec, selection);
    timing.pack += pack_elapsed;
    timing.total = total_start.elapsed();
    (outputs, timing)
}

fn centered_transform_batch_f32_loop_selected(
    inputs: &[Array2<Complex32>],
    inverse: bool,
    spec: Fft2Spec,
    selection: FftBackendSelection,
) -> (Vec<Array2<Complex32>>, FftTiming) {
    let mut timing = FftTiming::new(spec, selection);
    timing.plan_cache_hit = true;
    let total_start = Instant::now();
    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let single_spec = Fft2Spec::centered_c2c(
            input.shape()[0],
            input.shape()[1],
            FftPrecision::F32,
            spec.direction,
            spec.use_case,
            selection.selected_backend,
        );
        let single_selection = FftBackendSelection {
            requested_backend: selection.selected_backend,
            selected_backend: selection.selected_backend,
            requested_backend_supported: selection.requested_backend_supported,
            fallback_used: selection.fallback_used,
            reason: selection.reason,
        };
        let (output, plane_timing) = if selection.selected_backend == FftBackendChoice::RustFft {
            rustfft_centered_transform_f32_selected(input, inverse, single_spec, single_selection)
        } else {
            match transform_f32(
                selection.selected_backend,
                input,
                spec.direction,
                spec.use_case,
            ) {
                Ok(result) => result,
                Err(_) => {
                    let fallback_spec = Fft2Spec::centered_c2c(
                        input.shape()[0],
                        input.shape()[1],
                        FftPrecision::F32,
                        spec.direction,
                        spec.use_case,
                        FftBackendChoice::RustFft,
                    );
                    let fallback_selection = FftBackendSelection {
                        requested_backend: selection.selected_backend,
                        selected_backend: FftBackendChoice::RustFft,
                        requested_backend_supported: false,
                        fallback_used: true,
                        reason: "selected_batch_plane_backend_failed_using_rustfft",
                    };
                    rustfft_centered_transform_f32_selected(
                        input,
                        inverse,
                        fallback_spec,
                        fallback_selection,
                    )
                }
            }
        };
        timing.plan_cache_hit &= plane_timing.plan_cache_hit;
        timing.plan += plane_timing.plan;
        timing.pack += plane_timing.pack;
        timing.transfer_to_device += plane_timing.transfer_to_device;
        timing.exec += plane_timing.exec;
        timing.device_exec += plane_timing.device_exec;
        timing.transfer_from_device += plane_timing.transfer_from_device;
        timing.sync += plane_timing.sync;
        outputs.push(output);
    }
    timing.total = total_start.elapsed();
    (outputs, timing)
}

fn rustfft_centered_transform_f32_selected(
    input: &Array2<Complex32>,
    inverse: bool,
    spec: Fft2Spec,
    selection: FftBackendSelection,
) -> (Array2<Complex32>, FftTiming) {
    let mut timing = FftTiming::new(spec, selection);
    timing.plan_cache_hit = plans_cached_f32(input.shape()[1], input.shape()[0], inverse);

    let total_start = Instant::now();
    let pack_start = Instant::now();
    let mut shifted = ifftshift2(input);
    timing.pack += pack_start.elapsed();

    let exec_start = Instant::now();
    transform_axis(&mut shifted, Axis(0), inverse);
    transform_axis(&mut shifted, Axis(1), inverse);
    if inverse {
        let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f32;
        shifted.mapv_inplace(|value| value * scale);
    }
    timing.exec += exec_start.elapsed();

    let pack_start = Instant::now();
    let output = fftshift2(&shifted);
    timing.pack += pack_start.elapsed();
    timing.total = total_start.elapsed();
    (output, timing)
}

fn rustfft_centered_transform_f64(
    input: &Array2<Complex64>,
    inverse: bool,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Array2<Complex64>, FftTiming) {
    let direction = if inverse {
        FftDirection::Inverse
    } else {
        FftDirection::Forward
    };
    let spec = Fft2Spec::centered_c2c(
        input.shape()[0],
        input.shape()[1],
        FftPrecision::F64,
        direction,
        use_case,
        backend_choice,
    );
    let selection = select_fft_backend(spec);
    let mut timing = FftTiming::new(spec, selection);
    timing.plan_cache_hit = plans_cached_f64(input.shape()[1], input.shape()[0], inverse);

    let total_start = Instant::now();
    let pack_start = Instant::now();
    let mut shifted = ifftshift2_f64(input);
    timing.pack += pack_start.elapsed();

    let exec_start = Instant::now();
    transform_axis_f64(&mut shifted, Axis(0), inverse);
    transform_axis_f64(&mut shifted, Axis(1), inverse);
    if inverse {
        let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f64;
        shifted.mapv_inplace(|value| value * scale);
    }
    timing.exec += exec_start.elapsed();

    let pack_start = Instant::now();
    let output = fftshift2_f64(&shifted);
    timing.pack += pack_start.elapsed();
    timing.total = total_start.elapsed();
    (output, timing)
}

pub(crate) fn centered_ifft2_f64_owned(mut input: Array2<Complex64>) -> Array2<Complex64> {
    if !shift2_in_place_even_f64(&mut input) {
        return centered_ifft2_f64(&input);
    }
    inverse_fft2_scale_f64(&mut input);
    let shifted = shift2_in_place_even_f64(&mut input);
    debug_assert!(shifted);
    input
}

pub(crate) fn centered_ifft2_f64_owned_unshifted_even(
    mut input: Array2<Complex64>,
) -> Result<Array2<Complex64>, Array2<Complex64>> {
    if !is_even_contiguous_f64(&input) {
        return Err(input);
    }
    inverse_fft2_scale_centered_frequency_f64(&mut input);
    Ok(input)
}

pub(crate) fn centered_ifft2_dirty_f64_owned(input: Array2<Complex64>) -> Array2<Complex64> {
    if imaging_demote_dirty_f64_fft_to_f32() {
        return centered_ifft2_f64_timed_with_backend(
            &input,
            FftUseCase::DirtyPsfResidual,
            FftBackendChoice::Auto,
        )
        .0;
    }
    centered_ifft2_f64_owned(input)
}

pub(crate) fn centered_ifft2_dirty_f64_owned_unshifted_even(
    input: Array2<Complex64>,
) -> Result<Array2<Complex64>, Array2<Complex64>> {
    if imaging_demote_dirty_f64_fft_to_f32() {
        return Err(input);
    }
    centered_ifft2_f64_owned_unshifted_even(input)
}

pub(crate) fn centered_ifft2_dirty_f64_pair_to_f32(
    first: Array2<Complex64>,
    second: Array2<Complex64>,
) -> Result<DirtyF32PairWithTiming, Box<DirtyF64Pair>> {
    if !imaging_demote_dirty_f64_fft_to_f32() {
        return Err(Box::new((first, second)));
    }
    let inputs = vec![first, second];
    let (mut outputs, timing) = centered_ifft2_batch_f64_to_f32_timed_with_backend(
        &inputs,
        FftUseCase::DirtyPsfResidual,
        FftBackendChoice::Auto,
    );
    debug_assert_eq!(outputs.len(), 2);
    let second = outputs
        .pop()
        .expect("batched dirty f32 transform should return residual plane");
    let first = outputs
        .pop()
        .expect("batched dirty f32 transform should return PSF plane");
    Ok((first, second, timing))
}

pub(crate) fn dirty_f32_fft_batch_chunk_size(
    rows: usize,
    columns: usize,
    transform_count: usize,
) -> usize {
    if transform_count <= 1 {
        return transform_count.max(1);
    }
    if let Ok(value) = std::env::var(DIRTY_F32_FFT_BATCH_CHUNK_ENV) {
        if let Ok(parsed) = value.trim().parse::<usize>() {
            return parsed.max(1).min(transform_count);
        }
    }
    let target_bytes = std::env::var(DIRTY_F32_FFT_BATCH_TARGET_BYTES_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DIRTY_F32_FFT_BATCH_TARGET_BYTES);
    let bytes_per_transform = rows
        .saturating_mul(columns)
        .saturating_mul(std::mem::size_of::<Complex32>())
        .max(1);
    let estimated =
        (target_bytes / bytes_per_transform).clamp(2, DEFAULT_DIRTY_F32_FFT_BATCH_MAX_CHUNK);
    estimated.min(transform_count)
}

pub(crate) fn maybe_emit_dirty_f32_batch_fft_timing(
    timing: &FftTiming,
    rows: usize,
    columns: usize,
    transform_count: usize,
    chunk_size: usize,
) {
    if !dirty_f32_fft_profile_detail_enabled() {
        return;
    }
    let chunk_count = transform_count.div_ceil(chunk_size.max(1));
    eprintln!(
        "dirty_product_fft_timing use_case={} requested_backend={} selected_backend={} fallback_used={} reason={} precision={} direction={} rows={} columns={} transforms={} chunk_size={} chunk_count={} plan_cache_hit={} plan_ms={:.3} pack_ms={:.3} transfer_to_device_ms={:.3} exec_ms={:.3} device_exec_ms={:.3} transfer_from_device_ms={:.3} sync_ms={:.3} total_ms={:.3}",
        timing.spec.use_case,
        timing.selection.requested_backend,
        timing.selection.selected_backend,
        timing.selection.fallback_used,
        timing.selection.reason,
        timing.spec.precision,
        timing.spec.direction,
        rows,
        columns,
        transform_count,
        chunk_size,
        chunk_count,
        timing.plan_cache_hit,
        duration_ms(timing.plan),
        duration_ms(timing.pack),
        duration_ms(timing.transfer_to_device),
        duration_ms(timing.exec),
        duration_ms(timing.device_exec),
        duration_ms(timing.transfer_from_device),
        duration_ms(timing.sync),
        duration_ms(timing.total),
    );
}

fn dirty_f32_fft_profile_detail_enabled() -> bool {
    std::env::var_os("CASA_RS_FFT_PROFILE_DETAIL").is_some()
        || std::env::var_os("CASA_RS_STANDARD_MFS_PROFILE_DETAIL").is_some()
}

fn duration_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn inverse_fft2_scale_f64(input: &mut Array2<Complex64>) {
    transform_axis_f64(input, Axis(0), true);
    transform_axis_f64(input, Axis(1), true);
    let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f64;
    input.mapv_inplace(|value| value * scale);
}

fn rustfft_centered_transform_f64_via_f32(
    input: &Array2<Complex64>,
    inverse: bool,
    use_case: FftUseCase,
    backend_choice: FftBackendChoice,
) -> (Array2<Complex64>, FftTiming) {
    let direction = if inverse {
        FftDirection::Inverse
    } else {
        FftDirection::Forward
    };
    let spec = Fft2Spec::centered_c2c(
        input.shape()[0],
        input.shape()[1],
        FftPrecision::F64,
        direction,
        use_case,
        backend_choice,
    );
    let selection = select_fft_backend(spec);
    let mut timing = FftTiming::new(spec, selection);
    timing.plan_cache_hit = plans_cached_f32(input.shape()[1], input.shape()[0], inverse);

    let total_start = Instant::now();
    let pack_start = Instant::now();
    let input_f32 = input.mapv(|value| Complex32::new(value.re as f32, value.im as f32));
    timing.pack += pack_start.elapsed();

    let (output_f32, inner_timing) =
        rustfft_centered_transform_f32(&input_f32, inverse, use_case, backend_choice);
    timing.selection = inner_timing.selection;
    timing.plan += inner_timing.plan;
    timing.pack += inner_timing.pack;
    timing.exec += inner_timing.exec;
    timing.transfer_to_device += inner_timing.transfer_to_device;
    timing.device_exec += inner_timing.device_exec;
    timing.transfer_from_device += inner_timing.transfer_from_device;
    timing.sync += inner_timing.sync;

    let pack_start = Instant::now();
    let output = output_f32.mapv(|value| Complex64::new(value.re as f64, value.im as f64));
    timing.pack += pack_start.elapsed();
    timing.total = total_start.elapsed();
    (output, timing)
}

pub(crate) fn imaging_demote_dirty_f64_fft_to_f32() -> bool {
    std::env::var(IMAGING_FFT_PRECISION_ENV).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "f32" | "single" | "single-precision" | "fast-f32" | "auto-f32"
        )
    })
}

fn inverse_fft2_scale_centered_frequency_f64(input: &mut Array2<Complex64>) {
    transform_axis_f64(input, Axis(0), true);
    transform_axis_f64(input, Axis(1), true);
    let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f64;
    let [nx, ny]: [usize; 2] = input
        .shape()
        .try_into()
        .expect("2-D FFT input should have exactly two axes");
    let storage = input
        .as_slice_memory_order_mut()
        .expect("even centered-frequency FFT input should be contiguous");
    for x in 0..nx {
        let row_base = x * ny;
        let row_sign = if x % 2 == 0 { scale } else { -scale };
        for y in 0..ny {
            let sign = if y % 2 == 0 { row_sign } else { -row_sign };
            storage[row_base + y] *= sign;
        }
    }
}

fn transform_axis(data: &mut Array2<Complex32>, axis: Axis, inverse: bool) {
    if axis.index() == 0 {
        transform_rows(data, inverse);
    } else {
        transform_columns(data, inverse);
    }
}

fn transform_axis_f64(data: &mut Array2<Complex64>, axis: Axis, inverse: bool) {
    if axis.index() == 0 {
        transform_rows_f64(data, inverse);
    } else {
        transform_columns_f64(data, inverse);
    }
}

fn fft32(len: usize, inverse: bool) -> Arc<dyn Fft<f32>> {
    let mut cache = FFT32_CACHE.lock().expect("f32 FFT cache lock poisoned");
    if let Some(fft) = cache.get(&(len, inverse)) {
        return Arc::clone(fft);
    }
    let mut planner = FftPlanner::<f32>::new();
    let fft = if inverse {
        planner.plan_fft_inverse(len)
    } else {
        planner.plan_fft_forward(len)
    };
    cache.insert((len, inverse), Arc::clone(&fft));
    fft
}

fn fft64(len: usize, inverse: bool) -> Arc<dyn Fft<f64>> {
    let mut cache = FFT64_CACHE.lock().expect("f64 FFT cache lock poisoned");
    if let Some(fft) = cache.get(&(len, inverse)) {
        return Arc::clone(fft);
    }
    let mut planner = FftPlanner::<f64>::new();
    let fft = if inverse {
        planner.plan_fft_inverse(len)
    } else {
        planner.plan_fft_forward(len)
    };
    cache.insert((len, inverse), Arc::clone(&fft));
    fft
}

fn plans_cached_f32(row_len: usize, column_len: usize, inverse: bool) -> bool {
    let cache = FFT32_CACHE.lock().expect("f32 FFT cache lock poisoned");
    cache.contains_key(&(row_len, inverse)) && cache.contains_key(&(column_len, inverse))
}

fn plans_cached_f64(row_len: usize, column_len: usize, inverse: bool) -> bool {
    let cache = FFT64_CACHE.lock().expect("f64 FFT cache lock poisoned");
    cache.contains_key(&(row_len, inverse)) && cache.contains_key(&(column_len, inverse))
}

fn transform_rows(data: &mut Array2<Complex32>, inverse: bool) {
    let row_len = data.shape()[1];
    let fft = fft32(row_len, inverse);
    let mut scratch = vec![Complex32::default(); fft.get_inplace_scratch_len()];
    for mut row in data.rows_mut() {
        if let Some(row) = row.as_slice_mut() {
            fft.process_with_scratch(row, &mut scratch);
        } else {
            let mut lane = row.to_vec();
            fft.process_with_scratch(&mut lane, &mut scratch);
            for (column_index, value) in lane.into_iter().enumerate() {
                row[column_index] = value;
            }
        }
    }
}

fn transform_columns(data: &mut Array2<Complex32>, inverse: bool) {
    let [row_count, column_count]: [usize; 2] = data
        .shape()
        .try_into()
        .expect("2-D FFT input should have exactly two axes");
    let fft = fft32(row_count, inverse);
    let mut lane = vec![Complex32::default(); row_count];
    let mut scratch = vec![Complex32::default(); fft.get_inplace_scratch_len()];
    for column_index in 0..column_count {
        for row_index in 0..row_count {
            lane[row_index] = data[(row_index, column_index)];
        }
        fft.process_with_scratch(&mut lane, &mut scratch);
        for row_index in 0..row_count {
            data[(row_index, column_index)] = lane[row_index];
        }
    }
}

fn transform_rows_f64(data: &mut Array2<Complex64>, inverse: bool) {
    let row_len = data.shape()[1];
    let fft = fft64(row_len, inverse);
    let mut scratch = vec![Complex64::default(); fft.get_inplace_scratch_len()];
    for mut row in data.rows_mut() {
        if let Some(row) = row.as_slice_mut() {
            fft.process_with_scratch(row, &mut scratch);
        } else {
            let mut lane = row.to_vec();
            fft.process_with_scratch(&mut lane, &mut scratch);
            for (column_index, value) in lane.into_iter().enumerate() {
                row[column_index] = value;
            }
        }
    }
}

fn transform_columns_f64(data: &mut Array2<Complex64>, inverse: bool) {
    let [row_count, column_count]: [usize; 2] = data
        .shape()
        .try_into()
        .expect("2-D FFT input should have exactly two axes");
    let fft = fft64(row_count, inverse);
    let mut lane = vec![Complex64::default(); row_count];
    let mut scratch = vec![Complex64::default(); fft.get_inplace_scratch_len()];
    for column_index in 0..column_count {
        for row_index in 0..row_count {
            lane[row_index] = data[(row_index, column_index)];
        }
        fft.process_with_scratch(&mut lane, &mut scratch);
        for row_index in 0..row_count {
            data[(row_index, column_index)] = lane[row_index];
        }
    }
}

pub(crate) fn fftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, false)
}

pub(crate) fn ifftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, true)
}

fn fftshift2_f64(input: &Array2<Complex64>) -> Array2<Complex64> {
    shift2_f64(input, false)
}

fn ifftshift2_f64(input: &Array2<Complex64>) -> Array2<Complex64> {
    shift2_f64(input, true)
}

fn shift2_in_place_even_f64(input: &mut Array2<Complex64>) -> bool {
    if !is_even_contiguous_f64(input) {
        return false;
    }
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    let storage = input
        .as_slice_memory_order_mut()
        .expect("even contiguous f64 grid should have memory-order slice");
    let hx = nx / 2;
    let hy = ny / 2;
    for x in 0..hx {
        for y in 0..hy {
            let q00 = x * ny + y;
            let q11 = (x + hx) * ny + y + hy;
            storage.swap(q00, q11);

            let q10 = (x + hx) * ny + y;
            let q01 = x * ny + y + hy;
            storage.swap(q10, q01);
        }
    }
    true
}

fn is_even_contiguous_f64(input: &Array2<Complex64>) -> bool {
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    nx % 2 == 0 && ny % 2 == 0 && input.as_slice_memory_order().is_some()
}

fn shift2(input: &Array2<Complex32>, inverse: bool) -> Array2<Complex32> {
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    let mut output = Array2::<Complex32>::zeros((nx, ny));
    let x_shift = if inverse { nx.div_ceil(2) } else { nx / 2 };
    let y_shift = if inverse { ny.div_ceil(2) } else { ny / 2 };

    for x in 0..nx {
        for y in 0..ny {
            let new_x = (x + x_shift) % nx;
            let new_y = (y + y_shift) % ny;
            output[(x, y)] = input[(new_x, new_y)];
        }
    }

    output
}

fn shift2_f64(input: &Array2<Complex64>, inverse: bool) -> Array2<Complex64> {
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    let mut output = Array2::<Complex64>::zeros((nx, ny));
    let x_shift = if inverse { nx.div_ceil(2) } else { nx / 2 };
    let y_shift = if inverse { ny.div_ceil(2) } else { ny / 2 };

    for x in 0..nx {
        for y in 0..ny {
            let new_x = (x + x_shift) % nx;
            let new_y = (y + y_shift) % ny;
            output[(x, y)] = input[(new_x, new_y)];
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use ndarray::Array2;
    use num_complex::{Complex32, Complex64};

    use crate::fft_backend::{FftBackendChoice, FftPrecision, FftUseCase};

    use super::{
        centered_fft2, centered_fft2_batch_f32_timed_with_backend,
        centered_fft2_f64_timed_with_backend_and_policy, centered_fft2_timed_with_backend,
        centered_ifft2, centered_ifft2_batch_f32_timed_with_backend,
        centered_ifft2_batch_f64_to_f32_timed_with_backend, centered_ifft2_f64,
        centered_ifft2_f64_owned, centered_ifft2_f64_owned_unshifted_even,
        centered_ifft2_f64_timed_with_backend_and_policy, centered_ifft2_timed_with_backend,
        rustfft_centered_transform_f64_via_f32,
    };

    #[test]
    fn fft_round_trip_preserves_image() {
        let mut image = Array2::<Complex32>::zeros((8, 8));
        image[(4, 4)] = Complex32::new(1.0, 0.0);
        image[(5, 3)] = Complex32::new(-0.25, 0.5);

        let transformed = centered_fft2(&image);
        let restored = centered_ifft2(&transformed);

        for (expected, actual) in image.iter().zip(restored.iter()) {
            assert!((expected.re - actual.re).abs() < 1.0e-5);
            assert!((expected.im - actual.im).abs() < 1.0e-5);
        }
    }

    #[test]
    fn f32_model_and_restoration_helpers_use_shared_auto_backend_contract() {
        let image = Array2::from_shape_fn((8, 6), |(x, y)| {
            Complex32::new((x as f32 + 0.25) * 0.5, (y as f32 - 0.75) * 0.25)
        });

        let (_, model_timing) = centered_fft2_timed_with_backend(
            &image,
            FftUseCase::ModelDegrid,
            FftBackendChoice::Auto,
        );
        let (_, restoration_timing) = centered_ifft2_timed_with_backend(
            &image,
            FftUseCase::Restoration,
            FftBackendChoice::Auto,
        );

        assert_eq!(model_timing.spec.precision, FftPrecision::F32);
        assert_eq!(model_timing.spec.use_case, FftUseCase::ModelDegrid);
        assert_eq!(
            model_timing.selection.requested_backend,
            FftBackendChoice::Auto
        );
        assert_eq!(restoration_timing.spec.precision, FftPrecision::F32);
        assert_eq!(restoration_timing.spec.use_case, FftUseCase::Restoration);
        assert_eq!(
            restoration_timing.selection.requested_backend,
            FftBackendChoice::Auto
        );
    }

    #[test]
    fn explicit_f32_policy_demotes_generic_f64_fft_helpers() {
        let image = Array2::from_shape_fn((8, 6), |(x, y)| {
            Complex64::new(
                (x as f64 + 0.25) * 0.5 + 1.0e-8,
                (y as f64 - 0.75) * 0.25 - 2.0e-8,
            )
        });

        let (actual_forward, forward_timing) = centered_fft2_f64_timed_with_backend_and_policy(
            &image,
            FftUseCase::Benchmark,
            FftBackendChoice::RustFft,
            true,
        );
        let (expected_forward, _) = rustfft_centered_transform_f64_via_f32(
            &image,
            false,
            FftUseCase::Benchmark,
            FftBackendChoice::RustFft,
        );
        let (actual_inverse, inverse_timing) = centered_ifft2_f64_timed_with_backend_and_policy(
            &image,
            FftUseCase::Benchmark,
            FftBackendChoice::RustFft,
            true,
        );
        let (expected_inverse, _) = rustfft_centered_transform_f64_via_f32(
            &image,
            true,
            FftUseCase::Benchmark,
            FftBackendChoice::RustFft,
        );

        assert_eq!(
            forward_timing.selection.selected_backend,
            FftBackendChoice::RustFft
        );
        assert_eq!(
            inverse_timing.selection.selected_backend,
            FftBackendChoice::RustFft
        );
        for (actual, expected) in actual_forward.iter().zip(expected_forward.iter()) {
            assert!((actual.re - expected.re).abs() < 1.0e-12);
            assert!((actual.im - expected.im).abs() < 1.0e-12);
        }
        for (actual, expected) in actual_inverse.iter().zip(expected_inverse.iter()) {
            assert!((actual.re - expected.re).abs() < 1.0e-12);
            assert!((actual.im - expected.im).abs() < 1.0e-12);
        }
    }

    #[test]
    fn f32_batch_helpers_match_independent_centered_transforms() {
        let first = Array2::from_shape_fn((8, 6), |(x, y)| {
            Complex32::new((x as f32 + 0.25) * 0.5, (y as f32 - 0.75) * 0.25)
        });
        let second = Array2::from_shape_fn((8, 6), |(x, y)| {
            Complex32::new((x * 3 + y * 5) as f32 * 0.125, (x as f32 - y as f32) * 0.5)
        });
        let inputs = vec![first.clone(), second.clone()];

        let (batch_forward, forward_timing) = centered_fft2_batch_f32_timed_with_backend(
            &inputs,
            FftUseCase::Benchmark,
            FftBackendChoice::RustFft,
        );
        let (batch_inverse, inverse_timing) = centered_ifft2_batch_f32_timed_with_backend(
            &inputs,
            FftUseCase::Benchmark,
            FftBackendChoice::RustFft,
        );

        assert_eq!(forward_timing.spec.shape.batch, 2);
        assert_eq!(inverse_timing.spec.shape.batch, 2);
        for (input, actual) in inputs.iter().zip(batch_forward.iter()) {
            let (expected, _) = centered_fft2_timed_with_backend(
                input,
                FftUseCase::Benchmark,
                FftBackendChoice::RustFft,
            );
            for (actual, expected) in actual.iter().zip(expected.iter()) {
                assert!((actual.re - expected.re).abs() < 1.0e-5);
                assert!((actual.im - expected.im).abs() < 1.0e-5);
            }
        }
        for (input, actual) in inputs.iter().zip(batch_inverse.iter()) {
            let (expected, _) = centered_ifft2_timed_with_backend(
                input,
                FftUseCase::Benchmark,
                FftBackendChoice::RustFft,
            );
            for (actual, expected) in actual.iter().zip(expected.iter()) {
                assert!((actual.re - expected.re).abs() < 1.0e-5);
                assert!((actual.im - expected.im).abs() < 1.0e-5);
            }
        }
    }

    #[test]
    fn f64_to_f32_batch_helper_matches_explicit_f32_narrowing() {
        let first = Array2::from_shape_fn((7, 6), |(x, y)| {
            Complex64::new((x as f64 + 0.25) * 0.5, (y as f64 - 0.75) * 0.25)
        });
        let second = Array2::from_shape_fn((7, 6), |(x, y)| {
            Complex64::new((x * 3 + y * 5) as f64 * 0.125, (x as f64 - y as f64) * 0.5)
        });
        let inputs = vec![first, second];
        let expected_inputs = inputs
            .iter()
            .map(|input| input.mapv(|value| Complex32::new(value.re as f32, value.im as f32)))
            .collect::<Vec<_>>();

        let (actual, timing) = centered_ifft2_batch_f64_to_f32_timed_with_backend(
            &inputs,
            FftUseCase::Benchmark,
            FftBackendChoice::RustFft,
        );
        let (expected, _) = centered_ifft2_batch_f32_timed_with_backend(
            &expected_inputs,
            FftUseCase::Benchmark,
            FftBackendChoice::RustFft,
        );

        assert_eq!(timing.spec.precision, FftPrecision::F32);
        assert_eq!(timing.spec.shape.batch, inputs.len());
        for (actual, expected) in actual.iter().zip(expected.iter()) {
            for (actual, expected) in actual.iter().zip(expected.iter()) {
                assert!((actual.re - expected.re).abs() < 1.0e-5);
                assert!((actual.im - expected.im).abs() < 1.0e-5);
            }
        }
    }

    #[test]
    fn dirty_f64_fast_path_uses_shared_f32_backend_contract() {
        let image = Array2::from_shape_fn((8, 6), |(x, y)| {
            Complex64::new((x as f64 + 0.125) * 1.0e-3, (y as f64 - 0.375) * 2.0e-3)
        });
        let image_f32 = image.mapv(|value| Complex32::new(value.re as f32, value.im as f32));

        let (actual, timing) = rustfft_centered_transform_f64_via_f32(
            &image,
            true,
            FftUseCase::DirtyPsfResidual,
            FftBackendChoice::Auto,
        );
        let (expected_f32, _) = centered_ifft2_timed_with_backend(
            &image_f32,
            FftUseCase::DirtyPsfResidual,
            FftBackendChoice::Auto,
        );

        assert_eq!(timing.spec.precision, FftPrecision::F64);
        assert_eq!(timing.spec.use_case, FftUseCase::DirtyPsfResidual);
        assert_eq!(timing.selection.requested_backend, FftBackendChoice::Auto);
        for (actual, expected) in actual.iter().zip(expected_f32.iter()) {
            assert!((actual.re - f64::from(expected.re)).abs() < 1.0e-5);
            assert!((actual.im - f64::from(expected.im)).abs() < 1.0e-5);
        }
    }

    #[test]
    fn owned_f64_ifft_matches_borrowed_for_even_shape() {
        let image = Array2::from_shape_fn((8, 6), |(x, y)| {
            Complex64::new((x * 13 + y * 7) as f64, (x as isize - y as isize) as f64)
        });
        let borrowed = centered_ifft2_f64(&image);
        let owned = centered_ifft2_f64_owned(image);
        for (expected, actual) in borrowed.iter().zip(owned.iter()) {
            assert!((expected.re - actual.re).abs() < 1.0e-10);
            assert!((expected.im - actual.im).abs() < 1.0e-10);
        }
    }

    #[test]
    fn unshifted_owned_f64_ifft_can_reconstruct_centered_output() {
        let image = Array2::from_shape_fn((8, 6), |(x, y)| {
            Complex64::new((x * 17 + y * 3) as f64, (y as isize - x as isize) as f64)
        });
        let borrowed = centered_ifft2_f64(&image);
        let unshifted =
            centered_ifft2_f64_owned_unshifted_even(image).expect("even contiguous shape");
        let nx = unshifted.shape()[0];
        let ny = unshifted.shape()[1];
        for x in 0..nx {
            for y in 0..ny {
                let actual = unshifted[((x + nx / 2) % nx, (y + ny / 2) % ny)];
                let expected = borrowed[(x, y)];
                assert!((expected.re - actual.re).abs() < 1.0e-10);
                assert!((expected.im - actual.im).abs() < 1.0e-10);
            }
        }
    }

    #[test]
    fn owned_f64_ifft_matches_borrowed_for_odd_shape() {
        let image = Array2::from_shape_fn((7, 5), |(x, y)| {
            Complex64::new((x * 11 + y * 5) as f64, (y as isize - x as isize) as f64)
        });
        let borrowed = centered_ifft2_f64(&image);
        let owned = centered_ifft2_f64_owned(image);
        for (expected, actual) in borrowed.iter().zip(owned.iter()) {
            assert!((expected.re - actual.re).abs() < 1.0e-10);
            assert!((expected.im - actual.im).abs() < 1.0e-10);
        }
    }
}
