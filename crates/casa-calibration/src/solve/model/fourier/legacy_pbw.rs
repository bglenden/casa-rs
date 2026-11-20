// SPDX-License-Identifier: LGPL-3.0-or-later
//! Restore the cache cell retained with a legacy nPBWProjectFT recipe.

use casa_images::PagedImage;
use casa_imaging_operator::{
    ConvolutionFunctionSet, GridGeometry, LegacyPbWConvolution, PolarizationRouting,
};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_types::{Complex32, RecordValue, ScalarValue, Value};

use super::{FourierRow, GainSolveError, complex_model_image_path, model_error};
use crate::ft::legacy_pbw::KERNEL_IMAGE;

pub(super) fn load(
    record: &RecordValue,
    row: &FourierRow<'_>,
    geometry: &GridGeometry,
    polarization: &PolarizationRouting,
) -> Result<(Box<dyn ConvolutionFunctionSet>, Reservation), GainSolveError> {
    let dimension = |field| {
        let value = match record.get(field) {
            Some(Value::Scalar(ScalarValue::Int32(value))) => i64::from(*value),
            Some(Value::Scalar(ScalarValue::Int64(value))) => *value,
            _ => return Err(model_error(format!("legacy PB/W recipe has no {field}"))),
        };
        u16::try_from(value).map_err(model_error)
    };
    let support = dimension("casars_legacy_pbw_support")?;
    let sampling = dimension("casars_legacy_pbw_sampling")?;
    let host = HostResources::detect().map_err(model_error)?;
    let path = complex_model_image_path(record, row.ms)?.join(KERNEL_IMAGE);
    let kernel = PagedImage::<Complex32>::open_with_cache(path, 1).map_err(model_error)?;
    let [side, ny, correlations, 1] = *kernel.shape() else {
        return Err(model_error(
            "legacy PB/W recipe has an invalid kernel image",
        ));
    };
    let radius = usize::from(support) * usize::from(sampling) + usize::from(sampling / 2);
    if side != ny
        || correlations != row.correlations.len()
        || side <= 2 * radius
        || sampling == 0
        || support > u16::MAX / 2
    {
        return Err(model_error(
            "legacy PB/W recipe has an incompatible kernel shape",
        ));
    }
    let grid = geometry.grid_shape();
    let read_bytes = (side as u64)
        .saturating_mul(side as u64)
        .saturating_mul(correlations as u64)
        .saturating_mul(3 * size_of::<Complex32>() as u64);
    let model_bytes = (grid[0] as u64)
        .saturating_mul(grid[1] as u64)
        .saturating_mul((1 + polarization.grid_pols()) as u64)
        .saturating_mul(2 * size_of::<Complex32>() as u64);
    let memory = admit(
        &host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy PB/W saved Fourier image and taps",
            memory: read_bytes.saturating_add(model_bytes).saturating_add(
                LegacyPbWConvolution::forming_bytes(side, correlations, sampling, support, grid),
            ),
        },
    )
    .map_err(model_error)?;
    let samples = kernel.get().map_err(model_error)?;
    let terms = LegacyPbWConvolution::new(
        &samples,
        sampling,
        support,
        grid,
        polarization.grid_pols(),
        polarization.pol_map(),
    )
    .map_err(model_error)?;
    Ok((Box::new(terms), memory))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imaging_memory_test::{isolated_test, live_since, peak_since, reset_peak};
    use ndarray::{ArrayD, IxDyn};

    #[test]
    fn legacy_pbw_repacking_fits_its_charge() {
        if !isolated_test(module_path!(), "legacy_pbw_repacking_fits_its_charge") {
            return;
        }
        let kernel = ArrayD::from_elem(IxDyn(&[161, 161, 2, 1]), Complex32::new(0.3, -0.2));
        let grid = [256, 256];
        let budget = LegacyPbWConvolution::forming_bytes(161, 2, 20, 3, grid);
        let baseline = reset_peak();
        let taps = LegacyPbWConvolution::new(&kernel, 20, 3, grid, 4, &[Some(0), Some(3)]).unwrap();
        let peak = peak_since(baseline);
        assert!(peak <= budget, "{peak} bytes used over {budget}");
        assert!(live_since(baseline) < budget);
        assert_eq!(taps.max_half_support(), [3; 2]);
    }
}
