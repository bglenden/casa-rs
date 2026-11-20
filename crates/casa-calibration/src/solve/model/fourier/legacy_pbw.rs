// SPDX-License-Identifier: LGPL-3.0-or-later
//! Restore the cache cell retained with a legacy nPBWProjectFT recipe.

use casa_images::PagedImage;
use casa_imaging_operator::{GridGeometry, LegacyPbWConvolution, PolarizationRouting};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_types::{Complex32, RecordValue, ScalarValue, Value};

use super::{FourierRow, GainSolveError, complex_model_image_path, model_error, record_number};
use crate::ft::legacy_pbw::KERNEL_IMAGE;

pub(super) fn load(
    record: &RecordValue,
    row: &FourierRow<'_>,
    geometry: &GridGeometry,
    polarization: &PolarizationRouting,
) -> Result<(LegacyPbWConvolution, Reservation), GainSolveError> {
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
    let angle = record_number(record, "casars_legacy_pbw_angle")
        .filter(|angle| angle.is_finite())
        .ok_or_else(|| model_error("legacy PB/W recipe has no valid reference angle"))?;
    let path = complex_model_image_path(record, row.ms)?.join(KERNEL_IMAGE);
    let kernel = PagedImage::<Complex32>::open_with_cache(path, 1).map_err(model_error)?;
    let correlations = row.correlations.len();
    LegacyPbWConvolution::validate_shape(kernel.shape(), sampling, support, correlations)
        .map_err(model_error)?;
    let side = kernel.shape()[0];
    let cache_bytes = (side as u64)
        .saturating_mul(side as u64)
        .saturating_mul(correlations as u64)
        .saturating_mul(size_of::<Complex32>() as u64);
    let retained = LegacyPbWConvolution::resident_bytes(
        side,
        correlations,
        sampling,
        support,
        geometry.grid_shape(),
    );
    let memory = reserve("legacy PB/W cache image", retained)?;
    let _read_memory = reserve("legacy PB/W cache read", cache_bytes.saturating_mul(2))?;
    let samples = kernel.get().map_err(model_error)?;
    let terms = LegacyPbWConvolution::new(
        samples,
        sampling,
        support,
        geometry,
        polarization.grid_pols(),
        polarization.pol_map(),
        angle,
    )
    .map_err(model_error)?;
    Ok((terms, memory))
}

pub(super) fn reserve(phase: &'static str, memory: u64) -> Result<Reservation, GainSolveError> {
    let host = HostResources::detect().map_err(model_error)?;
    admit(&host, &ResourcePolicy::Balanced, &Demand { phase, memory }).map_err(model_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imaging_memory_test::{isolated_test, live_since, peak_since, reset_peak};
    use casa_imaging_operator::{
        CellHold, ConvolutionFunctionSet, GridPadding, ImageExtent, RowContext,
    };
    use ndarray::{ArrayD, IxDyn};

    #[test]
    fn legacy_pbw_repacking_fits_its_charge() {
        if !isolated_test(module_path!(), "legacy_pbw_repacking_fits_its_charge") {
            return;
        }
        let grid = [256, 256];
        let scale = (15.0_f64 / 3600.).to_radians();
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: grid,
                increment_rad: [-scale, scale],
                reference_pixel: [128; 2],
            },
            GridPadding::None,
        )
        .unwrap();
        let budget = LegacyPbWConvolution::resident_bytes(161, 2, 20, 3, grid);
        let baseline = reset_peak();
        let kernel = ArrayD::from_elem(IxDyn(&[161, 161, 2, 1]), Complex32::new(0.3, -0.2));
        let taps = LegacyPbWConvolution::new(kernel, 20, 3, &geometry, 4, &[Some(0), Some(3)], 0.)
            .unwrap();
        let mut first = CellHold::new();
        let mut second = CellHold::new();
        let mut row = RowContext {
            native_frequency_ratio: 1.,
            original_uvw_m: None,
            time_s: 0.,
            antennas: [0, 1],
            antenna_types: [0; 2],
            parallactic_angle_rad: [0.; 2],
            field: 0,
            spectral_window: 0,
        };
        let initial = taps.taps(taps.key(&row, 1.4e9, 0.), &mut first);
        row.parallactic_angle_rad = [0.5; 2];
        let rotated = taps.taps(taps.key(&row, 1.4e9, 0.), &mut second);
        assert_eq!(initial.half_support(), rotated.half_support());
        let peak = peak_since(baseline);
        assert!(peak <= budget, "{peak} bytes used over {budget}");
        assert!(live_since(baseline) < budget);
        assert_eq!(taps.max_half_support(), [3; 2]);
        assert!(taps.admits(500.));
        assert!(!taps.admits(-5000.));
    }

    #[test]
    fn pbw_shape_covers_both_fine_offset_endpoints() {
        assert!(LegacyPbWConvolution::validate_shape(&[121, 121, 2, 1], 20, 3, 2).is_err());
        assert!(LegacyPbWConvolution::validate_shape(&[9, 9, 2, 1], 3, 1, 2).is_err());
        assert!(LegacyPbWConvolution::validate_shape(&[11, 11, 2, 1], 3, 1, 2).is_ok());
    }

    #[test]
    fn shared_pbw_grids_hold_one_reservation_between_layouts() {
        use super::super::FourierGrids;
        use casa_imaging_model::PolarizationCoordinate;
        use casa_imaging_operator::{
            Basis, Boxcar, GridPrecision, MeasurementOperator, PlaneRange,
        };
        use casa_imaging_runtime::reserved_memory;
        use casa_types::Correlation;
        use std::sync::Arc;

        if !isolated_test(
            module_path!(),
            "shared_pbw_grids_hold_one_reservation_between_layouts",
        ) {
            return;
        }
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [32; 2],
                increment_rad: [-1e-4, 1e-4],
                reference_pixel: [16; 2],
            },
            GridPadding::None,
        )
        .unwrap();
        let routing = PolarizationRouting::from_saved_planes(
            &[Correlation::RR],
            &[PolarizationCoordinate::CircularRr],
        )
        .unwrap();
        let kernels = Box::new(Boxcar::new(&geometry, &routing));
        let operator = MeasurementOperator::new(
            geometry,
            Basis::Constant,
            routing,
            kernels,
            GridPrecision::F32,
        );
        let before = reserved_memory();
        let peak = (32 * 32 * size_of::<Complex32>()) as u64 * 5;
        let memory = reserve("test PB/W grid read", peak).unwrap();
        let grids = operator.empty_model_grids(PlaneRange::new(0, 1)).unwrap();
        let bytes = grids.bytes();
        let layout = Arc::new(FourierGrids::retaining(grids, Some(memory)));
        let second_layout = Arc::clone(&layout);
        assert_eq!(reserved_memory() - before, bytes);
        drop(layout);
        assert_eq!(reserved_memory() - before, bytes);
        drop(second_layout);
        assert_eq!(reserved_memory(), before);
    }
}
