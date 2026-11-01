// SPDX-License-Identifier: LGPL-3.0-or-later
//! The header checks used by SIImageStore::openImage before reusing pixels.
//! This is the bounded native MFS grid, not general CoordinateSystem::near.

use std::path::Path;

use casa_coordinates::{CoordinateModel, CoordinateSystem, Projection, ProjectionType};
use casa_images::{AnyPagedImage, ImagePixel, RegridTemplate};

use crate::PrepareError;

pub(crate) fn check_grid(path: &Path, template: &RegridTemplate) -> Result<(), PrepareError> {
    let refuse = |reason: String| PrepareError::Restart {
        path: path.into(),
        reason,
    };
    let image = AnyPagedImage::open(path).map_err(|error| refuse(error.to_string()))?;
    let result = match &image {
        AnyPagedImage::Float32(image) => {
            if path
                .extension()
                .is_some_and(|suffix| suffix == "model" || suffix == "mask")
                && image.default_mask_name().is_some()
            {
                return Err(refuse(
                    "continuation preserving an existing model pixel mask or CLEAN-mask pixel mask is not implemented yet"
                        .into(),
                ));
            }
            // SIImageStore::openImage deliberately excludes sumwt from grid checks.
            if path.extension().is_some_and(|suffix| suffix == "sumwt") {
                Ok(())
            } else {
                check_header(image, template)
            }
        }
        AnyPagedImage::Float64(_) => Err("CASA restart requires a Float32 image".into()),
        _ => Err("existing image must be real".into()),
    };
    result.map_err(refuse)
}

fn check_header<T: ImagePixel>(
    image: &casa_images::PagedImage<T>,
    template: &RegridTemplate,
) -> Result<(), String> {
    if image.shape() != template.shape {
        return Err(format!(
            "shape mismatch: existing {:?}, requested {:?}",
            image.shape(),
            template.shape
        ));
    }
    check_coordinates(image.coordinates(), &template.coordinates)
}

fn check_coordinates(
    actual: &CoordinateSystem,
    requested: &CoordinateSystem,
) -> Result<(), String> {
    // CASA's near comparison also uses the scalar WCS fields here. Our bounded
    // direct-reader slice admits native linear MFS grids only; it does not yet
    // define continuation for a lookup table with the same scalar WCS summary.
    for system in [actual, requested] {
        if (0..system.n_coordinates()).any(|index| {
            matches!(system.coordinate(index),
            CoordinateModel::Spectral(spectral) if !spectral.has_linear_mapping())
        }) {
            return Err(
                "continuation from a tabulated spectral coordinate is not implemented yet".into(),
            );
        }
    }
    if !same_grid(actual, requested) {
        return Err("coordinate system mismatch; choose a new imagename and supply the old model as startmodel to regrid it".into());
    }
    Ok(())
}

fn same_grid(a: &CoordinateSystem, b: &CoordinateSystem) -> bool {
    a.n_coordinates() == b.n_coordinates()
        && (0..a.n_coordinates()).all(|i| {
            let (a, b) = (a.coordinate(i), b.coordinate(i));
            let reference_tolerance = if matches!(a, CoordinateModel::Spectral(_)) {
                1e-13
            } else {
                1e-6
            };
            a.coordinate_type() == b.coordinate_type()
                && a.axis_names() == b.axis_names()
                && a.axis_units() == b.axis_units()
                && near_values(
                    &a.reference_value(),
                    &b.reference_value(),
                    reference_tolerance,
                )
                && near_values(&a.reference_pixel(), &b.reference_pixel(), 1e-6)
                && near_values(&a.increment(), &b.increment(), 1e-6)
                && match (a, b) {
                    (CoordinateModel::Direction(a), CoordinateModel::Direction(b)) => {
                        a.direction_ref() == b.direction_ref()
                            && a.projection().projection_type() == b.projection().projection_type()
                            && same_projection_parameters(a.projection(), b.projection())
                            && same_angle(a.longpole(), b.longpole())
                            && a.native_pole()
                                .into_iter()
                                .zip(b.native_pole())
                                .all(|(a, b)| same_angle(a, b))
                            && a.pc_matrix()
                                .iter()
                                .zip(b.pc_matrix())
                                .all(|(&a, &b)| near(a, b, 1e-6))
                    }
                    (CoordinateModel::Spectral(a), CoordinateModel::Spectral(b)) => {
                        a.frequency_ref() == b.frequency_ref()
                            && a.world_frequency_ref() == b.world_frequency_ref()
                            && near(a.rest_frequency(), b.rest_frequency(), 1e-6)
                    }
                    (CoordinateModel::Stokes(a), CoordinateModel::Stokes(b)) => {
                        a.stokes() == b.stokes()
                    }
                    _ => false,
                }
        })
}

/// CASA stores the default SIN slant parameters explicitly; native headers
/// omit them. Preserve all other parameter comparisons, including nonzero slant.
fn same_projection_parameters(a: &Projection, b: &Projection) -> bool {
    let (a_parameters, b_parameters) = (a.parameters(), b.parameters());
    if a.projection_type() == ProjectionType::SIN
        && b.projection_type() == ProjectionType::SIN
        && ((a_parameters.is_empty() && b_parameters == [0.0, 0.0])
            || (b_parameters.is_empty() && a_parameters == [0.0, 0.0]))
    {
        return true;
    }
    near_values(a_parameters, b_parameters, 1e-6)
}

/// A roundoff allowance for the resolved celestial rotation, in radians.
/// LATPOLE is a solution selector, so compare its resolved pole instead.
fn same_angle(a: f64, b: f64) -> bool {
    let delta = a - b;
    delta.sin().atan2(delta.cos()).abs() <= 1e-13
}

fn near_values(a: &[f64], b: &[f64], tolerance: f64) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(&a, &b)| near(a, b, tolerance))
}

/// casacore::near(Double, Double), including its special zero boundary.
fn near(a: f64, b: f64, tolerance: f64) -> bool {
    if a == b {
        true
    } else if a == 0.0 || b == 0.0 {
        (a - b).abs() <= (1.0 + tolerance) * f64::MIN_POSITIVE
    } else {
        a.signum() == b.signum() && (a - b).abs() <= tolerance * a.abs().max(b.abs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_coordinates::{
        Coordinate, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    };
    use casa_types::measures::{direction::DirectionRef, frequency::FrequencyRef};

    #[test]
    fn restart_grid_rejects_pixel_scale_and_projection_changes() {
        let direction = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.1, -0.3],
            [-1e-5, 1e-5],
            [32.0, 32.0],
        );
        let mut original = CoordinateSystem::new();
        original.add_coordinate(direction.clone());
        assert!(same_grid(&original, &original));
        let mut changed = CoordinateSystem::new();
        changed.add_coordinate(direction.clone().with_increment([-2e-5, 2e-5]));
        assert!(!same_grid(&original, &changed));
        let mut shifted = CoordinateSystem::new();
        shifted.add_coordinate(direction.with_reference_pixel([31.0, 32.0]));
        assert!(!same_grid(&original, &shifted));
        let mut projected = CoordinateSystem::new();
        projected.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::TAN),
            [0.1, -0.3],
            [-1e-5, 1e-5],
            [32.0, 32.0],
        ));
        assert!(!same_grid(&original, &projected));
    }

    #[test]
    fn near_matches_casacore_relative_and_zero_boundaries() {
        assert!(near(1.0, 1.0 + 1e-7, 1e-6));
        assert!(!near(1.0, 1.0 + 1e-5, 1e-6));
        assert!(!near(0.0, 1e-20, 1e-6));
    }

    #[test]
    fn scalar_wcs_equality_does_not_admit_an_unsupported_spectral_lookup() {
        let linear = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 1e9);
        let tabulated = SpectralCoordinate::from_tabular(
            FrequencyRef::LSRK,
            vec![0.0, 1.0],
            vec![1e9, 1e9 + 2e6],
            1e9,
            1e6,
            0.0,
            1e9,
        )
        .unwrap();
        assert_eq!(linear.reference_value(), tabulated.reference_value());
        assert_eq!(linear.reference_pixel(), tabulated.reference_pixel());
        assert_eq!(linear.increment(), tabulated.increment());
        assert_ne!(
            linear.to_world(&[1.0]).unwrap(),
            tabulated.to_world(&[1.0]).unwrap()
        );
        let mut requested = CoordinateSystem::new();
        requested.add_coordinate(linear);
        let mut existing = CoordinateSystem::new();
        existing.add_coordinate(tabulated);
        assert!(check_coordinates(&requested, &requested).is_ok());
        let refusal = check_coordinates(&existing, &requested).unwrap_err();
        assert!(refusal.contains("not implemented yet"), "{refusal}");
    }
    fn grid_direction(projection: Projection) -> DirectionCoordinate {
        DirectionCoordinate::new(
            DirectionRef::J2000,
            projection,
            [1.3, (-30.0_f64).to_radians()],
            [-2.3e-5, 1.7e-5],
            [31.25, 27.75],
        )
    }

    fn direction_system(direction: DirectionCoordinate) -> CoordinateSystem {
        let mut system = CoordinateSystem::new();
        system.add_coordinate(direction);
        system
    }

    #[test]
    fn restart_grid_accepts_equivalent_casa_sin_pole_and_zero_parameters() {
        let native = grid_direction(Projection::new(ProjectionType::SIN));
        let casa = grid_direction(Projection::with_parameters(
            ProjectionType::SIN,
            vec![0.0, 0.0],
        ))
        .with_latpole((-29.999999999999996_f64).to_radians());
        assert_ne!(native.latpole(), casa.latpole());
        assert_ne!(
            native.projection().parameters(),
            casa.projection().parameters()
        );
        for pixel in [[0.0, 0.0], [4.5, 57.25], [62.0, 13.0], [31.25, 27.75]] {
            let native_world = native.to_world(&pixel).unwrap();
            let casa_world = casa.to_world(&pixel).unwrap();
            for axis in 0..2 {
                assert!(
                    (native_world[axis] - casa_world[axis]).abs() <= 1e-13,
                    "{pixel:?}: native {native_world:?}, CASA {casa_world:?}"
                );
            }
            let back = native.to_pixel(&casa_world).unwrap();
            for axis in 0..2 {
                assert!(
                    (back[axis] - pixel[axis]).abs() <= 1e-8,
                    "{pixel:?} returned as {back:?}"
                );
            }
        }
        let native = direction_system(native);
        let casa = direction_system(casa);
        assert!(same_grid(&casa, &native));
        assert!(same_grid(&native, &casa));
    }

    #[test]
    fn restart_grid_rejects_real_rotation_pc_and_slant_changes() {
        let original = grid_direction(Projection::new(ProjectionType::SIN));
        let changed = [
            original.clone().with_longpole(std::f64::consts::PI + 0.01),
            original
                .clone()
                .with_pc_matrix(ndarray::arr2(&[[1.0, 0.04], [-0.03, 1.0]])),
            grid_direction(Projection::with_parameters(
                ProjectionType::SIN,
                vec![0.1, 0.0],
            )),
        ];
        let pixel = [3.0, 55.0];
        let world = original.to_world(&pixel).unwrap();
        let original = direction_system(original);
        for changed in changed {
            let changed_world = changed.to_world(&pixel).unwrap();
            assert!((0..2).any(|axis| (world[axis] - changed_world[axis]).abs() > 1e-10));
            assert!(!same_grid(&original, &direction_system(changed)));
        }
    }

    #[test]
    fn restart_grid_rejects_a_different_resolved_nonzenithal_pole_branch() {
        let direction = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::CAR),
            [1.3, 0.0],
            [-2.3e-5, 1.7e-5],
            [31.25, 27.75],
        )
        .with_longpole(0.7);
        let north = direction.clone().with_latpole(std::f64::consts::FRAC_PI_2);
        let south = direction.with_latpole(-std::f64::consts::FRAC_PI_2);
        let pixel = [3.0, 55.0];
        let north_world = north.to_world(&pixel).unwrap();
        let south_world = south.to_world(&pixel).unwrap();
        assert!((0..2).any(|axis| (north_world[axis] - south_world[axis]).abs() > 1e-8));
        assert!(!same_grid(
            &direction_system(north),
            &direction_system(south)
        ));
    }
}
