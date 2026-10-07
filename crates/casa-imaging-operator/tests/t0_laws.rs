// SPDX-License-Identifier: LGPL-3.0-or-later
//! T0 laws of the standard operator on the CPU backend, both precisions:
//! adjoint dot product, PSF peak and symmetry, point-source shift,
//! `sumwt = ΣW`, worker-count invariance and f32/f64 agreement.

mod common;

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CpuBackend, GridBackend, GridPrecision, Mode, ModeSet, ModelImages, ModelPlane,
    ModelPrescale, NormalImages, PlaneRange, Tile, Work,
};
use common::{IMAGE, INCREMENT_RAD, Rng, buffer, max_abs, operator, placements, samples};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];

fn tolerance(precision: GridPrecision) -> f64 {
    match precision {
        GridPrecision::F32 => 1.0e-4,
        GridPrecision::F64 => 1.0e-6,
    }
}

fn random_model(pols: usize, rng: &mut Rng) -> ModelImages {
    let images = (0..pols)
        .map(|_| Array2::from_shape_fn((IMAGE, IMAGE), |_| rng.signed() as f32))
        .collect();
    ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane { images }],
    }
}

/// `⟨A x, d⟩_W` and `⟨x, A* W d⟩` agree. With requested correlations or a
/// single Stokes I plane the polarization basis is the identity and the law
/// is exact; with `[I, Q]` from `[XX, YY]` CASA's image-domain basis is
/// `½·Sᴴ`, so the image-side product is half the visibility-side one.
fn adjoint_law(precision: GridPrecision, requested: &[PolarizationCoordinate], basis_factor: f64) {
    let operator = operator(precision, Basis::Constant, &XX_YY, requested);
    let mut rng = Rng::new(7);
    let placed = placements(operator.geometry(), 300, 1, &mut rng);
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let model = random_model(requested.len(), &mut rng);
    let mut backend = CpuBackend::new();

    let prepared = operator
        .prepare_model(&model, ModelPrescale::Unit)
        .expect("prepared model");
    let mut predicted = vec![Complex32::default(); placed.len() * 2];
    backend
        .apply(
            &block.block(),
            operator.cf(),
            Work::Predict {
                model: &prepared,
                out: &mut predicted,
            },
        )
        .expect("predict");
    let visibility_side = predicted
        .iter()
        .zip(&values)
        .zip(&weights)
        .map(|((ax, d), w)| (Complex64::new(f64::from(ax.re), f64::from(ax.im)).conj() * d * w).re)
        .sum::<f64>();

    let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA);
    backend
        .apply(
            &block.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut acc,
            },
        )
        .expect("grid");
    let normal = operator.finish(acc).expect("finish");
    let data = normal.planes[0].data.as_ref().expect("data section");
    let image_side = (0..requested.len())
        .map(|pol| {
            data.image(0, pol)
                .iter()
                .zip(&model.planes[0].images[pol])
                .map(|(image, model)| f64::from(*image) * f64::from(*model))
                .sum::<f64>()
        })
        .sum::<f64>();

    let scale = visibility_side.abs().max(image_side.abs());
    assert!(
        (visibility_side - basis_factor * image_side).abs() <= tolerance(precision) * scale,
        "⟨Ax,d⟩ = {visibility_side}, ⟨x,A*d⟩ = {image_side}"
    );
}

#[test]
fn adjoint_dot_product_holds_for_stokes_i_both_precisions() {
    adjoint_law(GridPrecision::F64, &STOKES_I, 1.0);
    adjoint_law(GridPrecision::F32, &STOKES_I, 1.0);
}

#[test]
fn adjoint_dot_product_holds_for_requested_correlations() {
    let xx_yy = [
        PolarizationCoordinate::LinearXx,
        PolarizationCoordinate::LinearYy,
    ];
    adjoint_law(GridPrecision::F64, &xx_yy, 1.0);
}

#[test]
fn casa_stokes_basis_is_half_the_adjoint() {
    let i_q = [
        PolarizationCoordinate::StokesI,
        PolarizationCoordinate::StokesQ,
    ];
    adjoint_law(GridPrecision::F64, &i_q, 2.0);
}

fn psf_and_dirty(precision: GridPrecision, flux: f64, shift: [i64; 2]) -> (NormalImages, f64) {
    let operator = operator(precision, Basis::Constant, &XX_YY, &STOKES_I);
    let mut rng = Rng::new(11);
    let placed = placements(operator.geometry(), 400, 1, &mut rng);
    let (_, weights) = samples(&placed, 2, &mut rng);
    let l = shift[0] as f64 * INCREMENT_RAD[0];
    let m = shift[1] as f64 * INCREMENT_RAD[1];
    // The source sits at (l, m) from the image centre; each row's data are
    // observed about a phase centre `p.phase` away, which the block's
    // pre-multiplication by `e^{iφ}` undoes.
    let values = placed
        .iter()
        .flat_map(|p| {
            let value =
                Complex64::from_polar(flux, std::f64::consts::TAU * (p.u * l + p.v * m) - p.phase);
            [value, value]
        })
        .collect::<Vec<_>>();
    let block = buffer(&placed, &values, &weights, 2);
    let mut backend = CpuBackend::new();
    let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA_PSF);
    for mode in [Mode::Data, Mode::Psf] {
        backend
            .apply(
                &block.block(),
                operator.cf(),
                Work::Grid {
                    mode,
                    acc: &mut acc,
                },
            )
            .expect("grid");
    }
    (operator.finish(acc).expect("finish"), weights.iter().sum())
}

fn psf_laws(precision: GridPrecision) {
    let (normal, weight_sum) = psf_and_dirty(precision, 1.0, [0, 0]);
    let psf = normal.planes[0].psf.as_ref().expect("psf section");
    let sumwt = psf.sumwt_of(0, 0);
    assert!(
        (sumwt - weight_sum).abs() <= 1.0e-6 * weight_sum,
        "sumwt {sumwt} != ΣW {weight_sum}"
    );
    let image = psf.image(0, 0);
    let centre = IMAGE / 2;
    let peak = f64::from(image[(centre, centre)]) / sumwt;
    assert!((peak - 1.0).abs() < 1.0e-3, "normalised PSF peak {peak}");
    let largest = max_abs(image.iter().copied()) / sumwt;
    assert!(
        (largest - peak).abs() < 1.0e-9,
        "peak is not at the reference pixel"
    );
    let symmetry = tolerance(precision).max(1.0e-9);
    for y in 1..IMAGE {
        for x in 1..IMAGE {
            let here = f64::from(image[(y, x)]);
            let mirrored = f64::from(image[(2 * centre - y, 2 * centre - x)]);
            assert!(
                (here - mirrored).abs() <= symmetry * sumwt,
                "PSF not point-symmetric at ({x}, {y}): {here} vs {mirrored}"
            );
        }
    }
}

#[test]
fn psf_peak_is_one_after_normalisation_and_point_symmetric() {
    psf_laws(GridPrecision::F64);
    psf_laws(GridPrecision::F32);
}

fn point_source_law(precision: GridPrecision) {
    let flux = 2.5;
    let shift = [5_i64, -3_i64];
    let (normal, _) = psf_and_dirty(precision, flux, shift);
    let plane = &normal.planes[0];
    let data = plane.data.as_ref().expect("data");
    let psf = plane.psf.as_ref().expect("psf");
    let sumwt = psf.sumwt_of(0, 0);
    assert_eq!(data.sumwt_of(0, 0), sumwt);
    let dirty = data.image(0, 0);
    let psf = psf.image(0, 0);
    let centre = (IMAGE / 2) as i64;
    let peak = dirty
        .indexed_iter()
        .max_by(|a, b| a.1.total_cmp(b.1))
        .map(|((y, x), _)| [x as i64, y as i64])
        .expect("peak");
    assert_eq!(peak, [centre + shift[0], centre + shift[1]]);
    let mut worst = 0.0_f64;
    for y in 0..IMAGE as i64 {
        for x in 0..IMAGE as i64 {
            let (sx, sy) = (x - shift[0], y - shift[1]);
            if sx < 0 || sy < 0 || sx >= IMAGE as i64 || sy >= IMAGE as i64 {
                continue;
            }
            let expected = flux * f64::from(psf[(sy as usize, sx as usize)]) / sumwt;
            let actual = f64::from(dirty[(y as usize, x as usize)]) / sumwt;
            worst = worst.max((expected - actual).abs());
        }
    }
    assert!(
        worst < 1.0e-3 * flux,
        "dirty differs from shifted PSF by {worst}"
    );
}

#[test]
fn point_source_dirty_image_is_flux_times_shifted_psf() {
    point_source_law(GridPrecision::F64);
    point_source_law(GridPrecision::F32);
}

#[test]
fn plane_partition_is_bitwise_invariant_in_f64() {
    let planes = 3;
    let operator = operator(
        GridPrecision::F64,
        Basis::ChannelLocal { planes },
        &XX_YY,
        &STOKES_I,
    );
    let mut rng = Rng::new(3);
    let placed = placements(operator.geometry(), 600, planes, &mut rng);
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let mut backend = CpuBackend::new();
    let mut whole = operator.accumulator(PlaneRange::new(0, planes), None, ModeSet::DATA);
    backend
        .apply(
            &block.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut whole,
            },
        )
        .expect("grid");
    let whole = operator.finish(whole).expect("finish");
    for plane in 0..planes {
        let mut own = operator.accumulator(PlaneRange::single(plane), None, ModeSet::DATA);
        let owned = placed
            .iter()
            .enumerate()
            .filter(|(_, p)| p.plane == plane)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let subset = owned.iter().map(|&i| placed[i]).collect::<Vec<_>>();
        let subset_values = owned
            .iter()
            .flat_map(|&i| values[i * 2..i * 2 + 2].iter().copied())
            .collect::<Vec<_>>();
        let subset_weights = owned
            .iter()
            .flat_map(|&i| weights[i * 2..i * 2 + 2].iter().copied())
            .collect::<Vec<_>>();
        let subset = buffer(&subset, &subset_values, &subset_weights, 2);
        let mut worker = CpuBackend::new();
        worker
            .apply(
                &subset.block(),
                operator.cf(),
                Work::Grid {
                    mode: Mode::Data,
                    acc: &mut own,
                },
            )
            .expect("grid");
        let own = operator.finish(own).expect("finish");
        assert_eq!(
            own.planes[0], whole.planes[plane as usize],
            "plane {plane} differs between one worker and a plane owner"
        );
    }
}

fn tiled_partition_law(precision: GridPrecision) {
    let operator = operator(precision, Basis::Constant, &XX_YY, &STOKES_I);
    let geometry = operator.geometry().clone();
    let [nx, ny] = geometry.grid_shape();
    let mut rng = Rng::new(5);
    let placed = placements(&geometry, 500, 1, &mut rng);
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let mut backend = CpuBackend::new();
    let mut whole = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA);
    backend
        .apply(
            &block.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut whole,
            },
        )
        .expect("grid");
    let reference = operator.finish(whole).expect("finish");

    // Two tiles split at y = ny/2 with a four-cell halo; samples go to the
    // tile owning their anchor row.
    let split = ny / 2;
    let halo = 4;
    let tiles = [
        Tile {
            origin: [0, 0],
            shape: [nx, split + halo],
        },
        Tile {
            origin: [0, split - halo],
            shape: [nx, ny - split + halo],
        },
    ];
    let mut merged = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA);
    for (which, tile) in tiles.iter().enumerate() {
        let owned = placed
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                let anchor = geometry.locate(p.u, p.v, 100).y as usize;
                (anchor < split) == (which == 0)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let subset = owned.iter().map(|&i| placed[i]).collect::<Vec<_>>();
        let subset_values = owned
            .iter()
            .flat_map(|&i| values[i * 2..i * 2 + 2].iter().copied())
            .collect::<Vec<_>>();
        let subset_weights = owned
            .iter()
            .flat_map(|&i| weights[i * 2..i * 2 + 2].iter().copied())
            .collect::<Vec<_>>();
        let subset = buffer(&subset, &subset_values, &subset_weights, 2);
        let mut acc = operator.accumulator(PlaneRange::single(0), Some(*tile), ModeSet::DATA);
        let mut worker = CpuBackend::new();
        worker
            .apply(
                &subset.block(),
                operator.cf(),
                Work::Grid {
                    mode: Mode::Data,
                    acc: &mut acc,
                },
            )
            .expect("grid");
        assert!(matches!(
            operator.finish(acc.clone()),
            Err(casa_imaging_operator::OperatorError::TiledAccumulator)
        ));
        merged.merge_from(&acc).expect("merge");
    }
    let merged = operator.finish(merged).expect("finish");
    let expected = reference.planes[0].data.as_ref().expect("data");
    let actual = merged.planes[0].data.as_ref().expect("data");
    assert!(
        (expected.sumwt_of(0, 0) - actual.sumwt_of(0, 0)).abs() <= 1.0e-9 * expected.sumwt_of(0, 0)
    );
    let peak = max_abs(expected.image(0, 0).iter().copied());
    let worst = expected
        .image(0, 0)
        .iter()
        .zip(actual.image(0, 0))
        .map(|(a, b)| f64::from((a - b).abs()))
        .fold(0.0_f64, f64::max);
    assert!(
        worst <= tolerance(precision) * peak,
        "tiled merge differs by {worst} of {peak}"
    );
}

#[test]
fn tiled_partition_merges_to_the_single_grid_result() {
    tiled_partition_law(GridPrecision::F64);
    tiled_partition_law(GridPrecision::F32);
}

#[test]
fn f32_and_f64_grids_agree() {
    let mut images = Vec::new();
    for precision in [GridPrecision::F64, GridPrecision::F32] {
        let (normal, _) = psf_and_dirty(precision, 1.7, [2, 4]);
        images.push(normal);
    }
    for section in [
        |plane: &casa_imaging_operator::NormalPlane| plane.data.clone(),
        |plane: &casa_imaging_operator::NormalPlane| plane.psf.clone(),
    ] {
        let f64_section = section(&images[0].planes[0]).expect("section");
        let f32_section = section(&images[1].planes[0]).expect("section");
        let sumwt = f64_section.sumwt_of(0, 0);
        let peak = max_abs(f64_section.image(0, 0).iter().copied()) / sumwt;
        let worst = f64_section
            .image(0, 0)
            .iter()
            .zip(f32_section.image(0, 0))
            .map(|(a, b)| f64::from((a - b).abs()) / sumwt)
            .fold(0.0_f64, f64::max);
        assert!(
            worst <= 1.0e-4 * peak,
            "precisions differ by {worst} of {peak}"
        );
    }
}

#[test]
fn residual_grid_of_the_predicted_model_is_empty() {
    let operator = operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let mut rng = Rng::new(13);
    let placed = placements(operator.geometry(), 200, 1, &mut rng);
    let (_, weights) = samples(&placed, 2, &mut rng);
    let model = random_model(1, &mut rng);
    let prepared = operator
        .prepare_model(&model, ModelPrescale::Unit)
        .expect("prepared");
    let mut backend = CpuBackend::new();
    // Predict with unit weights, then feed the prediction back as data.
    let unit = vec![1.0; placed.len() * 2];
    let zero = vec![Complex64::default(); placed.len() * 2];
    let probe = buffer(&placed, &zero, &unit, 2);
    let mut predicted = vec![Complex32::default(); placed.len() * 2];
    backend
        .apply(
            &probe.block(),
            operator.cf(),
            Work::Predict {
                model: &prepared,
                out: &mut predicted,
            },
        )
        .expect("predict");
    let predicted = predicted
        .iter()
        .map(|v| Complex64::new(f64::from(v.re), f64::from(v.im)))
        .collect::<Vec<_>>();
    let block = buffer(&placed, &predicted, &weights, 2);
    let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA);
    let mut residual = vec![Complex32::new(1.0, 1.0); placed.len() * 2];
    backend
        .apply(
            &block.block(),
            operator.cf(),
            Work::ResidualGrid {
                model: &prepared,
                acc: &mut acc,
                residual_out: Some(&mut residual),
            },
        )
        .expect("residual");
    let largest = predicted.iter().map(|v| v.norm()).fold(0.0_f64, f64::max);
    assert!(
        residual
            .iter()
            .all(|r| f64::from(r.norm()) <= 1.0e-5 * largest)
    );
    let normal = operator.finish(acc).expect("finish");
    let data = normal.planes[0].data.as_ref().expect("data");
    assert!(
        (data.sumwt_of(0, 0) - weights.iter().sum::<f64>()).abs() < 1.0e-6 * data.sumwt_of(0, 0)
    );
    let residual_peak = max_abs(data.image(0, 0).iter().copied()) / data.sumwt_of(0, 0);
    assert!(
        residual_peak <= 1.0e-5 * largest,
        "residual image peak {residual_peak}"
    );
}
