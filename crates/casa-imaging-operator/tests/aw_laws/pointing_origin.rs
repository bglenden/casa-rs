// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;

/// Make constant-tap caches, optionally extending either kind of cell to
/// an odd side. The area-normalized taps agree at both array origins.
fn prepare_constant_cache(root: &std::path::Path, odd: [bool; 2]) {
    for entry in std::fs::read_dir(root).expect("cache entries") {
        let path = entry.expect("entry").path();
        let name = path.file_name().expect("name").to_string_lossy();
        if !(name.starts_with("CFS_") || name.starts_with("WTCFS_")) || !name.ends_with(".im") {
            continue;
        }
        let mut image = casa_images::PagedImage::<Complex32>::open(&path).expect("open cell");
        let shape = image.shape().to_vec();
        assert!(shape[0].is_multiple_of(2) && shape[1].is_multiple_of(2));
        if !odd[usize::from(name.starts_with("WTCFS_"))] {
            let values =
                ndarray::ArrayD::from_elem(ndarray::IxDyn(&shape), Complex32::new(1.0, 0.0));
            image
                .put_slice(&values, &[0, 0, 0, 0])
                .expect("write even cell");
            image.save().expect("save even cell");
            continue;
        }
        let coords = image.coordinates().clone();
        let misc = image.misc_info();
        drop(image);
        let new_shape = vec![shape[0] + 1, shape[1] + 1, 1, 1];
        // Uniform cells make the normalizing areas and every sampled tap
        // identical despite the distinct array origins.
        let padded =
            ndarray::ArrayD::from_elem(ndarray::IxDyn(&new_shape), Complex32::new(1.0, 0.0));
        let replacement = path.with_extension("replacement");
        let mut output =
            casa_images::PagedImage::<Complex32>::create(new_shape, coords, &replacement)
                .expect("create odd cell");
        output
            .put_slice(&padded, &[0, 0, 0, 0])
            .expect("write odd cell");
        output.set_misc_info(misc).expect("copy metadata");
        output.save().expect("save odd cell");
        drop(output);
        std::fs::remove_dir_all(&path).expect("remove generated test cell");
        std::fs::rename(&replacement, &path).expect("replace generated test cell");
    }
}

/// Open a cache whose indexed taps are identical at both parities.
fn constant_cache(odd: [bool; 2]) -> (tempfile::TempDir, MeasurementOperator) {
    let root = tempfile::tempdir().expect("constant cache");
    AwCatalog::generate_native(root.path(), &request(false), false, 1).expect("generate");
    prepare_constant_cache(root.path(), odd);
    let operator = operator_with(
        AwCatalog::open_casa(
            root.path(),
            indexing(false),
            &geometry(),
            &routing(),
            usize::MAX,
        )
        .expect("open constant cache"),
    );
    (root, operator)
}

/// An odd CASA buffer shifts the pointing phase one fine pixel in its own
/// grid modes. Prediction takes the conjugate of the imaging phase (#1070).
#[test]
fn cache_buffer_origins_reach_gridding_and_prediction() {
    let (_root, even) = constant_cache([false, false]);
    let key = even.cf().key(&context(17.0, 0), FREQUENCIES_HZ[0], 0.0);
    let gradient = [0.37_f32, -0.21_f32];
    let placement = Placement {
        u: 13.0,
        v: -19.0,
        w: 0.0,
        prediction_w_positive: false,
        phase: 0.0,
        plane: 0,
        spectral: 0.0,
        cf: key,
        gradient,
    };
    assert!(fits(&even, &placement));
    let mut samples = SampleBuffer::new(2);
    samples.push(
        placement,
        &[Complex32::new(2.0, -0.5), Complex32::new(-0.7, 1.3)],
        &[1.0; 2],
    );
    let phase = Complex32::from_polar(1.0, -(gradient[0] + gradient[1]) / OVERSAMPLING as f32);
    let mut sky = Array2::zeros((IMAGE, IMAGE));
    sky[(IMAGE / 2 + 5, IMAGE / 2 - 3)] = 1.0;
    let model = ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane { images: vec![sky] }],
    };
    let predict = |operator: &MeasurementOperator| {
        let model = operator
            .prepare_model(&model, ModelPrescale::Unit)
            .expect("model");
        let mut out = [Complex32::default(); 2];
        CpuBackend::new()
            .apply(
                &samples.block(),
                operator.cf(),
                Work::Predict {
                    model: &model,
                    out: &mut out,
                },
            )
            .expect("predict");
        out
    };
    let baseline = predict(&even);
    assert!(
        baseline.iter().any(|v| v.norm() > 1e-6),
        "nonzero prediction"
    );
    for odd in [[true, true], [true, false], [false, true]] {
        let (_root, operator) = constant_cache(odd);
        assert!(fits(&operator, &placement));
        for mode in [Mode::Data, Mode::Psf, Mode::Weight] {
            let (a, wa) = grid(&even, &samples, mode);
            let (b, wb) = grid(&operator, &samples, mode);
            assert!(
                a.iter().any(|v| v.norm() > 1e-6),
                "{mode:?} must exercise taps"
            );
            let shift = if odd[usize::from(mode != Mode::Data)] {
                phase
            } else {
                Complex32::new(1.0, 0.0)
            };
            for (before, after) in a.iter().zip(&b) {
                assert!(
                    (*after - *before * shift).norm() < 1e-5,
                    "{odd:?} {mode:?}: {before} -> {after}"
                );
            }
            for (before, after) in wa.iter().zip(wb) {
                assert!((*before - after).abs() < 1e-9, "{mode:?} sumwt");
            }
        }
        let shift = if odd[0] {
            phase.conj()
        } else {
            Complex32::new(1.0, 0.0)
        };
        for (before, after) in baseline.iter().zip(predict(&operator)) {
            assert!(
                (after - *before * shift).norm() < 1e-5,
                "{odd:?} predict: {before} -> {after}"
            );
        }
    }
}
