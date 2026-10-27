// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image tasks' guard on existing products (#931): CASA's `overwrite` is
//! the consent. A task binary run a second time without `--overwrite` fails
//! and leaves the first product byte for byte as it was; with `--overwrite`
//! it replaces it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
};
use casa_images::{GaussianBeam, ImageBeamSet, ImageInfo, ImageType, PagedImage};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

/// A 4x4x3 direction-and-frequency image with a beam.
fn write_image(path: &Path, scale: f32) {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-2.0e-6, 3.0e-6],
        [1.5, 1.5],
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        372.0e9,
        2.0e6,
        0.0,
        372.0e9,
    ));
    let shape = vec![4, 4, 3];
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
    let data = ArrayD::from_shape_vec(
        IxDyn(&shape).f(),
        (1..=shape.iter().product::<usize>())
            .map(|value| value as f32 * scale)
            .collect(),
    )
    .unwrap();
    image.put_slice(&data, &[0, 0, 0]).unwrap();
    image.set_units("Jy/beam").unwrap();
    image
        .set_image_info(&ImageInfo {
            beam_set: ImageBeamSet::new(GaussianBeam::new(
                1.7e-4_f64.to_radians(),
                1.2e-4_f64.to_radians(),
                (-34.5_f64).to_radians(),
            )),
            image_type: ImageType::Intensity,
            object_name: "inventory".to_string(),
        })
        .unwrap();
    image.save().unwrap();
}

/// Every file under `path`, by relative path, with its bytes.
fn snapshot(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                walk(root, &entry.unwrap().path(), files);
            }
        } else {
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            files.insert(relative, std::fs::read(path).unwrap());
        }
    }
    let mut files = BTreeMap::new();
    walk(path, path, &mut files);
    files
}

fn run(binary: &str, args: &[&str], workspace: &Path) -> Output {
    Command::new(binary)
        .args(args)
        .current_dir(workspace)
        .output()
        .expect("run task binary")
}

/// Run `binary` with `args` (which write `product`) twice, then once more with
/// `--overwrite`.
fn assert_guards_its_product(binary: &str, args: &[&str], product: &str, workspace: &Path) {
    let first = run(binary, args, workspace);
    assert!(
        first.status.success(),
        "{binary} {args:?}: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let product = workspace.join(product);
    let before = snapshot(&product);
    assert!(!before.is_empty(), "{binary} wrote no {product:?}");

    let second = run(binary, args, workspace);
    assert!(!second.status.success(), "{binary} replaced {product:?}");
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("already exists"), "{binary}: {stderr}");
    assert_eq!(snapshot(&product), before, "{binary} changed {product:?}");

    let mut replacing = args.to_vec();
    replacing.push("--overwrite");
    let third = run(binary, &replacing, workspace);
    assert!(
        third.status.success(),
        "{binary} --overwrite: {}",
        String::from_utf8_lossy(&third.stderr)
    );
    assert!(product.exists());
}

#[test]
fn each_image_task_refuses_its_existing_product_without_overwrite() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path();
    write_image(&path.join("in.image"), 1.0);
    write_image(&path.join("low.image"), 0.5);

    let cases: [(&str, &[&str], &str); 8] = [
        (env!("CARGO_BIN_EXE_imsubimage"), &["in.image", "out.sub"], "out.sub"),
        (
            env!("CARGO_BIN_EXE_immoments"),
            &["in.image", "--outfile", "out.mom0", "--moments", "0"],
            "out.mom0",
        ),
        (
            env!("CARGO_BIN_EXE_immath"),
            &["--imagename", "in.image", "--expr", "2*IM0", "--outfile", "out.math"],
            "out.math",
        ),
        (
            env!("CARGO_BIN_EXE_impbcor"),
            &["--imagename", "in.image", "--pbimage", "in.image", "--outfile", "out.pbcor"],
            "out.pbcor",
        ),
        (
            env!("CARGO_BIN_EXE_imregrid"),
            &["--imagename", "in.image", "--template", "in.image", "--output", "out.regrid"],
            "out.regrid",
        ),
        (
            env!("CARGO_BIN_EXE_feather"),
            &["--imagename", "out.feather", "--highres", "in.image", "--lowres", "low.image"],
            "out.feather",
        ),
        (
            env!("CARGO_BIN_EXE_impv"),
            &["in.image", "--outfile", "out.pv", "--start", "0,0", "--end", "3,3"],
            "out.pv",
        ),
        (env!("CARGO_BIN_EXE_exportfits"), &["in.image", "out.fits"], "out.fits"),
    ];
    for (binary, args, product) in cases {
        assert_guards_its_product(binary, args, product, path);
    }
    // importfits reads the FITS file exportfits wrote.
    assert_guards_its_product(
        env!("CARGO_BIN_EXE_importfits"),
        &["out.fits", "back.image"],
        "back.image",
        path,
    );
}
