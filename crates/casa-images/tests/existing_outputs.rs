// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image tasks' guard on existing products (#931). With overwrite off,
//! each task leaves an existing product byte for byte as it was. With
//! overwrite on, the replaced pixels match a fresh run. Feather follows
//! CASA's overwrite default, so its refusal requires `--no-overwrite`.

mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use casa_images::PagedImage;
use ndarray::ArrayD;
use support::{write_image, write_image_with_geometry};

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

/// A product's pixels, from a CASA image or a FITS file.
fn pixels(path: &Path) -> ArrayD<f32> {
    if path
        .extension()
        .is_some_and(|extension| extension == "fits")
    {
        casa_images::read_fits_image(path).unwrap().pixels
    } else {
        PagedImage::<f32>::open(path).unwrap().get().unwrap()
    }
}

fn run(binary: &str, args: &[&str], workspace: &Path) -> Output {
    Command::new(binary)
        .args(args)
        .current_dir(workspace)
        .output()
        .expect("run task binary")
}

fn assert_ran(binary: &str, args: &[&str], output: &Output) {
    assert!(
        output.status.success(),
        "{binary} {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// `args` with the first `from` replaced by `to`.
fn substituted<'a>(args: &[&'a str], from: &str, to: &'a str) -> Vec<&'a str> {
    let mut args = args.to_vec();
    let at = args.iter().position(|arg| *arg == from).unwrap();
    args[at] = to;
    args
}

/// Write `product` from `input`, then attempt a second write with overwrite
/// off. Verify replacement with overwrite on against a fresh run from
/// `replacement`, an input with other pixels.
fn assert_guards_its_product(
    binary: &str,
    args: &[&str],
    product: &str,
    (input, replacement): (&str, &str),
    workspace: &Path,
    overwrite_by_default: bool,
) {
    assert_ran(binary, args, &run(binary, args, workspace));
    let path = workspace.join(product);
    let before = snapshot(&path);
    assert!(!before.is_empty(), "{binary} wrote no {product}");
    let first_pixels = pixels(&path);

    let mut refusing = args.to_vec();
    if overwrite_by_default {
        refusing.push("--no-overwrite");
    }
    let second = run(binary, &refusing, workspace);
    assert!(!second.status.success(), "{binary} replaced {product}");
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("already exists"), "{binary}: {stderr}");
    assert_eq!(snapshot(&path), before, "{binary} changed {product}");

    let from_replacement = substituted(args, input, replacement);
    let mut replacing = from_replacement.clone();
    if !overwrite_by_default {
        replacing.push("--overwrite");
    }
    assert_ran(binary, &replacing, &run(binary, &replacing, workspace));
    let fresh = format!("fresh-{product}");
    let reference = substituted(&from_replacement, product, &fresh);
    assert_ran(binary, &reference, &run(binary, &reference, workspace));

    let replaced = pixels(&path);
    assert_ne!(
        replaced, first_pixels,
        "{binary} kept {product} with overwrite on"
    );
    assert_eq!(
        replaced,
        pixels(&workspace.join(&fresh)),
        "{binary} did not write what a fresh run writes with overwrite on"
    );
}

#[test]
fn image_tasks_guard_existing_products_when_overwrite_is_off() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path();
    write_image(&path.join("in.image"), 1.0);
    write_image(&path.join("other.image"), 3.0);
    write_image(&path.join("low.image"), 0.5);

    let cases: [(&str, &[&str], &str, bool); 7] = [
        (
            env!("CARGO_BIN_EXE_imsubimage"),
            &["in.image", "out.sub"],
            "out.sub",
            false,
        ),
        (
            env!("CARGO_BIN_EXE_immoments"),
            &["in.image", "--outfile", "out.mom0", "--moments", "0"],
            "out.mom0",
            false,
        ),
        (
            env!("CARGO_BIN_EXE_immath"),
            &[
                "--imagename",
                "in.image",
                "--expr",
                "2*IM0",
                "--outfile",
                "out.math",
            ],
            "out.math",
            false,
        ),
        (
            env!("CARGO_BIN_EXE_impbcor"),
            &[
                "--imagename",
                "in.image",
                "--pbimage",
                "in.image",
                "--outfile",
                "out.pbcor",
            ],
            "out.pbcor",
            false,
        ),
        (
            env!("CARGO_BIN_EXE_imregrid"),
            &[
                "--imagename",
                "in.image",
                "--template",
                "in.image",
                "--output",
                "out.regrid",
            ],
            "out.regrid",
            false,
        ),
        (
            env!("CARGO_BIN_EXE_feather"),
            &[
                "--imagename",
                "out.feather",
                "--highres",
                "in.image",
                "--lowres",
                "low.image",
            ],
            "out.feather",
            true,
        ),
        (
            env!("CARGO_BIN_EXE_exportfits"),
            &["in.image", "out.fits"],
            "out.fits",
            false,
        ),
    ];
    for (binary, args, product, overwrite_by_default) in cases {
        assert_guards_its_product(
            binary,
            args,
            product,
            ("in.image", "other.image"),
            path,
            overwrite_by_default,
        );
    }
    // PV requires square pixels and endpoints two pixels inside the direction plane.
    let increment = [-2.0e-6, 2.0e-6];
    write_image_with_geometry(&path.join("pv-in.image"), 1.0, 8, increment);
    write_image_with_geometry(&path.join("pv-other.image"), 3.0, 8, increment);
    assert_guards_its_product(
        env!("CARGO_BIN_EXE_impv"),
        &[
            "pv-in.image",
            "--outfile",
            "out.pv",
            "--start",
            "2,3",
            "--end",
            "5,3",
        ],
        "out.pv",
        ("pv-in.image", "pv-other.image"),
        path,
        false,
    );
    // importfits reads the FITS files exportfits writes: out.fits now holds
    // other.image's pixels, and in.fits holds in.image's.
    let exportfits = env!("CARGO_BIN_EXE_exportfits");
    let setup = ["in.image", "in.fits"];
    assert_ran(exportfits, &setup, &run(exportfits, &setup, path));
    assert_guards_its_product(
        env!("CARGO_BIN_EXE_importfits"),
        &["out.fits", "back.image"],
        "back.image",
        ("out.fits", "in.fits"),
        path,
        false,
    );
}
