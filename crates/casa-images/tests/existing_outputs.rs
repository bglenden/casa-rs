// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image tasks' guard on existing products (#931): CASA's `overwrite` is
//! the consent. A task binary run a second time without `--overwrite` fails
//! and leaves the first product byte for byte as it was; with `--overwrite`
//! from an input with other pixels, the product holds what a fresh run from
//! that input writes.

mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use casa_images::PagedImage;
use ndarray::ArrayD;
use support::write_image;

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

/// Run `binary` with `args`, which write `product` from `input`, twice; then
/// with `--overwrite` from `replacement`, an input with other pixels. The
/// second run must change nothing, and the third must leave the pixels a
/// fresh run from `replacement` writes.
fn assert_guards_its_product(
    binary: &str,
    args: &[&str],
    product: &str,
    (input, replacement): (&str, &str),
    workspace: &Path,
) {
    assert_ran(binary, args, &run(binary, args, workspace));
    let path = workspace.join(product);
    let before = snapshot(&path);
    assert!(!before.is_empty(), "{binary} wrote no {product}");
    let first_pixels = pixels(&path);

    let second = run(binary, args, workspace);
    assert!(!second.status.success(), "{binary} replaced {product}");
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("already exists"), "{binary}: {stderr}");
    assert_eq!(snapshot(&path), before, "{binary} changed {product}");

    let from_replacement = substituted(args, input, replacement);
    let mut replacing = from_replacement.clone();
    replacing.push("--overwrite");
    assert_ran(binary, &replacing, &run(binary, &replacing, workspace));
    let fresh = format!("fresh-{product}");
    let reference = substituted(&from_replacement, product, &fresh);
    assert_ran(binary, &reference, &run(binary, &reference, workspace));

    let replaced = pixels(&path);
    assert_ne!(
        replaced, first_pixels,
        "{binary} --overwrite kept {product}"
    );
    assert_eq!(
        replaced,
        pixels(&workspace.join(&fresh)),
        "{binary} --overwrite did not write what a fresh run writes"
    );
}

#[test]
fn each_image_task_refuses_its_existing_product_without_overwrite() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path();
    write_image(&path.join("in.image"), 1.0);
    write_image(&path.join("other.image"), 3.0);
    write_image(&path.join("low.image"), 0.5);

    let cases: [(&str, &[&str], &str); 8] = [
        (
            env!("CARGO_BIN_EXE_imsubimage"),
            &["in.image", "out.sub"],
            "out.sub",
        ),
        (
            env!("CARGO_BIN_EXE_immoments"),
            &["in.image", "--outfile", "out.mom0", "--moments", "0"],
            "out.mom0",
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
        ),
        (
            env!("CARGO_BIN_EXE_impv"),
            &[
                "in.image",
                "--outfile",
                "out.pv",
                "--start",
                "0,0",
                "--end",
                "3,3",
            ],
            "out.pv",
        ),
        (
            env!("CARGO_BIN_EXE_exportfits"),
            &["in.image", "out.fits"],
            "out.fits",
        ),
    ];
    for (binary, args, product) in cases {
        assert_guards_its_product(binary, args, product, ("in.image", "other.image"), path);
    }
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
    );
}
