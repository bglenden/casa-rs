// SPDX-License-Identifier: LGPL-3.0-or-later

//! Both routes into `casars-imager` resolve their parameters through the
//! catalog into one request (IF-7): a clean run from command-line flags and
//! the same run from `--json-run` echo the same resolved parameters and write
//! the same products, to the rounding of separate runs.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

use casa_images::PagedImage;
use serde_json::json;

use super::fixture::{Component, Observation};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

/// Run `casars-imager` with `args` in `directory`.
fn imager(directory: &Path, args: &[&str]) {
    let output = Command::new(env!("CARGO_BIN_EXE_casars-imager"))
        .args(args)
        .current_dir(directory)
        .output()
        .expect("start casars-imager");
    assert!(
        output.status.success(),
        "casars-imager {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The products written under `prefix`, by CASA suffix.
fn products(directory: &Path, prefix: &str) -> BTreeSet<String> {
    std::fs::read_dir(directory)
        .expect("run directory")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.is_dir())
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?.to_string();
            name.strip_prefix(prefix).map(str::to_string)
        })
        .collect()
}

#[test]
fn command_line_and_json_runs_resolve_one_request_and_write_the_same_products() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let directory = observation.scratch("routes");
    std::fs::create_dir(&directory).expect("run directory");
    let measurement_set = observation.measurement_set().to_str().expect("UTF-8 path");
    let geometry = observation.geometry();
    let image_size = geometry.image_size.to_string();
    let cell = geometry.cell_arcsec.to_string();

    imager(
        &directory,
        &[
            "--ms",
            measurement_set,
            "--imagename",
            "cli",
            "--imsize",
            &image_size,
            "--cell-arcsec",
            &cell,
            "--deconvolver",
            "hogbom",
            "--niter",
            "200",
            "--threshold-jy",
            "0.005",
        ],
    );
    let request = json!({
        "kind": "run",
        "request": {
            "vis": measurement_set,
            "imagename": "json",
            "imsize": geometry.image_size,
            "cell": format!("{cell}arcsec"),
            "deconvolver": "hogbom",
            "niter": 200,
            "threshold": "0.005Jy",
        },
    });
    let request_path = directory.join("request.json");
    std::fs::write(&request_path, request.to_string()).expect("request file");
    imager(
        &directory,
        &["--json-run", request_path.to_str().expect("UTF-8 path")],
    );

    // Each run summary echoes the parameters its route resolved.
    let echo = |prefix: &str| {
        let summary: serde_json::Value = serde_json::from_slice(
            &std::fs::read(directory.join(format!("{prefix}.summary.json"))).expect("summary"),
        )
        .expect("summary JSON");
        let mut request = summary["request"].clone();
        request
            .as_object_mut()
            .expect("echoed parameters")
            .remove("imagename");
        request
    };
    assert_eq!(echo("cli"), echo("json"));

    let suffixes = products(&directory, "cli");
    assert_eq!(
        suffixes,
        BTreeSet::from(
            [".image", ".mask", ".model", ".psf", ".residual", ".sumwt"].map(String::from)
        )
    );
    assert_eq!(products(&directory, "json"), suffixes);
    for suffix in &suffixes {
        let open = |prefix: &str| {
            PagedImage::<f32>::open(directory.join(format!("{prefix}{suffix}")))
                .unwrap_or_else(|error| panic!("open {prefix}{suffix}: {error}"))
        };
        let (cli, json) = (open("cli"), open("json"));
        assert_eq!(cli.shape(), json.shape(), "{suffix}");
        assert_eq!(
            format!("{:?}", cli.coordinates()),
            format!("{:?}", json.coordinates()),
            "{suffix} coordinates"
        );
        let (cli, json) = (cli.get().expect("read"), json.get().expect("read"));
        let (error, power) =
            cli.iter()
                .zip(json.iter())
                .fold((0.0_f64, 0.0_f64), |(error, power), (a, b)| {
                    let (a, b) = (f64::from(*a), f64::from(*b));
                    (error + (a - b).powi(2), power + b * b)
                });
        assert!(
            error <= 1.0e-12 * power,
            "{suffix}: NRMS {} between the routes",
            (error / power).sqrt()
        );
    }
}
