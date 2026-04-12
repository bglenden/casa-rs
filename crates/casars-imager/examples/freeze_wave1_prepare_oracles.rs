// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use casars_imager::{
    CliConfig, DatasetTier, OracleBundleOverrides, infer_oracle_dataset_tier, sha256_hex_path,
    write_json_pretty, write_prepare_plane_oracle_bundle_from_config_with_overrides,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
struct FrozenWave1BundleIndex {
    schema_version: u32,
    casa_python: String,
    casa_version: String,
    casacore_version: String,
    bundles: Vec<FrozenWave1BundleEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
struct FrozenWave1BundleEntry {
    label: String,
    dataset_path: String,
    dataset_tier: DatasetTier,
    manifest_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CasaRuntimeInfo {
    program: PathBuf,
    casa_version: String,
    casacore_version: String,
}

#[derive(Debug, Clone, Copy)]
struct Wave1BundleSpec {
    label: &'static str,
    dataset_rel: &'static str,
    extra_args: &'static [&'static str],
}

const WAVE1_PREPARE_BUNDLES: &[Wave1BundleSpec] = &[
    Wave1BundleSpec {
        label: "ngc5921_mfs_prepare",
        dataset_rel: "measurementset/vla/ngc5921.ms",
        extra_args: &["--field", "0", "--phasecenter-field", "0"],
    },
    Wave1BundleSpec {
        label: "refim_point_withline_cube_prepare",
        dataset_rel: "measurementset/vla/refim_point_withline.ms",
        extra_args: &[
            "--field",
            "0",
            "--phasecenter-field",
            "0",
            "--specmode",
            "cube",
            "--channel-start",
            "0",
            "--channel-count",
            "8",
        ],
    },
    Wave1BundleSpec {
        label: "refim_alma_mosaic_mfs_prepare",
        dataset_rel: "measurementset/alma/refim_alma_mosaic.ms",
        extra_args: &[
            "--field",
            "0,1,2",
            "--phasecenter-field",
            "1",
            "--corr",
            "XX",
        ],
    },
    Wave1BundleSpec {
        label: "refim_point_linxy_prepare",
        dataset_rel: "measurementset/vla/refim_point_linXY.ms",
        extra_args: &["--field", "0", "--phasecenter-field", "0", "--corr", "XX"],
    },
];

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let (output_dir, requested_bundle) = parse_args()?;
    if output_dir.exists() {
        std::fs::remove_dir_all(&output_dir)
            .map_err(|error| format!("remove existing output {}: {error}", output_dir.display()))?;
    }
    std::fs::create_dir_all(&output_dir)
        .map_err(|error| format!("create output {}: {error}", output_dir.display()))?;

    let casa = discover_casa_runtime()?;
    let mut index = FrozenWave1BundleIndex {
        schema_version: 1,
        casa_python: casa.program.display().to_string(),
        casa_version: casa.casa_version.clone(),
        casacore_version: casa.casacore_version.clone(),
        bundles: Vec::new(),
    };

    for spec in WAVE1_PREPARE_BUNDLES.iter().copied() {
        if requested_bundle
            .as_deref()
            .is_some_and(|requested| requested != spec.label)
        {
            continue;
        }
        let dataset_path = resolve_dataset_path(spec.dataset_rel)?;
        let bundle_dir = output_dir.join(spec.label).join("rust");
        std::fs::create_dir_all(
            bundle_dir
                .parent()
                .expect("bundle dir always has a parent directory"),
        )
        .map_err(|error| format!("create bundle parent {}: {error}", bundle_dir.display()))?;

        let dataset_sha256 = sha256_hex_path(&dataset_path)?;
        let config = parse_spec_config(&dataset_path, spec.extra_args)?;
        let dataset_tier = infer_oracle_dataset_tier(&dataset_path);
        let manifest = write_prepare_plane_oracle_bundle_from_config_with_overrides(
            &config,
            &bundle_dir,
            dataset_tier,
            &OracleBundleOverrides {
                dataset_path: Some(dataset_path.display().to_string()),
                dataset_identity: Some(format!("sha256:{dataset_sha256}")),
                dataset_sha256: Some(dataset_sha256),
                casa_version: Some(casa.casa_version.clone()),
                casacore_version: Some(casa.casacore_version.clone()),
            },
        )?;
        index.bundles.push(FrozenWave1BundleEntry {
            label: spec.label.to_string(),
            dataset_path: dataset_path.display().to_string(),
            dataset_tier: manifest.dataset_tier,
            manifest_path: bundle_dir
                .join("bundle_manifest.json")
                .display()
                .to_string(),
        });
    }

    if let Some(label) = requested_bundle
        && !index.bundles.iter().any(|entry| entry.label == label)
    {
        return Err(format!(
            "unknown --bundle {label:?}; expected one of {}",
            WAVE1_PREPARE_BUNDLES
                .iter()
                .map(|spec| spec.label)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    write_json_pretty(&index, &output_dir.join("wave1_index.json"))?;
    println!(
        "Wrote {} Wave 1 frozen prepare bundles under {}",
        index.bundles.len(),
        output_dir.display()
    );
    Ok(())
}

fn parse_args() -> Result<(PathBuf, Option<String>), String> {
    let mut output_dir = None::<PathBuf>;
    let mut requested_bundle = None::<String>;
    let mut args = std::env::args_os().skip(1);
    while let Some(argument) = args.next() {
        match argument.to_string_lossy().as_ref() {
            "--help" | "-h" => return Err(help_text()),
            "--output-dir" => {
                output_dir =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        "missing value for --output-dir".to_string()
                    })?));
            }
            "--bundle" => {
                requested_bundle = Some(
                    args.next()
                        .ok_or_else(|| "missing value for --bundle".to_string())?
                        .to_string_lossy()
                        .to_string(),
                );
            }
            other => {
                return Err(format!("unsupported argument {other:?}\n\n{}", help_text()));
            }
        }
    }
    let output_dir = output_dir.ok_or_else(help_text)?;
    Ok((output_dir, requested_bundle))
}

fn help_text() -> String {
    "usage: cargo run -p casars-imager --example freeze_wave1_prepare_oracles -- --output-dir DIR [--bundle LABEL]\n\nFreeze the Wave 1 prepare-seam oracle bundles for the required source-backed datasets.".to_string()
}

fn parse_spec_config(ms_path: &Path, extra_args: &[&str]) -> Result<CliConfig, String> {
    let mut args = vec![
        OsString::from("--ms"),
        ms_path.as_os_str().to_os_string(),
        OsString::from("--imagename"),
        OsString::from("unused"),
        OsString::from("--imsize"),
        OsString::from("1"),
        OsString::from("--cell-arcsec"),
        OsString::from("1.0"),
    ];
    args.extend(extra_args.iter().map(OsString::from));
    CliConfig::parse(args)
}

fn resolve_dataset_path(relative: &str) -> Result<PathBuf, String> {
    dataset_root_candidates()
        .into_iter()
        .map(|root| root.join(relative))
        .find(|candidate| candidate.exists())
        .map(|candidate| candidate.canonicalize().unwrap_or(candidate))
        .ok_or_else(|| format!("resolve dataset path for {relative}"))
}

fn dataset_root_candidates() -> Vec<PathBuf> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root");
    let mut candidates = Vec::new();
    if let Some(root) = std::env::var_os("CASA_RS_TESTDATA_ROOT") {
        candidates.push(PathBuf::from(root));
    }
    candidates.push(repo_root.join("../casatestdata"));
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join("SoftwareProjects")
                .join("casatestdata"),
        );
    }
    candidates
}

fn discover_casa_runtime() -> Result<CasaRuntimeInfo, String> {
    for candidate in casa_python_candidates() {
        if let Ok(runtime) = probe_casa_runtime(&candidate) {
            return Ok(runtime);
        }
    }
    Err("discover CASA Python runtime".to_string())
}

fn casa_python_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for key in ["CASA_RS_CASA_PYTHON", "CASA_PYTHON"] {
        if let Some(value) = std::env::var_os(key) {
            candidates.push(PathBuf::from(value));
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join("SoftwareProjects")
                .join("casa-build")
                .join("venv")
                .join("bin")
                .join("python"),
        );
    }
    candidates.push(PathBuf::from("python3"));
    candidates.push(PathBuf::from("python"));
    candidates
}

fn probe_casa_runtime(program: &Path) -> Result<CasaRuntimeInfo, String> {
    let script = r#"
import json
import casatasks
import casatools

def resolve_version(value):
    if value is None:
        return None
    if callable(value):
        value = value()
    return str(value)

payload = {
    "casa_version": resolve_version(getattr(casatasks, "version", None)),
    "casacore_version": resolve_version(
        getattr(casatools, "version_string", None)
    ) or resolve_version(getattr(casatools, "version", None)),
}
print(json.dumps(payload))
"#;
    let output = Command::new(program)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn CASA Python {}: {error}", program.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "CASA Python {} failed: {stderr}",
            program.display()
        ));
    }
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|error| {
        format!(
            "parse CASA runtime JSON from {}: {error}",
            program.display()
        )
    })?;
    Ok(CasaRuntimeInfo {
        program: program.to_path_buf(),
        casa_version: payload
            .get("casa_version")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string(),
        casacore_version: payload
            .get("casacore_version")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string(),
    })
}
