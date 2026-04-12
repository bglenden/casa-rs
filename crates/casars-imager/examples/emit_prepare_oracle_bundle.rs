// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::OsString;
use std::path::PathBuf;

use casars_imager::{
    CliConfig, DatasetTier, infer_oracle_dataset_tier,
    write_prepare_plane_oracle_bundle_from_config,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut output_dir = None::<PathBuf>;
    let mut dataset_tier = None::<DatasetTier>;
    let mut imager_args = Vec::<OsString>::new();
    let mut saw_imagename = false;
    let mut saw_imsize = false;
    let mut saw_cell_arcsec = false;

    let mut args = std::env::args_os().skip(1);
    while let Some(argument) = args.next() {
        let text = argument.to_string_lossy();
        match text.as_ref() {
            "--help" | "-h" => return Err(help_text()),
            "--output-dir" => {
                output_dir =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        "missing value for --output-dir".to_string()
                    })?));
            }
            "--dataset-tier" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --dataset-tier".to_string())?;
                dataset_tier = Some(parse_dataset_tier(&value.to_string_lossy())?);
            }
            "--imagename" => {
                saw_imagename = true;
                imager_args.push(argument);
                imager_args.push(
                    args.next()
                        .ok_or_else(|| "missing value for --imagename".to_string())?,
                );
            }
            "--imsize" => {
                saw_imsize = true;
                imager_args.push(argument);
                imager_args.push(
                    args.next()
                        .ok_or_else(|| "missing value for --imsize".to_string())?,
                );
            }
            "--cell-arcsec" => {
                saw_cell_arcsec = true;
                imager_args.push(argument);
                imager_args.push(
                    args.next()
                        .ok_or_else(|| "missing value for --cell-arcsec".to_string())?,
                );
            }
            _ => imager_args.push(argument),
        }
    }

    let output_dir = output_dir.ok_or_else(help_text)?;
    if !saw_imagename {
        imager_args.push(OsString::from("--imagename"));
        imager_args.push(OsString::from("unused"));
    }
    if !saw_imsize {
        imager_args.push(OsString::from("--imsize"));
        imager_args.push(OsString::from("1"));
    }
    if !saw_cell_arcsec {
        imager_args.push(OsString::from("--cell-arcsec"));
        imager_args.push(OsString::from("1.0"));
    }
    let config = CliConfig::parse(imager_args)?;
    let dataset_tier = dataset_tier.unwrap_or_else(|| infer_oracle_dataset_tier(&config.ms));
    let manifest =
        write_prepare_plane_oracle_bundle_from_config(&config, &output_dir, dataset_tier)?;
    println!(
        "Wrote prepare oracle bundle at {} (tier={:?}, artifacts={})",
        output_dir.display(),
        manifest.dataset_tier,
        manifest.artifacts.len()
    );
    Ok(())
}

fn parse_dataset_tier(text: &str) -> Result<DatasetTier, String> {
    match text.to_ascii_lowercase().as_str() {
        "tier-a" | "a" => Ok(DatasetTier::TierA),
        "tier-b" | "b" => Ok(DatasetTier::TierB),
        "tier-c" | "c" => Ok(DatasetTier::TierC),
        _ => Err(format!(
            "unsupported --dataset-tier {text:?}; expected tier-a, tier-b, or tier-c"
        )),
    }
}

fn help_text() -> String {
    "usage: cargo run -p casars-imager --example emit_prepare_oracle_bundle -- --output-dir DIR [--dataset-tier tier-a|tier-b|tier-c] <prepare-seam casars-imager args>\n\nThis emits a frozen-oracle bundle for the current prepare_plane_input() seam. If you do not pass --imagename, --imsize, or --cell-arcsec, the example injects harmless defaults because the bundle path does not use image geometry.".to_string()
}
