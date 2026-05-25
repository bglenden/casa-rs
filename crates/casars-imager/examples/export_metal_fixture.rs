// SPDX-License-Identifier: LGPL-3.0-or-later

use std::env;
use std::ffi::OsString;
use std::path::PathBuf;

use casars_imager::{
    CliConfig, StandardMfsMetalFixtureExportOptions, export_standard_mfs_metal_fixture_from_config,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let (options, config_args) = parse_args(env::args_os().skip(1))?;
    let config = CliConfig::parse(config_args)?;
    export_standard_mfs_metal_fixture_from_config(&config, &options)?;
    Ok(())
}

fn parse_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<(StandardMfsMetalFixtureExportOptions, Vec<OsString>), String> {
    let mut output = None::<PathBuf>;
    let mut metadata = None::<PathBuf>;
    let mut max_samples = None::<usize>;
    let mut sample_stride = 1usize;
    let mut config_args = Vec::<OsString>::new();
    let mut args = args.into_iter().peekable();
    while let Some(argument) = args.next() {
        let arg = argument.to_string_lossy();
        match arg.as_ref() {
            "--output" => output = Some(next_path(&mut args, "--output")?),
            "--metadata" => metadata = Some(next_path(&mut args, "--metadata")?),
            "--max-samples" => {
                max_samples = Some(next_usize(&mut args, "--max-samples")?);
            }
            "--sample-stride" => sample_stride = next_usize(&mut args, "--sample-stride")?,
            "--" => {
                config_args.extend(args);
                break;
            }
            "--help" | "-h" => return Err(usage()),
            _ => {
                return Err(format!(
                    "unknown export argument {arg}; pass imager arguments after --\n{}",
                    usage()
                ));
            }
        }
    }
    let output = output.ok_or_else(|| format!("missing --output\n{}", usage()))?;
    if sample_stride == 0 {
        return Err("--sample-stride must be greater than zero".to_string());
    }
    Ok((
        StandardMfsMetalFixtureExportOptions {
            output,
            metadata,
            max_samples,
            sample_stride,
        },
        config_args,
    ))
}

fn next_path(
    args: &mut std::iter::Peekable<impl Iterator<Item = OsString>>,
    flag: &str,
) -> Result<PathBuf, String> {
    args.next()
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing value after {flag}"))
}

fn next_usize(
    args: &mut std::iter::Peekable<impl Iterator<Item = OsString>>,
    flag: &str,
) -> Result<usize, String> {
    let value = args
        .next()
        .ok_or_else(|| format!("missing value after {flag}"))?;
    value
        .to_string_lossy()
        .parse::<usize>()
        .map_err(|error| format!("parse {flag}: {error}"))
}

fn usage() -> String {
    "Usage: cargo run -p casars-imager --example export_metal_fixture -- \
     --output samples.bin [--metadata samples.json] [--max-samples N] \
     [--sample-stride N] -- <casars-imager args>"
        .to_string()
}
