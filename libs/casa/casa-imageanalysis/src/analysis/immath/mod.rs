// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-compatible `immath` task implementation (`evalexpr`, `spix`, `pola`,
//! `poli`, `lpoli`, and `tpoli`).

mod evalexpr;
mod pol;
mod selection;

use std::fs;
use std::path::{Path, PathBuf};

use super::expression_workspace::operand_path;
use crate::{ImageMathSummary, ImmathRequest};
use casa_images::add_image_history;
use casa_images::expr_parser::referenced_images;
use casa_images::{ImageError, ImagePixelType};

pub(super) struct ResolvedInputs {
    pub(super) imagename_paths: Vec<PathBuf>,
    pub(super) extra_quoted: Vec<PathBuf>,
}

impl ResolvedInputs {
    fn all_paths(&self) -> Vec<PathBuf> {
        self.imagename_paths
            .iter()
            .chain(self.extra_quoted.iter())
            .cloned()
            .collect()
    }
}

pub(super) fn run(request: &ImmathRequest) -> Result<ImageMathSummary, ImageError> {
    let mode = request.mode.trim().to_ascii_lowercase();
    let outfile = resolve_outfile_path(request)?;
    let varnames = effective_varnames(request);
    let inputs = resolve_inputs(request, &mode, &varnames)?;
    let all_filenames = inputs.all_paths();
    ensure_outfile_does_not_alias_inputs(&outfile, &all_filenames)?;
    if let Some(mask) = request
        .mask
        .as_deref()
        .filter(|mask| !mask.trim().is_empty())
    {
        let mask = selection::quote_var_aliases(mask, &varnames);
        let mask_paths = referenced_images(&mask)
            .map_err(|error| ImageError::CasaSelection(format!("Invalid image mask: {error}")))?
            .into_iter()
            .map(operand_path)
            .filter(|path| path.exists())
            .collect::<Vec<_>>();
        ensure_outfile_does_not_alias_inputs(&outfile, &mask_paths)?;
    }

    let summary = match mode.as_str() {
        "evalexpr" => {
            let prec = parse_prec(&request.prec)?;
            evalexpr::run_evalexpr(
                request,
                &outfile,
                &inputs,
                &varnames,
                &request.expr,
                false,
                prec,
            )?
        }
        "spix" => {
            let prec = parse_prec(&request.prec)?;
            if inputs.imagename_paths.len() != 2 {
                return Err(ImageError::InvalidMetadata(
                    "Requires two images at different frequencies".into(),
                ));
            }
            let spix_expr = format!("spectralindex({}, {})", varnames[0], varnames[1]);
            evalexpr::run_evalexpr(
                request, &outfile, &inputs, &varnames, &spix_expr, true, prec,
            )?
        }
        "pola" => pol::run_pola(request, &outfile, &inputs.imagename_paths, &varnames)?,
        "poli" | "lpoli" | "tpoli" => {
            pol::run_poli(request, &outfile, &inputs.imagename_paths, &varnames, &mode)?
        }
        _ => {
            return Err(ImageError::InvalidMetadata(format!(
                "Unsupported mode {}",
                request.mode
            )));
        }
    };

    write_history(&outfile, request, &mode, &all_filenames)?;
    Ok(summary)
}

fn resolve_outfile_path(request: &ImmathRequest) -> Result<PathBuf, ImageError> {
    let raw = request.outfile.to_string_lossy();
    let outfile = if raw.trim().is_empty() {
        PathBuf::from("immath_results.im")
    } else {
        request.outfile.clone()
    };
    if outfile.exists() && !request.overwrite {
        return Err(ImageError::Io(format!(
            "Output file {} already exists; immath can not proceed, please remove it or change the output file name.",
            outfile.display()
        )));
    }
    Ok(outfile)
}

pub(super) fn remove_existing_outfile_if_overwrite(
    outfile: &Path,
    overwrite: bool,
) -> Result<(), ImageError> {
    if overwrite && outfile.exists() {
        if outfile.is_dir() {
            fs::remove_dir_all(outfile).map_err(|e| ImageError::Io(e.to_string()))?;
        } else {
            fs::remove_file(outfile).map_err(|e| ImageError::Io(e.to_string()))?;
        }
    }
    Ok(())
}

fn ensure_outfile_does_not_alias_inputs(
    outfile: &Path,
    inputs: &[PathBuf],
) -> Result<(), ImageError> {
    let out_canon = fs::canonicalize(outfile).ok();
    for input in inputs {
        let same_lexical = outfile == input.as_path();
        let same_canon = out_canon
            .as_ref()
            .zip(fs::canonicalize(input).ok().as_ref())
            .is_some_and(|(a, b)| a == b);
        if same_lexical || same_canon {
            return Err(ImageError::InvalidMetadata(format!(
                "Output file {} matches input image {} and cannot overwrite it.",
                outfile.display(),
                input.display()
            )));
        }
    }
    Ok(())
}

fn parse_prec(prec: &str) -> Result<ImagePixelType, ImageError> {
    let trimmed = prec.trim().to_ascii_lowercase();
    if !trimmed.is_empty() && "float".starts_with(&trimmed) {
        Ok(ImagePixelType::Float32)
    } else if !trimmed.is_empty() && "double".starts_with(&trimmed) {
        Ok(ImagePixelType::Float64)
    } else {
        Err(ImageError::InvalidMetadata(
            "Unsupported value for type, it must be 'float' or 'double'.".into(),
        ))
    }
}

fn effective_varnames(request: &ImmathRequest) -> Vec<String> {
    let mut names: Vec<String> = request
        .varnames
        .iter()
        .filter(|name| !name.is_empty())
        .cloned()
        .collect();
    let n = request.imagename.len();
    names.truncate(n);
    for idx in names.len()..n {
        names.push(format!("IM{idx}"));
    }
    names
}

fn resolve_inputs(
    request: &ImmathRequest,
    mode: &str,
    varnames: &[String],
) -> Result<ResolvedInputs, ImageError> {
    for path in &request.imagename {
        if !Path::new(path).exists() {
            return Err(ImageError::Io(format!(
                "Image data set not found - please verify {}",
                path.display()
            )));
        }
    }
    let imagename_paths = request.imagename.clone();
    let mut extra_quoted = Vec::new();
    if mode == "evalexpr" {
        let expression = selection::quote_var_aliases(&request.expr, varnames);
        for name in referenced_images(&expression).map_err(|error| {
            ImageError::InvalidMetadata(format!("Invalid image expression: {error}"))
        })? {
            if varnames.contains(&name) {
                continue;
            }
            let p = operand_path(&name);
            if !imagename_paths.contains(&p) && !extra_quoted.contains(&p) {
                if !p.exists() {
                    return Err(ImageError::Io(format!(
                        "Image data set not found - please verify {name}"
                    )));
                }
                extra_quoted.push(p);
            }
        }
    }
    if imagename_paths.is_empty() && extra_quoted.is_empty() {
        return Err(ImageError::Io(
            "Image data set not found - please verify ".into(),
        ));
    }
    Ok(ResolvedInputs {
        imagename_paths,
        extra_quoted,
    })
}

fn write_history(
    outfile: &Path,
    request: &ImmathRequest,
    mode: &str,
    filenames: &[PathBuf],
) -> Result<(), ImageError> {
    let inputs = filenames
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>();
    let lines = [
        concat!("version: casa-rs ", env!("CARGO_PKG_VERSION")).into(),
        format!(
            "immath(imagename={inputs:?}, mode={mode:?}, outfile={:?}, expr={:?}, varnames={:?}, sigma={:?}, polithresh={:?}, mask={:?}, region={:?}, box={:?}, chans={:?}, stokes={:?}, stretch={}, imagemd={:?}, prec={:?}, overwrite={})",
            outfile.display().to_string(),
            request.expr,
            request.varnames,
            request.sigma,
            request.polithresh,
            request.mask,
            request.region,
            request.box_pixels,
            request.chans,
            request.stokes,
            request.stretch,
            request.imagemd,
            request.prec,
            request.overwrite,
        ),
    ];
    add_image_history(outfile, "immath", &lines)
}
