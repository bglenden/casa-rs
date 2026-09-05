// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA outlier-file parsing and image-domain request compilation.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use crate::{ApplicationError, ContinuumMask};

pub(crate) struct ContinuumDomainInput {
    pub(crate) name: String,
    pub(crate) output: PathBuf,
    pub(crate) image_size: usize,
    pub(crate) cell_arcsec: f64,
    pub(crate) phase_center: String,
    pub(crate) mask: ContinuumMask,
}

pub(crate) fn read_outlier_domains(
    path: &Path,
    default_size: usize,
    default_cell_arcsec: f64,
) -> Result<Vec<ContinuumDomainInput>, ApplicationError> {
    let text = fs::read_to_string(path).map_err(|error| {
        boxed(format!(
            "cannot read outlier file {}: {error}",
            path.display()
        ))
    })?;
    let records = parse_records(path, &text)?;
    if records.is_empty() {
        return Err(boxed("outlier file did not define any image domains"));
    }
    records
        .into_iter()
        .enumerate()
        .map(|(ordinal, fields)| {
            compile_record(path, ordinal, fields, default_size, default_cell_arcsec)
        })
        .collect()
}

fn parse_records(
    path: &Path,
    text: &str,
) -> Result<Vec<BTreeMap<String, String>>, ApplicationError> {
    let mut records = Vec::new();
    let mut current = BTreeMap::new();
    for (line_index, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(boxed(format!(
                "outlier file {} line {} must contain one parameter=value pair",
                path.display(),
                line_index + 1
            )));
        };
        if value.contains('=') {
            return Err(boxed(format!(
                "outlier file {} line {} contains more than one '='",
                path.display(),
                line_index + 1
            )));
        }
        let key = key.trim().to_ascii_lowercase();
        if key == "imagename" && !current.is_empty() {
            records.push(current);
            current = BTreeMap::new();
        }
        if current
            .insert(key.clone(), trim_string(value).to_owned())
            .is_some()
        {
            return Err(boxed(format!(
                "outlier file {} line {} repeats field {key:?}",
                path.display(),
                line_index + 1
            )));
        }
    }
    if !current.is_empty() {
        records.push(current);
    }
    Ok(records)
}

fn compile_record(
    path: &Path,
    ordinal: usize,
    mut fields: BTreeMap<String, String>,
    default_size: usize,
    default_cell_arcsec: f64,
) -> Result<ContinuumDomainInput, ApplicationError> {
    let name = take_required(&mut fields, "imagename", ordinal)?;
    let output = resolve_output(path, &name);
    let image_size = fields
        .remove("imsize")
        .map(|value| parse_square_size(&value))
        .transpose()?
        .unwrap_or(default_size);
    let cell_arcsec = fields
        .remove("cell")
        .map(|value| parse_square_cell(&value))
        .transpose()?
        .unwrap_or(default_cell_arcsec);
    let phase_center = take_required(&mut fields, "phasecenter", ordinal)?;
    let mask = fields
        .remove("mask")
        .filter(|value| !value.is_empty())
        .map(|value| circle_mask(&value, image_size))
        .transpose()?
        .unwrap_or(ContinuumMask::FullPlane);

    admit_default(&mut fields, "usemask", &["", "user"])?;
    admit_default(&mut fields, "specmode", &["", "mfs", "cont"])?;
    admit_default(&mut fields, "nchan", &["", "1"])?;
    admit_default(&mut fields, "nterms", &["", "1"])?;
    admit_default(&mut fields, "gridder", &["", "standard", "gridft", "ft"])?;
    admit_default(&mut fields, "deconvolver", &["", "hogbom"])?;
    admit_default(&mut fields, "wprojplanes", &["", "1"])?;
    for unsupported in ["startmodel", "start", "width", "reffreq"] {
        if fields
            .remove(unsupported)
            .is_some_and(|value| !value.is_empty())
        {
            return Err(boxed(format!(
                "outlier image {ordinal} sets unsupported field {unsupported:?}"
            )));
        }
    }
    if !fields.is_empty() {
        return Err(boxed(format!(
            "outlier image {ordinal} contains unsupported field(s): {}",
            fields.keys().cloned().collect::<Vec<_>>().join(", ")
        )));
    }
    Ok(ContinuumDomainInput {
        name,
        output,
        image_size,
        cell_arcsec,
        phase_center,
        mask,
    })
}

fn take_required(
    fields: &mut BTreeMap<String, String>,
    key: &str,
    ordinal: usize,
) -> Result<String, ApplicationError> {
    fields
        .remove(key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| boxed(format!("outlier image {ordinal} is missing required {key}")))
}

fn admit_default(
    fields: &mut BTreeMap<String, String>,
    key: &str,
    admitted: &[&str],
) -> Result<(), ApplicationError> {
    let Some(value) = fields.remove(key) else {
        return Ok(());
    };
    if admitted
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
    {
        Ok(())
    } else {
        Err(boxed(format!(
            "outlier field {key}={value:?} is outside the installed multi-domain slice"
        )))
    }
}

fn parse_square_size(text: &str) -> Result<usize, ApplicationError> {
    let values = parse_list(text)
        .into_iter()
        .map(|value| value.parse::<usize>())
        .collect::<Result<Vec<_>, _>>()?;
    match values.as_slice() {
        [size] if *size > 0 => Ok(*size),
        [width, height] if *width > 0 && width == height => Ok(*width),
        _ => Err(boxed(
            "outlier imsize must be a positive square scalar or pair",
        )),
    }
}

fn parse_square_cell(text: &str) -> Result<f64, ApplicationError> {
    let values = parse_list(text)
        .into_iter()
        .map(|value| parse_arcsec(&value))
        .collect::<Result<Vec<_>, _>>()?;
    match values.as_slice() {
        [cell] if cell.is_finite() && *cell > 0.0 => Ok(*cell),
        [x, y] if x.is_finite() && *x > 0.0 && (*x - *y).abs() <= f64::EPSILON => Ok(*x),
        _ => Err(boxed(
            "outlier cell must be one positive arcsec value or an equal pair",
        )),
    }
}

fn parse_arcsec(text: &str) -> Result<f64, ApplicationError> {
    let lower = trim_string(text).to_ascii_lowercase();
    lower
        .strip_suffix("arcsec")
        .unwrap_or(&lower)
        .trim()
        .parse::<f64>()
        .map_err(|error| boxed(format!("invalid outlier cell {text:?}: {error}")))
}

fn circle_mask(text: &str, image_size: usize) -> Result<ContinuumMask, ApplicationError> {
    let compact = trim_string(text)
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let body = compact
        .strip_prefix("circle[[")
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| boxed("outlier mask must use circle[[xpix,ypix],rpix]"))?;
    let (centre, radius) = body
        .split_once("],")
        .ok_or_else(|| boxed("outlier mask must use circle[[xpix,ypix],rpix]"))?;
    let (x, y) = centre
        .split_once(',')
        .ok_or_else(|| boxed("outlier mask must use circle[[xpix,ypix],rpix]"))?;
    let centre = [parse_pixels(x)?, parse_pixels(y)?];
    let radius = parse_pixels(radius)?;
    if radius < 0.0
        || centre
            .iter()
            .any(|value| *value < 0.0 || *value >= image_size as f64)
    {
        return Err(boxed("outlier circle mask exceeds its image domain"));
    }
    let radius_squared = radius * radius;
    let support_len = image_size
        .checked_mul(image_size)
        .ok_or_else(|| boxed("outlier mask support size overflowed"))?;
    let mut support = Vec::with_capacity(support_len);
    for x in 0..image_size {
        for y in 0..image_size {
            let dx = x as f64 - centre[0];
            let dy = y as f64 - centre[1];
            support.push(dx * dx + dy * dy <= radius_squared);
        }
    }
    Ok(ContinuumMask::PixelSupport(support.into_boxed_slice()))
}

fn parse_pixels(text: &str) -> Result<f64, ApplicationError> {
    let value = text
        .strip_suffix("pix")
        .ok_or_else(|| boxed("outlier circle coordinates must use pix units"))?
        .parse::<f64>()?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(boxed("outlier circle coordinates must be finite"))
    }
}

fn parse_list(text: &str) -> Vec<String> {
    text.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(trim_string)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn trim_string(value: &str) -> &str {
    value
        .trim()
        .trim_matches(',')
        .trim()
        .trim_matches(|character| character == '"' || character == '\'')
        .trim()
}

fn resolve_output(outlier_file: &Path, name: &str) -> PathBuf {
    let output = PathBuf::from(name);
    if output.is_absolute() {
        output
    } else {
        outlier_file
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map_or(output.clone(), |parent| parent.join(output))
    }
}

fn boxed(message: impl Into<String>) -> ApplicationError {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_casa_multifield_fixture_shape_and_circle_support() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("outlier.txt");
        std::fs::write(
            &path,
            "imagename='outlier'\nimsize=[80,80]\ncell=['8arcsec','8arcsec']\nphasecenter='J2000 19:58:40.895 +40.55.58.543'\nmask='circle[[40pix,40pix],10pix]'\n",
        )
        .expect("write outlier fixture");
        let domains = read_outlier_domains(&path, 100, 4.0).expect("compile outlier");
        assert_eq!(domains.len(), 1);
        assert_eq!(domains[0].image_size, 80);
        assert_eq!(domains[0].cell_arcsec, 8.0);
        assert_eq!(domains[0].output, directory.path().join("outlier"));
        let ContinuumMask::PixelSupport(support) = &domains[0].mask else {
            panic!("expected pixel support");
        };
        assert!(support[40 * 80 + 40]);
        assert!(!support[0]);
    }
}
