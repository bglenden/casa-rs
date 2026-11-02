// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA outlier-file parsing and image-domain request compilation.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use super::domains::DomainMask;
use super::{OutlierProblem, PrepareError};

/// One outlier image domain as the outlier file defines it.
pub(super) struct OutlierDomain {
    pub(super) name: String,
    pub(super) output: PathBuf,
    /// Pixels along x and y.
    pub(super) image_size: [usize; 2],
    /// Cell along x and y in arcseconds.
    pub(super) cell_arcsec: [f64; 2],
    pub(super) phase_center: String,
    pub(super) mask: DomainMask,
}

pub(super) fn read_outlier_domains(
    path: &Path,
    default_size: [usize; 2],
    default_cell_arcsec: [f64; 2],
) -> Result<Vec<OutlierDomain>, PrepareError> {
    outlier_domains(path, default_size, default_cell_arcsec).map_err(|problem| {
        PrepareError::OutlierFile {
            path: path.to_path_buf(),
            problem,
        }
    })
}

fn outlier_domains(
    path: &Path,
    default_size: [usize; 2],
    default_cell_arcsec: [f64; 2],
) -> Result<Vec<OutlierDomain>, OutlierProblem> {
    let text = fs::read_to_string(path).map_err(OutlierProblem::Read)?;
    let records = parse_records(&text)?;
    if records.is_empty() {
        return Err(OutlierProblem::Empty);
    }
    records
        .into_iter()
        .enumerate()
        .map(|(ordinal, fields)| {
            compile_record(path, ordinal, fields, default_size, default_cell_arcsec)
        })
        .collect()
}

fn parse_records(text: &str) -> Result<Vec<BTreeMap<String, String>>, OutlierProblem> {
    let mut records = Vec::new();
    let mut current = BTreeMap::new();
    for (line_index, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(OutlierProblem::NotAPair {
                line: line_index + 1,
            });
        };
        if value.contains('=') {
            return Err(OutlierProblem::SeveralEquals {
                line: line_index + 1,
            });
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
            return Err(OutlierProblem::RepeatedField {
                line: line_index + 1,
                key,
            });
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
    default_size: [usize; 2],
    default_cell_arcsec: [f64; 2],
) -> Result<OutlierDomain, OutlierProblem> {
    let name = take_required(&mut fields, "imagename", ordinal)?;
    let output = resolve_output(path, &name);
    let image_size = fields
        .remove("imsize")
        .map(|value| parse_size(&value))
        .transpose()?
        .unwrap_or(default_size);
    let cell_arcsec = fields
        .remove("cell")
        .map(|value| parse_cell(&value))
        .transpose()?
        .unwrap_or(default_cell_arcsec);
    let phase_center = take_required(&mut fields, "phasecenter", ordinal)?;
    let mask = fields
        .remove("mask")
        .filter(|value| !value.is_empty())
        .map(|value| circle_mask(&value, image_size))
        .transpose()?
        .unwrap_or(DomainMask::FullPlane);

    admit_default(&mut fields, "usemask", &["", "user"])?;
    admit_default(&mut fields, "specmode", &["", "mfs", "cont"])?;
    admit_default(&mut fields, "nchan", &["", "1"])?;
    admit_default(&mut fields, "nterms", &["", "1"])?;
    admit_default(&mut fields, "gridder", &["", "standard", "gridft", "ft"])?;
    admit_default(&mut fields, "deconvolver", &["", "hogbom"])?;
    admit_default(&mut fields, "wprojplanes", &["", "1"])?;
    // CASA lets these be empty; a value is a feature the slice lacks.
    for unsupported in ["startmodel", "start", "width", "reffreq"] {
        if fields
            .get(unsupported)
            .is_some_and(|value| value.is_empty())
        {
            fields.remove(unsupported);
        }
    }
    if !fields.is_empty() {
        return Err(OutlierProblem::UnsupportedFields {
            ordinal,
            fields: fields.into_keys().collect(),
        });
    }
    Ok(OutlierDomain {
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
    key: &'static str,
    ordinal: usize,
) -> Result<String, OutlierProblem> {
    fields
        .remove(key)
        .filter(|value| !value.is_empty())
        .ok_or(OutlierProblem::MissingField { ordinal, key })
}

fn admit_default(
    fields: &mut BTreeMap<String, String>,
    key: &'static str,
    admitted: &[&str],
) -> Result<(), OutlierProblem> {
    let Some(value) = fields.remove(key) else {
        return Ok(());
    };
    if admitted
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
    {
        Ok(())
    } else {
        Err(OutlierProblem::OutsideSlice { key, value })
    }
}

/// `imsize`: one size for both axes, or x and y.
fn parse_size(text: &str) -> Result<[usize; 2], OutlierProblem> {
    let values = parse_list(text)
        .into_iter()
        .map(|value| value.parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| OutlierProblem::ImageSize)?;
    match values.as_slice() {
        [size] if *size > 0 => Ok([*size; 2]),
        [width, height] if *width > 0 && *height > 0 => Ok([*width, *height]),
        _ => Err(OutlierProblem::ImageSize),
    }
}

/// `cell`: one cell for both axes, or x and y.
fn parse_cell(text: &str) -> Result<[f64; 2], OutlierProblem> {
    let values = parse_list(text)
        .into_iter()
        .map(|value| parse_arcsec(&value))
        .collect::<Result<Vec<_>, _>>()?;
    let valid = |cell: &f64| cell.is_finite() && *cell > 0.0;
    match values.as_slice() {
        [cell] if valid(cell) => Ok([*cell; 2]),
        [x, y] if valid(x) && valid(y) => Ok([*x, *y]),
        _ => Err(OutlierProblem::Cell),
    }
}

fn parse_arcsec(text: &str) -> Result<f64, OutlierProblem> {
    let lower = trim_string(text).to_ascii_lowercase();
    lower
        .strip_suffix("arcsec")
        .unwrap_or(&lower)
        .trim()
        .parse::<f64>()
        .map_err(|_| OutlierProblem::CellValue {
            text: text.to_string(),
        })
}

/// A `circle[[xpix,ypix],rpix]` mask over `image_size` (x, y) pixels,
/// x-major as reconstruction masks are.
fn circle_mask(text: &str, image_size: [usize; 2]) -> Result<DomainMask, OutlierProblem> {
    let compact = trim_string(text)
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let (centre, radius) = compact
        .strip_prefix("circle[[")
        .and_then(|value| value.strip_suffix(']'))
        .and_then(|body| body.split_once("],"))
        .ok_or(OutlierProblem::MaskSyntax)?;
    let (x, y) = centre.split_once(',').ok_or(OutlierProblem::MaskSyntax)?;
    let centre = [parse_pixels(x)?, parse_pixels(y)?];
    let radius = parse_pixels(radius)?;
    if radius < 0.0
        || centre
            .iter()
            .zip(image_size)
            .any(|(value, side)| *value < 0.0 || *value >= side as f64)
    {
        return Err(OutlierProblem::MaskOutside);
    }
    let radius_squared = radius * radius;
    let [nx, ny] = image_size;
    let support_len = nx.checked_mul(ny).ok_or(OutlierProblem::ImageSize)?;
    let mut support = Vec::with_capacity(support_len);
    for x in 0..nx {
        for y in 0..ny {
            let dx = x as f64 - centre[0];
            let dy = y as f64 - centre[1];
            support.push(dx * dx + dy * dy <= radius_squared);
        }
    }
    Ok(DomainMask::PixelSupport(support.into_boxed_slice()))
}

fn parse_pixels(text: &str) -> Result<f64, OutlierProblem> {
    text.strip_suffix("pix")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .ok_or(OutlierProblem::MaskSyntax)
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
        let domains = read_outlier_domains(&path, [100; 2], [4.0; 2]).expect("compile outlier");
        assert_eq!(domains.len(), 1);
        assert_eq!(domains[0].image_size, [80, 80]);
        assert_eq!(domains[0].cell_arcsec, [8.0, 8.0]);
        assert_eq!(domains[0].output, directory.path().join("outlier"));
        let DomainMask::PixelSupport(support) = &domains[0].mask else {
            panic!("expected pixel support");
        };
        assert!(support[40 * 80 + 40]);
        assert!(!support[0]);
    }

    #[test]
    fn a_non_square_outlier_keeps_each_axis_and_masks_x_major() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("outlier.txt");
        std::fs::write(
            &path,
            "imagename='outlier'\nimsize=[60,40]\ncell=['8arcsec','6arcsec']\nphasecenter='J2000 19:58:40.895 +40.55.58.543'\nmask='circle[[50pix,10pix],3pix]'\n",
        )
        .expect("write outlier fixture");
        let domains = read_outlier_domains(&path, [100; 2], [4.0; 2]).expect("compile outlier");
        assert_eq!(domains[0].image_size, [60, 40]);
        assert_eq!(domains[0].cell_arcsec, [8.0, 6.0]);
        let DomainMask::PixelSupport(support) = &domains[0].mask else {
            panic!("expected pixel support");
        };
        assert_eq!(support.len(), 60 * 40);
        // Pixel (x, y) is at x · ny + y; y-major indexing would put the
        // circle's centre at y · nx + x.
        assert!(support[50 * 40 + 10]);
        assert!(!support[10 * 60 + 50]);
        assert_eq!(support.iter().filter(|set| **set).count(), 29);
    }

    #[test]
    fn a_scalar_outlier_size_and_cell_size_both_axes() {
        assert_eq!(parse_size("64").expect("size"), [64, 64]);
        assert_eq!(parse_cell("'2arcsec'").expect("cell"), [2.0, 2.0]);
        assert!(matches!(
            parse_size("[64,0]"),
            Err(OutlierProblem::ImageSize)
        ));
        assert!(matches!(
            parse_cell("['2arcsec','0arcsec']"),
            Err(OutlierProblem::Cell)
        ));
    }

    #[test]
    fn a_line_without_a_parameter_names_its_line() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("outlier.txt");
        std::fs::write(&path, "imagename='outlier'\n# a comment\nphasecenter\n")
            .expect("write outlier fixture");
        let Err(error) = read_outlier_domains(&path, [100; 2], [4.0; 2]) else {
            panic!("a line without '=' is refused");
        };
        assert!(
            matches!(
                error,
                PrepareError::OutlierFile {
                    problem: OutlierProblem::NotAPair { line: 3 },
                    ..
                }
            ),
            "{error}"
        );
    }
}
