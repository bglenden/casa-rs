// SPDX-License-Identifier: LGPL-3.0-or-later
//! The pointings: laid out by `calc_pointings2` or read from `ptgfile`,
//! written to `<stem>.ptg.txt`, and checked against the model's extent.

use std::path::{Path, PathBuf};

use casa_types::quanta::Quantity;

use super::antennas::Array;
use super::sky::Sky;
use super::{Run, SimobserveError, entry_exists, make_way, python, remove};
use crate::simutil::direction::{Directions, is_direction, median_direction, split_direction};
use crate::simutil::pointings::{PBCOEFF, calc_pointings, read_pointings, write_pointings};
use crate::simutil::{SimutilError, qa};

/// `relmargin`: the primary beams between the model's edge and a pointing
/// that still overlaps it, and `calc_pointings2`'s margin.
const RELATIVE_MARGIN: f64 = 0.5;

/// The pointings the task observes.
pub(super) struct Pointings {
    /// The pointings, as `simutil` writes directions.
    pub(super) pointings: Vec<String>,
    /// `etime`: each pointing's scan, s; a pointing file may give more
    /// times than pointings.
    pub(super) times_s: Vec<f64>,
    /// `integration`, as `settimes` reads it: `"%fs"` of the integration
    /// time, or the shortest time in the pointing file.
    pub(super) integration: String,
}

/// The pointings, as CASA lays them out or reads them, with the pointing
/// file written and the overlap with the model checked.
pub(super) fn prepare(
    run: &mut Run<'_>,
    sky: &Sky,
    array: &Array,
) -> Result<Pointings, SimobserveError> {
    let parameters = run.parameters;
    let model = &sky.model;
    // A list whose first is a direction, or a direction string; the
    // model's centre otherwise.
    let (directions, centre) = match &parameters.direction {
        Directions::List(list) if list.first().is_some_and(|first| is_direction(first)) => {
            (parameters.direction.clone(), list[0].clone())
        }
        Directions::One(one) if is_direction(one) => (parameters.direction.clone(), one.clone()),
        _ => (Directions::One(model.refdir.clone()), model.refdir.clone()),
    };
    let (integration, integration_s) = integration(&parameters.integration)?;
    let beam = array.primary_beam_arcsec;
    let (ptgfile, pointings) = if parameters.setpointings {
        tracing::info!("calculating map pointings centered at {centre}");
        let spacing = spacing(&parameters.pointingspacing, run.uvmode, beam)?;
        if parameters.verbose {
            tracing::info!("pointing spacing in mosaic = {spacing}");
        }
        let size = map_size(&parameters.mapsize, &model.size)?;
        // `calc_pointings2` returns a list of several as it is, once each
        // splits.
        let listed = calc_pointings(
            &qa::read(&spacing)?,
            [&size[0], &size[1]],
            &parameters.maptype,
            &directions,
            RELATIVE_MARGIN,
            beam,
        )?;
        if parameters.verbose && matches!(&directions, Directions::List(list) if list.len() > 1) {
            tracing::warn!(
                "you are inputing the precise pointings in 'direction' - if you want to \
                 calculate a mosaic, give a single direction string and set maptype"
            );
        }
        let scan_s = qa::value_in(&qa::read(&integration)?, "s")?;
        let times_s = vec![scan_s; listed.len()];
        let pointings = Pointings {
            pointings: listed,
            times_s,
            integration,
        };
        (run.file(".ptg.txt"), pointings)
    } else {
        read_file(run, integration, integration_s)?
    };
    let overlap = overlaps(&pointings.pointings, sky, beam)?;
    if parameters.setpointings {
        if entry_exists(&ptgfile) {
            if !parameters.overwrite {
                return Err(SimobserveError::raised(format!(
                    "pointing file {} already exists and user does not want to overwrite",
                    ptgfile.display()
                )));
            }
            remove(&ptgfile)?;
        }
        write_pointings(&ptgfile, &pointings.pointings, &pointings.times_s)?;
    }
    if ptgfile == run.file(".ptg.txt") {
        run.wrote(&ptgfile);
    }
    if parameters.verbose && pointings.pointings.len() > 1 {
        for pointing in pointings.pointings.iter().take(20) {
            tracing::info!("   {pointing}");
        }
        if pointings.pointings.len() >= 20 {
            tracing::info!("   (printing only first 20 - see pointing file for full list)");
        }
    }
    if !overlap {
        return Err(SimobserveError::raised(
            "No overlap between model and pointings",
        ));
    }
    Ok(pointings)
}

/// `integration` as CASA reads it: a time, or a number of seconds; then
/// written `"%fs"`, with its value in seconds.
fn integration(text: &str) -> Result<(String, f64), SimobserveError> {
    if !qa::is_quantity(text) {
        return Err(SimutilError::Quantity(text.to_string()).into());
    }
    let quantity = qa::read(text)?;
    let seconds = if quantity.unit().name().is_empty() {
        let seconds = text.trim().parse().unwrap_or(quantity.value());
        tracing::warn!(
            "interpreting integration time parameter as {}s",
            python(seconds)
        );
        seconds
    } else {
        if !qa::conforms(&quantity, "s") {
            return Err(SimobserveError::raised(format!(
                "integration time {text} does not appear to represent a time interval ('s','min'; \
                 not 'sec','m')"
            )));
        }
        qa::value_in(&quantity, "s")?
    };
    Ok((format!("{seconds:.6}s"), seconds))
}

/// `pointingspacing` as an angle: λ/D/√3 (an interferometer) or λ/D/3 (a
/// single dish) in primary beams when empty, half a primary beam for
/// `nyquist`, and `<n>PB` times the primary beam `beam_arcsec`.
fn spacing(given: &str, uvmode: bool, beam_arcsec: f64) -> Result<String, SimobserveError> {
    let mut spacing = given.to_string();
    if spacing.is_empty() {
        let ratio = if uvmode { 1. / 3_f64.sqrt() } else { 1. / 3. };
        spacing = format!("{:.6}PB", ratio / PBCOEFF);
    }
    if spacing.to_uppercase() == "NYQUIST" {
        spacing = format!("{:.6}PB", 0.5 / PBCOEFF);
    }
    if let Some(number) = primary_beams(&spacing.to_uppercase()) {
        if beam_arcsec <= 0. {
            return Err(SimobserveError::raised(
                "Can't calculate pointingspacing in terms of primary beam because antennalist \
                 is not specified",
            ));
        }
        let beams: f64 = number.parse().map_err(|_| {
            SimobserveError::raised(format!("could not convert string to float: '{number}'"))
        })?;
        spacing = format!("{:.6}arcsec", beams * beam_arcsec);
    }
    Ok(spacing)
}

/// The group Python's `re.match('(\d+.?\d+)\s*PB', upper)` captures: two
/// digit runs, perhaps with one character between them, then `PB` after
/// any white space. The first run is greedy and the character tried before
/// none, as Python backtracks.
fn primary_beams(upper: &str) -> Option<String> {
    let chars: Vec<char> = upper.chars().collect();
    let digits_from = |start: usize| {
        chars[start.min(chars.len())..]
            .iter()
            .take_while(|c| c.is_ascii_digit())
            .count()
    };
    for first in (1..=digits_from(0)).rev() {
        for between in [1, 0] {
            if between == 1 && chars.get(first).is_none_or(|&c| c == '\n') {
                continue;
            }
            let second = first + between;
            let run = digits_from(second);
            if run == 0 {
                continue;
            }
            let end = second + run;
            let blank = chars[end..]
                .iter()
                .take_while(|c| c.is_whitespace())
                .count();
            if chars[end + blank..].starts_with(&['P', 'B']) {
                return Some(chars[..end].iter().collect());
            }
        }
    }
    None
}

/// `mapsize`, as `calc_pointings2` takes it: the model's size when unset,
/// one value for both axes, and every value a quantity.
fn map_size(mapsize: &[String], model: &[Quantity; 2]) -> Result<[Quantity; 2], SimobserveError> {
    match mapsize.first() {
        None => Ok(model.clone()),
        Some(first) if first.is_empty() => Ok(model.clone()),
        Some(first) => {
            if let Some(bad) = mapsize.iter().find(|size| !qa::is_quantity(size)) {
                return Err(SimutilError::Quantity(bad.clone()).into());
            }
            let x = qa::read(first)?;
            let y = mapsize.get(1).map_or(Ok(x.clone()), |y| qa::read(y))?;
            Ok([x, y])
        }
    }
}

/// The pointings of `ptgfile` (with `$project` the stem), copied into the
/// project as `<stem>.ptg.txt`, or read there; their times, or the
/// integration time when none is positive; and the integration shortened to
/// the shortest time. The copy replaces a link at `<stem>.ptg.txt`, not the
/// file it points to, unless that is `ptgfile` itself, which is read.
fn read_file(
    run: &Run<'_>,
    mut integration: String,
    integration_s: f64,
) -> Result<(PathBuf, Pointings), SimobserveError> {
    let name = run.parameters.ptgfile.replace("$project", &run.stem);
    let project_file = run.file(".ptg.txt");
    let path = if !name.is_empty() && Path::new(&name).exists() {
        if !same_file(Path::new(&name), &project_file) {
            make_way(&project_file)?;
            std::fs::copy(&name, &project_file).map_err(SimobserveError::io(Path::new(&name)))?;
        }
        project_file
    } else {
        let in_project = PathBuf::from(format!("{}/{name}", run.parameters.project));
        if !in_project.exists() {
            return Err(SimobserveError::raised(format!(
                "Can't find pointing file {name}"
            )));
        }
        in_project
    };
    let file = read_pointings(&path)?;
    let mut times_s = file.times;
    let longest = times_s.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if longest <= 0. {
        times_s = vec![integration_s; file.pointings.len()];
    }
    let shortest = times_s.iter().copied().fold(f64::INFINITY, f64::min);
    if shortest < integration_s {
        integration = format!("{}s", python(shortest));
        tracing::warn!(
            "Setting integration to {integration} to match the shortest time in the pointing file."
        );
    }
    let pointings = Pointings {
        pointings: file.pointings,
        times_s,
        integration,
    };
    Ok((path, pointings))
}

/// Whether `a` and `b` are the same file (CASA's `copyfile` would raise).
fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Whether any pointing falls within the model's extent and half a
/// primary beam of it, the pointings centred on their median direction and
/// shifted by its offset from the model's centre.
fn overlaps(pointings: &[String], sky: &Sky, beam_arcsec: f64) -> Result<bool, SimobserveError> {
    let centred = median_direction(pointings)?;
    let centre = split_direction(&centred.centre)?;
    let model = split_direction(&sky.model.refdir)?;
    let degrees = |angle: &Quantity| qa::convert(angle, "deg");
    let (mut ra, mut model_ra) = (
        degrees(&centre.longitude)?.value(),
        degrees(&model.longitude)?.value(),
    );
    let dec = degrees(&centre.latitude)?;
    let model_dec = degrees(&model.latitude)?.value();
    // CASA's test repeats `mra` where it means `ra` the second time.
    let near_zero = model_ra.abs() < 10.
        || (model_ra - 360.).abs() < 10.
        || ra.abs() < 10.
        || (model_ra - 360.).abs() < 10.;
    let wrap = if near_zero { 180. } else { 360. };
    if ra >= wrap {
        ra -= 360.;
    }
    if model_ra >= wrap {
        model_ra -= 360.;
    }
    let shift = [
        (ra - model_ra) * qa::value_in(&dec, "rad")?.cos(),
        dec.value() - model_dec,
    ];
    tracing::info!("center = {}", centred.centre);
    let margin = beam_arcsec * RELATIVE_MARGIN / 3600.;
    let half = |size: &Quantity| -> Result<f64, SimobserveError> {
        Ok(qa::value_in(size, "deg")? * 0.5 + margin)
    };
    let (x_max, y_max) = (half(&sky.model.size[0])?, half(&sky.model.size[1])?);
    Ok(centred
        .offsets
        .iter()
        .any(|offset| (offset[0] + shift[0]).abs() < x_max && (offset[1] + shift[1]).abs() < y_max))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_beam_spacings_match_as_python_matches_them() {
        let beams = |text: &str| primary_beams(text);
        assert_eq!(beams("0.510929PB").as_deref(), Some("0.510929"));
        assert_eq!(beams("12PB").as_deref(), Some("12"));
        assert_eq!(beams("1.5 PB").as_deref(), Some("1.5"));
        assert_eq!(beams("1X5PB").as_deref(), Some("1X5"));
        // Two digits at least, at the start.
        assert_eq!(beams("1PB"), None);
        assert_eq!(beams(" 12PB"), None);
        assert_eq!(beams("12ARCSEC"), None);
        assert_eq!(beams("123PBX").as_deref(), Some("123"));
    }

    #[test]
    fn spacings_default_to_casas_fractions_of_the_beam() {
        assert_eq!(spacing("", true, 10.).expect("a spacing"), "5.109290arcsec");
        assert_eq!(
            spacing("", false, 10.).expect("a spacing"),
            "2.949850arcsec"
        );
        assert_eq!(
            spacing("nyquist", true, 10.).expect("a spacing"),
            "4.424780arcsec"
        );
        assert_eq!(spacing("2arcsec", true, 10.).expect("a spacing"), "2arcsec");
        assert!(spacing("", true, 0.).is_err());
        let error = spacing("1x5PB", true, 10.).expect_err("not a float");
        assert_eq!(
            error.to_string(),
            "could not convert string to float: '1X5'"
        );
    }

    #[test]
    fn integrations_are_written_as_casa_writes_them() {
        let read = |text: &str| integration(text).expect("an integration");
        assert_eq!(read("10s"), ("10.000000s".to_string(), 10.));
        assert_eq!(read("1min"), ("60.000000s".to_string(), 60.));
        assert_eq!(read("2.5"), ("2.500000s".to_string(), 2.5));
        let error = integration("10m").expect_err("metres");
        assert!(
            error
                .to_string()
                .starts_with("integration time 10m does not"),
            "{error}"
        );
        let error = integration("").expect_err("empty");
        assert_eq!(error.to_string(), "can't interpret '' as a CASA quantity");
    }
}
