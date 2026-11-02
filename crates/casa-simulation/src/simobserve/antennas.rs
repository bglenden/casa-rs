// SPDX-License-Identifier: LGPL-3.0-or-later
//! The antenna list: the files' stem, ALMA's resolution strings
//! (`alma;0.4arcsec`, `alma.cycle1;…`, `alma.cycle2;…`), the search through
//! the project directory, the path given and CASA's `alma/simmos`, and the
//! primary beam and resolution the configuration gives.

use std::f64::consts::PI;
use std::path::{Path, PathBuf};

use super::sky::SkyInput;
use super::{Run, SimobserveError};
use crate::readantenna::{AntennaConfigFile, read_antenna_config};
use crate::simutil::baselines::approximate_beam;
use crate::simutil::pointings::PBCOEFF;
use crate::simutil::qa;

/// What the run takes from its antenna configuration.
pub(super) struct Array {
    /// The configuration, reduced to `sdant`'s antenna for a single dish.
    pub(super) config: AntennaConfigFile,
    /// `pb`: the primary beam, `1.13 λ/D` at the model's frequency for the
    /// mean dish, arcsec.
    pub(super) primary_beam_arcsec: f64,
    /// `psfsize`: the resolution, arcsec: `approxBeam` of the whole
    /// configuration for an interferometer, the primary beam for a single
    /// dish.
    pub(super) resolution_arcsec: f64,
}

/// `project + "." + <configuration>`: the antenna list's file name without
/// `.cfg`, with `;` made `_` in ALMA resolution strings.
pub(super) fn stem(project: &str, antennalist: &str) -> Result<String, SimobserveError> {
    let name = if upper_prefix(antennalist, 4) == "ALMA" {
        antennalist.replace(';', "_")
    } else if !antennalist.is_empty() {
        antennalist.to_string()
    } else {
        return Err(SimobserveError::raised(
            "The name of antenna list (antennalist/sdantlist) is not specified",
        ));
    };
    let name = name.replace(".cfg", "");
    let name = name.split('/').next_back().unwrap_or_default();
    Ok(format!("{project}.{name}"))
}

/// The first `count` characters of `text` in upper case, as Python's
/// `str.upper(text[0:count])`.
fn upper_prefix(text: &str, count: usize) -> String {
    text.chars().take(count).collect::<String>().to_uppercase()
}

/// `ctsys.resolve("alma/simmos")`: the first data root holding it, or the
/// unresolved name.
fn repository(data_roots: &[PathBuf]) -> PathBuf {
    data_roots
        .iter()
        .map(|root| root.join("alma").join("simmos"))
        .find(|path| path.is_dir())
        .unwrap_or_else(|| PathBuf::from("alma/simmos"))
}

/// Resolve the antenna list, read it, and form the primary beam and the
/// resolution; for a single dish, keep `sdant`'s antenna.
pub(super) fn resolve(run: &mut Run<'_>, input: &SkyInput) -> Result<Array, SimobserveError> {
    let repository = repository(&run.request.data_roots);
    let frequency_ghz = qa::value_in(&input.model.specrefval, "GHz")?;
    if upper_prefix(&run.antennalist, 4) == "ALMA" {
        run.antennalist = alma_configuration(&run.antennalist, frequency_ghz, &repository)?;
    }
    let path = search(run, &repository)?;
    let full = read_antenna_config(&path, &*run.request.options.measures)?;
    let mut config = full.clone();
    let antennas = config.x.len();
    if antennas == 1 {
        if run.predict && run.uvmode {
            return Err(SimobserveError::raised(
                "antennalist contains only 1 antenna",
            ));
        }
        run.uvmode = false;
    }
    if !run.uvmode {
        config = single_dish(config, run.parameters.sdant);
    }
    let mean_diameter = numpy_mean(&config.dish_diameter);
    let primary_beam_arcsec = PBCOEFF * 0.29979 / frequency_ghz / mean_diameter * 3600. * 180. / PI;
    let resolution_arcsec = if run.uvmode {
        approximate_beam(&full, frequency_ghz)?
    } else {
        if !input.components_only {
            let size = &input.model.size;
            let smallest = qa::value_in(&size[0], "arcsec")?.min(qa::value_in(&size[1], "arcsec")?);
            if smallest < 2.5 * primary_beam_arcsec {
                return Err(SimobserveError::raised(format!(
                    "skymodel should be larger than 2.5*primary beam. Your skymodel: {smallest:.3} \
                     arcsec < {:.3} arcsec: 2.5*primary beam",
                    2.5 * primary_beam_arcsec
                )));
            }
        }
        primary_beam_arcsec
    };
    check_correlator(&run.parameters.correlator, &config.telescope)?;
    Ok(Array {
        config,
        primary_beam_arcsec,
        resolution_arcsec,
    })
}

/// The configuration an ALMA resolution string names, in `repository`:
/// Cycle 1's or Cycle 2's (`alma.cycle1;0.5arcsec`), else the full array's
/// (`alma;0.5arcsec`), at `frequency_ghz`; any other string unchanged.
fn alma_configuration(
    antennalist: &str,
    frequency_ghz: f64,
    repository: &Path,
) -> Result<String, SimobserveError> {
    let upper = antennalist.to_uppercase();
    let resolution = |tail: &str| -> Result<Option<f64>, SimobserveError> {
        if !qa::is_quantity(tail) {
            return Ok(None);
        }
        if !repository.exists() {
            return Err(SimobserveError::raised(format!(
                "failed to find antenna configuration repository at {}",
                repository.display()
            )));
        }
        Ok(Some(qa::value_in(&qa::read(tail)?, "arcsec")?))
    };
    let resolved = |name: String| -> Result<String, SimobserveError> {
        let path = repository.join(name).display().to_string();
        tracing::info!("converted resolution to antennalist {path}");
        Ok(path)
    };
    if let Some(tail) = cycle_tail(&upper, '1')
        && let Some(arcsec) = resolution(&tail.to_lowercase())?
    {
        let number = 1.044 - 6.733 * (arcsec * frequency_ghz / 345.).log10();
        return resolved(format!("alma.cycle1.{}.cfg", configuration(number, 6.0)));
    }
    if let Some(tail) = cycle_tail(&upper, '2')
        && let Some(arcsec) = resolution(&tail.to_lowercase())?
    {
        let number = 10_f64.powf(0.91 - 0.74 * (arcsec * frequency_ghz / 345.));
        return resolved(format!("alma.cycle2.{}.cfg", configuration(number, 7.0)));
    }
    let tail: String = antennalist.chars().skip(5).collect();
    if let Some(arcsec) = resolution(&tail)? {
        let number = (2.867 - (arcsec * 1000. * frequency_ghz / 672.).log10()) / 0.0721;
        return resolved(format!("alma.out{:02}.cfg", configuration(number, 28.0)));
    }
    Ok(antennalist.to_string())
}

/// `str(int(round(max(1, min(largest, number)))))`, Python rounding half to
/// even.
fn configuration(number: f64, largest: f64) -> i64 {
    number.min(largest).max(1.0).round_ties_even() as i64
}

/// The tail Python's `re.match('.*CYCLE.?<cycle>.?;(.*)', upper)` captures:
/// the greedy `.*` takes the last `CYCLE` that matches, and each `.?` tries
/// one character before none. `.` matches anything but a newline.
fn cycle_tail(upper: &str, cycle: char) -> Option<String> {
    let chars: Vec<char> = upper.chars().collect();
    let line_end = chars.iter().position(|&c| c == '\n').unwrap_or(chars.len());
    let any = |index: usize| index < line_end;
    let word: Vec<char> = "CYCLE".chars().collect();
    if line_end < word.len() {
        return None;
    }
    for start in (0..=line_end - word.len()).rev() {
        if chars[start..start + word.len()] != word[..] {
            continue;
        }
        let after = start + word.len();
        for first in [1, 0] {
            let digit = after + first;
            if first == 1 && !any(after) || chars.get(digit) != Some(&cycle) || !any(digit) {
                continue;
            }
            for second in [1, 0] {
                let colon = digit + 1 + second;
                if second == 1 && !any(digit + 1) || chars.get(colon) != Some(&';') {
                    continue;
                }
                return Some(chars[colon + 1..line_end].iter().collect());
            }
        }
    }
    None
}

/// The antenna list's path, searched as CASA searches it: in the project
/// directory, then as given, then in the repository.
fn search(run: &Run<'_>, repository: &Path) -> Result<PathBuf, SimobserveError> {
    let name = &run.antennalist;
    let in_project = PathBuf::from(format!("{}/{name}", run.parameters.project));
    let path = if in_project.exists() {
        in_project
    } else if !Path::new(name).exists() && repository.join(name).exists() {
        repository.join(name)
    } else {
        PathBuf::from(name)
    };
    if !path.exists() {
        return Err(SimobserveError::raised(format!(
            "Couldn't find antennalist: {}",
            path.display()
        )));
    }
    Ok(path)
}

/// `config` reduced to antenna `sdant` (counted from the end when
/// negative), or to the first when `sdant` is out of range.
fn single_dish(mut config: AntennaConfigFile, sdant: i64) -> AntennaConfigFile {
    let count = config.x.len() as i64;
    let sdant = if sdant > count - 1 || sdant < -count {
        tracing::warn!("antenna index {sdant} is out of range. setting sdant=0");
        0
    } else {
        sdant
    };
    let index = (if sdant < 0 { count + sdant } else { sdant }) as usize;
    let keep = |values: &mut Vec<f64>| *values = vec![values[index]];
    keep(&mut config.x);
    keep(&mut config.y);
    keep(&mut config.z);
    keep(&mut config.dish_diameter);
    config.pad_names = vec![config.pad_names[index].clone()];
    config.antenna_names = vec![config.antenna_names[index].clone()];
    config
}

/// NumPy's mean of `values`: their pairwise sum (`add.reduce`) over their
/// count.
pub(super) fn numpy_mean(values: &[f64]) -> f64 {
    pairwise_sum(values) / values.len() as f64
}

/// NumPy's pairwise summation: plain below 8 values, eight running sums up
/// to 128, and halves (kept to multiples of 8) above.
fn pairwise_sum(values: &[f64]) -> f64 {
    let count = values.len();
    if count < 8 {
        return values.iter().fold(0.0, |sum, value| sum + value);
    }
    if count <= 128 {
        let mut sums: [f64; 8] = std::array::from_fn(|index| values[index]);
        let whole = count - count % 8;
        for block in values[8..whole].as_chunks::<8>().0 {
            for (sum, value) in sums.iter_mut().zip(block) {
                *sum += value;
            }
        }
        let sum = ((sums[0] + sums[1]) + (sums[2] + sums[3]))
            + ((sums[4] + sums[5]) + (sums[6] + sums[7]));
        return values[whole..].iter().fold(sum, |sum, value| sum + value);
    }
    let half = count / 2;
    let half = half - half % 8;
    pairwise_sum(&values[..half]) + pairwise_sum(&values[half..])
}

/// CASA's refusal of a correlator the telescope does not have.
fn check_correlator(correlator: &str, telescope: &str) -> Result<(), SimobserveError> {
    if correlator.is_empty() {
        return Ok(());
    }
    let allowed: &[&str] = match telescope {
        "ALMASD" => &["ACASpec", "ACA"],
        "ALMA" => &["BLC", "BLC_4x4bit"],
        "ACA" => &["BLC_4x4bit", "BLC", "ACA"],
        _ => return Ok(()),
    };
    if allowed.contains(&correlator) {
        return Ok(());
    }
    Err(SimobserveError::raised(format!(
        "correlator = {correlator} is not allowed for telescope = {telescope}. Allowed values: {}",
        allowed.join(" ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stem_names_the_configuration() {
        let stem = |list: &str| stem("p", list).expect("a stem");
        assert_eq!(stem("alma.out10.cfg"), "p.alma.out10");
        assert_eq!(stem("alma;0.5arcsec"), "p.alma_0.5arcsec");
        assert_eq!(stem("/data/vla.a.cfg"), "p.vla.a");
        assert_eq!(stem("configs/"), "p.");
        assert!(super::stem("p", "").is_err());
    }

    #[test]
    fn cycle_tails_match_as_python_matches_them() {
        let tail = |text: &str, cycle| cycle_tail(text, cycle);
        assert_eq!(
            tail("ALMA.CYCLE1;0.5ARCSEC", '1').as_deref(),
            Some("0.5ARCSEC")
        );
        assert_eq!(
            tail("ALMA.CYCLE.1.;1ARCSEC", '1').as_deref(),
            Some("1ARCSEC")
        );
        assert_eq!(tail("ALMA_CYCLE2;0.1ARCSEC", '1'), None);
        assert_eq!(tail("CYCLE1;A;B", '1').as_deref(), Some("A;B"));
        // The greedy `.*` takes the last CYCLE that matches.
        assert_eq!(tail("CYCLE1;X CYCLE1;Y", '1').as_deref(), Some("Y"));
        assert_eq!(tail("ALMA;0.5ARCSEC", '1'), None);
        assert_eq!(tail("AB", '1'), None);
    }

    #[test]
    fn configuration_numbers_round_half_to_even() {
        assert_eq!(configuration(2.5, 6.0), 2);
        assert_eq!(configuration(3.5, 6.0), 4);
        assert_eq!(configuration(-4.0, 6.0), 1);
        assert_eq!(configuration(40.0, 28.0), 28);
    }

    #[test]
    fn numpy_means_sum_pairwise() {
        // Below 8 values, NumPy sums in order.
        assert_eq!(numpy_mean(&[1.0, 2.0, 4.0]), 7.0 / 3.0);
        // From 8 on, eight running sums: summed in order, 1e16 swallows the
        // seven ones before -1e16 cancels it (7); the running sums keep
        // them (14).
        let mut values = vec![1.0; 16];
        values[0] = 1e16;
        values[8] = -1e16;
        assert_eq!(pairwise_sum(&values), 14.0);
        let large: Vec<f64> = (0..300).map(f64::from).collect();
        assert_eq!(pairwise_sum(&large), 44_850.0);
    }

    #[test]
    fn correlators_are_checked_for_alma() {
        assert!(check_correlator("", "ALMA").is_ok());
        assert!(check_correlator("BLC", "ALMA").is_ok());
        assert!(check_correlator("ACA", "VLA").is_ok());
        let error = check_correlator("ACA", "ALMA").expect_err("refused");
        assert_eq!(
            error.to_string(),
            "correlator = ACA is not allowed for telescope = ALMA. Allowed values: BLC BLC_4x4bit"
        );
    }
}
