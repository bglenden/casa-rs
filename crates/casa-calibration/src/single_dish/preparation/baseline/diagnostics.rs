// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's per-spectrum coefficient reports.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use casa_types::quanta::MvTime;
use tempfile::NamedTempFile;

use super::SingleDishBaselineDiagnostics;
use super::polynomial::Polynomial;
use super::table::{BaselineRow, SpectrumModel};

pub(super) struct Diagnostics {
    csv: Option<ReportFile>,
    text: Option<ReportFile>,
}

impl Diagnostics {
    pub(super) fn prepare(
        destinations: SingleDishBaselineDiagnostics<'_>,
        overwrite: bool,
    ) -> Result<Self, String> {
        // The tool appends to existing reports; the task writes fresh reports.
        let append = destinations.heading.is_none();
        let prepare = |path: Option<&Path>| {
            path.map(|path| ReportFile::prepare(path, overwrite, append))
                .transpose()
        };
        let csv = prepare(destinations.csv)?;
        let mut text = prepare(destinations.text)?;
        if let (Some(text), Some(heading)) = (&mut text, destinations.heading) {
            text.writer
                .write_all(heading.as_bytes())
                .map_err(error_text)?;
        }
        Ok(Self { csv, text })
    }

    pub(super) fn enabled(&self) -> bool {
        self.csv.is_some() || self.text.is_some()
    }

    pub(super) fn report(
        &mut self,
        row: &BaselineRow,
        pol: usize,
        model: &SpectrumModel,
        clipped: usize,
    ) -> Result<(), String> {
        let family = match model.family {
            Polynomial::Monomial => "poly",
            Polynomial::Chebyshev => "chebyshev",
        };
        let order = model.coefficients.len() - 1;
        let rms = significant(f64::from(model.rms), 8);
        if let Some(csv) = &mut self.csv {
            let writer = &mut csv.writer;
            writeln!(
                writer,
                "{},{},{},{},{},{},{},{},{},{},{}",
                row.scan,
                row.beam,
                row.window,
                pol,
                significant(row.time, 12),
                ranges(&model.mask, true),
                family,
                order,
                model
                    .coefficients
                    .iter()
                    .map(|&value| significant(f64::from(value as f32), 8))
                    .collect::<Vec<_>>()
                    .join(","),
                rms,
                clipped,
            )
            .map_err(error_text)?;
        }
        if let Some(text) = &mut self.text {
            let writer = &mut text.writer;
            let time = MvTime::from_mjd_seconds(row.time);
            let date = format!(
                "{:04}/{:02}/{:02}",
                time.year(),
                time.month(),
                time.month_day()
            );
            let time = time.format_time(2);
            writeln!(writer, "Scan[{}] Beam[{}] Spw[{}] Pol[{pol}] Time[{date}/{time}]\n\nFitter range = {}\n\nBaseline parameters  Function = {family}  order = {order}\n\nResults of baseline fit\n",
                     row.scan, row.beam, row.window, ranges(&model.mask, false)).map_err(error_text)?;
            for (index, &value) in model.coefficients.iter().enumerate() {
                write!(
                    writer,
                    "p{index} = {}  ",
                    significant(f64::from(value as f32), 8)
                )
                .map_err(error_text)?;
            }
            writeln!(writer, "\n\nrms = {rms}\n\nNumber of clipped channels = {clipped}\n\n------------------------------------------------------\n")
                .map_err(error_text)?;
        }
        Ok(())
    }

    pub(super) fn publish(self) -> Result<(), String> {
        for report in [self.csv, self.text].into_iter().flatten() {
            report.publish()?;
        }
        Ok(())
    }
}

struct ReportFile {
    destination: PathBuf,
    writer: BufWriter<NamedTempFile>,
    overwrite: bool,
}

impl ReportFile {
    fn prepare(destination: &Path, overwrite: bool, append: bool) -> Result<Self, String> {
        if destination.exists() && !overwrite {
            return Err(format!("{} exists.", destination.display()));
        }
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut writer = BufWriter::new(NamedTempFile::new_in(parent).map_err(error_text)?);
        if destination.exists() {
            if !destination.is_file() {
                return Err(format!("{} is not a regular file.", destination.display()));
            }
            if append {
                io::copy(
                    &mut File::open(destination).map_err(error_text)?,
                    &mut writer,
                )
                .map_err(error_text)?;
            }
        }
        Ok(Self {
            destination: destination.to_owned(),
            writer,
            overwrite,
        })
    }

    fn publish(self) -> Result<(), String> {
        let temporary = self.writer.into_inner().map_err(error_text)?;
        if self.overwrite {
            temporary.persist(self.destination)
        } else {
            temporary.persist_noclobber(self.destination)
        }
        .map_err(error_text)?;
        Ok(())
    }
}

fn ranges(mask: &[u32], csv: bool) -> String {
    let mut result = String::from("[");
    for (index, pair) in mask.as_chunks::<2>().0.iter().enumerate() {
        if index != 0 {
            result.push(if csv { ';' } else { ',' });
        }
        let divide = if csv || index == 0 { ';' } else { ',' };
        result.push_str(&format!("[{}{divide}{}]", pair[0], pair[1]));
    }
    result.push(']');
    result
}

fn significant(value: f64, precision: usize) -> String {
    if !value.is_finite() || value == 0. {
        return value.to_string();
    }
    let scientific = format!("{:.*e}", precision - 1, value);
    let (mantissa, exponent) = scientific.split_once('e').expect("scientific notation");
    let exponent: i32 = exponent.parse().expect("scientific exponent");
    if exponent < -4 || exponent >= precision as i32 {
        let mantissa = trim_decimals(mantissa);
        format!("{mantissa}e{exponent:+03}")
    } else {
        let decimals = (precision as i32 - 1 - exponent) as usize;
        trim_decimals(&format!("{value:.decimals$}")).to_string()
    }
}

fn trim_decimals(value: &str) -> &str {
    if value.contains('.') {
        value.trim_end_matches('0').trim_end_matches('.')
    } else {
        value
    }
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
