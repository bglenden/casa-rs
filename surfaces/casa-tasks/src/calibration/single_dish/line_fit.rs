// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public sdfit arguments, lists of results and optional text report.

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::CasaValue;
use casa_ms::MsSelection;

use casa_singledish::{SingleDishLineFinder, SingleDishLineFit, SingleDishSession};

use super::{baseline::selected_windows, selector};

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let input = task.input_path("infile")?;
    let outfile = task.required_file_text("outfile")?;
    let output = (!outfile.is_empty()).then(|| Path::new(&outfile));
    let overwrite = task.flag("overwrite")?;
    if let Some(output) = output {
        if output.exists() && !overwrite {
            return Err(TaskFailure::execution(format!("{outfile} exists.")).at("outfile"));
        }
        if casa_calibration::paths::overlap(&input, output).map_err(execution_error)? {
            return Err(TaskFailure::execution(
                "The fit report overlaps the input MeasurementSet.",
            )
            .at("outfile"));
        }
    }
    let mode = task.required_text("fitmode")?;
    let automatic = match mode.as_str() {
        "auto" => true,
        "list" => false,
        _ => {
            return Err(
                TaskFailure::execution(format!("fitmode='{mode}' is not supported yet"))
                    .at("fitmode"),
            );
        }
    };
    let spw = task.required_text("spw")?;
    let spw = if spw.is_empty() { "*" } else { &spw };
    let mut session = SingleDishSession::default();
    session.open(&input);
    session
        .set_selection(
            MsSelection {
                spw: Some(selected_windows(&input, spw)?),
                antenna: selector(task, "antenna")?,
                field: selector(task, "field")?,
                timerange: selector(task, "timerange")?,
                scan: selector(task, "scan")?,
                correlation: selector(task, "pol")?,
                intent: selector(task, "intent")?,
                ..MsSelection::default()
            },
            true,
        )
        .map_err(execution_error)?;
    let scratch = tempfile::tempdir().map_err(execution_error)?;
    let table = scratch.path().join("fit.ms");
    let raw = scratch.path().join("fit.csv");
    let counts = if automatic {
        Vec::new()
    } else {
        task.integers("nfit")?
    };
    let nfit = counts
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let fitfunc = task.required_text("fitfunc")?;
    let finder = if automatic {
        Some(SingleDishLineFinder::new(
            task.number("thresh")?,
            task.integer("avg_limit")?,
            task.integer("minwidth")?,
            &task.integers("edge")?,
        ))
    } else {
        None
    };
    session
        .fit_line(SingleDishLineFit {
            datacolumn: &task.required_text("datacolumn")?,
            spw,
            timebin: &task.required_text("timebin")?,
            timespan: &task.required_text("timespan")?,
            polaverage: &task.required_text("polaverage")?,
            fitfunc: &fitfunc,
            nfit: &nfit,
            finder,
            tempfile: &raw,
            tempoutfile: &table,
        })
        .map_err(execution_error)?;
    let component_count = (!automatic).then(|| counts.iter().sum::<i64>());
    let result = read_results(&raw, output, overwrite, component_count, &fitfunc)?;
    TaskSuccess::new(result, &())
}

const HEADER: &str = "#SCAN\tTIME\t\tANT\tBEAM\tSPW\tPOL\tFunction\tP0\t\tP1\t\tP2\n";

fn open_report(output: &Path) -> std::io::Result<tempfile::NamedTempFile> {
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    tempfile::NamedTempFile::new_in(parent)
}

fn read_results(
    input: &Path,
    output: Option<&Path>,
    overwrite: bool,
    requested_count: Option<i64>,
    fitfunc: &str,
) -> Result<CasaValue, TaskFailure> {
    let function = if fitfunc == "gaussian" {
        "gauss"
    } else {
        "lorentz"
    };
    let mut destination = output
        .map(open_report)
        .transpose()
        .map_err(execution_error)?;
    let mut writer = destination.as_mut().map(BufWriter::new);
    if let Some(writer) = &mut writer {
        writer
            .write_all(HEADER.as_bytes())
            .map_err(execution_error)?;
    }
    let mut centers = Vec::new();
    let mut peaks = Vec::new();
    let mut widths = Vec::new();
    let mut counts = Vec::new();
    let reader = File::open(input)
        .map_err(execution_error)
        .map(BufReader::new)?;
    for line in reader.lines() {
        let line = line.map_err(execution_error)?;
        let components = line.trim().split(':').collect::<Vec<_>>();
        let count = requested_count.unwrap_or(components.len() as i64);
        if count > 0 && components.len() != count as usize {
            return Err(invalid_result());
        }
        let mut values: [Vec<CasaValue>; 3] = std::array::from_fn(|_| Vec::new());
        for (component, text) in components.iter().take(count.max(0) as usize).enumerate() {
            let fields = text.split(',').collect::<Vec<_>>();
            if fields.len() != 12 {
                return Err(invalid_result());
            }
            for (pair, parameter) in fields[6..].as_chunks::<2>().0.iter().zip(&mut values) {
                let items = pair
                    .iter()
                    .map(|part| part.parse().map(CasaValue::float))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(execution_error)?;
                parameter.push(CasaValue::List { items });
            }
            if let Some(writer) = &mut writer {
                writeln!(
                    writer,
                    "{}\t{function}{component}\t\t{}\t{}\t{}\t",
                    fields[..6].join("\t"),
                    fields[8],
                    fields[6],
                    fields[10]
                )
                .map_err(execution_error)?;
            }
        }
        let [center, peak, width] = values.map(|items| CasaValue::List { items });
        centers.push(center);
        peaks.push(peak);
        widths.push(width);
        counts.push(CasaValue::Int { value: count });
    }
    if let Some(writer) = &mut writer {
        writer.flush().map_err(execution_error)?;
    }
    drop(writer);
    if let (Some(output), Some(destination)) = (output, destination) {
        let persist = if overwrite {
            destination.persist(output)
        } else {
            destination.persist_noclobber(output)
        };
        persist.map_err(execution_error)?;
    }
    Ok(CasaValue::dict([
        ("cent", CasaValue::List { items: centers }),
        ("peak", CasaValue::List { items: peaks }),
        ("fwhm", CasaValue::List { items: widths }),
        ("nfit", CasaValue::List { items: counts }),
    ]))
}

fn invalid_result() -> TaskFailure {
    TaskFailure::execution("Invalid line component result.").at("nfit")
}

fn execution_error(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}
