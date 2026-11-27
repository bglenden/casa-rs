// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task selection, time-domain smoothing and antenna reports for `wvrgcal`.

use std::path::{Path, PathBuf};

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::CasaValue;
use casa_core::quanta::{Quantity, Unit};

use casa_calibration::smooth::{Smoother, smooth_calibration_table};
use casa_calibration::wvr::{
    WvrAntennaReport, WvrError, WvrReport, WvrRequest, WvrSelectors, execute_wvr,
    resolve_wvr_selectors,
};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let destination = task.path("caltable")?;
    if destination.as_os_str().is_empty() || destination.symlink_metadata().is_ok() {
        return Err(TaskFailure::validation(format!(
            "Output caltable {} already exists or is empty",
            destination.display()
        ))
        .at("caltable"));
    }
    let smooth = task.text("smooth")?.unwrap_or_default();
    let (output, seconds) = smoothing(&smooth, &destination)?;
    let mut request = request(task, vis, output)?;
    let selection = selectors(task, request.segment_sources)?;
    resolve_wvr_selectors(&mut request, &selection)
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let mut report = match execute_wvr(&request) {
        Ok(report) => report,
        Err(
            error @ (WvrError::ReferenceUnavailable
            | WvrError::NoSolution(_)
            | WvrError::ZeroEvidence(_)),
        ) => WvrReport {
            output_table: request.output_table.clone(),
            status: if matches!(error, WvrError::ReferenceUnavailable) {
                -1
            } else {
                1
            },
            antennas: Vec::new(),
            log: error.to_string(),
        },
        Err(error) => return Err(TaskFailure::execution(error.to_string())),
    };
    if report.status == 0 && seconds > 0.0 {
        smooth_calibration_table(
            &request.measurement_set,
            &request.output_table,
            &destination,
            "",
            Smoother::Mean,
            seconds,
            false,
        )
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
        report.output_table = destination;
    }
    tracing::info!(target: "wvrgcal", "{}", report.log);
    TaskSuccess::new(antenna_record(&report), &report)
}

fn request(
    task: &TaskInvocation<'_>,
    vis: PathBuf,
    output: PathBuf,
) -> Result<WvrRequest, TaskFailure> {
    let segment_sources = task.flag("segsource")?;
    let seed = u32::try_from(task.integer("rseed")?).map_err(|_| {
        TaskFailure::validation("rseed must be from 0 through 4294967295").at("rseed")
    })?;
    let offsets = task.path("offsetstable")?;
    let count = task.integer("nsol")?.max(1) as usize;
    if segment_sources && count > 1 {
        return Err(TaskFailure::validation(
            "In order to use nsol>1, segsource must be set to False.",
        )
        .at("nsol"));
    }
    let minimum_neighbors = task.integer("minnumants")?;
    if !(1..=3).contains(&minimum_neighbors) {
        return Err(TaskFailure::validation("minnumants must be 1, 2 or 3").at("minnumants"));
    }
    Ok(WvrRequest {
        measurement_set: vis,
        output_table: output,
        time_offset_seconds: task.number("toffset")?,
        temperature_offsets: (!offsets.as_os_str().is_empty()).then_some(offsets),
        disperse: task.flag("disperse")?,
        radiometer_windows: windows(task, "wvrspw")?,
        output_windows: windows(task, "spw")?,
        reverse: false,
        reversed_windows: Vec::new(),
        segment_sources,
        solution_count: count,
        tied_sources: Vec::new(),
        flagged_sources: Vec::new(),
        statistic_sources: Vec::new(),
        statistic_fields: Vec::new(),
        use_field_table: task.flag("usefieldtab")?,
        continuum: task.flag("cont")?,
        seed,
        smoothing_samples: 1,
        scale: task.number("scale")?,
        flagged_antennas: Vec::new(),
        reference_antennas: Vec::new(),
        max_distance_meters: task.number("maxdistm")?,
        minimum_neighbors: minimum_neighbors as usize,
        minimum_good_fraction: task.number("mingoodfrac")?,
    })
}

fn selectors(task: &TaskInvocation<'_>, segmented: bool) -> Result<WvrSelectors, TaskFailure> {
    let flagged_sources = if segmented {
        task.texts("sourceflag")?
    } else {
        Vec::new()
    };
    if flagged_sources.iter().any(String::is_empty) {
        return Err(
            TaskFailure::validation("sourceflag must contain non-empty names").at("sourceflag"),
        );
    }
    let stat = |name| -> Result<Vec<String>, TaskFailure> {
        Ok(task
            .text(name)?
            .filter(|text| !text.is_empty())
            .into_iter()
            .collect())
    };
    Ok(WvrSelectors {
        flagged_sources,
        tied_sources: if segmented {
            task.texts("tie")?
        } else {
            Vec::new()
        },
        flagged_antennas: task.texts("wvrflag")?,
        reference_antennas: task.texts("refant")?,
        statistic_sources: stat("statsource")?,
        statistic_fields: stat("statfield")?,
        reversed_windows: task.text("reversespw")?.unwrap_or_default(),
    })
}

fn windows(task: &TaskInvocation<'_>, name: &str) -> Result<Vec<i32>, TaskFailure> {
    task.integers(name)?
        .into_iter()
        .map(|value| {
            i32::try_from(value)
                .ok()
                .filter(|&id| id >= 0)
                .ok_or_else(|| {
                    TaskFailure::validation("Spectral-window IDs must be nonnegative integers")
                        .at(name)
                })
        })
        .collect()
}

fn smoothing(text: &str, table: &Path) -> Result<(PathBuf, f64), TaskFailure> {
    if text.is_empty() {
        return Ok((table.to_owned(), 0.0));
    }
    let quantity = text
        .parse::<Quantity>()
        .map_err(|error| TaskFailure::validation(error.to_string()).at("smooth"))?;
    let value = quantity
        .get_value_in(&Unit::new("s").expect("seconds is a unit"))
        .map_err(|error| TaskFailure::validation(error.to_string()).at("smooth"))?;
    let mut unsmoothed = table.to_owned().into_os_string();
    unsmoothed.push("_unsmoothed");
    Ok((unsmoothed.into(), value))
}

fn antenna_record(report: &WvrReport) -> CasaValue {
    let antennas = if report.status == 0 {
        report.antennas.as_slice()
    } else {
        &[]
    };
    let entries = |convert: fn(&WvrAntennaReport) -> CasaValue| CasaValue::List {
        items: antennas.iter().map(convert).collect(),
    };
    CasaValue::dict([
        ("Name", entries(|antenna| CasaValue::string(&antenna.name))),
        (
            "WVR",
            entries(|antenna| CasaValue::Bool {
                value: antenna.has_radiometer,
            }),
        ),
        (
            "Flag",
            entries(|antenna| CasaValue::Bool {
                value: antenna.flagged,
            }),
        ),
        (
            "Frac_unflagged",
            entries(|antenna| CasaValue::float(antenna.good_fraction)),
        ),
        (
            "RMS_um",
            entries(|antenna| CasaValue::float(antenna.rms_microns)),
        ),
        (
            "Disc_um",
            entries(|antenna| CasaValue::float(antenna.discrepancy_microns)),
        ),
        (
            "rval",
            CasaValue::Int {
                value: i64::from(report.status),
            },
        ),
        (
            "success",
            CasaValue::Bool {
                value: report.status == 0,
            },
        ),
    ])
}
