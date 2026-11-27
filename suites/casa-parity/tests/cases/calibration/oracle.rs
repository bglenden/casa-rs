// SPDX-License-Identifier: LGPL-3.0-or-later
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value as JsonValue;

pub fn discover_casa_python() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    for key in ["CASA_RS_CASA_PYTHON", "CASA_PYTHON"] {
        if let Some(value) = std::env::var_os(key) {
            candidates.push(PathBuf::from(value));
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join("SoftwareProjects")
                .join("casa-build")
                .join("venv")
                .join("bin")
                .join("python"),
        );
    }
    candidates.push(PathBuf::from("python3"));
    candidates.push(PathBuf::from("python"));

    candidates.into_iter().find(|program| {
        Command::new(program)
            .current_dir(casa_test_support::test_log_dir())
            .arg("-c")
            .arg("import casatasks")
            .output()
            .is_ok_and(|output| output.status.success())
    })
}

pub fn ngc5921_ms_path() -> Option<PathBuf> {
    casa_test_support::casatestdata_path_for_tier(
        casa_test_support::CasaTestDataTier::SlowParity,
        "measurementset/vla/ngc5921.ms",
    )
    .filter(|path| path.exists())
}

pub fn casa_skip_reason() -> String {
    match (discover_casa_python(), ngc5921_ms_path()) {
        (None, _) => {
            "CASA calibration parity skipped: no CASA-capable python found via CASA_RS_CASA_PYTHON, CASA_PYTHON, python3, or python".to_string()
        }
        (_, None) => {
            "CASA calibration parity skipped: missing ngc5921.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, ~/SoftwareProjects/casatestdata, or slow-parity /Volumes/home/casatestdata".to_string()
        }
        _ => "CASA calibration parity skipped".to_string(),
    }
}

pub fn generate_casa_exemplars(output_dir: &Path) -> Result<(PathBuf, PathBuf, PathBuf), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let ms_path = ngc5921_ms_path().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
from casatasks import bandpass, gaincal

vis = os.environ["CASA_RS_CAL_MS"]
out = os.environ["CASA_RS_CAL_OUT"]
phase = os.path.join(out, "phase.gcal")
tsolve = os.path.join(out, "t.gcal")
bp = os.path.join(out, "b.bcal")

gaincal(
    vis=vis,
    caltable=phase,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    calmode="p",
    minsnr=0.0,
)
gaincal(
    vis=vis,
    caltable=tsolve,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    calmode="p",
    gaintype="T",
    minsnr=0.0,
)
bandpass(
    vis=vis,
    caltable=bp,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    bandtype="B",
    gaintable=[phase],
    minsnr=0.0,
)
"#;
    let output = Command::new(&python)
        .current_dir(output_dir)
        .env("CASA_RS_CAL_MS", &ms_path)
        .env("CASA_RS_CAL_OUT", output_dir)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA exemplar generation failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok((
        output_dir.join("phase.gcal"),
        output_dir.join("t.gcal"),
        output_dir.join("b.bcal"),
    ))
}

pub fn copy_measurement_set(source: &Path, destination: &Path) -> Result<(), String> {
    if source.is_dir() {
        fs::create_dir_all(destination).map_err(|error| {
            format!(
                "create destination directory {}: {error}",
                destination.display()
            )
        })?;
        for entry in fs::read_dir(source).map_err(|error| {
            format!(
                "read MeasurementSet directory {}: {error}",
                source.display()
            )
        })? {
            let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
            let child_source = entry.path();
            let child_destination = destination.join(entry.file_name());
            copy_measurement_set(&child_source, &child_destination)?;
        }
        return Ok(());
    }

    fs::copy(source, destination).map_err(|error| {
        format!(
            "copy MeasurementSet file {} -> {}: {error}",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

pub fn generate_casa_phase_gain(
    output_dir: &Path,
    field: &str,
    spw: &str,
    refant: &str,
) -> Result<PathBuf, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let source = ngc5921_ms_path().ok_or_else(casa_skip_reason)?;
    let ms_path = output_dir.join("ngc5921.ms");
    copy_measurement_set(&source, &ms_path)?;
    let script = r#"
import os
from casatasks import gaincal

gaincal(
    vis=os.environ["CASA_RS_CAL_MS"],
    caltable=os.environ["CASA_RS_CALTABLE"],
    field=os.environ["CASA_RS_CAL_FIELD"],
    spw=os.environ["CASA_RS_CAL_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_CAL_REFANT"],
    calmode="p",
    minsnr=0.0,
)
"#;
    let caltable = output_dir.join("phase.gcal");
    let output = Command::new(&python)
        .current_dir(output_dir)
        .env("CASA_RS_CAL_MS", &ms_path)
        .env("CASA_RS_CALTABLE", &caltable)
        .env("CASA_RS_CAL_FIELD", field)
        .env("CASA_RS_CAL_SPW", spw)
        .env("CASA_RS_CAL_REFANT", refant)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA phase gain generation failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(caltable)
}

#[allow(clippy::too_many_arguments)]
pub fn run_casa_applycal(
    ms_path: &Path,
    caltable: &Path,
    field: &str,
    spw: &str,
    scan: Option<&str>,
    applymode: &str,
    calwt: bool,
    parang: bool,
) -> Result<(), String> {
    run_casa_applycal_chain(
        ms_path,
        &[caltable],
        field,
        spw,
        scan,
        applymode,
        calwt,
        parang,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_casa_applycal_with_gainfield(
    ms_path: &Path,
    caltable: &Path,
    field: &str,
    spw: &str,
    gainfield: &str,
    applymode: &str,
    calwt: bool,
    parang: bool,
) -> Result<(), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
from casatasks import applycal

applycal(
    vis=os.environ["CASA_RS_APPLY_MS"],
    field=os.environ["CASA_RS_APPLY_FIELD"],
    spw=os.environ["CASA_RS_APPLY_SPW"],
    gaintable=[os.environ["CASA_RS_APPLY_GAINTABLE"]],
    gainfield=[os.environ["CASA_RS_APPLY_GAINFIELD"]],
    interp=["nearest"],
    calwt=os.environ["CASA_RS_APPLY_CALWT"] == "true",
    applymode=os.environ["CASA_RS_APPLY_MODE"],
    parang=os.environ["CASA_RS_APPLY_PARANG"] == "true",
    flagbackup=False,
)
"#;
    let output = Command::new(&python)
        .current_dir(casa_test_support::test_log_dir())
        .env("CASA_RS_APPLY_MS", ms_path)
        .env("CASA_RS_APPLY_FIELD", field)
        .env("CASA_RS_APPLY_SPW", spw)
        .env("CASA_RS_APPLY_GAINTABLE", caltable)
        .env("CASA_RS_APPLY_GAINFIELD", gainfield)
        .env("CASA_RS_APPLY_MODE", applymode)
        .env("CASA_RS_APPLY_CALWT", if calwt { "true" } else { "false" })
        .env(
            "CASA_RS_APPLY_PARANG",
            if parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA applycal with gainfield failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

pub fn run_casa_applycal_with_callib(
    ms_path: &Path,
    callib: &Path,
    field: &str,
    spw: &str,
    applymode: &str,
    parang: bool,
) -> Result<(), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
from casatasks import applycal

applycal(
    vis=os.environ["CASA_RS_APPLY_MS"],
    field=os.environ["CASA_RS_APPLY_FIELD"],
    spw=os.environ["CASA_RS_APPLY_SPW"],
    docallib=True,
    callib=os.environ["CASA_RS_APPLY_CALLIB"],
    applymode=os.environ["CASA_RS_APPLY_MODE"],
    parang=os.environ["CASA_RS_APPLY_PARANG"] == "true",
    flagbackup=False,
)
"#;
    let output = Command::new(&python)
        .current_dir(casa_test_support::test_log_dir())
        .env("CASA_RS_APPLY_MS", ms_path)
        .env("CASA_RS_APPLY_FIELD", field)
        .env("CASA_RS_APPLY_SPW", spw)
        .env("CASA_RS_APPLY_CALLIB", callib)
        .env("CASA_RS_APPLY_MODE", applymode)
        .env(
            "CASA_RS_APPLY_PARANG",
            if parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA applycal with callib failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn run_casa_applycal_chain(
    ms_path: &Path,
    caltables: &[&Path],
    field: &str,
    spw: &str,
    scan: Option<&str>,
    applymode: &str,
    calwt: bool,
    parang: bool,
) -> Result<(), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import applycal

applycal(
    vis=os.environ["CASA_RS_APPLY_MS"],
    field=os.environ["CASA_RS_APPLY_FIELD"],
    spw=os.environ["CASA_RS_APPLY_SPW"],
    scan=os.environ.get("CASA_RS_APPLY_SCAN", ""),
    gaintable=json.loads(os.environ["CASA_RS_APPLY_GAINTABLES"]),
    interp=["nearest"],
    calwt=os.environ["CASA_RS_APPLY_CALWT"] == "true",
    applymode=os.environ["CASA_RS_APPLY_MODE"],
    parang=os.environ["CASA_RS_APPLY_PARANG"] == "true",
    flagbackup=False,
)
"#;
    let mut command = Command::new(&python);
    command.current_dir(casa_test_support::test_log_dir());
    let gaintables_json = serde_json::to_string(
        &caltables
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
    )
    .expect("serialize applycal gaintables");
    command
        .env("CASA_RS_APPLY_MS", ms_path)
        .env("CASA_RS_APPLY_GAINTABLES", gaintables_json)
        .env("CASA_RS_APPLY_FIELD", field)
        .env("CASA_RS_APPLY_SPW", spw)
        .env("CASA_RS_APPLY_MODE", applymode)
        .env("CASA_RS_APPLY_CALWT", if calwt { "true" } else { "false" })
        .env(
            "CASA_RS_APPLY_PARANG",
            if parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script);
    if let Some(scan) = scan
        && !scan.is_empty()
    {
        command.env("CASA_RS_APPLY_SCAN", scan);
    }
    let output = command
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA applycal failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

pub fn run_casa_gaincal(
    output_dir: &Path,
    ms_path: &Path,
    options: CasaGaincalOptions<'_>,
) -> Result<PathBuf, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import gaincal

gaincal(
    vis=os.environ["CASA_RS_GAIN_MS"],
    caltable=os.environ["CASA_RS_GAIN_CALTABLE"],
    field=os.environ["CASA_RS_GAIN_FIELD"],
    spw=os.environ["CASA_RS_GAIN_SPW"],
    solint=os.environ["CASA_RS_GAIN_SOLINT"],
    refant=os.environ["CASA_RS_GAIN_REFANT"],
    gaintype=os.environ["CASA_RS_GAIN_TYPE"],
    calmode=os.environ["CASA_RS_GAIN_MODE"],
    combine=os.environ["CASA_RS_GAIN_COMBINE"],
    gaintable=json.loads(os.environ["CASA_RS_GAIN_GAINTABLES"]),
    smodel=[1,0,0,0],
    minsnr=0.0,
    parang=os.environ["CASA_RS_GAIN_PARANG"] == "true",
)
"#;
    let suffix = match options.gaintype {
        "T" => "t.gcal",
        "K" => "k.gcal",
        _ => "g.gcal",
    };
    let caltable = output_dir.join(suffix);
    let gaintables_json = serde_json::to_string(
        &options
            .prior_gaintables
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
    )
    .expect("serialize gaincal prior gaintables");
    let output = Command::new(&python)
        .current_dir(output_dir)
        .env("CASA_RS_GAIN_MS", ms_path)
        .env("CASA_RS_GAIN_CALTABLE", &caltable)
        .env("CASA_RS_GAIN_FIELD", options.field)
        .env("CASA_RS_GAIN_SPW", options.spw)
        .env("CASA_RS_GAIN_SOLINT", options.solint)
        .env("CASA_RS_GAIN_REFANT", options.refant)
        .env("CASA_RS_GAIN_TYPE", options.gaintype)
        .env("CASA_RS_GAIN_MODE", options.calmode)
        .env("CASA_RS_GAIN_COMBINE", options.combine)
        .env("CASA_RS_GAIN_GAINTABLES", gaintables_json)
        .env(
            "CASA_RS_GAIN_PARANG",
            if options.parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA gaincal failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(caltable)
}

pub fn generate_casa_fluxscale_gain_fixture(
    output_dir: &Path,
    reference_field: &str,
    transfer_field: &str,
    spw: &str,
    refant: &str,
) -> Result<(PathBuf, PathBuf), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let source_ms = ngc5921_ms_path().ok_or_else(casa_skip_reason)?;
    let ms_copy = output_dir.join("fluxscale.ms");
    copy_measurement_set(&source_ms, &ms_copy)?;
    let gain_table = output_dir.join("fluxscale.gcal");
    let script = r#"
import os
from casatasks import gaincal, setjy

vis = os.environ["CASA_RS_FLUXSCALE_MS"]
reference_field = os.environ["CASA_RS_FLUXSCALE_REFERENCE"]
transfer_field = os.environ["CASA_RS_FLUXSCALE_TRANSFER"]

setjy(
    vis=vis,
    field=reference_field,
    standard="Perley-Taylor 99",
    scalebychan=False,
    usescratch=True,
)
gaincal(
    vis=vis,
    caltable=os.environ["CASA_RS_FLUXSCALE_GCAL"],
    field=f"{reference_field},{transfer_field}",
    spw=os.environ["CASA_RS_FLUXSCALE_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_FLUXSCALE_REFANT"],
    gaintype="G",
    calmode="ap",
    smodel=[1,0,0,0],
    minsnr=0.0,
)
"#;
    let output = Command::new(&python)
        .current_dir(output_dir)
        .env("CASA_RS_FLUXSCALE_MS", &ms_copy)
        .env("CASA_RS_FLUXSCALE_GCAL", &gain_table)
        .env("CASA_RS_FLUXSCALE_REFERENCE", reference_field)
        .env("CASA_RS_FLUXSCALE_TRANSFER", transfer_field)
        .env("CASA_RS_FLUXSCALE_SPW", spw)
        .env("CASA_RS_FLUXSCALE_REFANT", refant)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA fluxscale gain fixture generation failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok((ms_copy, gain_table))
}

pub fn run_casa_fluxscale(
    ms_path: &Path,
    caltable: &Path,
    fluxtable: &Path,
    reference: &str,
    transfer: &str,
) -> Result<JsonValue, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import fluxscale

def normalize(value):
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if hasattr(value, "tolist"):
        return normalize(value.tolist())
    if hasattr(value, "item"):
        return normalize(value.item())
    return value

result = fluxscale(
    vis=os.environ["CASA_RS_FLUXSCALE_MS"],
    caltable=os.environ["CASA_RS_FLUXSCALE_INPUT"],
    fluxtable=os.environ["CASA_RS_FLUXSCALE_OUTPUT"],
    reference=os.environ["CASA_RS_FLUXSCALE_REFERENCE"],
    transfer=os.environ["CASA_RS_FLUXSCALE_TRANSFER"],
)
with open(os.environ["CASA_RS_CALIBRATION_JSON"], "w") as record:
    json.dump(normalize(result), record, sort_keys=True)
"#;
    let record = tempfile::NamedTempFile::new().map_err(|error| error.to_string())?;
    let output = Command::new(&python)
        .env("CASA_RS_CALIBRATION_JSON", record.path())
        .current_dir(casa_test_support::test_log_dir())
        .env("CASA_RS_FLUXSCALE_MS", ms_path)
        .env("CASA_RS_FLUXSCALE_INPUT", caltable)
        .env("CASA_RS_FLUXSCALE_OUTPUT", fluxtable)
        .env("CASA_RS_FLUXSCALE_REFERENCE", reference)
        .env("CASA_RS_FLUXSCALE_TRANSFER", transfer)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA fluxscale failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_reader(record.as_file())
        .map_err(|error| format!("parse CASA fluxscale json: {error}"))
}

#[allow(clippy::too_many_arguments)]
pub fn run_casa_bandpass(
    output_dir: &Path,
    ms_path: &Path,
    field: &str,
    spw: &str,
    refant: &str,
    prior_gaintables: &[&Path],
    combine: &str,
    solnorm: bool,
    parang: bool,
) -> Result<PathBuf, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"\
import os
import json
from casatasks import bandpass

bandpass(
    vis=os.environ["CASA_RS_BANDPASS_MS"],
    caltable=os.environ["CASA_RS_BANDPASS_CALTABLE"],
    field=os.environ["CASA_RS_BANDPASS_FIELD"],
    spw=os.environ["CASA_RS_BANDPASS_SPW"],
    solint="inf",
    combine=os.environ["CASA_RS_BANDPASS_COMBINE"],
    refant=os.environ["CASA_RS_BANDPASS_REFANT"],
    bandtype="B",
    gaintable=json.loads(os.environ["CASA_RS_BANDPASS_GAINTABLES"]),
    solnorm=os.environ["CASA_RS_BANDPASS_SOLNORM"] == "true",
    smodel=[1,0,0,0],
    minsnr=0.0,
    parang=os.environ["CASA_RS_BANDPASS_PARANG"] == "true",
)
"#;
    let caltable = output_dir.join("b.bcal");
    let gaintables_json = serde_json::to_string(
        &prior_gaintables
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
    )
    .expect("serialize bandpass prior gaintables");
    let output = Command::new(&python)
        .current_dir(output_dir)
        .env("CASA_RS_BANDPASS_MS", ms_path)
        .env("CASA_RS_BANDPASS_CALTABLE", &caltable)
        .env("CASA_RS_BANDPASS_FIELD", field)
        .env("CASA_RS_BANDPASS_SPW", spw)
        .env("CASA_RS_BANDPASS_COMBINE", combine)
        .env("CASA_RS_BANDPASS_REFANT", refant)
        .env("CASA_RS_BANDPASS_GAINTABLES", gaintables_json)
        .env(
            "CASA_RS_BANDPASS_SOLNORM",
            if solnorm { "true" } else { "false" },
        )
        .env(
            "CASA_RS_BANDPASS_PARANG",
            if parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA bandpass failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(caltable)
}

pub fn run_casa_bandpass_bpoly(
    output_dir: &Path,
    ms_path: &Path,
    field: &str,
    spw: &str,
    refant: &str,
    prior_gaintables: &[&Path],
) -> Result<PathBuf, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import bandpass

bandpass(
    vis=os.environ["CASA_RS_BPOLY_MS"],
    caltable=os.environ["CASA_RS_BPOLY_CALTABLE"],
    field=os.environ["CASA_RS_BPOLY_FIELD"],
    spw=os.environ["CASA_RS_BPOLY_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_BPOLY_REFANT"],
    bandtype="BPOLY",
    degamp=3,
    degphase=3,
    visnorm=False,
    solnorm=False,
    maskcenter=0,
    maskedge=0,
    gaintable=json.loads(os.environ["CASA_RS_BPOLY_GAINTABLES"]),
    smodel=[1,0,0,0],
    minsnr=0.0,
)
"#;
    let caltable = output_dir.join("bp.bpoly");
    let gaintables_json = serde_json::to_string(
        &prior_gaintables
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
    )
    .expect("serialize BPOLY prior gaintables");
    let output = Command::new(&python)
        .current_dir(output_dir)
        .env("CASA_RS_BPOLY_MS", ms_path)
        .env("CASA_RS_BPOLY_CALTABLE", &caltable)
        .env("CASA_RS_BPOLY_FIELD", field)
        .env("CASA_RS_BPOLY_SPW", spw)
        .env("CASA_RS_BPOLY_REFANT", refant)
        .env("CASA_RS_BPOLY_GAINTABLES", gaintables_json)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA BPOLY bandpass failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(caltable)
}

pub fn run_casa_calstat(
    caltable: &Path,
    axis: &str,
    datacolumn: &str,
) -> Result<JsonValue, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import calstat

def normalize(value):
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if hasattr(value, "item"):
        return normalize(value.item())
    return value

result = calstat(
    caltable=os.environ["CASA_RS_CALSTAT_TABLE"],
    axis=os.environ["CASA_RS_CALSTAT_AXIS"],
    datacolumn=os.environ["CASA_RS_CALSTAT_DATACOLUMN"],
)
with open(os.environ["CASA_RS_CALIBRATION_JSON"], "w") as record:
    json.dump(normalize(result), record, sort_keys=True)
"#;
    let record = tempfile::NamedTempFile::new().map_err(|error| error.to_string())?;
    let output = Command::new(&python)
        .env("CASA_RS_CALIBRATION_JSON", record.path())
        .current_dir(casa_test_support::test_log_dir())
        .env("CASA_RS_CALSTAT_TABLE", caltable)
        .env("CASA_RS_CALSTAT_AXIS", axis)
        .env("CASA_RS_CALSTAT_DATACOLUMN", datacolumn)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA calstat failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_reader(record.as_file())
        .map_err(|error| format!("parse CASA calstat json: {error}"))
}

pub struct CasaGaincalOptions<'a> {
    pub field: &'a str,
    pub spw: &'a str,
    pub refant: &'a str,
    pub gaintype: &'a str,
    pub calmode: &'a str,
    pub solint: &'a str,
    pub combine: &'a str,
    pub prior_gaintables: Vec<&'a Path>,
    pub parang: bool,
}
