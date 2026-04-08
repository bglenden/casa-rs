// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

use std::env;
use std::path::PathBuf;
use std::process::Command;

use casa_test_support::casatestdata_path;

/// Resolved local CASA Python environment.
#[derive(Debug, Clone)]
pub struct CasaPython {
    /// Python interpreter path.
    pub program: PathBuf,
    /// Whether the environment can import a callable `plotms`.
    pub plotms_available: bool,
}

/// Discover a CASA-capable Python interpreter.
pub fn discover_casa_python() -> Option<CasaPython> {
    casa_python_candidates()
        .into_iter()
        .find_map(probe_casa_python)
}

/// Resolve the shared `ngc5921.ms` MeasurementSet fixture.
pub fn ngc5921_ms_path() -> Option<PathBuf> {
    casatestdata_path("measurementset/vla/ngc5921.ms").filter(|path| path.exists())
}

/// Return a human-readable skip reason for CASA parity tests.
pub fn skip_reason(require_plotms: bool) -> String {
    let python = discover_casa_python();
    match (python, ngc5921_ms_path()) {
        (None, _) => {
            "CASA parity skipped: no CASA-capable python found via CASA_RS_CASA_PYTHON, CASA_PYTHON, python3, or python".to_string()
        }
        (Some(python), _) if require_plotms && !python.plotms_available => {
            format!(
                "CASA parity skipped: {} can import casatasks but does not expose plotms",
                python.program.display()
            )
        }
        (_, None) => {
            "CASA parity skipped: missing ngc5921.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata".to_string()
        }
        _ => "CASA parity skipped".to_string(),
    }
}

fn casa_python_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for key in ["CASA_RS_CASA_PYTHON", "CASA_PYTHON"] {
        if let Some(value) = env::var_os(key) {
            candidates.push(PathBuf::from(value));
        }
    }
    if let Some(home) = env::var_os("HOME") {
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
    dedup_paths(candidates)
}

fn dedup_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut unique = Vec::new();
    for path in paths {
        if !unique.iter().any(|candidate| candidate == &path) {
            unique.push(path);
        }
    }
    unique
}

fn probe_casa_python(program: PathBuf) -> Option<CasaPython> {
    if !python_can_import(&program, "casatasks") {
        return None;
    }
    Some(CasaPython {
        plotms_available: python_has_plotms(&program),
        program,
    })
}

fn python_can_import(program: &PathBuf, module: &str) -> bool {
    Command::new(program)
        .arg("-c")
        .arg(format!("import {module}"))
        .output()
        .is_ok_and(|output| output.status.success())
}

fn python_has_plotms(program: &PathBuf) -> bool {
    let script = r#"
import importlib.util
import casatasks
ok = hasattr(casatasks, "plotms") or importlib.util.find_spec("casaplotms") is not None
print("1" if ok else "0")
"#;
    Command::new(program)
        .arg("-c")
        .arg(script)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "1")
}
