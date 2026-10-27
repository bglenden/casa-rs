// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]
//! CASA reads the `MODEL_DATA` casa-rs adds (#912). After CASA's `setjy`
//! writes a virtual model, casa-rs's imaging writer adds `MODEL_DATA`, and
//! CASA's `gaincal` then solves against that column, as it does after CASA's
//! own `delmod` and `clearcal(addmodel=True)`, not against the virtual
//! model, which it prefers whenever one is defined.

use std::path::{Path, PathBuf};
use std::process::Command;

use casa_ms::{SelectedVisibilityWrite, SelectedVisibilityWriteTargets};
use flate2::read::GzDecoder;
use tar::Archive;

/// CASA's steps, by name: `setjy` a 5 Jy virtual model of field 0, copy
/// the MeasurementSet, CASA's own `MODEL_DATA` creation, `gaincal`, and the
/// largest relative difference of two tables' solutions.
const STEPS: &str = r#"
import shutil, sys
import numpy as np
from casatasks import clearcal, delmod, gaincal, setjy
from casatools import table
step, args = sys.argv[1], sys.argv[2:]
if step == "setjy":
    setjy(vis=args[0], field="0", standard="manual", fluxdensity=[5, 0, 0, 0],
          usescratch=False)
elif step == "copy":
    shutil.copytree(args[0], args[1])
elif step == "copy_without_scratch":
    shutil.copytree(args[0], args[1])
    tb = table()
    tb.open(args[1], nomodify=False)
    tb.removecols([c for c in ["MODEL_DATA", "CORRECTED_DATA"] if c in tb.colnames()])
    tb.close()
elif step == "casa_model":
    delmod(vis=args[0], otf=True, scr=False)
    clearcal(vis=args[0], addmodel=True)
elif step == "gaincal":
    gaincal(vis=args[0], caltable=args[1], field="0", solint="inf", refant="0",
            gaintype="G", calmode="ap", minsnr=0.0, minblperant=1)
elif step == "difference":
    tb = table()
    gains = []
    for path in args:
        tb.open(path)
        gains.append(tb.getcol("CPARAM"))
        tb.close()
    print(float(np.max(np.abs(gains[0] - gains[1]) / np.abs(gains[1]))))
"#;

fn unpack(root: &Path, archive: &str, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR")).join(archive);
    Archive::new(GzDecoder::new(
        std::fs::File::open(archive).expect("fixture"),
    ))
    .unpack(root)
    .expect("unpack");
    root.join(name)
}

/// Run CASA step `step` on `args`: its last line of output.
fn casa(python: &Path, step: &str, args: &[&Path]) -> String {
    let output = Command::new(python)
        .arg("-c")
        .arg(STEPS)
        .arg(step)
        .args(args)
        .output()
        .expect("CASA python");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "CASA {step}:\n{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout.lines().last().unwrap_or_default().to_string()
}

/// On `ms`: gains solved after casa-rs adds `MODEL_DATA` equal those after
/// CASA's own creation, and differ from those against the stale virtual
/// model by the model's 5 Jy.
fn casa_solves_against_casa_rs_model_data(python: &Path, ms: &Path) {
    let root = ms.parent().expect("root");
    let path = |name: &str| root.join(name);
    casa(python, "setjy", &[ms]);
    for copy in ["casa.ms", "stale.ms"] {
        casa(python, "copy", &[ms, &path(copy)]);
    }
    SelectedVisibilityWrite::begin(ms, SelectedVisibilityWriteTargets::new(true, false))
        .expect("begin")
        .complete()
        .expect("complete");
    casa(python, "casa_model", &[&path("casa.ms")]);
    for (vis, table) in [
        (ms.to_path_buf(), "casa-rs.G"),
        (path("casa.ms"), "casa.G"),
        (path("stale.ms"), "stale.G"),
    ] {
        casa(python, "gaincal", &[&vis, &path(table)]);
    }
    let difference = |a: &str, b: &str| -> f64 {
        casa(python, "difference", &[&path(a), &path(b)])
            .parse()
            .expect("a difference")
    };
    let same = difference("casa-rs.G", "casa.G");
    assert!(
        same < 1e-6,
        "{}: casa-rs against CASA, {same}",
        ms.display()
    );
    let stale = difference("casa-rs.G", "stale.G");
    assert!(
        stale > 1.0,
        "{}: the virtual model would have changed the gains ({stale})",
        ms.display()
    );
}

/// The trial fixture, an MS without SOURCE rows, whose virtual model CASA
/// writes into MAIN's keywords.
#[test]
fn casa_reads_the_model_data_casa_rs_adds() {
    let Some(python) = casa_test_support::discover_casa_python() else {
        eprintln!("scratch-column parity skipped: no CASA Python");
        return;
    };
    let root = tempfile::tempdir().expect("root");
    let ms = unpack(
        root.path(),
        "tests/fixtures/ms_trial/ms_trial.ms.tgz",
        "ms_trial.ms",
    );
    casa_solves_against_casa_rs_model_data(&python.program, &ms);
}

/// ngc5921, whose SOURCE has rows, so that CASA writes the model there
/// (its row's `SOURCE_MODEL`); its scratch columns are removed first, so
/// that casa-rs adds `MODEL_DATA`.
#[test]
fn casa_reads_the_model_data_casa_rs_adds_under_source() {
    let Some(python) = casa_test_support::discover_casa_python() else {
        eprintln!("scratch-column parity skipped: no CASA Python");
        return;
    };
    let Some(source) = casa_test_support::casatestdata_path_for_tier(
        casa_test_support::CasaTestDataTier::SlowParity,
        "measurementset/vla/ngc5921.ms",
    )
    .filter(|path| path.exists()) else {
        eprintln!("scratch-column parity skipped: no ngc5921.ms");
        return;
    };
    let root = tempfile::tempdir().expect("root");
    let ms = root.path().join("ngc5921.ms");
    casa(&python.program, "copy_without_scratch", &[&source, &ms]);
    casa_solves_against_casa_rs_model_data(&python.program, &ms);
}
