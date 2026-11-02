// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]
//! casa-rs reads the virtual model CASA's `tclean(savemodel='virtual')`
//! writes, and CASA reads the one casa-rs files (`casa_ms::virtual_model`):
//! on `refim_twochan.ms` (SOURCE rows, the model under SOURCE) and
//! `refim_point.ms` (none, the model among MAIN's keywords), CASA writes a
//! model and predicts from it (`mstransform(datacolumn='model')`, through
//! its visibility iterator); casa-rs reads the record, finds CASA's
//! combinations, clears every model, which CASA then no longer predicts
//! from, and files CASA's entries again (their grid images kept aside), from
//! which CASA predicts exactly what it predicted from its own record.

use std::path::{Path, PathBuf};
use std::process::Command;

use casa_ms::{
    MeasurementSet, MsSelection, VirtualModelEntry, VirtualModelKind, clear_virtual_model,
    put_virtual_model, virtual_model, virtual_model_index_combination,
};
use casa_types::{ScalarValue, Value};

/// CASA's steps, by name: copy a MeasurementSet, `tclean` a virtual model
/// of it, and its model visibilities as the iterator predicts them into an
/// `.npy` file (in a copy whose `MODEL_DATA` is zero, the column added
/// through the table tool when it has none, so that without a virtual
/// model the iterator reads zeros), and the largest difference of two.
const STEPS: &str = r#"
import os, shutil, sys, tempfile
import numpy as np
from casatasks import mstransform, tclean
from casatools import table
step, args = sys.argv[1], sys.argv[2:]
if step == "copy":
    shutil.copytree(args[0], args[1], symlinks=True)
elif step == "tclean":
    tclean(vis=args[0], imagename=args[1], imsize=100, cell="8.0arcsec", niter=10,
           specmode=args[2], savemodel="virtual", parallel=False)
elif step == "predict":
    scratch = tempfile.mkdtemp()
    copy = os.path.join(scratch, "copy.ms")
    shutil.copytree(args[0], copy, symlinks=True)
    tb = table()
    tb.open(copy, nomodify=False)
    if "MODEL_DATA" not in tb.colnames():
        tb.addcols({"MODEL_DATA": tb.getcoldesc("DATA")})
    tb.putcol("MODEL_DATA", np.zeros(tb.getcell("DATA", 0).shape + (tb.nrows(),),
                                     dtype=np.complex64))
    tb.close()
    out = os.path.join(scratch, "model.ms")
    mstransform(vis=copy, outputvis=out, datacolumn="model")
    tb.open(out)
    np.save(args[1], tb.getcol("DATA"))
    tb.close()
    shutil.rmtree(scratch)
elif step == "difference":
    a, b = np.load(args[0]), np.load(args[1])
    print(float(np.max(np.abs(a - b))), float(np.max(np.abs(b))))
"#;

fn casa(python: &Path, step: &str, args: &[&str]) -> String {
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

fn text(path: &Path) -> &str {
    path.to_str().expect("UTF-8 path")
}

/// `[largest |a − b|, largest |b|]` of two predictions.
fn difference(python: &Path, a: &Path, b: &Path) -> [f64; 2] {
    let line = casa(python, "difference", &[text(a), text(b)]);
    let values: Vec<f64> = line
        .split_whitespace()
        .map(|value| value.parse().expect("a number"))
        .collect();
    [values[0], values[1]]
}

fn interoperate(python: &Path, source: &Path, specmode: &str, pointer_under_source: bool) {
    let root = tempfile::tempdir().expect("root");
    let path = |name: &str| -> PathBuf { root.path().join(name) };
    let ms_path = path("vis.ms");
    casa(python, "copy", &[text(source), text(&ms_path)]);
    casa(
        python,
        "tclean",
        &[text(&ms_path), text(&path("casa")), specmode],
    );
    casa(
        python,
        "predict",
        &[text(&ms_path), text(&path("casa.npy"))],
    );

    // casa-rs reads CASA's record: one GridFT over the MS's combinations.
    let mut ms = MeasurementSet::open(&ms_path).expect("open");
    let found = virtual_model(&ms, 0).expect("read").expect("CASA's model");
    assert_eq!(found.source_row.is_some(), pointer_under_source);
    let entries = VirtualModelEntry::entries(&found.record).expect("entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, VirtualModelKind::FtMachine);
    let combinations = virtual_model_index_combination(&ms, &MsSelection::default())
        .expect("combinations")
        .index_combination;
    assert_eq!(entries[0].index_combination, combinations);

    // The grid images, kept aside from the clearing that deletes them.
    let kept: Vec<VirtualModelEntry> = entries
        .iter()
        .map(|entry| {
            let mut entry = entry.clone();
            let Some(Value::Scalar(ScalarValue::String(image))) = entry.container.get("diskimage")
            else {
                panic!("CASA's GridFT names its grid image");
            };
            let moved = path(&format!(
                "kept_{}",
                Path::new(image)
                    .file_name()
                    .expect("a name")
                    .to_string_lossy()
            ));
            casa(python, "copy", &[image, text(&moved)]);
            entry.container.upsert(
                "diskimage",
                Value::Scalar(ScalarValue::String(text(&moved).to_string())),
            );
            entry
        })
        .collect();

    // Cleared, CASA predicts no model from it.
    clear_virtual_model(&mut ms, &[], &[]).expect("clear");
    assert!(virtual_model(&ms, 0).expect("read").is_none());
    casa(
        python,
        "predict",
        &[text(&ms_path), text(&path("cleared.npy"))],
    );
    let [_, cleared] = difference(python, &path("casa.npy"), &path("cleared.npy"));
    assert!(
        cleared == 0.0,
        "a cleared model predicts nothing, {cleared}"
    );

    // Filed again by casa-rs, CASA predicts what it predicted from its own
    // record.
    for entry in &kept {
        put_virtual_model(&mut ms, entry, true).expect("put");
    }
    let refiled = virtual_model(&ms, 0).expect("read").expect("a model");
    assert_eq!(refiled.source_row.is_some(), pointer_under_source);
    drop(ms);
    casa(
        python,
        "predict",
        &[text(&ms_path), text(&path("refiled.npy"))],
    );
    let [change, peak] = difference(python, &path("refiled.npy"), &path("casa.npy"));
    assert!(peak > 0.1, "CASA's model predicts its flux, {peak}");
    assert!(
        change == 0.0,
        "{}: CASA predicts from casa-rs's filing what it predicted, {change}",
        source.display()
    );
}

fn fixture(relative: &str) -> Option<PathBuf> {
    casa_test_support::casatestdata_path_for_tier(
        casa_test_support::CasaTestDataTier::SlowParity,
        relative,
    )
    .filter(|path| path.exists())
}

#[test]
fn casa_and_casa_rs_read_each_other_s_virtual_models() {
    let Some(python) = casa_test_support::discover_casa_python() else {
        eprintln!("virtual-model interop skipped: no CASA Python");
        return;
    };
    for (relative, specmode, under_source) in [
        ("unittest/tclean/refim_twochan.ms", "mfs", true),
        ("unittest/tclean/refim_point.ms", "cube", false),
    ] {
        let Some(source) = fixture(relative) else {
            eprintln!("virtual-model interop skipped: no {relative}");
            continue;
        };
        interoperate(&python.program, &source, specmode, under_source);
    }
}
