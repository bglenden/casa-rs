// SPDX-License-Identifier: LGPL-3.0-or-later

//! The CASA image writer replaces each image whole, against C++ casacore
//! holding the old image open in another process.
//!
//! A reader that opened an image, and read it, before a run replaced it
//! keeps reading the old image's pixels; a reader that opens it afterwards
//! reads the new one, whole. casacore opens some storage, such as a pixel
//! mask, by path on its first read, so a held reader that first reads it
//! after the replacement reads the new image's (the writer's module doc).
//! The held reader runs in a child process of this test binary, as `fcntl`
//! locks belong to a process.

#![cfg(feature = "cpp-interop-tests")]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use casa_coordinates::CoordinateSystem;
use casa_imaging_model::{ModelCell, ModelDeltaTerm, ModelValue, ProductKind, RestoringBeamPolicy};
use casa_imaging_products::{
    CasaImageWriter, ContinuumProductControls, ContinuumProductInputs, PlannedContinuumGeneration,
    produce_continuum_members,
};
use casa_test_support::{HeldCppImageReader, ImageOracle, casacore_oracle_available};
use common::continuum::{SHAPE, continuum_problem_with_policy};
use common::full_window;
use common::synthetic_pass::{Scene, two_cycle_round};

/// The held reader child's image, passed from the parent test.
const HELD_IMAGE: &str = "CASA_RS_HELD_IMAGE";

/// Publish one run's `.psf`, `.residual` and `.sumwt` under `base`, the
/// fixture's point source cleaned by `cleaned`, and return the residual's
/// pixel sum as casa-rs computed it.
fn publish_run(base: &Path, cleaned: f64) -> f64 {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
    ];
    let problem = continuum_problem_with_policy(130, &products, RestoringBeamPolicy::None);
    let centre = [SHAPE[0] / 2, SHAPE[1] / 2];
    let scene = Scene::new(&problem);
    let planes = scene.planes();
    let scene = scene.with_point(0, centre, &vec![1.0; planes]);
    let join = two_cycle_round(
        &problem,
        &scene,
        vec![ModelDeltaTerm::new(
            ModelCell::new(0, 0, 0, centre),
            ModelValue::new(cleaned).expect("finite model value"),
        )],
    );
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned products");
    let writer = CasaImageWriter::new(base.to_path_buf(), CoordinateSystem::new());
    produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &writer)
        .expect("generated products");
    writer.publish(|_| ()).expect("published products");
    let residual = casa_images::PagedImage::<f32>::open(residual_path(base)).expect("residual");
    let shape = residual.shape().to_vec();
    residual
        .get_slice(&vec![0; shape.len()], &shape)
        .expect("residual pixels")
        .iter()
        .map(|value| f64::from(*value))
        .sum()
}

fn residual_path(base: &Path) -> PathBuf {
    let mut path = base.as_os_str().to_owned();
    path.push(".residual");
    PathBuf::from(path)
}

/// A child process of this test binary holding an image open in C++
/// casacore and reading it on request.
struct HeldReaderProcess {
    child: Child,
    commands: ChildStdin,
    replies: BufReader<ChildStdout>,
}

impl HeldReaderProcess {
    fn open(image: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--ignored",
                "--exact",
                "held_image_reader_child",
                "--nocapture",
            ])
            .env(HELD_IMAGE, image)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("held reader child");
        let commands = child.stdin.take().expect("child stdin");
        let mut replies = BufReader::new(child.stdout.take().expect("child stdout"));
        let ready = Self::reply_from(&mut replies);
        assert_eq!(ready, "ready", "the held reader opened the image");
        Self {
            child,
            commands,
            replies,
        }
    }

    fn reply_from(replies: &mut BufReader<ChildStdout>) -> String {
        let mut line = String::new();
        loop {
            line.clear();
            assert!(
                replies.read_line(&mut line).expect("child reply") > 0,
                "the held reader exited"
            );
            let reply = line.trim();
            // libtest prints its own lines around the test's output.
            if reply == "ready" || reply.starts_with("read ") || reply.starts_with("error ") {
                return reply.to_string();
            }
        }
    }

    /// The held image's pixel sum, pixel count and valid count.
    fn read(&mut self) -> (f64, u64, u64) {
        writeln!(self.commands, "read").expect("command the held reader");
        self.commands.flush().expect("flush the command");
        let reply = Self::reply_from(&mut self.replies);
        let fields = reply
            .strip_prefix("read ")
            .unwrap_or_else(|| panic!("held reader failed: {reply}"))
            .split(' ')
            .collect::<Vec<_>>();
        (
            fields[0].parse().expect("sum"),
            fields[1].parse().expect("values"),
            fields[2].parse().expect("valid"),
        )
    }
}

impl Drop for HeldReaderProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The held reader child: opens `CASA_RS_HELD_IMAGE`, prints `ready`, and
/// answers each `read` line with `read <sum> <values> <valid>`.
#[test]
#[ignore = "a child process of a_held_reader_keeps_the_old_image_whole_across_a_replacement"]
fn held_image_reader_child() {
    let image = std::env::var_os(HELD_IMAGE).expect("the parent names the image");
    let mut reader = HeldCppImageReader::open(Path::new(&image)).expect("open the held image");
    println!("ready");
    for line in std::io::stdin().lock().lines() {
        let line = line.expect("command");
        assert_eq!(line.trim(), "read", "unknown command");
        match reader.read() {
            Ok(read) => println!("read {} {} {}", read.sum, read.values, read.valid),
            Err(error) => println!("error {error}"),
        }
        std::io::stdout().flush().expect("flush the reply");
    }
}

#[test]
fn a_held_reader_keeps_the_old_image_whole_across_a_replacement() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let directory = tempfile::tempdir().expect("output directory");
    let base = directory.path().join("image");
    let residual = residual_path(&base);
    let cells = (SHAPE[0] * SHAPE[1]) as u64;

    let old = publish_run(&base, 0.25);
    let mut held = HeldReaderProcess::open(&residual);
    let (sum, values, valid) = held.read();
    assert_eq!((values, valid), (cells, cells));
    assert!(
        (sum - old).abs() <= 1.0e-5 * old.abs().max(1.0),
        "{sum} != {old}"
    );

    let new = publish_run(&base, 0.75);
    assert!(
        (new - old).abs() > 1.0e-3 * old.abs().max(1.0),
        "the two runs' residuals differ"
    );
    let (sum, values, valid) = held.read();
    assert_eq!(
        (values, valid),
        (cells, cells),
        "the held image stays whole"
    );
    assert!(
        (sum - old).abs() <= 1.0e-5 * old.abs().max(1.0),
        "the held reader keeps the old residual: {sum} != {old}"
    );

    let fresh = ImageOracle::read_image_data(&residual, cells as usize)
        .expect("C++ casacore opens the replaced image")
        .iter()
        .map(|value| f64::from(*value))
        .sum::<f64>();
    assert!(
        (fresh - new).abs() <= 1.0e-5 * new.abs().max(1.0),
        "a fresh reader reads the new residual: {fresh} != {new}"
    );
    let staging = std::fs::read_dir(directory.path())
        .expect("output directory")
        .filter_map(|entry| {
            let name = entry.expect("entry").file_name();
            name.to_string_lossy()
                .contains("casa-rs-stage")
                .then_some(name)
        })
        .collect::<Vec<_>>();
    assert!(
        staging.is_empty(),
        "publishing leaves no staging: {staging:?}"
    );
}
