// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

const CASA_SOURCE_ROOT: &str = "/Users/brianglendenning/SoftwareProjects/casa/casatools/src/code";
const CASA_BUILD_ROOT: &str = "/Users/brianglendenning/SoftwareProjects/casa-build";

#[test]
#[ignore = "diagnostic helper for CASA VLAFiller internals; run explicitly"]
fn vlafiller_reports_first_spw_seed_for_xp1() {
    let archive_path =
        PathBuf::from("/Volumes/home/casatestdata/unittest/importvla/AS758_C030425.xp1");
    if !archive_path.exists() {
        eprintln!("skipping: {} does not exist", archive_path.display());
        return;
    }

    let source_root = PathBuf::from(CASA_SOURCE_ROOT);
    if !source_root.exists() {
        eprintln!(
            "skipping: CASA source root not found at {}",
            source_root.display()
        );
        return;
    }

    let build_root = PathBuf::from(CASA_BUILD_ROOT);
    if !build_root.exists() {
        eprintln!(
            "skipping: CASA build root not found at {}",
            build_root.display()
        );
        return;
    }

    let Some(pkg_config_args) = casa_build_pkg_config_args(&build_root) else {
        eprintln!("skipping: CASA build casacore pkg-config unavailable");
        return;
    };

    let tempdir = TempDir::new().expect("create tempdir");
    let helper = compile_cpp_helper(tempdir.path(), &source_root, &build_root, &pkg_config_args)
        .expect("compile CASA VLAFiller helper");
    let ms_path = tempdir.path().join("debug.ms");

    let output = Command::new(&helper)
        .arg(&archive_path)
        .arg(&ms_path)
        .output()
        .expect("run CASA VLAFiller helper");
    assert!(
        output.status.success(),
        "C++ helper failed: status={:?}\nstdout={}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let values = parse_key_value_output(&String::from_utf8_lossy(&output.stdout));
    eprintln!("{values:#?}");
}

fn casa_build_pkg_config_args(build_root: &Path) -> Option<Vec<String>> {
    let pkg_config_path = build_root.join("install/lib/pkgconfig");
    let output = Command::new("pkg-config")
        .env("PKG_CONFIG_PATH", pkg_config_path)
        .args(["--cflags", "--libs", "casacore"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(
        String::from_utf8(output.stdout)
            .ok()?
            .split_whitespace()
            .map(ToString::to_string)
            .collect(),
    )
}

fn compile_cpp_helper(
    tempdir: &Path,
    source_root: &Path,
    build_root: &Path,
    pkg_config_args: &[String],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let source = tempdir.join("cpp_vlafiller_seed_debug.cc");
    std::fs::write(&source, helper_source())?;
    let binary = tempdir.join("cpp_vlafiller_seed_debug");

    let install_include = build_root.join("install/include");
    let install_lib = build_root.join("install/lib");

    let mut command = Command::new("c++");
    command.arg("-std=c++17");
    command.arg(format!("-I{}", source_root.display()));
    command.arg(format!("-I{}", install_include.display()));
    command.arg(format!("-L{}", install_lib.display()));
    command.arg(format!("-Wl,-rpath,{}", install_lib.display()));
    for arg in pkg_config_args {
        command.arg(arg);
    }
    command.arg("-lcasacpp_nrao");
    command.arg(&source);
    command.arg("-o");
    command.arg(&binary);

    let output = command.output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to compile C++ helper:\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(binary)
}

fn parse_key_value_output(stdout: &str) -> BTreeMap<String, String> {
    stdout
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn helper_source() -> &'static str {
    r#"
#define private public
#include <nrao/VLA/VLAFiller.h>
#undef private

#include <nrao/VLA/VLADiskInput.h>
#include <nrao/VLA/VLAObsModeFilter.h>

#include <casacore/casa/OS/Path.h>
#include <casacore/measures/Measures/MEpoch.h>
#include <casacore/measures/Measures/MDirection.h>

#include <iostream>
#include <iomanip>

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: cpp_vlafiller_seed_debug <archive> <ms>\n";
    return 2;
  }

  try {
    casacore::Path archivePath(argv[1]);
    casacore::Path msPath(argv[2]);

    VLALogicalRecord input(new VLADiskInput(archivePath));
    casacore::MeasurementSet ms = VLAFiller::emptyMS(msPath, true);
    VLAFiller filler(ms, input, 150000.0, false, "new", true);

    VLAFilterSet filters;
    VLAObsModeFilter obsModeFilter;
    filters.addFilter(obsModeFilter);
    filler.setFilter(filters);

    // Stop immediately after the first supported xp1 record (logical record 26).
    filler.checkStop = true;
    filler.stopTime = casacore::MVEpoch(52754.91920);

    std::cout << std::setprecision(17);
    filler.fill(-1);
    std::cout << "spw_rows=" << filler.spectralWindow().nrow() << "\n";
    std::cout << "main_rows=" << filler.nrow() << "\n";
    if (filler.spectralWindow().nrow() > 0) {
      const auto chanFreq = filler.spectralWindow().chanFreq()(0);
      std::cout << "spw0_chan0_hz=" << chanFreq[0] << "\n";
      std::cout << "spw0_ref_hz=" << filler.spectralWindow().refFrequency()(0) << "\n";
      std::cout << "spw0_total_bw_hz=" << filler.spectralWindow().totalBandwidth()(0) << "\n";
      const auto* epoch = dynamic_cast<const casacore::MEpoch*>(filler.itsFrame.epoch());
      if (epoch != nullptr) {
        std::cout << "frame_epoch_day=" << epoch->getValue().getDay() << "\n";
        std::cout << "frame_epoch_dayfrac=" << epoch->getValue().getDayFraction() << "\n";
      }
      const auto* dir = dynamic_cast<const casacore::MDirection*>(filler.itsFrame.direction());
      if (dir != nullptr) {
        std::cout << "frame_dir_lon_rad=" << dir->getValue().getLong() << "\n";
        std::cout << "frame_dir_lat_rad=" << dir->getValue().getLat() << "\n";
        std::cout << "frame_dir_type=" << static_cast<int>(dir->getRef().getType()) << "\n";
      }
      std::cout << "current_source_name=" << filler.itsRecord.SDA().sourceName() << "\n";
      std::cout << "current_obs_mode=" << filler.itsRecord.SDA().obsMode() << "\n";
    }
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "exception=" << ex.what() << "\n";
    return 3;
  } catch (...) {
    std::cerr << "exception=unknown\n";
    return 4;
  }
}
"#
}
