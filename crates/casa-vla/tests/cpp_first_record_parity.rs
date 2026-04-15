// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use casa_vla::{CdaId, VlaDiskReader};
use tempfile::TempDir;

const CASA_VLA_SOURCE_ROOT: &str =
    "/Users/brianglendenning/SoftwareProjects/casa/casatools/src/code";

#[test]
fn first_record_matches_cpp_when_configured() {
    let Some(archive_path) = std::env::var_os("CASA_RS_IMPORTVLA_ARCHIVE").map(PathBuf::from)
    else {
        eprintln!("skipping: CASA_RS_IMPORTVLA_ARCHIVE not set");
        return;
    };
    if !archive_path.exists() {
        eprintln!("skipping: {} does not exist", archive_path.display());
        return;
    }

    let source_root = PathBuf::from(CASA_VLA_SOURCE_ROOT);
    if !source_root.exists() {
        eprintln!(
            "skipping: CASA VLA source root not found at {}",
            source_root.display()
        );
        return;
    }

    let Some(pkg_config) = pkg_config_args() else {
        eprintln!("skipping: pkg-config casacore unavailable");
        return;
    };

    let target_record_index = std::env::var("CASA_RS_IMPORTVLA_LOGICAL_RECORD_INDEX")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut reader = VlaDiskReader::open(&archive_path).expect("open archive");
    let record = (0..=target_record_index)
        .map(|_| reader.next_record().expect("read logical record"))
        .last()
        .flatten()
        .expect("archive contains requested logical record");

    let tempdir = TempDir::new().expect("create tempdir");
    let helper = compile_cpp_helper(tempdir.path(), &source_root, &pkg_config)
        .expect("compile C++ parity helper");
    let record_path = tempdir.path().join("record.bin");
    std::fs::write(&record_path, record.bytes()).expect("write logical record bytes");

    let output = Command::new(&helper)
        .arg(&record_path)
        .output()
        .expect("run C++ parity helper");
    assert!(
        output.status.success(),
        "C++ helper failed: status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let cpp = parse_key_value_output(&String::from_utf8_lossy(&output.stdout));

    assert_eq!(
        cpp["revision"],
        record.rca().revision().unwrap().to_string()
    );
    assert_eq!(cpp["obs_day"], record.rca().obs_day().unwrap().to_string());
    assert_eq!(
        cpp["n_antennas"],
        record.rca().n_antennas().unwrap().to_string()
    );

    let sda = record.sda().expect("decode SDA");
    assert_eq!(cpp["source_name"], sda.source_name().unwrap());
    assert_eq!(cpp["obs_id"], sda.observation_id().unwrap());
    assert_eq!(
        cpp["corr_mode"],
        corr_mode_name(sda.correlator_mode().unwrap())
    );
    let cpp_dir_lon = cpp["source_dir_lon_rad"]
        .parse::<f64>()
        .expect("parse cpp source dir lon");
    let cpp_dir_lat = cpp["source_dir_lat_rad"]
        .parse::<f64>()
        .expect("parse cpp source dir lat");
    let rust_dir = sda
        .source_direction_radians()
        .expect("decode source direction");
    assert_close(cpp_dir_lon, rust_dir[0], 1.0e-12, "source dir lon");
    assert_close(cpp_dir_lat, rust_dir[1], 1.0e-12, "source dir lat");
    assert_eq!(
        cpp["direction_epoch"],
        direction_epoch_name(sda.direction_epoch().unwrap())
    );
    assert_eq!(
        cpp["cda0_true_channels"],
        sda.true_channels(CdaId::Cda0).unwrap().to_string()
    );

    let cpp_obs_freq = cpp["cda0_obs_freq_hz"]
        .parse::<f64>()
        .expect("parse cpp obs freq");
    assert_close(
        cpp_obs_freq,
        sda.observed_frequency_hz(CdaId::Cda0).unwrap(),
        1.0e-10,
        "cda0 observed frequency",
    );
    let cpp_edge_freq = cpp["cda0_edge_freq_hz"]
        .parse::<f64>()
        .expect("parse cpp edge freq");
    assert_close(
        cpp_edge_freq,
        sda.edge_frequency_hz(CdaId::Cda0).unwrap(),
        1.0e-10,
        "cda0 edge frequency",
    );
    let cpp_chan_width = cpp["cda0_chan_width_hz"]
        .parse::<f64>()
        .expect("parse cpp chan width");
    assert_close(
        cpp_chan_width,
        sda.channel_width_hz(CdaId::Cda0).unwrap(),
        1.0e-10,
        "cda0 channel width",
    );

    let ada0 = record.ada(0).expect("decode ADA0");
    assert_eq!(cpp["ada0_ant_id"], ada0.antenna_id().unwrap().to_string());
    assert_eq!(cpp["ada0_ant_name"], ada0.antenna_name(true).unwrap());

    let cpp_ada0_u = cpp["ada0_u_m"].parse::<f64>().expect("parse cpp ada0 u");
    assert_close(cpp_ada0_u, ada0.u_meters().unwrap(), 1.0e-10, "ada0 u");

    if cpp.get("cda0_valid").is_some_and(|value| value == "1") {
        let cda0 = record.cda(CdaId::Cda0).expect("decode CDA0");
        let baseline = cda0.auto_corr(0).expect("decode CDA0 autocorr 0");
        assert_eq!(
            cpp["cda0_auto0_scale"],
            baseline.scale().unwrap().to_string()
        );
        assert_eq!(cpp["cda0_auto0_ant1"], baseline.ant1().unwrap().to_string());
        assert_eq!(cpp["cda0_auto0_ant2"], baseline.ant2().unwrap().to_string());
        assert_eq!(
            cpp["cda0_auto0_len"],
            baseline.data().unwrap().len().to_string()
        );

        let cpp_real = cpp["cda0_auto0_first_real"]
            .parse::<f32>()
            .expect("parse cpp first real");
        let cpp_imag = cpp["cda0_auto0_first_imag"]
            .parse::<f32>()
            .expect("parse cpp first imag");
        let first = baseline
            .data()
            .unwrap()
            .into_iter()
            .next()
            .expect("baseline has at least one sample");
        assert_close(
            cpp_real as f64,
            first.re as f64,
            1.0e-6,
            "cda0 auto0 first real",
        );
        assert_close(
            cpp_imag as f64,
            first.im as f64,
            1.0e-6,
            "cda0 auto0 first imag",
        );
    }
}

fn pkg_config_args() -> Option<Vec<String>> {
    let output = Command::new("pkg-config")
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
    pkg_config_args: &[String],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let source = tempdir.join("cpp_vla_parity.cc");
    std::fs::write(&source, helper_source())?;
    let binary = tempdir.join("cpp_vla_parity");

    let mut command = Command::new("c++");
    command.arg("-std=c++17");
    command.arg(format!("-I{}", source_root.display()));
    for arg in pkg_config_args {
        command.arg(arg);
    }
    command.arg(&source);
    for file in [
        "nrao/VLA/VLARCA.cc",
        "nrao/VLA/VLASDA.cc",
        "nrao/VLA/VLAADA.cc",
        "nrao/VLA/VLACDA.cc",
        "nrao/VLA/VLABaselineRecord.cc",
        "nrao/VLA/VLAContinuumRecord.cc",
        "nrao/VLA/VLASpectralLineRecord.cc",
        "nrao/VLA/VLAEnum.cc",
    ] {
        command.arg(source_root.join(file));
    }
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

fn corr_mode_name(mode: casa_vla::CorrelatorMode) -> &'static str {
    match mode {
        casa_vla::CorrelatorMode::Continuum => " ",
        casa_vla::CorrelatorMode::A => "1A",
        casa_vla::CorrelatorMode::B => "1B",
        casa_vla::CorrelatorMode::C => "1C",
        casa_vla::CorrelatorMode::D => "1D",
        casa_vla::CorrelatorMode::Ab => "2AB",
        casa_vla::CorrelatorMode::Ac => "2AC",
        casa_vla::CorrelatorMode::Ad => "2AD",
        casa_vla::CorrelatorMode::Bc => "2BC",
        casa_vla::CorrelatorMode::Bd => "2BD",
        casa_vla::CorrelatorMode::Cd => "2CD",
        casa_vla::CorrelatorMode::Abcd => "4",
        casa_vla::CorrelatorMode::Pa => "PA",
        casa_vla::CorrelatorMode::Pb => "PB",
        casa_vla::CorrelatorMode::Unknown => "UNKNOWN",
    }
}

fn direction_epoch_name(epoch: casa_vla::DirectionEpoch) -> &'static str {
    match epoch {
        casa_vla::DirectionEpoch::J2000 => "J2000",
        casa_vla::DirectionEpoch::B1950Vla => "B1950",
        casa_vla::DirectionEpoch::Apparent => "APP",
        casa_vla::DirectionEpoch::Unknown(_) => "UNKNOWN",
    }
}

fn assert_close(expected: f64, actual: f64, rel_tol: f64, label: &str) {
    let scale = expected.abs().max(actual.abs()).max(1.0);
    let rel = (expected - actual).abs() / scale;
    assert!(
        rel <= rel_tol,
        "{label}: expected {expected:.15e}, got {actual:.15e}, rel diff {rel:.3e}"
    );
}

fn helper_source() -> &'static str {
    r#"
#include <casacore/casa/IO/ByteSource.h>
#include <casacore/casa/IO/ConversionIO.h>
#include <casacore/casa/IO/MemoryIO.h>
#include <casacore/casa/OS/ModcompDataConversion.h>
#include <casacore/casa/BasicSL/Complex.h>
#include <nrao/VLA/VLAADA.h>
#include <nrao/VLA/VLACDA.h>
#include <nrao/VLA/VLAEnum.h>
#include <nrao/VLA/VLARCA.h>
#include <nrao/VLA/VLASDA.h>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <vector>

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: cpp_vla_parity <logical-record-bytes>\n";
    return 2;
  }

  std::ifstream in(argv[1], std::ios::binary);
  if (!in) {
    std::cerr << "failed to open " << argv[1] << "\n";
    return 2;
  }
  std::vector<casacore::uChar> bytes((std::istreambuf_iterator<char>(in)),
                                     std::istreambuf_iterator<char>());
  auto memory = std::make_shared<casacore::MemoryIO>(bytes.data(), bytes.size());
  auto modcomp = std::make_shared<casacore::ModcompDataConversion>();
  auto conversion = std::make_shared<casacore::ConversionIO>(modcomp, memory, 2048);
  casacore::ByteSource record(conversion);

  VLARCA rca(record);
  VLASDA sda(record, rca.SDAOffset());

  std::cout << std::setprecision(17);
  std::cout << "revision=" << rca.revision() << "\n";
  std::cout << "obs_day=" << rca.obsDay() << "\n";
  std::cout << "n_antennas=" << rca.nAntennas() << "\n";
  std::cout << "source_name=" << sda.sourceName() << "\n";
  std::cout << "obs_id=" << sda.obsId() << "\n";
  casacore::Vector<casacore::Double> source_dir = sda.sourceDir();
  std::cout << "source_dir_lon_rad=" << source_dir[0] << "\n";
  std::cout << "source_dir_lat_rad=" << source_dir[1] << "\n";
  const auto epoch = sda.epoch();
  if (epoch == casacore::MDirection::J2000) {
    std::cout << "direction_epoch=J2000\n";
  } else if (epoch == casacore::MDirection::B1950) {
    std::cout << "direction_epoch=B1950\n";
  } else if (epoch == casacore::MDirection::APP) {
    std::cout << "direction_epoch=APP\n";
  } else {
    std::cout << "direction_epoch=UNKNOWN\n";
  }
  std::cout << "corr_mode=" << VLAEnum::name(sda.correlatorMode()) << "\n";
  std::cout << "cda0_true_channels=" << sda.trueChannels(VLAEnum::CDA0) << "\n";
  std::cout << "cda0_obs_freq_hz=" << sda.obsFrequency(VLAEnum::CDA0) << "\n";
  std::cout << "cda0_edge_freq_hz=" << sda.edgeFrequency(VLAEnum::CDA0) << "\n";
  std::cout << "cda0_chan_width_hz=" << sda.channelWidth(VLAEnum::CDA0) << "\n";

  if (rca.nAntennas() > 0) {
    VLAADA ada;
    ada.attach(record, rca.ADAOffset(0));
    std::cout << "ada0_ant_id=" << ada.antId() << "\n";
    std::cout << "ada0_ant_name=" << ada.antName(true) << "\n";
    std::cout << "ada0_u_m=" << ada.u() << "\n";
  }

  if (rca.CDAOffset(0) != 0 && rca.nAntennas() > 0) {
    VLACDA cda(record, rca.CDAOffset(0), rca.CDABaselineBytes(0), rca.nAntennas(),
               sda.trueChannels(VLAEnum::CDA0));
    const VLABaselineRecord& baseline = cda.autoCorr(0);
    casacore::Vector<casacore::Complex> values = baseline.data();
    std::cout << "cda0_valid=1\n";
    std::cout << "cda0_auto0_scale=" << baseline.scale() << "\n";
    std::cout << "cda0_auto0_ant1=" << baseline.ant1() << "\n";
    std::cout << "cda0_auto0_ant2=" << baseline.ant2() << "\n";
    std::cout << "cda0_auto0_len=" << values.nelements() << "\n";
    if (values.nelements() > 0) {
      std::cout << "cda0_auto0_first_real=" << real(values[0]) << "\n";
      std::cout << "cda0_auto0_first_imag=" << imag(values[0]) << "\n";
    }
  } else {
    std::cout << "cda0_valid=0\n";
  }

  return 0;
}
"#
}
