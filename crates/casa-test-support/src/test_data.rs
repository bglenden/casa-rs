// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared CASA test-data and reference-environment discovery.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Shared CASA dataset tier used by test-data discovery and gate preflights.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasaTestDataTier {
    /// Small shared fixtures that may be used by default local gates.
    DefaultFixture,
    /// Tutorial parity data that must be selected explicitly by longer gates.
    TutorialParity,
    /// Slow parity or performance data that must be selected explicitly.
    SlowParity,
}

impl CasaTestDataTier {
    /// Stable lower-case name used in diagnostics and scripts.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DefaultFixture => "default-fixture",
            Self::TutorialParity => "tutorial-parity",
            Self::SlowParity => "slow-parity",
        }
    }
}

/// One tutorial or long-gate dataset that can be staged under `CASA_RS_TUTORIAL_DATA_ROOT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TutorialDataset {
    /// Stable registry key.
    pub key: &'static str,
    /// CASA Guide or tutorial source URL.
    pub source_url: &'static str,
    /// Download URL for the source artifact.
    pub artifact_url: &'static str,
    /// Expected source artifact filename.
    pub expected_filename: &'static str,
    /// Declared CASA Guide or CASA version, when known.
    pub casa_guide_version: Option<&'static str>,
    /// Expected byte size, when known from the source inventory.
    pub expected_size_bytes: Option<u64>,
    /// Expected SHA-256 checksum, once a local mirror has been verified.
    pub expected_sha256: Option<&'static str>,
    /// Dataset gate tier.
    pub tier: CasaTestDataTier,
    /// Path relative to `CASA_RS_TUTORIAL_DATA_ROOT`.
    pub relative_path: &'static str,
}

/// Tutorial dataset registry entries needed by the first tutorial-parity waves.
pub const TUTORIAL_DATASETS: &[TutorialDataset] = &[
    TutorialDataset {
        key: "alma/first-look/twhya/calibrated-ms",
        source_url: "https://casaguides.nrao.edu/index.php/First_Look_at_Imaging",
        artifact_url: "https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.1/twhya_calibrated.ms.tar",
        expected_filename: "twhya_calibrated.ms.tar",
        casa_guide_version: Some("6.6.1"),
        expected_size_bytes: Some(435_742_720),
        expected_sha256: Some("f0cfeee5b9dec09ac9ed4d3e4e048d5eb28023c11cbc8295c09ddefe6b8a97b2"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/first-look/twhya/twhya_calibrated.ms.tar",
    },
    TutorialDataset {
        key: "alma/first-look/twhya/uncalibrated-ms",
        source_url: "https://casaguides.nrao.edu/index.php/First_Look_at_Imaging",
        artifact_url: "https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_uncalibrated.ms.tar",
        expected_filename: "twhya_uncalibrated.ms.tar",
        casa_guide_version: Some("6.6.6"),
        expected_size_bytes: Some(765_388_800),
        expected_sha256: Some("4eb09a74e9be71fea9761a54884869dce361fee83c0b9d636ffa4b2bdc882835"),
        tier: CasaTestDataTier::SlowParity,
        relative_path: "tutorial-parity/alma/first-look/twhya/twhya_uncalibrated.ms.tar",
    },
    TutorialDataset {
        key: "alma/first-look/twhya/calibrated-unflagged-ms",
        source_url: "https://casaguides.nrao.edu/index.php/First_Look_at_Imaging",
        artifact_url: "https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_calibrated_unflagged.ms.tar",
        expected_filename: "twhya_calibrated_unflagged.ms.tar",
        casa_guide_version: Some("6.6.6"),
        expected_size_bytes: Some(623_800_320),
        expected_sha256: Some("3d2c460c126957d02025ec842c4279718a7a58b2147980d84ce0523e4cf1309d"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/first-look/twhya/twhya_calibrated_unflagged.ms.tar",
    },
    TutorialDataset {
        key: "alma/first-look/twhya/selfcal-ms",
        source_url: "https://casaguides.nrao.edu/index.php/First_Look_at_Line_Imaging",
        artifact_url: "https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_selfcal.ms.tgz",
        expected_filename: "twhya_selfcal.ms.tgz",
        casa_guide_version: Some("6.6.6"),
        expected_size_bytes: Some(392_786_323),
        expected_sha256: Some("6d720b89a7b433fbc9b0cc04cde973c03bde1b63945a3f40f6e59816ae6769fc"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/first-look/twhya/twhya_selfcal.ms.tgz",
    },
    TutorialDataset {
        key: "alma/automasking/contsub-ms",
        source_url: "https://casaguides.nrao.edu/index.php?title=Automasking_Guide_CASA_6.5.4",
        artifact_url: "https://bulk.cv.nrao.edu/almadata/public/casaguides/Automasking_Guide/twhya_selfcal.ms.contsub.tar",
        expected_filename: "twhya_selfcal.ms.contsub.tar",
        casa_guide_version: Some("6.5.4"),
        expected_size_bytes: Some(257_537_974),
        expected_sha256: Some("9cd1b5f9a3bc80a5758e945d1c398e79a64fec9e2d40cad4336edbe7ea787de6"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/automasking/twhya_selfcal.ms.contsub.tar",
    },
    TutorialDataset {
        key: "alma/first-look/twhya/continuum-image",
        source_url: "https://casaguides.nrao.edu/index.php/First_Look_at_Image_Analysis",
        artifact_url: "https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_cont.image",
        expected_filename: "twhya_cont.image",
        casa_guide_version: Some("6.6.6"),
        expected_size_bytes: Some(369_373),
        expected_sha256: None,
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/first-look/twhya/twhya_cont.image",
    },
    TutorialDataset {
        key: "alma/first-look/twhya/n2hp-image",
        source_url: "https://casaguides.nrao.edu/index.php/First_Look_at_Image_Analysis",
        artifact_url: "https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_n2hp.image",
        expected_filename: "twhya_n2hp.image",
        casa_guide_version: Some("6.6.6"),
        expected_size_bytes: Some(3_859_246),
        expected_sha256: None,
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/first-look/twhya/twhya_n2hp.image",
    },
    TutorialDataset {
        key: "alma/antennae/band7/calibrated-data",
        source_url: "https://casaguides.nrao.edu/index.php/AntennaeBand7_Imaging_6.6.6",
        artifact_url: "https://bulk.cv.nrao.edu/almadata/public/casaguides/Antennae_Band7_6.6.6/Antennae_Band7_CalibratedData.tgz",
        expected_filename: "Antennae_Band7_CalibratedData.tgz",
        casa_guide_version: Some("6.6.6"),
        expected_size_bytes: Some(912_711_095),
        expected_sha256: Some("1976fea9239dea06c144c963c3750b03e7c53e82787f0a3c46b72fe17b5df339"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/antennae/band7/Antennae_Band7_CalibratedData.tgz",
    },
    TutorialDataset {
        key: "alma/antennae/band7/reference-images",
        source_url: "https://casaguides.nrao.edu/index.php/AntennaeBand7",
        artifact_url: "https://bulk.cv.nrao.edu/almadata/public/casaguides/Antennae_Band7_6.6.6/Antennae_Band7_ReferenceImages.tgz",
        expected_filename: "Antennae_Band7_ReferenceImages.tgz",
        casa_guide_version: Some("6.6.6"),
        expected_size_bytes: Some(83_981_505),
        expected_sha256: Some("cd52ffdc8f7b18f28ede2be70f6334f2f3f435fe31d7cff66f6e3a446eed2190"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/antennae/band7/Antennae_Band7_ReferenceImages.tgz",
    },
    TutorialDataset {
        key: "alma/m100/band3-combine/12m-calibrated-data",
        source_url: "https://casaguides.nrao.edu/index.php/M100_Band3_Combine_6.6.6",
        artifact_url: "https://almascience.nrao.edu/almadata/sciver/M100Band3_12m/M100_Band3_12m_CalibratedData.tgz",
        expected_filename: "M100_Band3_12m_CalibratedData.tgz",
        casa_guide_version: Some("6.6.6; CASA 5.1.1 calibration"),
        expected_size_bytes: Some(15_580_494_468),
        expected_sha256: None,
        tier: CasaTestDataTier::SlowParity,
        relative_path: "tutorial-parity/alma/m100/band3-combine/raw/M100_Band3_12m_CalibratedData.tgz",
    },
    TutorialDataset {
        key: "alma/m100/band3-combine/7m-calibrated-data",
        source_url: "https://casaguides.nrao.edu/index.php/M100_Band3_Combine_6.6.6",
        artifact_url: "https://almascience.nrao.edu/almadata/sciver/M100Band3ACA/M100_Band3_7m_CalibratedData.tgz",
        expected_filename: "M100_Band3_7m_CalibratedData.tgz",
        casa_guide_version: Some("6.6.6"),
        expected_size_bytes: Some(9_774_558_254),
        expected_sha256: None,
        tier: CasaTestDataTier::SlowParity,
        relative_path: "tutorial-parity/alma/m100/band3-combine/raw/M100_Band3_7m_CalibratedData.tgz",
    },
    TutorialDataset {
        key: "alma/m100/band3-combine/tp-calibrated-data",
        source_url: "https://casaguides.nrao.edu/index.php/M100_Band3_Combine_6.6.6",
        artifact_url: "https://almascience.nrao.edu/almadata/sciver/M100Band3ACA/M100_Band3_TP_CalibratedData_5.1.tgz",
        expected_filename: "M100_Band3_TP_CalibratedData_5.1.tgz",
        casa_guide_version: Some("6.6.6; CASA 5.1 TP archive"),
        expected_size_bytes: Some(14_372_792_248),
        expected_sha256: None,
        tier: CasaTestDataTier::SlowParity,
        relative_path: "tutorial-parity/alma/m100/band3-combine/raw/M100_Band3_TP_CalibratedData_5.1.tgz",
    },
    TutorialDataset {
        key: "alma/m100/band3-combine/aca-reference-images",
        source_url: "https://casaguides.nrao.edu/index.php/M100_Band3_Combine_6.6.6",
        artifact_url: "https://almascience.nrao.edu/almadata/sciver/M100Band3ACA/M100_Band3_ACA_ReferenceImages_5.1.tgz",
        expected_filename: "M100_Band3_ACA_ReferenceImages_5.1.tgz",
        casa_guide_version: Some("5.1 reference images; current guide page"),
        expected_size_bytes: Some(24_775_689),
        expected_sha256: None,
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/m100/band3-combine/raw/M100_Band3_ACA_ReferenceImages_5.1.tgz",
    },
    TutorialDataset {
        key: "alma/m100/band3-combine/reference-images",
        source_url: "https://casaguides.nrao.edu/index.php/M100_Band3_Combine",
        artifact_url: "https://almascience.nrao.edu/almadata/sciver/M100Band3ACA/M100_Band3_DataComb_ReferenceImages_5.1.tgz",
        expected_filename: "M100_Band3_DataComb_ReferenceImages_5.1.tgz",
        casa_guide_version: Some("5.1 reference images; current guide page"),
        expected_size_bytes: Some(411_602_337),
        expected_sha256: Some("04e3e88f1393e93c18eab7fd4a9ae5c57e768dbb8be85259c3006ae9d4c7634b"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/alma/m100/band3-combine/M100_Band3_DataComb_ReferenceImages_5.1.tgz",
    },
    TutorialDataset {
        key: "vla/irc10216/ms-10s",
        source_url: "https://casaguides.nrao.edu/index.php?title=VLA_high_frequency_Spectral_Line_tutorial_-_IRC%2B10216",
        artifact_url: "http://casa.nrao.edu/Data/EVLA/IRC10216/TDRW0001_10s.ms.tgz",
        expected_filename: "TDRW0001_10s.ms.tgz",
        casa_guide_version: None,
        expected_size_bytes: Some(1_068_298_240),
        expected_sha256: Some("96292e62103b51a456e9a6620ffab54ca00785448935122eaf714aa5b21308cb"),
        tier: CasaTestDataTier::SlowParity,
        relative_path: "tutorial-parity/vla/irc10216/TDRW0001_10s.ms.tgz",
    },
    TutorialDataset {
        key: "vla/irc10216/fors1-fits",
        source_url: "https://casaguides.nrao.edu/index.php?title=VLA_high_frequency_Spectral_Line_tutorial_-_IRC%2B10216",
        artifact_url: "http://casa.nrao.edu/Data/EVLA/IRC10216/irc_fors1_dec_header.fits",
        expected_filename: "irc_fors1_dec_header.fits",
        casa_guide_version: None,
        expected_size_bytes: Some(16_784_640),
        expected_sha256: Some("9e476e1f98f63d9d870dfa1d72f6705ca40aed3c006115742a0bb2922cbd8071"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/vla/irc10216/irc_fors1_dec_header.fits",
    },
    TutorialDataset {
        key: "vla/3c391/raw-10s-spw0",
        source_url: "https://casaguides.nrao.edu/index.php?title=VLA_Continuum_Tutorial_3C391-CASA6.4.1",
        artifact_url: "https://casa.nrao.edu/Data/EVLA/3C391/3c391_ctm_mosaic_10s_spw0.ms.tgz",
        expected_filename: "3c391_ctm_mosaic_10s_spw0.ms.tgz",
        casa_guide_version: Some("6.4.1"),
        expected_size_bytes: Some(221_474_816),
        expected_sha256: Some("03e36442d56607f48caa6061713bf138ec38348c1bcb8cce11ddaba16af70f7d"),
        tier: CasaTestDataTier::SlowParity,
        relative_path: "tutorial-parity/vla/3c391/3c391_ctm_mosaic_10s_spw0.ms.tgz",
    },
    TutorialDataset {
        key: "vla/3c391/final-calibrated-mosaic-ms",
        source_url: "https://casaguides.nrao.edu/index.php?title=VLA_Continuum_Tutorial_3C391-CASA6.4.1",
        artifact_url: "https://casa.nrao.edu/Data/EVLA/3C391/EVLA_3C391_FinalCalibratedMosaicMS.tgz",
        expected_filename: "EVLA_3C391_FinalCalibratedMosaicMS.tgz",
        casa_guide_version: Some("6.4.1"),
        expected_size_bytes: Some(1_410_442_215),
        expected_sha256: Some("c9084b2794d4b39ebab17f03a97f9b3a3a61717e8d76118f09c1e2d99e0c5268"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/vla/3c391/EVLA_3C391_FinalCalibratedMosaicMS.tgz",
    },
    TutorialDataset {
        key: "vla/imaging/calibrated-ms",
        source_url: "https://casaguides.nrao.edu/index.php?title=VLA_CASA_Imaging-CASA6.5.4",
        artifact_url: "https://casa.nrao.edu/Data/EVLA/SNRG55/SNR_G55_10s.calib.tar.gz",
        expected_filename: "SNR_G55_10s.calib.tar.gz",
        casa_guide_version: Some("6.5.4"),
        expected_size_bytes: Some(1_250_616_054),
        expected_sha256: Some("b79a63d1142674c89c4c3ae702a28625867728a420a3c156e0ec44078200bf6a"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/vla/imaging/SNR_G55_10s.calib.tar.gz",
    },
    TutorialDataset {
        key: "simulation/vla-ppdisk/model-fits",
        source_url: "https://casaguides.nrao.edu/index.php?title=Protoplanetary_Disk_Simulation_-_VLA-CASA6.7.2",
        artifact_url: "https://casa.nrao.edu/Data/EVLA/simulation/ppdisk672_GHz_50pc.fits",
        expected_filename: "ppdisk672_GHz_50pc.fits",
        casa_guide_version: Some("6.7.2"),
        expected_size_bytes: Some(276_480),
        expected_sha256: Some("e4416bfa0732251d5a7fef48e6c6f9cf8426de264626b63e7ad42fa76faef70e"),
        tier: CasaTestDataTier::TutorialParity,
        relative_path: "tutorial-parity/simulation/vla-ppdisk/ppdisk672_GHz_50pc.fits",
    },
];

/// Resolve the shared `casatestdata` checkout used by CASA parity tests.
///
/// The default-fixture lookup order is:
/// 1. `CASA_RS_TESTDATA_ROOT`
/// 2. `../casatestdata` relative to the repo root
/// 3. `~/SoftwareProjects/casatestdata`
pub fn casatestdata_root() -> Option<PathBuf> {
    casatestdata_root_for_tier(CasaTestDataTier::DefaultFixture)
}

/// Resolve the shared `casatestdata` checkout for a gate tier.
pub fn casatestdata_root_for_tier(tier: CasaTestDataTier) -> Option<PathBuf> {
    if let Some(root) = std::env::var_os("CASA_RS_TESTDATA_ROOT") {
        let path = PathBuf::from(root);
        if path.exists() {
            return Some(normalize_existing_path(&path));
        }
    }

    select_casatestdata_root(
        casatestdata_root_candidates_for_tier(tier),
        &[] as &[PathBuf],
    )
}

/// Resolve the shared `casatestdata` checkout for a gate tier, preferring roots
/// that contain all paths required by that gate.
pub fn casatestdata_root_for_tier_with_required_paths(
    tier: CasaTestDataTier,
    required_paths: &[impl AsRef<Path>],
) -> Option<PathBuf> {
    if let Some(root) = std::env::var_os("CASA_RS_TESTDATA_ROOT") {
        let path = PathBuf::from(root);
        if path.exists() {
            return Some(normalize_existing_path(&path));
        }
    }

    select_casatestdata_root(casatestdata_root_candidates_for_tier(tier), required_paths)
}

/// Resolve a path relative to the shared `casatestdata` checkout.
pub fn casatestdata_path(relative: impl AsRef<Path>) -> Option<PathBuf> {
    casatestdata_root().map(|root| root.join(relative.as_ref()))
}

/// Resolve a path relative to the shared `casatestdata` checkout for a gate tier.
pub fn casatestdata_path_for_tier(
    tier: CasaTestDataTier,
    relative: impl AsRef<Path>,
) -> Option<PathBuf> {
    let relative = relative.as_ref();
    if let Some(root) = std::env::var_os("CASA_RS_TESTDATA_ROOT") {
        let path = PathBuf::from(root);
        if path.exists() {
            return Some(normalize_existing_path(&path).join(relative));
        }
    }

    let mut first_existing = None;
    for candidate in casatestdata_root_candidates_for_tier(tier) {
        if !candidate.exists() {
            continue;
        }
        let root = normalize_existing_path(&candidate);
        let path = root.join(relative);
        if path.exists() {
            return Some(path);
        }
        first_existing.get_or_insert(path);
    }
    first_existing
}

/// Resolve the local CASA tutorial data mirror used by tutorial-registry keys.
///
/// This stays separate from the external C++ `casatestdata` checkout.
pub fn casa_tutorial_data_root() -> Option<PathBuf> {
    if let Some(root) = std::env::var_os("CASA_RS_TUTORIAL_DATA_ROOT") {
        let path = PathBuf::from(root);
        if path.exists() {
            return Some(normalize_existing_path(&path));
        }
    }

    let home = std::env::var_os("HOME").map(PathBuf::from)?;
    let candidate = home.join("SoftwareProjects/casa-tutorial-data");
    if candidate.exists() {
        Some(normalize_existing_path(&candidate))
    } else {
        None
    }
}

/// Resolve a tutorial registry entry by key.
pub fn tutorial_dataset(key: &str) -> Option<&'static TutorialDataset> {
    TUTORIAL_DATASETS.iter().find(|dataset| dataset.key == key)
}

/// Iterate tutorial registry entries that belong to a gate tier.
pub fn tutorial_datasets_for_tier(
    tier: CasaTestDataTier,
) -> impl Iterator<Item = &'static TutorialDataset> {
    TUTORIAL_DATASETS
        .iter()
        .filter(move |dataset| dataset.tier == tier)
}

/// Resolve a tutorial registry entry to its expected local path.
pub fn tutorial_dataset_path(key: &str) -> Option<PathBuf> {
    let dataset = tutorial_dataset(key)?;
    casa_tutorial_data_root().map(|root| root.join(dataset.relative_path))
}

/// Resolved local CASA Python environment used by opt-in parity tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasaPython {
    /// Python interpreter path.
    pub program: PathBuf,
    /// Whether the environment exposes `plotms`.
    pub plotms_available: bool,
    /// Whether the environment exposes `tclean`.
    pub tclean_available: bool,
}

/// Discover a CASA-capable Python interpreter.
pub fn discover_casa_python() -> Option<CasaPython> {
    casa_python_candidates()
        .into_iter()
        .find_map(probe_casa_python)
}

/// Resolve the local CASA source checkout used as an implementation reference.
pub fn casa_source_root() -> Option<PathBuf> {
    existing_path_from_candidates([
        std::env::var_os("CASA_RS_CASA_ROOT").map(PathBuf::from),
        std::env::var_os("CASA_ROOT").map(PathBuf::from),
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join("SoftwareProjects").join("casa")),
    ])
}

/// Resolve the local casacore source checkout used as an implementation reference.
pub fn casacore_source_root() -> Option<PathBuf> {
    existing_path_from_candidates([
        std::env::var_os("CASA_RS_CASACORE_ROOT").map(PathBuf::from),
        std::env::var_os("CASACORE_ROOT").map(PathBuf::from),
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join("SoftwareProjects").join("casacore")),
    ])
}

/// Return `git rev-parse HEAD` for a local source tree when available.
pub fn git_head_commit(path: &Path) -> Option<String> {
    Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|stdout| stdout.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
pub(crate) fn casatestdata_root_candidates() -> Vec<PathBuf> {
    casatestdata_root_candidates_for_tier(CasaTestDataTier::DefaultFixture)
}

pub(crate) fn casatestdata_root_candidates_for_tier(tier: CasaTestDataTier) -> Vec<PathBuf> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root");
    let mut candidates = vec![repo_root.join("../casatestdata")];
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join("SoftwareProjects")
                .join("casatestdata"),
        );
    }
    if matches!(
        tier,
        CasaTestDataTier::TutorialParity | CasaTestDataTier::SlowParity
    ) {
        candidates.push(PathBuf::from("/Volumes/home/casatestdata"));
    }
    candidates
}

pub(crate) fn select_casatestdata_root(
    candidates: Vec<PathBuf>,
    required_paths: &[impl AsRef<Path>],
) -> Option<PathBuf> {
    let mut first_existing = None;
    for candidate in candidates {
        if !candidate.exists() {
            continue;
        }
        let root = normalize_existing_path(&candidate);
        first_existing.get_or_insert_with(|| root.clone());
        if required_paths
            .iter()
            .all(|relative| root.join(relative.as_ref()).exists())
        {
            return Some(root);
        }
    }
    first_existing
}

pub(crate) fn normalize_existing_path(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

pub(crate) fn casa_python_candidates() -> Vec<PathBuf> {
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
    dedup_paths(candidates)
}

pub(crate) fn probe_casa_python(program: PathBuf) -> Option<CasaPython> {
    if !python_can_import(&program, "casatasks") {
        return None;
    }
    Some(CasaPython {
        plotms_available: python_has_callable(&program, "plotms"),
        tclean_available: python_has_callable(&program, "tclean"),
        program,
    })
}

pub(crate) fn python_can_import(program: &Path, module: &str) -> bool {
    Command::new(program)
        .arg("-c")
        .arg(format!("import {module}"))
        .output()
        .is_ok_and(|output| output.status.success())
}

pub(crate) fn python_has_callable(program: &Path, attribute: &str) -> bool {
    let script = if attribute == "plotms" {
        "import importlib.util\nimport casatasks\nok = hasattr(casatasks, 'plotms') or importlib.util.find_spec('casaplotms') is not None\nprint('1' if ok else '0')\n".to_string()
    } else {
        format!(
            "import casatasks\nok = hasattr(casatasks, {attribute:?})\nprint('1' if ok else '0')\n"
        )
    };
    Command::new(program)
        .arg("-c")
        .arg(script)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "1")
}

pub(crate) fn existing_path_from_candidates<const N: usize>(
    candidates: [Option<PathBuf>; N],
) -> Option<PathBuf> {
    candidates
        .into_iter()
        .flatten()
        .find(|path| path.exists())
        .map(|path| normalize_existing_path(&path))
}

pub(crate) fn dedup_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut unique = Vec::new();
    for path in paths {
        if !unique.iter().any(|candidate| candidate == &path) {
            unique.push(path);
        }
    }
    unique
}
