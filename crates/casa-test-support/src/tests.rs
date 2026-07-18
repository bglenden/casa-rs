// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_aipsio::{ArrayValue, ByteOrder, ScalarValue, TypeTag, Value};
use ndarray::{ArrayD, IxDyn};
use std::path::{Path, PathBuf};
use std::process::Command;

use super::*;
use casa_types::{PrimitiveType, RecordValue};
use std::sync::Mutex;
use tempfile::tempdir;

static TEST_ENV_LOCK: Mutex<()> = Mutex::new(());

unsafe fn restore_env_var(key: &str, value: Option<std::ffi::OsString>) {
    match value {
        Some(value) => unsafe { std::env::set_var(key, value) },
        None => unsafe { std::env::remove_var(key) },
    }
}

#[test]
fn primitive_case_set_is_non_empty() {
    let values = primitive_cross_check_values();
    assert!(!values.is_empty());
    assert!(values.iter().all(|v| v.type_tag().is_some()));
}

#[test]
fn multidimensional_cases_use_fortran_linearization() {
    let original = Value::Array(ArrayValue::Int32(
        ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![0, 1, 2, 3, 4, 5]).expect("shape"),
    ));

    let case = prepare_primitive_case(&original).expect("prepare case");
    let Value::Array(ArrayValue::Int32(flattened)) = case.wire_value.clone() else {
        panic!("expected int32 array wire case");
    };
    let flattened_vec: Vec<i32> = flattened.iter().copied().collect();
    assert_eq!(flattened_vec, vec![0, 3, 1, 4, 2, 5]);

    let restored =
        restore_decoded_shape(case.wire_value, case.original_shape.as_deref()).expect("restore");
    assert_eq!(restored, original);
}

#[test]
fn rust_backend_round_trip_for_primitive_cases() {
    let backend = RustBackend::new();
    for value in primitive_cross_check_values() {
        let case = prepare_primitive_case(&value).expect("case should be supported");
        let tag = case.wire_value.type_tag().expect("primitive case has tag");
        for order in [ByteOrder::BigEndian, ByteOrder::LittleEndian] {
            let wire = backend
                .encode_value(&case.wire_value, order)
                .expect("rust encode should succeed");
            let decoded = backend
                .decode_value(&wire, tag, order)
                .expect("rust decode should succeed");
            let decoded = restore_decoded_shape(decoded, case.original_shape.as_deref())
                .expect("restore decoded shape");
            assert_eq!(decoded, case.expected_value);
        }
    }
}

#[test]
fn primitive_case_preparation_rejects_non_primitive_values() {
    let record = Value::Record(RecordValue::default());
    let table_ref = Value::TableRef("ANTENNA".to_string());

    assert!(matches!(
        prepare_primitive_case(&record),
        Err(AipsIoCrossError::UnsupportedValue(message))
            if message.contains("record values")
    ));
    assert!(matches!(
        prepare_primitive_case(&table_ref),
        Err(AipsIoCrossError::UnsupportedValue(message))
            if message.contains("table references")
    ));
}

#[test]
fn restore_decoded_shape_requires_array_when_shape_is_present() {
    let decoded = Value::Scalar(ScalarValue::Int32(42));
    assert!(matches!(
        restore_decoded_shape(decoded, Some(&[2, 2])),
        Err(AipsIoCrossError::UnsupportedValue(message))
            if message.contains("expected to be an array")
    ));
}

#[test]
fn casatestdata_candidates_include_repo_sibling_and_home_workspace_fallback() {
    let candidates = casatestdata_root_candidates();
    assert!(
        candidates
            .iter()
            .any(|path| path.ends_with("../casatestdata"))
    );
    if let Some(home) = std::env::var_os("HOME") {
        let home_candidate = PathBuf::from(home)
            .join("SoftwareProjects")
            .join("casatestdata");
        assert!(candidates.iter().any(|path| path == &home_candidate));
    }
}

#[test]
fn long_gate_candidates_include_shared_volume_without_defaulting_to_it() {
    let default_candidates =
        casatestdata_root_candidates_for_tier(CasaTestDataTier::DefaultFixture);
    assert!(
        !default_candidates
            .iter()
            .any(|path| path == Path::new("/Volumes/home/casatestdata"))
    );

    let slow_candidates = casatestdata_root_candidates_for_tier(CasaTestDataTier::SlowParity);
    assert!(
        slow_candidates
            .iter()
            .any(|path| path == Path::new("/Volumes/home/casatestdata"))
    );
}

#[test]
fn long_gate_required_path_selection_skips_incomplete_earlier_root() {
    let dir = tempdir().unwrap();
    let incomplete = dir.path().join("incomplete");
    let complete = dir.path().join("complete");
    std::fs::create_dir_all(&incomplete).unwrap();
    std::fs::create_dir_all(complete.join("measurementset/vla")).unwrap();
    std::fs::write(
        complete.join("measurementset/vla/refim_point.ms"),
        b"fixture",
    )
    .unwrap();

    let selected = select_casatestdata_root(
        vec![incomplete, complete.clone()],
        &[Path::new("measurementset/vla/refim_point.ms")],
    );
    assert_eq!(selected, Some(normalize_existing_path(&complete)));
}

#[test]
fn long_gate_path_lookup_falls_through_to_later_root_with_file() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("casatestdata");
    std::fs::create_dir_all(root.join("unittest/tclean")).unwrap();
    std::fs::write(root.join("unittest/tclean/refim_twochan.ms"), b"fixture").unwrap();

    let selected = select_casatestdata_root(
        vec![dir.path().join("empty"), root.clone()],
        &[Path::new("unittest/tclean/refim_twochan.ms")],
    )
    .expect("selected root");
    assert_eq!(
        selected.join("unittest/tclean/refim_twochan.ms"),
        normalize_existing_path(&root).join("unittest/tclean/refim_twochan.ms")
    );
}

#[test]
fn normalize_existing_path_preserves_missing_paths() {
    let dir = tempdir().unwrap();
    let missing = dir.path().join("missing");
    assert_eq!(normalize_existing_path(&missing), missing);
}

#[test]
fn casatestdata_env_override_and_path_join_are_honored() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("casatestdata");
    std::fs::create_dir(&root).unwrap();

    let _guard = TEST_ENV_LOCK.lock().unwrap();
    let old_root = std::env::var_os("CASA_RS_TESTDATA_ROOT");
    unsafe { std::env::set_var("CASA_RS_TESTDATA_ROOT", &root) };
    assert_eq!(casatestdata_root(), Some(normalize_existing_path(&root)));
    assert_eq!(
        casatestdata_path("measurementset/demo.ms"),
        Some(normalize_existing_path(&root).join("measurementset/demo.ms"))
    );
    unsafe { restore_env_var("CASA_RS_TESTDATA_ROOT", old_root) };
}

#[test]
fn tutorial_dataset_path_uses_separate_tutorial_root() {
    let dir = tempdir().unwrap();
    let tutorial_root = dir.path().join("casa-tutorial-data");
    std::fs::create_dir(&tutorial_root).unwrap();

    let _guard = TEST_ENV_LOCK.lock().unwrap();
    let old_tutorial_root = std::env::var_os("CASA_RS_TUTORIAL_DATA_ROOT");
    unsafe { std::env::set_var("CASA_RS_TUTORIAL_DATA_ROOT", &tutorial_root) };
    let path = tutorial_dataset_path("alma/first-look/twhya/calibrated-ms").unwrap();
    assert_eq!(
        path,
        normalize_existing_path(&tutorial_root)
            .join("tutorial-parity/alma/first-look/twhya/twhya_calibrated.ms.tar")
    );
    unsafe { restore_env_var("CASA_RS_TUTORIAL_DATA_ROOT", old_tutorial_root) };
}

#[test]
fn tutorial_data_root_uses_home_workspace_fallback_and_reports_missing() {
    let dir = tempdir().unwrap();
    let tutorial_root = dir
        .path()
        .join("SoftwareProjects")
        .join("casa-tutorial-data");
    std::fs::create_dir_all(&tutorial_root).unwrap();

    let _guard = TEST_ENV_LOCK.lock().unwrap();
    let old_tutorial_root = std::env::var_os("CASA_RS_TUTORIAL_DATA_ROOT");
    let old_home = std::env::var_os("HOME");
    unsafe {
        std::env::remove_var("CASA_RS_TUTORIAL_DATA_ROOT");
        std::env::set_var("HOME", dir.path());
    }

    assert_eq!(
        casa_tutorial_data_root(),
        Some(normalize_existing_path(&tutorial_root))
    );
    assert_eq!(
        tutorial_dataset_path("simulation/vla-ppdisk/model-fits"),
        Some(
            normalize_existing_path(&tutorial_root)
                .join("tutorial-parity/simulation/vla-ppdisk/ppdisk672_GHz_50pc.fits")
        )
    );

    std::fs::remove_dir(&tutorial_root).unwrap();
    assert_eq!(casa_tutorial_data_root(), None);

    unsafe {
        restore_env_var("CASA_RS_TUTORIAL_DATA_ROOT", old_tutorial_root);
        restore_env_var("HOME", old_home);
    }
}

#[test]
fn tutorial_dataset_registry_contains_first_wave_candidates() {
    let twhya = tutorial_dataset("alma/first-look/twhya/calibrated-ms").unwrap();
    assert_eq!(twhya.expected_filename, "twhya_calibrated.ms.tar");
    assert_eq!(twhya.expected_size_bytes, Some(435_742_720));
    assert_eq!(
        twhya.expected_sha256,
        Some("f0cfeee5b9dec09ac9ed4d3e4e048d5eb28023c11cbc8295c09ddefe6b8a97b2")
    );
    assert_eq!(twhya.tier, CasaTestDataTier::TutorialParity);
    assert!(twhya.relative_path.starts_with("tutorial-parity/"));

    let irc10216 = tutorial_dataset("vla/irc10216/ms-10s").unwrap();
    assert_eq!(irc10216.expected_size_bytes, Some(1_068_298_240));
    assert_eq!(irc10216.tier, CasaTestDataTier::SlowParity);

    let c391_final = tutorial_dataset("vla/3c391/final-calibrated-mosaic-ms").unwrap();
    assert_eq!(
        c391_final.expected_filename,
        "EVLA_3C391_FinalCalibratedMosaicMS.tgz"
    );
    assert_eq!(c391_final.expected_size_bytes, Some(1_410_442_215));
    assert_eq!(c391_final.tier, CasaTestDataTier::TutorialParity);

    let c391_raw = tutorial_dataset("vla/3c391/raw-10s-spw0").unwrap();
    assert_eq!(c391_raw.expected_size_bytes, Some(221_474_816));
    assert_eq!(c391_raw.tier, CasaTestDataTier::SlowParity);

    let m100_reference = tutorial_dataset("alma/m100/band3-combine/reference-images").unwrap();
    assert_eq!(
        m100_reference.expected_filename,
        "M100_Band3_DataComb_ReferenceImages_5.1.tgz"
    );
    assert_eq!(m100_reference.expected_size_bytes, Some(411_602_337));
    assert_eq!(m100_reference.tier, CasaTestDataTier::TutorialParity);

    let m100_12m = tutorial_dataset("alma/m100/band3-combine/12m-calibrated-data").unwrap();
    assert_eq!(m100_12m.expected_size_bytes, Some(15_580_494_468));
    assert_eq!(m100_12m.tier, CasaTestDataTier::SlowParity);

    let m100_7m = tutorial_dataset("alma/m100/band3-combine/7m-calibrated-data").unwrap();
    assert_eq!(m100_7m.expected_size_bytes, Some(9_774_558_254));
    assert_eq!(m100_7m.tier, CasaTestDataTier::SlowParity);

    let m100_tp = tutorial_dataset("alma/m100/band3-combine/tp-calibrated-data").unwrap();
    assert_eq!(m100_tp.expected_size_bytes, Some(14_372_792_248));
    assert_eq!(m100_tp.tier, CasaTestDataTier::SlowParity);

    let tutorial_keys = tutorial_datasets_for_tier(CasaTestDataTier::TutorialParity)
        .map(|dataset| dataset.key)
        .collect::<Vec<_>>();
    assert!(tutorial_keys.contains(&"alma/first-look/twhya/calibrated-ms"));
    assert!(tutorial_keys.contains(&"alma/automasking/contsub-ms"));
    assert!(tutorial_keys.contains(&"alma/antennae/band7/calibrated-data"));
    assert!(tutorial_keys.contains(&"alma/antennae/band7/reference-images"));
    assert!(tutorial_keys.contains(&"alma/m100/band3-combine/aca-reference-images"));
    assert!(tutorial_keys.contains(&"alma/m100/band3-combine/reference-images"));
    assert!(tutorial_keys.contains(&"vla/irc10216/fors1-fits"));
    assert!(tutorial_keys.contains(&"vla/3c391/final-calibrated-mosaic-ms"));
    assert!(!tutorial_keys.contains(&"alma/m100/band3-combine/12m-calibrated-data"));
    assert!(!tutorial_keys.contains(&"alma/m100/band3-combine/7m-calibrated-data"));
    assert!(!tutorial_keys.contains(&"alma/m100/band3-combine/tp-calibrated-data"));
    assert!(!tutorial_keys.contains(&"vla/irc10216/ms-10s"));
    assert!(!tutorial_keys.contains(&"vla/3c391/raw-10s-spw0"));

    let antennae = tutorial_dataset("alma/antennae/band7/calibrated-data").unwrap();
    assert_eq!(antennae.expected_size_bytes, Some(912_711_095));
    assert_eq!(antennae.casa_guide_version, Some("6.6.6"));

    let automasking = tutorial_dataset("alma/automasking/contsub-ms").unwrap();
    assert_eq!(
        automasking.expected_filename,
        "twhya_selfcal.ms.contsub.tar"
    );
    assert_eq!(automasking.expected_size_bytes, Some(257_537_974));
    assert_eq!(
        automasking.expected_sha256,
        Some("9cd1b5f9a3bc80a5758e945d1c398e79a64fec9e2d40cad4336edbe7ea787de6")
    );

    let ppdisk = tutorial_dataset("simulation/vla-ppdisk/model-fits").unwrap();
    assert_eq!(ppdisk.casa_guide_version, Some("6.7.2"));

    assert_eq!(CasaTestDataTier::DefaultFixture.as_str(), "default-fixture");
    assert_eq!(CasaTestDataTier::TutorialParity.as_str(), "tutorial-parity");
    assert_eq!(CasaTestDataTier::SlowParity.as_str(), "slow-parity");
    assert!(tutorial_dataset("missing/key").is_none());
}

#[test]
fn source_root_discovery_prefers_existing_env_over_fallbacks() {
    let dir = tempdir().unwrap();
    let casa_root = dir.path().join("casa-src");
    let casacore_root = dir.path().join("casacore-src");
    std::fs::create_dir(&casa_root).unwrap();
    std::fs::create_dir(&casacore_root).unwrap();

    let _guard = TEST_ENV_LOCK.lock().unwrap();
    unsafe {
        std::env::set_var("CASA_RS_CASA_ROOT", &casa_root);
        std::env::set_var("CASA_RS_CASACORE_ROOT", &casacore_root);
        std::env::set_var("CASA_ROOT", dir.path().join("ignored-casa"));
        std::env::set_var("CASACORE_ROOT", dir.path().join("ignored-casacore"));
    }

    assert_eq!(
        casa_source_root(),
        Some(normalize_existing_path(&casa_root))
    );
    assert_eq!(
        casacore_source_root(),
        Some(normalize_existing_path(&casacore_root))
    );

    unsafe {
        std::env::remove_var("CASA_RS_CASA_ROOT");
        std::env::remove_var("CASA_RS_CASACORE_ROOT");
        std::env::remove_var("CASA_ROOT");
        std::env::remove_var("CASACORE_ROOT");
    }
}

#[test]
fn source_root_discovery_uses_legacy_env_home_fallback_and_deduplicates_candidates() {
    let dir = tempdir().unwrap();
    let casa_root = dir.path().join("legacy-casa");
    let casacore_root = dir.path().join("legacy-casacore");
    let home_casa = dir.path().join("SoftwareProjects").join("casa");
    let home_casacore = dir.path().join("SoftwareProjects").join("casacore");
    std::fs::create_dir_all(&home_casa).unwrap();
    std::fs::create_dir_all(&home_casacore).unwrap();
    std::fs::create_dir(&casa_root).unwrap();
    std::fs::create_dir(&casacore_root).unwrap();

    let _guard = TEST_ENV_LOCK.lock().unwrap();
    let old_casa_rs_root = std::env::var_os("CASA_RS_CASA_ROOT");
    let old_casa_root = std::env::var_os("CASA_ROOT");
    let old_casacore_rs_root = std::env::var_os("CASA_RS_CASACORE_ROOT");
    let old_casacore_root = std::env::var_os("CASACORE_ROOT");
    let old_home = std::env::var_os("HOME");
    unsafe {
        std::env::remove_var("CASA_RS_CASA_ROOT");
        std::env::remove_var("CASA_RS_CASACORE_ROOT");
        std::env::set_var("CASA_ROOT", &casa_root);
        std::env::set_var("CASACORE_ROOT", &casacore_root);
        std::env::set_var("HOME", dir.path());
    }

    assert_eq!(
        casa_source_root(),
        Some(normalize_existing_path(&casa_root))
    );
    assert_eq!(
        casacore_source_root(),
        Some(normalize_existing_path(&casacore_root))
    );

    unsafe {
        std::env::remove_var("CASA_ROOT");
        std::env::remove_var("CASACORE_ROOT");
    }
    assert_eq!(
        casa_source_root(),
        Some(normalize_existing_path(&home_casa))
    );
    assert_eq!(
        casacore_source_root(),
        Some(normalize_existing_path(&home_casacore))
    );

    let duplicated = vec![
        PathBuf::from("python3"),
        PathBuf::from("python"),
        PathBuf::from("python3"),
    ];
    assert_eq!(
        dedup_paths(duplicated),
        vec![PathBuf::from("python3"), PathBuf::from("python")]
    );
    assert_eq!(
        existing_path_from_candidates([None, Some(home_casa.clone())]),
        Some(normalize_existing_path(&home_casa))
    );

    unsafe {
        restore_env_var("CASA_RS_CASA_ROOT", old_casa_rs_root);
        restore_env_var("CASA_ROOT", old_casa_root);
        restore_env_var("CASA_RS_CASACORE_ROOT", old_casacore_rs_root);
        restore_env_var("CASACORE_ROOT", old_casacore_root);
        restore_env_var("HOME", old_home);
    }
}

#[test]
fn casa_python_discovery_uses_env_candidates_and_callable_probes() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let fake_python = dir.path().join("python");
        std::fs::write(
            &fake_python,
            "#!/bin/sh\nscript=\"$2\"\ncase \"$script\" in\n  *\"import casatasks\"*\"plotms\"*) echo 1; exit 0 ;;\n  *\"import casatasks\"*\"tclean\"*) echo 1; exit 0 ;;\n  *\"import casatasks\"*) exit 0 ;;\nesac\nexit 1\n",
        )
        .unwrap();
        let mut perms = std::fs::metadata(&fake_python).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&fake_python, perms).unwrap();

        let _guard = TEST_ENV_LOCK.lock().unwrap();
        let old_casa_rs_python = std::env::var_os("CASA_RS_CASA_PYTHON");
        let old_casa_python = std::env::var_os("CASA_PYTHON");
        let old_home = std::env::var_os("HOME");
        unsafe {
            std::env::set_var("CASA_RS_CASA_PYTHON", &fake_python);
            std::env::set_var("CASA_PYTHON", &fake_python);
            std::env::set_var("HOME", dir.path());
        }

        let candidates = casa_python_candidates();
        assert_eq!(candidates.first(), Some(&fake_python));
        assert_eq!(
            candidates
                .iter()
                .filter(|path| *path == &fake_python)
                .count(),
            1
        );
        assert!(python_can_import(&fake_python, "casatasks"));
        assert!(!python_can_import(&fake_python, "not_casa"));
        assert!(python_has_callable(&fake_python, "plotms"));
        assert!(python_has_callable(&fake_python, "tclean"));

        let discovered = discover_casa_python().unwrap();
        assert_eq!(discovered.program, fake_python);
        assert!(discovered.plotms_available);
        assert!(discovered.tclean_available);

        unsafe {
            restore_env_var("CASA_RS_CASA_PYTHON", old_casa_rs_python);
            restore_env_var("CASA_PYTHON", old_casa_python);
            restore_env_var("HOME", old_home);
        }
    }
}

#[test]
fn git_head_commit_reports_only_successful_nonempty_revisions() {
    let dir = tempdir().unwrap();
    assert_eq!(git_head_commit(dir.path()), None);

    let repo = dir.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(&repo)
            .arg("init")
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["config", "user.email", "codex@example.invalid"])
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["config", "user.name", "Codex"])
            .status()
            .unwrap()
            .success()
    );
    std::fs::write(repo.join("README.md"), "fixture\n").unwrap();
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["add", "README.md"])
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["commit", "-m", "fixture"])
            .status()
            .unwrap()
            .success()
    );

    let head = git_head_commit(&repo).unwrap();
    assert_eq!(head.len(), 40);
    assert!(head.chars().all(|ch| ch.is_ascii_hexdigit()));
}

#[test]
fn cpp_backend_matches_build_configuration() {
    assert_eq!(casacore_oracle_available(), cfg!(has_casacore_cpp));

    let value = Value::Scalar(ScalarValue::Int32(7));
    let rust_backend = RustBackend::new();
    assert!(
        rust_backend
            .encode_value(&value, ByteOrder::BigEndian)
            .is_ok()
    );

    let cpp_backend = CppBackend::new();
    let encoded = cpp_backend.encode_value(&value, ByteOrder::BigEndian);
    let decoded = cpp_backend.decode_value(
        &[0, 0, 0, 7],
        TypeTag::scalar(PrimitiveType::Int32),
        ByteOrder::BigEndian,
    );

    if cfg!(has_casacore_cpp) {
        assert!(encoded.is_ok());
        assert!(decoded.is_ok());
    } else {
        assert_eq!(rust_backend.name(), "rust");
        assert_eq!(cpp_backend.name(), "cpp");
        assert!(
            encoded
                .unwrap_err()
                .contains("casacore C++ backend unavailable")
        );
        assert!(
            decoded
                .unwrap_err()
                .contains("casacore C++ backend unavailable")
        );
    }
}

#[test]
fn flatten_and_reshape_cover_all_array_variants() {
    use casa_types::{Complex32, Complex64};

    let cases = vec![
        ArrayValue::Bool(
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap(),
        ),
        ArrayValue::UInt8(ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1u8, 2, 3, 4]).unwrap()),
        ArrayValue::UInt16(ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1u16, 2, 3, 4]).unwrap()),
        ArrayValue::UInt32(ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1u32, 2, 3, 4]).unwrap()),
        ArrayValue::Int16(ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1i16, 2, 3, 4]).unwrap()),
        ArrayValue::Int32(ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1i32, 2, 3, 4]).unwrap()),
        ArrayValue::Int64(ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1i64, 2, 3, 4]).unwrap()),
        ArrayValue::Float32(
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap(),
        ),
        ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f64, 2.0, 3.0, 4.0]).unwrap(),
        ),
        ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2]),
                vec![
                    Complex32::new(1.0, 0.5),
                    Complex32::new(2.0, 0.5),
                    Complex32::new(3.0, 0.5),
                    Complex32::new(4.0, 0.5),
                ],
            )
            .unwrap(),
        ),
        ArrayValue::Complex64(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2]),
                vec![
                    Complex64::new(1.0, 0.5),
                    Complex64::new(2.0, 0.5),
                    Complex64::new(3.0, 0.5),
                    Complex64::new(4.0, 0.5),
                ],
            )
            .unwrap(),
        ),
        ArrayValue::String(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2]),
                vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                    "d".to_string(),
                ],
            )
            .unwrap(),
        ),
    ];

    for case in cases {
        let flattened = flatten_array_value_fortran(&case);
        assert_eq!(flattened.ndim(), 1);
        let reshaped = reshape_array_value_from_fortran(flattened, &[2, 2]).unwrap();
        assert_eq!(reshaped, case);
    }
}

#[test]
fn reshape_helpers_validate_lengths_and_index_conversions() {
    let err = reshape_from_fortran(&[1u8, 2u8, 3u8], &[2, 2]).unwrap_err();
    assert!(
        matches!(err, AipsIoCrossError::UnsupportedValue(message) if message.contains("length"))
    );

    assert_eq!(unravel_fortran_index(4, &[2, 3]), vec![0, 2]);
    assert_eq!(unravel_c_index(4, &[2, 3]), vec![1, 1]);
    assert_eq!(ravel_fortran_index(&[1, 2], &[2, 3]), 5);
    assert_eq!(
        flatten_ndarray_fortran(
            &ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![0, 1, 2, 3, 4, 5]).unwrap()
        ),
        vec![0, 3, 1, 4, 2, 5]
    );
}

#[cfg(not(has_casacore_cpp))]
fn assert_cpp_unavailable<T>(result: Result<T, OracleError>) {
    match result {
        Err(OracleError::Unavailable { .. }) => {}
        Err(error) => panic!("expected unavailable C++ backend error, got {error}"),
        Ok(_) => panic!("expected unavailable C++ backend error"),
    }
}

#[cfg(not(has_casacore_cpp))]
#[test]
fn cpp_unavailable_fallbacks_are_explicit_for_image_and_lattice_helpers() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("fixture.image");
    let shape = [2, 2];
    let tile = [1, 1];
    let data_f32 = [1.0f32, 2.0, 3.0, 4.0];
    let data_f64 = [1.0f64, 2.0, 3.0, 4.0];
    let data_c32 = [Complex32::new(1.0, 0.5), Complex32::new(2.0, -0.5)];
    let data_c64 = [Complex64::new(1.0, 0.5), Complex64::new(2.0, -0.5)];
    let cell_slice = CellSliceBenchParams {
        nrows: 4,
        dim0: 4,
        dim1: 4,
        slice_start0: 1,
        slice_start1: 1,
        slice_end0: 3,
        slice_end1: 3,
    };

    assert_cpp_unavailable(TableOracle::table_write(
        CppTableFixture::ScalarPrimitives,
        &path,
    ));
    assert_cpp_unavailable(TableOracle::table_verify(
        CppTableFixture::ScalarPrimitives,
        &path,
    ));
    assert_cpp_unavailable(TableOracle::columns_index_time_lookups(&path, 1, 4));
    assert_cpp_unavailable(TableOracle::vararray_bench(&path, 4));
    assert_cpp_unavailable(TableOracle::set_algebra_bench(&path, 8, 3, 5));
    assert_cpp_unavailable(TableOracle::copy_rows_bench(&path, 8));
    assert_cpp_unavailable(TableOracle::cell_slice_bench(&path, &cell_slice));
    assert_cpp_unavailable(TableOracle::bulk_scalar_io_bench(&path, 8));
    assert_cpp_unavailable(TableOracle::deep_copy_bench(&path, 8));
    assert_cpp_unavailable(ImageOracle::create_image(&path, &shape, &data_f32, "Jy"));
    assert_cpp_unavailable(ImageOracle::create_image_tiled(
        &path, &shape, &tile, &data_f32, "Jy",
    ));
    assert_cpp_unavailable(ImageOracle::read_image_data(&path, 16));
    assert_cpp_unavailable(ImageOracle::create_image_f64(
        &path, &shape, &data_f64, "Jy",
    ));
    assert_cpp_unavailable(ImageOracle::read_image_data_f64(&path, 16));
    assert_cpp_unavailable(ImageOracle::create_image_complex32(
        &path, &shape, &data_c32, "Jy",
    ));
    assert_cpp_unavailable(ImageOracle::read_image_data_complex32(&path, 16));
    assert_cpp_unavailable(ImageOracle::create_image_complex64(
        &path, &shape, &data_c64, "Jy",
    ));
    assert_cpp_unavailable(ImageOracle::read_image_data_complex64(&path, 16));
    assert_cpp_unavailable(ImageOracle::read_image_shape(&path));
    assert_cpp_unavailable(ImageOracle::read_image_units(&path));
    assert_cpp_unavailable(ImageOracle::create_temp_image_materialized(
        &path,
        &shape,
        &data_f32,
        "Jy",
        "obj",
        "Intensity",
    ));
    assert_cpp_unavailable(ImageOracle::read_image_coordinate_count(&path));
    assert_cpp_unavailable(ImageOracle::read_image_default_mask_name(&path));
    assert_cpp_unavailable(ImageOracle::read_image_default_mask(&path, 16));
    assert_cpp_unavailable(ImageOracle::read_image_info_object_name(&path));
    assert_cpp_unavailable(ImageOracle::read_image_info_type(&path));
    assert_cpp_unavailable(ImageOracle::read_image_slice(&path, &[0, 0], &[1, 2]));
    assert_cpp_unavailable(ImageOracle::eval_image_expr_unary(
        &path,
        CppImageExprUnaryOp::Exp,
        16,
    ));
    assert_cpp_unavailable(ImageOracle::eval_image_expr_binary(
        &path,
        &path,
        CppImageExprBinaryOp::Add,
        16,
    ));
    assert_cpp_unavailable(ImageOracle::eval_image_expr_scalar(
        &path,
        2.0,
        CppImageExprBinaryOp::Multiply,
        16,
    ));
    assert_cpp_unavailable(ImageOracle::eval_image_mask_range(
        &path,
        CppImageExprCompareOp::GreaterThan,
        1.0,
        CppMaskLogicalOp::And,
        CppImageExprCompareOp::LessEqual,
        4.0,
        16,
    ));
    assert_cpp_unavailable(ImageOracle::eval_image_expr_closeout_slice(
        &path,
        &[0, 0],
        &[1, 2],
    ));
    assert_cpp_unavailable(ImageOracle::eval_lel_expr("1+2", 8));
    assert_cpp_unavailable(ImageOracle::profile_lel_scalar_expr("1+2", 2));
    assert_cpp_unavailable(ImageOracle::eval_lel_expr_mask("1>0", 8));
    assert_cpp_unavailable(ImageOracle::save_lel_expr_file("1+2", &path));
    assert_cpp_unavailable(ImageOracle::open_lel_expr_file(&path, 8));
    assert_cpp_unavailable(ImageOracle::bench_image_plane_by_plane(
        &path, &shape, &tile, 0,
    ));
    assert_cpp_unavailable(ImageOracle::bench_image_spectrum_by_spectrum(
        &path, &shape, &tile, 0,
    ));
    assert_cpp_unavailable(ImageOracle::bench_image_plane_by_plane_complex(
        &path, &shape, &tile, 0,
    ));
    assert_cpp_unavailable(LatticeOracle::lattice_statistics_forced_io_bench(
        &path,
        &[2, 2, 2],
        &[1, 1, 1],
        1,
    ));
    assert_cpp_unavailable(LatticeOracle::lattice_statistics_forced_io_repeated_basic(
        &path,
        &[2, 2, 2],
        &[1, 1, 1],
        1,
        2,
    ));
}
