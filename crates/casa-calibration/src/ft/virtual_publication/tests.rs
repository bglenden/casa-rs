// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::Path;

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_core::measures::{direction::DirectionRef, frequency::FrequencyRef};
use casa_core::{ArrayValue, PrimitiveType, ScalarValue, Value};
use casa_images::PagedImage;
use casa_ms::schema::SubtableId;
use casa_ms::{MsSelection, VirtualModelEntry, virtual_model};
use casa_tables::{ColumnSchema, TableOptions};

use super::*;
use crate::ft::image::{TaylorTerm, prepare_image};
use crate::session::resolve_casa_calibration_selection;
use crate::{
    AttachmentMode, AttachmentOptions, CalibrationAttachment, ImagerPrediction,
    ImagerPredictionModel,
};

fn measures_available() -> bool {
    match casa_ms::open_measures_runtime() {
        Ok(_) => true,
        Err(error) => {
            eprintln!("skipping FT publication with no Measures tables: {error}");
            false
        }
    }
}

fn fixture(root: &Path) -> PathBuf {
    fs::create_dir_all(root).unwrap();
    assert!(!root.join("mssel_test_small.ms").exists());
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../casa-ms/tests/fixtures/mssel_test_small.ms.tgz");
    tar::Archive::new(flate2::read::GzDecoder::new(
        fs::File::open(archive).unwrap(),
    ))
    .unpack(root)
    .unwrap();
    root.join("mssel_test_small.ms")
}

fn opened(path: &Path) -> (MeasurementSet, TableWriteLock) {
    let lock = TableWriteLock::acquire_to_reread(path, 1).unwrap();
    (MeasurementSet::open(path).unwrap(), lock)
}

fn models(ms: &MeasurementSet, root: &Path, fluxes: &[f32]) -> Vec<ImageModel> {
    let field = ms.field().unwrap();
    let ArrayValue::Float64(direction) = field.phase_dir(0).unwrap() else {
        panic!("expected a double phase direction");
    };
    let selected = resolve_casa_calibration_selection(
        ms,
        &MsSelection {
            field: Some("0".into()),
            ..MsSelection::default()
        },
    )
    .unwrap();
    let spw = selected.selected_rows[0].spectral_window_id as usize;
    let frequency = ms.spectral_window().unwrap().chan_freq(spw).unwrap()[0];
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        casa_coordinates::Projection::new(casa_coordinates::ProjectionType::SIN),
        [direction[[0, 0]], direction[[1, 0]]],
        [-1e-7, 1e-7],
        [16., 16.],
    ));
    coords.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coords.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::TOPO,
        frequency,
        5e9,
        0.,
        frequency,
    ));
    fluxes
        .iter()
        .enumerate()
        .map(|(index, &flux)| {
            let path = root.join(format!("term-{index}.im"));
            let mut image =
                PagedImage::<f32>::create(vec![32, 32, 1, 1], coords.clone(), &path).unwrap();
            image.set_units("Jy/pixel").unwrap();
            image.put_at(flux, &[16, 16, 0, 0]).unwrap();
            drop(image);
            let term = (fluxes.len() > 1).then_some(TaylorTerm {
                count: fluxes.len() as i32,
                index: index as i32,
                reference_hz: frequency * 0.99,
            });
            prepare_image(ms, &selected, &path, term, false, false)
                .unwrap()
                .unwrap()
        })
        .collect()
}

fn stale_main(ms: &mut MeasurementSet, lock: &mut TableWriteLock) {
    let old = virtual_model(ms, 0).unwrap().unwrap().record;
    let main = ms.main_table_mut();
    main.keywords_mut().upsert("model_0", Value::Record(old));
    main.keywords_mut().upsert(
        "definedmodel_field_0",
        Value::Scalar(ScalarValue::String("model_0".to_owned())),
    );
    main.save_metadata_only(TableOptions::new(lock.path()))
        .unwrap();
    lock.record_write();
}

fn grid_path(entry: &VirtualModelEntry) -> PathBuf {
    let machine = match entry.container.get("subftm") {
        Some(Value::Record(grid)) => grid,
        _ => &entry.container,
    };
    let Some(Value::Scalar(ScalarValue::String(path))) = machine.get("diskimage") else {
        panic!("a saved FT machine needs a disk image");
    };
    PathBuf::from(path)
}

fn grids(path: &Path) -> Vec<PathBuf> {
    let ms = MeasurementSet::open(path).unwrap();
    let record = virtual_model(&ms, 0).unwrap().unwrap().record;
    VirtualModelEntry::entries(&record)
        .unwrap()
        .iter()
        .map(|entry| {
            let path = grid_path(entry);
            assert!(path.join("table.dat").is_file(), "missing grid {path:?}");
            path
        })
        .collect()
}

#[test]
#[cfg(unix)]
fn imager_retries_a_published_increment_without_adding_it_again() {
    if !measures_available() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let path = fixture(temp.path());
    let (mut ms, mut lock) = opened(&path);
    drop(models(&ms, temp.path(), &[2.]));
    lock.release().unwrap();
    let attachment = CalibrationAttachment::attach(
        &path,
        AttachmentMode::Selection,
        AttachmentOptions::default(),
        MsSelection {
            field: Some("0".into()),
            ..MsSelection::default()
        },
    )
    .unwrap();
    let images = [temp.path().join("term-0.im")];
    let mut imager = ImagerPrediction::default();
    let request = |incremental| ImagerPredictionModel {
        images: &images,
        components: None,
        incremental,
        phase_centre_time: None,
    };
    assert!(imager.predict(&attachment, request(false)).unwrap());
    assert_eq!(grids(&path).len(), 1);

    (ms, lock) = opened(&path);
    stale_main(&mut ms, &mut lock);
    lock.release().unwrap();
    let info = path.join("table.info");
    let original = fs::metadata(&info).unwrap().permissions();
    let mut readonly = original.clone();
    readonly.set_readonly(true);
    fs::set_permissions(&info, readonly).unwrap();
    if fs::OpenOptions::new().write(true).open(&info).is_ok() {
        fs::set_permissions(&info, original).unwrap();
        eprintln!("skipping imager retry fault because this process can write read-only files");
        return;
    }
    let first = imager.predict(&attachment, request(true));
    fs::set_permissions(&info, original).unwrap();
    assert!(
        first.is_err(),
        "MAIN metadata save should fail after SOURCE"
    );
    let written = grids(&path);
    assert_eq!(written.len(), 2);
    assert!(imager.predict(&attachment, request(false)).is_err());
    assert!(imager.predict(&attachment, request(true)).unwrap());
    assert_eq!(grids(&path), written);
    let reopened = MeasurementSet::open(&path).unwrap();
    assert!(reopened.main_table().keywords().get("model_0").is_none());
    assert!(imager.predict(&attachment, request(true)).unwrap());
    assert_eq!(grids(&path).len(), 3);
}

#[test]
fn source_save_keeps_every_taylor_grid_when_main_cleanup_fails() {
    if !measures_available() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let external = std::env::var_os("CASARS_FT_PUBLICATION_WORK").map(PathBuf::from);
    let root = external.as_deref().unwrap_or(temp.path());
    let path = fixture(root);
    let (mut ms, mut lock) = opened(&path);
    let base_dir = root.join("base");
    fs::create_dir(&base_dir).unwrap();
    let base = models(&ms, &base_dir, &[1.5]);
    VirtualFtPublication::new([&ms].into_iter(), vec![], base, false, false)
        .unwrap()
        .publish([(&mut ms, &mut lock)].into_iter())
        .unwrap();
    let old = grids(&path);
    stale_main(&mut ms, &mut lock);
    let prepared = models(&ms, root, &[2., 0.5]);
    let mut publication =
        VirtualFtPublication::new([&ms].into_iter(), vec![], prepared, false, false).unwrap();
    let main = path.join("table.dat");
    let backup = path.join("table.dat.before-test");
    fs::rename(&main, &backup).unwrap();
    let first = publication.publish([(&mut ms, &mut lock)].into_iter());
    fs::rename(&backup, &main).unwrap();
    assert!(first.is_err(), "MAIN metadata save should fail");
    let written = grids(&path);
    assert_eq!(written.len(), 2, "SOURCE has the full Taylor record");
    assert!(
        old[0].join("table.dat").is_file(),
        "MAIN still names the old image"
    );
    assert!(
        MeasurementSet::open(&path)
            .unwrap()
            .main_table()
            .keywords()
            .get("model_0")
            .is_some(),
        "the stale MAIN keyword still exists"
    );
    if external.is_none() {
        // Retry on the same handle whose in-memory cleanup already ran
        // but whose control file could not be reloaded after the error.
        publication
            .publish([(&mut ms, &mut lock)].into_iter())
            .unwrap();
        assert!(
            MeasurementSet::open(&path)
                .unwrap()
                .main_table()
                .keywords()
                .get("model_0")
                .is_none(),
            "the resumed cleanup has to remove the on-disk keyword"
        );
        assert_eq!(grids(&path), written);
    }
    lock.release().unwrap();
}

#[test]
fn retry_after_later_target_failure_applies_the_increment_once() {
    if !measures_available() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let left = fixture(&temp.path().join("left"));
    let right = fixture(&temp.path().join("right"));

    let (mut first, mut first_lock) = opened(&left);
    let (mut second, mut second_lock) = opened(&right);
    let base = models(&first, temp.path(), &[2.]);
    VirtualFtPublication::new([&first, &second].into_iter(), vec![], base, false, false)
        .unwrap()
        .publish(
            [
                (&mut first, &mut first_lock),
                (&mut second, &mut second_lock),
            ]
            .into_iter(),
        )
        .unwrap();
    let first_base = grids(&left);
    let second_base = grids(&right);
    assert_ne!(first_base, second_base);

    stale_main(&mut second, &mut second_lock);
    let incremental_path = temp.path().join("increment");
    fs::create_dir(&incremental_path).unwrap();
    let incremental = models(&first, &incremental_path, &[0.5]);
    let mut publication = VirtualFtPublication::new(
        [&first, &second].into_iter(),
        vec![],
        incremental,
        true,
        false,
    )
    .unwrap();
    let main = right.join("table.dat");
    let backup = right.join("table.dat.before-test");
    fs::rename(&main, &backup).unwrap();
    let failing = publication.publish(
        [
            (&mut first, &mut first_lock),
            (&mut second, &mut second_lock),
        ]
        .into_iter(),
    );
    fs::rename(&backup, &main).unwrap();
    assert!(failing.is_err(), "later MAIN metadata save should fail");
    first_lock.release().unwrap();
    second_lock.release().unwrap();
    let first_written = grids(&left);
    let second_written = grids(&right);
    assert_eq!((first_written.len(), second_written.len()), (2, 2));
    assert_eq!(first_written[0], first_base[0]);
    assert_eq!(second_written[0], second_base[0]);
    assert_ne!(first_written[1], second_written[1]);

    let (mut first, mut first_lock) = opened(&left);
    let (mut second, mut second_lock) = opened(&right);
    publication
        .publish(
            [
                (&mut first, &mut first_lock),
                (&mut second, &mut second_lock),
            ]
            .into_iter(),
        )
        .unwrap();
    first_lock.release().unwrap();
    second_lock.release().unwrap();
    drop(publication);
    assert_eq!(grids(&left), first_written);
    assert_eq!(grids(&right), second_written);
    assert!(
        MeasurementSet::open(right)
            .unwrap()
            .main_table()
            .keywords()
            .get("model_0")
            .is_none(),
        "retry finishes the previous MAIN cleanup"
    );
}

#[cfg(unix)]
#[test]
fn a_changed_source_route_refuses_the_batch_before_any_increment() {
    if !measures_available() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let left = fixture(&temp.path().join("left"));
    let right = fixture(&temp.path().join("right"));
    let (mut first, mut first_lock) = opened(&left);
    let (second, second_lock) = opened(&right);
    let model = models(&first, temp.path(), &[2.]);
    let mut publication =
        VirtualFtPublication::new([&first, &second].into_iter(), vec![], model, true, false)
            .unwrap();

    let original_source = temp.path().join("original-right-source");
    fs::rename(right.join("SOURCE"), &original_source).unwrap();
    std::os::unix::fs::symlink(left.join("SOURCE"), right.join("SOURCE")).unwrap();
    let mut second = MeasurementSet::open(&right).unwrap();
    let mut second_lock = second_lock;
    let error = publication
        .publish(
            [
                (&mut first, &mut first_lock),
                (&mut second, &mut second_lock),
            ]
            .into_iter(),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("storage target changed"),
        "{error}"
    );
    assert!(
        virtual_model(&MeasurementSet::open(&left).unwrap(), 0)
            .unwrap()
            .is_none()
    );

    // Also check an alias skipped by the plan, with the routes reversed.
    fs::create_dir(temp.path().join("second-model")).unwrap();
    let model = models(&first, &temp.path().join("second-model"), &[3.]);
    let mut alias =
        VirtualFtPublication::new([&first, &second].into_iter(), vec![], model, true, false)
            .unwrap();
    fs::remove_file(right.join("SOURCE")).unwrap();
    fs::rename(&original_source, right.join("SOURCE")).unwrap();
    let mut second = MeasurementSet::open(right).unwrap();
    let error = alias
        .publish(
            [
                (&mut first, &mut first_lock),
                (&mut second, &mut second_lock),
            ]
            .into_iter(),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("storage target changed"),
        "{error}"
    );
    assert!(
        virtual_model(&MeasurementSet::open(left).unwrap(), 0)
            .unwrap()
            .is_none()
    );
    first_lock.release().unwrap();
    second_lock.release().unwrap();
}

#[test]
fn a_rejected_source_cell_leaves_the_publication_unwritten() {
    if !measures_available() {
        return;
    }
    for retry in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = fixture(temp.path());
        let (mut ms, mut lock) = opened(&path);
        let source_path = path.join("SOURCE");
        let original_source = temp.path().join("original-source");
        crate::paths::copy_dir_recursive(&source_path, &original_source).unwrap();
        let source = ms.subtable_mut(SubtableId::Source).unwrap();
        source
            .add_column(
                ColumnSchema::scalar("SOURCE_MODEL", PrimitiveType::String),
                Some(Value::Scalar(ScalarValue::String(String::new()))),
            )
            .unwrap();
        source.save(TableOptions::new(&source_path)).unwrap();

        let images = models(&ms, temp.path(), &[2.]);
        let grid = grid_path(&images[0].entry);
        let mut publication =
            VirtualFtPublication::new([&ms].into_iter(), vec![], images, true, false).unwrap();
        assert!(
            publication
                .publish([(&mut ms, &mut lock)].into_iter())
                .is_err()
        );
        assert!(
            virtual_model(&MeasurementSet::open(&path).unwrap(), 0)
                .unwrap()
                .is_none()
        );
        if retry {
            let invalid_source = temp.path().join("invalid-source");
            fs::rename(&source_path, &invalid_source).unwrap();
            fs::rename(&original_source, &source_path).unwrap();
            // Correct the table while preserving the already-prepared grid.
            fs::rename(invalid_source.join(grid.file_name().unwrap()), &grid).unwrap();
            publication
                .publish([(&mut ms, &mut lock)].into_iter())
                .unwrap();
            assert_eq!(grids(&path), [grid]);
        } else {
            drop(publication);
            assert!(
                !grid.exists(),
                "a pre-save refusal must not retain the grid"
            );
        }
        lock.release().unwrap();
    }
}
