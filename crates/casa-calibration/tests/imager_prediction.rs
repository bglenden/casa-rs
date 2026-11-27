// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_calibration::{
    AttachmentMode, AttachmentOptions, CalibrationAttachment, ImagerGridder, ImagerImageDefinition,
    ImagerPrediction, ImagerPredictionModel, ImagerTransformOptions, ImagerVoltagePattern,
};
use casa_coordinates::{CoordinateModel, CoordinateType};
use casa_core::ScalarValue;
use casa_images::PagedImage;
use casa_imaging_operator::WPlaneCount;
use casa_ms::{MeasurementSet, MsSelection};

fn attachment(path: &Path) -> CalibrationAttachment {
    CalibrationAttachment::attach(
        path,
        AttachmentMode::Selection,
        AttachmentOptions {
            write_access: true,
            use_scratch: true,
        },
        MsSelection {
            field: Some("0".into()),
            ..MsSelection::default()
        },
    )
    .unwrap()
}

fn definition(imager: &mut ImagerPrediction, path: &Path) -> f64 {
    let image = PagedImage::<f32>::open(path).unwrap();
    imager.define_image(ImagerImageDefinition {
        shape: image.shape().try_into().unwrap(),
        coordinates: image.coordinates().clone(),
        field_id: 0,
    });
    let coords = image.coordinates();
    let index = coords.find_coordinate(CoordinateType::Spectral).unwrap();
    let CoordinateModel::Spectral(frequency) = coords.coordinate(index) else {
        panic!("an imager model must have a spectral coordinate");
    };
    frequency.world_frequency_hz(0.).unwrap()
}

fn predict(imager: &mut ImagerPrediction, attachment: &CalibrationAttachment, images: &[PathBuf]) {
    assert!(
        imager
            .predict(
                attachment,
                ImagerPredictionModel {
                    images,
                    components: None,
                    incremental: false,
                    phase_centre_time: None,
                }
            )
            .unwrap()
    );
}

#[test]
#[ignore = "run with the installed CASA imager settings oracle"]
fn installed_casa_prediction_settings() {
    let root = PathBuf::from(std::env::var_os("CASARS_IMAGER_PREDICTION_WORK").unwrap());
    let mut imager = ImagerPrediction::default();
    let point = root.join("off-axis.im");
    let term = root.join("term-1.im");
    let reference = definition(&mut imager, &point);

    let wide = attachment(&root.join("native-wproject.ms"));
    imager.set_options(ImagerTransformOptions {
        gridder: ImagerGridder::WProject(WPlaneCount::Fixed(16)),
        padding: 1.4,
        threads: 1,
        ..ImagerTransformOptions::default()
    });
    predict(&mut imager, &wide, std::slice::from_ref(&point));
    let virtual_target = CalibrationAttachment::attach(
        &root.join("native-wvirtual.ms"),
        AttachmentMode::Selection,
        AttachmentOptions::default(),
        MsSelection {
            field: Some("0".into()),
            ..MsSelection::default()
        },
    )
    .unwrap();
    predict(&mut imager, &virtual_target, std::slice::from_ref(&point));

    imager.set_options(ImagerTransformOptions {
        padding: 1.4,
        ..ImagerTransformOptions::default()
    });
    let control = attachment(&root.join("native-grid.ms"));
    predict(&mut imager, &control, std::slice::from_ref(&point));

    imager.set_options(ImagerTransformOptions::default());
    imager.set_taylor_terms(2, Some(0.99 * reference)).unwrap();
    let taylor = attachment(&root.join("native-taylor.ms"));
    predict(&mut imager, &taylor, &[point.clone(), term.clone()]);

    imager.set_taylor_terms(2, Some(0.98 * reference)).unwrap();
    let changed = attachment(&root.join("native-changed.ms"));
    predict(&mut imager, &changed, &[point.clone(), term]);

    let beam_model = root.join("beam-target.im");
    definition(&mut imager, &beam_model);
    imager.set_taylor_terms(1, None).unwrap();
    imager.set_voltage_pattern(Some(ImagerVoltagePattern::default()));
    for case in ["vp", "pointing-vp", "dish-vp"] {
        let beam = attachment(&root.join(format!("native-{case}.ms")));
        predict(&mut imager, &beam, std::slice::from_ref(&beam_model));
    }
    let all_fields = CalibrationAttachment::attach(
        &root.join("native-all-vp.ms"),
        AttachmentMode::Selection,
        AttachmentOptions {
            write_access: true,
            use_scratch: true,
        },
        MsSelection::default(),
    )
    .unwrap();
    predict(&mut imager, &all_fields, std::slice::from_ref(&beam_model));
    let virtual_beam = CalibrationAttachment::attach(
        &root.join("native-vpvirtual.ms"),
        AttachmentMode::Selection,
        AttachmentOptions::default(),
        MsSelection::default(),
    )
    .unwrap();
    predict(
        &mut imager,
        &virtual_beam,
        std::slice::from_ref(&beam_model),
    );
    imager.set_voltage_pattern(None);
    let disabled = attachment(&root.join("native-no-vp.ms"));
    predict(&mut imager, &disabled, std::slice::from_ref(&beam_model));
    let definition_distance = attachment(&root.join("native-definition-distance.ms"));
    predict(
        &mut imager,
        &definition_distance,
        std::slice::from_ref(&beam_model),
    );
    let image_distance = attachment(&root.join("native-image-distance.ms"));
    predict(
        &mut imager,
        &image_distance,
        &[root.join("focus-target.im")],
    );

    definition(&mut imager, &point);
    let varying = root.join("native-phase-default.ms");
    predict(
        &mut imager,
        &attachment(&varying),
        std::slice::from_ref(&point),
    );
    let fixed = root.join("native-phase-time.ms");
    let ms = MeasurementSet::open(&fixed).unwrap();
    let table = ms.main_table();
    let ScalarValue::Float64(epoch) = table
        .cell_accessor(table.row_count() - 1, "TIME")
        .unwrap()
        .scalar()
        .unwrap()
    else {
        panic!("a MeasurementSet row needs an epoch");
    };
    assert!(
        imager
            .predict(
                &attachment(&fixed),
                ImagerPredictionModel {
                    images: std::slice::from_ref(&point),
                    components: None,
                    incremental: false,
                    phase_centre_time: Some(*epoch),
                }
            )
            .unwrap()
    );
    let create = attachment(&root.join("native-defineimage.ms"));
    let mut fresh = ImagerPrediction::default();
    let missing = [root.join("new-native.im")];
    assert!(
        !fresh
            .predict(
                &create,
                ImagerPredictionModel {
                    images: &missing,
                    components: None,
                    incremental: false,
                    phase_centre_time: None,
                }
            )
            .unwrap()
    );
    assert!(!missing[0].exists());
    definition(&mut fresh, &point);
    predict(&mut fresh, &create, &missing);
    let created = PagedImage::<f32>::open(&missing[0]).unwrap();
    assert_eq!(created.shape(), [32, 32, 1, 1]);
}
