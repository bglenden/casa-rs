// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn new_spectral_windows_use_the_most_recently_added_source_row() {
    let tempdir = tempfile::tempdir().unwrap();
    let ms_path = tempdir.path().join("doppler-update.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let ms = MeasurementSet::create(&ms_path, builder).unwrap();
    let mut writer = VlaImportState::new(ImportVlaOptions::default(), ms, 3, 1).unwrap();
    set_flag_category_names(writer.ms.main_table_mut()).unwrap();

    let data = ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(1.0, 0.0); 4]).unwrap(),
    );
    let model = ArrayValue::Complex32(
        ArrayD::from_shape_vec(
            IxDyn(&[4, 1]).f(),
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(1.0, 0.0),
            ],
        )
        .unwrap(),
    );
    let flag =
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap());
    let flag_category =
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1, 1]).f(), vec![false; 4]).unwrap());
    let weight =
        ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());
    let sigma = ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());
    let descriptor = SpectralDescriptor {
        edge_frequency_hz: 1.4e9 - 0.5 * 5.0e7,
        observed_frequency_hz: 1.4e9,
        channel_width_hz: 5.0e7,
        num_chan: 1,
        if_conv_chain: 0,
        rest_frequency_hz: 0.0,
        rest_frame: FrequencyFrame::Topocentric,
        doppler_definition: DopplerDefinition::Radio,
        doppler_velocity_mps: 0.0,
        doppler_tracking: false,
    };
    let antennas = vec![
        NormalizedAntenna {
            antenna_id: 1,
            name: "VA01".to_string(),
            station: "VA01".to_string(),
            position_itrf_m: [-1601185.4, -5041977.5, 3554875.9],
        },
        NormalizedAntenna {
            antenna_id: 2,
            name: "VA02".to_string(),
            station: "VA02".to_string(),
            position_itrf_m: [-1601085.4, -5041977.5, 3554875.9],
        },
    ];
    let baseline = || NormalizedBaselineRow {
        antenna1_archive: 0,
        antenna2_archive: 1,
        corr_types: vec![5, 6, 7, 8],
        uvw_m: [10.0, 20.0, 30.0],
        data: data.clone(),
        corrected_data: data.clone(),
        model_data: model.clone(),
        flag: flag.clone(),
        flag_category: flag_category.clone(),
        weight: weight.clone(),
        sigma: sigma.clone(),
    };

    writer
        .push_record(NormalizedRecord {
            project: "AG189".to_string(),
            source_name: "0836+710".to_string(),
            calibration_code: "".to_string(),
            source_num_lines: 1,
            source_direction: [1.0, 0.5],
            array_id: 0,
            time_seconds: 5_097_600_000.0,
            integration_seconds: 10.0,
            direction_epoch: DirectionEpoch::J2000,
            antennas: antennas.clone(),
            groups: vec![NormalizedGroup {
                cda_ids: vec![CdaId::Cda0],
                descriptor: descriptor.clone(),
                baselines: vec![baseline()],
            }],
        })
        .unwrap();
    writer
        .push_record(NormalizedRecord {
            project: "AG189".to_string(),
            source_name: "N2146".to_string(),
            calibration_code: "".to_string(),
            source_num_lines: 1,
            source_direction: [1.5, 0.25],
            array_id: 0,
            time_seconds: 5_097_600_100.0,
            integration_seconds: 10.0,
            direction_epoch: DirectionEpoch::J2000,
            antennas: antennas.clone(),
            groups: vec![NormalizedGroup {
                cda_ids: vec![CdaId::Cda0],
                descriptor: descriptor.clone(),
                baselines: vec![baseline()],
            }],
        })
        .unwrap();
    writer
        .push_record(NormalizedRecord {
            project: "AG189".to_string(),
            source_name: "0836+710".to_string(),
            calibration_code: "".to_string(),
            source_num_lines: 1,
            source_direction: [1.0, 0.5],
            array_id: 0,
            time_seconds: 5_097_600_200.0,
            integration_seconds: 10.0,
            direction_epoch: DirectionEpoch::J2000,
            antennas,
            groups: vec![NormalizedGroup {
                cda_ids: vec![CdaId::Cda0],
                descriptor: SpectralDescriptor {
                    edge_frequency_hz: 1.5e9 - 0.5 * 5.0e7,
                    observed_frequency_hz: 1.5e9,
                    ..descriptor
                },
                baselines: vec![baseline()],
            }],
        })
        .unwrap();
    writer.finish().unwrap();

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    let doppler = reopened.doppler().unwrap();
    assert_eq!(doppler.table().row_count(), 2);
    let row = doppler.table().row_accessor().row(1).unwrap();
    let Value::Scalar(ScalarValue::Int32(source_id)) = row.get("SOURCE_ID").unwrap() else {
        panic!("expected SOURCE_ID scalar");
    };
    assert_eq!(*source_id, 1);
}

#[test]
fn doppler_tracked_spectral_windows_use_rest_frame_metadata() {
    let descriptor = SpectralDescriptor {
        edge_frequency_hz: 23_691_189_479.547_69 - 127.0 / 2.0 * 24_414.062_5,
        observed_frequency_hz: 23_691_189_479.547_69,
        channel_width_hz: 24_414.062_5,
        num_chan: 127,
        if_conv_chain: 0,
        rest_frequency_hz: 23_694_044_992.921_69,
        rest_frame: FrequencyFrame::Lsrk,
        doppler_definition: DopplerDefinition::Radio,
        doppler_velocity_mps: 5_700.0,
        doppler_tracking: true,
    };

    let ref_freq = spectral_reference_frequency(&descriptor);
    assert_eq!(ref_freq.refer(), FrequencyRef::LSRK);
    assert_eq!(
        spectral_window_name(&descriptor, ref_freq.hz(), ref_freq.refer()),
        "127*24.4 kHz channels @ 23.7 GHz (LSRK)"
    );
}

#[test]
fn doppler_tracked_descriptor_matching_uses_rest_frame_frequency() {
    let left = SpectralDescriptor {
        edge_frequency_hz: 23_691_189_479.547_69 - 127.0 / 2.0 * 24_414.062_5,
        observed_frequency_hz: 23_691_189_479.547_69,
        channel_width_hz: 24_414.062_5,
        num_chan: 127,
        if_conv_chain: 0,
        rest_frequency_hz: 23_694_044_992.921_69,
        rest_frame: FrequencyFrame::Lsrk,
        doppler_definition: DopplerDefinition::Radio,
        doppler_velocity_mps: 5_700.0,
        doppler_tracking: true,
    };
    let mut right = left.clone();
    right.observed_frequency_hz += 250_000.0;

    assert!(same_spectral_descriptor(&left, &right, 150_000.0));
}
