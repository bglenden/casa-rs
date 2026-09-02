// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CorrelationType, DirectionFrame, Epoch, FrequencyFrame, MeasurementSetIdentity,
    SelectedImageDomainProjection, SelectedImageDomainProjections, SelectedObservationSample,
    SelectedPhaseCentreProjection, SelectedPredictionTarget, SelectedSampleAddress,
    SelectedSampleCoordinates, SelectedSampleMetadata, SelectedVisibilitySample, SkyDirection,
    TimeScale, UvwCoordinateLaw,
};

mod common;

use common::identity;

#[test]
fn selected_observation_sample_schema_carries_exact_science_and_provenance() {
    let measurement_set = MeasurementSetIdentity::new(identity(1));
    let coordinates = SelectedSampleCoordinates {
        raw_uvw_m: [12.0, -4.0, 2.0],
        density_uvw_m: [12.5, -4.25, 2.25],
        transformed_uvw_m: [11.75, -3.75, 1.5],
        phase_shift_m: 0.125,
        uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
        time: Epoch::new(59_000.0, TimeScale::Utc),
        time_centroid: Epoch::new(59_000.000_001, TimeScale::Utc),
        interval_seconds: 1.0,
        exposure_seconds: 0.8,
        parallactic_angles_rad: [0.2, 0.25],
        phase_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        delay_direction: SkyDirection::new(DirectionFrame::J2000, 1.000_5, -0.500_5),
        pointing_directions: casa_imaging_model::SelectedPointingDirections {
            antenna1: SkyDirection::new(DirectionFrame::J2000, 1.001, -0.499),
            antenna2: SkyDirection::new(DirectionFrame::J2000, 1.002, -0.498),
        },
    };
    let projection = SelectedPhaseCentreProjection::new(
        coordinates.transformed_uvw_m,
        coordinates.phase_shift_m,
    )
    .expect("finite one-domain projection");
    let domain_projections =
        SelectedImageDomainProjections::new([SelectedImageDomainProjection::with_shared_psf(
            0, projection,
        )])
        .expect("canonical one-domain projections");
    let sample = SelectedObservationSample {
        address: SelectedSampleAddress {
            measurement_set,
            physical_row: 11,
            data_description_id: 2,
            spectral_window_id: 3,
            channel_index: 7,
            frequency_centre_hz: 1_400_000_000.0,
            frequency_lower_hz: 1_399_500_000.0,
            frequency_upper_hz: 1_400_500_000.0,
            channel_width_hz: 1_000_000.0,
            frequency_frame: FrequencyFrame::Topocentric,
            polarization_id: 5,
            correlation_index: 1,
            correlation_type: CorrelationType::LinearXy,
        },
        visibility: SelectedVisibilitySample::Complex32([1.25, -0.5]),
        prediction_target: SelectedPredictionTarget::ModelData,
        channel_flag: true,
        parallel_hand_group_flag: true,
        row_flag: false,
        input_weight: 2.5,
        coordinates,
        domain_projections: domain_projections.clone(),
        metadata: SelectedSampleMetadata {
            field_id: 14,
            antenna1: 10,
            antenna2: 11,
            antenna_responses: None,
            feed1: 12,
            feed2: 13,
            scan_number: 15,
            state_id: -1,
            observation_id: 17,
            array_id: 18,
        },
    };

    assert_eq!(SelectedObservationSample::SCHEMA_VERSION, 4);
    assert_eq!(sample.as_view().domain_projections(), &domain_projections);
    assert_eq!(
        sample.as_view().to_owned().domain_projections,
        domain_projections
    );
    assert_eq!(sample.address.measurement_set, measurement_set);
    assert_eq!(sample.address.physical_row, 11);
    assert_eq!(sample.address.data_description_id, 2);
    assert_eq!(sample.address.spectral_window_id, 3);
    assert_eq!(sample.address.channel_index, 7);
    assert_eq!(sample.address.frequency_centre_hz, 1_400_000_000.0);
    assert_eq!(sample.address.frequency_lower_hz, 1_399_500_000.0);
    assert_eq!(sample.address.frequency_upper_hz, 1_400_500_000.0);
    assert_eq!(sample.address.channel_width_hz, 1_000_000.0);
    assert_eq!(sample.address.frequency_frame, FrequencyFrame::Topocentric);
    assert_eq!(sample.address.polarization_id, 5);
    assert_eq!(sample.address.correlation_index, 1);
    assert_eq!(sample.address.correlation_type, CorrelationType::LinearXy);
    assert_eq!(
        sample.visibility,
        SelectedVisibilitySample::Complex32([1.25, -0.5])
    );
    assert_eq!(
        sample.prediction_target,
        SelectedPredictionTarget::ModelData
    );
    assert!(sample.channel_flag);
    assert!(sample.parallel_hand_group_flag);
    assert!(!sample.row_flag);
    assert_eq!(sample.input_weight, 2.5_f32);
    assert_eq!(sample.coordinates.raw_uvw_m, [12.0, -4.0, 2.0]);
    assert_eq!(sample.coordinates.density_uvw_m, [12.5, -4.25, 2.25]);
    assert_eq!(sample.coordinates.transformed_uvw_m, [11.75, -3.75, 1.5]);
    assert_eq!(sample.coordinates.phase_shift_m, 0.125);
    assert_eq!(
        sample.coordinates.uvw_law,
        UvwCoordinateLaw::PhaseTrackingCentre
    );
    assert_eq!(sample.coordinates.time.mjd_days(), 59_000.0);
    assert_eq!(sample.coordinates.time_centroid.mjd_days(), 59_000.000_001);
    assert_eq!(sample.coordinates.interval_seconds, 1.0);
    assert_eq!(sample.coordinates.exposure_seconds, 0.8);
    assert_eq!(
        sample.coordinates.phase_direction,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5)
    );
    assert_eq!(
        sample.coordinates.delay_direction,
        SkyDirection::new(DirectionFrame::J2000, 1.000_5, -0.500_5)
    );
    assert_eq!(
        sample.coordinates.pointing_directions.antenna1,
        SkyDirection::new(DirectionFrame::J2000, 1.001, -0.499)
    );
    assert_eq!(
        sample.coordinates.pointing_directions.antenna2,
        SkyDirection::new(DirectionFrame::J2000, 1.002, -0.498)
    );
    assert_eq!(sample.metadata.field_id, 14);
    assert_eq!(sample.metadata.antenna1, 10);
    assert_eq!(sample.metadata.antenna2, 11);
    assert_eq!(sample.metadata.feed1, 12);
    assert_eq!(sample.metadata.feed2, 13);
    assert_eq!(sample.metadata.scan_number, 15);
    assert_eq!(sample.metadata.state_id, -1);
    assert_eq!(sample.metadata.observation_id, 17);
    assert_eq!(sample.metadata.array_id, 18);

    let variants = [
        SelectedVisibilitySample::Float32(3.0),
        SelectedVisibilitySample::Complex32([3.0, -4.0]),
    ];
    assert_eq!(variants[0], SelectedVisibilitySample::Float32(3.0));
    assert_eq!(
        variants[1],
        SelectedVisibilitySample::Complex32([3.0, -4.0])
    );
    assert_eq!(
        SelectedPredictionTarget::NotRequested,
        SelectedPredictionTarget::NotRequested
    );

    // This is a closed value schema only. A caller may hash these reports into
    // a non-authoritative content digest, but cannot thereby prove traversal
    // coverage, retained access, execution-attempt freshness, completion, or
    // authority to weight or publish.
}
