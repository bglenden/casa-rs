// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::Complex32;

use super::{
    AntennaDataArea, BaselineRecord, CdaId, CircularPolarization, CorrelatorDataArea,
    CorrelatorMode, DirectionEpoch, DopplerDefinition, FrequencyFrame, IfId, IfUsage,
    RecordControlArea, SubarrayDataArea,
};

#[test]
fn rca_decodes_ada_layout() {
    let mut bytes = vec![0_u8; 128];
    bytes[0..4].copy_from_slice(&(64_i32).to_be_bytes());
    bytes[2 * 3..2 * 3 + 2].copy_from_slice(&27_u16.to_be_bytes());
    bytes[2 * 4..2 * 4 + 4].copy_from_slice(&12345_u32.to_be_bytes());
    bytes[2 * 12..2 * 12 + 4].copy_from_slice(&100_u32.to_be_bytes());
    bytes[2 * 14..2 * 14 + 4].copy_from_slice(&150_u32.to_be_bytes());
    bytes[2 * 16..2 * 16 + 2].copy_from_slice(&8_u16.to_be_bytes());
    bytes[2 * 17..2 * 17 + 2].copy_from_slice(&3_u16.to_be_bytes());
    bytes[2 * 18..2 * 18 + 4].copy_from_slice(&200_u32.to_be_bytes());
    bytes[2 * 20..2 * 20 + 2].copy_from_slice(&14_u16.to_be_bytes());
    bytes[2 * 21..2 * 21 + 2].copy_from_slice(&28_u16.to_be_bytes());
    bytes[2 * 22..2 * 22 + 4].copy_from_slice(&300_u32.to_be_bytes());

    let rca = RecordControlArea::new(&bytes);
    assert_eq!(rca.length_bytes().unwrap(), 128);
    assert_eq!(rca.revision().unwrap(), 27);
    assert_eq!(rca.obs_day().unwrap(), 12345);
    assert_eq!(rca.sda_offset_bytes().unwrap(), 200);
    assert_eq!(rca.ada_size_bytes().unwrap(), 16);
    assert_eq!(rca.ada_offset_bytes(0).unwrap(), 300);
    assert_eq!(rca.ada_offset_bytes(2).unwrap(), 332);
    assert_eq!(rca.cda_offset_bytes(0).unwrap(), 400);
    assert_eq!(rca.cda_header_bytes(0).unwrap(), 28);
    assert_eq!(rca.cda_baseline_bytes(0).unwrap(), 56);
    assert_eq!(rca.cda_offset_bytes(1).unwrap(), 600);
    assert_eq!(rca.used_cda_count().unwrap(), 2);
    assert!(
        rca.ada_offset_bytes(3)
            .unwrap_err()
            .contains("out of range")
    );
    assert!(
        rca.cda_offset_bytes(4)
            .unwrap_err()
            .contains("out of range")
    );
    assert!(
        RecordControlArea::new(&[0, 0, 0, 0])
            .length_bytes()
            .unwrap_err()
            .contains("invalid logical-record length")
    );
}

#[test]
fn sda_decodes_mode_channel_counts_and_strings() {
    let mut bytes = vec![0_u8; 512];
    bytes[2..18].copy_from_slice(b"3C286           ");
    bytes[2 * 9..2 * 9 + 2].copy_from_slice(&7_i16.to_be_bytes());
    bytes[2 * 10..2 * 10 + 2].copy_from_slice(b"D ");
    bytes[2 * 11..2 * 11 + 6].copy_from_slice(b"AB123 ");
    bytes[2 * 15..2 * 15 + 2].copy_from_slice(b"PA");
    bytes[2 * 16] = b'C';
    bytes[2 * 18] = 0x20;
    bytes[2 * 19..2 * 19 + 2].copy_from_slice(&192_i16.to_be_bytes());
    bytes[2 * 157..2 * 157 + 4].copy_from_slice(b"PA  ");
    bytes[2 * 159] = b'H';
    bytes[2 * 161..2 * 161 + 2].copy_from_slice(&2000_i16.to_be_bytes());

    let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
    assert_eq!(sda.source_name().unwrap(), "3C286");
    assert_eq!(sda.source_qualifier().unwrap(), 7);
    assert_eq!(sda.array_configuration().unwrap(), "D");
    assert_eq!(sda.observation_id().unwrap(), "AB123");
    assert_eq!(sda.observation_mode_code().unwrap(), "PA");
    assert_eq!(sda.calibration_code().unwrap(), "C");
    assert_eq!(sda.correlator_mode().unwrap(), CorrelatorMode::Pa);
    assert_eq!(sda.true_channels(CdaId::Cda0).unwrap(), 4);
    assert_eq!(sda.n_channels(CdaId::Cda0).unwrap(), 3);
    assert_eq!(sda.integration_time_seconds().unwrap(), 10.0);
    assert!(sda.smoothed().unwrap());
    assert_eq!(
        sda.observation_mode_description().unwrap(),
        "Single dish pointing mode (IF A)"
    );
    assert_eq!(sda.direction_epoch().unwrap(), DirectionEpoch::J2000);
    assert_eq!(sda.electronic_path(CdaId::Cda2).unwrap(), 0);
    assert_eq!(
        sda.if_usage(CdaId::Cda2).unwrap(),
        vec![IfUsage {
            ant1: IfId::A,
            ant2: IfId::C
        }]
    );
}

#[test]
fn sda_mode_tables_cover_casa_if_usage_and_observing_codes() {
    let cases = [
        (
            b"    ",
            CorrelatorMode::Continuum,
            CdaId::Cda0,
            vec![
                (IfId::A, IfId::A),
                (IfId::C, IfId::C),
                (IfId::A, IfId::C),
                (IfId::C, IfId::A),
            ],
        ),
        (
            b"1A  ",
            CorrelatorMode::A,
            CdaId::Cda0,
            vec![(IfId::A, IfId::A)],
        ),
        (
            b"1B  ",
            CorrelatorMode::B,
            CdaId::Cda1,
            vec![(IfId::B, IfId::B)],
        ),
        (
            b"1C  ",
            CorrelatorMode::C,
            CdaId::Cda2,
            vec![(IfId::C, IfId::C)],
        ),
        (
            b"1D  ",
            CorrelatorMode::D,
            CdaId::Cda3,
            vec![(IfId::D, IfId::D)],
        ),
        (
            b"2AB ",
            CorrelatorMode::Ab,
            CdaId::Cda1,
            vec![(IfId::B, IfId::B)],
        ),
        (
            b"2AC ",
            CorrelatorMode::Ac,
            CdaId::Cda2,
            vec![(IfId::C, IfId::C)],
        ),
        (
            b"2AD ",
            CorrelatorMode::Ad,
            CdaId::Cda3,
            vec![(IfId::D, IfId::D)],
        ),
        (
            b"2BC ",
            CorrelatorMode::Bc,
            CdaId::Cda1,
            vec![(IfId::B, IfId::B)],
        ),
        (
            b"2BD ",
            CorrelatorMode::Bd,
            CdaId::Cda3,
            vec![(IfId::D, IfId::D)],
        ),
        (
            b"2CD ",
            CorrelatorMode::Cd,
            CdaId::Cda2,
            vec![(IfId::C, IfId::C)],
        ),
        (
            b"4   ",
            CorrelatorMode::Abcd,
            CdaId::Cda3,
            vec![(IfId::D, IfId::D)],
        ),
        (
            b"PB  ",
            CorrelatorMode::Pb,
            CdaId::Cda2,
            vec![(IfId::B, IfId::D)],
        ),
        (b"ZZ  ", CorrelatorMode::Unknown, CdaId::Cda0, vec![]),
    ];
    for (raw_mode, expected_mode, cda, expected_usage) in cases {
        let mut bytes = vec![0_u8; 512];
        bytes[2 * 157..2 * 157 + 4].copy_from_slice(raw_mode);
        let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
        assert_eq!(sda.correlator_mode().unwrap(), expected_mode);
        let usage = expected_usage
            .into_iter()
            .map(|(ant1, ant2)| IfUsage { ant1, ant2 })
            .collect::<Vec<_>>();
        assert_eq!(sda.if_usage(cda).unwrap(), usage);
    }

    let mut bytes = vec![0_u8; 512];
    for (code, expected) in [
        (b"D ", "Delay center determination mode"),
        (b"IR", "Interferometer reference pointing mode"),
        (b"TB", "Test back-end and front-end"),
        (
            b"VA",
            "Self-phasing mode for VLBI phased-array (IFs A and D)",
        ),
        (b"??", "Unknown mode: ??"),
    ] {
        bytes[2 * 15..2 * 15 + 2].copy_from_slice(code);
        let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
        assert_eq!(sda.observation_mode_description().unwrap(), expected);
    }
}

#[test]
fn sda_frequency_metadata_decodes_doppler_and_bandwidth_branches() {
    let mut bytes = vec![0_u8; 512];
    bytes[2 * 18] = 0x10;
    bytes[2 * 100] = 0x70;
    bytes[2 * 101] = 0x30;
    bytes[2 * 153] = b'L';
    bytes[2 * 153 + 1] = b'V';
    bytes[2 * 154] = b'B';
    bytes[2 * 154 + 1] = b'Z';
    bytes[2 * 159 + 1] = b'H';
    bytes[2 * 161..2 * 161 + 2].copy_from_slice(&1950_i16.to_be_bytes());
    bytes[2 * 166..2 * 166 + 2].copy_from_slice(&2_i16.to_be_bytes());

    let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
    assert_eq!(sda.true_channels(CdaId::Cda0).unwrap(), 2);
    assert_eq!(sda.n_channels(CdaId::Cda0).unwrap(), 1);
    assert_eq!(sda.channel_width_hz(CdaId::Cda0).unwrap(), 12.5e6);
    assert_eq!(
        sda.correlated_bandwidth_hz(CdaId::Cda0).unwrap(),
        25.0 / 32.0 * 1.0e6
    );
    assert_eq!(sda.filter_bandwidth_hz(CdaId::Cda0).unwrap(), 1.0e200);
    assert!(sda.doppler_tracking(CdaId::Cda0).unwrap());
    assert_eq!(sda.rest_frame(CdaId::Cda0).unwrap(), FrequencyFrame::Lsrk);
    assert_eq!(
        sda.doppler_definition(CdaId::Cda0).unwrap(),
        DopplerDefinition::Radio
    );
    assert_eq!(
        sda.rest_frame(CdaId::Cda1).unwrap(),
        FrequencyFrame::Barycentric
    );
    assert_eq!(
        sda.doppler_definition(CdaId::Cda1).unwrap(),
        DopplerDefinition::Optical
    );
    assert!(sda.smoothed().unwrap());
    assert_eq!(sda.direction_epoch().unwrap(), DirectionEpoch::B1950Vla);

    bytes[2 * 18] = 0x00;
    bytes[2 * 100] = 0x90;
    bytes[2 * 101] = 0x40;
    bytes[2 * 153 + 1] = b'F';
    bytes[2 * 161..2 * 161 + 2].copy_from_slice(&(-1_i16).to_be_bytes());
    let sda = SubarrayDataArea::new(&bytes, 0).unwrap();
    assert_eq!(sda.true_channels(CdaId::Cda0).unwrap(), 1);
    assert!(
        sda.correlated_bandwidth_hz(CdaId::Cda0)
            .unwrap()
            .is_infinite()
    );
    assert!(sda.filter_bandwidth_hz(CdaId::Cda0).unwrap().is_nan());
    assert!(!sda.doppler_tracking(CdaId::Cda0).unwrap());
    assert_eq!(sda.direction_epoch().unwrap(), DirectionEpoch::Apparent);
}

#[test]
fn ada_decodes_identity_and_if_bits() {
    let mut bytes = vec![0_u8; 256];
    bytes[0] = 12;
    bytes[2..4].copy_from_slice(&0x8040_u16.to_be_bytes());
    bytes[2 * 3] = 0xa0;
    bytes[2 * 3 + 1] = 0x50;
    bytes[2 * (64 + IfId::B.index())..2 * (64 + IfId::B.index()) + 2]
        .copy_from_slice(&0x2000_i16.to_be_bytes());

    let ada = AntennaDataArea::new(&bytes, 0).unwrap();
    assert_eq!(ada.antenna_id().unwrap(), 12);
    assert_eq!(ada.antenna_name(true).unwrap(), "EA12");
    assert_eq!(ada.antenna_name(false).unwrap(), "12");
    assert_eq!(ada.array_name().unwrap(), "EVLA:");
    assert_eq!(ada.if_status(IfId::A).unwrap(), 0x0a);
    assert_eq!(ada.if_status(IfId::D).unwrap(), 0x00);
    assert_eq!(
        ada.if_polarization(IfId::A).unwrap(),
        CircularPolarization::Left
    );
    assert_eq!(
        ada.if_polarization(IfId::C).unwrap(),
        CircularPolarization::Right
    );
    assert!(ada.nominal_sensitivity_applied(IfId::B, 25).unwrap());
    assert!(ada.nominal_sensitivity_applied(IfId::B, 24).unwrap());

    bytes[0] = 29;
    let ada = AntennaDataArea::new(&bytes, 0).unwrap();
    assert_eq!(ada.antenna_name(true).unwrap(), "VA29");
    assert_eq!(ada.array_name().unwrap(), "VLA:_");
}

#[test]
fn ada_pad_names_and_offsets_cover_position_helpers() {
    let mut bytes = vec![0_u8; 256];
    bytes[0] = 1;
    // Zero-valued ModComp doubles decode to zero ns, outside the 0.5 m pad-name tolerance.
    let ada = AntennaDataArea::new(&bytes, 0).unwrap();
    assert_eq!(ada.position_meters().unwrap(), [0.0, 0.0, 0.0]);
    assert_eq!(ada.u_meters().unwrap(), 0.0);
    assert_eq!(ada.v_meters().unwrap(), 0.0);
    assert_eq!(ada.w_meters().unwrap(), 0.0);
    assert_eq!(ada.pad_name().unwrap(), "UNKNOWN");
}

#[test]
fn cda_decodes_continuum_baseline() {
    let mut bytes = vec![0_u8; 64];
    let offset = 4_u32;
    bytes[offset as usize + 1] = 0;
    bytes[offset as usize + 2] = 0x40;
    bytes[offset as usize + 3] = 0x02;

    let samples = [
        (100_i16, -200_i16, 25_u16),
        (-300_i16, 400_i16, 26_u16),
        (500_i16, -600_i16, 27_u16),
        (-700_i16, 800_i16, 28_u16),
    ];
    for (index, (real, imag, variance)) in samples.into_iter().enumerate() {
        let base = offset as usize + 4 + index * 6;
        bytes[base..base + 2].copy_from_slice(&real.to_be_bytes());
        bytes[base + 2..base + 4].copy_from_slice(&imag.to_be_bytes());
        bytes[base + 4..base + 6].copy_from_slice(&variance.to_be_bytes());
    }

    let cda = CorrelatorDataArea::new(&bytes, offset, 28, 1, 1).unwrap();
    assert!(cda.is_valid());
    assert_eq!(cda.n_antennas(), 1);
    assert_eq!(cda.baseline_size_bytes(), 28);
    assert_eq!(cda.n_channels_true(), 1);
    assert_eq!(cda.n_cross_correlations(), 0);
    assert!(cda.cross_corr(0).unwrap_err().contains("out of range"));
    let baseline = cda.auto_corr(0).unwrap();
    assert_eq!(baseline.scale().unwrap(), 256);
    assert_eq!(baseline.ant1().unwrap(), 10);
    assert_eq!(baseline.ant2().unwrap(), 2);
    assert_eq!(baseline.flags().unwrap(), vec![true, true, false, true]);
    assert_eq!(
        baseline.data().unwrap(),
        vec![
            Complex32::new(100.0 / 256.0, -200.0 / 256.0),
            Complex32::new(-300.0 / 256.0, 400.0 / 256.0),
            Complex32::new(500.0 / 256.0, -600.0 / 256.0),
            Complex32::new(-700.0 / 256.0, 800.0 / 256.0),
        ]
    );

    let BaselineRecord::Continuum(record) = baseline else {
        panic!("expected continuum record");
    };
    assert_eq!(
        record.variance().unwrap(),
        vec![25.0 / 256.0, 26.0 / 256.0, 27.0 / 256.0, 28.0 / 256.0]
    );
}

#[test]
fn cda_decodes_spectral_line_baseline() {
    let mut bytes = vec![0_u8; 64];
    let offset = 4_u32;
    bytes[offset as usize + 2] = 0x20;
    bytes[offset as usize + 3] = 0x00;
    bytes[offset as usize + 4] = 0x20;
    bytes[offset as usize + 5] = 0x00;
    bytes[offset as usize + 10..offset as usize + 12].copy_from_slice(&10_i16.to_be_bytes());
    bytes[offset as usize + 12..offset as usize + 14].copy_from_slice(&20_i16.to_be_bytes());
    bytes[offset as usize + 14..offset as usize + 16].copy_from_slice(&30_i16.to_be_bytes());
    bytes[offset as usize + 16..offset as usize + 18].copy_from_slice(&40_i16.to_be_bytes());

    let cda = CorrelatorDataArea::new(&bytes, offset, 18, 1, 3).unwrap();
    let baseline = cda.auto_corr(0).unwrap();
    assert_eq!(baseline.scale().unwrap(), 256);
    assert_eq!(baseline.ant1().unwrap(), 1);
    assert_eq!(baseline.ant2().unwrap(), 0);
    assert_eq!(
        baseline.data().unwrap(),
        vec![
            Complex32::new(10.0 / 256.0, 20.0 / 256.0),
            Complex32::new(30.0 / 256.0, 40.0 / 256.0),
        ]
    );
    assert_eq!(baseline.flags().unwrap(), vec![false, false]);
}

#[test]
fn cda_matches_casa_wrapping_scale_behavior_for_large_exponents() {
    let mut bytes = vec![0_u8; 64];
    let offset = 4_u32;
    bytes[offset as usize + 1] = 48;

    let cda = CorrelatorDataArea::new(&bytes, offset, 28, 1, 1).unwrap();
    let baseline = cda.auto_corr(0).unwrap();
    assert_eq!(baseline.scale().unwrap(), 16_777_216);
}

#[test]
fn cda_constructor_and_cross_correlation_validate_layout() {
    assert!(
        CorrelatorDataArea::new(&[0; 16], 0, 8, 2, 1)
            .unwrap_err()
            .contains("not present")
    );
    assert!(
        CorrelatorDataArea::new(&[0; 16], u32::MAX - 1, 8, 2, 1)
            .unwrap_err()
            .contains("out of range")
    );

    let mut bytes = vec![0_u8; 128];
    let offset = 8_u32;
    let baseline_size = 28_u16;
    let cross_offset = offset as usize + baseline_size as usize * 2;
    bytes[cross_offset + 1] = 0;
    bytes[cross_offset + 2] = 0x60;
    bytes[cross_offset + 3] = 0x03;
    bytes[cross_offset + 4..cross_offset + 6].copy_from_slice(&64_i16.to_be_bytes());
    bytes[cross_offset + 6..cross_offset + 8].copy_from_slice(&(-128_i16).to_be_bytes());

    let cda = CorrelatorDataArea::new(&bytes, offset, baseline_size, 2, 1).unwrap();
    assert_eq!(cda.n_cross_correlations(), 1);
    assert!(cda.auto_corr(2).unwrap_err().contains("out of range"));
    let cross = cda.cross_corr(0).unwrap();
    assert_eq!(cross.ant1().unwrap(), 15);
    assert_eq!(cross.ant2().unwrap(), 3);
    assert_eq!(
        cross.data().unwrap()[0],
        Complex32::new(64.0 / 256.0, -128.0 / 256.0)
    );
}
