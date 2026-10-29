// SPDX-License-Identifier: LGPL-3.0-or-later
//! The catalogue's sources and lines (#976) as each store states them: the
//! MSv2 `SOURCE` of `ms_trial_lines.ms`, whose field's source has one line
//! in its first spectral window and two in its second, and XRADIO's
//! conversion of it (`tools/ms-trial/make_lines_fixtures.sh`), whose
//! `field_and_source` datasets state the same for each window: the
//! direction, the transitions, the rest frequencies and the systemic
//! velocities, the last without a frame. The two still bind.

use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{BindingTolerance, Catalogue, FieldId, SourceRecord, WindowId};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::radial_velocity::RadialVelocityRef;

use super::fixture;

/// Each window's lines as the fixture states them: transition, rest
/// frequency (Hz, LSRK) and systemic velocity (m/s).
const LINES: [&[(&str, f64, f64)]; 2] = [
    &[("CO_6-5", 691.473_076_3e9, 1_500.0)],
    &[
        ("H2O_658", 658.006_55e9, 1_500.0),
        ("HCN_8-7", 708.877_21e9, 1_520.0),
    ],
];

/// The window of `catalogue` whose first channel is within a channel of
/// `first_hz`, the window's simulated start.
fn window_at(catalogue: &Catalogue, first_hz: f64) -> WindowId {
    catalogue
        .partitions
        .iter()
        .find(|entry| (entry.frequencies_hz[0] - first_hz).abs() <= 2.0e6)
        .map(|entry| entry.window)
        .expect("a window at the frequency")
}

/// The first record of field 0's source for `window`.
fn record(catalogue: &Catalogue, window: WindowId) -> &SourceRecord {
    catalogue
        .source_records(FieldId(0), window)
        .first()
        .copied()
        .expect("a record of the field's source for the window")
}

/// A line: its transition, rest frequency (Hz) and systemic velocity (m/s).
type Line = (String, f64, f64);

/// What `record` states of its lines, and its velocities' frames.
fn lines(record: &SourceRecord) -> (Vec<Line>, Vec<Option<RadialVelocityRef>>) {
    let transitions = record.transitions.as_ref().expect("transitions");
    let rest = record.rest_frequencies.as_ref().expect("rest frequencies");
    let velocities = record.systemic_velocities.as_ref().expect("velocities");
    assert_eq!(transitions.len(), record.num_lines as usize);
    assert!(rest.iter().all(|line| line.refer() == FrequencyRef::LSRK));
    (
        transitions
            .iter()
            .zip(rest)
            .zip(velocities)
            .map(|((name, rest), velocity)| (name.clone(), rest.hz(), velocity.value_m_s))
            .collect(),
        velocities.iter().map(|velocity| velocity.frame).collect(),
    )
}

#[test]
fn both_stores_state_a_fields_source_and_its_lines_by_window() {
    let root = tempfile::tempdir().expect("temp");
    let ms = fixture(root.path(), "ms_trial_lines.ms");
    let ps = fixture(root.path(), "ms_trial_lines.ps.zarr");
    let ours = open(&ms, &OpenOptions::default()).expect("msv2");
    let theirs = open(&ps, &OpenOptions::default()).expect("msv4");
    let (ours, theirs) = (ours.catalogue(), theirs.catalogue());

    // One source, XRADIO's name `NAME_SOURCEID`, with a record per window.
    assert_eq!(theirs.sources.len(), 1);
    assert_eq!(theirs.sources[0].name, "Lines_0");
    assert_eq!(ours.sources[0].name, "Lines");
    assert_eq!(theirs.sources[0].records.len(), 2);
    assert!(theirs.fields[0].source.is_some());
    for (first_hz, expected) in [(672.0e9, LINES[0]), (676.0e9, LINES[1])] {
        let (mine, xradio) = (
            record(ours, window_at(ours, first_hz)),
            record(theirs, window_at(theirs, first_hz)),
        );
        let expected: Vec<Line> = expected
            .iter()
            .map(|(name, rest, velocity)| (name.to_string(), *rest, *velocity))
            .collect();
        let (stated, frames) = lines(mine);
        assert_eq!(stated, expected);
        assert!(
            frames
                .iter()
                .all(|frame| *frame == Some(RadialVelocityRef::LSRK))
        );
        let (converted, frames) = lines(xradio);
        assert_eq!(converted, expected);
        assert!(frames.iter().all(Option::is_none), "no frame is invented");
        // XRADIO states the direction, at no time, and nothing else.
        let direction = |record: &SourceRecord| {
            let direction = record.direction.as_ref().expect("direction");
            (direction.as_angles(), direction.refer())
        };
        assert_eq!(direction(xradio), direction(mine));
        assert_eq!(direction(xradio).1, DirectionRef::J2000);
        assert!(xradio.time.is_none() && xradio.code.is_none());
    }

    ours.bind_to(theirs, &BindingTolerance::EXACT)
        .expect("the MSv2 binds to its conversion");
}
