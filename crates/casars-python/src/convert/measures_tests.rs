// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tests of CASA's measure argument conventions, with expectations from CASA
//! 6.7.6 `sm` probes (2026-10-21): the SOURCE TIME, FIELD PHASE_DIR and
//! ANTENNA POSITION each argument produced.

use casa_types::measures::direction::DirectionRef;
use casa_types::measures::{NamedSourceDirection, ObservatoryPosition};

use super::*;

/// Observatories and sources for the tests.
#[derive(Debug)]
struct Catalogue;

impl MeasuresProvider for Catalogue {
    fn observatory(&self, name: &str) -> Result<Option<ObservatoryPosition>, String> {
        Ok((name == "VLA").then_some(ObservatoryPosition::Itrf {
            x_m: -1_601_185.4,
            y_m: -5_041_977.5,
            z_m: 3_554_875.9,
        }))
    }

    fn source(&self, name: &str) -> Result<Option<NamedSourceDirection>, String> {
        Ok(name
            .eq_ignore_ascii_case("CASA")
            .then(|| NamedSourceDirection {
                reference: "J2000".to_string(),
                longitude_rad: 6.123_487_680_622_104,
                latitude_rad: 1.026_515_399_560_464_8,
            }))
    }
}

fn catalogue() -> Arc<dyn MeasuresProvider> {
    Arc::new(Catalogue)
}

fn text(value: &str) -> Variant {
    Variant::Text(value.to_string())
}

fn quantity_field(value: f64, unit: &str) -> Variant {
    Variant::Record(BTreeMap::from([
        ("value".to_string(), Variant::Number(value)),
        ("unit".to_string(), text(unit)),
    ]))
}

fn measure(kind: &str, refer: &str, values: &[(f64, &str)]) -> Variant {
    let mut record = BTreeMap::from([
        ("type".to_string(), text(kind)),
        ("refer".to_string(), text(refer)),
    ]);
    for (index, (value, unit)) in values.iter().enumerate() {
        record.insert(format!("m{index}"), quantity_field(*value, unit));
    }
    Variant::Record(record)
}

fn close(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-9 * left.abs().max(right.abs()).max(1.0)
}

/// The epoch as (UTC-or-not MJD days, reference).
fn read_epoch(value: Variant) -> (f64, EpochRef) {
    let epoch = epoch(&value).unwrap().unwrap();
    (epoch.value().as_mjd(), epoch.refer())
}

#[test]
fn epochs_read_as_casa_reads_them() {
    for value in [
        text("2020/01/01/00:00:00"),
        text("UTC 2020/01/01/00:00:00"),
        text("BOGUS 2020/01/01"),
        text("U 2020/01/01"),
        text("5084553600s"),
        Variant::Number(58849.0),
        quantity_field(58849.0, "d"),
        measure("epoch", "UTC", &[(58849.0, "d")]),
        measure("epoch", "UTC", &[(5_084_553_600.0, "s")]),
        Variant::Texts(vec!["UTC".to_string(), "2020/01/01".to_string()]),
    ] {
        let (mjd, refer) = read_epoch(value.clone());
        assert!(close(mjd, 58849.0) && refer == EpochRef::UTC, "{value:?}");
    }
    assert_eq!(read_epoch(text("TAI 2020/01/01")).1, EpochRef::TAI);
    assert_eq!(read_epoch(text("ut 2020/01/01")).1, EpochRef::UT1);
    assert_eq!(
        read_epoch(measure("epoch", "utc", &[(58849.5, "d")])),
        (58849.5, EpochRef::UTC)
    );
    // `[]` is MJD 0; "" is CASA's `false`.
    assert_eq!(read_epoch(Variant::Empty), (0.0, EpochRef::UTC));
    assert!(epoch(&text("")).unwrap().is_none());
}

#[test]
fn epochs_casa_refuses_raise_its_messages() {
    let message = |value: Variant| epoch(&value).unwrap_err().to_string();
    for value in ["12:30:00", "2020/01/01 12:00:00", "58849d"] {
        assert_eq!(
            message(text(value)),
            "Quantum::assure non-conforming unit type 'deg'",
            "{value}"
        );
    }
    assert!(message(text("garbage")).starts_with(EMPTY_HOLDER));
    assert!(message(text("UTC garbage")).starts_with(EMPTY_HOLDER));
    assert_eq!(
        message(measure("direction", "J2000", &[(0.0, "rad"), (0.0, "rad")])),
        "Empty or wrong MeasureHolder for asMEpoch"
    );
}

fn read_direction(value: Variant) -> MDirection {
    direction(&value, &catalogue()).unwrap().unwrap()
}

fn assert_angles(direction: &MDirection, longitude: f64, latitude: f64, refer: DirectionRef) {
    let (found_longitude, found_latitude) = direction.as_angles();
    let wrapped = (found_longitude - longitude)
        .sin()
        .atan2((found_longitude - longitude).cos());
    assert!(
        wrapped.abs() < 1e-12 && close(found_latitude, latitude) && direction.refer() == refer,
        "{direction:?} is not ({longitude}, {latitude}, {refer:?})"
    );
}

#[test]
fn directions_read_as_casa_reads_them() {
    let (ra, dec) = (std::f64::consts::PI, 34_f64.to_radians());
    for value in [
        text("J2000 12h00m00 34d00m00"),
        text("12h00m00 34d00m00"),
        text("J2000,12h00m00,34d00m00"),
        text("BOGUS 12h00m00 34d00m00"),
        text("J2000 12h00m00 34.00.00"),
        text("J2000 180deg 34deg"),
        Variant::Texts(vec!["J2000".into(), "12h00m00".into(), "34d00m00".into()]),
        measure("direction", "J2000", &[(180.0, "deg"), (34.0, "deg")]),
    ] {
        assert_angles(&read_direction(value), ra, dec, DirectionRef::J2000);
    }
    assert_angles(
        &read_direction(measure("direction", "j20", &[(1.0, "rad"), (0.5, "rad")])),
        1.0,
        0.5,
        DirectionRef::J2000,
    );
    assert_angles(
        &read_direction(text("GAL 120deg 30deg")),
        120_f64.to_radians(),
        30_f64.to_radians(),
        DirectionRef::GALACTIC,
    );
    // A colon latitude is a time-angle: 34 hours, 510 degrees, latitude 30.
    assert_angles(
        &read_direction(text("J2000 12:00:00 34:00:00")),
        0.0,
        30_f64.to_radians(),
        DirectionRef::J2000,
    );
    // A frame alone is its pole; a catalogue name is the source.
    assert_angles(
        &read_direction(text("J2000")),
        0.0,
        std::f64::consts::FRAC_PI_2,
        DirectionRef::J2000,
    );
    assert_angles(
        &read_direction(text("casa")),
        6.123_487_680_622_104,
        1.026_515_399_560_464_8,
        DirectionRef::J2000,
    );
}

#[test]
fn directions_casa_cannot_read_are_false_or_raise() {
    for value in [
        text("garbage"),
        text(""),
        Variant::Empty,
        Variant::Number(1.0),
    ] {
        assert!(
            direction(&value, &catalogue()).unwrap().is_none(),
            "{value:?}"
        );
    }
    let message = |value: &str| {
        direction(&text(value), &catalogue())
            .unwrap_err()
            .to_string()
    };
    assert_eq!(
        message("J2000 10h 20d"),
        "Quantum::cos illegal unit type 'h'"
    );
    assert_eq!(
        message("J2000 0.5 0.5"),
        "Quantum::cos illegal unit type ''"
    );
    // Divergence: CASA reuses the longitude for an unreadable latitude.
    assert!(message("J2000 12h00m00 garbage").contains("latitude"));
    assert!(message("JUPITER").contains("not yet implemented in casa-rs"));
}

fn read_position(value: Variant) -> [f64; 3] {
    position(&value, &catalogue()).unwrap().unwrap().as_itrf()
}

/// Positions agree to a millimetre.
fn assert_xyz(found: [f64; 3], expected: [f64; 3]) {
    for axis in 0..3 {
        assert!(
            (found[axis] - expected[axis]).abs() < 1e-3,
            "{found:?} != {expected:?}"
        );
    }
}

#[test]
fn positions_read_as_casa_reads_them() {
    let vla = [-1_601_185.4, -5_041_977.5, 3_554_875.9];
    assert_xyz(read_position(text("VLA")), vla);
    assert_xyz(read_position(text("vla")), vla);
    // `me.observatory('VLA')`: longitude, latitude and the geocentric radius.
    let (longitude, latitude, radius) = MPosition::new_itrf(vla[0], vla[1], vla[2]).as_spherical();
    let observatory = measure(
        "position",
        "ITRF",
        &[(longitude, "rad"), (latitude, "rad"), (radius, "m")],
    );
    assert_xyz(read_position(observatory), vla);
    let cartesian = measure(
        "position",
        "ITRF",
        &[(vla[0], "m"), (vla[1], "m"), (vla[2], "m")],
    );
    assert_xyz(read_position(cartesian), vla);
    // A string is a length, a longitude and a latitude.
    assert_xyz(
        read_position(text("ITRF 6378137m 0deg 0deg")),
        [6_378_137.0, 0.0, 0.0],
    );
    let tilted = read_position(text("6378137m 10deg 20deg"));
    assert_xyz(
        tilted,
        [5_902_433.719_096, 1_040_758.316_520, 2_181_451.330_891],
    );
    // A WGS84 record (`me.position('WGS84', '-107.6deg', '34.07deg', '2115m')`)
    // holds the height; CASA puts it at these ITRF x and y.
    let record = measure(
        "position",
        "WGS84",
        &[
            (-1.877_974_275_145_9, "rad"),
            (0.594_633_676_154_468, "rad"),
            (2115.0, "m"),
        ],
    );
    let wgs84 = position(&record, &catalogue()).unwrap().unwrap();
    assert_eq!(wgs84.refer(), casa_types::measures::PositionRef::WGS84);
    let itrf = wgs84.as_itrf();
    assert_xyz(itrf, [-1_599_737.633_737, -5_043_011.986_449, itrf[2]]);
}

#[test]
fn positions_casa_cannot_read_are_false_or_raise() {
    assert!(position(&Variant::Empty, &catalogue()).unwrap().is_none());
    let message = |value: &str| {
        position(&text(value), &catalogue())
            .unwrap_err()
            .to_string()
    };
    assert_eq!(
        message("NOWHERE"),
        "Could not understand Position parameter"
    );
    assert_eq!(
        message("ITRF -1601185.4m -5041977.5m 3554875.9m"),
        "Quantum::cos illegal unit type 'm'"
    );
    assert_eq!(
        message("WGS84 -107.6deg 34.07deg 2115m"),
        "Quantum::cos illegal unit type 'm'"
    );
}

/// `casaMFrequency` takes a first token that is no frame as LSRK, with a
/// warning, and reads the two tokens again as one, so that `"1.4 GHz"` is
/// 1.4 GHz; when they do not read as one, the second token's value stands,
/// and when the second token does not read, it raises (CASA 6.7.6 probes,
/// 2026-10-23).
#[test]
fn a_frequency_whose_frame_is_not_one_reads_its_tokens_again_as_one() {
    let warning = Some("Could not understand frequency frame..defaulting to LSRK".to_string());
    for (value, hz, warned) in [
        ("1.4 GHz", 1.4e9, &warning),
        ("foo 1.4GHz", 1.4e9, &warning),
        ("1.4 Hz", 1.4, &warning),
        ("1.4 m", 299_792_458.0 / 1.4, &warning),
        ("BARY 1.4GHz", 1.4e9, &None),
    ] {
        let (frequency, warning) = frequency(&text(value)).unwrap();
        let frequency = frequency.unwrap();
        let expected = if value.starts_with("BARY") {
            FrequencyRef::BARY
        } else {
            FrequencyRef::LSRK
        };
        assert_eq!(frequency.refer(), expected, "{value}");
        assert!(
            (frequency.hz() - hz).abs() <= 1e-12 * hz,
            "{value}: {} Hz",
            frequency.hz()
        );
        assert_eq!(&warning, warned, "{value}");
    }
    assert!(
        frequency(&text("1.4GHz foo"))
            .unwrap_err()
            .to_string()
            .starts_with(EMPTY_HOLDER)
    );
}

#[test]
fn frequency_codes_follow_casa() {
    assert_eq!(frequency_reference("").unwrap(), FrequencyRef::TOPO);
    assert_eq!(frequency_reference("lsrk").unwrap(), FrequencyRef::LSRK);
    assert_eq!(frequency_reference("BAR").unwrap(), FrequencyRef::BARY);
    assert_eq!(
        frequency_reference("bogus").unwrap_err().to_string(),
        "Invalid frequency reference 'BOGUS'"
    );
}
