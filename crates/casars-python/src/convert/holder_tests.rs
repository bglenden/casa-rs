// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tests of the `MeasureHolder` port, with expectations from CASA 6.7.6's
//! `me` tool (probed 2026-10-21).

use super::*;

fn text(value: &str) -> Variant {
    Variant::Text(value.to_string())
}

fn quantity_field(value: f64, unit: &str) -> Variant {
    Variant::Record(BTreeMap::from([
        ("value".to_string(), Variant::Number(value)),
        ("unit".to_string(), text(unit)),
    ]))
}

fn record(kind: &str, refer: &str, values: &[(f64, &str)]) -> BTreeMap<String, Variant> {
    let mut record = BTreeMap::from([
        ("type".to_string(), text(kind)),
        ("refer".to_string(), text(refer)),
    ]);
    for (index, (value, unit)) in values.iter().enumerate() {
        record.insert(format!("m{index}"), quantity_field(*value, unit));
    }
    record
}

fn quantity(text: &str) -> Quantity {
    Quantity::read(text).unwrap()
}

/// The `(m0, m1, m2)` values of a measure's record.
fn record_values(measure: &Measure) -> Vec<f64> {
    let CasaValue::Record(fields) = measure.to_value() else {
        panic!("a record");
    };
    fields
        .iter()
        .filter(|(key, _)| key.starts_with('m'))
        .map(|(_, value)| match value {
            CasaValue::Record(quantity) => match quantity[1].1 {
                CasaValue::Float(value) => value,
                _ => panic!("a float value"),
            },
            _ => panic!("a quantity record"),
        })
        .collect()
}

#[test]
fn reference_codes_resolve_as_casacore_resolves_them() {
    for (input, code) in [
        ("TT", "TDT"),
        ("IAT", "TAI"),
        ("ET", "TDT"),
        ("UT", "UT1"),
        ("GMST", "GMST1"),
        ("tai", "TAI"),
        ("ta", "TAI"),
    ] {
        assert_eq!(EPOCH_CODES.resolve(input), Some(code), "{input}");
    }
    for (input, code) in [
        ("AZELNE", "AZEL"),
        ("AZELNEGEO", "AZELGEO"),
        ("icrs", "ICRS"),
        ("j20", "J2000"),
        ("B1950_VLA", "B1950_VLA"),
        ("SUN", "SUN"),
    ] {
        assert_eq!(DIRECTION_CODES.resolve(input), Some(code), "{input}");
    }
    assert_eq!(EPOCH_CODES.resolve("U"), None, "UT1, UT2, UTC and UT");
    assert_eq!(POSITION_CODES.resolve_or_default("FOO"), "ITRF");
    assert_eq!(FREQUENCY_CODES.resolve_or_default("lsrk"), "LSRK");
}

#[test]
fn records_read_and_write_as_measure_holder_does() {
    let epoch = record("epoch", "UTC", &[(58849.0, "d")]);
    let read = Measure::from_record(&epoch).unwrap();
    assert_eq!(read.warning, None);
    assert_eq!(read.measure.refer, "UTC");
    assert_eq!(record_values(&read.measure), [58849.0]);
    assert_eq!(read.measure.kind().tell_me(), "Epoch");

    let unknown = record("position", "FOO", &[(0.0, "m"), (0.0, "m"), (1.0, "m")]);
    let read = Measure::from_record(&unknown).unwrap();
    assert_eq!(read.measure.refer, "ITRF");
    assert_eq!(
        read.warning.as_deref(),
        Some(
            "Illegal or unknown reference type 'FOO' for position definition. DEFAULT (ITRF) assumed."
        )
    );

    let not_a_measure = BTreeMap::from([("value".to_string(), Variant::Number(1.0))]);
    assert_eq!(
        Measure::from_record(&not_a_measure),
        Err(HolderError::NotAMeasure)
    );
    let unknown_kind = record("bogus", "UTC", &[]);
    assert!(
        Measure::from_record(&unknown_kind)
            .unwrap_err()
            .message()
            .starts_with("bogus is an unknown measure type")
    );
    let doppler = record("doppler", "RADIO", &[(0.0, "")]);
    assert!(
        Measure::from_record(&doppler)
            .unwrap_err()
            .message()
            .contains("not yet implemented in casa-rs")
    );
    let wrong_unit = record("epoch", "UTC", &[(1.0, "deg")]);
    assert_eq!(
        Measure::from_record(&wrong_unit).unwrap_err().message(),
        "Illegal quantity in MeasureHolder::fromRecord\n"
    );
}

#[test]
fn directions_keep_casacores_angles() {
    // `me.direction('J2000', '270deg', '20deg')`.
    let cosines = direction_cosines(
        quantity("270deg").get_si_value(),
        quantity("20deg").get_si_value(),
    );
    let (longitude, latitude) = direction_angles(cosines);
    assert_eq!(longitude, -1.570_796_326_794_896_8);
    assert_eq!(latitude, 0.349_065_850_398_865_9);
    // A latitude past the pole: `me.direction('J2000', '10deg', '100deg')`.
    let (longitude, latitude) = direction_angles(direction_cosines(
        quantity("10deg").get_si_value(),
        quantity("100deg").get_si_value(),
    ));
    assert_eq!(
        (longitude, latitude),
        (-2.967_059_728_390_360_4, 1.396_263_401_595_463_4)
    );
}

#[test]
fn positions_encode_heights_as_casacore_does() {
    let position = |values: &[&str]| {
        let quantities = values
            .iter()
            .map(|value| quantity(value))
            .collect::<Vec<_>>();
        Measure::from_values(MeasureKind::Position, "WGS84", &quantities).unwrap()
    };
    // A height of zero is a 1e-6 m vector.
    assert_eq!(
        record_values(&position(&["0m", "10deg", "20deg"])),
        [0.174_532_925_199_432_95, 0.349_065_850_398_865_9, 1e-6]
    );
    assert_eq!(
        record_values(&position(&["30m", "10deg", "20deg"])),
        [
            0.174_532_925_199_432_95,
            0.349_065_850_398_865_9,
            30.000_000_000_000_004
        ]
    );
    // Negative heights are shifted into the band at 743.568 m and back;
    // heights in the band itself move 1 mm up and stay there.
    for (height, read_back) in [(-100.0, -100.0), (2115.0, 2115.0), (743.5685, 743.5695)] {
        let measure = position(&[&format!("{height}m"), "10deg", "20deg"]);
        let (_, _, length) = position_spherical(match measure.value {
            MeasureValue::Position(xyz) => xyz,
            _ => unreachable!(),
        });
        assert!((length - read_back).abs() < 1e-6, "{height} {length}");
    }
    // Two angles then a length read too; an angle first then a length
    // second does not.
    assert_eq!(
        record_values(&position(&["10deg", "20deg", "30m"]))[2],
        30.000_000_000_000_004
    );
    let unreadable = [quantity("10deg"), quantity("30m"), quantity("20deg")];
    assert!(Measure::from_values(MeasureKind::Position, "WGS84", &unreadable).is_err());
}

#[test]
fn frequencies_read_any_frequency_like_unit() {
    let hz = |text: &str| frequency_hz(&quantity(text)).unwrap();
    assert_eq!(hz("1.4GHz"), 1.4e9);
    assert_eq!(hz("21cm"), 1_427_583_133.333_333_3);
    assert_eq!(hz("1s"), 1.0);
    assert!(frequency_hz(&quantity("1Jy")).is_none());
}

#[test]
fn casa_types_measures_round_trip() {
    let read = Measure::from_record(&record(
        "position",
        "WGS84",
        &[
            (-1.182_546_595_504_989_2, "rad"),
            (-0.401_825_164_011_307_2, "rad"),
            (5056.8, "m"),
        ],
    ))
    .unwrap()
    .measure;
    let CasaMeasure::Position(position) = read.to_casa().unwrap() else {
        panic!("a position");
    };
    assert_eq!(position.refer(), PositionRef::WGS84);
    assert!((position.values()[2] - 5056.8).abs() < 1e-9);
    assert_eq!(
        record_values(&Measure::from_casa(&CasaMeasure::Position(position)))[2],
        5056.8
    );
    let sun = Measure {
        refer: "SUN",
        value: MeasureValue::Direction([0.0, 0.0, 1.0]),
    };
    assert!(sun.to_casa().unwrap_err().contains("SUN direction frame"));
}
