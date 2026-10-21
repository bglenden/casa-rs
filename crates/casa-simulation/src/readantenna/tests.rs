// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unit tests of `readantenna` on small inline configuration files. The
//! comparison with CASA's own results is `tests/readantenna_casa.rs`.

use casa_types::measures::ObservatoryPosition;

use super::geodesy::{self, WGS84};
use super::*;

/// An observatory table: case-insensitive exact names, trimmed queries
/// (as casa-measures-data looks names up). Positions are CASA's.
#[derive(Debug)]
struct Observatories(Vec<(&'static str, ObservatoryPosition)>);

impl MeasuresProvider for Observatories {
    fn observatory(&self, name: &str) -> Result<Option<ObservatoryPosition>, String> {
        let name = name.trim();
        Ok(self
            .0
            .iter()
            .find(|(entry, _)| entry.eq_ignore_ascii_case(name))
            .map(|(_, position)| position.clone()))
    }
}

const ALMA: (f64, f64, f64) = (-1.182_546_595_504_989_2, -0.401_825_164_011_307_2, 5056.8);

fn observatories() -> Observatories {
    Observatories(vec![
        (
            "ALMA",
            ObservatoryPosition::Wgs84 {
                longitude_rad: ALMA.0,
                latitude_rad: ALMA.1,
                height_m: ALMA.2,
            },
        ),
        (
            "VLA",
            ObservatoryPosition::Itrf {
                x_m: -1_601_185.365,
                y_m: -5_041_977.547,
                z_m: 3_554_875.87,
            },
        ),
        (
            "GB",
            ObservatoryPosition::Wgs84 {
                longitude_rad: -1.390_9,
                latitude_rad: 0.670_4,
                height_m: 880.0,
            },
        ),
        (
            "GBT",
            ObservatoryPosition::Wgs84 {
                longitude_rad: -1.391_0,
                latitude_rad: 0.670_5,
                height_m: 880.0,
            },
        ),
    ])
}

fn read(text: &str) -> Result<AntennaConfigFile, ReadAntennaError> {
    read_antenna_config_str(text, &observatories())
}

fn message(text: &str) -> String {
    read(text).expect_err("the file is rejected").to_string()
}

const VLA_ROWS: &str = "\
-1601188.989351 -5042000.518599 3554843.384480 25. W01
-1601225.230987 -5041980.390730 3554855.657987 25. W02
-1601265.110332 -5041982.563379 3554834.816409 25. W03
";

const LOC_ROWS: &str = "\
-5.850273514   -125.9985379    -1.590364043   12.   A058     DA41
-19.90369337    52.82680653    -1.892119601   12.   A023     DA42
13.45860758    -5.790196849    -2.087805181   12.   A035     DA43
";

fn assert_close(got: &[f64], want: &[f64], tolerance: f64) {
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(want) {
        assert!((g - w).abs() <= tolerance, "{got:?} vs {want:?}");
    }
}

#[test]
fn xyz_rows_with_four_five_and_six_columns_and_inline_comments() {
    let config = read(
        "# observatory=VLA\n\
         ## coordsys = XYZ #\n\
         # x y z diam pad ant\n\
         -1601188.989351 -5042000.518599 3554843.384480 25. W01 EA01 # six\n\
         -1601225.230987\t-5041980.390730\t3554855.657987\t25.\tW02\t# five\n\
         -1601265.110332 -5041982.563379 3554834.816409 25 # four\n\
         -1601315.874282 -5041985.324465 3554808.263784 2.5e1 W04 EA04 extra\n",
    )
    .expect("reads");
    assert_eq!(config.coordinates, ConfigCoordinates::Xyz);
    assert_eq!(
        config.x,
        [
            -1_601_188.989_351,
            -1_601_225.230_987,
            -1_601_265.110_332,
            -1_601_315.874_282
        ]
    );
    assert_eq!(config.z[3], 3_554_808.263_784);
    assert_eq!(config.dish_diameter, [25.0; 4]);
    assert_eq!(config.pad_names, ["W01", "W02", "A02", "W04"]);
    assert_eq!(config.antenna_names, ["EA01", "W02", "A02", "EA04"]);
    assert_eq!(config.telescope, "VLA");
    assert_eq!(config.observatory_source, ObservatorySource::Catalog);
    assert_eq!(config.observatory.refer(), PositionRef::WGS84);
    let [lon, lat, height] = config.observatory.values();
    // CASA's me.measure(me.observatory('VLA'), 'WGS84').
    assert!((lon + 1.878_294_258_139_436_2).abs() < 1e-12);
    assert!((lat - 0.594_787_497_912_807_3).abs() < 1e-12);
    assert!((height - 2_114.890_229_039_825).abs() < 1e-6);
}

#[test]
fn blank_and_short_lines_add_no_antennas() {
    let config = read(&format!(
        "# observatory = VLA\n# coordsys = XYZ\n\n# comment\n\n{VLA_ROWS}one two\n\n"
    ))
    .expect("reads");
    assert_eq!(config.x.len(), 3);
    assert_eq!(config.pad_names, ["W01", "W02", "W03"]);
    assert_eq!(config.antenna_names, config.pad_names);
}

#[test]
fn carriage_return_line_ends_split_lines() {
    let text = format!(
        "# observatory=VLA\r# coordsys=XYZ\r\n{}",
        VLA_ROWS.replace('\n', "\r")
    );
    let config = read(&text).expect("reads");
    assert_eq!(config.x.len(), 3);
    assert_eq!(config.coordinates, ConfigCoordinates::Xyz);
}

#[test]
fn local_coordinates_about_a_known_observatory() {
    let config = read(&format!(
        "# observatory=ALMA\n# coordsys=LOC (local tangent plane)\n{LOC_ROWS}"
    ))
    .expect("reads");
    assert_eq!(config.coordinates, ConfigCoordinates::Local);
    assert_eq!(config.observatory_source, ObservatorySource::Catalog);
    assert_eq!(config.pad_names, ["A058", "A023", "A035"]);
    assert_eq!(config.antenna_names, ["DA41", "DA42", "DA43"]);
    let (lat, lon) = (ALMA.1.to_degrees(), ALMA.0.to_degrees());
    let first = geodesy::local_to_itrf(
        lat,
        lon,
        ALMA.2,
        [-5.850_273_514, -125.998_537_9, -1.590_364_043],
    );
    assert_close(&[config.x[0], config.y[0], config.z[0]], &first, 1e-9);
    // The antenna sits its local offset away from the observatory.
    let origin = MPosition::new_wgs84(ALMA.0, ALMA.1, ALMA.2).as_itrf();
    let distance = ((config.x[0] - origin[0]).powi(2)
        + (config.y[0] - origin[1]).powi(2)
        + (config.z[0] - origin[2]).powi(2))
    .sqrt();
    let offset =
        (5.850_273_514_f64.powi(2) + 125.998_537_9_f64.powi(2) + 1.590_364_043_f64.powi(2)).sqrt();
    assert!((distance - offset).abs() < 1e-3, "{distance} vs {offset}");
}

#[test]
fn lower_case_names_and_coordinate_systems_match() {
    let config = read(&format!("# observatory=alma\n# coordsys=loc\n{LOC_ROWS}")).expect("reads");
    assert_eq!(config.coordinates, ConfigCoordinates::Local);
    assert_eq!(config.telescope, "alma");
    assert_eq!(config.observatory_source, ObservatorySource::Catalog);
}

#[test]
fn cofa_places_an_unknown_observatory() {
    let config = read(&format!(
        "# observatory=ZZQQ\n# COFA=-67.75,-23.02\n# coordsys=LOC\n{LOC_ROWS}"
    ))
    .expect("reads");
    assert_eq!(config.observatory_source, ObservatorySource::Cofa);
    assert_eq!(config.telescope, "ZZQQ");
    let [lon, lat, height] = config.observatory.values();
    assert_eq!(
        (lon, lat, height),
        ((-67.75_f64).to_radians(), (-23.02_f64).to_radians(), 0.0)
    );
    let first = geodesy::local_to_itrf(
        lat.to_degrees(),
        lon.to_degrees(),
        0.0,
        [-5.850_273_514, -125.998_537_9, -1.590_364_043],
    );
    assert_close(&[config.x[0], config.y[0], config.z[0]], &first, 1e-9);
}

#[test]
fn cofa_overrides_a_known_observatory() {
    let config = read(&format!(
        "# observatory=ALMA\n# COFA=-67.70, -23.00\n# coordsys=LOC\n{LOC_ROWS}"
    ))
    .expect("reads");
    assert_eq!(config.observatory_source, ObservatorySource::Cofa);
    let [lon, lat, _] = config.observatory.values();
    assert_eq!(
        (lon, lat),
        ((-67.7_f64).to_radians(), (-23.0_f64).to_radians())
    );
    assert_eq!(
        message("# observatory=ALMA\n# COFA=-67.7\n# coordsys=LOC\n"),
        "COFA must be longitude,latitude in degrees (COFA=-67.7)"
    );
}

#[test]
fn local_coordinates_need_an_observatory() {
    let expected = "To use local coordinates in the antenna position file, you must either use a \
                    known observatory name, or provide the COFA explicitly";
    assert_eq!(
        message(&format!("# observatory=ZZQQ\n# coordsys=LOC\n{LOC_ROWS}")),
        expected
    );
    assert_eq!(message(&format!("# coordsys=LOCAL\n{LOC_ROWS}")), expected);
}

#[test]
fn utm_rows_convert_with_the_datum() {
    let config = read(
        "# observatory=ALMA\n# coordsys=UTM\n# datum=SAM56\n# zone=19\n# hemisphere=S\n\
         627789.81  7453079.62  5029.4  12.0       1\n\
         627806.26  7453082.62  5028.7  12.0       2\n",
    )
    .expect("reads");
    assert_eq!(config.coordinates, ConfigCoordinates::Utm);
    assert_eq!(config.pad_names, ["1", "2"]);
    let datum = geodesy::datum("SAM56").expect("SAM56");
    let first = geodesy::utm_to_xyz(627_789.81, 7_453_079.62, 5029.4, 19, datum, true);
    assert_close(&[config.x[0], config.y[0], config.z[0]], &first, 0.0);
    // The pads are on the Chajnantor plateau, a few km from ALMA's centre.
    let centre = MPosition::new_wgs84(ALMA.0, ALMA.1, ALMA.2).as_itrf();
    let distance = ((first[0] - centre[0]).powi(2)
        + (first[1] - centre[1]).powi(2)
        + (first[2] - centre[2]).powi(2))
    .sqrt();
    assert!(distance < 10_000.0, "{distance}");
}

#[test]
fn utm_headers_are_required_in_casas_order() {
    let rows = "627789.81  7453079.62  5029.4  12.0\n";
    let base = "# observatory=ALMA\n# coordsys=UTM\n";
    assert_eq!(
        message(&format!("{base}# datum=SAM56\n# hemisphere=S\n{rows}")),
        "You must specify zone=NN in your antenna file"
    );
    assert_eq!(
        message(&format!("{base}# zone=19\n# hemisphere=S\n{rows}")),
        "You must specify datum in your antenna file"
    );
    assert_eq!(
        message(&format!("{base}# zone=19\n# datum=SAM56\n{rows}")),
        "You must specify hemisphere=N|S in your antenna file"
    );
    assert_eq!(
        message(&format!(
            "{base}# zone=19\n# datum=SAM56\n# hemisphere=\n{rows}"
        )),
        "You must specify hemisphere=N|S in your antenna file"
    );
    assert_eq!(
        message(&format!(
            "{base}# zone=19S\n# datum=SAM56\n# hemisphere=S\n{rows}"
        )),
        "You must specify zone=NN in your antenna file (zone=19S)"
    );
    assert_eq!(
        message(&format!(
            "{base}# zone=19\n# datum=sam56\n# hemisphere=S\n{rows}"
        )),
        "unknown datum sam56"
    );
}

#[test]
fn the_coordinate_system_is_required_and_must_be_known() {
    assert_eq!(
        message(&format!("# observatory=VLA\n{VLA_ROWS}")),
        "Must specify XYZ, UTM or LOC coordinate system in antenna file header"
    );
    // Only the text before the '=' is the key.
    assert_eq!(
        message(&format!(
            "# observatory=VLA\n# note: coordsys=XYZ\n{VLA_ROWS}"
        )),
        "Must specify XYZ, UTM or LOC coordinate system in antenna file header"
    );
    assert_eq!(
        message(&format!(
            "# observatory=VLA\n# coordsys=XYZ # earth-centred\n{VLA_ROWS}"
        )),
        "Must specify XYZ, UTM or LOC coordinate system in antenna file header \
         (coordsys=XYZ  earth-centred)"
    );
}

#[test]
fn a_bad_number_is_an_error() {
    let text = "# observatory=VLA\n# coordsys=XYZ\n1 2 3 4\n1 2 3 big\n";
    assert_eq!(
        message(text),
        "Could not read file: line 4: could not convert string to float: 'big'"
    );
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("bad.cfg");
    std::fs::write(&path, text).expect("writes");
    let error = read_antenna_config(&path, &observatories()).expect_err("bad row");
    assert_eq!(
        error.to_string(),
        format!(
            "Could not read file: '{}': line 4: could not convert string to float: 'big'",
            path.display()
        )
    );
}

#[test]
fn an_unknown_observatory_sits_at_the_mean_antenna_position() {
    for header in [
        "# observatory=ZZQQ\n",
        "",
        "# observatory=xALMAx\n",
        "# observatory=GBTX\n",
    ] {
        let config = read(&format!("{header}# coordsys=XYZ\n{VLA_ROWS}")).expect("reads");
        assert_eq!(
            config.observatory_source,
            ObservatorySource::MeanAntennaPosition,
            "{header}"
        );
        let mean = |v: &[f64]| v.iter().sum::<f64>() / 3.0;
        let (lon, lat, height) =
            geodesy::xyz_to_geodetic(mean(&config.x), mean(&config.y), mean(&config.z), &WGS84)
                .expect("valid");
        assert_eq!(config.observatory.values(), [lon, lat, height]);
        // SOFA's gc2gd through casa-types agrees.
        let sofa = MPosition::new_itrf(mean(&config.x), mean(&config.y), mean(&config.z))
            .convert_to(PositionRef::WGS84)
            .expect("converts")
            .values();
        assert_close(&[lon, lat], &sofa[..2], 1e-13);
        assert!((height - sofa[2]).abs() < 1e-6);
    }
    let config = read(&format!("# coordsys=XYZ\n{VLA_ROWS}")).expect("reads");
    assert_eq!(config.telescope, SIMULATED_TELESCOPE);
}

#[test]
fn prefix_names_resolve_as_casacore_resolves_them() {
    for name in ["ALMASD", "ALMA-foo", "ALMA SD", "almaX"] {
        let config =
            read(&format!("# observatory={name}\n# coordsys=LOC\n{LOC_ROWS}")).expect("reads");
        assert_eq!(
            config.observatory_source,
            ObservatorySource::Catalog,
            "{name}"
        );
        assert_close(&config.observatory.values(), &[ALMA.0, ALMA.1, ALMA.2], 0.0);
    }
    // GB and GBT are both prefixes of GBTX: casacore resolves neither.
    let expected = "To use local coordinates in the antenna position file, you must either use a \
                    known observatory name, or provide the COFA explicitly";
    assert_eq!(
        message(&format!("# observatory=GBTX\n# coordsys=LOC\n{LOC_ROWS}")),
        expected
    );
    assert_eq!(
        message(&format!("# observatory=xALMAx\n# coordsys=LOC\n{LOC_ROWS}")),
        expected
    );
}

#[test]
fn a_file_without_antennas() {
    let config = read("# observatory=VLA\n# coordsys=XYZ\n").expect("reads");
    assert!(config.x.is_empty() && config.pad_names.is_empty());
    assert_eq!(config.observatory_source, ObservatorySource::Catalog);
    assert_eq!(
        message("# observatory=ZZQQ\n# coordsys=XYZ\n"),
        "Cannot place an unknown observatory without COFA: the file has no antennas"
    );
}

#[test]
fn the_header_is_returned() {
    let config = read(&format!(
        "# observatory=VLA\n# coordsys=XYZ\n# comment=kept\n{VLA_ROWS}"
    ))
    .expect("reads");
    assert_eq!(
        config.header.get("comment").map(String::as_str),
        Some("kept")
    );
    assert_eq!(config.header.len(), 3);
}

#[test]
fn a_missing_file_names_its_path() {
    let error = read_antenna_config(Path::new("/nonexistent/antennas.cfg"), &observatories())
        .expect_err("missing");
    assert!(
        error
            .to_string()
            .starts_with("Could not read file: '/nonexistent/antennas.cfg': ")
    );
}
