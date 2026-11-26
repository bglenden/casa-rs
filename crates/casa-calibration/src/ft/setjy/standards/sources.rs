// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA named calibrators and its optional one-arcsecond cone search.

use std::path::Path;

use casa_core::ScalarValue;
use casa_core::measures::direction::MDirection;
use casa_ms::MsResult;
use casa_tables::{Table, TableOptions};

use crate::ft::invalid;

// CASA uppercases the field, then tests each published alias verbatim.
const CALIBRATORS: &[(&str, &[&str])] = &[
    (
        "3C286",
        &[
            "3C286",
            "3C 286",
            "3C_286",
            "1328+307",
            "1331+305",
            "J1331+3030",
        ],
    ),
    (
        "3C48",
        &[
            "3C48",
            "3C 48",
            "3C_48",
            "0134+329",
            "0137+331",
            "J0137+3309",
        ],
    ),
    (
        "3C147",
        &[
            "3C147",
            "3C 147",
            "3C_147",
            "0538+498",
            "0542+498",
            "J0542+4951",
        ],
    ),
    (
        "3C138",
        &[
            "3C138",
            "3C 138",
            "3C_138",
            "0518+165",
            "0521+166",
            "J0521+1638",
        ],
    ),
    ("1934-638", &["1934-638", "J1939-6342"]),
    (
        "3C295",
        &[
            "3C295",
            "3C 295",
            "3C_295",
            "1409+524",
            "1411+522",
            "J1411+5212",
        ],
    ),
    (
        "3C196",
        &[
            "3C196",
            "3C 196",
            "3C_196",
            "0809+483",
            "0813+482",
            "J0813+4813",
        ],
    ),
    (
        "3C123",
        &[
            "3C123",
            "3C 123",
            "3C_123",
            "0433+295",
            "0437+296",
            "J0437+2940",
        ],
    ),
    (
        "3C380",
        &[
            "3C380",
            "3C 380",
            "3C_380",
            "1828+487",
            "1829+487",
            "J1829+4845",
        ],
    ),
    ("0133-3629", &["0133-3629"]),
    (
        "FORNAX_A",
        &[
            "FORNAX_A",
            "FORNAX A",
            "FOR-A",
            "FOR A",
            "ForA",
            "J0322-3712",
        ],
    ),
    ("J0444-2809", &["J0444-2809"]),
    (
        "PICTOR_A",
        &[
            "PICTOR_A",
            "PICTOR A",
            "PIC-A",
            "PIC A",
            "PicA",
            "J0519-4546",
        ],
    ),
    (
        "TAURUS_A",
        &[
            "TAURUS_A",
            "TAURUS A",
            "TAU-A",
            "TAU A",
            "TauA",
            "J0534+2200",
            "3C144",
            "3C 144",
            "3C_144",
            "CRAB",
        ],
    ),
    (
        "HYDRA_A",
        &[
            "HYDRA_A",
            "HYDRA A",
            "HYA-A",
            "HYA A",
            "HyaA",
            "J0918-1205",
            "3C218",
            "3C 218",
            "3C_218",
        ],
    ),
    (
        "VIRGO_A",
        &[
            "VIRGO_A",
            "VIRGO A",
            "VIR-A",
            "VIR A",
            "VirA",
            "J1230+1223",
            "3C274",
            "3C 274",
            "3C_274",
            "M87",
        ],
    ),
    (
        "HERCULES_A",
        &[
            "HERCULES_A",
            "HERCULES A",
            "HER-A",
            "HER A",
            "HerA",
            "J1651+0459",
            "3C348",
            "3C 348",
            "3C_348",
        ],
    ),
    ("3C353", &["3C353", "3C 353", "3C_353", "J1720-0059"]),
    (
        "CYGNUS_A",
        &[
            "CYGNUS_A",
            "CYGNUS A",
            "CYG-A",
            "CYG A",
            "CygA",
            "J1959+4044",
            "3C405",
            "3C 405",
            "3C_405",
        ],
    ),
    ("3C444", &["3C444", "3C 444", "3C_444", "J2214-1701"]),
    (
        "CASSIOPEIA_A",
        &[
            "CASSIOPEIA_A",
            "CASSIOPEIA A",
            "CAS-A",
            "CAS A",
            "CasA",
            "J2323+5848",
            "3C461",
            "3C 461",
            "3C_461",
        ],
    ),
];

pub(super) fn identify(
    field: &str,
    direction: &MDirection,
    cone_table: Option<&Path>,
) -> MsResult<Option<&'static str>> {
    if let Some(name) = named(field) {
        return Ok(Some(name));
    }
    let Some(path) = cone_table else {
        return Ok(None);
    };
    let table = Table::open(TableOptions::new(path))?;
    let name = table.column_accessor("Name")?;
    let ra = table.column_accessor("RA_J2000")?;
    let dec = table.column_accessor("Dec_J2000")?;
    let lon = direction.longitude_rad();
    let lat = direction.latitude_rad();
    let cone = (0.5_f64.to_radians() / 3600.).sin().powi(2);
    for row in 0..table.row_count() {
        let (ScalarValue::Float64(ra), ScalarValue::Float64(dec)) =
            (ra.scalar_cell(row)?, dec.scalar_cell(row)?)
        else {
            return Err(invalid(
                "flux calibrator direction must be in double-precision radians",
            ));
        };
        let haversine = ((dec - lat) * 0.5).sin().powi(2)
            + dec.cos() * lat.cos() * ((ra - lon) * 0.5).sin().powi(2);
        if haversine > cone {
            continue;
        }
        let ScalarValue::String(name) = name.scalar_cell(row)? else {
            return Err(invalid("flux calibrator Name must be a string"));
        };
        return Ok(CALIBRATORS
            .iter()
            .map(|(known, _)| *known)
            .find(|known| name == known));
    }
    Ok(None)
}

fn named(field: &str) -> Option<&'static str> {
    let field = field.to_ascii_uppercase();
    CALIBRATORS.iter().find_map(|&(name, aliases)| {
        aliases
            .iter()
            .any(|alias| field.contains(*alias))
            .then_some(name)
    })
}

/// The reference positions compiled into CASA's FluxStdSrcs.
pub fn source_direction(field: &str) -> Option<MDirection> {
    use casa_core::measures::direction::DirectionRef;

    // fluxcalibrator.data is used for positional source identification.
    // The component still uses the calibrated source's fixed J2000 position.
    let [ra, dec] = match named(field)? {
        "3C286" => [3.539257626070549, 0.5324850225220917],
        "3C48" => [0.4262457643630985, 0.5787463318245085],
        "3C147" => [1.4948817765383597, 0.8700805690768509],
        "3C138" => [1.401346673041897, 0.2904130912582342],
        "1934-638" => [5.146176021557448, -1.1119977478136984],
        "3C295" => [3.7146787856873478, 0.9111103509091509],
        "3C196" => [2.1537362969610023, 0.8415541320803659],
        "3C123" => [1.2089586878736391, 0.5178480078663621],
        "3C380" => [4.84123669, 0.85078092],
        "0133-3629" => [0.40998511349868466, -0.636928821694464],
        "FORNAX_A" => [0.8844302540951193, -0.649405598740554],
        "J0444-2809" => [1.2419253902826484, -0.49157683196101404],
        "PICTOR_A" => [1.3955160370653494, -0.7989916117952379],
        "TAURUS_A" => [1.4596748494230258, 0.3842250233666105],
        "HYDRA_A" => [2.4351465533014114, -0.21110704143990625],
        "VIRGO_A" => [-3.0070987886171765, 0.2162659001873615],
        "HERCULES_A" => [-1.871273156204919, 0.08713711430959646],
        "3C353" => [-1.7432823443920011, -0.017099960309150668],
        "CYGNUS_A" => [-1.0494987241821205, 0.7109409485219165],
        "3C444" => [-0.46063951349110815, -0.2971727999325764],
        "CASSIOPEIA_A" => [-0.15969762655748126, 1.0265153995604648],
        _ => return None,
    };
    Some(MDirection::from_angles(ra, dec, DirectionRef::J2000))
}
