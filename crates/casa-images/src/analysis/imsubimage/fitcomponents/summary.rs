// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fmt::Write;

const ARCSEC_PER_RADIAN: f64 = 180. * 3600. / std::f64::consts::PI;

#[derive(Clone, Copy)]
pub(super) struct SummaryRow {
    pub(super) channel: usize,
    pub(super) peak: [f64; 2],
    pub(super) flux: [f64; 2],
    pub(super) direction: [f64; 2],
    pub(super) direction_errors: [f64; 2],
    /// Major, minor, angle and their respective errors, in radians.
    pub(super) convolved: [f64; 6],
    pub(super) deconvolved: [f64; 6],
    pub(super) frequency_hz: f64,
}

struct Column<'a> {
    title: &'a str,
    unit: &'a str,
    width: usize,
    signed: bool,
}

impl Column<'_> {
    fn value(&self, value: f64) -> String {
        let precision = self.width - if self.signed { 7 } else { 6 };
        let raw = if self.signed {
            format!("{value:+.*e}", precision)
        } else {
            format!("{value:.*e}", precision)
        };
        let value = raw
            .split_once('e')
            .and_then(|(mantissa, exp)| {
                let exponent = exp.parse::<i32>().ok()?;
                Some(format!("{mantissa}e{exponent:+03}"))
            })
            .unwrap_or(raw);
        format!("{value:>width$} ", width = self.width)
    }
}

pub(super) fn format_summary(
    rows: impl IntoIterator<Item = SummaryRow>,
    frame: &str,
    stokes: &str,
    flux_unit: &str,
    peak_unit: &str,
    beam: bool,
    spectral: bool,
) -> String {
    let equatorial = matches!(frame, "J2000" | "B1950");
    let longitude = format!("{}{frame}", if equatorial { "RA" } else { "Long" });
    let latitude = format!("{}{frame}", if equatorial { "Dec" } else { "Lat" });
    let longitude_error = format!("{longitude}err");
    let latitude_error = format!("{latitude}err");
    let flux_error = format!("{stokes}err");
    let mut columns = vec![
        column(stokes, flux_unit, 12, true),
        column(&flux_error, flux_unit, 12, false),
        column("Peak", peak_unit, 12, true),
        column("PeakErr", peak_unit, 12, false),
        column(&longitude, "deg", 16, true),
        column(&latitude, "deg", 16, true),
        column(&longitude_error, "arcsec", 16, false),
        column(&latitude_error, "arcsec", 16, false),
    ];
    columns.extend(shape_columns([
        "ConMaj",
        "ConMin",
        "ConPA",
        "ConMajErr",
        "ConMinErr",
        "ConPAErr",
    ]));
    if beam {
        columns.extend(shape_columns([
            "DeconMaj",
            "DeconMin",
            "DeconPA",
            "DeconMajErr",
            "DeconMinErr",
            "DeconPAErr",
        ]));
    }
    if spectral {
        columns.push(column("Freq", "GHz", 15, false));
    }

    let mut text = String::new();
    for names in [false, true] {
        let plane = if names { "Plane" } else { "" };
        let _ = write!(text, "# {plane:>6} ");
        for spec in &columns {
            let item = if names { spec.title } else { spec.unit };
            let _ = write!(text, "{item:>width$} ", width = spec.width);
        }
        text.push('\n');
    }
    for row in rows {
        let _ = write!(text, "  {:>6} ", row.channel);
        let values = row
            .flux
            .into_iter()
            .chain(row.peak)
            .chain(row.direction.map(f64::to_degrees))
            .chain(row.direction_errors.map(|angle| angle * ARCSEC_PER_RADIAN))
            .chain(shape_values(row.convolved))
            .chain(
                beam.then(|| shape_values(row.deconvolved))
                    .into_iter()
                    .flatten(),
            )
            .chain(spectral.then_some(row.frequency_hz / 1e9));
        for (spec, value) in columns.iter().zip(values) {
            text.push_str(&spec.value(value));
        }
        text.push('\n');
    }
    text
}

fn column<'a>(title: &'a str, unit: &'a str, minimum: usize, signed: bool) -> Column<'a> {
    Column {
        title,
        unit,
        width: minimum.max(title.len()).max(unit.len()),
        signed,
    }
}

fn shape_columns(titles: [&str; 6]) -> [Column<'_>; 6] {
    std::array::from_fn(|i| {
        column(
            titles[i],
            if i % 3 == 2 { "deg" } else { "arcsec" },
            12,
            i == 2,
        )
    })
}

fn shape_values(axes: [f64; 6]) -> [f64; 6] {
    std::array::from_fn(|i| {
        if i % 3 == 2 {
            axes[i].to_degrees()
        } else {
            axes[i] * ARCSEC_PER_RADIAN
        }
    })
}
