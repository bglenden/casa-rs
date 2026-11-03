// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's `uvtaper`, read into the taper the weighting applies.

use std::str::FromStr;

use casa_imaging_model::UvTaper;
use casa_types::quanta::{Quantity, Unit};
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::Value;

use super::unexpected;

/// CASA's `uvtaper` (`synthesisimager::setweighting`, then
/// `VisImagingWeight::setFilter`): `[bmaj]`, `[bmaj, bmin]` or
/// `[bmaj, bmin, bpa]`, with `bmin = bmaj` and `bpa = 0` when not given,
/// and no taper when the list or its first entry is empty. In wavelengths
/// (`lambda`, `klambda`, …) the axes are the taper's uv-plane HWHM; as
/// angles they are its image-plane FWHM θ, whose uv-plane HWHM is
/// 2 ln 2 / (π θ). CASA reads an axis with no unit as radians and tapers
/// every weight to zero; casa-rs refuses it (`docs/CASA (C++) bugs.md`).
pub(super) fn uv_taper<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<UvTaper>, D::Error> {
    let entries = match Value::deserialize(deserializer)? {
        Value::Array(items) => items,
        value => vec![value],
    };
    let texts = entries
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::trim)
                .ok_or_else(|| unexpected(entry, "a quantity"))
        })
        .collect::<Result<Vec<_>, D::Error>>()?;
    let taper = match texts.as_slice() {
        [] | ["", ..] => return Ok(None),
        [bmaj] => uv_taper_axes(bmaj, bmaj, None),
        [bmaj, bmin] => uv_taper_axes(bmaj, bmin, None),
        [bmaj, bmin, bpa] => uv_taper_axes(bmaj, bmin, Some(bpa)),
        _ => Err("uvtaper takes at most [bmaj, bmin, bpa]".to_string()),
    };
    taper.map(Some).map_err(D::Error::custom)
}

/// One `uvtaper` axis: its uv-plane HWHM in wavelengths, and whether it was
/// given as an angle.
fn uv_taper_axis(text: &str) -> Result<(f64, bool), String> {
    let refused = || {
        format!(
            "uvtaper {text:?}: give an angle (the image-plane FWHM) or wavelengths (the uv-plane \
             HWHM, lambda or klambda)"
        )
    };
    let quantity: Quantity = text.parse().map_err(|_| refused())?;
    let unit = |name: &str| Unit::from_str(name).expect("a casacore unit");
    let hwhm = if let Ok(wavelengths) = quantity.get_value_in(&unit("lambda")) {
        (wavelengths, false)
    } else if let Ok(fwhm) = quantity.get_value_in(&unit("rad")) {
        (
            2.0 * std::f64::consts::LN_2 / (std::f64::consts::PI * fwhm),
            true,
        )
    } else {
        return Err(refused());
    };
    if quantity.unit().name().is_empty() || !(hwhm.0.is_finite() && hwhm.0 > 0.0) {
        return Err(refused());
    }
    Ok(hwhm)
}

fn uv_taper_axes(bmaj: &str, bmin: &str, bpa: Option<&str>) -> Result<UvTaper, String> {
    let (major, major_angle) = uv_taper_axis(bmaj)?;
    let (minor, minor_angle) = uv_taper_axis(bmin)?;
    if major_angle != minor_angle {
        return Err(format!(
            "uvtaper {bmaj:?}, {bmin:?}: give both axes as angles or both in wavelengths"
        ));
    }
    let position_angle_rad = match bpa {
        None => 0.0,
        Some(text) => text
            .parse::<Quantity>()
            .ok()
            .filter(|angle| !angle.unit().name().is_empty())
            .and_then(|angle| angle.get_value_in(&Unit::from_str("rad").ok()?).ok())
            .ok_or_else(|| format!("uvtaper position angle {text:?} needs an angle unit"))?,
    };
    Ok(UvTaper::new(major, minor, position_angle_rad))
}
