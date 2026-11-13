// SPDX-License-Identifier: LGPL-3.0-or-later

use std::str::FromStr;

use casa_imaging_model::{FixedRestoringBeam, RestoringBeamPolicy};
use casa_types::quanta::{Quantity, Unit};
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::Value;

use super::{unexpected, unset};

/// `none` (each plane's fitted beam), `common`, or a beam's
/// `[major, minor, angle]`, read as `SynthesisParamsDeconv::fromRecord`
/// reads `restoringbeam`'s quantities: each with casacore's quantity
/// reader, as casacore's `GaussianBeam::setMajorMinor` and `setPA` accept
/// them (angular units, axes neither negative nor inverted). A zero axis is
/// casacore's null beam, which restores each plane with its fitted beam.
pub(super) fn restoring_beam<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<RestoringBeamPolicy, D::Error> {
    match Value::deserialize(deserializer)? {
        Value::String(text) if unset(&text) => Ok(RestoringBeamPolicy::PerPlane),
        Value::String(text) if text == "common" => Ok(RestoringBeamPolicy::Common),
        Value::Array(items) => {
            let texts = items
                .iter()
                .map(|item| item.as_str().ok_or_else(|| unexpected(item, "text")))
                .collect::<Result<Vec<_>, D::Error>>()?;
            let [major, minor, angle] = texts.as_slice() else {
                return Err(D::Error::custom(format!(
                    "restoringbeam {texts:?}: expected its major and minor axes and angle"
                )));
            };
            given_restoring_beam([major, minor, angle]).map_err(|_| {
                D::Error::custom(format!(
                    "Cannot construct a restoringbeam from supplied parameters {texts:?}: give \
                     angular axes, the major axis at least the minor, and an angular position \
                     angle, as CASA's tclean requires"
                ))
            })
        }
        value => Err(unexpected(
            &value,
            "none, common or a beam's axes and angle",
        )),
    }
}

/// A beam's `[major, minor, angle]` quantities, or the fitted beams for
/// casacore's null beam.
fn given_restoring_beam(texts: [&str; 3]) -> Result<RestoringBeamPolicy, ()> {
    let radians = Unit::from_str("rad").expect("a casacore unit");
    let [major, minor, angle] = texts.map(|text| {
        Quantity::read(text)
            .ok()
            .and_then(|quantity| quantity.get_value_in(&radians).ok())
            .filter(|value| value.is_finite())
            .ok_or(())
    });
    let [major, minor, angle] = [major?, minor?, angle?];
    if major < 0.0 || minor < 0.0 || major < minor {
        return Err(());
    }
    if major == 0.0 || minor == 0.0 {
        return Ok(RestoringBeamPolicy::PerPlane);
    }
    FixedRestoringBeam::new(major, minor, angle)
        .map(RestoringBeamPolicy::Fixed)
        .map_err(|_| ())
}
