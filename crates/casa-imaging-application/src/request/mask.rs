// SPDX-License-Identifier: LGPL-3.0-or-later

//! Typed mask inputs and the geometry supported for image lists.

use std::path::PathBuf;

use casa_imaging_model::PolarizationCoordinate;
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::Value;

use super::{
    Deconvolver, Gridder, ImagingRequest, InvalidRequest, SpecMode, SpectralMaskRange, UserMaskBox,
    comma_list, optional, unexpected, unset,
};

impl ImagingRequest {
    /// Whether an image is combined with a CRTF region of limited spectral range.
    pub(crate) fn has_ranged_image_mask(&self) -> bool {
        self.mask_images.len() == 1 && self.mask_box.iter().any(|region| region.range.is_some())
    }

    /// Keep image mask combinations within their compared geometries.
    pub(crate) fn validate_mask_image_list(&self) -> Result<(), InvalidRequest> {
        let standard_hogbom = self.stokes.as_slice() == [PolarizationCoordinate::StokesI]
            && matches!(self.gridder, Gridder::Standard { facets } if facets.get() == 1)
            && self.deconvolver == Deconvolver::Hogbom
            && self.niter > 0
            && self.outlierfile.is_none();
        if self.mask_images.len() > 1
            && (!self.mask_box.is_empty()
                || self.mask_polygon.is_some()
                || self.specmode != SpecMode::Mfs
                || !standard_hogbom)
        {
            return Err(InvalidRequest {
                parameters: "mask_image",
                reason: "multiple mask images support standard Stokes I MFS Högbom cleaning without region masks or outliers (#823)",
            });
        }
        if self.has_ranged_image_mask() && (!standard_hogbom || self.specmode != SpecMode::Cube) {
            return Err(InvalidRequest {
                parameters: "mask_image",
                reason: "a mask image with a spectral-range region supports standard Stokes I Högbom cubes (#823)",
            });
        }
        Ok(())
    }
}

/// The native single mask path, or the list of paths resolved by tclean.
pub(super) fn mask_images<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<PathBuf>, D::Error> {
    match Value::deserialize(deserializer)? {
        value @ Value::Array(_) => Vec::<PathBuf>::deserialize(value).map_err(D::Error::custom),
        value => optional::<_, PathBuf>(value)
            .map(|path| path.into_iter().collect())
            .map_err(D::Error::custom),
    }
}

/// Boxes `x0,y0,x1,y1` separated by `;`, each limited to a spectral range
/// by an optional `@<low>~<high>`, both ends in `Hz` or both in `pix`.
pub(super) fn mask_boxes<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<UserMaskBox>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    let text = value
        .as_str()
        .ok_or_else(|| unexpected(&value, "pixel boxes"))?;
    if unset(text) {
        return Ok(Vec::new());
    }
    text.split(';')
        .map(|item| {
            let (corners, range) = match item.split_once('@') {
                Some((corners, range)) => (corners, Some(spectral_mask_range(range)?)),
                None => (item, None),
            };
            let corners = comma_list::<i64>(corners)?
                .try_into()
                .map_err(|_| format!("a mask box has four corners, not {corners:?}"))?;
            Ok(UserMaskBox { corners, range })
        })
        .collect::<Result<_, String>>()
        .map_err(D::Error::custom)
}

/// A mask box's `<low>~<high>` range: two finite values, both in `Hz` or
/// both in `pix`.
fn spectral_mask_range(text: &str) -> Result<SpectralMaskRange, String> {
    let (low, high) = text
        .split_once('~')
        .ok_or_else(|| format!("a mask box range is <low>~<high>, not {text:?}"))?;
    let end = |end: &str, unit: &str| {
        end.trim()
            .strip_suffix(unit)
            .and_then(|number| number.trim().parse::<f64>().ok())
            .filter(|number| number.is_finite())
    };
    match (
        end(low, "Hz"),
        end(high, "Hz"),
        end(low, "pix"),
        end(high, "pix"),
    ) {
        (Some(low), Some(high), _, _) => Ok(SpectralMaskRange::Frequency([low, high])),
        (_, _, Some(low), Some(high)) => Ok(SpectralMaskRange::Pixel([low, high])),
        _ => Err(format!(
            "a mask box range has two finite ends, both in Hz or both in pix, not {text:?}"
        )),
    }
}
