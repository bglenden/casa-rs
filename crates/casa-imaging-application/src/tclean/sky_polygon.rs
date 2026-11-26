// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::Arc;

use casa_core::quanta::{Quantity, Unit};
use serde::{Deserialize, Serialize};

use super::TcleanRefusal;

/// An unqualified CRTF sky region encoded as angular polygon vertices in radians.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct MaskPolygon {
    pub(crate) vertices: Arc<[[f64; 2]]>,
    /// Explicit J2000 overrides the output direction coordinate's frame.
    pub(crate) j2000: bool,
}

pub(super) fn parse_sky_region(text: &str) -> Result<Option<MaskPolygon>, TcleanRefusal> {
    let mut lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .peekable();
    // A pixel rectangle keeps the existing CRTF pixel-box path.
    let Some(shape) = lines.clone().find_map(|line| {
        if line.starts_with("poly") {
            Some("poly")
        } else if line.starts_with("box") && !line.contains("pix") {
            Some("box")
        } else {
            None
        }
    }) else {
        return Ok(None);
    };
    let refuse = || {
        TcleanRefusal::not_yet(
            "mask",
            "CRTF sky masks take one polygon or box with the image's frame or coord=J2000",
            823,
        )
    };
    let mut j2000 = false;
    if let Some(global) = lines.peek().and_then(|line| {
        line.strip_prefix("global")
            .filter(|tail| tail.starts_with(char::is_whitespace))
    }) {
        if !j2000_frame(global) {
            return Err(refuse());
        }
        j2000 = true;
        lines.next();
    }
    let mut rest = lines
        .next()
        .and_then(|line| line.strip_prefix(shape))
        .map(str::trim_start)
        .and_then(|line| line.strip_prefix('['))
        .ok_or_else(refuse)?;
    if lines.next().is_some() {
        return Err(refuse());
    }

    let radians = Unit::new("rad").expect("radian unit");
    let mut vertices = Vec::new();
    loop {
        let (vertex, tail) = rest
            .trim_start()
            .strip_prefix('[')
            .and_then(|vertex| vertex.split_once(']'))
            .ok_or_else(refuse)?;
        let (longitude, latitude) = vertex.split_once(',').ok_or_else(refuse)?;
        let angle = |value: &str| {
            Quantity::read(value.trim())
                .ok()
                .and_then(|quantity| quantity.get_value_in(&radians).ok())
                .filter(|angle| angle.is_finite())
                .ok_or_else(refuse)
        };
        let direction = [angle(longitude)?, angle(latitude)?];
        if direction[1].abs() > std::f64::consts::FRAC_PI_2 {
            return Err(refuse());
        }
        vertices.push(direction);
        if let Some(attributes) = tail.trim_start().strip_prefix(']') {
            let attributes = attributes.trim().trim_start_matches(',').trim();
            if !attributes.is_empty() {
                if !j2000_frame(attributes) {
                    return Err(refuse());
                }
                j2000 = true;
            }
            break;
        }
        rest = tail.trim_start().strip_prefix(',').ok_or_else(refuse)?;
    }
    if shape == "box" {
        let &[[longitude0, latitude0], [longitude1, latitude1]] = vertices.as_slice() else {
            return Err(refuse());
        };
        // AnnRectBox uses AnnPolygon::_initCorners in the input sky frame.
        // Projection of all four corners follows on the image grid.
        vertices = vec![
            [longitude0, latitude0],
            [longitude1, latitude0],
            [longitude1, latitude1],
            [longitude0, latitude1],
        ];
    } else if vertices.len() < 3 {
        return Err(refuse());
    }
    Ok(Some(MaskPolygon {
        vertices: vertices.into(),
        j2000,
    }))
}

fn j2000_frame(text: &str) -> bool {
    text.split_once('=').is_some_and(|(key, value)| {
        key.trim() == "coord" && value.trim().eq_ignore_ascii_case("J2000")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn angular_box_supplies_four_corners_in_the_input_frame() {
        let polygon = parse_sky_region("box [[128deg,-22deg],[124deg,-19deg]]")
            .unwrap()
            .unwrap();
        let expected = [
            [128.0_f64, -22.0],
            [124.0, -22.0],
            [124.0, -19.0],
            [128.0, -19.0],
        ];
        assert_eq!(polygon.vertices.len(), expected.len());
        for (index, (actual, desired)) in polygon.vertices.iter().zip(expected).enumerate() {
            let desired = desired.map(f64::to_radians);
            assert!(
                (actual[0] - desired[0]).abs() < 1e-15,
                "corner {index} longitude"
            );
            assert!(
                (actual[1] - desired[1]).abs() < 1e-15,
                "corner {index} latitude"
            );
        }
        assert!(
            parse_sky_region("box[[8pix,4pix],[20pix,19pix]]")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn crtf_longitude_respects_explicit_degrees_and_hours() {
        let polygon = parse_sky_region("poly [[10d00m00s,20deg],[00h40m00s,21deg],[11deg,20deg]]")
            .unwrap()
            .unwrap();
        for vertex in &polygon.vertices[..2] {
            assert!(
                (vertex[0] - 10.0_f64.to_radians()).abs() < 1.0e-15,
                "longitude {} rad",
                vertex[0]
            );
        }
        assert_eq!(polygon.vertices.len(), 3);
    }
}
