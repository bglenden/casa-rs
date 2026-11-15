// SPDX-License-Identifier: LGPL-3.0-or-later

use serde::{Deserialize, Serialize};

use super::TcleanRefusal;
use crate::compile::parse_phase_center_angle;

/// An unqualified CRTF polygon, with angular direction vertices in radians.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct MaskPolygon {
    pub(crate) vertices: Vec<[f64; 2]>,
    /// Explicit J2000 overrides the output direction coordinate's frame.
    pub(crate) j2000: bool,
}

pub(super) fn parse_polygon(text: &str) -> Result<Option<MaskPolygon>, TcleanRefusal> {
    let mut lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .peekable();
    if !lines.clone().any(|line| line.starts_with("poly")) {
        return Ok(None);
    }
    let refuse = || {
        TcleanRefusal::not_yet(
            "mask",
            "CRTF polygon masks take one sky polygon with the image's frame or coord=J2000",
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
        .and_then(|line| line.strip_prefix("poly"))
        .map(str::trim_start)
        .and_then(|line| line.strip_prefix('['))
        .ok_or_else(refuse)?;
    if lines.next().is_some() {
        return Err(refuse());
    }

    let mut vertices = Vec::new();
    loop {
        let (vertex, tail) = rest
            .trim_start()
            .strip_prefix('[')
            .and_then(|vertex| vertex.split_once(']'))
            .ok_or_else(refuse)?;
        let (longitude, latitude) = vertex.split_once(',').ok_or_else(refuse)?;
        let angle = |value: &str, is_longitude| {
            parse_phase_center_angle(value.trim(), is_longitude)
                .ok()
                .filter(|angle| angle.is_finite())
                .ok_or_else(refuse)
        };
        let direction = [angle(longitude, true)?, angle(latitude, false)?];
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
    if vertices.len() < 3 {
        return Err(refuse());
    }
    Ok(Some(MaskPolygon { vertices, j2000 }))
}

fn j2000_frame(text: &str) -> bool {
    text.split_once('=').is_some_and(|(key, value)| {
        key.trim() == "coord" && value.trim().eq_ignore_ascii_case("J2000")
    })
}
