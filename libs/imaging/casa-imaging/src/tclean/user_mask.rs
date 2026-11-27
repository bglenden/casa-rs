// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA image-mask lists and CRTF regions read from tclean's mask parameter.

use super::{MaskPolygon, TcleanRefusal, sky_polygon};

#[derive(Default)]
pub(super) struct UserMask<'a> {
    pub(super) boxes: Vec<String>,
    pub(super) images: Vec<&'a str>,
    pub(super) before_image: Option<usize>,
    pub(super) polygon: Option<MaskPolygon>,
}

/// A user mask from CRTF regions, a saved mask image, or both. The
/// image's index in the compiled boxes preserves the list order for
/// signed mask values. Consecutive text fragments are joined because
/// the CLI can split a region at its commas.
pub(super) fn user_mask<'a>(entries: &[&'a str]) -> Result<UserMask<'a>, TcleanRefusal> {
    let multiple_images = || {
        TcleanRefusal::not_yet(
            "mask",
            "more than one mask image mixed with other entries",
            823,
        )
    };
    if entries.len() > 1
        && entries
            .iter()
            .all(|entry| std::path::Path::new(entry).is_dir())
    {
        return Ok(UserMask {
            images: entries.to_vec(),
            ..UserMask::default()
        });
    }
    if entries.len() > 1
        && let Some((index, &image)) = entries
            .iter()
            .enumerate()
            .find(|(_, entry)| std::path::Path::new(entry).is_dir())
    {
        let after = &entries[index + 1..];
        if after
            .iter()
            .any(|entry| std::path::Path::new(entry).is_dir())
        {
            return Err(multiple_images());
        }
        let mut boxes = mask_region_entries(&entries[..index])?;
        let before = boxes.len();
        boxes.extend(mask_region_entries(after)?);
        return Ok(UserMask {
            boxes,
            images: vec![image],
            before_image: Some(before),
            polygon: None,
        });
    }
    if let [region] = entries
        && std::path::Path::new(region).is_file()
    {
        return region_mask(&read_region_file(region)?);
    }
    let text = entries.join(",");
    if text.contains('[') {
        return region_mask(&text);
    }
    if let Some(boxes) = pixel_box_list(&text) {
        return Ok(UserMask {
            boxes,
            ..UserMask::default()
        });
    }
    match entries {
        [] => Ok(UserMask::default()),
        [path] => Ok(UserMask {
            images: vec![path],
            ..UserMask::default()
        }),
        _ => Err(multiple_images()),
    }
}

fn region_mask<'a>(text: &str) -> Result<UserMask<'a>, TcleanRefusal> {
    if let Some(polygon) = sky_polygon::parse_sky_region(text)? {
        Ok(UserMask {
            polygon: Some(polygon),
            ..UserMask::default()
        })
    } else {
        Ok(UserMask {
            boxes: crtf_pixel_boxes(text)?,
            ..UserMask::default()
        })
    }
}

fn read_region_file(path: &str) -> Result<String, TcleanRefusal> {
    std::fs::read_to_string(path).map_err(|error| {
        TcleanRefusal::new("mask", format!("read the region file {path:?}: {error}"))
    })
}

fn mask_region_entries(entries: &[&str]) -> Result<Vec<String>, TcleanRefusal> {
    let mut text = String::new();
    for entry in entries {
        if std::path::Path::new(entry).is_file() {
            text.push('\n');
            text.push_str(&read_region_file(entry)?);
            text.push('\n');
        } else {
            text.push_str(entry);
            text.push(',');
        }
    }
    crtf_pixel_boxes(&text)
}

/// casa-rs's `x0,y0,x1,y1` boxes separated by `;`, if `text` is that.
fn pixel_box_list(text: &str) -> Option<Vec<String>> {
    text.split(';')
        .map(|corners| {
            let corners = corners
                .split(',')
                .map(|corner| corner.trim().parse::<i64>().ok())
                .collect::<Option<Vec<_>>>()?;
            let [x0, y0, x1, y1] = corners.as_slice() else {
                return None;
            };
            Some(format!("{x0},{y0},{x1},{y1}"))
        })
        .collect()
}

/// The `box[[Xpix, Ypix], [Xpix, Ypix]]` and `circle[[Xpix, Ypix], Rpix]`
/// regions of one CRTF text, as `x0,y0,x1,y1` boxes with the lower corner
/// first (a circle as one box per row it covers, [`circle_rows`]). A region
/// limited to a spectral `range=[<low>, <high>]` gives each of its boxes
/// that range as `@<low>~<high>`, in `Hz` or `pix` ([`spectral_range`]);
/// compile turns it into channels. A `#` line is a comment, and any other
/// region or keyword is refused.
pub(super) fn crtf_pixel_boxes(text: &str) -> Result<Vec<String>, TcleanRefusal> {
    let refused = || {
        TcleanRefusal::not_yet(
            "mask",
            format!("the region {text:?} (casa-rs takes pixel boxes and circles)"),
            980,
        )
    };
    let mut boxes = Vec::new();
    for line in text
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#'))
    {
        let mut rest = line;
        while !rest.is_empty() {
            let (region, after) = if let Some(body) = rest
                .strip_prefix("circle")
                .map(str::trim_start)
                .and_then(|body| body.strip_prefix("[["))
            {
                let (centre, after) = body.split_once(']').ok_or_else(refused)?;
                let centre = centre
                    .split(',')
                    .map(|value| pixels(value).ok_or_else(refused))
                    .collect::<Result<Vec<_>, _>>()?;
                let [x, y] = centre.as_slice() else {
                    return Err(refused());
                };
                let (radius, after) = after
                    .trim_start()
                    .strip_prefix(',')
                    .and_then(|after| after.split_once(']'))
                    .ok_or_else(refused)?;
                let radius = pixels(radius).filter(|radius| *radius > 0.0);
                (circle_rows([*x, *y], radius.ok_or_else(refused)?), after)
            } else {
                let body = rest
                    .strip_prefix("box")
                    .map(str::trim_start)
                    .and_then(|body| body.strip_prefix("[["))
                    .ok_or_else(refused)?;
                let (inside, after) = body.split_once("]]").ok_or_else(refused)?;
                let corners = inside
                    .split(',')
                    .map(|corner| pixels(corner.trim_matches(['[', ']', ' '])).ok_or_else(refused))
                    .collect::<Result<Vec<_>, _>>()?;
                let [xa, ya, xb, yb] = corners.as_slice() else {
                    return Err(refused());
                };
                let region = if corners.iter().all(|value| value.fract() == 0.0) {
                    vec![format!(
                        "{},{},{},{}",
                        xa.min(*xb),
                        ya.min(*yb),
                        xa.max(*xb),
                        ya.max(*yb)
                    )]
                } else {
                    crate::compile::mask_polygon::fractional_pixel_box([*xa, *ya, *xb, *yb])
                        .into_iter()
                        .map(|[x0, y0, x1, y1]| format!("{x0},{y0},{x1},{y1}"))
                        .collect()
                };
                (region, after)
            };
            // The region's keywords run to the next region. casacore keeps
            // the last `range` a region gives.
            let mut range = None;
            rest = after.trim_start_matches([',', ' ', '\t']);
            while let Some(value) = rest
                .strip_prefix("range")
                .map(str::trim_start)
                .and_then(|value| value.strip_prefix('='))
                .map(str::trim_start)
                .and_then(|value| value.strip_prefix('['))
            {
                let (ends, after) = value.split_once(']').ok_or_else(refused)?;
                range = Some(spectral_range(ends, text)?);
                rest = after.trim_start_matches([',', ' ', '\t']);
            }
            let suffix = range.unwrap_or_default();
            boxes.extend(region.into_iter().map(|corners| corners + &suffix));
        }
    }
    Ok(boxes)
}

/// A CRTF `range`'s ends, `<low>, <high>` in either order, as a mask box's
/// `@<low>~<high>`: frequencies in hertz (`Hz` to `THz`) or channel pixels
/// (`pix`), both ends of one kind, as casacore's
/// `AnnotationBase::setFrequencyLimits` requires. Velocities are refused.
fn spectral_range(ends: &str, text: &str) -> Result<String, TcleanRefusal> {
    let quantity = |end: &str| {
        let end = end.trim();
        let split = end
            .find(|c: char| c.is_ascii_alphabetic() && c != 'e' && c != 'E')
            .unwrap_or(end.len());
        let (number, unit) = end.split_at(split);
        let number = number
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|n| n.is_finite())?;
        let (scale, kind) = match unit.trim() {
            "Hz" => (1.0, "Hz"),
            "kHz" => (1.0e3, "Hz"),
            "MHz" => (1.0e6, "Hz"),
            "GHz" => (1.0e9, "Hz"),
            "THz" => (1.0e12, "Hz"),
            "pix" => (1.0, "pix"),
            _ => return None,
        };
        Some((number * scale, kind))
    };
    let refused = || {
        TcleanRefusal::not_yet(
            "mask",
            format!(
                "the region {text:?}: a spectral range of two frequencies or two channel pixels"
            ),
            980,
        )
    };
    let ends = ends.split(',').map(quantity).collect::<Option<Vec<_>>>();
    match ends.as_deref() {
        Some(&[(low, low_kind), (high, high_kind)]) if low_kind == high_kind => {
            Ok(format!("@{low}{low_kind}~{high}{high_kind}"))
        }
        _ => Err(refused()),
    }
}

/// A CRTF pixel value, `Npix`, fractional or not.
fn pixels(text: &str) -> Option<f64> {
    text.trim()
        .strip_suffix("pix")
        .and_then(|number| number.trim().parse::<f64>().ok())
        .filter(|number| number.is_finite())
}

/// The pixels of the circle about `centre` of `radius` pixels, one
/// `x0,y,x1,y` box per row. casacore's `LCEllipsoid` holds a pixel whose
/// offset from the centre, in radii, has a squared length of at most one,
/// in `Float`; a circle's rows are runs of pixels, so the boxes hold the
/// same pixels. Rows and columns outside the image are clipped as a box's.
fn circle_rows(centre: [f64; 2], radius: f64) -> Vec<String> {
    let [x, y] = centre.map(|value| value as f32);
    let radius = radius as f32;
    let inside = |column: i64, row: i64| {
        let dx = (column as f32 - x) / radius;
        let dy = (row as f32 - y) / radius;
        dx * dx + dy * dy <= 1.0
    };
    let span =
        |centre: f32| (centre - radius).floor() as i64 - 1..=(centre + radius).ceil() as i64 + 1;
    span(y)
        .filter_map(|row| {
            let mut columns = span(x).filter(|column| inside(*column, row));
            let first = columns.next()?;
            let last = columns.next_back().unwrap_or(first);
            Some(format!("{first},{row},{last},{row}"))
        })
        .collect()
}
