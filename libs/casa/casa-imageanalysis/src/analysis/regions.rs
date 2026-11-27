// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct Selection {
    pub(super) start: Vec<usize>,
    pub(super) shape: Vec<usize>,
    /// The spectral axis its named channels belong to.
    pub(super) channel_axis: Option<usize>,
    pub(super) channel_indices: Option<Vec<usize>>,
    /// The Stokes axis its named planes belong to.
    pub(super) stokes_axis: Option<usize>,
    /// The image's Stokes-axis indices the selection holds, when a Stokes
    /// selection named them.
    pub(super) stokes_indices: Option<Vec<usize>>,
    /// The image axes a box or polygon's x and y run along: the direction
    /// plane's.
    pub(super) plane: [usize; 2],
}

impl Selection {
    pub(super) fn new<T: ImagePixel>(
        image: &impl ImageInterface<T>,
        region: Option<&PixelRegion>,
        chans: Option<&str>,
        stokes: Option<&str>,
    ) -> Result<Self, ImageError> {
        let mut start = vec![0; image.ndim()];
        let mut shape = image.shape().to_vec();
        let plane = region.map_or(Ok([0, 1]), |_| direction_plane_axes(image))?;
        if let Some(region) = region {
            let [x0, y0, x1, y1] = region.bounding_box();
            let [x, y] = plane;
            if image.ndim() < 2 || x1 >= shape[x] || y1 >= shape[y] {
                return Err(ImageError::ShapeMismatch {
                    expected: image.shape().to_vec(),
                    got: vec![x0, y0, x1, y1],
                });
            }
            start[x] = x0;
            start[y] = y0;
            shape[x] = x1 - x0 + 1;
            shape[y] = y1 - y0 + 1;
        }
        let (channel_axis, channel_indices) =
            if let Some(chans) = chans.filter(|text| !text.trim().is_empty()) {
                let spectral_axis = image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
                    ImageError::InvalidMetadata(
                        "channel selection requires a spectral axis".to_string(),
                    )
                })?;
                let indices = parse_image_channel_selection(chans, image.shape()[spectral_axis])?;
                start[spectral_axis] = indices[0];
                shape[spectral_axis] = indices[indices.len() - 1] - indices[0] + 1;
                (Some(spectral_axis), Some(indices))
            } else {
                (None, None)
            };
        let (stokes_axis, stokes_indices) =
            if let Some(text) = stokes.filter(|text| !text.trim().is_empty()) {
                let indices = parse_stokes_selection(image, text)?;
                let axis = image
                    .find_axis(CoordinateType::Stokes)
                    .expect("parse_stokes_selection found the axis");
                start[axis] = indices[0];
                shape[axis] = indices[indices.len() - 1] - indices[0] + 1;
                (Some(axis), Some(indices))
            } else {
                (None, None)
            };
        Ok(Self {
            start,
            shape,
            channel_axis,
            channel_indices,
            stokes_axis,
            stokes_indices,
            plane,
        })
    }

    /// The image-axis indices this selection holds along `axis`, in
    /// order: a named set's, or the bounding rectangle's span.
    pub(super) fn axis_positions(&self, axis: usize) -> Vec<usize> {
        if Some(axis) == self.channel_axis
            && let Some(indices) = &self.channel_indices
        {
            return indices.clone();
        }
        if Some(axis) == self.stokes_axis
            && let Some(indices) = &self.stokes_indices
        {
            return indices.clone();
        }
        (self.start[axis]..self.start[axis] + self.shape[axis]).collect()
    }

    /// Whether the named Stokes set is not a contiguous span, which the
    /// streaming classical path cannot express.
    pub(super) fn stokes_has_gaps(&self) -> bool {
        Self::named_set_has_gaps(self.stokes_axis, &self.stokes_indices)
    }

    /// Whether the named channel set is not a contiguous span, which the
    /// streaming classical path cannot express.
    pub(super) fn channels_have_gaps(&self) -> bool {
        Self::named_set_has_gaps(self.channel_axis, &self.channel_indices)
    }

    /// Whether a named selection set skips indices between its first and
    /// last.
    fn named_set_has_gaps(axis: Option<usize>, indices: &Option<Vec<usize>>) -> bool {
        match (axis, indices) {
            (Some(_), Some(indices)) => {
                indices.len() > 1 && indices[indices.len() - 1] - indices[0] + 1 != indices.len()
            }
            _ => false,
        }
    }
}

/// Parse CASA's `stokes` selection: comma-separated Stokes plane names,
/// concatenated as in `QU` or `IQUV`. Numeric forms are not names.
pub(crate) fn parse_stokes_selection<T: ImagePixel>(
    image: &(impl ImageInterface<T> + ?Sized),
    text: &str,
) -> Result<Vec<usize>, ImageError> {
    let axis = image.find_axis(CoordinateType::Stokes).ok_or_else(|| {
        ImageError::InvalidMetadata("Stokes selection requires a Stokes axis".to_string())
    })?;
    parse_stokes_names(image.coordinates(), axis, text)
}

pub(super) fn parse_stokes_names(
    coordinates: &CoordinateSystem,
    axis: usize,
    text: &str,
) -> Result<Vec<usize>, ImageError> {
    let Some(CoordinateModel::Stokes(stokes)) = coordinate_for_axis(coordinates, axis) else {
        return Err(ImageError::InvalidMetadata(
            "Stokes selection requires a Stokes coordinate".to_string(),
        ));
    };
    let mut indices = Vec::new();
    for part in text.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut rest = part;
        while !rest.is_empty() {
            let name = if rest.len() >= 2 && StokesType::from_name(&rest[..2]).is_some() {
                rest[..2].to_string()
            } else if let Some(kind) = rest
                .chars()
                .next()
                .and_then(|first| StokesType::from_name(&first.to_string()))
            {
                let _ = kind;
                rest[..1].to_string()
            } else {
                return Err(ImageError::CasaSelection(format!(
                    "(Sub)String {part} in stokes specification part {part} does not match a known polarization"
                )));
            };
            let kind =
                StokesType::from_name(&name).expect("the name was matched against the table");
            let Some(index) = stokes.stokes().iter().position(|known| *known == kind) else {
                return Err(ImageError::CasaSelection(format!(
                    "Stokes {name} is not one of this image's Stokes planes"
                )));
            };
            indices.push(index);
            rest = &rest[name.len()..];
        }
    }
    if indices.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "the Stokes selection names no plane".to_string(),
        ));
    }
    indices.sort_unstable();
    indices.dedup();
    Ok(indices)
}

/// The `axes` selection: all axes, or the named axes to collapse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AxisSelection {
    /// Every axis: the statistics of the whole selection.
    All,
    /// Collapse these pixel axes; the statistics vary along the rest.
    Collapsed(Vec<usize>),
}

/// Parse CASA's `axes` argument: a pixel-axis list, `-1` for all of them,
/// or an empty list for the default. A negative axis other than a lone
/// `-1` is invalid, with CASA's wording.
pub(super) fn parse_axes(axes: Option<&[i64]>, n_dim: usize) -> Result<AxisSelection, String> {
    let Some(axes) = axes else {
        return Ok(AxisSelection::All);
    };
    if axes.is_empty() {
        return Ok(AxisSelection::All);
    }
    if axes == [-1] {
        return Ok(AxisSelection::All);
    }
    let mut named = Vec::with_capacity(axes.len());
    for &axis in axes {
        if !(0..n_dim as i64).contains(&axis) {
            return Err(format!("Invalid cursor axes: {axes:?}."));
        }
        if named.contains(&(axis as usize)) {
            return Err(format!(
                "Invalid cursor axes: {axes:?}: an axis is named twice; CASA 6.7.6.14 aborts on it"
            ));
        }
        named.push(axis as usize);
    }
    named.sort_unstable();
    Ok(AxisSelection::Collapsed(named))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PixelRegion {
    /// One or more rectangular boxes: CASA's task `box` parameter takes
    /// comma-separated quadruplets, and CRTF strings name one box.
    Boxes(Vec<[usize; 4]>),
    Polygon(Vec<[usize; 2]>),
}

impl PixelRegion {
    pub(super) fn bounding_box(&self) -> [usize; 4] {
        match self {
            Self::Boxes(boxes) => {
                let min_x = boxes.iter().map(|b| b[0]).min().unwrap_or(0);
                let min_y = boxes.iter().map(|b| b[1]).min().unwrap_or(0);
                let max_x = boxes.iter().map(|b| b[2]).max().unwrap_or(min_x);
                let max_y = boxes.iter().map(|b| b[3]).max().unwrap_or(min_y);
                [min_x, min_y, max_x, max_y]
            }
            Self::Polygon(vertices) => {
                let min_x = vertices.iter().map(|vertex| vertex[0]).min().unwrap_or(0);
                let min_y = vertices.iter().map(|vertex| vertex[1]).min().unwrap_or(0);
                let max_x = vertices
                    .iter()
                    .map(|vertex| vertex[0])
                    .max()
                    .unwrap_or(min_x);
                let max_y = vertices
                    .iter()
                    .map(|vertex| vertex[1])
                    .max()
                    .unwrap_or(min_y);
                [min_x, min_y, max_x, max_y]
            }
        }
    }
}

pub(crate) fn region_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    region: Option<&str>,
) -> Result<Option<PixelRegion>, ImageError> {
    let Some(region) = region.map(str::trim).filter(|text| !text.is_empty()) else {
        return Ok(None);
    };
    if let Some(parsed) = crtf_box_pixels(image, region)? {
        return Ok(Some(PixelRegion::Boxes(vec![parse_box(&parsed)?])));
    }
    if let Some(parsed) = crtf_polygon_pixels(image, region)? {
        return Ok(Some(PixelRegion::Polygon(parsed)));
    }
    if let Some(box_text) = region.strip_prefix("box:") {
        let parsed = parse_box(box_text.trim())?;
        return Ok(Some(PixelRegion::Boxes(vec![parsed])));
    }
    if let Some(contents) = region
        .strip_prefix("pixelbox(")
        .and_then(|text| text.strip_suffix(')'))
    {
        let parsed = parse_box(contents.trim())?;
        return Ok(Some(PixelRegion::Boxes(vec![parsed])));
    }
    if looks_like_box(region) {
        let parsed = parse_box(region)?;
        return Ok(Some(PixelRegion::Boxes(vec![parsed])));
    }

    let region_path = Path::new(region);
    let contents = fs::read_to_string(region_path).map_err(|error| {
        ImageError::Io(format!(
            "read region file {}: {error}",
            region_path.display()
        ))
    })?;
    if let Some(box_pixels) = crtf_box_pixels(image, &contents)? {
        return Ok(Some(PixelRegion::Boxes(vec![parse_box(&box_pixels)?])));
    }
    if let Some(polygon) = crtf_polygon_pixels(image, &contents)? {
        return Ok(Some(PixelRegion::Polygon(polygon)));
    }
    let format = if contents.trim_start().starts_with('{') {
        "casa-rs JSON region files are not a persistent interchange format; use CASA CRTF"
    } else {
        "expected CASA CRTF box or polygon, for example box[[100pix,100pix],[150pix,150pix]] or poly [[100pix,100pix],...]"
    };
    Err(ImageError::InvalidMetadata(format!(
        "unsupported region file {}: {format}",
        region_path.display()
    )))
}

pub(crate) fn region_pixel_mask(
    region: &PixelRegion,
    selection: &Selection,
) -> Result<ArrayD<bool>, ImageError> {
    let mask = match region {
        PixelRegion::Boxes(boxes) => {
            if boxes.len() < 2 {
                // One box is the selection itself; every pixel is in it.
                ArrayD::from_elem(IxDyn(&selection.shape).f(), true)
            } else {
                // Several boxes: the selection is their bounding box, and
                // a pixel is in the region when some box holds it.
                let [x_axis, y_axis] = selection.plane;
                ArrayD::from_shape_fn(IxDyn(&selection.shape).f(), |idx| {
                    let x = selection.start[x_axis] + idx[x_axis];
                    let y = selection.start[y_axis] + idx[y_axis];
                    boxes
                        .iter()
                        .any(|b| b[0] <= x && x <= b[2] && b[1] <= y && y <= b[3])
                })
            }
        }
        PixelRegion::Polygon(vertices) => {
            if vertices.len() < 3 {
                return Err(ImageError::InvalidMetadata(
                    "CRTF polygon regions require at least three vertices".to_string(),
                ));
            }
            let [x_axis, y_axis] = selection.plane;
            ArrayD::from_shape_fn(IxDyn(&selection.shape).f(), |idx| {
                let x = selection.start[x_axis] + idx[x_axis];
                let y = selection.start[y_axis] + idx[y_axis];
                point_in_polygon_or_boundary(x as f64 + 0.5, y as f64 + 0.5, vertices)
            })
        }
    };
    Ok(mask)
}

pub(super) fn crtf_box_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    text: &str,
) -> Result<Option<String>, ImageError> {
    let Some(parts) = crtf_box_parts(text)? else {
        return Ok(None);
    };
    let pixels = parts
        .iter()
        .map(|part| parse_crtf_pixel_quantity(part))
        .collect::<Result<Vec<_>, ImageError>>()?;
    if pixels.iter().all(Option::is_some) {
        let values = pixels.into_iter().flatten().collect::<Vec<_>>();
        return Ok(Some(format!(
            "{},{},{},{}",
            values[0], values[1], values[2], values[3]
        )));
    }
    if pixels.iter().any(Option::is_some) {
        return Err(ImageError::InvalidMetadata(
            "cannot mix pix and non-pix coordinates in a CRTF box region".to_string(),
        ));
    }

    let world = parts
        .iter()
        .map(|part| parse_crtf_world_quantity(part))
        .collect::<Result<Vec<_>, ImageError>>()?;
    Ok(Some(crtf_world_box_pixels(
        image,
        [world[0], world[1], world[2], world[3]],
    )?))
}

pub(super) fn crtf_polygon_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    text: &str,
) -> Result<Option<Vec<[usize; 2]>>, ImageError> {
    let Some(pairs) = crtf_polygon_parts(text)? else {
        return Ok(None);
    };
    if pairs.len() < 3 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: expected at least three vertices"
        )));
    }
    crtf_coordinate_pairs_to_pixels(image, &pairs).map(Some)
}

pub(super) fn crtf_polygon_parts(text: &str) -> Result<Option<Vec<[String; 2]>>, ImageError> {
    let normalized = text.trim();
    let Some(after) = find_crtf_body(normalized, "poly", "[") else {
        return Ok(None);
    };
    let end = after.rfind(']').ok_or_else(|| {
        ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: missing closing ]"
        ))
    })?;
    let body = &after[..=end];
    let body = body
        .strip_prefix('[')
        .and_then(|body| body.strip_suffix(']'))
        .unwrap_or(body);
    let mut pairs = Vec::new();
    let mut cursor = 0usize;
    while let Some(relative_start) = body[cursor..].find('[') {
        let pair_start = cursor + relative_start + 1;
        let pair_end = body[pair_start..].find(']').ok_or_else(|| {
            ImageError::InvalidMetadata(format!(
                "invalid CASA CRTF polygon region {text:?}: missing vertex closing ]"
            ))
        })? + pair_start;
        let pair = body[pair_start..pair_end]
            .split(',')
            .map(|part| part.trim().to_string())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if pair.len() == 2 {
            pairs.push([pair[0].clone(), pair[1].clone()]);
        }
        cursor = pair_end + 1;
    }
    if pairs.is_empty() {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: expected poly [[x0,y0],...]"
        )));
    }
    Ok(Some(pairs))
}

pub(super) fn crtf_coordinate_pairs_to_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    pairs: &[[String; 2]],
) -> Result<Vec<[usize; 2]>, ImageError> {
    let pixel_pairs = pairs
        .iter()
        .map(|pair| {
            Ok([
                parse_crtf_pixel_quantity(&pair[0])?,
                parse_crtf_pixel_quantity(&pair[1])?,
            ])
        })
        .collect::<Result<Vec<_>, ImageError>>()?;
    if pixel_pairs
        .iter()
        .all(|pair| pair[0].is_some() && pair[1].is_some())
    {
        return Ok(pixel_pairs
            .into_iter()
            .map(|pair| [pair[0].unwrap(), pair[1].unwrap()])
            .collect());
    }
    if pixel_pairs
        .iter()
        .any(|pair| pair[0].is_some() || pair[1].is_some())
    {
        return Err(ImageError::InvalidMetadata(
            "cannot mix pix and non-pix coordinates in a CRTF polygon region".to_string(),
        ));
    }

    let coordinates = image.coordinates();
    if coordinates.n_pixel_axes() < 2 || coordinates.n_world_axes() < 2 {
        return Err(ImageError::InvalidMetadata(
            "world-coordinate CRTF polygon regions require an image coordinate system with at least two axes"
                .to_string(),
        ));
    }
    let [x, y] = direction_plane_axes(image)?;
    let [longitude, latitude] = world_axes_for_plane(coordinates, [x, y])?;
    let base_pixel = vec![0.0; coordinates.n_pixel_axes()];
    let base_world = coordinates.to_world(&base_pixel)?;
    pairs
        .iter()
        .map(|pair| {
            let mut world = base_world.clone();
            world[longitude] = parse_crtf_world_quantity(&pair[0])?;
            world[latitude] = parse_crtf_world_quantity(&pair[1])?;
            let pixel = coordinates.to_pixel(&world)?;
            Ok([
                rounded_nonnegative_pixel_value(pixel[x], "CRTF polygon world x")?,
                rounded_nonnegative_pixel_value(pixel[y], "CRTF polygon world y")?,
            ])
        })
        .collect()
}

pub(super) fn crtf_box_parts(text: &str) -> Result<Option<[String; 4]>, ImageError> {
    let normalized = text.trim();
    let Some(after) = find_crtf_body(normalized, "box", "[[").and_then(|s| s.strip_prefix("[["))
    else {
        return Ok(None);
    };
    let end = after.find("]]").ok_or_else(|| {
        ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF box region {text:?}: missing closing ]]"
        ))
    })?;
    let body = &after[..end];
    let body = body
        .replace("], [", ",")
        .replace("],[", ",")
        .replace(['[', ']'], "");
    let parts = body
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 4 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF box region {text:?}: expected four coordinates"
        )));
    }
    Ok(Some([
        parts[0].clone(),
        parts[1].clone(),
        parts[2].clone(),
        parts[3].clone(),
    ]))
}

pub(super) fn find_crtf_body<'a>(text: &'a str, keyword: &str, open: &str) -> Option<&'a str> {
    let mut offset = 0usize;
    while let Some(rel) = text[offset..].find(keyword) {
        let pos = offset + rel;
        let boundary = pos == 0
            || text[..pos]
                .chars()
                .next_back()
                .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_');
        let rest = &text[pos + keyword.len()..];
        if boundary {
            let trimmed = rest.trim_start();
            if trimmed.starts_with(open) {
                return Some(trimmed);
            }
        }
        offset = pos + keyword.len();
    }
    None
}

pub(super) fn parse_crtf_pixel_quantity(text: &str) -> Result<Option<usize>, ImageError> {
    let text = text.trim();
    let lowercase = text.to_ascii_lowercase();
    let Some(value) = lowercase.strip_suffix("pix") else {
        return Ok(None);
    };
    rounded_nonnegative_pixel(value.trim(), text).map(Some)
}

pub(super) fn parse_crtf_world_quantity(text: &str) -> Result<f64, ImageError> {
    let text = text.trim();
    let lowercase = text.to_ascii_lowercase();
    let (value, scale) = if let Some(value) = lowercase.strip_suffix("arcsec") {
        (value, std::f64::consts::PI / 180.0 / 3600.0)
    } else if let Some(value) = lowercase.strip_suffix("arcmin") {
        (value, std::f64::consts::PI / 180.0 / 60.0)
    } else if let Some(value) = lowercase.strip_suffix("deg") {
        (value, std::f64::consts::PI / 180.0)
    } else if let Some(value) = lowercase.strip_suffix("rad") {
        (value, 1.0)
    } else {
        (lowercase.as_str(), 1.0)
    };
    let parsed = value.trim().parse::<f64>().map_err(|error| {
        ImageError::InvalidMetadata(format!("invalid CRTF world coordinate {text:?}: {error}"))
    })?;
    if !parsed.is_finite() {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF world coordinate {text:?}: value is not finite"
        )));
    }
    Ok(parsed * scale)
}

pub(super) fn crtf_world_box_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    world_box: [f64; 4],
) -> Result<String, ImageError> {
    let coordinates = image.coordinates();
    if coordinates.n_pixel_axes() < 2 || coordinates.n_world_axes() < 2 {
        return Err(ImageError::InvalidMetadata(
            "world-coordinate CRTF box regions require an image coordinate system with at least two axes"
                .to_string(),
        ));
    }
    let [x, y] = direction_plane_axes(image)?;
    let base_pixel = vec![0.0; coordinates.n_pixel_axes()];
    let mut first_world = coordinates.to_world(&base_pixel)?;
    let [longitude, latitude] = world_axes_for_plane(coordinates, [x, y])?;
    let mut second_world = first_world.clone();
    first_world[longitude] = world_box[0];
    first_world[latitude] = world_box[1];
    second_world[longitude] = world_box[2];
    second_world[latitude] = world_box[3];
    let first_pixel = coordinates.to_pixel(&first_world)?;
    let second_pixel = coordinates.to_pixel(&second_world)?;
    let x0 = rounded_nonnegative_pixel_value(first_pixel[x], "first CRTF world x")?;
    let y0 = rounded_nonnegative_pixel_value(first_pixel[y], "first CRTF world y")?;
    let x1 = rounded_nonnegative_pixel_value(second_pixel[x], "second CRTF world x")?;
    let y1 = rounded_nonnegative_pixel_value(second_pixel[y], "second CRTF world y")?;
    Ok(format!(
        "{},{},{},{}",
        x0.min(x1),
        y0.min(y1),
        x0.max(x1),
        y0.max(y1)
    ))
}

pub(super) fn rounded_nonnegative_pixel(text: &str, original: &str) -> Result<usize, ImageError> {
    let value = text.parse::<f64>().map_err(|error| {
        ImageError::InvalidMetadata(format!(
            "invalid CRTF pixel coordinate {original:?}: {error}"
        ))
    })?;
    rounded_nonnegative_pixel_value(value, original)
}

pub(super) fn rounded_nonnegative_pixel_value(
    value: f64,
    label: &str,
) -> Result<usize, ImageError> {
    if !value.is_finite() || value < -1.0e-6 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF pixel coordinate {label:?}: value is not a non-negative finite number"
        )));
    }
    Ok(value.max(0.0).round() as usize)
}

pub(super) fn point_in_polygon_or_boundary(x: f64, y: f64, vertices: &[[usize; 2]]) -> bool {
    let mut inside = false;
    let mut previous = vertices[vertices.len() - 1];
    for &current in vertices {
        let x0 = previous[0] as f64;
        let y0 = previous[1] as f64;
        let x1 = current[0] as f64;
        let y1 = current[1] as f64;
        if point_on_segment(x, y, x0, y0, x1, y1) {
            return true;
        }
        let crosses = (y0 > y) != (y1 > y);
        if crosses {
            let x_intersection = (x1 - x0) * (y - y0) / (y1 - y0) + x0;
            if x <= x_intersection {
                inside = !inside;
            }
        }
        previous = current;
    }
    inside
}

pub(super) fn point_on_segment(x: f64, y: f64, x0: f64, y0: f64, x1: f64, y1: f64) -> bool {
    let cross = (x - x0) * (y1 - y0) - (y - y0) * (x1 - x0);
    if cross.abs() > 1.0e-9 {
        return false;
    }
    x >= x0.min(x1) - 1.0e-9
        && x <= x0.max(x1) + 1.0e-9
        && y >= y0.min(y1) - 1.0e-9
        && y <= y0.max(y1) + 1.0e-9
}

pub(super) fn looks_like_box(text: &str) -> bool {
    let mut count = 0usize;
    for part in text.split(',') {
        let part = part.trim();
        if part.is_empty() || part.parse::<usize>().is_err() {
            return false;
        }
        count += 1;
    }
    count == 4
}

pub(super) fn parse_box(text: &str) -> Result<[usize; 4], ImageError> {
    let values = text
        .split(',')
        .map(|part| {
            part.trim().parse::<usize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid box {text:?}: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.len() != 4 || values[2] < values[0] || values[3] < values[1] {
        return Err(ImageError::InvalidMetadata(format!(
            "box must be x0,y0,x1,y1 with inclusive upper bounds: {text:?}"
        )));
    }
    Ok([values[0], values[1], values[2], values[3]])
}

/// Parse CASA's task `box` parameter: comma-separated quadruplets of
/// inclusive pixel corners, one box per quadruplet. CASA's
/// `CasacRegionManager` refuses any other count with the same wording.
pub(super) fn parse_boxes(text: &str) -> Result<Vec<[usize; 4]>, ImageError> {
    let values = text
        .split(',')
        .map(|part| {
            part.trim().parse::<usize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid box {text:?}: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() || values.len() % 4 != 0 {
        return Err(ImageError::CasaSelection(
            "box not specified correctly.".to_string(),
        ));
    }
    Ok(values
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| [b[0], b[1], b[2], b[3]])
        .collect())
}

/// A task `box` parameter as a pixel region, validated as CASA's
/// `CasacRegionManager::fromBCS` validates it: each quadruplet's trc
/// inside the direction plane, and each box's blc no greater than its
/// trc, with CASA's own refusals. A box's x and y run along the
/// direction plane's axes wherever they are.
pub(crate) fn parse_box_region(
    text: &str,
    image_shape: &[usize],
    plane: [usize; 2],
) -> Result<PixelRegion, ImageError> {
    let boxes = parse_boxes(text)?;
    let [x, y] = plane;
    for [x0, y0, x1, y1] in &boxes {
        if image_shape.len() < 2 || *x1 >= image_shape[x] || *y1 >= image_shape[y] {
            return Err(ImageError::CasaSelection(
                "trc in box spec is greater than or equal to number of direction \
                 coordinate pixels in the image"
                    .to_string(),
            ));
        }
        if x1 < x0 || y1 < y0 {
            let mut blc = vec![0; image_shape.len()];
            blc[x] = *x0;
            blc[y] = *y0;
            let mut trc: Vec<usize> = image_shape.iter().map(|axis| axis - 1).collect();
            trc[x] = *x1;
            trc[y] = *y1;
            return Err(ImageError::CasaSelection(format!(
                "LCBox::LCBox - blc {blc:?} must be <= trc {trc:?}"
            )));
        }
    }
    Ok(PixelRegion::Boxes(boxes))
}

/// CASA imstat's `flux` over the selection, as casacore's
/// `ImageStatistics::_flux` computes it: a brightness in kelvin times the
/// pixel area in square arcseconds; a brightness per beam divided by the
/// beam's area in pixels (one beam; per-plane beams are not computed). Over
/// a selection of more than one channel the flux is integrated along the
/// spectral axis: times the channel width in km/s (radio, casacore's default
/// velocity) when the axis has a rest frequency, else its frequency
/// increment in Hz.
pub(super) fn image_flux<T: ImagePixel>(
    image: &PagedImage<T>,
    sum: f64,
    selection: &Selection,
) -> Option<f64> {
    image_flux_with_integration(image, sum, selection, true)
}

pub(super) fn image_flux_with_integration<T: ImagePixel>(
    image: &PagedImage<T>,
    sum: f64,
    selection: &Selection,
    spectral_axis_collapsed: bool,
) -> Option<f64> {
    let spectral_axis = spectral_axis_collapsed
        .then(|| image.find_axis(CoordinateType::Spectral))
        .flatten()
        .filter(|&axis| selection.shape[axis] > 1);
    let channels = spectral_axis.map(|axis| [selection.start[axis], selection.start[axis] + 1]);
    let beam = image
        .units()
        .contains("/beam")
        .then(|| image.image_info().ok()?.beam_set.single_beam())
        .flatten();
    statistics_flux_scale(image.coordinates(), image.units(), beam, channels)
        .map(|factor| sum * factor)
}
