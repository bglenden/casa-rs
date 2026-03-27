// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generic image-processing helpers used by panel and Kitty rendering paths.

use image::{DynamicImage, Rgba, RgbaImage};
use thiserror::Error;

/// Errors returned by generic image operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ImageOpsError {
    /// The aspect ratio inputs must be non-zero.
    #[error("aspect ratio must be non-zero, got {aspect_width}:{aspect_height}")]
    InvalidAspectRatio {
        aspect_width: u32,
        aspect_height: u32,
    },
}

/// Holds both opaque and background-keyed variants of an image.
#[derive(Clone, Debug)]
pub struct ImageLayers {
    /// The source image converted to RGBA without chroma-keying.
    pub opaque: RgbaImage,
    /// The source image with the specified background chroma-keyed to transparency.
    pub keyed_background: RgbaImage,
}

/// Fit a target aspect ratio within the provided maximum pixel bounds.
pub fn fit_pixels_preserving_aspect(
    max_width: u32,
    max_height: u32,
    aspect_width: u32,
    aspect_height: u32,
) -> Result<(u32, u32), ImageOpsError> {
    if max_width == 0 || max_height == 0 {
        return Ok((0, 0));
    }
    if aspect_width == 0 || aspect_height == 0 {
        return Err(ImageOpsError::InvalidAspectRatio {
            aspect_width,
            aspect_height,
        });
    }

    let width_limited_height =
        (u64::from(max_width) * u64::from(aspect_height)) / u64::from(aspect_width);
    if width_limited_height <= u64::from(max_height) {
        Ok((max_width, width_limited_height.max(1) as u32))
    } else {
        let height_limited_width =
            (u64::from(max_height) * u64::from(aspect_width)) / u64::from(aspect_height);
        Ok((height_limited_width.max(1) as u32, max_height))
    }
}

/// Build both opaque and background-keyed variants of an image.
pub fn prepare_image_layers(
    image: &DynamicImage,
    background: [u8; 3],
    tolerance: u8,
) -> ImageLayers {
    ImageLayers {
        opaque: image.to_rgba8(),
        keyed_background: key_background_transparency(image, background, tolerance),
    }
}

/// Chroma-key the given background color to transparency.
///
/// This helper compares each pixel against `background` using the provided channel-distance
/// tolerance. Existing alpha is preserved by multiplying it with the chroma-keyed alpha.
pub fn key_background_transparency(
    image: &DynamicImage,
    background: [u8; 3],
    tolerance: u8,
) -> RgbaImage {
    let mut rgba = image.to_rgba8();

    for pixel in rgba.pixels_mut() {
        let [r, g, b, existing_alpha] = pixel.0;
        let distance_from_background = r
            .abs_diff(background[0])
            .max(g.abs_diff(background[1]))
            .max(b.abs_diff(background[2]));
        let keyed_alpha = if distance_from_background <= tolerance {
            0
        } else {
            ((u16::from(distance_from_background) * 255) / 170).min(255) as u8
        };
        let combined_alpha =
            ((u16::from(existing_alpha) * u16::from(keyed_alpha)) / 255).min(255) as u8;

        *pixel = Rgba([r, g, b, combined_alpha]);
    }

    rgba
}

/// Scale the alpha channel of every pixel by the provided opacity percent.
pub fn apply_opacity(base: &RgbaImage, opacity_percent: u8) -> RgbaImage {
    let mut image = base.clone();
    let scale = u16::from(opacity_percent.min(100));

    for pixel in image.pixels_mut() {
        let alpha = u16::from(pixel[3]);
        pixel[3] = ((alpha * scale) / 100).min(255) as u8;
    }

    image
}

#[cfg(test)]
mod tests {
    use image::{DynamicImage, Rgba, RgbaImage};

    use super::{
        ImageLayers, ImageOpsError, apply_opacity, fit_pixels_preserving_aspect,
        key_background_transparency, prepare_image_layers,
    };

    #[test]
    fn fit_pixels_handles_zero_max_dimensions() {
        assert_eq!(fit_pixels_preserving_aspect(0, 500, 16, 9).unwrap(), (0, 0));
        assert_eq!(fit_pixels_preserving_aspect(800, 0, 16, 9).unwrap(), (0, 0));
    }

    #[test]
    fn fit_pixels_rejects_zero_aspect_ratios() {
        let err = fit_pixels_preserving_aspect(800, 600, 0, 9).unwrap_err();
        assert_eq!(
            err,
            ImageOpsError::InvalidAspectRatio {
                aspect_width: 0,
                aspect_height: 9,
            }
        );
    }

    #[test]
    fn fit_pixels_uses_aspect_ratio() {
        assert_eq!(
            fit_pixels_preserving_aspect(1400, 900, 14, 9).unwrap(),
            (1400, 900)
        );
        assert_eq!(
            fit_pixels_preserving_aspect(800, 300, 14, 9).unwrap(),
            (466, 300)
        );
    }

    #[test]
    fn apply_opacity_scales_alpha() {
        let mut image = RgbaImage::new(1, 1);
        image.put_pixel(0, 0, Rgba([10, 20, 30, 200]));
        let adjusted = apply_opacity(&image, 50);
        assert_eq!(adjusted.get_pixel(0, 0).0, [10, 20, 30, 100]);
    }

    #[test]
    fn background_keying_preserves_existing_alpha() {
        let mut image = RgbaImage::new(2, 1);
        image.put_pixel(0, 0, Rgba([250, 249, 246, 255]));
        image.put_pixel(1, 0, Rgba([220, 100, 100, 128]));
        let keyed =
            key_background_transparency(&DynamicImage::ImageRgba8(image), [250, 249, 246], 8);
        assert_eq!(keyed.get_pixel(0, 0).0[3], 0);
        assert!(keyed.get_pixel(1, 0).0[3] > 0);
        assert!(keyed.get_pixel(1, 0).0[3] <= 128);
    }

    #[test]
    fn prepare_image_layers_returns_named_layers() {
        let image = DynamicImage::ImageRgba8(RgbaImage::from_pixel(1, 1, Rgba([1, 2, 3, 255])));
        let layers: ImageLayers = prepare_image_layers(&image, [1, 2, 3], 4);
        assert_eq!(layers.opaque.get_pixel(0, 0).0, [1, 2, 3, 255]);
        assert_eq!(layers.keyed_background.get_pixel(0, 0).0[3], 0);
    }
}
