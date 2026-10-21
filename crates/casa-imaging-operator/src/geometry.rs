// SPDX-License-Identifier: LGPL-3.0-or-later
//! The padded uv grid and the one rounding rule shared by every backend.

use crate::error::OperatorError;

/// Pixel extent and sampling of the output image, as its direction
/// coordinate states them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ImageExtent {
    /// `[nx, ny]` pixels.
    pub shape: [usize; 2],
    /// Signed radians per pixel `[Δx, Δy]`; `Δx < 0` when right ascension
    /// increases to the left, as CASA images have it.
    pub increment_rad: [f64; 2],
    /// Pixel `[x, y]` of the phase reference direction.
    pub reference_pixel: [usize; 2],
}

/// How the gridding plane extends beyond the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GridPadding {
    /// Grid and image coincide (mosaic and AW kernel sets).
    None,
    /// CASA `FTMachine` padding (standard and W-projection kernel sets):
    /// each axis of `n` pixels grows to
    /// `CompositeNumber::nextLargerEven(Int(factor · n − 0.5))`, the first
    /// even length above the padded size whose prime factors are 2, 3
    /// and 5.
    CasaComposite(PaddingFactor),
}

/// A CASA grid padding factor (`padding_p`), held as the `Float` CASA
/// multiplies it in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PaddingFactor(u32);

impl PaddingFactor {
    /// tclean's padding, 1.2.
    pub const TCLEAN: Self = Self(1.2_f32.to_bits());
    /// The simulator tool's default padding (`sm.setoptions(padding=1.3)`).
    pub const SIMULATOR: Self = Self(1.3_f32.to_bits());

    /// A padding factor; CASA keeps only factors of at least 1.
    ///
    /// # Errors
    ///
    /// A factor below 1 or not finite.
    pub fn new(factor: f32) -> Result<Self, OperatorError> {
        if factor.is_finite() && factor >= 1.0 {
            Ok(Self(factor.to_bits()))
        } else {
            Err(OperatorError::Geometry {
                reason: "padding factor must be finite and at least 1",
            })
        }
    }

    /// The factor.
    #[must_use]
    pub fn get(self) -> f32 {
        f32::from_bits(self.0)
    }
}

/// The padded uv grid, its relation to the image and the mapping from
/// baseline coordinates to grid cells.
///
/// Grid planes are stored `[y][x]`, x fastest. Baseline coordinates map as
/// `x = u · scale_x + nx/2`, `y = v · scale_y + ny/2` with
/// `scale = [−nx·Δx, −ny·Δy]`, so that the MeasurementSet phase convention
/// `V = ∫ I e^{+2πi(ul + vm)}` places a source at direction cosines
/// `(l, m)` on image pixel `(l/Δx, m/Δy)` from the reference pixel after an
/// unnormalised inverse FFT.
#[derive(Clone, Debug, PartialEq)]
pub struct GridGeometry {
    image: ImageExtent,
    grid_shape: [usize; 2],
    image_origin: [usize; 2],
    scale: [f64; 2],
}

/// Integer anchor cell and fine-offset rows of one sample on the padded grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellLocation {
    /// Anchor cell along x (the nearest cell to the sample).
    pub x: i64,
    /// Anchor cell along y.
    pub y: i64,
    /// Fine-offset row along x in `0..=oversampling`.
    pub ox: u16,
    /// Fine-offset row along y in `0..=oversampling`.
    pub oy: u16,
}

impl GridGeometry {
    /// Build the padded grid for `image`.
    ///
    /// Both grid extents must be even (the centring shift swaps half
    /// planes), increments must be finite and non-zero, and the reference
    /// pixel must lie inside the image.
    pub fn new(image: ImageExtent, padding: GridPadding) -> Result<Self, OperatorError> {
        let [nx, ny] = image.shape;
        if nx == 0 || ny == 0 {
            return Err(OperatorError::Geometry {
                reason: "image extent is zero",
            });
        }
        if image
            .increment_rad
            .iter()
            .any(|increment| !increment.is_finite() || *increment == 0.0)
        {
            return Err(OperatorError::Geometry {
                reason: "pixel increment is zero or not finite",
            });
        }
        if image.reference_pixel[0] >= nx || image.reference_pixel[1] >= ny {
            return Err(OperatorError::Geometry {
                reason: "reference pixel lies outside the image",
            });
        }
        let grid_shape = match padding {
            GridPadding::None => image.shape,
            GridPadding::CasaComposite(factor) => [
                casa_composite_padded_len(nx, factor),
                casa_composite_padded_len(ny, factor),
            ],
        };
        if grid_shape.iter().any(|extent| extent % 2 != 0) {
            return Err(OperatorError::Geometry {
                reason: "grid extent must be even",
            });
        }
        // The reference pixel sits on the grid centre; the image must fit
        // on both sides of it.
        let origin = |extent: usize, reference: usize, size: usize| {
            (extent / 2)
                .checked_sub(reference)
                .filter(|origin| origin + size <= extent)
        };
        let (Some(x0), Some(y0)) = (
            origin(grid_shape[0], image.reference_pixel[0], nx),
            origin(grid_shape[1], image.reference_pixel[1], ny),
        ) else {
            return Err(OperatorError::Geometry {
                reason: "image does not fit the grid around its reference pixel",
            });
        };
        let image_origin = [x0, y0];
        Ok(Self {
            image,
            grid_shape,
            image_origin,
            scale: [
                -(grid_shape[0] as f64) * image.increment_rad[0],
                -(grid_shape[1] as f64) * image.increment_rad[1],
            ],
        })
    }

    /// The image extent.
    #[must_use]
    pub const fn image(&self) -> ImageExtent {
        self.image
    }

    /// `[nx, ny]` of the padded grid.
    #[must_use]
    pub const fn grid_shape(&self) -> [usize; 2] {
        self.grid_shape
    }

    /// Cells per grid plane.
    #[must_use]
    pub const fn cells(&self) -> usize {
        self.grid_shape[0] * self.grid_shape[1]
    }

    /// Grid cell `[x, y]` holding image pixel `[0, 0]`.
    #[must_use]
    pub const fn image_origin(&self) -> [usize; 2] {
        self.image_origin
    }

    /// Grid cells per wavelength along `[x, y]`, signed.
    #[must_use]
    pub const fn scale(&self) -> [f64; 2] {
        self.scale
    }

    /// Grid cell and fine-offset rows of one sample.
    ///
    /// The anchor is the nearest cell; the fine offset is
    /// `round((anchor − coordinate) · oversampling) + oversampling / 2`,
    /// which is why `oversampling` must be even. This is the only place
    /// that rounds sample coordinates; CPU and Metal kernels both use it.
    #[must_use]
    pub fn locate(&self, u: f64, v: f64, oversampling: u16) -> CellLocation {
        debug_assert!(oversampling.is_multiple_of(2), "oversampling must be even");
        let oversampling = f64::from(oversampling);
        let cx = u * self.scale[0] + (self.grid_shape[0] / 2) as f64;
        let cy = v * self.scale[1] + (self.grid_shape[1] / 2) as f64;
        let x = cx.round();
        let y = cy.round();
        let ox = ((x - cx) * oversampling).round() + oversampling / 2.0;
        let oy = ((y - cy) * oversampling).round() + oversampling / 2.0;
        CellLocation {
            x: x as i64,
            y: y as i64,
            ox: ox as u16,
            oy: oy as u16,
        }
    }

    /// Whether a kernel with `half_support` taps on each side of the anchor
    /// lies entirely inside the padded grid (CASA drops the sample
    /// otherwise).
    #[must_use]
    pub fn fits(&self, location: CellLocation, half_support: [u16; 2]) -> bool {
        let hx = i64::from(half_support[0]);
        let hy = i64::from(half_support[1]);
        location.x - hx >= 0
            && location.x + hx < self.grid_shape[0] as i64
            && location.y - hy >= 0
            && location.y + hy < self.grid_shape[1] as i64
    }

    /// The pointing phase gradient in radians per grid cell `[g_x, g_y]`
    /// of a pointing `offset_rad` (direction cosines `[Δl, Δm]` along the
    /// image axes) from the image centre: CASA's `pixFieldDir` per
    /// convolution-function pixel (`SimplePBConvFunc::findConvFunction`,
    /// `PointingOffsets::gradPerPixel`) times the oversampling, with the
    /// pointing's pixel offset `Δl/Δx` and the grid extent `n`:
    /// `g = −2π · (Δl/Δx) / n`.
    #[must_use]
    pub fn pointing_gradient(&self, offset_rad: [f64; 2]) -> [f32; 2] {
        let gradient = |axis: usize| {
            let pixels = offset_rad[axis] / self.image.increment_rad[axis];
            (-std::f64::consts::TAU * pixels / self.grid_shape[axis] as f64) as f32
        };
        [gradient(0), gradient(1)]
    }
}

/// CASA's padded grid length, `nextLargerEven(Int(padding_p*Float(n)-0.5))`
/// (`GridFT::init`): the product is a `Float`, the `0.5` a `double`, `Int`
/// truncates, and `CompositeNumber::nextLargerEven` takes the first even
/// 2·3·5 composite strictly above its argument.
fn casa_composite_padded_len(image_len: usize, factor: PaddingFactor) -> usize {
    let padded = f64::from(factor.get() * image_len as f32) - 0.5;
    next_larger_even_composite(padded as usize + 1)
}

/// The smallest even number at least `n` whose prime factors are 2, 3 and
/// 5. casacore's `CompositeNumber::nextLargerEven(m)` is this of `m + 1`.
pub(crate) fn next_larger_even_composite(n: usize) -> usize {
    let mut value = n.max(2);
    if !value.is_multiple_of(2) {
        value += 1;
    }
    while !is_casa_composite_len(value) {
        value += 2;
    }
    value
}

fn is_casa_composite_len(mut value: usize) -> bool {
    for factor in [2, 3, 5] {
        while value > 1 && value.is_multiple_of(factor) {
            value /= factor;
        }
    }
    value == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn geometry(nx: usize, ny: usize) -> GridGeometry {
        GridGeometry::new(
            ImageExtent {
                shape: [nx, ny],
                increment_rad: [-1.0e-5, 1.0e-5],
                reference_pixel: [nx / 2, ny / 2],
            },
            GridPadding::CasaComposite(PaddingFactor::TCLEAN),
        )
        .expect("valid geometry")
    }

    #[test]
    fn casa_padding_is_even_and_composite() {
        let tclean = PaddingFactor::TCLEAN;
        assert_eq!(casa_composite_padded_len(64, tclean), 80);
        assert_eq!(casa_composite_padded_len(100, tclean), 120);
        assert_eq!(casa_composite_padded_len(1024, tclean), 1250);
        assert_eq!(casa_composite_padded_len(7, tclean), 8);
    }

    #[test]
    fn casa_padding_steps_past_a_composite_padded_size() {
        // `Int(1.2 · 81 − 0.5) = 96` is itself an even composite, and
        // `nextLargerEven` takes the next one (#842).
        let tclean = PaddingFactor::TCLEAN;
        for (image, grid) in [(81, 100), (101, 128), (121, 150), (161, 200)] {
            assert_eq!(casa_composite_padded_len(image, tclean), grid, "{image}");
        }
        let simulator = PaddingFactor::SIMULATOR;
        assert_eq!(casa_composite_padded_len(250, simulator), 360);
        assert_eq!(casa_composite_padded_len(256, simulator), 360);
        // The product is a `Float`: 1.3f · 185 is just below 240.5, so
        // `Int(… − 0.5)` is 239, not 240.
        assert_eq!(casa_composite_padded_len(185, simulator), 240);
        assert_eq!(casa_composite_padded_len(385, simulator), 500);
    }

    #[test]
    fn padding_factors_below_one_are_refused() {
        assert_eq!(PaddingFactor::new(1.3).map(PaddingFactor::get), Ok(1.3));
        assert!(PaddingFactor::new(0.9).is_err());
        assert!(PaddingFactor::new(f32::NAN).is_err());
    }

    #[test]
    fn locate_rounds_to_nearest_cell_and_fine_row() {
        let geometry = geometry(64, 64);
        let origin = geometry.locate(0.0, 0.0, 100);
        assert_eq!(
            origin,
            CellLocation {
                x: 40,
                y: 40,
                ox: 50,
                oy: 50
            }
        );
        // 1.3 cells along +u moves against the negative x increment: x = 41.3.
        let cell = 1.0 / geometry.scale()[0];
        let sample = geometry.locate(1.3 * cell, 0.0, 100);
        assert_eq!(sample.x, 41);
        assert_eq!(sample.ox, 20);
        let sample = geometry.locate(-0.6 * cell, 0.0, 100);
        assert_eq!(sample.x, 39);
        assert_eq!(sample.ox, 10);
    }

    #[test]
    fn fits_requires_the_whole_support() {
        let geometry = geometry(64, 64);
        let edge = CellLocation {
            x: 3,
            y: 76,
            ox: 0,
            oy: 0,
        };
        assert!(geometry.fits(edge, [3, 3]));
        assert!(!geometry.fits(edge, [4, 3]));
        assert!(!geometry.fits(edge, [3, 4]));
    }

    #[test]
    fn rejects_odd_unpadded_grids_and_outside_reference_pixels() {
        let odd = GridGeometry::new(
            ImageExtent {
                shape: [63, 64],
                increment_rad: [-1.0e-5, 1.0e-5],
                reference_pixel: [31, 32],
            },
            GridPadding::None,
        );
        assert_eq!(
            odd,
            Err(OperatorError::Geometry {
                reason: "grid extent must be even"
            })
        );
        let outside = GridGeometry::new(
            ImageExtent {
                shape: [64, 64],
                increment_rad: [-1.0e-5, 1.0e-5],
                reference_pixel: [64, 32],
            },
            GridPadding::None,
        );
        assert!(matches!(outside, Err(OperatorError::Geometry { .. })));
    }

    #[test]
    fn rejects_a_reference_pixel_beyond_the_padded_grid_midpoint() {
        // 64 pixels pad to 80; the reference pixel can be at most 40 from
        // the left edge and at least 24 from the right edge.
        for reference in [[41, 32], [32, 41], [23, 32]] {
            let geometry = GridGeometry::new(
                ImageExtent {
                    shape: [64, 64],
                    increment_rad: [-1.0e-5, 1.0e-5],
                    reference_pixel: reference,
                },
                GridPadding::CasaComposite(PaddingFactor::TCLEAN),
            );
            assert_eq!(
                geometry,
                Err(OperatorError::Geometry {
                    reason: "image does not fit the grid around its reference pixel"
                }),
                "reference {reference:?}"
            );
        }
        let edge = GridGeometry::new(
            ImageExtent {
                shape: [64, 64],
                increment_rad: [-1.0e-5, 1.0e-5],
                reference_pixel: [40, 24],
            },
            GridPadding::CasaComposite(PaddingFactor::TCLEAN),
        )
        .expect("fits exactly");
        assert_eq!(edge.image_origin(), [0, 16]);
    }
}
