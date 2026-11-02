// SPDX-License-Identifier: LGPL-3.0-or-later
//! UVW coordinates as a measure: casacore's `Muvw`.

use super::direction::DirectionRef;

/// One baseline's `[u, v, w]` in metres, in a direction frame.
///
/// casacore's `Muvw` references are the direction frames (`J2000`, `ICRS`,
/// `ITRF`, …), so the reference is a [`DirectionRef`]. Bulk UVW live in a
/// [`UvwView`](super::UvwView); this is the scalar measure of one of its
/// elements, assembled on the stack.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MUvw {
    metres: [f64; 3],
    refer: DirectionRef,
}

impl MUvw {
    /// The coordinates `metres` in frame `refer`.
    #[must_use]
    pub const fn new(metres: [f64; 3], refer: DirectionRef) -> Self {
        Self { metres, refer }
    }

    /// `[u, v, w]` in metres.
    #[must_use]
    pub const fn metres(&self) -> [f64; 3] {
        self.metres
    }

    /// The reference frame.
    #[must_use]
    pub const fn refer(&self) -> DirectionRef {
        self.refer
    }

    /// The baseline length `sqrt(u² + v² + w²)` in metres.
    #[must_use]
    pub fn length_m(&self) -> f64 {
        let [u, v, w] = self.metres;
        (u * u + v * v + w * w).sqrt()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_uvw_measure_keeps_its_frame_and_length() {
        let uvw = MUvw::new([3.0, 4.0, 12.0], DirectionRef::J2000);
        assert_eq!(uvw.refer(), DirectionRef::J2000);
        assert_eq!(uvw.length_m(), 13.0);
    }
}
