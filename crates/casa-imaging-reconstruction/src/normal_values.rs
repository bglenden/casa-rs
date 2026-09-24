// SPDX-License-Identifier: LGPL-3.0-or-later

//! Allocation-free views of the normal state's actual numerical storage.

use num_complex::Complex64;
use std::borrow::Cow;
use std::ops::Range;

/// Borrowed image values. Reading a compact real image never materializes a
/// complex image; callers needing scratch storage must allocate it explicitly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NormalValues<'a> {
    /// Compact real channel-local image.
    Real(&'a [f32]),
    /// Complex normal family, including coupled spectral coefficients.
    Complex(&'a [Complex64]),
}

impl<'a> NormalValues<'a> {
    /// Number of image values.
    pub fn len(self) -> usize {
        match self {
            Self::Real(v) => v.len(),
            Self::Complex(v) => v.len(),
        }
    }

    /// Whether the view contains no values.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Read one value, widening only that scalar when storage is real.
    pub fn value(self, index: usize) -> Complex64 {
        match self {
            Self::Real(v) => Complex64::new(f64::from(v[index]), 0.0),
            Self::Complex(v) => v[index],
        }
    }

    /// Borrow a bounded subrange without allocating.
    pub fn slice(self, range: Range<usize>) -> Option<Self> {
        match self {
            Self::Real(v) => v.get(range).map(Self::Real),
            Self::Complex(v) => v.get(range).map(Self::Complex),
        }
    }

    /// Borrow the existing complex representation, if present. This does not
    /// convert a real image or silently return an empty complex image.
    pub fn complex(self) -> Option<&'a [Complex64]> {
        match self {
            Self::Complex(v) => Some(v),
            Self::Real(_) => None,
        }
    }

    /// Borrow the existing compact real representation, if present.
    pub fn real(self) -> Option<&'a [f32]> {
        match self {
            Self::Real(v) => Some(v),
            Self::Complex(_) => None,
        }
    }

    /// Iterate values without retaining a converted array.
    pub fn iter(self) -> NormalValuesIter<'a> {
        NormalValuesIter {
            values: self,
            indices: 0..self.len(),
        }
    }
}

impl<'a, T: AsRef<[Complex64]> + ?Sized> From<&'a T> for NormalValues<'a> {
    fn from(values: &'a T) -> Self {
        Self::Complex(values.as_ref())
    }
}

/// Scalar iterator over a borrowed normal image, without a conversion buffer.
pub struct NormalValuesIter<'a> {
    values: NormalValues<'a>,
    indices: Range<usize>,
}

impl Iterator for NormalValuesIter<'_> {
    type Item = Complex64;
    fn next(&mut self) -> Option<Self::Item> {
        self.indices.next().map(|i| self.values.value(i))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.indices.size_hint()
    }
}
impl ExactSizeIterator for NormalValuesIter<'_> {}

impl<'a> IntoIterator for NormalValues<'a> {
    type Item = Complex64;
    type IntoIter = NormalValuesIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Owned or borrowed plane payload. Exactly one representation is live.
#[derive(Debug)]
pub(crate) enum NormalPlane<'a> {
    Real(Cow<'a, [f32]>),
    Complex(Cow<'a, [Complex64]>),
}

impl<'a> NormalPlane<'a> {
    pub(crate) fn borrowed(values: NormalValues<'a>) -> Self {
        match values {
            NormalValues::Real(v) => Self::Real(Cow::Borrowed(v)),
            NormalValues::Complex(v) => Self::Complex(Cow::Borrowed(v)),
        }
    }

    pub(crate) fn values(&self) -> NormalValues<'_> {
        match self {
            Self::Real(v) => NormalValues::Real(v),
            Self::Complex(v) => NormalValues::Complex(v),
        }
    }
}

/// Borrowed sensitivity, either spatially varying or constant within each plane.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SensitivityValues<'a> {
    /// Spatially varying response.
    Dense(&'a [f64]),
    /// One scalar response per image plane, without a repeated image buffer.
    PerPlane {
        /// Response of each plane, in storage order.
        weights: &'a [f64],
        /// Number of pixels in one plane.
        cells: usize,
    },
}

impl<'a> SensitivityValues<'a> {
    /// Borrow an existing dense response, if the response is spatially varying.
    pub fn dense(self) -> Option<&'a [f64]> {
        match self {
            Self::Dense(v) => Some(v),
            Self::PerPlane { .. } => None,
        }
    }

    /// Iterate logical values without constructing a repeated response array.
    pub fn iter(self) -> impl Iterator<Item = f64> + 'a {
        let (dense, weights, cells): (&[f64], &[f64], usize) = match self {
            Self::Dense(v) => (v, &[], 0),
            Self::PerPlane { weights, cells } => (&[], weights, cells),
        };
        dense.iter().copied().chain(
            weights
                .iter()
                .flat_map(move |&weight| std::iter::repeat_n(weight, cells)),
        )
    }
}

#[test]
fn plane_borrows_and_moves_preserve_the_single_allocation() {
    let real = vec![1.0_f32, -2.5, 0.0];
    let pointer = real.as_ptr();
    let plane = NormalPlane::Real(Cow::Owned(real));
    let borrowed = NormalPlane::borrowed(plane.values().slice(1..3).unwrap());
    assert_eq!(
        borrowed.values().real().unwrap().as_ptr(),
        pointer.wrapping_add(1)
    );
    assert!(borrowed.values().complex().is_none());
    assert_eq!(borrowed.values().value(0), Complex64::new(-2.5, 0.0));
    assert!(plane.values().slice(0..4).is_none());
    drop(borrowed);
    let moved = plane;
    assert_eq!(moved.values().real().unwrap().as_ptr(), pointer);

    let complex = [Complex64::new(2.0, -3.0), Complex64::new(4.0, 5.0)];
    let borrowed = NormalPlane::borrowed(NormalValues::Complex(&complex));
    assert_eq!(
        borrowed.values().complex().unwrap().as_ptr(),
        complex.as_ptr()
    );
    assert_eq!(borrowed.values().iter().collect::<Vec<_>>(), complex);
    assert!(borrowed.values().real().is_none());
    assert_eq!(
        SensitivityValues::PerPlane {
            weights: &[2.0, 3.0],
            cells: 2
        }
        .iter()
        .collect::<Vec<_>>(),
        [2.0, 2.0, 3.0, 3.0]
    );
}
