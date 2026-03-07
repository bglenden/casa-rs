// SPDX-License-Identifier: LGPL-3.0-or-later
//! In-memory lattice backed by an `ArrayD<T>`.

use ndarray::{ArrayD, IxDyn, SliceInfoElem};

use crate::element::LatticeElement;
use crate::error::LatticeError;
use crate::lattice::{Lattice, LatticeMut};

/// An in-memory lattice wrapping an [`ndarray::ArrayD<T>`].
///
/// Corresponds to the C++ `ArrayLattice<T>` class: a simple lattice whose
/// data lives entirely in process memory. This is useful for testing, for
/// small datasets, and as a building block for [`TempLattice`](crate::TempLattice)
/// when the data fits below the paging threshold.
///
/// `ArrayLattice` is always writable, never persistent, and never paged.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::{ArrayLattice, Lattice, LatticeMut};
///
/// // Create a 4×4 lattice of zeros:
/// let mut lat = ArrayLattice::<f64>::zeros(vec![4, 4]);
/// assert_eq!(lat.shape(), &[4, 4]);
/// assert_eq!(lat.nelements(), 16);
///
/// // Set a value and read it back:
/// lat.put_at(3.14, &[1, 2]).unwrap();
/// assert_eq!(lat.get_at(&[1, 2]).unwrap(), 3.14);
/// ```
#[derive(Debug, Clone)]
pub struct ArrayLattice<T: LatticeElement> {
    data: ArrayD<T>,
}

impl<T: LatticeElement> ArrayLattice<T> {
    /// Creates an `ArrayLattice` wrapping an existing array.
    ///
    /// The lattice shape is taken from the array's shape.
    pub fn new(data: ArrayD<T>) -> Self {
        Self { data }
    }

    /// Creates an `ArrayLattice` filled with the default value for `T`.
    ///
    /// The default value is determined by [`LatticeElement::default_value`].
    pub fn zeros(shape: Vec<usize>) -> Self {
        Self {
            data: ArrayD::from_elem(IxDyn(&shape), T::default_value()),
        }
    }

    /// Returns a shared reference to the underlying array.
    pub fn array(&self) -> &ArrayD<T> {
        &self.data
    }

    /// Returns a mutable reference to the underlying array.
    pub fn array_mut(&mut self) -> &mut ArrayD<T> {
        &mut self.data
    }

    /// Consumes the lattice and returns the underlying array.
    pub fn into_array(self) -> ArrayD<T> {
        self.data
    }
}

impl<T: LatticeElement> From<ArrayD<T>> for ArrayLattice<T> {
    fn from(data: ArrayD<T>) -> Self {
        Self::new(data)
    }
}

/// Validates that `position` is within `shape` bounds.
fn validate_position(position: &[usize], shape: &[usize]) -> Result<(), LatticeError> {
    if position.len() != shape.len() {
        return Err(LatticeError::NdimMismatch {
            expected: shape.len(),
            got: position.len(),
        });
    }
    for (i, (&p, &s)) in position.iter().zip(shape.iter()).enumerate() {
        if p >= s {
            return Err(LatticeError::IndexOutOfBounds {
                index: position.to_vec(),
                shape: shape.to_vec(),
            });
        }
        let _ = i;
    }
    Ok(())
}

/// Validates slice parameters against a lattice shape.
fn validate_slice(
    start: &[usize],
    slice_shape: &[usize],
    stride: &[usize],
    lattice_shape: &[usize],
) -> Result<(), LatticeError> {
    let ndim = lattice_shape.len();
    if start.len() != ndim || slice_shape.len() != ndim || stride.len() != ndim {
        return Err(LatticeError::NdimMismatch {
            expected: ndim,
            got: start.len(),
        });
    }
    for axis in 0..ndim {
        // The last element touched is: start + (shape-1) * stride
        // This must be < lattice_shape[axis]
        if slice_shape[axis] == 0 {
            continue;
        }
        let last = start[axis] + (slice_shape[axis] - 1) * stride[axis];
        if last >= lattice_shape[axis] {
            return Err(LatticeError::SliceOutOfBounds {
                start: start.to_vec(),
                slice_shape: slice_shape.to_vec(),
                stride: stride.to_vec(),
                lattice_shape: lattice_shape.to_vec(),
            });
        }
    }
    Ok(())
}

impl<T: LatticeElement> Lattice<T> for ArrayLattice<T> {
    fn shape(&self) -> &[usize] {
        self.data.shape()
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        validate_position(position, self.data.shape())?;
        Ok(self.data[IxDyn(position)].clone())
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        validate_slice(start, shape, stride, self.data.shape())?;

        let slice_info: Vec<SliceInfoElem> = start
            .iter()
            .zip(shape.iter())
            .zip(stride.iter())
            .map(|((&s, &n), &st)| {
                let end = if n == 0 { s } else { s + n * st };
                SliceInfoElem::Slice {
                    start: s as isize,
                    end: Some(end as isize),
                    step: st as isize,
                }
            })
            .collect();

        let view = self.data.slice(slice_info.as_slice());
        Ok(view.to_owned())
    }
}

impl<T: LatticeElement> LatticeMut<T> for ArrayLattice<T> {
    fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError> {
        validate_position(position, self.data.shape())?;
        self.data[IxDyn(position)] = value;
        Ok(())
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        let shape = self.data.shape();
        if start.len() != shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: shape.len(),
                got: start.len(),
            });
        }
        // Validate that data fits within lattice bounds.
        for (axis, ((&s, &ds), &ls)) in start
            .iter()
            .zip(data.shape().iter())
            .zip(shape.iter())
            .enumerate()
        {
            if s + ds > ls {
                return Err(LatticeError::SliceOutOfBounds {
                    start: start.to_vec(),
                    slice_shape: data.shape().to_vec(),
                    stride: vec![1; shape.len()],
                    lattice_shape: shape.to_vec(),
                });
            }
            let _ = axis;
        }

        // Write data into the lattice.
        let slice_info: Vec<SliceInfoElem> = start
            .iter()
            .zip(data.shape().iter())
            .map(|(&s, &n)| SliceInfoElem::Slice {
                start: s as isize,
                end: Some((s + n) as isize),
                step: 1,
            })
            .collect();

        let mut view = self.data.slice_mut(slice_info.as_slice());
        view.assign(data);
        Ok(())
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
        self.data.fill(value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::Array;
    use num_complex::Complex64;

    #[test]
    fn zeros_shape() {
        let lat = ArrayLattice::<f64>::zeros(vec![3, 4, 5]);
        assert_eq!(lat.shape(), &[3, 4, 5]);
        assert_eq!(lat.ndim(), 3);
        assert_eq!(lat.nelements(), 60);
    }

    #[test]
    fn not_persistent_or_paged() {
        let lat = ArrayLattice::<f32>::zeros(vec![2, 2]);
        assert!(!lat.is_persistent());
        assert!(!lat.is_paged());
        assert!(lat.is_writable());
    }

    #[test]
    fn get_at_put_at_roundtrip() {
        let mut lat = ArrayLattice::<i32>::zeros(vec![3, 4]);
        lat.put_at(42, &[1, 2]).unwrap();
        assert_eq!(lat.get_at(&[1, 2]).unwrap(), 42);
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 0);
    }

    #[test]
    fn get_at_out_of_bounds() {
        let lat = ArrayLattice::<f64>::zeros(vec![3, 4]);
        assert!(lat.get_at(&[3, 0]).is_err());
        assert!(lat.get_at(&[0, 4]).is_err());
    }

    #[test]
    fn get_at_ndim_mismatch() {
        let lat = ArrayLattice::<f64>::zeros(vec![3, 4]);
        assert!(matches!(
            lat.get_at(&[0]),
            Err(LatticeError::NdimMismatch { .. })
        ));
    }

    #[test]
    fn get_slice_full() {
        let data = Array::from_shape_fn(IxDyn(&[3, 4]), |idx| (idx[0] * 4 + idx[1]) as f64);
        let lat = ArrayLattice::new(data.clone());
        let result = lat.get_slice(&[0, 0], &[3, 4], &[1, 1]).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn get_slice_strided() {
        let data = Array::from_shape_fn(IxDyn(&[6, 6]), |idx| (idx[0] * 6 + idx[1]) as f64);
        let lat = ArrayLattice::new(data);
        let result = lat.get_slice(&[0, 0], &[3, 3], &[2, 2]).unwrap();
        assert_eq!(result.shape(), &[3, 3]);
        // Element at [1,1] in result = data[2,2] = 14
        assert_eq!(result[IxDyn(&[1, 1])], 14.0);
    }

    #[test]
    fn get_slice_out_of_bounds() {
        let lat = ArrayLattice::<f64>::zeros(vec![4, 4]);
        assert!(lat.get_slice(&[2, 2], &[3, 3], &[1, 1]).is_err());
    }

    #[test]
    fn put_slice_writes_subregion() {
        let mut lat = ArrayLattice::<i32>::zeros(vec![4, 4]);
        let patch = Array::from_shape_fn(IxDyn(&[2, 2]), |idx| (idx[0] * 2 + idx[1] + 1) as i32);
        lat.put_slice(&patch, &[1, 1]).unwrap();

        assert_eq!(lat.get_at(&[1, 1]).unwrap(), 1);
        assert_eq!(lat.get_at(&[1, 2]).unwrap(), 2);
        assert_eq!(lat.get_at(&[2, 1]).unwrap(), 3);
        assert_eq!(lat.get_at(&[2, 2]).unwrap(), 4);
        assert_eq!(lat.get_at(&[0, 0]).unwrap(), 0); // untouched
    }

    #[test]
    fn put_slice_out_of_bounds() {
        let mut lat = ArrayLattice::<f64>::zeros(vec![4, 4]);
        let patch = ArrayD::from_elem(IxDyn(&[3, 3]), 1.0);
        assert!(lat.put_slice(&patch, &[2, 2]).is_err());
    }

    #[test]
    fn set_fills_all() {
        let mut lat = ArrayLattice::<f64>::zeros(vec![3, 3]);
        lat.set(7.0).unwrap();
        for &v in lat.array().iter() {
            assert_eq!(v, 7.0);
        }
    }

    #[test]
    fn apply_transforms() {
        let mut lat = ArrayLattice::new(ArrayD::from_elem(IxDyn(&[2, 3]), 5i32));
        lat.apply(|&v| v * 2).unwrap();
        for &v in lat.array().iter() {
            assert_eq!(v, 10);
        }
    }

    #[test]
    fn copy_data_between_lattices() {
        let src = ArrayLattice::new(Array::from_shape_fn(IxDyn(&[3, 3]), |idx| {
            (idx[0] * 3 + idx[1]) as f64
        }));
        let mut dst = ArrayLattice::<f64>::zeros(vec![3, 3]);
        dst.copy_data(&src).unwrap();
        assert_eq!(dst.array(), src.array());
    }

    #[test]
    fn copy_data_shape_mismatch() {
        let src = ArrayLattice::<f64>::zeros(vec![3, 3]);
        let mut dst = ArrayLattice::<f64>::zeros(vec![4, 4]);
        assert!(matches!(
            dst.copy_data(&src),
            Err(LatticeError::ShapeMismatch { .. })
        ));
    }

    #[test]
    fn get_full_lattice() {
        let data = Array::from_shape_fn(IxDyn(&[2, 3]), |idx| (idx[0] * 3 + idx[1]) as i32);
        let lat = ArrayLattice::new(data.clone());
        let result = lat.get().unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn from_arrayd_conversion() {
        let data = ArrayD::from_elem(IxDyn(&[2, 2]), 1.0f32);
        let lat: ArrayLattice<f32> = data.clone().into();
        assert_eq!(lat.shape(), &[2, 2]);
        assert_eq!(lat.array(), &data);
    }

    // Test all 12 element types compile and work.
    #[test]
    fn all_element_types() {
        let _ = ArrayLattice::<bool>::zeros(vec![2]);
        let _ = ArrayLattice::<u8>::zeros(vec![2]);
        let _ = ArrayLattice::<i16>::zeros(vec![2]);
        let _ = ArrayLattice::<u16>::zeros(vec![2]);
        let _ = ArrayLattice::<i32>::zeros(vec![2]);
        let _ = ArrayLattice::<u32>::zeros(vec![2]);
        let _ = ArrayLattice::<i64>::zeros(vec![2]);
        let _ = ArrayLattice::<f32>::zeros(vec![2]);
        let _ = ArrayLattice::<f64>::zeros(vec![2]);
        let _ = ArrayLattice::<num_complex::Complex32>::zeros(vec![2]);
        let _ = ArrayLattice::<Complex64>::zeros(vec![2]);
        let _ = ArrayLattice::<String>::zeros(vec![2]);
    }

    #[test]
    fn complex_element_roundtrip() {
        let mut lat = ArrayLattice::<Complex64>::zeros(vec![2, 2]);
        let val = Complex64::new(1.5, -2.5);
        lat.put_at(val, &[0, 1]).unwrap();
        assert_eq!(lat.get_at(&[0, 1]).unwrap(), val);
    }

    #[test]
    fn string_element_roundtrip() {
        let mut lat = ArrayLattice::<String>::zeros(vec![3]);
        lat.put_at("hello".to_string(), &[1]).unwrap();
        assert_eq!(lat.get_at(&[1]).unwrap(), "hello");
        assert_eq!(lat.get_at(&[0]).unwrap(), "");
    }

    #[test]
    fn nice_cursor_shape_small() {
        let lat = ArrayLattice::<f64>::zeros(vec![4, 4]);
        let cursor = lat.nice_cursor_shape();
        assert_eq!(cursor, vec![4, 4]); // Small enough to fit entirely
    }

    #[test]
    fn nice_cursor_shape_large() {
        let lat = ArrayLattice::<f64>::zeros(vec![1024, 1024, 512]);
        let cursor = lat.nice_cursor_shape();
        let product: usize = cursor.iter().product();
        assert!(product <= lat.advised_max_pixels());
        assert_eq!(cursor.len(), 3);
    }
}
