// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::{ImageInterface, ImagePixel};
use ndarray::{ArrayD, Dimension, IxDyn, ShapeBuilder, Zip};
use num_complex::{Complex32, Complex64};
use numpy::{
    Element, PyArrayDescrMethods, PyArrayDyn, PyArrayMethods, PyUntypedArray,
    PyUntypedArrayMethods, npyffi::PY_ARRAY_API,
};
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use crate::convert::{IntList, Variant, value::CasaValue};

pub(super) enum Pixels {
    Real(ArrayD<f64>),
    Complex(ArrayD<Complex64>),
}

impl Pixels {
    pub(super) fn capture(
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        creating: bool,
    ) -> PyResult<Self> {
        let format_error = if creating {
            "pixels is not understood, try using an array"
        } else {
            "Unsupported type for pixels parameter. It must be either a vector of doubles or ints"
        };
        if let Some(value) = value
            && let Ok(array) = value.cast::<PyUntypedArray>()
        {
            let dtype = array.dtype();
            let pixels = match (dtype.kind(), dtype.itemsize()) {
                (b'i' | b'u', _) | (b'f', 4 | 8) => Self::Real(snapshot_array(py, array)?),
                (b'c', 8 | 16) => Self::Complex(snapshot_array(py, array)?),
                _ => return Err(raised(format_error)),
            };
            return pixels.check_shape("The pixels array cannot be empty");
        }
        let variant = value.map(|v| v.extract::<Variant>()).transpose()?;
        Self::from_variant(variant, format_error)
    }

    fn from_variant(value: Option<Variant>, format_error: &str) -> PyResult<Self> {
        let pixels = match value.unwrap_or(Variant::Empty) {
            Variant::Numbers(values) => Self::Real(ndarray::Array1::from_vec(values).into_dyn()),
            Variant::Complexes(values) => {
                Self::Complex(ndarray::Array1::from_vec(values).into_dyn())
            }
            Variant::Array { shape, values } => {
                Self::Real(ArrayD::from_shape_vec(IxDyn(&shape).f(), values).map_err(raised)?)
            }
            Variant::ComplexArray { shape, values } => {
                Self::Complex(ArrayD::from_shape_vec(IxDyn(&shape).f(), values).map_err(raised)?)
            }
            Variant::Empty => return Err(raised("Exception: The pixels array cannot be empty")),
            _ => return Err(raised(format_error)),
        };
        pixels.check_shape("The pixels array cannot be empty")
    }

    fn check_shape(self, empty: &str) -> PyResult<Self> {
        if self.shape().is_empty() || self.shape().contains(&0) {
            return Err(raised(format!("Exception: {empty}")));
        }
        Ok(self)
    }

    pub(super) fn shape(&self) -> &[usize] {
        match self {
            Self::Real(data) => data.shape(),
            Self::Complex(data) => data.shape(),
        }
    }

    pub(super) fn is_complex(&self) -> bool {
        matches!(self, Self::Complex(_))
    }

    pub(super) fn f32(&self) -> ArrayD<f32> {
        match self {
            Self::Real(data) => data.mapv(|v| v as f32),
            Self::Complex(_) => unreachable!(),
        }
    }

    pub(super) fn f64(&self) -> ArrayD<f64> {
        match self {
            Self::Real(data) => data.clone(),
            Self::Complex(_) => unreachable!(),
        }
    }

    pub(super) fn c32(&self) -> ArrayD<Complex32> {
        match self {
            Self::Real(data) => data.mapv(|v| Complex32::new(v as f32, 0.0)),
            Self::Complex(data) => data.mapv(|v| Complex32::new(v.re as f32, v.im as f32)),
        }
    }

    pub(super) fn c64(&self) -> ArrayD<Complex64> {
        match self {
            Self::Real(data) => data.mapv(|v| Complex64::new(v, 0.0)),
            Self::Complex(data) => data.clone(),
        }
    }

    pub(super) fn history(&self) -> String {
        if self.shape().iter().product::<usize>() > 100 {
            return "\"[...]\"".into();
        }
        let values: Vec<String> = match self {
            Self::Real(data) => data
                .view()
                .reversed_axes()
                .iter()
                .map(|v| format!("{v:.6}"))
                .collect(),
            Self::Complex(data) => data
                .view()
                .reversed_axes()
                .iter()
                .map(|v| format!("({:.6},{:.6})", v.re, v.im))
                .collect(),
        };
        format!("[{}]", values.join(","))
    }
}

// NumPy copies into a private contiguous buffer. No caller can mutate that
// buffer while Rust reads it with the interpreter detached.
pub(super) fn snapshot_array<T: Element + Copy + Send + Sync>(
    py: Python<'_>,
    source: &Bound<'_, PyUntypedArray>,
) -> PyResult<ArrayD<T>> {
    let shape = source.shape().to_vec();
    let snapshot = PyArrayDyn::<T>::zeros(py, IxDyn(&shape), true);
    // SAFETY: both pointers name live NumPy arrays with identical shapes and
    // numeric dtypes. The destination is freshly allocated and cannot alias
    // either the input or an object another Python thread can access.
    let status = unsafe {
        PY_ARRAY_API.PyArray_CopyInto(py, snapshot.as_array_ptr(), source.as_array_ptr())
    };
    if status != 0 {
        return Err(PyErr::fetch(py));
    }
    let snapshot = snapshot.readonly();
    let values = snapshot.as_slice().map_err(raised)?;
    py.detach(|| ArrayD::from_shape_vec(IxDyn(&shape).f(), values.to_vec()).map_err(raised))
}

pub(super) struct Chunk {
    pub(super) start: Vec<usize>,
    pub(super) size: Vec<usize>,
    pub(super) stride: Vec<usize>,
}

impl Chunk {
    fn read(shape: &[usize], blc: &[i64], trc: &[i64], inc: &[i64]) -> PyResult<Self> {
        let mut chunk = Self {
            start: Vec::new(),
            size: Vec::new(),
            stride: Vec::new(),
        };
        for (axis, &length) in shape.iter().enumerate() {
            let valid = |v| usize::try_from(v).ok().filter(|&i| i < length);
            let first = blc.get(axis).and_then(|&v| valid(v)).unwrap_or(0);
            let last = trc.get(axis).and_then(|&v| valid(v)).unwrap_or(length - 1);
            let (first, last) = if first <= last {
                (first, last)
            } else {
                (0, length - 1)
            };
            let step = inc.get(axis).copied().unwrap_or(1);
            if step < 1 {
                return Err(raised("Exception: stride must be positive"));
            }
            let step = if step as usize > length {
                1
            } else {
                step as usize
            };
            chunk.start.push(first);
            chunk.size.push((last - first) / step + 1);
            chunk.stride.push(step);
        }
        Ok(chunk)
    }

    fn write(
        shape: &[usize],
        data: &[usize],
        blc: &[i64],
        inc: &[i64],
        replicate: bool,
    ) -> PyResult<Self> {
        if data.len() > shape.len() {
            return Err(raised(
                "Exception: Pixel array cannot have more dimensions than the image!",
            ));
        }
        let mut chunk = Self {
            start: Vec::new(),
            size: Vec::new(),
            stride: Vec::new(),
        };
        for (axis, &length) in shape.iter().enumerate() {
            let start = blc
                .get(axis)
                .and_then(|&v| usize::try_from(v).ok())
                .filter(|&v| v < length)
                .unwrap_or(0);
            let stride = inc.get(axis).copied().unwrap_or(1).max(1) as usize;
            let count = data.get(axis).copied().unwrap_or(1);
            if start >= length || (length - 1 - start) / stride < count - 1 {
                return Err(raised(
                    "Exception: Pixels array, including inc, extends beyond edge of image.",
                ));
            }
            let extent = if replicate {
                (length - 1 - start) / stride + 1
            } else {
                count
            };
            if replicate && extent % count != 0 {
                return Err(raised(
                    "Exception: region shape is not an integer multiple of the input array shape",
                ));
            }
            chunk.start.push(start);
            chunk.size.push(extent);
            chunk.stride.push(stride);
        }
        Ok(chunk)
    }

    pub(super) fn mask<T: ImagePixel>(
        &self,
        image: &impl ImageInterface<T>,
    ) -> PyResult<Option<ArrayD<bool>>> {
        image
            .get_mask_slice(&self.start, &self.size, &self.stride)
            .map_err(raised)
    }

    fn put<T: ImagePixel>(&self, storage: &mut Storage<T>, mut data: ArrayD<T>) -> PyResult<()> {
        if let Storage::Temp { image, .. } = storage {
            image
                .require_writable_pixels()
                .map_err(casa_images::ImageError::from)
                .map_err(raised)?;
        }
        while data.ndim() < self.start.len() {
            let axis = ndarray::Axis(data.ndim());
            data = data.insert_axis(axis);
        }
        if data.shape() != self.size {
            data = ArrayD::from_shape_fn(IxDyn(&self.size).f(), |index| {
                let source: Vec<_> = index
                    .slice()
                    .iter()
                    .zip(data.shape())
                    .map(|(&i, &n)| i % n)
                    .collect();
                data[IxDyn(&source)]
            });
        }
        if self.stride.iter().all(|&step| step == 1) {
            with_storage!(storage, image => image.put_slice(&data, &self.start)).map_err(raised)?;
        } else {
            let mut pos = self.start.clone();
            for (index, &value) in data.indexed_iter() {
                for (axis, &i) in index.slice().iter().enumerate() {
                    pos[axis] = self.start[axis] + i * self.stride[axis];
                }
                with_storage!(storage, image => image.put_at(value, &pos)).map_err(raised)?;
            }
        }
        Ok(())
    }

    fn get<T: ImagePixel, U: Average>(
        &self,
        image: &impl ImageInterface<T>,
        request: &ChunkRead<'_>,
        expression: Option<ArrayD<bool>>,
        wide: (impl Fn(T) -> U, impl Fn(U) -> U),
    ) -> PyResult<CasaValue> {
        let mask = if request.getmask || !request.axes.is_empty() {
            and_mask(self.mask(image)?, expression)
        } else {
            None
        };
        self.format(
            request.axes,
            request.dropdeg,
            request.getmask,
            wide,
            mask,
            || {
                image
                    .get_slice(&self.start, &self.size, &self.stride)
                    .map_err(raised)
            },
        )
    }

    fn format<T: ImagePixel, U: Average>(
        &self,
        axes: &[usize],
        dropdeg: bool,
        getmask: bool,
        wide: (impl Fn(T) -> U, impl Fn(U) -> U),
        mask: Option<ArrayD<bool>>,
        read: impl FnOnce() -> PyResult<ArrayD<T>>,
    ) -> PyResult<CasaValue> {
        let (widen, round) = wide;
        let mut shape = self.size.clone();
        for &axis in axes {
            shape[axis] = 1;
        }
        let output_shape = if dropdeg {
            let axes: Vec<_> = shape.iter().copied().filter(|&n| n != 1).collect();
            if axes.is_empty() { vec![1] } else { axes }
        } else {
            shape.clone()
        };
        let collapse_index = |index: &[usize], output: &mut [usize]| {
            output.copy_from_slice(index);
            for &axis in axes {
                output[axis] = 0;
            }
        };
        if !axes.is_empty() && mask.as_ref().is_some_and(|mask| !mask.iter().any(|&v| v)) {
            return Err(raised("All selected pixels are masked."));
        }
        if getmask {
            let values = match mask {
                None => vec![true; shape.iter().product()],
                Some(mask) if axes.is_empty() => fortran_values(mask),
                Some(mask) => {
                    let mut valid = ArrayD::from_elem(IxDyn(&shape).f(), false);
                    let mut output = vec![0; shape.len()];
                    for (index, &pixel_valid) in mask.indexed_iter() {
                        if pixel_valid {
                            collapse_index(index.slice(), &mut output);
                            valid[IxDyn(&output)] = true;
                        }
                    }
                    fortran_values(valid)
                }
            };
            return Ok(CasaValue::Bools {
                shape: output_shape,
                values,
            });
        }
        let pixels = read()?;
        if axes.is_empty() {
            return Ok(U::output(
                output_shape,
                pixels.reversed_axes().iter().copied().map(&widen).collect(),
            ));
        }

        let mut sums = ArrayD::from_elem(IxDyn(&shape).f(), U::default());
        let mut counts = ArrayD::from_elem(IxDyn(&shape).f(), 0_usize);
        let mut out_index = vec![0; shape.len()];
        for (index, &pixel) in pixels.indexed_iter() {
            if mask.as_ref().is_some_and(|mask| !mask[&index]) {
                continue;
            }
            collapse_index(index.slice(), &mut out_index);
            counts[IxDyn(&out_index)] += 1;
            sums[IxDyn(&out_index)] += widen(pixel);
        }
        let values = fortran_values(sums)
            .into_iter()
            .zip(fortran_values(counts))
            .map(|(sum, n)| {
                if n == 0 {
                    U::default()
                } else {
                    round(sum / (n as f64))
                }
            })
            .collect();
        Ok(U::output(output_shape, values))
    }
}

pub(super) fn and_mask(
    original: Option<ArrayD<bool>>,
    expression: Option<ArrayD<bool>>,
) -> Option<ArrayD<bool>> {
    match (original, expression) {
        (Some(mut original), Some(expression)) => {
            Zip::from(&mut original)
                .and(&expression)
                .for_each(|valid, &passes| *valid &= passes);
            Some(original)
        }
        (original, expression) => original.or(expression),
    }
}

pub(super) struct ChunkRead<'a> {
    pub axes: &'a [usize],
    pub dropdeg: bool,
    pub getmask: bool,
}

pub(super) fn collapse_axes(axes: &[i64], rank: usize) -> PyResult<Vec<usize>> {
    if matches!(axes, [only] if *only < 0) {
        return Ok(Vec::new());
    }
    let mut collapse = Vec::with_capacity(axes.len());
    for &axis in axes {
        if axis < 0 || axis as usize >= rank || collapse.contains(&(axis as usize)) {
            return Err(raised("IPosition::makeAxisPath: invalid defined axes"));
        }
        collapse.push(axis as usize);
    }
    Ok(collapse)
}

impl Chunk {
    pub(super) fn read_from(
        &self,
        image: &ImageHandle,
        request: &ChunkRead<'_>,
        expression: Option<ArrayD<bool>>,
    ) -> PyResult<CasaValue> {
        if let Some(fits) = image.fits() {
            let pixels = fits
                .read(&self.start, &self.size, &self.stride)
                .map_err(raised)?;
            let mask = (request.getmask || !request.axes.is_empty())
                .then(|| pixels.mapv(|value| !value.is_nan()));
            return self.format(
                request.axes,
                request.dropdeg,
                request.getmask,
                (f64::from, |v| f64::from(v as f32)),
                and_mask(mask, expression),
                || Ok(pixels),
            );
        }
        match image {
            ImageHandle::Float(storage) => {
                with_storage!(storage, image => self.get(image.as_ref(), request, expression, (f64::from, |v| f64::from(v as f32))))
            }
            ImageHandle::Double(storage) => {
                with_storage!(storage, image => self.get(image.as_ref(), request, expression, (f64::from, std::convert::identity)))
            }
            ImageHandle::Complex(storage) => {
                with_storage!(storage, image => self.get(image.as_ref(), request, expression, (widen_complex, |v: Complex64| widen_complex(Complex32::new(v.re as f32, v.im as f32)))))
            }
            ImageHandle::DComplex(storage) => {
                with_storage!(storage, image => self.get(image.as_ref(), request, expression, (std::convert::identity, std::convert::identity)))
            }
        }
    }
}

fn widen_complex(value: Complex32) -> Complex64 {
    Complex64::new(f64::from(value.re), f64::from(value.im))
}

fn fortran_values<T: Copy>(data: ArrayD<T>) -> Vec<T> {
    data.reversed_axes().iter().copied().collect()
}

trait Average: Copy + Default + std::ops::AddAssign + std::ops::Div<f64, Output = Self> {
    fn output(shape: Vec<usize>, values: Vec<Self>) -> CasaValue;
}
impl Average for f64 {
    fn output(shape: Vec<usize>, values: Vec<Self>) -> CasaValue {
        CasaValue::Floats { shape, values }
    }
}
impl Average for Complex64 {
    fn output(shape: Vec<usize>, values: Vec<Self>) -> CasaValue {
        CasaValue::Complexes { shape, values }
    }
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyImageTool {
    #[pyo3(signature = (blc=None, trc=None, inc=None, axes=None, list=false, dropdeg=false, getmask=false), text_signature = "($self, blc=[-1], trc=[-1], inc=[1], axes=[-1], list=False, dropdeg=False, getmask=False)")]
    fn getchunk(
        &self,
        py: Python<'_>,
        blc: Option<IntList>,
        trc: Option<IntList>,
        inc: Option<IntList>,
        axes: Option<IntList>,
        list: bool,
        dropdeg: bool,
        getmask: bool,
    ) -> PyResult<Py<PyAny>> {
        let _ = list;
        let value = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::Bools { shape: vec![0], values: Vec::new() });
            };
            let mut image = attachment.lock();
            attachment.require_pixel_read_lock(&mut image).map_err(raised)?;
            let shape = image.shape();
            if image.coordinates().n_pixel_axes() != shape.len() {
                return Err(raised("LCSlicer::makeComplete - referencePixel and newLatticeShape vectors do not have same length"));
            }
            let chunk = Chunk::read(shape, &blc.map_or_else(Vec::new, |arg| arg.0), &trc.map_or_else(Vec::new, |arg| arg.0), &inc.map_or_else(Vec::new, |arg| arg.0))?;
            let collapse = collapse_axes(axes.as_ref().map_or(&[], |axes| axes.0.as_slice()), shape.len())?;
            let value = chunk.read_from(&image, &ChunkRead { axes: &collapse, dropdeg, getmask }, None)?;
            attachment.release_requested_lock(&mut image).map_err(raised)?;
            Ok(value)
        })?;
        value.into_python(py)
    }

    #[pyo3(signature = (pixels=None, blc=None, inc=None, list=false, locking=true, replicate=false), text_signature = "($self, pixels=[], blc=[-1], inc=[1], list=False, locking=True, replicate=False)")]
    fn putchunk(
        &self,
        py: Python<'_>,
        pixels: Option<&Bound<'_, PyAny>>,
        blc: Option<IntList>,
        inc: Option<IntList>,
        list: bool,
        locking: bool,
        replicate: bool,
    ) -> PyResult<bool> {
        let pixels = Pixels::capture(py, pixels, false);
        py.detach(|| {
            let state = self.state();
            let history = state.history;
            let Some(attachment) = state.image.as_ref() else { return Ok(false) };
            let mut image = attachment.lock();
            let pixels = pixels?;
            let blc = blc.unwrap_or_else(|| IntList::of(&[-1])).0;
            let inc = inc.unwrap_or_else(|| IntList::of(&[1])).0;
            let chunk = Chunk::write(image.shape(), pixels.shape(), &blc, &inc, replicate)?;
            if pixels.is_complex() && matches!(&*image, ImageHandle::Float(_) | ImageHandle::Double(_)) {
                return Err(raised("Exception: Unsupported type for pixels parameter. It must be either a vector of doubles or ints"));
            }
            if image.fits().is_some() {
                return Err(raised("FITSImage::putSlice - is not possible as FITSImage is not writable"));
            }
            attachment.require_lock(&mut image, casa_tables::LockType::Write).map_err(raised)?;
            match &mut *image {
                ImageHandle::Float(storage) => chunk.put(storage, pixels.f32())?,
                ImageHandle::Double(storage) => chunk.put(storage, pixels.f64())?,
                ImageHandle::Complex(storage) => chunk.put(storage, pixels.c32())?,
                ImageHandle::DComplex(storage) => chunk.put(storage, pixels.c64())?,
            }
            if locking && cfg!(unix) {
                image.unlock_storage().map_err(raised)?;
            } else {
                image.save().map_err(raised)?;
                attachment.release_requested_lock(&mut image).map_err(raised)?;
            }
            if history {
                let command = format!(
                    "ia.putchunk(pixels={}, blc={}, inc={}, list={list}, locking={locking}, replicate={replicate})",
                    pixels.history(), format!("{blc:?}").replace(' ', ""), format!("{inc:?}").replace(' ', "")
                );
                image.record_history("putchunk", command).map_err(raised)?;
            }
            Ok(true)
        })
    }
}
