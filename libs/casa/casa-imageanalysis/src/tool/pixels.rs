// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel array chunk extraction and writing (`ia.getchunk`, `ia.putchunk`, `ia.set`).

use casa_core::{Complex32, Complex64};
use casa_images::{ImageInterface, ImagePixel};
use ndarray::{ArrayD, Dimension, IxDyn, ShapeBuilder, Zip};

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, Storage, with_storage};

/// Multi-dimensional pixel buffer tagged with its CASA pixel type.
#[derive(Debug, Clone)]
pub enum PixelChunk {
    /// Single-precision real pixels.
    Float(ArrayD<f32>),
    /// Double-precision real pixels.
    Double(ArrayD<f64>),
    /// Single-precision complex pixels.
    Complex(ArrayD<Complex32>),
    /// Double-precision complex pixels.
    DComplex(ArrayD<Complex64>),
}

impl PixelChunk {
    /// Returns the array dimensionality.
    pub fn shape(&self) -> &[usize] {
        match self {
            Self::Float(a) => a.shape(),
            Self::Double(a) => a.shape(),
            Self::Complex(a) => a.shape(),
            Self::DComplex(a) => a.shape(),
        }
    }
}

/// Element-wise logical AND between an optional image/region mask and an optional expression mask.
pub fn and_mask(
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

/// Validates CASA reduction/collapse axis indices against image `rank`.
///
/// A single negative entry (e.g. `[-1]`) denotes collapsing all axes (`Ok(vec![])`).
pub fn collapse_axes(axes: &[i64], rank: usize) -> ImageToolResult<Vec<usize>> {
    if matches!(axes, [only] if *only < 0) {
        return Ok(Vec::new());
    }
    let mut collapse = Vec::with_capacity(axes.len());
    for &axis in axes {
        if axis < 0 || axis as usize >= rank || collapse.contains(&(axis as usize)) {
            return Err(ImageToolError::new(
                "IPosition::makeAxisPath: invalid defined axes",
            ));
        }
        collapse.push(axis as usize);
    }
    Ok(collapse)
}

/// Flattened Fortran-order pixel or mask array returned by `ia.getchunk` and `ia.getregion`.
#[derive(Debug, Clone)]
pub enum ChunkData {
    /// Boolean mask values (`getmask=True` or detached fallback).
    Bools {
        /// Output array shape.
        shape: Vec<usize>,
        /// Column-major values.
        values: Vec<bool>,
    },
    /// Real values promoted to `f64` matching casacore `getchunk` output.
    Floats {
        /// Output array shape.
        shape: Vec<usize>,
        /// Column-major values.
        values: Vec<f64>,
    },
    /// Complex values promoted to `Complex64` matching casacore `getchunk` output.
    Complexes {
        /// Output array shape.
        shape: Vec<usize>,
        /// Column-major values.
        values: Vec<Complex64>,
    },
}

/// Optional axis-averaging and mask flags for [`Chunk::read_from`].
pub struct ChunkRead<'a> {
    /// Axes along which to collapse by weighted mean.
    pub axes: &'a [usize],
    /// Whether to squeeze singleton axes (keeping at least rank 1).
    pub dropdeg: bool,
    /// Whether to return the validity mask instead of pixel values.
    pub getmask: bool,
}

/// Strided box selection used by `ia.getchunk`, `ia.putchunk`, and `ia.getregion`.
pub struct Chunk {
    /// Inclusive starting pixel along each axis.
    pub start: Vec<usize>,
    /// Number of samples along each axis.
    pub size: Vec<usize>,
    /// Positive stride along each axis.
    pub stride: Vec<usize>,
}

impl Chunk {
    /// Resolves `getchunk` bounds `(blc, trc, inc)` against `shape` with CASA's defaults/clamping.
    pub fn read(shape: &[usize], blc: &[i64], trc: &[i64], inc: &[i64]) -> ImageToolResult<Self> {
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
                return Err(ImageToolError::new("Exception: stride must be positive"));
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

    /// Validates `putchunk` bounds `(blc, inc, replicate)` against `shape` and input `data` shape.
    pub fn write(
        shape: &[usize],
        data: &[usize],
        blc: &[i64],
        inc: &[i64],
        replicate: bool,
    ) -> ImageToolResult<Self> {
        if data.len() > shape.len() {
            return Err(ImageToolError::new(
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
                return Err(ImageToolError::new(
                    "Exception: Pixels array, including inc, extends beyond edge of image.",
                ));
            }
            let extent = if replicate {
                (length - 1 - start) / stride + 1
            } else {
                count
            };
            if replicate && extent % count != 0 {
                return Err(ImageToolError::new(
                    "Exception: region shape is not an integer multiple of the input array shape",
                ));
            }
            chunk.start.push(start);
            chunk.size.push(extent);
            chunk.stride.push(stride);
        }
        Ok(chunk)
    }

    fn mask<T: ImagePixel>(
        &self,
        image: &impl ImageInterface<T>,
    ) -> ImageToolResult<Option<ArrayD<bool>>> {
        Ok(image.get_mask_slice(&self.start, &self.size, &self.stride)?)
    }

    fn put<T: ImagePixel>(
        &self,
        storage: &mut Storage<T>,
        mut data: ArrayD<T>,
    ) -> ImageToolResult<()> {
        if let Storage::Temp { image, .. } = storage {
            image
                .require_writable_pixels()
                .map_err(casa_images::ImageError::from)?;
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
            with_storage!(storage, image => image.put_slice(&data, &self.start))?;
        } else {
            let mut pos = self.start.clone();
            for (index, &value) in data.indexed_iter() {
                for (axis, &i) in index.slice().iter().enumerate() {
                    pos[axis] = self.start[axis] + i * self.stride[axis];
                }
                with_storage!(storage, image => image.put_at(value, &pos))?;
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
    ) -> ImageToolResult<ChunkData> {
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
            || Ok(image.get_slice(&self.start, &self.size, &self.stride)?),
        )
    }

    fn format<T: ImagePixel, U: Average>(
        &self,
        axes: &[usize],
        dropdeg: bool,
        getmask: bool,
        wide: (impl Fn(T) -> U, impl Fn(U) -> U),
        mask: Option<ArrayD<bool>>,
        read: impl FnOnce() -> ImageToolResult<ArrayD<T>>,
    ) -> ImageToolResult<ChunkData> {
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
            return Err(ImageToolError::new("All selected pixels are masked."));
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
            return Ok(ChunkData::Bools {
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

    /// Reads this chunk from `image`, applying optional axis averaging and `expression` mask.
    pub fn read_from(
        &self,
        image: &ImageHandle,
        request: &ChunkRead<'_>,
        expression: Option<ArrayD<bool>>,
    ) -> ImageToolResult<ChunkData> {
        if let Some(fits) = image.fits() {
            let pixels = fits.read(&self.start, &self.size, &self.stride)?;
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
    fn output(shape: Vec<usize>, values: Vec<Self>) -> ChunkData;
}
impl Average for f64 {
    fn output(shape: Vec<usize>, values: Vec<Self>) -> ChunkData {
        ChunkData::Floats { shape, values }
    }
}
impl Average for Complex64 {
    fn output(shape: Vec<usize>, values: Vec<Self>) -> ChunkData {
        ChunkData::Complexes { shape, values }
    }
}

/// CASA `ia.getchunk` entrypoint supporting axis collapse and FITS backing.
#[allow(clippy::too_many_arguments)]
pub fn read_chunk_casa(
    attached: &AttachedImage,
    blc: &[i64],
    trc: &[i64],
    inc: &[i64],
    axes: &[i64],
    dropdeg: bool,
    getmask: bool,
) -> ImageToolResult<ChunkData> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    let shape = image.shape();
    if image.coordinates().n_pixel_axes() != shape.len() {
        return Err(ImageToolError::new(
            "LCSlicer::makeComplete - referencePixel and newLatticeShape vectors do not have same length",
        ));
    }
    let chunk = Chunk::read(shape, blc, trc, inc)?;
    let collapse = collapse_axes(axes, shape.len())?;
    let value = chunk.read_from(
        &image,
        &ChunkRead {
            axes: &collapse,
            dropdeg,
            getmask,
        },
        None,
    );
    let release = attached.release_requested_lock(&mut image);
    let value = value?;
    release?;
    Ok(value)
}

/// CASA `ia.putchunk` entrypoint supporting stride, replication, locking, and history.
#[allow(clippy::too_many_arguments)]
pub fn write_chunk_casa(
    attached: &AttachedImage,
    pixels: PixelChunk,
    blc: &[i64],
    inc: &[i64],
    locking: bool,
    replicate: bool,
    history_command: Option<String>,
) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    let chunk = Chunk::write(image.shape(), pixels.shape(), blc, inc, replicate)?;
    if matches!(&pixels, PixelChunk::Complex(_) | PixelChunk::DComplex(_))
        && matches!(&*image, ImageHandle::Float(_) | ImageHandle::Double(_))
    {
        return Err(ImageToolError::new(
            "Exception: Unsupported type for pixels parameter. It must be either a vector of doubles or ints",
        ));
    }
    if image.fits().is_some() {
        return Err(ImageToolError::new(
            "FITSImage::putSlice - is not possible as FITSImage is not writable",
        ));
    }
    attached.require_lock(&mut image, casa_tables::LockType::Write)?;
    match (&mut *image, pixels) {
        (ImageHandle::Float(storage), PixelChunk::Float(arr)) => chunk.put(storage, arr)?,
        (ImageHandle::Float(storage), PixelChunk::Double(arr)) => {
            chunk.put(storage, arr.mapv(|v| v as f32))?;
        }
        (ImageHandle::Double(storage), PixelChunk::Double(arr)) => chunk.put(storage, arr)?,
        (ImageHandle::Double(storage), PixelChunk::Float(arr)) => {
            chunk.put(storage, arr.mapv(f64::from))?;
        }
        (ImageHandle::Complex(storage), PixelChunk::Complex(arr)) => chunk.put(storage, arr)?,
        (ImageHandle::Complex(storage), PixelChunk::DComplex(arr)) => {
            chunk.put(
                storage,
                arr.mapv(|v| Complex32::new(v.re as f32, v.im as f32)),
            )?;
        }
        (ImageHandle::DComplex(storage), PixelChunk::DComplex(arr)) => chunk.put(storage, arr)?,
        (ImageHandle::DComplex(storage), PixelChunk::Complex(arr)) => {
            chunk.put(
                storage,
                arr.mapv(|v| Complex64::new(v.re.into(), v.im.into())),
            )?;
        }
        _ => {
            return Err(ImageToolError::new(
                "Exception: Unsupported type for pixels parameter.",
            ));
        }
    }
    if locking && cfg!(unix) {
        image.unlock_storage()?;
    } else {
        image.save()?;
        attached.release_requested_lock(&mut image)?;
    }
    if let Some(command) = history_command {
        image.record_history("putchunk", command)?;
    }
    Ok(true)
}
