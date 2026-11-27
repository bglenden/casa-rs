// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 pixel chunk wrappers delegating to [`casa_imageanalysis::tool::pixels`].

use casa_imageanalysis::tool::pixels::{ChunkData, PixelChunk, read_chunk_casa, write_chunk_casa};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::{Complex32, Complex64};
use numpy::{
    Element, PyArrayDescrMethods, PyArrayDyn, PyArrayMethods, PyUntypedArray,
    PyUntypedArrayMethods, npyffi::PY_ARRAY_API,
};
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::raised;
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

pub(super) fn chunk_data_to_casa_value(data: ChunkData) -> CasaValue {
    match data {
        ChunkData::Bools { shape, values } => CasaValue::Bools { shape, values },
        ChunkData::Floats { shape, values } => CasaValue::Floats { shape, values },
        ChunkData::Complexes { shape, values } => CasaValue::Complexes { shape, values },
    }
}

impl Pixels {
    fn to_pixel_chunk(&self, target_kind: casa_images::ImagePixelType) -> PixelChunk {
        match target_kind {
            casa_images::ImagePixelType::Float32 => PixelChunk::Float(self.f32()),
            casa_images::ImagePixelType::Float64 => PixelChunk::Double(self.f64()),
            casa_images::ImagePixelType::Complex32 => PixelChunk::Complex(self.c32()),
            casa_images::ImagePixelType::Complex64 => PixelChunk::DComplex(self.c64()),
        }
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
        let blc = blc.map_or_else(Vec::new, |arg| arg.0);
        let trc = trc.map_or_else(Vec::new, |arg| arg.0);
        let inc = inc.map_or_else(Vec::new, |arg| arg.0);
        let axes = axes.map_or_else(Vec::new, |arg| arg.0);
        let value = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::Bools {
                    shape: vec![0],
                    values: Vec::new(),
                });
            };
            read_chunk_casa(attachment, &blc, &trc, &inc, &axes, dropdeg, getmask)
                .map(chunk_data_to_casa_value)
                .map_err(raised)
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
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            let pixels = pixels?;
            let blc = blc.unwrap_or_else(|| IntList::of(&[-1])).0;
            let inc = inc.unwrap_or_else(|| IntList::of(&[1])).0;
            let kind = attachment.lock().kind();
            let history_command = history.then(|| {
                format!(
                    "ia.putchunk(pixels={}, blc={}, inc={}, list={list}, locking={locking}, replicate={replicate})",
                    pixels.history(),
                    format!("{blc:?}").replace(' ', ""),
                    format!("{inc:?}").replace(' ', "")
                )
            });
            let chunk = if pixels.is_complex() {
                match kind {
                    casa_images::ImagePixelType::Complex64 => PixelChunk::DComplex(pixels.c64()),
                    _ => PixelChunk::Complex(pixels.c32()),
                }
            } else {
                pixels.to_pixel_chunk(kind)
            };
            write_chunk_casa(
                attachment,
                chunk,
                &blc,
                &inc,
                locking,
                replicate,
                history_command,
            )
            .map_err(raised)
        })
    }
}
