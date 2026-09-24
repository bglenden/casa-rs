// SPDX-License-Identifier: LGPL-3.0-or-later

//! Real Float normal planes behind the reconstruction owner's scalar interface.

use std::{borrow::Cow, fmt, io, path::Path, sync::Arc};

use casa_imaging_reconstruction::SpectralOperatorError;
use casa_imaging_reconstruction::runtime_adapter::{NormalArrayStorage, NormalStorageFactory};
use num_complex::Complex64;

use crate::managed_cube_blocks::{CubeResidency, ManagedPlaneArray};

pub(crate) struct ManagedNormalFactory {
    residency: Arc<CubeResidency>,
    retention: Arc<dyn fmt::Debug + Send + Sync>,
    parent: Box<Path>,
    axes: [usize; 2],
}

impl fmt::Debug for ManagedNormalFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedNormalFactory")
            .field("axes", &self.axes)
            .finish_non_exhaustive()
    }
}

impl ManagedNormalFactory {
    pub(crate) fn new(
        residency: Arc<CubeResidency>,
        retention: Arc<dyn fmt::Debug + Send + Sync>,
        parent: &Path,
        axes: [usize; 2],
    ) -> io::Result<Self> {
        if axes.contains(&0) || axes[0].checked_mul(axes[1]).is_none() {
            return Err(io::Error::other("managed normal shape is invalid"));
        }
        Ok(Self {
            residency,
            retention,
            parent: parent.into(),
            axes,
        })
    }
}

impl NormalStorageFactory for ManagedNormalFactory {
    fn scalar_sensitivity(&self) -> bool {
        true
    }

    fn create(
        &self,
        _domain: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, SpectralOperatorError> {
        let cells = self.axes[0] * self.axes[1];
        let complex = scalars / 2;
        if scalars == 0 || scalars % 2 != 0 || complex % cells != 0 {
            return Err(storage_error(
                "normal scalar extent is not a whole image plane",
            ));
        }
        let array = ManagedPlaneArray::create(
            self.residency.clone(),
            &self.parent,
            self.axes[0],
            self.axes[1],
            complex / cells,
            None::<f32>,
        )
        .map_err(storage_error)?;
        Ok(Box::new(ManagedNormal {
            residency: self.residency.clone(),
            _retention: self.retention.clone(),
            array,
            cells,
            scalars,
        }))
    }
}

struct ManagedNormal {
    residency: Arc<CubeResidency>,
    _retention: Arc<dyn fmt::Debug + Send + Sync>,
    array: ManagedPlaneArray<f32>,
    cells: usize,
    scalars: usize,
}

impl fmt::Debug for ManagedNormal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedNormal")
            .field("cells", &self.cells)
            .field("scalars", &self.scalars)
            .finish_non_exhaustive()
    }
}

impl ManagedNormal {
    fn for_each_window(
        &self,
        start: usize,
        values: usize,
        mut visit: impl FnMut(
            usize,
            std::ops::Range<usize>,
            std::ops::Range<usize>,
        ) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        if start % 2 != 0
            || start
                .checked_add(values.saturating_mul(2))
                .is_none_or(|end| end > self.scalars)
        {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let mut processed = 0;
        while processed < values {
            let absolute = start / 2 + processed;
            let plane = absolute / self.cells;
            let from = absolute % self.cells;
            let count = (self.cells - from).min(values - processed);
            visit(plane, from..from + count, processed..processed + count)?;
            processed += count;
        }
        Ok(())
    }
}

impl NormalArrayStorage for ManagedNormal {
    fn len(&self) -> usize {
        self.scalars
    }

    fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, SpectralOperatorError> {
        if len % 2 != 0 {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let mut result = vec![0.0; len];
        self.for_each_window(start, len / 2, |plane, in_plane, output| {
            let pins = self
                .residency
                .admit(
                    &[self.array.request(plane, false).map_err(storage_error)?],
                    0,
                )
                .map_err(storage_error)?;
            let values = self
                .array
                .read(&pins, plane, in_plane)
                .map_err(storage_error)?;
            for (&value, target) in values
                .iter()
                .zip(result[output.start * 2..output.end * 2].chunks_exact_mut(2))
            {
                target[0] = f64::from(value);
            }
            Ok(())
        })?;
        Ok(Cow::Owned(result))
    }

    fn read_complex(
        &self,
        start: usize,
        values: usize,
    ) -> Result<Cow<'_, [Complex64]>, SpectralOperatorError> {
        let mut result = vec![Complex64::default(); values];
        self.for_each_window(start, values, |plane, in_plane, output| {
            let pins = self
                .residency
                .admit(
                    &[self.array.request(plane, false).map_err(storage_error)?],
                    0,
                )
                .map_err(storage_error)?;
            let source = self
                .array
                .read(&pins, plane, in_plane)
                .map_err(storage_error)?;
            for (target, &value) in result[output].iter_mut().zip(source.iter()) {
                target.re = f64::from(value);
            }
            Ok(())
        })?;
        Ok(Cow::Owned(result))
    }

    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError> {
        if values.len() % 2 != 0 {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        self.for_each_window(start, values.len() / 2, |plane, in_plane, input| {
            let pins = self
                .residency
                .admit(
                    &[self.array.request(plane, true).map_err(storage_error)?],
                    0,
                )
                .map_err(storage_error)?;
            let mut destination = self
                .array
                .write(&pins, plane, in_plane)
                .map_err(storage_error)?;
            for (target, pair) in destination
                .iter_mut()
                .zip(values[input.start * 2..input.end * 2].chunks_exact(2))
            {
                let value = pair[0] as f32;
                if !value.is_finite() {
                    return Err(storage_error("normal value is not finite in Float storage"));
                }
                *target = value;
            }
            destination.finish();
            Ok(())
        })
    }

    fn retire(self: Box<Self>) -> Result<(), SpectralOperatorError> {
        self.array.retire_dead().map_err(storage_error)
    }
}

fn storage_error(error: impl std::fmt::Display) -> SpectralOperatorError {
    SpectralOperatorError::NormalStorage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_planes_round_once_and_release_at_retirement() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(1 << 20).unwrap();
        let factory =
            ManagedNormalFactory::new(manager.clone(), Arc::new(()), directory.path(), [2, 3])
                .unwrap();
        let mut normal = factory.create(0, 24).unwrap();
        normal
            .write(10, &[1.25, 7.0, -0.125, -4.0, 0.5, 1.0])
            .unwrap();
        let found = normal.read_complex(10, 3).unwrap();
        assert_eq!(
            &*found,
            &[
                Complex64::new(1.25, 0.0),
                Complex64::new(-0.125, 0.0),
                Complex64::new(0.5, 0.0)
            ]
        );
        let live = manager.used_bytes();
        normal.retire().unwrap();
        assert!(manager.used_bytes() < live);
    }
}
