// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compact channel-local model values and support in the run's cube residency.

use std::{fmt, io, path::Path, sync::Arc};

use casa_imaging_model::{ModelSample, ModelSupport, ModelValue};
use casa_imaging_reconstruction::{ModelLifecycleError, ModelSampleStorage, ModelStorageFactory};

use crate::managed_cube_blocks::{CubeResidency, ManagedPlaneArray};

pub(crate) struct ManagedModelFactory {
    residency: Arc<CubeResidency>,
    retention: Arc<dyn fmt::Debug + Send + Sync>,
    parent: Box<Path>,
    axes: [usize; 2],
    planes: usize,
}

impl fmt::Debug for ManagedModelFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedModelFactory")
            .field("axes", &self.axes)
            .field("planes", &self.planes)
            .finish_non_exhaustive()
    }
}

impl ManagedModelFactory {
    pub(crate) fn new(
        residency: Arc<CubeResidency>,
        retention: Arc<dyn fmt::Debug + Send + Sync>,
        parent: &Path,
        axes: [usize; 2],
        planes: usize,
    ) -> io::Result<Self> {
        if axes.contains(&0) || planes == 0 {
            return Err(io::Error::other("managed model shape must be positive"));
        }
        axes[0]
            .checked_mul(axes[1])
            .and_then(|cells| cells.checked_mul(planes))
            .ok_or_else(|| io::Error::other("managed model shape overflow"))?;
        Ok(Self {
            residency,
            retention,
            parent: parent.into(),
            axes,
            planes,
        })
    }

    fn cells(&self) -> usize {
        self.axes[0] * self.axes[1]
    }
}

impl ModelStorageFactory for ManagedModelFactory {
    fn create(
        &self,
        sample_count: usize,
    ) -> Result<Box<dyn ModelSampleStorage>, ModelLifecycleError> {
        let expected = self.cells() * self.planes;
        if sample_count != expected {
            return Err(ModelLifecycleError::SampleCountMismatch {
                expected,
                actual: sample_count,
            });
        }
        let values = ManagedPlaneArray::create(
            self.residency.clone(),
            &self.parent,
            self.axes[0],
            self.axes[1],
            self.planes,
            Some(0.0_f32),
        )
        .map_err(storage_error)?;
        let support = ManagedPlaneArray::create(
            self.residency.clone(),
            &self.parent,
            self.axes[0],
            self.axes[1],
            self.planes,
            Some(false),
        )
        .map_err(storage_error)?;
        Ok(Box::new(ManagedModel {
            residency: self.residency.clone(),
            _retention: self.retention.clone(),
            values,
            support,
            cells: self.cells(),
            samples: sample_count,
        }))
    }
}

struct ManagedModel {
    residency: Arc<CubeResidency>,
    _retention: Arc<dyn fmt::Debug + Send + Sync>,
    values: ManagedPlaneArray<f32>,
    support: ManagedPlaneArray<bool>,
    cells: usize,
    samples: usize,
}

impl fmt::Debug for ManagedModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedModel")
            .field("cells", &self.cells)
            .field("samples", &self.samples)
            .finish_non_exhaustive()
    }
}

impl ManagedModel {
    fn for_each_window(
        &self,
        start: usize,
        len: usize,
        mut visit: impl FnMut(
            usize,
            std::ops::Range<usize>,
            std::ops::Range<usize>,
        ) -> Result<(), ModelLifecycleError>,
    ) -> Result<(), ModelLifecycleError> {
        if start
            .checked_add(len)
            .is_none_or(|end| end > self.samples || len > self.cells)
        {
            return Err(ModelLifecycleError::CellOutsideShape);
        }
        let mut processed = 0;
        while processed < len {
            let absolute = start + processed;
            let plane = absolute / self.cells;
            let from = absolute % self.cells;
            let count = (self.cells - from).min(len - processed);
            visit(plane, from..from + count, processed..processed + count)?;
            processed += count;
        }
        Ok(())
    }
}

impl ModelSampleStorage for ManagedModel {
    fn sample_count(&self) -> usize {
        self.samples
    }

    fn read(
        &self,
        start: usize,
        destination: &mut [ModelSample],
    ) -> Result<(), ModelLifecycleError> {
        self.for_each_window(start, destination.len(), |plane, in_plane, output| {
            let pins = self
                .residency
                .admit(
                    &[
                        self.values.request(plane, false).map_err(storage_error)?,
                        self.support.request(plane, false).map_err(storage_error)?,
                    ],
                    0,
                )
                .map_err(storage_error)?;
            let values = self
                .values
                .read(&pins, plane, in_plane.clone())
                .map_err(storage_error)?;
            let support = self
                .support
                .read(&pins, plane, in_plane)
                .map_err(storage_error)?;
            for ((destination, &value), &supported) in destination[output]
                .iter_mut()
                .zip(values.iter())
                .zip(support.iter())
            {
                *destination = if supported {
                    ModelSample::valid(ModelValue::new(f64::from(value))?)
                } else if value == 0.0 {
                    ModelSample::invalid()
                } else {
                    return Err(ModelLifecycleError::InvalidSupportPayload);
                };
            }
            Ok(())
        })
    }

    fn write(&mut self, start: usize, samples: &[ModelSample]) -> Result<(), ModelLifecycleError> {
        self.for_each_window(start, samples.len(), |plane, in_plane, input| {
            let pins = self
                .residency
                .admit(
                    &[
                        self.values.request(plane, true).map_err(storage_error)?,
                        self.support.request(plane, true).map_err(storage_error)?,
                    ],
                    0,
                )
                .map_err(storage_error)?;
            let mut values = self
                .values
                .write(&pins, plane, in_plane.clone())
                .map_err(storage_error)?;
            let mut support = self
                .support
                .write(&pins, plane, in_plane)
                .map_err(storage_error)?;
            for ((value, supported), sample) in values
                .iter_mut()
                .zip(support.iter_mut())
                .zip(&samples[input])
            {
                *supported = sample.support() == ModelSupport::Valid;
                *value = if *supported {
                    let value = sample.value().value() as f32;
                    if !value.is_finite() {
                        return Err(storage_error("model value is not finite in Float storage"));
                    }
                    value
                } else {
                    0.0
                };
            }
            values.finish();
            support.finish();
            Ok(())
        })
    }

    fn retire(self: Box<Self>) -> Result<(), ModelLifecycleError> {
        let Self {
            residency: _,
            _retention: _,
            values,
            support,
            cells: _,
            samples: _,
        } = *self;
        values.retire_dead().map_err(storage_error)?;
        support.retire_dead().map_err(storage_error)
    }
}

fn storage_error(error: impl std::fmt::Display) -> ModelLifecycleError {
    ModelLifecycleError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_values_and_support_cross_planes_without_intermediate_windows() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(1 << 20).unwrap();
        let factory =
            ManagedModelFactory::new(manager.clone(), Arc::new(()), directory.path(), [2, 3], 3)
                .unwrap();
        let mut model = factory.create(18).unwrap();
        let samples = [
            ModelSample::valid(ModelValue::new(1.25).unwrap()),
            ModelSample::invalid(),
            ModelSample::valid(ModelValue::new(-0.125).unwrap()),
            ModelSample::valid(ModelValue::new(0.5).unwrap()),
        ];
        model.write(4, &samples).unwrap();
        let mut found = [ModelSample::invalid(); 4];
        model.read(4, &mut found).unwrap();
        assert_eq!(found, samples);
        assert_eq!(
            model.read(18, &mut [ModelSample::invalid()]),
            Err(ModelLifecycleError::CellOutsideShape)
        );
        let live = manager.used_bytes();
        model.retire().unwrap();
        assert!(manager.used_bytes() < live);
    }
}
