// SPDX-License-Identifier: LGPL-3.0-or-later

//! Fallible model-value access at the reconstruction/runtime ownership seam.

use std::{fmt, ops::Range, sync::Arc};

use casa_imaging_model::ModelSample;

use crate::ModelLifecycleError;

/// Runtime-owned storage for canonical model samples.
///
/// The reconstruction owner retains this handle exclusively. Implementations
/// must preserve exact values and support bits, check every requested range,
/// and report I/O failures. Reads do not resize a cache beyond the physical
/// plan's admitted capacity. Mutable access is available only while the owner
/// is constructing a generation; external handles must not mutate its values.
#[doc(hidden)]
pub trait ModelSampleStorage: fmt::Debug + Send + Sync {
    /// Return the immutable logical number of samples.
    fn sample_count(&self) -> usize;

    /// Copy one canonical range into the caller's already allocated window.
    fn read(
        &self,
        start: usize,
        destination: &mut [ModelSample],
    ) -> Result<(), ModelLifecycleError>;

    /// Replace one canonical range without changing the logical shape.
    fn write(&mut self, start: usize, samples: &[ModelSample]) -> Result<(), ModelLifecycleError>;
}

/// Physical allocation capability supplied by execution composition.
#[doc(hidden)]
pub trait ModelStorageFactory: fmt::Debug + Send + Sync {
    /// Allocate an uninitialized logical model. The owner writes every sample
    /// before it can mint a generation from the returned storage.
    fn create(
        &self,
        sample_count: usize,
    ) -> Result<Box<dyn ModelSampleStorage>, ModelLifecycleError>;
}

/// Admitted storage and maximum resident window for model lifecycle work.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ModelStoragePlan {
    factory: Arc<dyn ModelStorageFactory>,
    window_samples: usize,
}

impl ModelStoragePlan {
    /// Bind a physical storage capability to a positive resident sample limit.
    pub fn new(
        factory: Arc<dyn ModelStorageFactory>,
        window_samples: usize,
    ) -> Result<Self, ModelLifecycleError> {
        if window_samples == 0 {
            return Err(ModelLifecycleError::Storage(
                "model storage window must contain at least one sample".into(),
            ));
        }
        Ok(Self {
            factory,
            window_samples,
        })
    }

    /// Select a fully resident allocation when the execution plan admits it.
    /// This uses the same windowed model algorithms as paged allocations.
    pub fn resident(window_samples: usize) -> Result<Self, ModelLifecycleError> {
        Self::new(Arc::new(ResidentModelStorage), window_samples)
    }

    pub(crate) fn create(&self, count: usize) -> Result<ModelSamples, ModelLifecycleError> {
        let storage = self.factory.create(count)?;
        if storage.sample_count() != count {
            return Err(ModelLifecycleError::SampleCountMismatch {
                expected: count,
                actual: storage.sample_count(),
            });
        }
        Ok(ModelSamples {
            storage,
            window_samples: self.window_samples.min(count),
        })
    }
}

#[derive(Debug)]
pub(crate) struct ModelSamples {
    storage: Box<dyn ModelSampleStorage>,
    window_samples: usize,
}

impl ModelSamples {
    pub(crate) fn iter(&self) -> ModelSampleReader<'_> {
        ModelSampleReader {
            owner: self,
            next: 0,
            window: Vec::new().into_iter(),
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.storage.sample_count()
    }

    pub(crate) fn window_samples(&self) -> usize {
        self.window_samples
    }

    pub(crate) fn read(
        &self,
        range: Range<usize>,
    ) -> Result<Box<[ModelSample]>, ModelLifecycleError> {
        if range.start > range.end || range.end > self.len() {
            return Err(ModelLifecycleError::CellOutsideShape);
        }
        if range.len() > self.window_samples {
            return Err(ModelLifecycleError::Storage(format!(
                "model read requests {} samples beyond admitted window {}",
                range.len(),
                self.window_samples
            )));
        }
        let mut result = vec![ModelSample::invalid(); range.len()].into_boxed_slice();
        self.storage.read(range.start, &mut result)?;
        Ok(result)
    }

    pub(crate) fn write(
        &mut self,
        start: usize,
        samples: &[ModelSample],
    ) -> Result<(), ModelLifecycleError> {
        if start
            .checked_add(samples.len())
            .is_none_or(|end| end > self.len())
        {
            return Err(ModelLifecycleError::CellOutsideShape);
        }
        if samples.len() > self.window_samples {
            return Err(ModelLifecycleError::Storage(format!(
                "model write requests {} samples beyond admitted window {}",
                samples.len(),
                self.window_samples
            )));
        }
        self.storage.write(start, samples)
    }

    pub(crate) fn for_each_window(
        &self,
        mut visit: impl FnMut(usize, &[ModelSample]) -> Result<(), ModelLifecycleError>,
    ) -> Result<(), ModelLifecycleError> {
        for start in (0..self.len()).step_by(self.window_samples) {
            let samples =
                self.read(start..start.saturating_add(self.window_samples).min(self.len()))?;
            visit(start, &samples)?;
        }
        Ok(())
    }
}

pub(crate) struct ModelSampleReader<'a> {
    owner: &'a ModelSamples,
    next: usize,
    window: std::vec::IntoIter<ModelSample>,
}

impl Iterator for ModelSampleReader<'_> {
    type Item = Result<ModelSample, ModelLifecycleError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(sample) = self.window.next() {
            return Some(Ok(sample));
        }
        if self.next == self.owner.len() {
            return None;
        }
        let start = self.next;
        self.next = start
            .saturating_add(self.owner.window_samples())
            .min(self.owner.len());
        match self.owner.read(start..self.next) {
            Ok(window) => {
                self.window = window.into_vec().into_iter();
                self.window.next().map(Ok)
            }
            Err(error) => {
                self.next = self.owner.len();
                Some(Err(error))
            }
        }
    }
}

#[derive(Debug)]
struct ResidentModelStorage;

impl ModelStorageFactory for ResidentModelStorage {
    fn create(
        &self,
        sample_count: usize,
    ) -> Result<Box<dyn ModelSampleStorage>, ModelLifecycleError> {
        Ok(Box::new(
            vec![ModelSample::invalid(); sample_count].into_boxed_slice(),
        ))
    }
}

impl ModelSampleStorage for Box<[ModelSample]> {
    fn sample_count(&self) -> usize {
        self.len()
    }

    fn read(
        &self,
        start: usize,
        destination: &mut [ModelSample],
    ) -> Result<(), ModelLifecycleError> {
        let end = start
            .checked_add(destination.len())
            .ok_or(ModelLifecycleError::CellOutsideShape)?;
        let source = self
            .get(start..end)
            .ok_or(ModelLifecycleError::CellOutsideShape)?;
        destination.copy_from_slice(source);
        Ok(())
    }

    fn write(&mut self, start: usize, samples: &[ModelSample]) -> Result<(), ModelLifecycleError> {
        let end = start
            .checked_add(samples.len())
            .ok_or(ModelLifecycleError::CellOutsideShape)?;
        let destination = self
            .get_mut(start..end)
            .ok_or(ModelLifecycleError::CellOutsideShape)?;
        destination.copy_from_slice(samples);
        Ok(())
    }
}
