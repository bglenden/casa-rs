// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compact channel-local model values and support in the run's cube residency.

use std::{fmt, io, path::Path, sync::Arc};

use casa_imaging_model::{ModelSample, ModelSupport, ModelValue, NumericPrecision};
use casa_imaging_reconstruction::{
    ModelLifecycleError, ModelSampleStorage, ModelSampleUpdate, ModelStorageFactory,
};

use crate::managed_cube_blocks::{CubeResidency, ManagedPlaneArray};

/// Paged model storage for every image domain of a run: each domain's
/// `planes` planes of its own `[height, width]`, in the model's
/// domain-major sample order.
pub(crate) struct ManagedModelFactory {
    residency: Arc<CubeResidency>,
    retention: Arc<dyn fmt::Debug + Send + Sync>,
    parent: Box<Path>,
    domains: Vec<[usize; 2]>,
    planes: usize,
}

impl fmt::Debug for ManagedModelFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedModelFactory")
            .field("domains", &self.domains)
            .field("planes", &self.planes)
            .finish_non_exhaustive()
    }
}

impl ManagedModelFactory {
    /// Storage for `planes` planes of each `[height, width]` in `domains`.
    pub(crate) fn new(
        residency: Arc<CubeResidency>,
        retention: Arc<dyn fmt::Debug + Send + Sync>,
        parent: &Path,
        domains: &[[usize; 2]],
        planes: usize,
    ) -> io::Result<Self> {
        if domains.is_empty() || domains.iter().any(|axes| axes.contains(&0)) || planes == 0 {
            return Err(io::Error::other("managed model shape must be positive"));
        }
        domains
            .iter()
            .try_fold(0_usize, |total, axes| {
                axes[0]
                    .checked_mul(axes[1])?
                    .checked_mul(planes)
                    .and_then(|samples| total.checked_add(samples))
            })
            .ok_or_else(|| io::Error::other("managed model shape overflow"))?;
        Ok(Self {
            residency,
            retention,
            parent: parent.into(),
            domains: domains.to_vec(),
            planes,
        })
    }

    fn samples(&self) -> usize {
        self.domains
            .iter()
            .map(|axes| axes[0] * axes[1] * self.planes)
            .sum()
    }
}

impl ModelStorageFactory for ManagedModelFactory {
    fn create(
        &self,
        sample_count: usize,
    ) -> Result<Box<dyn ModelSampleStorage>, ModelLifecycleError> {
        let expected = self.samples();
        if sample_count != expected {
            return Err(ModelLifecycleError::SampleCountMismatch {
                expected,
                actual: sample_count,
            });
        }
        let mut segments = Vec::with_capacity(self.domains.len());
        let mut start = 0;
        for axes in &self.domains {
            let cells = axes[0] * axes[1];
            segments.push(Segment {
                values: ManagedPlaneArray::create(
                    self.residency.clone(),
                    &self.parent,
                    axes[0],
                    axes[1],
                    self.planes,
                    Some(0.0_f32),
                )
                .map_err(storage_error)?,
                support: ManagedPlaneArray::create(
                    self.residency.clone(),
                    &self.parent,
                    axes[0],
                    axes[1],
                    self.planes,
                    Some(false),
                )
                .map_err(storage_error)?,
                start,
                cells,
            });
            start += cells * self.planes;
        }
        Ok(Box::new(ManagedModel {
            segments,
            residency: self.residency.clone(),
            _retention: self.retention.clone(),
            samples: sample_count,
        }))
    }
}

/// One domain's planes: values, support and where they start in the model's
/// sample order.
struct Segment {
    values: ManagedPlaneArray<f32>,
    support: ManagedPlaneArray<bool>,
    start: usize,
    cells: usize,
}

struct ManagedModel {
    segments: Vec<Segment>,
    residency: Arc<CubeResidency>,
    _retention: Arc<dyn fmt::Debug + Send + Sync>,
    samples: usize,
}

impl fmt::Debug for ManagedModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedModel")
            .field("domains", &self.segments.len())
            .field("samples", &self.samples)
            .finish_non_exhaustive()
    }
}

impl ManagedModel {
    /// The domain segment, plane and offset in the plane of sample `index`.
    fn locate(&self, index: usize) -> (&Segment, usize, usize) {
        let segment = self
            .segments
            .iter()
            .rev()
            .find(|segment| segment.start <= index)
            .expect("the first segment starts at sample 0");
        let offset = index - segment.start;
        (segment, offset / segment.cells, offset % segment.cells)
    }

    /// Visit `[start, start + len)` one plane window at a time: the window's
    /// segment, plane, range in the plane and range in the caller's slice.
    fn for_each_window(
        &self,
        start: usize,
        len: usize,
        mut visit: impl FnMut(
            &Segment,
            usize,
            std::ops::Range<usize>,
            std::ops::Range<usize>,
        ) -> Result<(), ModelLifecycleError>,
    ) -> Result<(), ModelLifecycleError> {
        if start.checked_add(len).is_none_or(|end| end > self.samples) {
            return Err(ModelLifecycleError::CellOutsideShape);
        }
        let mut processed = 0;
        while processed < len {
            let (segment, plane, from) = self.locate(start + processed);
            let count = (segment.cells - from).min(len - processed);
            visit(
                segment,
                plane,
                from..from + count,
                processed..processed + count,
            )?;
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
        self.for_each_window(
            start,
            destination.len(),
            |segment, plane, in_plane, output| {
                let pins = self
                    .residency
                    .admit(
                        &[
                            segment
                                .values
                                .request(plane, false)
                                .map_err(storage_error)?,
                            segment
                                .support
                                .request(plane, false)
                                .map_err(storage_error)?,
                        ],
                        0,
                    )
                    .map_err(storage_error)?;
                let values = segment
                    .values
                    .read(&pins, plane, in_plane.clone())
                    .map_err(storage_error)?;
                let support = segment
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
            },
        )
    }

    fn write(&mut self, start: usize, samples: &[ModelSample]) -> Result<(), ModelLifecycleError> {
        self.for_each_window(start, samples.len(), |segment, plane, in_plane, input| {
            let pins = self
                .residency
                .admit(
                    &[
                        segment.values.request(plane, true).map_err(storage_error)?,
                        segment
                            .support
                            .request(plane, true)
                            .map_err(storage_error)?,
                    ],
                    0,
                )
                .map_err(storage_error)?;
            let mut values = segment
                .values
                .write(&pins, plane, in_plane.clone())
                .map_err(storage_error)?;
            let mut support = segment
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

    fn apply_updates(
        &self,
        updates: &[ModelSampleUpdate],
        precision: NumericPrecision,
        bound: f64,
    ) -> Result<f64, ModelLifecycleError> {
        if updates
            .last()
            .is_some_and(|update| update.index() >= self.samples)
        {
            return Err(ModelLifecycleError::CellOutsideShape);
        }
        let mut remaining = updates;
        let mut maximum: f64 = 0.0;
        while let Some(first) = remaining.first() {
            let (segment, plane, first_offset) = self.locate(first.index());
            let plane_start = segment.start + plane * segment.cells;
            let plane_end = plane_start + segment.cells;
            let count = remaining
                .partition_point(|update| (plane_start..plane_end).contains(&update.index()));
            let range = first_offset..remaining[count - 1].index() - plane_start + 1;
            let pins = self
                .residency
                .admit(
                    &[
                        segment.values.request(plane, true).map_err(storage_error)?,
                        segment
                            .support
                            .request(plane, false)
                            .map_err(storage_error)?,
                    ],
                    0,
                )
                .map_err(storage_error)?;
            let support = segment
                .support
                .read(&pins, plane, range.clone())
                .map_err(storage_error)?;
            let mut values = segment
                .values
                .write(&pins, plane, range.clone())
                .map_err(storage_error)?;
            for update in &remaining[..count] {
                let offset = update.index() - plane_start - range.start;
                let sample = if support[offset] {
                    ModelSample::valid(ModelValue::new(f64::from(values[offset]))?)
                } else {
                    ModelSample::invalid()
                };
                let updated = update.apply(sample, precision, bound)?.value().value() as f32;
                if !updated.is_finite() {
                    return Err(storage_error("model value is not finite in Float storage"));
                }
                values[offset] = updated;
                maximum = maximum.max(f64::from(updated).abs());
            }
            values.finish();
            remaining = &remaining[count..];
        }
        Ok(maximum)
    }
}

fn storage_error(error: impl std::fmt::Display) -> ModelLifecycleError {
    ModelLifecycleError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Weak,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Debug)]
    struct PermitReleaseCheck {
        residency: Weak<CubeResidency>,
        released_after_payload: Arc<AtomicBool>,
    }

    impl Drop for PermitReleaseCheck {
        fn drop(&mut self) {
            self.released_after_payload
                .store(self.residency.upgrade().is_none(), Ordering::SeqCst);
        }
    }

    struct RetainedRun {
        _residency: Arc<CubeResidency>,
        _check: PermitReleaseCheck,
    }

    impl fmt::Debug for RetainedRun {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("RetainedRun")
        }
    }

    #[test]
    fn final_model_owner_drops_payload_before_capacity_on_success_and_error() {
        for error_unwind in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let manager = CubeResidency::new(1 << 20).unwrap();
            let released_after_payload = Arc::new(AtomicBool::new(false));
            let retention: Arc<dyn fmt::Debug + Send + Sync> = Arc::new(RetainedRun {
                _residency: manager.clone(),
                _check: PermitReleaseCheck {
                    residency: Arc::downgrade(&manager),
                    released_after_payload: released_after_payload.clone(),
                },
            });
            let factory = ManagedModelFactory::new(
                manager.clone(),
                retention.clone(),
                directory.path(),
                &[[2, 3]],
                1,
            )
            .unwrap();
            let mut model = factory.create(6).unwrap();
            model.write(0, &[ModelSample::invalid()]).unwrap();
            drop(manager);
            drop(factory);
            drop(retention);
            if error_unwind {
                let fail = || -> Result<(), ModelLifecycleError> {
                    let _owned = model;
                    Err(storage_error("injected caller failure"))
                };
                assert!(fail().is_err());
            } else {
                drop(model);
            }
            assert!(released_after_payload.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn model_values_and_support_cross_planes_without_intermediate_windows() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(1 << 20).unwrap();
        let factory = ManagedModelFactory::new(
            manager.clone(),
            Arc::new(()),
            directory.path(),
            &[[2, 3]],
            3,
        )
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
    }

    /// Domains of different shapes each keep their own planes; reads and
    /// writes cross from one domain's last plane into the next domain's
    /// first.
    #[test]
    fn domains_of_different_shapes_share_one_sample_order() {
        let directory = tempfile::tempdir().unwrap();
        let manager = CubeResidency::new(1 << 20).unwrap();
        // 2 × 3 then 4 × 5 pixels, two planes each: 12 + 40 samples.
        let factory = ManagedModelFactory::new(
            manager.clone(),
            Arc::new(()),
            directory.path(),
            &[[2, 3], [4, 5]],
            2,
        )
        .unwrap();
        assert!(matches!(
            factory.create(2 * 6 * 2),
            Err(ModelLifecycleError::SampleCountMismatch { expected: 52, .. })
        ));
        let mut model = factory.create(52).unwrap();
        let samples = (0..30)
            .map(|index| ModelSample::valid(ModelValue::new(f64::from(index) + 0.5).unwrap()))
            .collect::<Vec<_>>();
        model.write(8, &samples).unwrap();
        let mut found = vec![ModelSample::invalid(); 30];
        model.read(8, &mut found).unwrap();
        assert_eq!(found, samples);
        assert_eq!(
            model.read(51, &mut [ModelSample::invalid(); 2]),
            Err(ModelLifecycleError::CellOutsideShape)
        );
    }
}
