// SPDX-License-Identifier: LGPL-3.0-or-later

//! Fallible model-value access at the reconstruction/runtime ownership seam.

use std::{
    fmt,
    ops::Range,
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use casa_imaging_model::{ModelSample, ModelSupport, ModelValue, NumericPrecision};

use crate::ModelLifecycleError;

/// Runtime-owned storage for canonical model samples.
///
/// The reconstruction owner retains this handle exclusively. Implementations
/// must preserve exact values and support bits, check every requested range,
/// and report I/O failures. Reads do not resize a cache beyond the physical
/// plan's admitted capacity. Only owner-queued increments may mutate a logically
/// immutable generation on first access. External handles must not mutate it.
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

    /// Apply sorted sparse increments within one admitted window, returning the
    /// largest updated magnitude. The owner serializes updates to overlapping
    /// windows; disjoint windows may execute concurrently. Implementations must
    /// batch backing access, preserve support, and propagate partial-write errors.
    fn apply_updates(
        &self,
        updates: &[ModelSampleUpdate],
        precision: NumericPrecision,
        bound: f64,
    ) -> Result<f64, ModelLifecycleError>;
}

/// A canonical sparse increment evaluated at its owning model window.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ModelSampleUpdate {
    pub(crate) index: usize,
    pub(crate) increment: f64,
}

impl ModelSampleUpdate {
    /// Absolute canonical sample index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Apply the compiled arithmetic and scientific checks to one changed cell.
    pub fn apply(
        &self,
        sample: ModelSample,
        precision: NumericPrecision,
        bound: f64,
    ) -> Result<ModelSample, ModelLifecycleError> {
        if sample.support() != ModelSupport::Valid {
            return Err(ModelLifecycleError::DeltaOutsideValidSupport);
        }
        let value = ModelValue::new(add_with_precision(
            precision,
            sample.value().value(),
            self.increment,
        ))?;
        crate::validate_model_value(value, bound)?;
        Ok(ModelSample::valid(value))
    }
}

/// `left + right`, rounded to the lifecycle's arithmetic precision.
fn add_with_precision(precision: NumericPrecision, left: f64, right: f64) -> f64 {
    match precision {
        NumericPrecision::F32 => f64::from((left as f32) + (right as f32)),
        NumericPrecision::F64 => left + right,
    }
}

#[derive(Debug)]
struct PendingWindow {
    range: Range<usize>,
    updates: Box<[ModelSampleUpdate]>,
    precision: NumericPrecision,
    bound: f64,
    applied: OnceLock<Result<(), ModelLifecycleError>>,
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
    /// Maximum queued sparse payload for a phase, excluding the input and
    /// compiled delta which coexist during transfer. Each changed window has
    /// one completion cell; arrays are allocated at their exact logical sizes.
    pub fn pending_update_bytes(
        terms: usize,
        samples: usize,
        window_samples: usize,
    ) -> Option<usize> {
        if window_samples == 0 {
            return None;
        }
        let windows = terms.min(samples.div_ceil(window_samples));
        terms
            .checked_mul(size_of::<ModelSampleUpdate>())?
            .checked_add(windows.checked_mul(size_of::<PendingWindow>())?)
    }
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
            maximum_magnitude: AtomicU64::new(0),
            pending: Vec::new(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct ModelSamples {
    storage: Box<dyn ModelSampleStorage>,
    window_samples: usize,
    maximum_magnitude: AtomicU64,
    pending: Vec<PendingWindow>,
}

impl ModelSamples {
    pub(crate) fn maximum_magnitude(&self) -> f64 {
        f64::from_bits(self.maximum_magnitude.load(Ordering::Relaxed))
    }

    pub(crate) fn record_validated_bound(&mut self, bound: f64) {
        self.maximum_magnitude
            .fetch_min(bound.to_bits(), Ordering::Relaxed);
    }

    pub(crate) fn queue_updates<I>(
        &mut self,
        updates: I,
        precision: NumericPrecision,
        bound: f64,
    ) -> Result<(), ModelLifecycleError>
    where
        I: IntoIterator<Item = ModelSampleUpdate>,
        I::IntoIter: Clone,
    {
        self.finish_updates()?;
        let updates = updates.into_iter();
        let mut previous = None;
        let windows = updates
            .clone()
            .filter(|update| {
                let window = update.index / self.window_samples;
                let distinct = previous != Some(window);
                previous = Some(window);
                distinct
            })
            .count();
        self.pending = Vec::with_capacity(windows);
        let mut updates = updates.peekable();
        while let Some(first) = updates.next() {
            let start = first.index / self.window_samples * self.window_samples;
            let end = start.saturating_add(self.window_samples).min(self.len());
            let count = 1 + updates.clone().take_while(|next| next.index < end).count();
            let mut window = Vec::with_capacity(count);
            window.push(first);
            while updates.peek().is_some_and(|next| next.index < end) {
                window.push(updates.next().expect("peeked update"));
            }
            self.pending.push(PendingWindow {
                range: start..end,
                updates: window.into_boxed_slice(),
                precision,
                bound,
                applied: OnceLock::new(),
            });
        }
        Ok(())
    }

    fn apply_window(&self, window: &PendingWindow) -> Result<(), ModelLifecycleError> {
        window
            .applied
            .get_or_init(|| {
                let maximum =
                    self.storage
                        .apply_updates(&window.updates, window.precision, window.bound)?;
                self.maximum_magnitude
                    .fetch_max(maximum.to_bits(), Ordering::Relaxed);
                Ok(())
            })
            .clone()
    }

    /// Complete only still-pending sparse writes before transferring the model.
    /// Already-applied windows require no backing access or content verification.
    pub(crate) fn finish_updates(&self) -> Result<(), ModelLifecycleError> {
        for window in &self.pending {
            self.apply_window(window)?;
        }
        Ok(())
    }

    pub(crate) fn complete_updates(&mut self) -> Result<(), ModelLifecycleError> {
        self.finish_updates()?;
        self.pending = Vec::new();
        Ok(())
    }

    fn prepare_range(&self, range: Range<usize>) -> Result<(), ModelLifecycleError> {
        if range.is_empty() {
            return Ok(());
        }
        let first = self
            .pending
            .partition_point(|window| window.range.end <= range.start);
        for window in self.pending[first..]
            .iter()
            .take_while(|window| window.range.start < range.end)
        {
            self.apply_window(window)?;
        }
        Ok(())
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
        self.prepare_range(range.clone())?;
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
        self.prepare_range(start..start + samples.len())?;
        self.storage.write(start, samples)?;
        let mut maximum = self.maximum_magnitude();
        for sample in samples {
            maximum = maximum.max(sample.value().value().abs());
        }
        self.maximum_magnitude
            .store(maximum.to_bits(), Ordering::Relaxed);
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, atomic::AtomicUsize, mpsc};
    use std::time::Duration;

    #[derive(Debug)]
    struct ProbeStorage {
        samples: RwLock<Box<[ModelSample]>>,
        calls: Arc<AtomicUsize>,
        rendezvous: Option<(mpsc::Sender<usize>, Mutex<mpsc::Receiver<()>>)>,
        fail_after_write: bool,
    }

    impl ModelSampleStorage for ProbeStorage {
        fn sample_count(&self) -> usize {
            8
        }
        fn read(
            &self,
            start: usize,
            destination: &mut [ModelSample],
        ) -> Result<(), ModelLifecycleError> {
            ModelSampleStorage::read(&self.samples, start, destination)
        }
        fn write(
            &mut self,
            start: usize,
            values: &[ModelSample],
        ) -> Result<(), ModelLifecycleError> {
            ModelSampleStorage::write(&mut self.samples, start, values)
        }
        fn apply_updates(
            &self,
            updates: &[ModelSampleUpdate],
            precision: NumericPrecision,
            bound: f64,
        ) -> Result<f64, ModelLifecycleError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if let Some((entered, resume)) = &self.rendezvous {
                entered.send(updates[0].index()).unwrap();
                resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(|e| ModelLifecycleError::Storage(e.to_string()))?;
            }
            let maximum = self.samples.apply_updates(updates, precision, bound)?;
            if self.fail_after_write {
                return Err(ModelLifecycleError::Storage("partial write".into()));
            }
            Ok(maximum)
        }
    }

    fn probe(
        rendezvous: Option<(mpsc::Sender<usize>, Mutex<mpsc::Receiver<()>>)>,
        fail: bool,
    ) -> (ModelSamples, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let storage = ProbeStorage {
            samples: RwLock::new(vec![ModelSample::valid(ModelValue::new(0.0).unwrap()); 8].into()),
            calls: calls.clone(),
            rendezvous,
            fail_after_write: fail,
        };
        let mut samples = ModelSamples {
            storage: Box::new(storage),
            window_samples: 4,
            maximum_magnitude: AtomicU64::new(0),
            pending: Vec::new(),
        };
        samples
            .queue_updates(
                [
                    ModelSampleUpdate {
                        index: 1,
                        increment: 2.0,
                    },
                    ModelSampleUpdate {
                        index: 6,
                        increment: -3.0,
                    },
                ],
                NumericPrecision::F32,
                10.0,
            )
            .unwrap();
        (samples, calls)
    }

    #[test]
    fn sparse_updates_are_lazy_shared_once_and_disjoint_windows_can_start_together() {
        let (entered, entries) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        let (samples, calls) = probe(Some((entered, Mutex::new(resumed))), false);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "no serial cube preparation"
        );
        std::thread::scope(|scope| {
            let first = scope.spawn(|| samples.read(0..4).unwrap());
            let second = scope.spawn(|| samples.read(4..8).unwrap());
            let shared = scope.spawn(|| samples.read(0..4).unwrap());
            let mut indices = [
                entries.recv_timeout(Duration::from_secs(10)).unwrap(),
                entries.recv_timeout(Duration::from_secs(10)).unwrap(),
            ];
            indices.sort();
            assert_eq!(indices, [1, 6], "both regions enter before either finishes");
            resume.send(()).unwrap();
            resume.send(()).unwrap();
            let first = first.join().unwrap();
            assert_eq!(first[1].value().value(), 2.0);
            assert_eq!(second.join().unwrap()[2].value().value(), -3.0);
            assert_eq!(
                shared.join().unwrap(),
                first,
                "overlapping halo waits for one update"
            );
        });
        samples.finish_updates().unwrap();
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "exactly one sparse call per changed window"
        );
        assert_eq!(samples.maximum_magnitude(), 3.0);
    }

    #[test]
    fn completion_applies_unvisited_updates_but_no_unchanged_cells() {
        let (samples, calls) = probe(None, false);
        assert!(samples.read(2..2).unwrap().is_empty());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let first = samples.read(0..2).unwrap();
        assert_eq!(first[0].value().value(), 0.0);
        assert_eq!(first[1].value().value(), 2.0);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        samples.finish_updates().unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(samples.read(4..8).unwrap()[2].value().value(), -3.0);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn failed_partial_update_is_sticky_and_cannot_be_reapplied_or_completed() {
        let (samples, calls) = probe(None, true);
        let error = ModelLifecycleError::Storage("partial write".into());
        assert_eq!(samples.read(0..4), Err(error.clone()));
        assert_eq!(samples.read(0..4), Err(error.clone()));
        assert_eq!(samples.finish_updates(), Err(error));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn lazy_arithmetic_enforces_bound_and_support_before_returning_values() {
        let (mut samples, _) = probe(None, false);
        samples.finish_updates().unwrap();
        samples
            .queue_updates(
                [ModelSampleUpdate {
                    index: 1,
                    increment: 20.0,
                }],
                NumericPrecision::F32,
                10.0,
            )
            .unwrap();
        assert_eq!(
            samples.read(0..4),
            Err(ModelLifecycleError::ModelValueBoundExceeded)
        );
        assert_eq!(
            samples.finish_updates(),
            Err(ModelLifecycleError::ModelValueBoundExceeded)
        );
        let update = ModelSampleUpdate {
            index: 1,
            increment: 1.0,
        };
        assert_eq!(
            update.apply(ModelSample::invalid(), NumericPrecision::F32, 10.0),
            Err(ModelLifecycleError::DeltaOutsideValidSupport)
        );
    }

    #[test]
    fn sparse_workspace_charge_bounds_exact_allocations_and_checks_overflow() {
        let (samples, _) = probe(None, false);
        let owned = samples.pending.capacity() * size_of::<PendingWindow>()
            + samples
                .pending
                .iter()
                .map(|window| size_of_val(window.updates.as_ref()))
                .sum::<usize>();
        assert_eq!(Some(owned), ModelStoragePlan::pending_update_bytes(2, 8, 4));
        assert_eq!(ModelStoragePlan::pending_update_bytes(0, 8, 4), Some(0));
        assert_eq!(ModelStoragePlan::pending_update_bytes(1, 8, 0), None);
        assert_eq!(
            ModelStoragePlan::pending_update_bytes(usize::MAX, usize::MAX, 1),
            None
        );
    }
}

#[derive(Debug)]
struct ResidentModelStorage;

impl ModelStorageFactory for ResidentModelStorage {
    fn create(
        &self,
        sample_count: usize,
    ) -> Result<Box<dyn ModelSampleStorage>, ModelLifecycleError> {
        Ok(Box::new(RwLock::new(
            vec![ModelSample::invalid(); sample_count].into_boxed_slice(),
        )))
    }
}

impl ModelSampleStorage for RwLock<Box<[ModelSample]>> {
    fn sample_count(&self) -> usize {
        self.read().expect("model storage poisoned").len()
    }

    fn read(
        &self,
        start: usize,
        destination: &mut [ModelSample],
    ) -> Result<(), ModelLifecycleError> {
        let end = start
            .checked_add(destination.len())
            .ok_or(ModelLifecycleError::CellOutsideShape)?;
        let samples = self
            .read()
            .map_err(|e| ModelLifecycleError::Storage(e.to_string()))?;
        let source = samples
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
            .get_mut()
            .map_err(|e| ModelLifecycleError::Storage(e.to_string()))?
            .get_mut(start..end)
            .ok_or(ModelLifecycleError::CellOutsideShape)?;
        destination.copy_from_slice(samples);
        Ok(())
    }

    fn apply_updates(
        &self,
        updates: &[ModelSampleUpdate],
        precision: NumericPrecision,
        bound: f64,
    ) -> Result<f64, ModelLifecycleError> {
        let mut samples = self
            .write()
            .map_err(|e| ModelLifecycleError::Storage(e.to_string()))?;
        let mut maximum: f64 = 0.0;
        for update in updates {
            let sample = samples
                .get_mut(update.index)
                .ok_or(ModelLifecycleError::CellOutsideShape)?;
            *sample = update.apply(*sample, precision, bound)?;
            maximum = maximum.max(sample.value().value().abs());
        }
        Ok(maximum)
    }
}
