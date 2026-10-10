// SPDX-License-Identifier: LGPL-3.0-or-later

//! Owner of a run's model generations and their sparse updates.

use casa_imaging_model::{
    CompiledProblem, ModelCell, ModelContractError, ModelDeltaTerm, ModelLifecycleContract,
    ModelSample, ModelSourceShape, ModelSupport, ModelValue,
};
use thiserror::Error;

use crate::model_storage::{ModelSampleUpdate, ModelSamples, ModelStoragePlan};

/// One generation of a run's model.
///
/// It has no public constructor and is deliberately not `Clone`: a
/// generation moves from the lifecycle into a major cycle, out of its
/// completion and into the next cycle's final-model preparation.
/// Sparse updates are materialized once per admitted window on first access,
/// allowing plane workers to reuse the owned backing without a cube-wide copy.
#[derive(Debug)]
pub struct ModelGeneration {
    shape: ModelSourceShape,
    samples: ModelSamples,
}

impl ModelGeneration {
    /// Return the exact model-space shape.
    #[must_use]
    pub const fn shape(&self) -> &ModelSourceShape {
        &self.shape
    }

    /// Return the logical sample count, independently of resident storage.
    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Heap bytes this generation holds resident: its samples outside paged
    /// storage and its queued sparse updates.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.samples.resident_bytes()
    }

    /// Read an explicitly bounded canonical sample range.
    pub fn read_samples(
        &self,
        range: std::ops::Range<usize>,
    ) -> Result<Box<[ModelSample]>, ModelLifecycleError> {
        self.samples.read(range)
    }

    /// Read one complete spatial model plane in canonical y, x order.
    pub fn read_plane(
        &self,
        domain: usize,
        coefficient: usize,
        polarization: usize,
    ) -> Result<Box<[ModelSample]>, ModelLifecycleError> {
        let start = self
            .shape
            .flat_index(ModelCell::new(domain, coefficient, polarization, [0, 0]))
            .ok_or(ModelLifecycleError::CellOutsideShape)?;
        let [width, height] = self.shape.domains()[domain].pixels();
        self.samples.read(start..start + width * height)
    }
}

/// A major cycle's final model: its base generation with the minor cycle's
/// update queued, which the cycle's pass reads and its completion flushes.
///
/// Pending sparse arithmetic is checked on window access and must succeed
/// before completion; a storage or scientific error fails the cycle.
#[derive(Debug)]
pub struct PreparedFinalModel {
    generation: ModelGeneration,
}

impl PreparedFinalModel {
    /// Borrow the final generation for the cycle's pass.
    #[must_use]
    pub const fn generation(&self) -> &ModelGeneration {
        &self.generation
    }

    /// Resolve every pending update and release the final generation.
    pub(crate) fn complete(mut self) -> Result<ModelGeneration, ModelLifecycleError> {
        self.generation.samples.complete_updates()?;
        Ok(self.generation)
    }
}

/// A run's compiled model lifecycle: the model space, its bounds and the
/// storage its generations use.
#[derive(Debug)]
pub struct ModelLifecycle {
    contract: ModelLifecycleContract,
    storage: ModelStoragePlan,
}

impl ModelLifecycle {
    /// The model lifecycle of `problem`, storing generations as `storage`
    /// plans.
    #[must_use]
    pub fn new(problem: &CompiledProblem, storage: ModelStoragePlan) -> Self {
        Self {
            contract: problem.model_lifecycle().clone(),
            storage,
        }
    }

    /// Return the exact compiled lifecycle commitment.
    #[must_use]
    pub const fn contract(&self) -> &ModelLifecycleContract {
        &self.contract
    }

    /// Heap bytes one of this run's generations holds resident, before any
    /// sparse update is queued.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.storage
            .resident_bytes(self.contract.target().sample_count())
    }

    /// The empty initial generation every run begins from.
    pub fn initial_empty(&self) -> Result<ModelGeneration, ModelLifecycleError> {
        let zero = ModelValue::new(0.0)?;
        let mut samples = self.storage.create(self.contract.target().sample_count())?;
        // Written in bounded chunks, not one model-sized copy.
        let window = vec![ModelSample::valid(zero); samples.window_samples().min(1 << 16)];
        for start in (0..samples.len()).step_by(window.len()) {
            samples.write(start, &window[..window.len().min(samples.len() - start)])?;
        }
        Ok(ModelGeneration {
            shape: self.contract.target().clone(),
            samples,
        })
    }

    /// The final model of a major cycle: `base` updated by the minor cycle's
    /// `terms`, with later domains' shared pixels restored into earlier
    /// domains. No terms leave the samples unchanged.
    ///
    /// Terms must arrive in strictly increasing canonical cell order, each
    /// non-zero, within the compiled bounds and on valid support of `base`.
    /// The update is queued on `base`'s storage; each bounded window is
    /// updated once on first access.
    ///
    /// # Panics
    ///
    /// If `base` is not in this lifecycle's model space.
    pub fn prepare_final_model(
        &self,
        mut base: ModelGeneration,
        terms: impl IntoIterator<Item = ModelDeltaTerm>,
    ) -> Result<PreparedFinalModel, ModelLifecycleError> {
        self.validate_generation(&base)?;
        let updates = self.validate_terms(&base, terms)?;
        if !updates.is_empty() {
            base.samples.queue_updates(
                updates,
                self.contract.arithmetic_precision(),
                self.contract.bounds().max_absolute_model_value(),
            )?;
        }
        self.restore_later_domain_overlap(&mut base)?;
        Ok(PreparedFinalModel { generation: base })
    }

    fn validate_terms(
        &self,
        base: &ModelGeneration,
        terms: impl IntoIterator<Item = ModelDeltaTerm>,
    ) -> Result<Vec<ModelSampleUpdate>, ModelLifecycleError> {
        let terms = terms.into_iter();
        let bound = self.contract.bounds().max_delta_terms();
        let mut updates =
            Vec::with_capacity(terms.size_hint().0.min(bound).min(base.sample_count()));
        let mut prior = None;
        for term in terms {
            if updates.len() == bound {
                return Err(ModelLifecycleError::DeltaTermBoundExceeded {
                    terms: updates.len() + 1,
                    bound,
                });
            }
            let index = self
                .contract
                .target()
                .flat_index(term.cell())
                .ok_or(ModelLifecycleError::CellOutsideShape)?;
            if prior.is_some_and(|previous| previous >= index) {
                return Err(ModelLifecycleError::NonCanonicalDelta);
            }
            prior = Some(index);
            if term.increment().value() == 0.0 {
                return Err(ModelLifecycleError::ZeroDeltaTerm);
            }
            validate_model_value(
                term.increment(),
                self.contract.bounds().max_absolute_delta_value(),
            )
            .map_err(|_| ModelLifecycleError::DeltaValueBoundExceeded)?;
            if base.samples.read(index..index + 1)?[0].support() != ModelSupport::Valid {
                return Err(ModelLifecycleError::DeltaOutsideValidSupport);
            }
            updates.push(ModelSampleUpdate {
                index,
                increment: term.increment().value(),
            });
        }
        Ok(updates)
    }

    /// Restore CASA's canonical multi-domain model ownership after an update.
    ///
    /// Later compiled domains own shared sky pixels. CASA omits those pixels from
    /// earlier-domain prediction and restores the later values into earlier models
    /// before publication and continuation.
    fn restore_later_domain_overlap(
        &self,
        generation: &mut ModelGeneration,
    ) -> Result<(), ModelLifecycleError> {
        let shape = self.contract.target();
        if shape.domains().len() < 2 {
            return Ok(());
        }
        for earlier in (0..shape.domains().len() - 1).rev() {
            let [width, height] = shape.domains()[earlier].pixels();
            let target_coordinate = shape
                .direction(earlier)
                .ok_or(ModelLifecycleError::UnsupportedDirectionConversion)?;
            for y in 0..height {
                for x in 0..width {
                    let mut owner = None;
                    for later in earlier + 1..shape.domains().len() {
                        let source_coordinate = shape
                            .direction(later)
                            .ok_or(ModelLifecycleError::UnsupportedDirectionConversion)?;
                        if let Some(pixel) = crate::mask::reprojected_pixel(
                            source_coordinate,
                            shape.domains()[later].pixels(),
                            target_coordinate,
                            [x, y],
                        )
                        .map_err(|_| ModelLifecycleError::UnsupportedDirectionConversion)?
                        {
                            owner = Some((later, pixel));
                        }
                    }
                    let Some((owner_domain, owner_pixel)) = owner else {
                        continue;
                    };
                    for coefficient in 0..shape.coefficients() {
                        for polarization in 0..shape.polarizations() {
                            let target = shape
                                .flat_index(ModelCell::new(
                                    earlier,
                                    coefficient,
                                    polarization,
                                    [x, y],
                                ))
                                .ok_or(ModelLifecycleError::CellOutsideShape)?;
                            let source = shape
                                .flat_index(ModelCell::new(
                                    owner_domain,
                                    coefficient,
                                    polarization,
                                    owner_pixel,
                                ))
                                .ok_or(ModelLifecycleError::CellOutsideShape)?;
                            let sample =
                                match generation.samples.read(target..target + 1)?[0].support() {
                                    ModelSupport::Valid => ModelSample::valid(
                                        generation.samples.read(source..source + 1)?[0].value(),
                                    ),
                                    ModelSupport::Invalid => ModelSample::invalid(),
                                };
                            generation.samples.write(target, &[sample])?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Check a generation against the compiled value bound.
    ///
    /// # Panics
    ///
    /// If the generation is not in this lifecycle's model space: generations
    /// are created only by a lifecycle and move through one problem's major
    /// cycles.
    fn validate_generation(&self, generation: &ModelGeneration) -> Result<(), ModelLifecycleError> {
        generation.samples.finish_updates()?;
        assert!(
            generation.shape == *self.contract.target()
                && generation.samples.len() == generation.shape.sample_count(),
            "a model generation stays in the model space of the lifecycle that created it"
        );
        // A tighter scientific bound is a new constraint, unlike a routine
        // transfer of an already validated model. Overlap restoration can lower
        // the recorded maximum, so validate rather than reject this transition.
        if generation.samples.maximum_magnitude()
            <= self.contract.bounds().max_absolute_model_value()
        {
            return Ok(());
        }
        generation.samples.for_each_window(|_, samples| {
            for sample in samples {
                if sample.support() == ModelSupport::Valid {
                    validate_model_value(
                        sample.value(),
                        self.contract.bounds().max_absolute_model_value(),
                    )?;
                } else if sample.value().value() != 0.0 {
                    return Err(ModelLifecycleError::InvalidSupportPayload);
                }
            }
            Ok(())
        })?;
        Ok(())
    }
}

/// Exact reason model lifecycle validation failed closed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ModelLifecycleError {
    /// An admitted model backing or window could not be accessed.
    #[error("model storage: {0}")]
    Storage(String),
    /// A model schema value was invalid.
    #[error(transparent)]
    Contract(#[from] ModelContractError),
    /// A sample stream length differed from its shape.
    #[error("model stream requires {expected} samples but received {actual}")]
    SampleCountMismatch {
        /// Shape-derived sample count.
        expected: usize,
        /// Supplied sample count.
        actual: usize,
    },
    /// A cell lay outside its typed shape.
    #[error("model cell lies outside its declared shape")]
    CellOutsideShape,
    /// Two image domains' direction laws require an unsupported frame or tangent-point conversion.
    #[error("model domains overlap only through an exact affine mapping within one tangent plane")]
    UnsupportedDirectionConversion,
    /// A model value exceeded the lifecycle ceiling.
    #[error("model value exceeds the compiled lifecycle bound")]
    ModelValueBoundExceeded,
    /// A model update exceeded its compiled term ceiling.
    #[error("model update has {terms} terms, exceeding bound {bound}")]
    DeltaTermBoundExceeded {
        /// Exact term count observed before failure.
        terms: usize,
        /// Compiled term ceiling.
        bound: usize,
    },
    /// Update terms were not unique and strictly canonical.
    #[error("model update terms must be unique and in canonical cell order")]
    NonCanonicalDelta,
    /// An update term exceeded its value ceiling.
    #[error("model update term exceeds the compiled lifecycle bound")]
    DeltaValueBoundExceeded,
    /// An update term had no numerical update.
    #[error("model update terms must be non-zero")]
    ZeroDeltaTerm,
    /// An update attempted to create a value outside valid support.
    #[error("model update terms may update only valid model support")]
    DeltaOutsideValidSupport,
    /// Invalid support carried a numeric payload.
    #[error("invalid model support may not carry a numeric value")]
    InvalidSupportPayload,
}

pub(crate) fn validate_model_value(
    value: ModelValue,
    bound: f64,
) -> Result<(), ModelLifecycleError> {
    if value.value().abs() > bound {
        Err(ModelLifecycleError::ModelValueBoundExceeded)
    } else {
        Ok(())
    }
}
