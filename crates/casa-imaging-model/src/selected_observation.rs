// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiler-owned scientific identity of one selected-observation evaluation.

use std::fmt;

use thiserror::Error;

use crate::MsColumnKind;
use crate::{
    compiled_problem::{CanonicalEncoder, SpectralSamplingLaw, encode_spectral_sampling_law},
    geometry::CompiledGeometryId,
    measurement_equation::VisibilityInnerProduct,
    observation::{
        CorrelationSelection, DataDescriptionSelection, MeasurementSetIdentity,
        ObservationSnapshotId, SelectedMainRow, SelectedRowSequenceAccumulator,
        SelectedRowSequenceError, SpectralWindowSelection, VisibilityColumn, WeightColumn,
    },
    selected_observation_sample::{
        SelectedObservationGenerationEncoder, SelectedObservationGenerationId,
        SelectedObservationSample, SelectedObservationSampleView, SelectedPredictionTarget,
        SelectedVisibilitySample,
    },
    transaction::{
        MeasurementSetReadAccess, ObservationReadSet, ObservationTransactionContract,
        ObservationWriteSet,
    },
};

const SELECTED_OBSERVATION_COMMITMENT_IDENTITY_DOMAIN: &[u8] =
    b"casa-rs-selected-observation-commitment";
const SELECTED_OBSERVATION_COMMITMENT_IDENTITY_VERSION: u32 = 1;

/// Stable compiler-derived identity of one selected-observation scientific commitment.
///
/// There is deliberately no constructor from raw digest bytes. Only logical
/// problem compilation can mint this identity.
///
/// ```compile_fail
/// use casa_imaging_model::SelectedObservationCommitmentId;
///
/// let _ = SelectedObservationCommitmentId::from_sha256([0; 32]);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectedObservationCommitmentId([u8; 32]);

impl SelectedObservationCommitmentId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = SELECTED_OBSERVATION_COMMITMENT_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for SelectedObservationCommitmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SelectedObservationCommitmentId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for SelectedObservationCommitmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Sample-space evaluation semantics owned by the selected-observation compiler seam.
///
/// Reconstruction, product, weighting, numerical, and physical execution
/// choices are deliberately absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedSampleEvaluation {
    visibility_inner_product: VisibilityInnerProduct,
    spectral_sampling: SpectralSamplingLaw,
}

impl SelectedSampleEvaluation {
    /// Return the exact visibility-sample inner product.
    #[must_use]
    pub const fn visibility_inner_product(self) -> VisibilityInnerProduct {
        self.visibility_inner_product
    }

    /// Return the paired spectral sampling applied to selected samples.
    #[must_use]
    pub const fn spectral_sampling(self) -> SpectralSamplingLaw {
        self.spectral_sampling
    }
}

/// Immutable compiler-owned science that every selected-observation traversal must realize.
///
/// The commitment contains no block size, read-ahead depth, double-buffering,
/// worker, backend, resource policy, reconstruction, weighting, or publication
/// choice. The retained read set is derived from the same snapshot during
/// compilation and supplies the exact typed source projection to traversal.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedObservationCommitment {
    commitment_id: SelectedObservationCommitmentId,
    observation_snapshot_id: ObservationSnapshotId,
    geometry_id: CompiledGeometryId,
    sample_evaluation: SelectedSampleEvaluation,
    read_set: ObservationReadSet,
}

impl SelectedObservationCommitment {
    /// Return the compiler-derived identity of this exact scientific commitment.
    #[must_use]
    pub const fn commitment_id(&self) -> SelectedObservationCommitmentId {
        self.commitment_id
    }

    /// Return the immutable atomic snapshot defining source selection and generations.
    #[must_use]
    pub const fn observation_snapshot_id(&self) -> ObservationSnapshotId {
        self.observation_snapshot_id
    }

    /// Return the compiled coordinate, phase, and spectral-geometry identity.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry_id
    }

    /// Return the exact selected-sample evaluation semantics.
    #[must_use]
    pub const fn sample_evaluation(&self) -> SelectedSampleEvaluation {
        self.sample_evaluation
    }

    /// Return exact source selection and generation semantics in canonical source order.
    #[must_use]
    pub const fn read_set(&self) -> &ObservationReadSet {
        &self.read_set
    }

    /// Return a conservative bound for the validation and digest state live
    /// during one canonical traversal.
    ///
    /// This includes the fixed SHA/coverage state and the active row's
    /// broadcast-weight vector. It is independent of selected MAIN row and
    /// DATA_DESCRIPTION cardinality.
    #[must_use]
    pub fn inspection_scratch_bytes(&self) -> Option<usize> {
        let maximum_correlations = self
            .read_set
            .sources()
            .iter()
            .flat_map(|source| source.selection().correlations())
            .map(|selection| selection.products().len())
            .max()
            .unwrap_or(0);
        size_of::<SelectedObservationInspection<'static>>()
            .checked_add(maximum_correlations.checked_mul(size_of::<Option<f32>>())?)
    }
}

/// Failure to inspect reports as one exact compiled selected observation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SelectedObservationInspectionError {
    /// The observed physical MAIN rows did not match the compact committed manifest.
    #[error("selected physical MAIN rows do not match source {measurement_set}")]
    SelectedRowSequenceMismatch {
        /// Source whose physical row sequence differed.
        measurement_set: MeasurementSetIdentity,
    },
    /// A reported DDID/SPW/polarization tuple contradicted the compiled catalog.
    #[error("selected DATA_DESCRIPTION coordinate does not match source {measurement_set}")]
    DataDescriptionCoordinateMismatch {
        /// Source containing the inconsistent coordinate.
        measurement_set: MeasurementSetIdentity,
        /// Reported physical MAIN row.
        physical_row: u64,
    },
    /// A row ended before every selected channel/correlation member was reported.
    #[error("selected sample is missing from source {measurement_set} row {physical_row}")]
    MissingSample {
        /// Source with incomplete sample coverage.
        measurement_set: MeasurementSetIdentity,
        /// Incomplete physical MAIN row.
        physical_row: u64,
    },
    /// A report did not name the next compiled channel/correlation member.
    #[error("unexpected selected sample in source {measurement_set} row {physical_row}")]
    UnexpectedSample {
        /// Source containing the unexpected member.
        measurement_set: MeasurementSetIdentity,
        /// Reported physical MAIN row.
        physical_row: u64,
    },
    /// One selected channel/correlation member was reported twice.
    #[error("duplicate selected sample in source {measurement_set} row {physical_row}")]
    DuplicateSample {
        /// Source containing the duplicated member.
        measurement_set: MeasurementSetIdentity,
        /// Reported physical MAIN row.
        physical_row: u64,
    },
    /// Sources or physical rows were not supplied in the canonical logical order.
    #[error(
        "selected samples are not in canonical order at source {measurement_set} row {physical_row}"
    )]
    NonCanonicalSampleOrder {
        /// Source observed out of order.
        measurement_set: MeasurementSetIdentity,
        /// Physical row observed out of order.
        physical_row: u64,
    },
    /// Prediction-destination reporting contradicted the transaction write set.
    #[error("prediction target does not match source {measurement_set} row {physical_row}")]
    PredictionTargetMismatch {
        /// Source containing the wrong prediction target.
        measurement_set: MeasurementSetIdentity,
        /// Physical row containing the wrong prediction target.
        physical_row: u64,
    },
    /// Visibility storage representation contradicted the selected visibility column.
    #[error("visibility storage does not match source {measurement_set} row {physical_row}")]
    VisibilityStorageMismatch {
        /// Source containing the wrong visibility representation.
        measurement_set: MeasurementSetIdentity,
        /// Physical MAIN row containing the wrong representation.
        physical_row: u64,
    },
    /// A broadcast MAIN `WEIGHT` value varied across selected channels.
    #[error(
        "broadcast WEIGHT differs across channels in source {measurement_set} row {physical_row}"
    )]
    WeightBroadcastMismatch {
        /// Source containing inconsistent broadcast weights.
        measurement_set: MeasurementSetIdentity,
        /// Physical MAIN row containing inconsistent weights.
        physical_row: u64,
    },
}

pub(crate) fn inspect_selected_observation<E>(
    commitment: &SelectedObservationCommitment,
    write_set: &ObservationWriteSet,
    samples: impl IntoIterator<Item = Result<SelectedObservationSample, E>>,
    mut consume: impl FnMut(SelectedObservationSample) -> Result<(), E>,
) -> Result<(SelectedObservationGenerationId, u64), SelectedObservationPassError<E>> {
    let mut inspection = SelectedObservationInspection::new(commitment, write_set);
    for sample in samples {
        let sample = sample.map_err(SelectedObservationPassError::External)?;
        inspection
            .push(&sample)
            .map_err(SelectedObservationPassError::Inspection)?;
        consume(sample).map_err(SelectedObservationPassError::External)?;
    }
    inspection
        .finish()
        .map_err(SelectedObservationPassError::Inspection)
}

/// Failure during one closed selected-observation inspection pass.
#[derive(Debug)]
pub enum SelectedObservationPassError<E> {
    /// The source iterator or post-validation consumer failed.
    External(E),
    /// A sample or final coverage proof contradicted the compiled commitment.
    Inspection(SelectedObservationInspectionError),
}

impl<E: fmt::Display> fmt::Display for SelectedObservationPassError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::External(error) => error.fmt(formatter),
            Self::Inspection(error) => error.fmt(formatter),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for SelectedObservationPassError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::External(error) => Some(error),
            Self::Inspection(error) => Some(error),
        }
    }
}

/// Incremental validator and content-identity encoder for one canonical
/// selected-observation pass.
///
/// This state supports bounded block transports without materializing the
/// pass. It does not prove retained source access or mint storage completion.
pub struct SelectedObservationInspection<'a> {
    expected_sources: &'a [MeasurementSetReadAccess],
    write_set: &'a ObservationWriteSet,
    source_index: usize,
    source: SourceInspection<'a>,
    generation: SelectedObservationGenerationEncoder,
}

impl<'a> SelectedObservationInspection<'a> {
    pub(crate) fn new(
        commitment: &'a SelectedObservationCommitment,
        write_set: &'a ObservationWriteSet,
    ) -> Self {
        let expected_sources = commitment.read_set().sources();
        Self {
            expected_sources,
            write_set,
            source_index: 0,
            source: SourceInspection::new(
                &expected_sources[0],
                prediction_target(write_set, expected_sources[0].measurement_set()),
            ),
            generation: SelectedObservationGenerationEncoder::new(),
        }
    }

    /// Validate the next sample in canonical source/row/channel/correlation order.
    /// Validate and record the next canonical sample.
    pub fn push(
        &mut self,
        sample: &SelectedObservationSample,
    ) -> Result<(), SelectedObservationInspectionError> {
        self.push_view(sample.as_view())
    }

    /// Validate and record one borrowed scalar or run member.
    pub fn push_view(
        &mut self,
        sample: SelectedObservationSampleView<'_>,
    ) -> Result<(), SelectedObservationInspectionError> {
        let address = sample.address();
        while address.measurement_set != self.source.expected.measurement_set() {
            if self.source.observed_row_count() == 0
                && self.source.expected.selection().rows().selected_row_count() > 0
            {
                return Err(
                    SelectedObservationInspectionError::NonCanonicalSampleOrder {
                        measurement_set: address.measurement_set,
                        physical_row: address.physical_row,
                    },
                );
            }
            let next_index = self.source_index + 1;
            let Some(expected) = self.expected_sources.get(next_index) else {
                return Err(
                    SelectedObservationInspectionError::NonCanonicalSampleOrder {
                        measurement_set: address.measurement_set,
                        physical_row: address.physical_row,
                    },
                );
            };
            let prior = std::mem::replace(
                &mut self.source,
                SourceInspection::new(
                    expected,
                    prediction_target(self.write_set, expected.measurement_set()),
                ),
            );
            prior.finish()?;
            self.source_index = next_index;
        }
        self.source.push(sample)?;
        self.generation.push_view(sample);
        Ok(())
    }

    /// Finish exhaustive coverage validation and return content identity and count.
    pub fn finish(
        self,
    ) -> Result<(SelectedObservationGenerationId, u64), SelectedObservationInspectionError> {
        self.source.finish()?;
        for expected in &self.expected_sources[self.source_index + 1..] {
            SourceInspection::new(
                expected,
                prediction_target(self.write_set, expected.measurement_set()),
            )
            .finish()?;
        }
        Ok(self.generation.finish())
    }
}

fn prediction_target(
    write_set: &ObservationWriteSet,
    measurement_set: MeasurementSetIdentity,
) -> SelectedPredictionTarget {
    if write_set.visibility_columns().iter().any(|write| {
        write.column() == MsColumnKind::ModelData && write.measurement_set() == measurement_set
    }) {
        SelectedPredictionTarget::ModelData
    } else {
        SelectedPredictionTarget::NotRequested
    }
}

struct SourceInspection<'a> {
    expected: &'a MeasurementSetReadAccess,
    prediction_target: SelectedPredictionTarget,
    rows: SelectedRowSequenceAccumulator,
    row: Option<RowInspection<'a>>,
}

impl<'a> SourceInspection<'a> {
    fn new(
        expected: &'a MeasurementSetReadAccess,
        prediction_target: SelectedPredictionTarget,
    ) -> Self {
        let selected_rows = expected.selection().rows();
        Self {
            expected,
            prediction_target,
            rows: SelectedRowSequenceAccumulator::new(selected_rows.source_row_count()),
            row: None,
        }
    }

    fn observed_row_count(&self) -> u64 {
        self.rows.observed_row_count()
    }

    fn push(
        &mut self,
        sample: SelectedObservationSampleView<'_>,
    ) -> Result<(), SelectedObservationInspectionError> {
        let address = sample.address();
        if sample.prediction_target() != self.prediction_target {
            return Err(
                SelectedObservationInspectionError::PredictionTargetMismatch {
                    measurement_set: address.measurement_set,
                    physical_row: address.physical_row,
                },
            );
        }
        let visibility_matches = matches!(
            (
                self.expected.selected_columns().visibility(),
                sample.visibility()
            ),
            (
                VisibilityColumn::FloatData,
                SelectedVisibilitySample::Float32(_)
            ) | (
                VisibilityColumn::Data | VisibilityColumn::CorrectedData,
                SelectedVisibilitySample::Complex32(_)
            )
        );
        if !visibility_matches {
            return Err(
                SelectedObservationInspectionError::VisibilityStorageMismatch {
                    measurement_set: address.measurement_set,
                    physical_row: address.physical_row,
                },
            );
        }
        let start_new_row = self
            .row
            .as_ref()
            .is_none_or(|row| row.physical_row != address.physical_row);
        if start_new_row {
            if let Some(row) = self.row.take() {
                let previous_row = row.physical_row;
                row.finish()?;
                if address.physical_row < previous_row {
                    return Err(
                        SelectedObservationInspectionError::NonCanonicalSampleOrder {
                            measurement_set: address.measurement_set,
                            physical_row: address.physical_row,
                        },
                    );
                }
            }
            let data_description_id = u32::try_from(address.data_description_id).map_err(|_| {
                SelectedObservationInspectionError::DataDescriptionCoordinateMismatch {
                    measurement_set: address.measurement_set,
                    physical_row: address.physical_row,
                }
            })?;
            self.rows
                .push(SelectedMainRow::new(
                    address.physical_row,
                    data_description_id,
                ))
                .map_err(|error| match error {
                    SelectedRowSequenceError::DuplicatePhysicalRow { .. }
                    | SelectedRowSequenceError::DescendingPhysicalRow { .. } => {
                        SelectedObservationInspectionError::NonCanonicalSampleOrder {
                            measurement_set: address.measurement_set,
                            physical_row: address.physical_row,
                        }
                    }
                    _ => SelectedObservationInspectionError::SelectedRowSequenceMismatch {
                        measurement_set: address.measurement_set,
                    },
                })?;
            self.row = Some(RowInspection::new(self.expected, sample)?);
        }
        self.row
            .as_mut()
            .expect("row inspection starts before sample validation")
            .push(sample)
    }

    fn finish(self) -> Result<(), SelectedObservationInspectionError> {
        if let Some(row) = self.row {
            row.finish()?;
        }
        let (observed_count, sequence_id, _) = self.rows.finish();
        let expected_rows = self.expected.selection().rows();
        if observed_count != expected_rows.selected_row_count()
            || sequence_id != expected_rows.sequence_id()
        {
            return Err(
                SelectedObservationInspectionError::SelectedRowSequenceMismatch {
                    measurement_set: self.expected.measurement_set(),
                },
            );
        }
        Ok(())
    }
}

struct RowInspection<'a> {
    measurement_set: MeasurementSetIdentity,
    physical_row: u64,
    data_description: DataDescriptionSelection,
    spectral_window: &'a SpectralWindowSelection,
    correlation: &'a CorrelationSelection,
    next_ordinal: usize,
    previous: Option<(u32, u32)>,
    weight_column: WeightColumn,
    broadcast_weights: Vec<Option<f32>>,
}

impl<'a> RowInspection<'a> {
    fn new(
        expected: &'a MeasurementSetReadAccess,
        sample: SelectedObservationSampleView<'_>,
    ) -> Result<Self, SelectedObservationInspectionError> {
        let address = sample.address();
        let Ok(data_description_id) = u32::try_from(address.data_description_id) else {
            return Err(
                SelectedObservationInspectionError::DataDescriptionCoordinateMismatch {
                    measurement_set: address.measurement_set,
                    physical_row: address.physical_row,
                },
            );
        };
        let Some(data_description) = expected
            .selection()
            .data_descriptions()
            .iter()
            .copied()
            .find(|member| member.data_description_id() == data_description_id)
        else {
            return Err(
                SelectedObservationInspectionError::DataDescriptionCoordinateMismatch {
                    measurement_set: address.measurement_set,
                    physical_row: address.physical_row,
                },
            );
        };
        let spectral_window = expected
            .selection()
            .spectral_windows()
            .iter()
            .find(|selection| {
                selection.spectral_window_id() == data_description.spectral_window_id()
            })
            .expect("compiled DATA_DESCRIPTION references an exact spectral window");
        let correlation = expected
            .selection()
            .correlations()
            .iter()
            .find(|selection| selection.polarization_id() == data_description.polarization_id())
            .expect("compiled DATA_DESCRIPTION references an exact polarization");
        Ok(Self {
            measurement_set: address.measurement_set,
            physical_row: address.physical_row,
            data_description,
            spectral_window,
            correlation,
            next_ordinal: 0,
            previous: None,
            weight_column: expected.selected_columns().weights(),
            broadcast_weights: vec![None; correlation.products().len()],
        })
    }

    fn push(
        &mut self,
        sample: SelectedObservationSampleView<'_>,
    ) -> Result<(), SelectedObservationInspectionError> {
        let address = sample.address();
        if address.data_description_id
            != i32::try_from(self.data_description.data_description_id())
                .expect("compiled DATA_DESC_ID fits the MeasurementSet Int domain")
            || address.spectral_window_id != self.data_description.spectral_window_id()
            || address.polarization_id != self.data_description.polarization_id()
        {
            return Err(
                SelectedObservationInspectionError::DataDescriptionCoordinateMismatch {
                    measurement_set: address.measurement_set,
                    physical_row: address.physical_row,
                },
            );
        }
        let products = self.correlation.products();
        let expected_count = self
            .spectral_window
            .channel_indices()
            .len()
            .checked_mul(products.len())
            .expect("compiled selected-sample count fits usize");
        if self.next_ordinal >= expected_count {
            return Err(SelectedObservationInspectionError::DuplicateSample {
                measurement_set: address.measurement_set,
                physical_row: address.physical_row,
            });
        }
        let channel = self.spectral_window.channel_indices()[self.next_ordinal / products.len()];
        let product = products[self.next_ordinal % products.len()];
        let actual = (address.channel_index, address.correlation_index);
        if self.previous == Some(actual) {
            return Err(SelectedObservationInspectionError::DuplicateSample {
                measurement_set: address.measurement_set,
                physical_row: address.physical_row,
            });
        }
        if actual != (channel, product.correlation_index())
            || address.correlation_type != product.correlation_type()
        {
            return Err(SelectedObservationInspectionError::UnexpectedSample {
                measurement_set: address.measurement_set,
                physical_row: address.physical_row,
            });
        }
        if self.weight_column == WeightColumn::Weight {
            let correlation_ordinal = self.next_ordinal % products.len();
            let observed = &mut self.broadcast_weights[correlation_ordinal];
            if let Some(expected) = observed {
                if !canonical_f32_eq(*expected, sample.input_weight()) {
                    return Err(
                        SelectedObservationInspectionError::WeightBroadcastMismatch {
                            measurement_set: address.measurement_set,
                            physical_row: address.physical_row,
                        },
                    );
                }
            } else {
                *observed = Some(sample.input_weight());
            }
        }
        self.previous = Some(actual);
        self.next_ordinal += 1;
        Ok(())
    }

    fn finish(self) -> Result<(), SelectedObservationInspectionError> {
        let expected_count = self
            .spectral_window
            .channel_indices()
            .len()
            .checked_mul(self.correlation.products().len())
            .expect("compiled selected-sample count fits usize");
        if self.next_ordinal != expected_count {
            return Err(SelectedObservationInspectionError::MissingSample {
                measurement_set: self.measurement_set,
                physical_row: self.physical_row,
            });
        }
        Ok(())
    }
}

fn canonical_f32_eq(left: f32, right: f32) -> bool {
    let canonical_bits = |value: f32| if value == 0.0 { 0 } else { value.to_bits() };
    canonical_bits(left) == canonical_bits(right)
}

pub(crate) fn compile_selected_observation_commitment(
    observation_transaction: &ObservationTransactionContract,
    geometry_id: CompiledGeometryId,
    visibility_inner_product: VisibilityInnerProduct,
    spectral_sampling: SpectralSamplingLaw,
) -> SelectedObservationCommitment {
    let observation_snapshot_id = observation_transaction.observation_snapshot_id();
    let sample_evaluation = SelectedSampleEvaluation {
        visibility_inner_product,
        spectral_sampling,
    };
    let commitment_id =
        selected_observation_commitment_id(observation_snapshot_id, geometry_id, sample_evaluation);
    SelectedObservationCommitment {
        commitment_id,
        observation_snapshot_id,
        geometry_id,
        sample_evaluation,
        read_set: observation_transaction.read_set().clone(),
    }
}

fn selected_observation_commitment_id(
    observation_snapshot_id: ObservationSnapshotId,
    geometry_id: CompiledGeometryId,
    sample_evaluation: SelectedSampleEvaluation,
) -> SelectedObservationCommitmentId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(SELECTED_OBSERVATION_COMMITMENT_IDENTITY_DOMAIN);
    encoder.u32(SELECTED_OBSERVATION_COMMITMENT_IDENTITY_VERSION);
    encoder.digest(observation_snapshot_id.as_bytes());
    encoder.digest(geometry_id.as_bytes());
    encode_visibility_inner_product(&mut encoder, sample_evaluation.visibility_inner_product);
    encode_spectral_sampling(&mut encoder, sample_evaluation.spectral_sampling);
    SelectedObservationCommitmentId(encoder.finish())
}

fn encode_visibility_inner_product(
    encoder: &mut CanonicalEncoder,
    inner_product: VisibilityInnerProduct,
) {
    encoder.u8(match inner_product {
        VisibilityInnerProduct::HermitianEuclidean => 0,
    });
}

fn encode_spectral_sampling(encoder: &mut CanonicalEncoder, sampling: SpectralSamplingLaw) {
    encode_spectral_sampling_law(encoder, sampling);
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
