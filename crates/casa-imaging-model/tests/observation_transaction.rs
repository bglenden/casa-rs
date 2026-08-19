// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_imaging_model::{
    ModelColumnInitialization, ModelColumnPrecondition, ModelColumnState, ModelColumnWrite,
    ModelColumnWriteDisposition, ModelStateIdentity, MsColumnKind, ObservationSnapshotInput,
    ObservationTransactionRequirements, compile_observation, compile_observation_transaction,
};

#[test]
fn transaction_contract_derives_the_exact_snapshot_read_set() {
    let snapshot =
        common::observation_snapshot(7, Vec::new(), casa_imaging_model::ModelStateIdentity::Empty);

    let contract = compile_observation_transaction(
        &snapshot,
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
    );

    assert_eq!(contract.observation_snapshot_id(), snapshot.snapshot_id());
    assert_eq!(contract.read_set().sources().len(), 1);
    let source = &contract.read_set().sources()[0];
    assert_eq!(source.measurement_set(), snapshot.sources()[0].identity());
    assert_eq!(source.selection(), snapshot.sources()[0].selection());
    assert_eq!(
        source.column_generations(),
        snapshot.sources()[0].generations().columns().generations()
    );
    assert_eq!(
        source.consistency_token(),
        snapshot.sources()[0].generations().consistency_token()
    );
    assert_eq!(
        source.metadata(),
        snapshot.sources()[0].generations().metadata_generations()
    );
    assert!(contract.write_set().model_columns().is_empty());
}

#[test]
fn selected_model_column_writes_have_a_pinned_schema_two_identity() {
    let snapshot =
        common::observation_snapshot(8, Vec::new(), casa_imaging_model::ModelStateIdentity::Empty);
    let read_only = compile_observation_transaction(
        &snapshot,
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
    );

    let writable = compile_observation_transaction(
        &snapshot,
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
    );

    assert_ne!(read_only.transaction_id(), writable.transaction_id());
    assert_eq!(
        casa_imaging_model::ObservationTransactionId::SCHEMA_VERSION,
        2
    );
    assert_eq!(
        writable.transaction_id().to_string(),
        "8c9e8d636c8e1599f69548161498880ae31cfc1831ccbb8e2c2e471e1f570c87"
    );
    assert_eq!(writable.write_set().model_columns().len(), 1);
    let write = &writable.write_set().model_columns()[0];
    assert_eq!(write.measurement_set(), snapshot.sources()[0].identity());
    assert_eq!(write.selection(), snapshot.sources()[0].selection());
    assert_eq!(write.column(), MsColumnKind::ModelData);
    assert_eq!(write.precondition(), ModelColumnPrecondition::Absent);
    assert_eq!(
        write.disposition(),
        ModelColumnWriteDisposition::CreateAndInitializeAllRows {
            row_count: snapshot.sources()[0].selection().rows().source_row_count(),
            initialization: ModelColumnInitialization::Zero,
        }
    );
    assert_eq!(
        write.expected_consistency_token(),
        snapshot.sources()[0].generations().consistency_token()
    );
}

#[test]
fn model_write_preconditions_preserve_the_previous_generation() {
    let previous_generation = common::identity(99);
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![common::observation_source_with_model_generation(
            9,
            Some(previous_generation),
        )],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile observation with MODEL_DATA");

    let contract = compile_observation_transaction(
        &snapshot,
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
    );

    assert_eq!(
        contract.write_set().model_columns()[0].precondition(),
        ModelColumnPrecondition::Generation(previous_generation)
    );
    assert_eq!(
        contract.write_set().model_columns()[0].disposition(),
        ModelColumnWriteDisposition::ReplaceSelectedCells
    );
}

#[test]
fn output_only_model_columns_are_preconditioned_without_entering_the_read_set() {
    let previous_generation = common::identity(98);
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![common::observation_source_with_model_state(
            10,
            ModelColumnState::Present(previous_generation),
            None,
        )],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile observation with output-only MODEL_DATA");

    let contract = compile_observation_transaction(
        &snapshot,
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
    );

    assert!(
        contract.read_set().sources()[0]
            .column_generations()
            .iter()
            .all(|generation| generation.kind() != MsColumnKind::ModelData)
    );
    assert_eq!(
        contract.write_set().model_columns()[0].precondition(),
        ModelColumnPrecondition::Generation(previous_generation)
    );
}

#[test]
fn multi_ms_read_and_write_sets_are_canonical() {
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![
            common::observation_source(12),
            common::observation_source(11),
        ],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile reversed multi-MS observation");

    let contract = compile_observation_transaction(
        &snapshot,
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
    );
    let canonical_sources = snapshot
        .sources()
        .iter()
        .map(|source| source.identity())
        .collect::<Vec<_>>();

    assert_eq!(
        contract
            .read_set()
            .sources()
            .iter()
            .map(|source| source.measurement_set())
            .collect::<Vec<_>>(),
        canonical_sources
    );
    assert_eq!(
        contract
            .write_set()
            .model_columns()
            .iter()
            .map(|write| write.measurement_set())
            .collect::<Vec<_>>(),
        canonical_sources
    );
}
