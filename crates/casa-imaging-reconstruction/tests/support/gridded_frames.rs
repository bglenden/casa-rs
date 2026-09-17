// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::CompiledProblem;
use casa_imaging_reconstruction::runtime_adapter::{
    GriddedNormalCompilationPlan, GriddedNormalOperatorFrame, gridded_normal_operator_record_bytes,
};

#[derive(Debug, PartialEq, Eq)]
pub struct RecordedFrame {
    sequence: u64,
    record_count: u64,
    encoded: Vec<u8>,
}

impl From<GriddedNormalOperatorFrame<'_>> for RecordedFrame {
    fn from(frame: GriddedNormalOperatorFrame<'_>) -> Self {
        Self {
            sequence: frame.sequence(),
            record_count: frame.record_count(),
            encoded: frame.encoded_bytes().to_vec(),
        }
    }
}

impl RecordedFrame {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn record_count(&self) -> u64 {
        self.record_count
    }
    pub fn encoded_bytes(&self) -> &[u8] {
        &self.encoded
    }
}

pub fn compilation_plan(
    problem: &CompiledProblem,
    maximum_source_samples: usize,
    total_source_samples: usize,
) -> GriddedNormalCompilationPlan {
    let atom = GriddedNormalCompilationPlan::maximum_atom_records(problem).expect("atom bound");
    let record_capacity = maximum_source_samples
        .checked_mul(atom)
        .expect("test record capacity");
    let record_bytes = gridded_normal_operator_record_bytes(problem).expect("record width");
    let frame_header_bytes = 32;
    // Finite capacity for these fixtures, not a general fine-grid expansion bound.
    let artifact_atom_capacity = total_source_samples
        .checked_mul(problem.geometry().spectral().output_channels().max(1))
        .expect("test atom ceiling");
    let maximum_artifact_bytes = artifact_atom_capacity
        .checked_mul(atom.checked_mul(record_bytes).unwrap() + frame_header_bytes)
        .and_then(|bytes| u64::try_from(bytes.max(1)).ok())
        .expect("finite test artifact ceiling");
    GriddedNormalCompilationPlan::new(
        problem,
        maximum_source_samples,
        record_capacity,
        record_capacity,
        maximum_artifact_bytes,
        frame_header_bytes,
    )
    .expect("bounded test compiler plan")
}
