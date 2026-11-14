// SPDX-License-Identifier: LGPL-3.0-or-later
//! Grids retained or prepared for a simulation pointing.
use super::*;

impl Prediction {
    /// Index the grids by antenna beam direction, independently of FIELD.
    pub(crate) fn grid_id(&self, scan: &super::super::observe::Scan) -> usize {
        match &self.grids {
            StepGrids::Prepared(..) => 0,
            StepGrids::Jones(held) => held
                .visits
                .binary_search_by_key(&image_beams::visit_key(scan), |(key, _)| *key)
                .map(|index| held.visits[index].1)
                .expect("prepared Jones visit"),
            StepGrids::PerField(held) => held
                .centres
                .binary_search_by_key(&scan.pointing_rad.map(f64::to_bits), |(direction, _)| {
                    *direction
                })
                .expect("prepared pointing"),
        }
    }

    /// Where the grids rows of `field` are predicted from come from; `None`
    /// for a field the step did not observe.
    pub(crate) fn grids_for(&self, field: usize) -> Option<FieldGrids<'_>> {
        match &self.grids {
            StepGrids::Prepared(grid_field, grids) => grid_field
                .is_none_or(|grid_field| grid_field == field)
                .then_some(FieldGrids::Prepared(grids)),
            StepGrids::PerField(held) => held.centres.get(field).map(|_| FieldGrids::PerField),
            StepGrids::Jones(held) => held.centres.get(field).map(|_| FieldGrids::PerField),
        }
    }

    /// Bytes the grids prepared at `predict` hold: one set, or none when
    /// `close` prepares each field's.
    pub(crate) fn prepared_bytes(&self) -> u64 {
        match &self.grids {
            StepGrids::Prepared(_, grids) => grids.bytes(),
            StepGrids::PerField(_) | StepGrids::Jones(_) => 0,
        }
    }

    /// Whether `close` prepares each field's grids
    /// ([`Self::prepare_field`]).
    pub(crate) fn per_field(&self) -> bool {
        matches!(self.grids, StepGrids::PerField(_) | StepGrids::Jones(_))
    }

    /// Bytes one set of grids holds ([`PreparedModelGrids::bytes`]).
    pub(crate) fn set_bytes(&self) -> u64 {
        self.model.grid_bytes()
    }

    /// Bytes preparing a field's grids holds beside them on `workers`
    /// workers: a plane's scratch for each worker that has a plane
    /// ([`ModelOperator::plane_scratch_bytes`]); none when `close` prepares
    /// no grids.
    pub(crate) fn preparation_bytes(&self, workers: usize) -> u64 {
        match &self.grids {
            StepGrids::Prepared(..) => 0,
            StepGrids::Jones(held) => {
                workers.clamp(1, held.model.planes().max(1)) as u64
                    * self.model.jones_scratch_bytes(&held.model)
            }
            StepGrids::PerField(held) => {
                workers.clamp(1, held.model.planes().max(1)) as u64
                    * self.model.plane_scratch_bytes(&held.model)
            }
        }
    }

    /// Prepare the grids of `field`, which the step observed under the
    /// beam, on `team`: the planes held, multiplied by the beam's power
    /// centred on the field and transformed, as `predict` prepares one
    /// field's ([`ModelOperator::prepare`]); the same grids whatever the
    /// workers.
    ///
    /// # Errors
    ///
    /// A raised error for a field the step did not observe or a step whose
    /// grids were prepared at `predict`, and the operator's refusal, which
    /// for a model `predict` read is only a transform's failure.
    pub(crate) fn prepare_field(
        &self,
        field: usize,
        team: &WorkerTeam,
    ) -> SimulatorResult<PreparedModelGrids> {
        if let StepGrids::Jones(held) = &self.grids {
            let (key, angle) = held
                .centres
                .get(field)
                .ok_or_else(|| SimulatorError::raised("Jones pointing was not prepared"))?;
            return self
                .model
                .prepare_jones(
                    &held.model,
                    &held.image,
                    key.0.map(f64::from_bits),
                    *angle,
                    team,
                )
                .map_err(model_error);
        }
        let StepGrids::PerField(held) = &self.grids else {
            return Err(SimulatorError::raised(
                "the step's grids were prepared at predict",
            ));
        };
        let centre = held
            .centres
            .get(field)
            .map(|(_, centre)| *centre)
            .ok_or_else(|| {
                SimulatorError::raised(format!("predict did not observe field {field}"))
            })?;
        self.model
            .prepare(&held.model, Some((&held.pattern, centre)), Some(team))
            .map_err(model_error)
    }
}
