// SPDX-License-Identifier: LGPL-3.0-or-later
//! The two state owners of an MVC cleaning cycle (#1069).

use super::*;

/// The cube completion and the Taylor PB and admission retained while the
/// ordinary cleaning controller operates on the Taylor completion.
pub(in crate::imaging::cycle) struct MvcState<'a> {
    spec: MvcCycleSpec<'a>,
    cube: Option<MajorCycleCompletion>,
    mean_pb: Vec<f32>,
    taylor_reservation: Reservation,
}

impl<'a> MvcState<'a> {
    /// Reduce the first cube pass to the Taylor state used by the minor cycle.
    pub(in crate::imaging::cycle) fn begin(
        spec: MvcCycleSpec<'a>,
        cube: MajorCycleCompletion,
        run: &Run<'_>,
    ) -> Result<(Self, MajorCycleCompletion), ImagingError> {
        let taylor = crate::mvc::assemble_taylor_completion(
            crate::mvc::TaylorAssemblyInputs {
                cube_problem: run.problem,
                cube: &cube,
                taylor_problem: spec.taylor_problem,
                expansion_reference_hz: spec.expansion_reference_hz,
                controls: spec.controls,
                centres: spec.centres,
            },
            (&run.host, &run.policy, &run.cancel),
        )?;
        Ok((
            Self {
                spec,
                cube: Some(cube),
                mean_pb: taylor.value.primary_beam,
                taylor_reservation: taylor.reservation,
            },
            taylor.value.scientific,
        ))
    }

    /// Apply a Taylor minor update, predict it through the cube, then reduce
    /// the refreshed cube residual back around the same Taylor model.
    pub(super) fn refresh(
        &mut self,
        run: &mut Run<'_>,
        taylor: MajorCycleCompletion,
        (terms, results): (Vec<ModelDeltaTerm>, Reservation),
        last: bool,
    ) -> Result<MajorCycleCompletion, ImagingError> {
        let shape = taylor.normal_state().shape();
        let cells = shape[0]
            .checked_mul(shape[1])
            .ok_or(ImagingError::Unsupported {
                reason: "MVC Taylor model shape exceeds addressable size",
            })?;
        let lifecycle =
            ModelLifecycle::new(self.spec.taylor_problem, ModelStoragePlan::resident(cells)?);
        let (old_normal, model) = taylor.into_parts();
        drop(old_normal);
        let prepared = lifecycle.prepare_final_model(model, terms)?;
        let cube = self.cube.as_ref().ok_or(ImagingError::Unsupported {
            reason: "MVC cube state is absent",
        })?;
        let projected = crate::mvc::project_mvc_model(
            crate::mvc::MvcModelProjectionInputs {
                cube_problem: run.problem,
                cube,
                cube_lifecycle: &run.lifecycle,
                taylor_problem: self.spec.taylor_problem,
                taylor_model: prepared.generation(),
                mean_pb: &self.mean_pb,
                expansion_reference_hz: self.spec.expansion_reference_hz,
                controls: self.spec.controls,
                centres: self.spec.centres,
            },
            (&run.host, &run.policy, &run.cancel),
        )?;
        let cube = self.cube.take().expect("the cube was borrowed above");
        let major = run.refresh_with_projected_model(cube, projected, last)?;
        let taylor = crate::mvc::assemble_taylor_with_model(
            crate::mvc::TaylorAssemblyInputs {
                cube_problem: run.problem,
                cube: &major.completion,
                taylor_problem: self.spec.taylor_problem,
                expansion_reference_hz: self.spec.expansion_reference_hz,
                controls: self.spec.controls,
                centres: self.spec.centres,
            },
            Some(prepared),
            (&run.host, &run.policy, &run.cancel),
        )?;
        // The minor result's charge covers its queued Taylor update until
        // assembly has completed and released the prepared model.
        drop(results);
        self.cube = Some(major.completion);
        self.mean_pb = taylor.value.primary_beam;
        self.taylor_reservation = taylor.reservation;
        Ok(taylor.value.scientific)
    }

    /// Return the cube as the major result and the Taylor family with its
    /// own admission for staged publication.
    pub(super) fn finish(
        mut self,
        taylor: MajorCycleCompletion,
    ) -> (MajorCycleCompletion, Admitted<crate::mvc::TaylorCompletion>) {
        let cube = self.cube.take().expect("an MVC cycle retains its cube");
        (
            cube,
            Admitted {
                value: crate::mvc::TaylorCompletion {
                    scientific: taylor,
                    primary_beam: self.mean_pb,
                },
                reservation: self.taylor_reservation,
            },
        )
    }
}

impl Run<'_> {
    /// Refresh a cube using a separately admitted model projected from its
    /// Taylor minor cycle, rather than applying cube-space sparse terms.
    fn refresh_with_projected_model(
        &mut self,
        completion: MajorCycleCompletion,
        projected: Admitted<ModelGeneration>,
        last: bool,
    ) -> Result<Major, ImagingError> {
        let (normal, old_model) = completion.into_parts();
        drop(old_model);
        let model = self.lifecycle.prepare_final_model(projected.value, [])?;
        let Admitted {
            value: plan,
            reservation: mut pass,
        } = self.admit_pass(
            ModeSet::DATA,
            true,
            self.write_bytes(last),
            self.writes(last),
        )?;
        pass.join(projected.reservation);
        let cycle = MajorCycle::refresh(
            self.problem,
            normal,
            model,
            self.normal_storage(plan.residency)?,
        )?;
        let major = self.reconcile(cycle, ModeSet::DATA, true, last, plan)?;
        self.retain(&major, pass)?;
        Ok(major)
    }
}
