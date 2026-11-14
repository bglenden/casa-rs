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
        run: &mut Run<'_>,
    ) -> Result<(Self, MajorCycleCompletion), ImagingError> {
        let (cube, model, seed_charge) = seed(spec, run, cube)?;
        let taylor = crate::mvc::assemble_taylor_with_model(
            crate::mvc::TaylorAssemblyInputs {
                cube_problem: run.problem,
                cube: &cube,
                taylor_problem: spec.taylor_problem,
                expansion_reference_hz: spec.expansion_reference_hz,
                controls: spec.controls,
                centres: spec.centres,
            },
            model,
            spec.start.and_then(|start| start.saved_psf.as_ref()),
            (&run.host, &run.policy, &run.cancel),
        )?;
        drop(seed_charge);
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
        let saved = self.spec.start.and_then(|start| start.saved_psf.as_ref());
        let saved_read = saved
            .map(|saved| run.admit("saved MVC cube PB", saved.cube_pb.reading_bytes()))
            .transpose()?;
        let saved_cube_pb = saved
            .map(|saved| saved.cube_pb.open())
            .transpose()
            .map_err(ImagingError::Existing)?;
        let projected = crate::mvc::project_mvc_model(
            crate::mvc::MvcModelProjectionInputs {
                cube_problem: run.problem,
                cube,
                cube_lifecycle: &run.lifecycle,
                taylor_problem: self.spec.taylor_problem,
                taylor_model: prepared.generation(),
                mean_pb: &self.mean_pb,
                saved_pb: saved_cube_pb.as_ref(),
                expansion_reference_hz: self.spec.expansion_reference_hz,
                controls: self.spec.controls,
                centres: self.spec.centres,
            },
            (&run.host, &run.policy, &run.cancel),
        )?;
        drop(saved_cube_pb);
        drop(saved_read);
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
            saved,
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

/// Seed the first cube residual from the explicit Taylor images and
/// previously published beams, before the minor cycle examines it.
fn seed(
    spec: MvcCycleSpec<'_>,
    run: &mut Run<'_>,
    cube: MajorCycleCompletion,
) -> Result<
    (
        MajorCycleCompletion,
        Option<casa_imaging_deconvolution::PreparedFinalModel>,
        Reservation,
    ),
    ImagingError,
> {
    let Some(start) = spec.start else {
        return Ok((cube, None, Reservation::none()));
    };
    let [nx, ny] = cube.normal_state().shape();
    let lifecycle = ModelLifecycle::new(spec.taylor_problem, ModelStoragePlan::resident(nx * ny)?);
    let held = run.admit("MVC start model", lifecycle.resident_bytes())?;
    let model = {
        let _reading = run.admit("MVC start model regrid", start.model.seeding_bytes())?;
        let samples = start
            .model
            .main_samples()
            .map_err(ImagingError::StartModel)?;
        run.start_model_flux =
            super::super::seeding::plane_fluxes(spec.taylor_problem, &start.model, &samples);
        lifecycle.prepare_final_model(lifecycle.initial_from_start_model(&samples)?, [])?
    };
    let Some((cube_pb, taylor_pb)) = &start.projection_beams else {
        return Ok((cube, Some(model), held));
    };
    let projected = {
        // CASA reads the previous run's PB cube and the Taylor PB. Keep
        // just the latter and one cube channel in the projection workspace.
        let cells = (nx * ny) as u64;
        let _reading = run.admit(
            "saved MVC primary beams",
            cells * (2 * size_of::<f32>()) as u64
                + taylor_pb.reading_bytes()
                + cube_pb.reading_bytes(),
        )?;
        let mean_pb = taylor_pb
            .open()
            .map_err(ImagingError::Existing)?
            .get()
            .map_err(ImagingError::Existing)?
            .into_iter()
            .collect::<Vec<_>>();
        let cube_pb = cube_pb.open().map_err(ImagingError::Existing)?;
        crate::mvc::project_mvc_model(
            crate::mvc::MvcModelProjectionInputs {
                cube_problem: run.problem,
                cube: &cube,
                cube_lifecycle: &run.lifecycle,
                taylor_problem: spec.taylor_problem,
                taylor_model: model.generation(),
                mean_pb: &mean_pb,
                saved_pb: Some(&cube_pb),
                expansion_reference_hz: spec.expansion_reference_hz,
                controls: spec.controls,
                centres: spec.centres,
            },
            (&run.host, &run.policy, &run.cancel),
        )?
    };
    let seeded = run.refresh_with_projected_model(cube, projected, false)?;
    Ok((seeded.completion, Some(model), held))
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
