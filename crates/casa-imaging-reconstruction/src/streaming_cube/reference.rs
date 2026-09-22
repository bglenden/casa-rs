// SPDX-License-Identifier: LGPL-3.0-or-later

//! Test-only access to the historical operator. Delete at cube cutover.

use super::*;

pub(crate) struct Reference(SpectralSlabOperator);

pub(crate) struct ReferenceGrids {
    pub(crate) dirty: Vec<Array2<Complex64>>,
    pub(crate) residual: Vec<Array2<Complex64>>,
    pub(crate) psf: Vec<Array2<Complex64>>,
    pub(crate) sum_weight: Vec<f64>,
    pub(crate) mapped: Vec<u64>,
}

impl Reference {
    pub(crate) fn finish_images(self, model: ModelGenerationId) -> SpectralOperatorPrimitives {
        self.0
            .finish_bound(Some(ReconstructionModelBinding::Evaluated(model)))
            .unwrap()
            .promote_major_cycle_residual(model)
            .unwrap()
    }

    pub(crate) fn new(model: &[Complex64]) -> Self {
        let mut operator = tests::cube_operator(
            SpectralSlabPlan::compile(4, 0, 4, SpectralKernel::Linear).unwrap(),
        );
        operator.prepare_prediction_grid(model).unwrap();
        operator.residual_grids = Some(vec![Array2::zeros((10, 10)); 4]);
        operator.residual_compensations = Some(vec![Array2::zeros((10, 10)); 4]);
        Self(operator)
    }

    pub(crate) fn predict(
        &mut self,
        uvw_m: [f64; 3],
        phase_shift_m: f64,
        term: casa_imaging_model::SelectedSpectralContribution,
    ) -> Complex64 {
        self.0
            .predict_one(
                SpectralOperatorSample::new(
                    term.output_channel() as usize,
                    uvw_m,
                    term.evaluation_frequency_hz(),
                    phase_shift_m,
                    [0.0; 2],
                    1.0,
                    term.factor(),
                )
                .unwrap(),
            )
            .unwrap()
    }

    pub(crate) fn push(
        &mut self,
        uvw_and_phase: ([f64; 3], f64),
        fine: crate::spectral_sampling::CasaLinearSample,
        observed: Complex64,
        predicted: Complex64,
        weight: f64,
    ) {
        let sample = SpectralOperatorSample::new(
            fine.output_channel(),
            uvw_and_phase.0,
            fine.frequency_hz(),
            uvw_and_phase.1,
            [observed.re, observed.im],
            weight,
            1.0,
        )
        .unwrap();
        self.0
            .push_with_residual_polarization(sample, predicted, 0)
            .unwrap();
    }

    pub(crate) fn finish(self) -> ReferenceGrids {
        ReferenceGrids {
            dirty: self.0.dirty_grids.unwrap(),
            residual: self.0.residual_grids.unwrap(),
            psf: self.0.psf_grids.unwrap(),
            sum_weight: self.0.sum_weights,
            mapped: self.0.mapped_samples,
        }
    }
}
