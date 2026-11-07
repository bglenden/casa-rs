// SPDX-License-Identifier: LGPL-3.0-or-later
//! Product publication of channel-local Float-backed normal planes.

use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use casa_imaging_deconvolution::{
    MajorCycle, ModelLifecycle, ModelStoragePlan, NormalArrayStorage, NormalStateError,
    NormalStorageFactory, NormalStoragePlan,
};

use super::*;

#[derive(Debug, Default)]
struct Reads {
    real: Vec<(usize, usize, usize)>,
    widened: Vec<(usize, usize, usize)>,
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Real,
    Widened,
    Error(usize),
}

#[derive(Debug)]
struct Factory {
    reads: Arc<Mutex<Reads>>,
    mode: Mode,
}

#[derive(Debug)]
struct FloatBacking {
    values: Box<[f32]>,
    slot: usize,
    reads: Arc<Mutex<Reads>>,
    mode: Mode,
}

impl NormalStorageFactory for Factory {
    fn create(
        &self,
        domain: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, NormalStateError> {
        assert!(scalars.is_multiple_of(2));
        Ok(Box::new(FloatBacking {
            values: vec![0.0; scalars / 2].into(),
            slot: domain,
            reads: self.reads.clone(),
            mode: self.mode,
        }))
    }
}

impl NormalArrayStorage for FloatBacking {
    fn len(&self) -> usize {
        self.values.len() * 2
    }
    fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, NormalStateError> {
        self.reads
            .lock()
            .unwrap()
            .widened
            .push((self.slot, start, len));
        Ok(Cow::Owned(
            (start..start + len)
                .map(|i| {
                    if i.is_multiple_of(2) {
                        f64::from(self.values[i / 2])
                    } else {
                        0.0
                    }
                })
                .collect(),
        ))
    }
    fn read_real(
        &self,
        start: usize,
        values: usize,
    ) -> Result<Option<Cow<'_, [f32]>>, NormalStateError> {
        self.reads
            .lock()
            .unwrap()
            .real
            .push((self.slot, start, values));
        match self.mode {
            Mode::Real => Ok(Some(Cow::Borrowed(
                &self.values[start / 2..start / 2 + values],
            ))),
            Mode::Widened => Ok(None),
            Mode::Error(slot) if self.slot == slot => Err(NormalStateError::NormalStorage(
                "injected real-read error".into(),
            )),
            Mode::Error(_) => Ok(Some(Cow::Borrowed(
                &self.values[start / 2..start / 2 + values],
            ))),
        }
    }
    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), NormalStateError> {
        for (i, value) in values.iter().enumerate().step_by(2) {
            self.values[(start + i) / 2] = *value as f32;
        }
        Ok(())
    }
    fn write_real(&mut self, start: usize, values: &[f32]) -> Result<(), NormalStateError> {
        self.values[start / 2..start / 2 + values.len()].copy_from_slice(values);
        Ok(())
    }
}

fn completion(
    problem: &CompiledProblem,
    mode: Mode,
    reads: Arc<Mutex<Reads>>,
) -> MajorCycleCompletion {
    let scene = Scene::new(problem).with_point(0, CENTRE, &[1.0, -0.5]);
    let lifecycle = ModelLifecycle::new(problem, ModelStoragePlan::resident(usize::MAX).unwrap());
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let storage = NormalStoragePlan::new(Arc::new(Factory { reads, mode }), 1).unwrap();
    let mut cycle = MajorCycle::initial(problem, model, storage).unwrap();
    {
        let (model, mut state) = cycle.parts();
        let all = scene.pass_images(0, model, true);
        // The production normal owner accepts one bounded channel at a time.
        let cells = SHAPE[0] * SHAPE[1];
        for channel in 0..2 {
            let range = channel * cells..(channel + 1) * cells;
            state
                .append(casa_imaging_deconvolution::PassImages {
                    domain: 0,
                    shape: SHAPE,
                    channels: channel..channel + 1,
                    polarizations: 1,
                    residual: all.residual[range.clone()].to_vec(),
                    psf: Some(all.psf.as_ref().unwrap()[range].to_vec()),
                    sum_weights: vec![all.sum_weights[channel]],
                    published_sum_weights: vec![all.published_sum_weights[channel]],
                    weight: None,
                })
                .unwrap();
        }
    }
    cycle.finish(2, 1).unwrap()
}

#[test]
fn product_publication_reads_float_planes_without_widening() {
    let problem = continuum_problem_with_reconstruction(
        81,
        &[ProductKind::Psf, ProductKind::Residual],
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let reads = Arc::new(Mutex::new(Reads::default()));
    let real = completion(&problem, Mode::Real, reads.clone());
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &real);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    *reads.lock().unwrap() = Reads::default();
    let products = generate_for(&planned, &inputs);
    let observed = reads.lock().unwrap();
    let cells = SHAPE[0] * SHAPE[1];
    for slot in [0, 1] {
        assert!(
            observed.real.contains(&(slot, 2 * cells, cells)),
            "reads channel 1 of the residual and PSF as Float planes: {observed:?}"
        );
    }
    assert!(
        observed.widened.is_empty(),
        "does not widen Float planes: {observed:?}"
    );
    println!("Float-backed product reads: {observed:?}");
    drop(observed);
    let fallback_reads = Arc::new(Mutex::new(Reads::default()));
    let fallback = completion(&problem, Mode::Widened, fallback_reads.clone());
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &fallback);
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    *fallback_reads.lock().unwrap() = Reads::default();
    let reference = generate_for(&planned, &inputs);
    let fallback_observed = fallback_reads.lock().unwrap();
    assert!(
        !fallback_observed.widened.is_empty(),
        "a non-Float backing uses the complex fallback"
    );
    println!("Complex-backed product reads: {fallback_observed:?}");
    drop(fallback_observed);
    for (a, b) in products.members().iter().zip(reference.members()) {
        assert_eq!(a.name(), b.name());
        assert_eq!(a.payload(), b.payload());
    }
}

#[test]
fn product_publication_propagates_a_real_plane_read_error() {
    let problem = continuum_problem_with_reconstruction(
        81,
        &[ProductKind::Psf, ProductKind::Residual],
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    for slot in [0, 1] {
        let join = completion(
            &problem,
            Mode::Error(slot),
            Arc::new(Mutex::new(Reads::default())),
        );
        let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join);
        let planned = planned_for(&inputs, &ContinuumProductControls::default());
        let output = ImageOutput::for_problem(&problem);
        let error =
            produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
                .unwrap_err();
        assert!(
            error.to_string().contains("injected real-read error"),
            "{error}"
        );
    }
}
