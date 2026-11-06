// SPDX-License-Identifier: LGPL-3.0-or-later

//! Counting law for MVC's first and second cube-plane passes (#1069).

use std::borrow::Cow;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use casa_imaging_deconvolution::{
    MajorCycle, ModelLifecycle, ModelStoragePlan, NormalArrayStorage, NormalStateError,
    NormalStorageFactory, NormalStoragePlan,
};
use casa_imaging_products::{read_mvc_cube_plane, read_mvc_cube_primary_beam};

use super::*;

#[derive(Debug, Default)]
struct Counts {
    calls: AtomicUsize,
    scalars: AtomicUsize,
}

#[derive(Debug)]
struct Factory(Arc<Counts>);

impl NormalStorageFactory for Factory {
    fn create(
        &self,
        _domain: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, NormalStateError> {
        Ok(Box::new(Storage {
            values: vec![0.0; scalars],
            counts: Arc::clone(&self.0),
        }))
    }
}

#[derive(Debug)]
struct Storage {
    values: Vec<f64>,
    counts: Arc<Counts>,
}

impl NormalArrayStorage for Storage {
    fn len(&self) -> usize {
        self.values.len()
    }
    fn read(&self, start: usize, len: usize) -> Result<Cow<'_, [f64]>, NormalStateError> {
        let values = self
            .values
            .get(start..start + len)
            .ok_or(NormalStateError::InvalidSlab)?;
        self.counts.calls.fetch_add(1, Ordering::Relaxed);
        self.counts.scalars.fetch_add(len, Ordering::Relaxed);
        Ok(Cow::Borrowed(values))
    }
    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), NormalStateError> {
        self.values
            .get_mut(start..start + values.len())
            .ok_or(NormalStateError::InvalidSlab)?
            .copy_from_slice(values);
        Ok(())
    }
}

/// An analytic-PB first pass reads no normal image arrays. The second pass
/// reads only the requested channel's PSF and residual, once each.
#[test]
fn mvc_cube_reads_are_bounded_to_the_fields_each_pass_uses() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
    ];
    let problem = continuum_problem_with_reconstruction(
        214,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 3 },
        ReconstructionAlgorithm::Dirty,
        3,
    );
    let scene = Scene::new(&problem).with_point(0, CENTRE, &[1.0, 1.0, 1.0]);
    let lifecycle = ModelLifecycle::new(&problem, ModelStoragePlan::resident(usize::MAX).unwrap());
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let counts = Arc::new(Counts::default());
    let storage = NormalStoragePlan::new(Arc::new(Factory(Arc::clone(&counts))), 3).unwrap();
    let mut cycle = MajorCycle::initial(&problem, model, storage).unwrap();
    {
        let (model, mut writer) = cycle.parts();
        writer.append(scene.pass_images(0, model, true)).unwrap();
    }
    let completion = cycle.finish(2, 1).unwrap();
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &completion);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let plan = planned_for(&inputs, &controls);
    counts.calls.store(0, Ordering::Relaxed);
    counts.scalars.store(0, Ordering::Relaxed);
    let beam = read_mvc_cube_primary_beam(&plan, &inputs, 1).unwrap();
    assert_eq!(beam.sum_weight, 2.0);
    assert_eq!(counts.calls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.scalars.load(Ordering::Relaxed), 0);
    let plane = read_mvc_cube_plane(&plan, &inputs, 1).unwrap();
    assert_eq!(plane.primary_beam, beam.pixels);
    assert_eq!(counts.calls.load(Ordering::Relaxed), 2);
    assert_eq!(
        counts.scalars.load(Ordering::Relaxed),
        2 * 2 * SHAPE[0] * SHAPE[1]
    );
    counts.calls.store(0, Ordering::Relaxed);
    counts.scalars.store(0, Ordering::Relaxed);
    let cells = SHAPE[0] * SHAPE[1];
    let mut mean = vec![0.0; cells];
    let mut psf = vec![0.0; 3 * cells];
    let mut residual = vec![0.0; 2 * cells];
    let mut sumwt = [0.0; 3];
    casa_imaging_products::reduce_mvc_cube(
        &plan,
        &inputs,
        1.4e9,
        &mut mean,
        &mut psf,
        &mut residual,
        &mut sumwt,
    )
    .unwrap();
    assert_eq!(counts.calls.load(Ordering::Relaxed), 3 * 2);
    assert_eq!(counts.scalars.load(Ordering::Relaxed), 3 * 2 * 2 * cells);
}
