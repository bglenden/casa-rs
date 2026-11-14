// SPDX-License-Identifier: LGPL-3.0-or-later
//! BeamSkyJones's ordered angle cache and PBMath1D's circular-beam taper.
use std::sync::Mutex;

use casa_components::{ComponentError, Polarisation, stokes_in};
use casa_types::measures::{EpochRef, MEpoch, MeasFrame, direction::MDirection};
use num_complex::Complex64;

use super::{BeamPattern, PrimaryBeam, separation_rad};

pub(super) type Config = (PrimaryBeam, f64, f64);

#[derive(Clone, Copy, Debug, Default)]
struct History {
    key: Option<(usize, usize)>,
    time_s: Option<f64>,
    angle: Option<f64>,
}

/// A job's starting state, captured in row order before numerical workers run.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BeamContext {
    history: History,
    key: (usize, usize),
}

pub(super) struct Squint {
    pub(super) config: Config,
    pattern: BeamPattern,
    history: Mutex<History>,
    workspaces: Vec<Mutex<Vec<Complex64>>>,
    channels: usize,
}

impl std::fmt::Debug for Squint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Squint")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

fn error(reason: impl ToString) -> ComponentError {
    ComponentError::Visibility {
        shape: "component beam",
        reason: reason.to_string(),
    }
}

impl BeamContext {
    pub(super) fn advance(
        &mut self,
        time_s: f64,
        phase: &MDirection,
        frame: &MeasFrame,
        increment: f64,
    ) -> Result<f64, ComponentError> {
        let changed = self.history.key != Some(self.key) || self.history.angle.is_none();
        if changed || (self.history.time_s != Some(time_s) && increment < std::f64::consts::TAU) {
            let frame = frame
                .clone()
                .with_epoch(MEpoch::from_mjd(time_s / 86400., EpochRef::UTC));
            let angle = f64::from(phase.parallactic_angle(&frame).map_err(error)? as f32);
            if changed
                || self
                    .history
                    .angle
                    .is_some_and(|previous| (angle - previous).abs() > increment)
            {
                self.history.angle = Some(angle);
            }
        }
        self.history.key = Some(self.key);
        self.history.time_s = Some(time_s);
        Ok(self
            .history
            .angle
            .expect("the first row establishes its feed angle"))
    }
}

impl Squint {
    pub(super) fn bytes(workers: usize, channels: usize) -> u64 {
        size_of::<Self>() as u64
            + BeamPattern::BYTES
            + (workers
                * (size_of::<Mutex<Vec<Complex64>>>() + channels * 4 * size_of::<Complex64>()))
                as u64
    }

    pub(super) fn resident_bytes(&self) -> u64 {
        Self::bytes(self.workspaces.len(), self.channels)
    }

    pub(super) fn new(
        config: Config,
        pattern: &BeamPattern,
        workers: usize,
        channels: usize,
    ) -> Self {
        Self {
            config,
            pattern: pattern.clone(),
            history: Mutex::default(),
            workspaces: (0..workers)
                .map(|_| Mutex::new(vec![Complex64::new(0., 0.); channels * 4]))
                .collect(),
            channels,
        }
    }

    pub(super) fn workspace(
        &self,
        worker: usize,
    ) -> Result<std::sync::MutexGuard<'_, Vec<Complex64>>, ComponentError> {
        self.workspaces[worker].lock().map_err(error)
    }

    pub(super) fn prepare_job(
        &self,
        key: (usize, usize),
        times: impl Iterator<Item = f64>,
        phase: &MDirection,
        frame: &MeasFrame,
    ) -> Result<BeamContext, ComponentError> {
        let mut history = self.history.lock().map_err(error)?;
        let initial = BeamContext {
            history: *history,
            key,
        };
        let mut context = initial;
        for time in times {
            context.advance(time, phase, frame, self.config.2)?;
        }
        *history = context.history;
        Ok(initial)
    }

    pub(super) fn flux(
        &self,
        circular: [Complex64; 4],
        direction: [f64; 2],
        pointing: [f64; 2],
        angle: f64,
        frequency: f64,
        basis: Polarisation,
    ) -> [Complex64; 4] {
        let powers = [true, false].map(|right| {
            let [dx, dy] =
                self.config
                    .0
                    .squint_displacement_rad(self.config.1, frequency, angle, right);
            let shifted = shifted(pointing, dx, dy);
            f64::from(
                self.pattern
                    .power_at(separation_rad(direction, shifted), frequency),
            )
        });
        let centre = f64::from(
            self.pattern
                .power_at(separation_rad(direction, pointing), frequency),
        );
        let [rr, rl, lr, ll] = circular;
        let [rr, ll] = [rr.re * powers[0], ll.re * powers[1]];
        stokes_in(
            basis,
            [
                (rr + ll) / 2.,
                (rl.re + lr.re) * centre / 2.,
                (rl.im - lr.im) * centre / 2.,
                (rr - ll) / 2.,
            ],
        )
    }
}

/// MVDirection::shift(dx,dy,true): Euler(-dx,Z,dec+dy,Y,-ra,Z).
fn shifted([ra, dec]: [f64; 2], dx: f64, dy: f64) -> [f64; 2] {
    let (sin, cos) = (dec + dy).sin_cos();
    [ra + dx.sin().atan2(dx.cos() * cos), (dx.cos() * sin).asin()]
}
