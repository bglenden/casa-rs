// SPDX-License-Identifier: LGPL-3.0-or-later
//! Antenna-based corruption: CASA's `G` (`sm.setgain`) and `D`
//! (`sm.setleakage`) terms, simulated as CASA's `SolvableVisCal` simulates
//! them and applied by `corrupt` as `VisEquation::collapseForSim` applies
//! them.
//!
//! # What CASA simulates
//!
//! `Simulator::setgain`/`setleakage` build the term through
//! `SolvableVisCal::setSimulate`, which walks the MeasurementSet observed
//! so far in solution chunks and fills one solution per chunk, spectral
//! window and antenna (`CalCorruptor`, `GJonesCorruptor`,
//! `DJonesCorruptor`, measured against CASA 6.7.6, #841 Phase 3):
//! - **G, `mode='fbm'`.** The chunks are the integrations (`setgain`'s
//!   `interval` is not read; its docstring says so). Each antenna and
//!   receptor has an amplitude and a phase screen of fractional Brownian
//!   motion on even slots of 5 s spanning the chunks
//!   (`fBM::initialize`: a power-law spectrum `(k+1)^(-β/2)`, β = 1.1, of
//!   normal amplitudes and uniform phases, transformed to real). A screen
//!   is scaled to the term's scale `s` (`|camp|`, at most 0.9) over its
//!   root-mean-square about its mean, the mean kept; the gain is
//!   `(1 + s·a/rms(a)) · exp(i·π·s·p/rms(p))` (CASA writes π as 3.141592,
//!   2·10⁻⁷ short), and a chunk takes the slot nearest its time.
//! - **G, `mode='random'`.** The chunks are the integrations; each draws
//!   `camp.re·n₁ + i·camp.im·n₂` per antenna and receptor.
//! - **D.** The interval is infinite: one chunk per spectral window,
//!   spanning every scan and field (CASA's table has one solution per
//!   window and antenna). Each takes, per antenna and receptor,
//!   `d = camp.re·n₁ + i·camp.im·n₂ + offset`, the second receptor's
//!   offset with its real part negated.
//!
//! `corrupt` applies a term's solution nearest in time within the row's
//! spectral window (`tInterpType = "nearest"`):
//! - G multiplies each correlation by `g_p[r₁] · conj(g_q[r₂])`, `r₁ r₂`
//!   its receptors, an autocorrelation included;
//! - D is CASA's linearised leakage (`JonesGenLin`): the cross hands
//!   become `XY + d_px·YY + conj(d_qy)·XX` and `YX + d_py·XX +
//!   conj(d_qx)·YY`, the parallel hands are left as they are, and CASA
//!   raises on data without the cross hands.
//!
//! # Where casa-rs differs
//!
//! The draws are casa-rs's, from Philox streams of their own purpose byte
//! (as thermal noise's, [`super`]), so the realisations differ from CASA's
//! and are compared statistically. CASA's defects in the simulation are
//! not followed (`docs/CASA (C++) bugs.md`):
//! - `fBM::initialize` copies a screen's last two or three slots from past
//!   the end of its transform's output, and the screen's normalisation
//!   includes them, so CASA's scale varies from run to run; casa-rs
//!   transforms every slot;
//! - CASA seeds antenna `a`'s second receptor and antenna `a+1`'s first
//!   with the same `seed + a + 1`, so their screens coincide; casa-rs keys
//!   every screen apart;
//! - `random` gains are `1 + camp.re·n₁ + i·camp.im·n₂` in casa-rs; CASA
//!   leaves out the 1, so its gains scatter about zero;
//! - with autocorrelations written, the autocorrelation row resets its
//!   antenna's solution to 1 after the cross rows set it, which leaves the
//!   last antenna with unit gain or a leakage of 1; casa-rs simulates every
//!   antenna;
//! - CASA simulates leakage for the first two spectral windows only and
//!   leaves later windows uncorrupted; casa-rs simulates every window.

use std::ops::Deref;

use casa_fft::RealFft2;
use casa_imaging_runtime::Reservation;
use casa_numerics::cell_noise::philox4x32_10;
use num_complex::{Complex32, Complex64};

/// The purpose byte of fractional-Brownian-motion gain screens' streams.
const PURPOSE_GAIN_SCREEN: u32 = 0x02;

/// The purpose byte of leakages' streams.
const PURPOSE_LEAKAGE: u32 = 0x03;

/// The purpose byte of `setgain(mode='random')` gains' streams.
const PURPOSE_RANDOM_GAIN: u32 = 0x04;

/// `GJones::createCorruptor`'s spectral exponent of the screens.
const SCREEN_BETA: f64 = 1.1;

/// The screens' slot width: `max(interval, 5 s)`, the interval being 0 for
/// solutions per integration.
pub(crate) const SCREEN_SLOT_S: f64 = 5.0;

/// The largest gain-fluctuation scale `GJones::createCorruptor` keeps.
pub(crate) const MAXIMUM_GAIN_SCALE: f64 = 0.9;

/// Which CASA calibration type a term is. Within one `corrupt`, CASA
/// corrupts the model from the sky side in: D before G.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum JonesKind {
    /// `G Jones`: antenna gains.
    Gain,
    /// `D`: polarization leakage.
    Leakage,
}

/// One simulated antenna-based term: per spectral window, its solutions in
/// time order, each a block of one value per antenna and receptor.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JonesTerm {
    pub(crate) kind: JonesKind,
    antennas: usize,
    /// `[window]`: (solution time, block) in time order.
    solutions: Vec<Vec<(f64, u32)>>,
    /// `[block · antennas + antenna]`: the receptors' values.
    values: Vec<[Complex32; 2]>,
}

impl JonesTerm {
    /// Heap bytes the term keeps.
    pub(crate) fn bytes(&self) -> u64 {
        self.solutions.capacity() as u64 * WINDOW_BYTES
            + self
                .solutions
                .iter()
                .map(|times| times.capacity() as u64 * SOLUTION_BYTES)
                .sum::<u64>()
            + self.values.capacity() as u64 * VALUE_BYTES
    }

    /// Whether the term has a solution in spectral window `window`: CASA
    /// does not corrupt a window it has none for.
    pub(crate) fn covers(&self, window: usize) -> bool {
        self.solutions
            .get(window)
            .is_some_and(|times| !times.is_empty())
    }

    /// The receptors' values of `antenna` at the solution nearest `time_s`
    /// in `window`, the earlier of two as near (`CalCorruptor::setCurrTime`).
    fn at(&self, window: usize, time_s: f64, antenna: usize) -> [Complex32; 2] {
        let times = &self.solutions[window];
        let after = times.partition_point(|&(time, _)| time < time_s);
        let nearest = match (after.checked_sub(1), times.get(after)) {
            (Some(before), Some(&(later, _))) => {
                if (later - time_s).abs() < (time_s - times[before].0).abs() {
                    after
                } else {
                    before
                }
            }
            (Some(before), None) => before,
            (None, _) => 0,
        };
        let block = times[nearest].1 as usize;
        self.values[block * self.antennas + antenna]
    }

    /// Corrupt `cell`, a row of `antennas` at `time_s` in `window`, whose
    /// correlations have the receptors `receptors`, as CASA's `corrupt`
    /// applies the term.
    pub(crate) fn apply(
        &self,
        (window, time_s): (usize, f64),
        (antenna1, antenna2): (usize, usize),
        receptors: &[[usize; 2]],
        cell: &mut [Complex32],
    ) {
        let p = self.at(window, time_s, antenna1);
        let q = self.at(window, time_s, antenna2);
        let correlations = receptors.len();
        match self.kind {
            JonesKind::Gain => {
                // A cell has at most four correlations.
                let mut factors = [Complex32::new(1.0, 0.0); 4];
                for (factor, &[r1, r2]) in factors.iter_mut().zip(receptors) {
                    *factor = p[r1] * q[r2].conj();
                }
                for sample in cell.chunks_exact_mut(correlations) {
                    for (value, factor) in sample.iter_mut().zip(&factors) {
                        *value *= *factor;
                    }
                }
            }
            JonesKind::Leakage => {
                let position = |pair: [usize; 2]| receptors.iter().position(|&r| r == pair);
                // `corrupt` refuses leakage on data without all four
                // correlations.
                let (Some(xx), Some(xy), Some(yx), Some(yy)) = (
                    position([0, 0]),
                    position([0, 1]),
                    position([1, 0]),
                    position([1, 1]),
                ) else {
                    return;
                };
                for sample in cell.chunks_exact_mut(correlations) {
                    let (vxx, vyy) = (sample[xx], sample[yy]);
                    sample[xy] += p[0] * vyy + q[1].conj() * vxx;
                    sample[yx] += p[1] * vxx + q[0].conj() * vyy;
                }
            }
        }
    }
}

/// A term with the memory admitted for what it keeps. The session's terms
/// and the corruptions recorded with them share it, so the charge lasts as
/// long as the term does (#890 review).
#[derive(Debug)]
pub(crate) struct HeldTerm {
    term: JonesTerm,
    _reservation: Reservation,
}

impl HeldTerm {
    /// `term`, holding `reservation`, which covers what it keeps.
    pub(crate) fn new(term: JonesTerm, reservation: Reservation) -> Self {
        debug_assert!(
            term.bytes() <= reservation.memory(),
            "a term keeps at most what was admitted for it"
        );
        Self {
            term,
            _reservation: reservation,
        }
    }
}

impl Deref for HeldTerm {
    type Target = JonesTerm;

    fn deref(&self) -> &JonesTerm {
        &self.term
    }
}

/// Heap bytes of a window's vector of solutions.
const WINDOW_BYTES: u64 = size_of::<Vec<(f64, u32)>>() as u64;
/// Heap bytes of a solution (time, block).
const SOLUTION_BYTES: u64 = size_of::<(f64, u32)>() as u64;
/// Heap bytes of an antenna's receptors' values in a block.
const VALUE_BYTES: u64 = size_of::<[Complex32; 2]>() as u64;

/// The memory a term is admitted for before it is formed: what the term
/// keeps, and what forming it holds beside that until it is formed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TermBytes {
    pub(crate) kept: u64,
    pub(crate) forming: u64,
}

impl TermBytes {
    /// [`fbm_gains`] for chunks counted `per_window` on `slots` slots: it
    /// keeps the solutions and a value per slot and antenna; forming them
    /// holds the chunks, the screens' transform (FFTW's plans, the plane it
    /// plans on, and the spectrum) and an antenna's screens: both
    /// receptors' gains, and a receptor's amplitude and phase with the
    /// phase's samples (32 bytes a slot).
    pub(crate) fn fbm(per_window: &[usize], slots: usize, antennas: usize) -> Self {
        let spectrum = slots / 2 + 1;
        let transform = if slots >= MINIMUM_SCREEN_SLOTS {
            casa_fft::plan_pair_bytes(1) as u64
                + casa_fft::planning_bytes::<f64>(spectrum)
                + (spectrum * size_of::<Complex64>()) as u64
        } else {
            0
        };
        Self {
            kept: solutions_bytes(per_window) + (slots * antennas) as u64 * VALUE_BYTES,
            forming: solutions_bytes(per_window) + transform + 32 * slots as u64,
        }
    }

    /// [`random_gains`] and [`leakages`] for chunks counted `per_window`:
    /// the chunks become the solutions, and each has a value per antenna.
    pub(crate) fn per_chunk(per_window: &[usize], antennas: usize) -> Self {
        let chunks: usize = per_window.iter().sum();
        Self {
            kept: solutions_bytes(per_window) + (chunks * antennas) as u64 * VALUE_BYTES,
            forming: 0,
        }
    }
}

/// Heap bytes of the solutions of chunks counted `per_window`, in vectors
/// sized to them ([`Chunks::with_capacity`]).
fn solutions_bytes(per_window: &[usize]) -> u64 {
    per_window.len() as u64 * WINDOW_BYTES
        + per_window.iter().sum::<usize>() as u64 * SOLUTION_BYTES
}

/// The slots of the screens spanning chunk times `span`
/// (`CalCorruptor::setEvenSlots`).
pub(crate) fn screen_slots(span: Option<[f64; 2]>) -> usize {
    let [start, stop] = span.unwrap_or([0.0, 0.0]);
    (((stop - start) / SCREEN_SLOT_S + 1.0) as usize).max(1)
}

/// The fewest slots a screen has a spectrum for; on fewer CASA's screen is
/// flat.
const MINIMUM_SCREEN_SLOTS: usize = 4;

/// The solution chunks of a term: per spectral window, (time, chunk) in
/// time order, and how many chunks there are.
#[derive(Debug, Clone, Default)]
pub(crate) struct Chunks {
    /// `[window]`: (the chunk's time, chunk) in time order.
    pub(crate) by_window: Vec<Vec<(f64, u32)>>,
    pub(crate) count: u32,
}

impl Chunks {
    /// No chunks yet, with room for those counted `per_window`, so the term
    /// that keeps them as its solutions keeps no spare room.
    pub(crate) fn with_capacity(per_window: &[usize]) -> Self {
        Self {
            by_window: per_window
                .iter()
                .map(|&count| Vec::with_capacity(count))
                .collect(),
            count: 0,
        }
    }

    /// Add a chunk at `time_s` in `window`; chunks are added in time order
    /// within a window.
    pub(crate) fn push(&mut self, window: usize, time_s: f64) -> u32 {
        if self.by_window.len() <= window {
            self.by_window.resize_with(window + 1, Vec::new);
        }
        let chunk = self.count;
        self.by_window[window].push((time_s, chunk));
        self.count += 1;
        chunk
    }

    /// The earliest and latest chunk times.
    fn span(&self) -> Option<[f64; 2]> {
        self.by_window
            .iter()
            .flatten()
            .map(|&(time, _)| time)
            .fold(None, |span, time| {
                Some(span.map_or([time, time], |[low, high]: [f64; 2]| {
                    [low.min(time), high.max(time)]
                }))
            })
    }
}

/// A `setgain(mode='fbm')` term of scale `scale` (`|camp|`, at most
/// [`MAXIMUM_GAIN_SCALE`]) for `antennas` antennas, with the solution
/// `chunks` (one per integration), drawn from `seed`'s streams.
pub(crate) fn fbm_gains(seed: i32, scale: f64, antennas: usize, chunks: Chunks) -> JonesTerm {
    let span = chunks.span();
    let start = span.map_or(0.0, |[start, _]| start);
    let slots = screen_slots(span);
    let mut fft = ScreenFft::new(slots);
    // `[slot · antennas + antenna]`.
    let mut screens = vec![[Complex32::new(1.0, 0.0); 2]; slots * antennas];
    for antenna in 0..antennas {
        let gains = [0_u32, 1].map(|receptor| {
            let counter = |kind: u32| [antenna as u32, receptor | (kind << 8), 0, 0];
            let amplitude = fft.normalised_fbm(seed, counter(0), scale);
            let phase = fft.normalised_fbm(seed, counter(1), scale * std::f64::consts::PI);
            amplitude
                .iter()
                .zip(&phase)
                .map(|(a, p)| {
                    let gain = Complex64::from_polar(1.0 + a, *p);
                    Complex32::new(gain.re as f32, gain.im as f32)
                })
                .collect::<Vec<_>>()
        });
        for (slot, values) in screens.chunks_exact_mut(antennas).enumerate() {
            values[antenna] = [gains[0][slot], gains[1][slot]];
        }
    }
    // Each chunk takes the slot nearest its time, the earlier of two as
    // near (`CalCorruptor::setCurrTime`).
    let slot_of = |time_s: f64| {
        let position = (time_s - start) / SCREEN_SLOT_S - 0.5;
        let slot = (position - 0.5).ceil().max(0.0) as usize;
        slot.min(slots - 1) as u32
    };
    let solutions = chunks
        .by_window
        .iter()
        .map(|times| {
            times
                .iter()
                .map(|&(time, _)| (time, slot_of(time)))
                .collect()
        })
        .collect();
    JonesTerm {
        kind: JonesKind::Gain,
        antennas,
        solutions,
        values: screens,
    }
}

/// A `setleakage` term of `camp` and `offset` for `antennas` antennas, a
/// leakage per chunk (one per spectral window), drawn from `seed`'s
/// streams.
pub(crate) fn leakages(
    seed: i32,
    (camp, offset): (Complex64, Complex64),
    antennas: usize,
    chunks: Chunks,
) -> JonesTerm {
    let mut values = Vec::with_capacity(chunks.count as usize * antennas);
    for chunk in 0..chunks.count {
        for antenna in 0..antennas {
            let draw = |receptor: u32| {
                let [n1, n2] = normal_pair(
                    [seed as u32, PURPOSE_LEAKAGE << 24],
                    [chunk, antenna as u32, receptor, 0],
                );
                let offset = if receptor == 0 {
                    offset
                } else {
                    Complex64::new(-offset.re, offset.im)
                };
                let d = Complex64::new(n1 * camp.re, n2 * camp.im) + offset;
                Complex32::new(d.re as f32, d.im as f32)
            };
            values.push([draw(0), draw(1)]);
        }
    }
    JonesTerm {
        kind: JonesKind::Leakage,
        antennas,
        solutions: chunks.by_window,
        values,
    }
}

/// A `setgain(mode='random')` term of `camp` for `antennas` antennas: per
/// chunk (one per integration), antenna and receptor, the gain
/// `1 + camp.re·n₁ + i·camp.im·n₂`, drawn from `seed`'s streams. CASA's
/// `GJonesCorruptor::simPar` leaves out the 1, so its gains scatter about
/// zero; casa-rs does not follow it (`docs/CASA (C++) bugs.md`).
pub(crate) fn random_gains(
    seed: i32,
    camp: Complex64,
    antennas: usize,
    chunks: Chunks,
) -> JonesTerm {
    let mut values = Vec::with_capacity(chunks.count as usize * antennas);
    for chunk in 0..chunks.count {
        for antenna in 0..antennas {
            let draw = |receptor: u32| {
                let [n1, n2] = normal_pair(
                    [seed as u32, PURPOSE_RANDOM_GAIN << 24],
                    [chunk, antenna as u32, receptor, 0],
                );
                let gain = Complex64::new(1.0 + n1 * camp.re, n2 * camp.im);
                Complex32::new(gain.re as f32, gain.im as f32)
            };
            values.push([draw(0), draw(1)]);
        }
    }
    JonesTerm {
        kind: JonesKind::Gain,
        antennas,
        solutions: chunks.by_window,
        values,
    }
}

/// Two independent standard normal draws from counter `counter` of stream
/// `key`, by Box–Muller in double precision.
fn normal_pair(key: [u32; 2], counter: [u32; 4]) -> [f64; 2] {
    let words = philox4x32_10(counter, key);
    let uniform = |high: u32, low: u32| {
        // 53 bits, never 0: (n + 0.5) / 2⁵³.
        let bits = (u64::from(high) << 21) ^ u64::from(low >> 11);
        (bits as f64 + 0.5) / (1_u64 << 53) as f64
    };
    let (u1, u2) = (uniform(words[0], words[1]), uniform(words[2], words[3]));
    let radius = (-2.0 * u1.ln()).sqrt();
    let angle = 2.0 * std::f64::consts::PI * u2;
    [radius * angle.cos(), radius * angle.sin()]
}

/// One uniform draw in [0, 1) from counter `counter` of stream `key`.
fn uniform(key: [u32; 2], counter: [u32; 4]) -> f64 {
    let words = philox4x32_10(counter, key);
    let bits = (u64::from(words[0]) << 21) ^ u64::from(words[1] >> 11);
    bits as f64 / (1_u64 << 53) as f64
}

/// The real transform of one screen's length, reused across screens.
struct ScreenFft {
    slots: usize,
    fft: Option<RealFft2<f64>>,
    storage: Vec<Complex64>,
}

impl ScreenFft {
    fn new(slots: usize) -> Self {
        let fft = (slots >= MINIMUM_SCREEN_SLOTS).then(|| {
            RealFft2::with_threads([1, slots], 1)
                .expect("a one-row real transform")
                .with_estimated_plan()
                .with_owned_plan()
        });
        let storage = vec![Complex64::new(0.0, 0.0); slots / 2 + 1];
        Self {
            slots,
            fft,
            storage,
        }
    }

    /// A fractional-Brownian-motion screen of the slots (`fBM::initialize`
    /// in one dimension), drawn from `seed`'s gain-screen stream at
    /// `counter`, scaled to `scale` over its root-mean-square about its
    /// mean, the mean kept (`GJonesCorruptor::initialize`).
    fn normalised_fbm(&mut self, seed: i32, counter: [u32; 4], scale: f64) -> Vec<f64> {
        let key = [seed as u32, PURPOSE_GAIN_SCREEN << 24];
        let Some(fft) = &mut self.fft else {
            // Too few slots for a spectrum: CASA's screen is flat.
            return vec![0.0; self.slots];
        };
        self.storage.fill(Complex64::new(0.0, 0.0));
        // `F(i)` for `i < N/2 - 1`, the rest zero.
        for bin in 0..self.slots / 2 - 1 {
            let mut bin_counter = counter;
            bin_counter[2] = bin as u32;
            let phase = 2.0 * std::f64::consts::PI * uniform(key, bin_counter);
            bin_counter[3] = 1;
            let [normal, _] = normal_pair(key, bin_counter);
            let amplitude = ((bin + 1) as f64).powf(-0.5 * SCREEN_BETA) * normal;
            self.storage[bin] = Complex64::from_polar(amplitude, phase);
        }
        fft.inverse(&mut self.storage)
            .expect("a one-row real transform");
        let reals: Vec<f64> = self
            .storage
            .iter()
            .flat_map(|value| [value.re, value.im])
            .take(self.slots)
            .collect();
        let mean = reals.iter().sum::<f64>() / self.slots as f64;
        let rms = (reals
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / self.slots as f64)
            .sqrt();
        if rms == 0.0 {
            return vec![0.0; self.slots];
        }
        reals.iter().map(|value| value * scale / rms).collect()
    }
}

#[cfg(test)]
#[path = "jones_tests.rs"]
mod tests;
