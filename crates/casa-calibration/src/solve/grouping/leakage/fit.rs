// SPDX-License-Identifier: LGPL-3.0-or-later
//! Complex normal equations for two-receptor leakage on a baseline.

use num_complex::{Complex32, Complex64};

use crate::solve::kernel::complex32_to_64;

pub(super) struct LeakageFit {
    normal: Vec<Complex64>,
    rhs: Vec<Complex64>,
    antennas: Vec<bool>,
}

impl LeakageFit {
    pub(super) fn new(antennas: usize) -> Self {
        let parameters = 2 * antennas;
        Self {
            normal: vec![Complex64::ZERO; parameters * parameters],
            rhs: vec![Complex64::ZERO; parameters],
            antennas: vec![false; antennas],
        }
    }

    pub(super) fn add_baseline(
        &mut self,
        [first, second]: [usize; 2],
        [_, xy, yx, _]: [Complex32; 4],
        model: [Complex32; 4],
        weights: [f32; 4],
    ) {
        let [mxx, mxy, myx, myy] = model;
        let coefficients = [mxx, myy].map(complex32_to_64);
        let cross_hands = [
            complex32_to_64(xy - mxy),
            complex32_to_64((yx - myx).conj()),
        ];

        // Compute D_x and conj(D_y) together. The two measured cross-hands
        // couple the X receptor at one antenna to the Y receptor at the other.
        let left = self.add_equation(
            [2 * second + 1, 2 * first],
            coefficients,
            cross_hands[0],
            weights[1],
        );
        let right = self.add_equation(
            [2 * first + 1, 2 * second],
            coefficients,
            cross_hands[1],
            weights[2],
        );
        if left || right {
            self.antennas[first] = true;
            self.antennas[second] = true;
        }
    }

    fn add_equation(
        &mut self,
        indices: [usize; 2],
        elements: [Complex64; 2],
        observed: Complex64,
        weight: f32,
    ) -> bool {
        if weight <= 0.0
            || !weight.is_finite()
            || !observed.is_finite()
            || elements.iter().any(|coefficient| !coefficient.is_finite())
        {
            return false;
        }
        let count = self.rhs.len();
        for (row, left) in indices.into_iter().zip(elements) {
            let weighted = left.conj() * f64::from(weight);
            self.rhs[row] += weighted * observed;
            for (column, right) in indices.into_iter().zip(elements) {
                self.normal[row * count + column] += weighted * right;
            }
        }
        true
    }

    pub(super) fn solve(self, reference: i32) -> Option<Vec<Option<[Complex32; 2]>>> {
        let mut active = self
            .antennas
            .iter()
            .enumerate()
            .filter(|(_, valid)| **valid)
            .flat_map(|(antenna, _)| [2 * antenna, 2 * antenna + 1])
            .collect::<Vec<_>>();
        if active.is_empty() {
            return None;
        }
        let reference = usize::try_from(reference)
            .ok()
            .filter(|&antenna| self.antennas.get(antenna) == Some(&true));
        let (mut solution, rank) = self.solve_active(&active)?;
        if rank < active.len()
            && let Some(reference) = reference
        {
            // Fix the reference within the measured equations. Subtracting an
            // offset afterward can change a baseline whose parallel model
            // visibilities have different amplitudes.
            active.retain(|&parameter| parameter != 2 * reference);
            solution = self.solve_active(&active)?.0;
        }
        let mut full = vec![Complex64::ZERO; self.rhs.len()];
        for (index, value) in active.into_iter().zip(solution) {
            full[index] = value;
        }
        let offset = reference.map_or(Complex64::ZERO, |antenna| full[2 * antenna]);
        let narrow = |value: Complex64| Complex32::new(value.re as f32, value.im as f32);
        Some(
            full.chunks_exact(2)
                .zip(self.antennas)
                .map(|(params, valid)| {
                    valid.then(|| {
                        [
                            narrow(params[0] - offset),
                            narrow((params[1] + offset).conj()),
                        ]
                    })
                })
                .collect(),
        )
    }

    fn solve_active(&self, active: &[usize]) -> Option<(Vec<Complex64>, usize)> {
        let coefficients = active
            .iter()
            .flat_map(|&row| {
                active
                    .iter()
                    .map(move |&column| self.normal[row * self.rhs.len() + column])
            })
            .collect::<Vec<_>>();
        let observations = active
            .iter()
            .map(|&parameter| self.rhs[parameter])
            .collect::<Vec<_>>();
        casa_numerics::solve_complex_normal_equations(&coefficients, &observations)
    }
}
