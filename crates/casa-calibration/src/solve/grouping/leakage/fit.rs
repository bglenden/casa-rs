// SPDX-License-Identifier: LGPL-3.0-or-later
//! Complex normal equations for two-receptor leakage on a baseline.

use num_complex::{Complex32, Complex64};

use crate::solve::kernel::complex32_to_64;

pub(super) struct LeakageFit {
    diagonal: Vec<f64>,
    // Only opposite receptors are coupled. Store the X-by-Y block of the
    // Hermitian matrix and reconstruct its conjugate transpose on demand.
    cross: Vec<Complex64>,
    rhs: Vec<Complex64>,
    antennas: Vec<bool>,
}

impl LeakageFit {
    pub(super) fn new(antennas: usize) -> Self {
        let parameters = 2 * antennas;
        Self {
            diagonal: vec![0.0; parameters],
            cross: vec![Complex64::ZERO; antennas * antennas],
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
        let weight = f64::from(weight);
        for (row, left) in indices.into_iter().zip(elements) {
            self.rhs[row] += left.conj() * weight * observed;
            self.diagonal[row] += left.norm_sqr() * weight;
        }
        let [y, x] = indices.map(|index| index / 2);
        self.cross[x * self.antennas.len() + y] += elements[1].conj() * elements[0] * weight;
        true
    }

    pub(super) fn has_antenna(&self, antenna: i32) -> bool {
        self.antennas.get(antenna as usize) == Some(&true)
    }

    pub(super) fn solve(
        self,
        reference: i32,
        reference_value: Complex32,
    ) -> Option<Vec<Option<[Complex32; 2]>>> {
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
        let target = complex32_to_64(reference_value);
        let (mut solution, rank) = self.solve_active(&active, None)?;
        let mut full = vec![Complex64::ZERO; self.rhs.len()];
        if let Some(antenna) = reference.filter(|_| rank < active.len()) {
            // Fix the reference within the measured equations. Subtracting an
            // offset afterward can change a baseline whose parallel model
            // visibilities have different amplitudes.
            let parameter = 2 * antenna;
            active.retain(|&active_parameter| active_parameter != parameter);
            solution = self.solve_active(&active, Some((parameter, target)))?.0;
            full[parameter] = target;
        }
        for (index, value) in active.into_iter().zip(solution) {
            full[index] = value;
        }
        let offset = reference.map_or(Complex64::ZERO, |antenna| full[2 * antenna] - target);
        let narrow = |value: Complex64| Complex32::new(value.re as f32, value.im as f32);
        Some(
            full.as_chunks::<2>()
                .0
                .iter()
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

    fn coefficient(&self, row: usize, column: usize) -> Complex64 {
        let antennas = self.antennas.len();
        match (row % 2, column % 2) {
            (0, 1) => self.cross[(row / 2) * antennas + column / 2],
            (1, 0) => self.cross[(column / 2) * antennas + row / 2].conj(),
            _ if row == column => self.diagonal[row].into(),
            _ => Complex64::ZERO,
        }
    }

    fn solve_active(
        &self,
        active: &[usize],
        constraint: Option<(usize, Complex64)>,
    ) -> Option<(Vec<Complex64>, usize)> {
        let coefficients = active
            .iter()
            .flat_map(|&row| {
                active
                    .iter()
                    .map(move |&column| self.coefficient(row, column))
            })
            .collect::<Vec<_>>();
        let observations = active
            .iter()
            .map(|&parameter| {
                self.rhs[parameter]
                    - constraint.map_or(Complex64::ZERO, |(fixed, value)| {
                        self.coefficient(parameter, fixed) * value
                    })
            })
            .collect::<Vec<_>>();
        casa_numerics::solve_complex_normal_equations(coefficients, observations)
    }
}
