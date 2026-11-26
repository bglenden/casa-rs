// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's three orderings, with shape-unit metadata following its component.
use super::State;
use crate::convert::{ConvertError, ConvertResult};
use casa_components::Polarisation;

impl State {
    pub(crate) fn sort(&mut self, criteria: &str) -> ConvertResult<bool> {
        let criterion = criteria.to_ascii_lowercase();
        if !matches!(criterion.as_str(), "flux" | "position" | "polarization") {
            return Err(ConvertError::runtime(
                "Bad sort criteria.\nAllowed values are: 'flux', 'position' & 'polarization'\nNo sorting done.",
            ));
        }
        let order = self
            .order
            .take()
            .unwrap_or_else(|| (0..self.list.len()).collect());
        let mut components = std::mem::take(&mut self.list)
            .into_components()
            .into_iter()
            .enumerate()
            .map(|(index, mut component)| {
                let key = if criterion == "position" {
                    let direction = component.shape().reference_direction();
                    let [x, y, z] = direction.cosines();
                    let half_chord = ((x - 1.0).powi(2) + y * y + z * z).sqrt() / 2.0;
                    2.0 * half_chord.min(1.0).asin()
                } else {
                    let mut flux = component.flux().clone();
                    flux.convert_polarisation(Polarisation::Stokes);
                    let [i, q, u, v] = flux.values().map(|v| v.re);
                    if criterion == "flux" {
                        *component.flux_mut() = flux;
                        i.abs()
                    } else if i.abs() <= f64::MIN_POSITIVE {
                        0.0
                    } else {
                        (q * q + u * u + v * v).sqrt() / i
                    }
                };
                (key, index, component)
            })
            .collect::<Vec<_>>();
        components.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| order[a.1].cmp(&order[b.1]))
        });
        if criterion != "position" {
            components.reverse();
        }
        let mut units = std::mem::take(&mut self.shape_units);
        let mut offsets = std::mem::take(&mut self.offsets);
        let mut sorted_order = Vec::with_capacity(order.len());
        for (_, old, component) in components {
            sorted_order.push(order[old]);
            if let Some(offsets) = offsets.remove(&old) {
                self.offsets.insert(self.list.len(), offsets);
            }
            if let Some(unit) = units.remove(&old) {
                self.shape_units.insert(self.list.len(), unit);
            }
            self.list.push(component);
        }
        self.order = Some(sorted_order);
        Ok(true)
    }
}
