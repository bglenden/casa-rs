// SPDX-License-Identifier: LGPL-3.0-or-later
//! Parallactic-angle rotation epochs for an imported aperture buffer.

use std::{path::Path, sync::Arc};

use super::{
    AwCatalog, AwCatalogError, AwIndexing, GridGeometry, PolarizationRouting, first_covered_pa,
};

impl AwCatalog {
    /// Open cells selected during legacy CASA generation, retaining the
    /// individual live PA buffers before CASA's warm import coalesces them.
    /// When rotation is enabled, `base_pa_deg` contains the actual selected
    /// buffer for each observation time. An unrotated catalog has no row map.
    ///
    /// # Errors
    /// The cells or explicitly selected rotation bases are invalid.
    pub fn open_legacy_live(
        root: impl AsRef<Path>,
        indexing: AwIndexing,
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        bound_bytes: usize,
        base_pa_deg: &[f64],
    ) -> Result<Self, AwCatalogError> {
        Self::open(
            root.as_ref(),
            indexing,
            geometry,
            polarization,
            bound_bytes,
            true,
            Some(base_pa_deg),
        )
    }
}

/// Feed angles at the selected observation times and the rotation threshold.
#[derive(Clone, Debug, PartialEq)]
pub struct AwPaRotation {
    /// Minimum absolute change that triggers rotation, in radians.
    pub step_rad: f64,
    /// Taylor passes share one schedule only when the retained angle at
    /// the end of the first pass yields the same effective angles on replay.
    pub require_repeatable_schedule: bool,
    /// The selected times in increasing order, seconds since MJD 0, with
    /// the corresponding antenna-0 feed PA in Float radians.
    pub samples: Arc<[(f64, f32)]>,
}

impl AwPaRotation {
    /// The input is shared with the deployment and stays allocated.
    pub(super) fn samples_bytes(&self) -> usize {
        self.samples.len() * size_of::<(f64, f32)>() + 2 * size_of::<usize>()
    }

    /// In the single-buffer case, record each rotation epoch. The cached
    /// angle is retained until the next sample differs strictly more than
    /// the requested threshold (`AWConvFunc::prepareConvFunction`).
    pub(super) fn schedule(&self, base_deg: f64) -> Option<Vec<(f64, f64)>> {
        let base = f64::from(base_deg as f32).to_radians();
        let mut last = base;
        let mut rotations = Vec::new();
        for &(time, pa) in self.samples.iter() {
            let pa = f64::from(pa);
            if (last - pa).abs() > self.step_rad {
                rotations.push((time, base - pa));
                last = pa;
            }
        }
        if self.require_repeatable_schedule && !self.repeats_from(base, last) {
            return None;
        }
        Some(rotations)
    }

    fn repeats_from(&self, mut fresh: f64, mut replay: f64) -> bool {
        self.samples.iter().all(|&(_, pa)| {
            let pa = f64::from(pa);
            if (fresh - pa).abs() > self.step_rad {
                fresh = pa;
            }
            if (replay - pa).abs() > self.step_rad {
                replay = pa;
            }
            // The saved header has Float degrees, while feed PA is Float
            // radians. Thresholds use their original radian values.
            fresh as f32 == replay as f32
        })
    }
}

/// Original apertures followed by the rotation epochs required by selected
/// rows. Each legacy rotation retains cells from its own base PA only.
#[derive(Debug)]
pub(super) enum PaSchedule {
    Unrotated,
    Single(Vec<(f64, f64)>),
    Legacy {
        epochs: Vec<(f64, usize, Option<usize>)>,
        variants: Vec<(usize, f64)>,
    },
}

impl PaSchedule {
    pub(super) fn new(
        indexing: &AwIndexing,
        pa_deg: &[f64],
        rotatable: bool,
        legacy: bool,
        live_pa_deg: Option<&[f64]>,
    ) -> Result<Self, &'static str> {
        let Some(rotation) = &indexing.rotation else {
            return Ok(Self::Unrotated);
        };
        if !rotatable {
            return Err("runtime rotation needs axis-aligned linear cells (#1070)");
        }
        if legacy {
            return Self::legacy(rotation, pa_deg, indexing.pa_tolerance_rad, live_pa_deg);
        }
        if pa_deg.len() != 1 {
            return Err("runtime rotation needs one PA buffer (#1070)");
        }
        rotation.schedule(pa_deg[0]).map(Self::Single).ok_or(
            "runtime rotation needs the same effective PA schedule across wideband passes (#1070)",
        )
    }

    fn legacy(
        rotation: &AwPaRotation,
        pa_deg: &[f64],
        tolerance_rad: f32,
        live_pa_deg: Option<&[f64]>,
    ) -> Result<Self, &'static str> {
        let mut active = pa_deg
            .iter()
            .map(|&degrees| {
                let base = f64::from(degrees as f32).to_radians();
                (base, base, None)
            })
            .collect::<Vec<_>>();
        if live_pa_deg.is_some_and(|base| base.len() != rotation.samples.len()) {
            return Err("legacy AW live bases do not match the recorded samples");
        }
        let mut variants = Vec::new();
        let mut epochs = Vec::new();
        for (index, &(time, angle)) in rotation.samples.iter().enumerate() {
            let pa = if let Some(base) = live_pa_deg {
                pa_deg
                    .binary_search_by(|degrees| degrees.total_cmp(&base[index]))
                    .ok()
            } else {
                first_covered_pa(pa_deg, angle, tolerance_rad)
            }
            .ok_or("no imported AW buffer within pastep of a selected row")?;
            let (base, current, variant) = &mut active[pa];
            let angle = f64::from(angle);
            if (*current - angle).abs() > rotation.step_rad {
                *current = angle;
                *variant = Some(variants.len());
                variants.push((pa, *base - angle));
            }
            if !epochs
                .last()
                .is_some_and(|&(_, p, v)| (p, v) == (pa, *variant))
            {
                epochs.push((time, pa, *variant));
            }
        }
        Ok(Self::Legacy { epochs, variants })
    }

    pub(super) fn group_count(&self, base: usize, per_pa: usize) -> Option<usize> {
        let extra = match self {
            Self::Unrotated => 0,
            Self::Single(epochs) => epochs.len().checked_mul(base)?,
            Self::Legacy { variants, .. } => variants.len().checked_mul(per_pa)?,
        };
        base.checked_add(extra)
    }

    pub(super) fn resident_bytes(&self) -> usize {
        match self {
            Self::Unrotated => 0,
            Self::Single(epochs) => epochs.capacity() * size_of::<(f64, f64)>(),
            Self::Legacy { epochs, variants } => {
                epochs.capacity() * size_of::<(f64, usize, Option<usize>)>()
                    + variants.capacity() * size_of::<(usize, f64)>()
            }
        }
    }

    pub(super) fn has_rotation(&self) -> bool {
        match self {
            Self::Unrotated => false,
            Self::Single(epochs) => !epochs.is_empty(),
            Self::Legacy { variants, .. } => !variants.is_empty(),
        }
    }

    pub(super) fn legacy_pa(&self, time: f64) -> Option<usize> {
        self.legacy_epoch(time).map(|(pa, _)| pa)
    }

    fn legacy_epoch(&self, time: f64) -> Option<(usize, Option<usize>)> {
        let Self::Legacy { epochs, .. } = self else {
            return None;
        };
        let end = epochs.partition_point(|&(start, _, _)| start <= time);
        if end == 0 {
            epochs.first().map(|&(_, pa, _)| (pa, None))
        } else {
            let (_, pa, variant) = epochs[end - 1];
            Some((pa, variant))
        }
    }

    /// Base offset of the selected PA block within the flattened group IDs.
    pub(super) fn group_start(&self, time: f64, pa: usize, base: usize, per_pa: usize) -> usize {
        match self {
            Self::Single(epochs) => {
                pa * per_pa + epochs.partition_point(|&(start, _)| start <= time) * base
            }
            Self::Legacy { .. } => self
                .legacy_epoch(time)
                .and_then(|(_, variant)| variant)
                .map_or(pa * per_pa, |variant| base + variant * per_pa),
            Self::Unrotated => pa * per_pa,
        }
    }

    pub(super) fn source(&self, group: usize, base: usize, per_pa: usize) -> (usize, f64) {
        let Some(extra) = group.checked_sub(base) else {
            return (group, 0.);
        };
        match self {
            Self::Single(epochs) => (extra % base, epochs[extra / base].1),
            Self::Legacy { variants, .. } => {
                let (pa, angle) = variants[extra / per_pa];
                (pa * per_pa + extra % per_pa, angle)
            }
            Self::Unrotated => unreachable!("unrotated group is within the original cells"),
        }
    }
}

/// The inverse pixel transform for an axis-aligned linear CF plane.
pub(super) fn pixel_rotation(angle_rad: f64) -> [[f64; 2]; 2] {
    let (sin, cos) = angle_rad.sin_cos();
    [[cos, -sin], [sin, cos]]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_samples_the_inverse_linear_coordinate_transform() {
        let [[xx, xy], [yx, yy]] = pixel_rotation(std::f64::consts::FRAC_PI_2);
        assert!(xx.abs() < 1.0e-15 && yy.abs() < 1.0e-15);
        assert_eq!([xy, yx], [-1.0, 1.0]);
    }

    #[test]
    fn rotation_epochs_use_the_previous_angle_and_rotate_the_base() {
        let rotation = AwPaRotation {
            step_rad: 0.125,
            require_repeatable_schedule: false,
            samples: Arc::new([(1.0, 0.125), (2.0, 0.25), (3.0, 0.375), (4.0, 0.5)]),
        };
        assert_eq!(rotation.schedule(0.0).unwrap(), [(2.0, -0.25), (4.0, -0.5)]);
    }

    #[test]
    fn shared_rotation_requires_the_same_angles_after_wraparound() {
        let rotation = |first_deg: f32| AwPaRotation {
            step_rad: 5.0_f64.to_radians(),
            require_repeatable_schedule: true,
            samples: Arc::new([
                (1.0, first_deg.to_radians()),
                (2.0, 5.5_f32.to_radians()),
                (3.0, 12.0_f32.to_radians()),
            ]),
        };
        // The second pass rotates from 12 degrees to 1, then stays at 1
        // for the next sample. The first pass uses 0 and then 5.5.
        assert!(rotation(1.0).schedule(0.0).is_none());
        // With first PA equal to the saved PA, both passes use the same
        // angles even though the first replay sample triggers a rotation.
        assert_eq!(rotation(0.0).schedule(0.0).unwrap().len(), 2);
    }
}
