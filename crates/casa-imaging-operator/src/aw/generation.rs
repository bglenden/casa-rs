// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native EVLA cell generation on several workers (#708): the cells of one
//! (parallactic angle, frequency, Mueller element) share one prepared
//! aperture ([`EvlaAwWorkspace::prepare`]), so a worker takes a run of
//! their w planes at a time, each worker with its own workspace.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use casa_imaging_model::{EvlaAwCellRequest, NativeAwRequestInput};

use super::cell_image::{CellMisc, write_cell};
use super::native_cache::cell_suffix;
use super::{
    AwCatalogError, EvlaApertureModel, EvlaAwWorkspace, IMAGING_PREFIX, MUELLER_ELEMENTS,
    WEIGHT_PREFIX, conjugate_frequency, nearest,
};

/// One worker's share: cells of one (parallactic angle, frequency, Mueller
/// element), indices into `input`'s lists, at some of its w planes.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Unit {
    pa: usize,
    frequency: usize,
    mueller: usize,
    w_planes: Vec<usize>,
}

/// The cells of `input` to generate into `root`, as units for `workers`:
/// every cell, or with `only_missing` those without both images. Each
/// (parallactic angle, frequency, Mueller element) whose cells are not all
/// present is split into as many runs of w planes as give every worker a
/// unit, in the serial generator's order. A run prepares its aperture once.
pub(super) fn units(
    root: &Path,
    input: &NativeAwRequestInput,
    only_missing: bool,
    workers: usize,
) -> Vec<Unit> {
    let present = |pa: usize, frequency: usize, w: usize, mueller: usize| {
        let suffix = cell_suffix(pa, frequency, w, mueller);
        only_missing
            && root.join(format!("{IMAGING_PREFIX}{suffix}")).is_dir()
            && root.join(format!("{WEIGHT_PREFIX}{suffix}")).is_dir()
    };
    let mut pending = Vec::new();
    for pa in 0..input.pa_values.len() {
        for frequency in 0..input.frequencies.len() {
            for mueller in 0..input.mueller_elements.len() {
                let w_planes = (0..input.w_values.len())
                    .filter(|&w| !present(pa, frequency, w, mueller))
                    .collect::<Vec<_>>();
                if !w_planes.is_empty() {
                    pending.push(Unit {
                        pa,
                        frequency,
                        mueller,
                        w_planes,
                    });
                }
            }
        }
    }
    let runs = workers.max(1).div_ceil(pending.len().max(1));
    pending
        .into_iter()
        .flat_map(|unit| {
            let length = unit.w_planes.len().div_ceil(runs);
            unit.w_planes
                .chunks(length)
                .map(|w_planes| Unit {
                    w_planes: w_planes.to_vec(),
                    ..unit.clone()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Generate `units` of `input` into `root` on up to `workers` threads, each
/// taking the next unit until none is left; the first failure stops the
/// others before their next unit and is returned.
pub(super) fn generate(
    root: &Path,
    input: &NativeAwRequestInput,
    units: &[Unit],
    workers: usize,
) -> Result<(), AwCatalogError> {
    let model = EvlaApertureModel::new(input.surface.clone());
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let work = || {
        let mut workspace = None;
        while !failed.load(Ordering::Relaxed) {
            let Some(unit) = units.get(next.fetch_add(1, Ordering::Relaxed)) else {
                break;
            };
            if let Err(error) = generate_unit(root, input, &model, unit, &mut workspace) {
                failed.store(true, Ordering::Relaxed);
                return Err(error);
            }
        }
        Ok(())
    };
    let workers = workers.clamp(1, units.len().max(1));
    if workers == 1 {
        return work();
    }
    std::thread::scope(|scope| {
        let threads = (0..workers).map(|_| scope.spawn(work)).collect::<Vec<_>>();
        threads
            .into_iter()
            .map(|thread| {
                thread
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            })
            .collect::<Vec<_>>()
    })
    .into_iter()
    .collect()
}

/// Generate `unit`'s cells with `workspace`, made on its first cell, and
/// write each pair of images.
fn generate_unit(
    root: &Path,
    input: &NativeAwRequestInput,
    model: &EvlaApertureModel,
    unit: &Unit,
    workspace: &mut Option<EvlaAwWorkspace>,
) -> Result<(), AwCatalogError> {
    let grid = input.grid;
    let pa = input.pa_values[unit.pa];
    let frequency = input.frequencies[unit.frequency].cf_frequency_hz;
    // `AWConvFunc::makeConvFunction2`: the conjugate beam uses the nearest
    // listed frequency to `√(2f₀² − f²)`, the first on a tie
    // (`SynthesisUtils::nearestValue`).
    let conjugate_frequency = if input.terms.conjugate_beams {
        let listed = input
            .frequencies
            .iter()
            .map(|group| group.cf_frequency_hz)
            .collect::<Vec<_>>();
        nearest(
            &listed,
            conjugate_frequency(frequency, input.reference_frequency_hz),
        )
        .1
    } else {
        frequency
    };
    let mueller = input.mueller_elements[unit.mueller];
    for &iw in &unit.w_planes {
        let w = input.w_values[iw];
        let request = EvlaAwCellRequest {
            size: grid.size,
            sky_increment_rad: grid.sky_increment_rad,
            frequency_hz: frequency,
            conjugate_frequency_hz: conjugate_frequency,
            w_wavelengths: w,
            parallactic_angle_rad: pa,
            mueller,
            oversampling: grid.oversampling,
            prolate_spheroidal: input.terms.prolate_spheroidal,
            aperture: input.terms.aperture,
        };
        let workspace = match workspace {
            Some(workspace) => workspace,
            None => workspace.insert(EvlaAwWorkspace::new(model.clone(), request)?),
        };
        // Only the W screen depends on w: the aperture and the weight cell
        // are formed once per unit.
        workspace.prepare(request)?;
        let pair = workspace.realize(request)?;
        let misc = CellMisc {
            sampling: grid.oversampling as f64,
            pa_deg: pa.to_degrees(),
            mueller,
            w_value: w,
            w_increment: input.w_increment,
            conjugate_frequency,
            conjugate_mueller: MUELLER_ELEMENTS - 1 - mueller as u32,
            diameter_m: input.antenna_diameter_m,
            sky_increment_rad: grid.sky_increment_rad[1].abs(),
            working_size: grid.size,
        };
        let suffix = cell_suffix(unit.pa, unit.frequency, iw, unit.mueller);
        write_cell(
            &root.join(format!("{IMAGING_PREFIX}{suffix}")),
            &pair.imaging,
            frequency,
            grid,
            &misc,
        )?;
        write_cell(
            &root.join(format!("{WEIGHT_PREFIX}{suffix}")),
            &pair.weight,
            frequency,
            grid,
            &misc,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use casa_imaging_model::{
        EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwTerms,
    };

    /// A request of one parallactic angle, `frequencies` SPWs, `w_planes`
    /// w planes and two Mueller elements; only its lists matter here.
    fn input(frequencies: usize, w_planes: usize) -> NativeAwRequestInput {
        NativeAwRequestInput {
            surface: EvlaDishSurface::new(
                (0..=125)
                    .map(|i| {
                        let r = f64::from(i) / 10.0;
                        [r, r * r / 36.0, r / 18.0]
                    })
                    .collect(),
            )
            .expect("surface"),
            antenna_diameter_m: 25.0,
            frequencies: (0..frequencies)
                .map(|window| NativeAwFrequencyGroup {
                    spectral_window: window as u32,
                    channel_frequencies_hz: vec![1.4e9 + window as f64 * 1.0e8],
                    cf_frequency_hz: 1.4e9 + window as f64 * 1.0e8,
                })
                .collect(),
            w_values: (0..w_planes).map(|w| w as f64).collect(),
            w_increment: 1.0,
            pa_values: vec![0.31],
            mueller_elements: vec![0, 15],
            reference_frequency_hz: 1.5e9,
            grid: NativeAwGrid {
                size: 64,
                sky_increment_rad: [-4.0e-4, 4.0e-4],
                oversampling: 4,
            },
            terms: NativeAwTerms {
                aperture: true,
                w_term: true,
                prolate_spheroidal: false,
                wideband: true,
                conjugate_beams: false,
            },
            maximum_cells: 2 * frequencies * w_planes,
        }
    }

    /// One worker takes every cell of an aperture in one unit, in the serial
    /// generator's order; more workers split each aperture's w planes into
    /// runs until each worker has a unit, and every cell is in one run.
    #[test]
    fn units_cover_every_cell_once_in_serial_order() {
        let root = std::path::Path::new("/nonexistent");
        let input = input(2, 5);
        let apertures = 2 * input.mueller_elements.len();
        let serial = units(root, &input, false, 1);
        assert_eq!(serial.len(), apertures);
        assert!(serial.iter().all(|unit| unit.w_planes == [0, 1, 2, 3, 4]));
        for workers in [2, 3, 8, 64] {
            let split = units(root, &input, false, workers);
            assert!(
                split.len() >= workers.min(apertures * 5),
                "{workers} workers"
            );
            let cells = split
                .iter()
                .flat_map(|unit| {
                    unit.w_planes
                        .iter()
                        .map(|&w| (unit.pa, unit.frequency, unit.mueller, w))
                })
                .collect::<Vec<_>>();
            let ordered = serial
                .iter()
                .flat_map(|unit| {
                    unit.w_planes
                        .iter()
                        .map(|&w| (unit.pa, unit.frequency, unit.mueller, w))
                })
                .collect::<Vec<_>>();
            assert_eq!(cells, ordered, "{workers} workers");
        }
    }
}
