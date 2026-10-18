// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native EVLA cell generation on several workers (#708): the cells of one
//! (parallactic angle, frequency, Mueller element) share one prepared
//! aperture ([`EvlaAwWorkspace::prepare`]), so a worker takes a run of
//! their w planes at a time, each worker with its own workspace.

use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use casa_imaging_model::{EvlaAwCellRequest, NativeAwRequestInput};

use super::cell_image::{CellMisc, write_cell};
use super::native_cache::cell_suffix;
use super::{
    AwCatalogError, EvlaApertureModel, EvlaAwWorkspace, IMAGING_PREFIX, MUELLER_ELEMENTS,
    WEIGHT_PREFIX, conjugate_frequency, nearest,
};

/// One worker's share: the cells of one (parallactic angle, frequency,
/// Mueller element), indices into `input`'s lists, at the w planes of a
/// range, less those already present when only missing cells are made.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Unit {
    pa: usize,
    frequency: usize,
    mueller: usize,
    w_planes: Range<usize>,
}

/// Heap bytes [`units`] holds at most for `apertures` (parallactic angle,
/// frequency, Mueller element) triples on `workers` workers: its count of
/// cells per aperture and the units, of which there are fewer than
/// `apertures + workers`.
pub(super) const fn units_bytes(apertures: usize, workers: usize) -> usize {
    apertures * size_of::<usize>() + (apertures + workers) * size_of::<Unit>()
}

/// Whether both images of the cell are in `root`.
fn present(root: &Path, pa: usize, frequency: usize, w: usize, mueller: usize) -> bool {
    let suffix = cell_suffix(pa, frequency, w, mueller);
    root.join(format!("{IMAGING_PREFIX}{suffix}")).is_dir()
        && root.join(format!("{WEIGHT_PREFIX}{suffix}")).is_dir()
}

/// The cells of `input` to generate into `root`, as units for `workers`:
/// every cell, or with `only_missing` those without both images. Each
/// (parallactic angle, frequency, Mueller element) is split into runs of w
/// planes, in the serial generator's order, holding as near equal numbers
/// of cells to generate as can be and none empty. Each aperture takes the
/// same number of runs, or all its cells if fewer, the fewest that give
/// every worker a unit while there are cells for them. A run prepares its
/// aperture once.
pub(super) fn units(
    root: &Path,
    input: &NativeAwRequestInput,
    only_missing: bool,
    workers: usize,
) -> Vec<Unit> {
    let planes = input.w_values.len();
    let to_generate =
        |pa, frequency, w, mueller| !(only_missing && present(root, pa, frequency, w, mueller));
    let apertures = || {
        (0..input.pa_values.len()).flat_map(move |pa| {
            (0..input.frequencies.len()).flat_map(move |frequency| {
                (0..input.mueller_elements.len()).map(move |mueller| (pa, frequency, mueller))
            })
        })
    };
    // Sized exactly, as `units_bytes` charges: a collect would grow it.
    let mut counts = Vec::with_capacity(
        input.pa_values.len() * input.frequencies.len() * input.mueller_elements.len(),
    );
    counts.extend(apertures().map(|(pa, frequency, mueller)| {
        (0..planes)
            .filter(|&w| to_generate(pa, frequency, w, mueller))
            .count()
    }));
    let cells = counts.iter().sum::<usize>();
    if cells == 0 {
        return Vec::new();
    }
    let wanted = workers.clamp(1, cells);
    let units_for = |runs: usize| counts.iter().map(|&count| count.min(runs)).sum::<usize>();
    // At `wanted` runs there are at least `wanted` units, so this ends.
    let runs = (1..=wanted)
        .find(|&runs| units_for(runs) >= wanted)
        .unwrap_or(wanted);
    let mut units = Vec::with_capacity(units_for(runs));
    for ((pa, frequency, mueller), &count) in apertures().zip(&counts) {
        if count == 0 {
            continue;
        }
        // Run j takes the cells to generate of rank j·count/runs up to
        // (j + 1)·count/runs; the last also covers the planes after them.
        let runs = count.min(runs);
        let (mut start, mut rank, mut run) = (0, 0, 0);
        for w in (0..planes).filter(|&w| to_generate(pa, frequency, w, mueller)) {
            rank += 1;
            if rank == (run + 1) * count / runs {
                run += 1;
                let end = if run == runs { planes } else { w + 1 };
                units.push(Unit {
                    pa,
                    frequency,
                    mueller,
                    w_planes: start..end,
                });
                start = end;
            }
        }
    }
    units
}

/// Sets `failed` when a worker unwinds, so a panic stops the others before
/// their next unit as an error does.
struct StopOnPanic<'a>(&'a AtomicBool);

impl Drop for StopOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Relaxed);
        }
    }
}

/// Generate `units` of `input` into `root` on up to `workers` threads, each
/// taking the next unit until none is left, and skipping cells already
/// present with `only_missing`. The first failure or panic stops the others
/// before their next unit and is returned or resumed.
pub(super) fn generate(
    root: &Path,
    input: &NativeAwRequestInput,
    units: &[Unit],
    only_missing: bool,
    workers: usize,
) -> Result<(), AwCatalogError> {
    let model = EvlaApertureModel::new(input.surface.clone());
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let work = || {
        let _stop = StopOnPanic(&failed);
        let mut workspace = None;
        while !failed.load(Ordering::Relaxed) {
            let Some(unit) = units.get(next.fetch_add(1, Ordering::Relaxed)) else {
                break;
            };
            let generated = generate_unit(root, input, &model, unit, only_missing, &mut workspace);
            if let Err(error) = generated {
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
/// write each pair of images; with `only_missing`, skip those present.
fn generate_unit(
    root: &Path,
    input: &NativeAwRequestInput,
    model: &EvlaApertureModel,
    unit: &Unit,
    only_missing: bool,
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
    for iw in unit.w_planes.clone() {
        // No other worker writes this unit's cells, so what is present now
        // is what was present when the units were made.
        if only_missing && present(root, unit.pa, unit.frequency, iw, unit.mueller) {
            continue;
        }
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

    /// The cells `units` generates, in order: the planes of each unit's
    /// range, less those present in `root` with `only_missing`.
    fn cells(
        root: &Path,
        units: &[Unit],
        only_missing: bool,
    ) -> Vec<Vec<(usize, usize, usize, usize)>> {
        units
            .iter()
            .map(|unit| {
                unit.w_planes
                    .clone()
                    .filter(|&w| {
                        !(only_missing && present(root, unit.pa, unit.frequency, w, unit.mueller))
                    })
                    .map(|w| (unit.pa, unit.frequency, unit.mueller, w))
                    .collect()
            })
            .collect()
    }

    /// Every unit has a cell; there is a unit for each worker while there
    /// are cells for them; the units' cells are `expected`, once each and
    /// in order; and the runs of one aperture differ by at most one cell.
    fn assert_units(
        root: &Path,
        input: &NativeAwRequestInput,
        only_missing: bool,
        expected: &[(usize, usize, usize, usize)],
    ) {
        for workers in [1, 2, 3, 4, 5, 8, 15, 16, 17, 20, 21, 64] {
            let units = units(root, input, only_missing, workers);
            let cells = cells(root, &units, only_missing);
            assert!(
                cells.iter().all(|cells| !cells.is_empty()),
                "{workers} workers"
            );
            assert!(
                units.len() >= workers.min(expected.len()),
                "{workers} workers, {} units",
                units.len()
            );
            assert_eq!(cells.concat(), expected, "{workers} workers");
            let aperture = |cells: &Vec<(usize, usize, usize, usize)>| {
                let (pa, frequency, mueller, _) = cells[0];
                (pa, frequency, mueller)
            };
            for aperture in cells.chunk_by(|a, b| aperture(a) == aperture(b)) {
                let sizes = aperture.iter().map(Vec::len);
                let spread = sizes.clone().max().unwrap() - sizes.min().unwrap();
                assert!(spread <= 1, "{workers} workers");
            }
        }
    }

    /// One worker takes every cell of an aperture in one unit, in the serial
    /// generator's order; more workers split each aperture's w planes into
    /// balanced runs until each worker has a unit, and every cell is in one
    /// run. A fill makes units of the missing cells alone.
    #[test]
    fn units_cover_every_cell_once_in_serial_order() {
        let root = tempfile::tempdir().expect("cache directory");
        let input = input(2, 5);
        let apertures = 2 * input.mueller_elements.len();
        let serial = units(root.path(), &input, false, 1);
        assert_eq!(serial.len(), apertures);
        assert!(serial.iter().all(|unit| unit.w_planes == (0..5)));
        let every = cells(root.path(), &serial, false).concat();
        assert_units(root.path(), &input, false, &every);

        // Present: every plane of the first aperture, the first three of the
        // second, the last of the third and none of the fourth.
        let held = |&(pa, frequency, mueller, w): &(usize, usize, usize, usize)| match (frequency, mueller) {
            (0, 0) => true,
            (0, 1) => w < 3,
            (1, 0) => w == 4,
            _ => false,
        } && pa == 0;
        for &(pa, frequency, mueller, w) in every.iter().filter(|cell| held(cell)) {
            let suffix = cell_suffix(pa, frequency, w, mueller);
            for prefix in [IMAGING_PREFIX, WEIGHT_PREFIX] {
                std::fs::create_dir(root.path().join(format!("{prefix}{suffix}"))).expect("cell");
            }
        }
        let missing = every
            .iter()
            .copied()
            .filter(|cell| !held(cell))
            .collect::<Vec<_>>();
        assert_eq!(missing.len(), 11);
        assert_units(root.path(), &input, true, &missing);
    }
}
