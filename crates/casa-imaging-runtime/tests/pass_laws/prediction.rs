// SPDX-License-Identifier: LGPL-3.0-or-later
//! The block predictor (`BlockPredictor`): on the CPU it predicts each row
//! as `SpectralResampler::predict_row` does, bit for bit, whatever the
//! workers, the chunks or the blocks the rows arrive in, and sums its
//! domains' predictions; autocorrelations predict their zero spacing only
//! when the resampler is asked to; on Metal it agrees with the CPU within
//! the T0 law, `max|metal − cpu| ≤ 1e-4·max|cpu|`.

use casa_imaging_operator::{
    Basis, CpuBackend, GridPrecision, MeasurementOperator, PlaneRange, PredictionScratch,
    PreparedModelGrids, SpectralResampler,
};
use casa_imaging_runtime::pass::{
    BackendChoice, BlockPredictor, PassError, PredictionDomain, WorkerTeam,
};
use num_complex::Complex32;

use crate::fixture::{
    IMAGE, RowKind, Rows, nearest_cube, offset_linear_cube, operator, prediction_block,
    prediction_rows, prediction_shape, row_kind, sparse_model, window_of,
};

/// Rows a predictor's blocks hold at most in these laws.
const BLOCK_ROWS: usize = 37;

/// Every row of `rows` predicted in blocks of `block_rows` rows by a
/// predictor over `domains` on `backend` and `workers` workers, made for
/// blocks of [`BLOCK_ROWS`] rows.
fn predict(
    domains: &[PredictionDomain<'_>],
    rows: &Rows,
    backend: BackendChoice,
    workers: usize,
    block_rows: usize,
) -> Vec<Complex32> {
    let team = WorkerTeam::new(workers).expect("team");
    let mut predictor = BlockPredictor::new(
        domains,
        backend,
        prediction_shape(rows, BLOCK_ROWS),
        workers,
        false,
    )
    .expect("predictor");
    let cells = rows.frequencies.len() * 2;
    let mut out = vec![Complex32::new(7.0, 7.0); rows.rows.len() * cells];
    for (start, chunk) in (0..rows.rows.len())
        .step_by(block_rows)
        .zip(out.chunks_mut(block_rows * cells))
    {
        let block = prediction_block(rows, start..(start + block_rows).min(rows.rows.len()));
        predictor
            .predict_into(&block, &team, chunk)
            .expect("prediction");
    }
    out
}

/// Each row predicted alone ([`SpectralResampler::predict_row`]) on every
/// domain: the first domain's, then each later one's added, as the domains
/// sum at native cells.
fn predict_alone(domains: &[PredictionDomain<'_>], rows: &Rows) -> Vec<Complex32> {
    let block = prediction_block(rows, 0..rows.rows.len());
    let cells = rows.frequencies.len() * 2;
    let mut total = vec![Complex32::default(); rows.rows.len() * cells];
    let mut alone = vec![Complex32::default(); cells];
    for (index, out) in total.chunks_mut(cells).enumerate() {
        for (domain_index, domain) in domains.iter().enumerate() {
            domain
                .resampler
                .predict_row(
                    domain.operator,
                    &mut CpuBackend::new(),
                    domain.model,
                    &block.row(domain_index, index),
                    &mut PredictionScratch::default(),
                    &mut alone,
                )
                .expect("alone");
            if domain_index == 0 {
                out.copy_from_slice(&alone);
            } else {
                for (sum, value) in out.iter_mut().zip(&alone) {
                    *sum += value;
                }
            }
        }
    }
    total
}

/// The prepared model of every plane of `operator`'s basis.
fn model_of(operator: &MeasurementOperator, seed: u64) -> PreparedModelGrids {
    let planes = operator.basis().planes();
    window_of(
        operator,
        &sparse_model(IMAGE, planes as usize, seed),
        PlaneRange::new(0, planes),
    )
}

/// Whether every value of each row of `predicted` is zero.
fn zero_rows(predicted: &[Complex32], cells: usize) -> Vec<bool> {
    predicted
        .chunks(cells)
        .map(|row| row.iter().all(|value| *value == Complex32::default()))
        .collect()
}

/// The CPU predictor gives each row what predicting it alone gives, bit for
/// bit, under direct, nearest and linear mapping; flagged rows predict zero
/// and autocorrelations predict zero unless the resampler predicts them.
#[test]
fn the_cpu_block_predictor_predicts_each_row_as_alone() {
    let direct = SpectralResampler::direct(Basis::Constant).expect("direct");
    for base in [direct, nearest_cube(), offset_linear_cube()] {
        let operator = operator(GridPrecision::F64, base.basis());
        let model = model_of(&operator, 3);
        let rows = prediction_rows(90, 13, &operator, 1);
        for resampler in [base, base.predicting_autocorrelations()] {
            let domains = [PredictionDomain {
                operator: &operator,
                resampler: &resampler,
                model: &model,
            }];
            let predicted = predict(&domains, &rows, BackendChoice::Cpu, 2, BLOCK_ROWS);
            assert!(
                predicted == predict_alone(&domains, &rows),
                "{resampler:?}: a block predicts each row as alone"
            );
            let zero = zero_rows(&predicted, rows.frequencies.len() * 2);
            for (index, zero) in zero.into_iter().enumerate() {
                match row_kind(index) {
                    RowKind::Flagged => assert!(zero, "a flagged row predicts zero"),
                    RowKind::Auto => assert_eq!(
                        zero,
                        !resampler.predicts_autocorrelations(),
                        "{resampler:?}: autocorrelation {index}"
                    ),
                    RowKind::Cross => assert!(!zero, "row {index} predicts the model"),
                }
            }
        }
    }
}

/// The CPU predictor's visibilities are the same bits on one to four
/// workers, whether the rows come in blocks of 37, 5 or one, and from a
/// predictor made for one-row blocks.
#[test]
fn block_predictions_do_not_depend_on_workers_chunks_or_blocks() {
    let resampler = offset_linear_cube().predicting_autocorrelations();
    let operator = operator(GridPrecision::F64, resampler.basis());
    let model = model_of(&operator, 5);
    let rows = prediction_rows(111, 19, &operator, 1);
    let domains = [PredictionDomain {
        operator: &operator,
        resampler: &resampler,
        model: &model,
    }];
    let reference = predict(&domains, &rows, BackendChoice::Cpu, 1, BLOCK_ROWS);
    for (workers, block_rows) in [(2, BLOCK_ROWS), (3, 5), (4, 1), (1, 5)] {
        assert!(
            predict(&domains, &rows, BackendChoice::Cpu, workers, block_rows) == reference,
            "{workers} workers, blocks of {block_rows} rows"
        );
    }
    let team = WorkerTeam::new(3).expect("team");
    let mut narrow = BlockPredictor::new(
        &domains,
        BackendChoice::Cpu,
        prediction_shape(&rows, 1),
        3,
        false,
    )
    .expect("predictor");
    let cells = rows.frequencies.len() * 2;
    let mut out = vec![Complex32::default(); cells];
    for (index, expected) in reference.chunks(cells).enumerate() {
        narrow
            .predict_into(&prediction_block(&rows, index..index + 1), &team, &mut out)
            .expect("prediction");
        assert!(out == expected, "row {index} alone in its block");
    }
}

/// Two image domains' predictions sum at native cells: the predictor
/// equals each row's prediction on the first domain plus its prediction on
/// the second, bit for bit.
#[test]
fn a_block_predictor_sums_its_domains() {
    let resampler = nearest_cube().predicting_autocorrelations();
    let first = operator(GridPrecision::F64, resampler.basis());
    let second = operator(GridPrecision::F64, resampler.basis());
    let (first_model, second_model) = (model_of(&first, 7), model_of(&second, 11));
    let rows = prediction_rows(80, 23, &first, 2);
    let domains = [
        PredictionDomain {
            operator: &first,
            resampler: &resampler,
            model: &first_model,
        },
        PredictionDomain {
            operator: &second,
            resampler: &resampler,
            model: &second_model,
        },
    ];
    let summed = predict(&domains, &rows, BackendChoice::Cpu, 3, BLOCK_ROWS);
    assert!(summed == predict_alone(&domains, &rows), "the domains sum");
    assert!(
        summed != predict_alone(&domains[..1], &rows),
        "the second domain contributes"
    );
}

/// A predictor needs a domain and a worker.
#[test]
fn a_block_predictor_without_domains_or_workers_is_refused() {
    let resampler = nearest_cube();
    let operator = operator(GridPrecision::F64, resampler.basis());
    let model = model_of(&operator, 2);
    let shape = prediction_shape(&prediction_rows(4, 2, &operator, 1), 4);
    assert!(matches!(
        BlockPredictor::new(&[], BackendChoice::Cpu, shape, 1, false),
        Err(PassError::NoDomains)
    ));
    let domains = [PredictionDomain {
        operator: &operator,
        resampler: &resampler,
        model: &model,
    }];
    assert!(matches!(
        BlockPredictor::new(&domains, BackendChoice::Cpu, shape, 0, false),
        Err(PassError::Workers)
    ));
}

/// On Metal the predictor agrees with the CPU's within the T0 law, for
/// nearest and linear cubes over two domains with autocorrelations
/// predicted, block after block: a later block's flagged rows and
/// autocorrelations, at positions where an earlier block's rows crossed,
/// keep no stale model from the device cells.
#[test]
fn metal_block_predictions_equal_the_cpus() {
    if !casa_imaging_metal::available() {
        eprintln!("skipped: no Metal device");
        return;
    }
    for base in [nearest_cube(), offset_linear_cube()] {
        let resampler = base.predicting_autocorrelations();
        let first = operator(GridPrecision::F32, resampler.basis());
        let second = operator(GridPrecision::F32, resampler.basis());
        let (first_model, second_model) = (model_of(&first, 29), model_of(&second, 31));
        let rows = prediction_rows(150, 37, &first, 2);
        let domains = [
            PredictionDomain {
                operator: &first,
                resampler: &resampler,
                model: &first_model,
            },
            PredictionDomain {
                operator: &second,
                resampler: &resampler,
                model: &second_model,
            },
        ];
        for (workers, block_rows) in [(1, BLOCK_ROWS), (3, 29)] {
            let cpu = predict(&domains, &rows, BackendChoice::Cpu, workers, block_rows);
            let metal = predict(&domains, &rows, BackendChoice::Metal, workers, block_rows);
            let peak = cpu
                .iter()
                .fold(0.0_f32, |peak, value| peak.max(value.norm()));
            let worst = metal
                .iter()
                .zip(&cpu)
                .fold(0.0_f32, |worst, (m, c)| worst.max((m - c).norm()));
            assert!(peak > 0.0, "the CPU predicts a model");
            assert!(
                worst <= 1.0e-4 * peak,
                "{base:?}, {workers} workers, blocks of {block_rows}: {worst} against {peak}"
            );
            let zero = zero_rows(&metal, rows.frequencies.len() * 2);
            for (index, zero) in zero.into_iter().enumerate() {
                match row_kind(index) {
                    RowKind::Flagged => assert!(zero, "Metal: flagged row {index} predicts zero"),
                    RowKind::Auto | RowKind::Cross => {
                        assert!(!zero, "Metal: row {index} predicts the model");
                    }
                }
            }
        }
    }
}
