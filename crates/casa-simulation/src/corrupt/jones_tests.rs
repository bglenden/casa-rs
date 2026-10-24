// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;

const LINEAR: [[usize; 2]; 4] = [[0, 0], [0, 1], [1, 0], [1, 1]];

/// Chunks at the integration centres of `integrations` integrations of
/// 10 s from t = 1000 s, in window 0.
fn integrations(integrations: usize) -> Chunks {
    let mut chunks = Chunks::default();
    for index in 0..integrations {
        chunks.push(0, 1000.0 + 10.0 * index as f64 + 5.0, 0);
    }
    chunks
}

/// Every slot's value of `antenna`'s receptor `receptor`.
fn screen(term: &JonesTerm, antenna: usize, receptor: usize) -> Vec<Complex32> {
    term.values
        .chunks_exact(term.antennas)
        .map(|values| values[antenna][receptor])
        .collect()
}

fn mean_and_rms(values: impl Iterator<Item = f64> + Clone) -> (f64, f64) {
    let count = values.clone().count() as f64;
    let mean = values.clone().sum::<f64>() / count;
    let rms = (values.map(|value| (value - mean).powi(2)).sum::<f64>() / count).sqrt();
    (mean, rms)
}

/// Each screen's amplitude fluctuates about its mean by the scale, and its
/// phase by π times the scale, over the even slots of 5 s spanning the
/// chunks (`GJonesCorruptor::initialize`).
#[test]
fn gain_screens_fluctuate_by_the_scale_over_even_slots() {
    let scale = 0.05;
    let term = fbm_gains(11_111, scale, 4, integrations(60));
    // From 1005 s to 1595 s: 119 slots of 5 s.
    assert_eq!(term.values.len(), 119 * 4);
    for antenna in 0..4 {
        for receptor in 0..2 {
            let values = screen(&term, antenna, receptor);
            let (_, amplitude_rms) = mean_and_rms(values.iter().map(|g| f64::from(g.norm())));
            let (_, phase_rms) = mean_and_rms(values.iter().map(|g| f64::from(g.arg())));
            assert!(
                (amplitude_rms - scale).abs() < 1e-5,
                "antenna {antenna} receptor {receptor}: amplitude rms {amplitude_rms}"
            );
            assert!(
                (phase_rms - std::f64::consts::PI * scale).abs() < 1e-5,
                "antenna {antenna} receptor {receptor}: phase rms {phase_rms}"
            );
        }
    }
}

/// Every antenna's and receptor's screen is drawn apart (CASA's coincide
/// for antenna `a`'s second receptor and antenna `a+1`'s first); a seed
/// draws the same screens again, and another seed others.
#[test]
fn gain_screens_are_keyed_by_seed_antenna_and_receptor() {
    let term = fbm_gains(7, 0.1, 3, integrations(40));
    let screens: Vec<Vec<Complex32>> = (0..3)
        .flat_map(|antenna| (0..2).map(move |receptor| (antenna, receptor)))
        .map(|(antenna, receptor)| screen(&term, antenna, receptor))
        .collect();
    for (index, a) in screens.iter().enumerate() {
        for b in &screens[index + 1..] {
            assert_ne!(a, b, "two screens coincide");
        }
    }
    assert_eq!(fbm_gains(7, 0.1, 3, integrations(40)), term);
    assert_ne!(fbm_gains(8, 0.1, 3, integrations(40)).values, term.values);
}

/// A chunk takes the even slot nearest its time, and a row the chunk
/// nearest its time in its window, the earlier of two as near.
#[test]
fn rows_take_the_nearest_solution() {
    let term = fbm_gains(1, 0.1, 2, integrations(10));
    // Integration centres 1005, 1015, …; slots at 1005 + 2.5 + 5k: the
    // chunk at 1015 s is 2.5 s from slots 1 (1012.5) and 2 (1017.5) and
    // takes slot 1.
    let solution = |index: usize| term.solutions[0][index];
    assert_eq!((solution(1).time_s, solution(1).block), (1015.0, 1));
    assert_eq!((solution(0).time_s, solution(0).block), (1005.0, 0));
    let at = |time_s: f64| term.at(0, time_s, 1);
    let chunk = |index: usize| term.values[solution(index).block as usize * 2 + 1];
    assert_eq!(at(1015.0)[0], chunk(1)[0]);
    assert_eq!(at(1019.9)[0], chunk(1)[0]);
    // Halfway between chunks 1 and 2: the earlier.
    assert_eq!(at(1020.0)[0], chunk(1)[0]);
    assert_eq!(at(1020.1)[0], chunk(2)[0]);
    assert_eq!(at(0.0)[0], chunk(0)[0]);
    assert_eq!(at(1e9)[0], chunk(9)[0]);
}

/// A gain multiplies each correlation by `g_p[r₁]·conj(g_q[r₂])`, every
/// channel alike.
#[test]
fn a_gain_multiplies_each_correlation_by_its_receptors_gains() {
    let mut chunks = Chunks::default();
    chunks.push(0, 0.0, 0);
    let term = JonesTerm {
        kind: JonesKind::Gain,
        antennas: 2,
        solutions: chunks.by_window,
        values: vec![
            [Complex32::new(1.1, 0.2), Complex32::new(0.9, -0.1)],
            [Complex32::new(0.8, 0.3), Complex32::new(1.2, 0.4)],
        ],
    };
    let model = [
        Complex32::new(1.0, 0.5),
        Complex32::new(0.1, -0.2),
        Complex32::new(-0.3, 0.1),
        Complex32::new(2.0, 0.0),
    ];
    let mut cell: Vec<Complex32> = model.iter().chain(&model).copied().collect();
    term.apply((0, 0.0), (0, 1), &LINEAR, &mut cell);
    let [p, q] = [term.values[0], term.values[1]];
    for (index, value) in cell.iter().enumerate() {
        let [r1, r2] = LINEAR[index % 4];
        assert_eq!(*value, model[index % 4] * (p[r1] * q[r2].conj()));
    }
}

/// Leakage is CASA's linearised `D`: the cross hands gain `d_px·YY +
/// conj(d_qy)·XX` and `d_py·XX + conj(d_qx)·YY`, the parallel hands are
/// left as they are (measured on CASA 6.7.6 to 5·10⁻⁸).
#[test]
fn leakage_moves_only_the_cross_hands_to_first_order() {
    let mut chunks = Chunks::default();
    chunks.push(0, 0.0, 0);
    let term = JonesTerm {
        kind: JonesKind::Leakage,
        antennas: 2,
        solutions: chunks.by_window,
        values: vec![
            [Complex32::new(0.03, -0.01), Complex32::new(-0.02, 0.04)],
            [Complex32::new(0.05, 0.02), Complex32::new(0.01, -0.03)],
        ],
    };
    let (xx, xy, yx, yy) = (
        Complex32::new(1.3, 0.0),
        Complex32::new(0.2, 0.05),
        Complex32::new(0.2, -0.05),
        Complex32::new(0.7, 0.0),
    );
    let mut cell = vec![xx, xy, yx, yy];
    term.apply((0, 0.0), (0, 1), &LINEAR, &mut cell);
    let [p, q] = [term.values[0], term.values[1]];
    assert_eq!(cell[0], xx);
    assert_eq!(cell[3], yy);
    assert_eq!(cell[1], xy + (p[0] * yy + q[1].conj() * xx));
    assert_eq!(cell[2], yx + (p[1] * xx + q[0].conj() * yy));
}

/// A random gain is one plus normal draws scaled by the complex
/// amplitude, drawn afresh each integration, every antenna and receptor
/// apart.
#[test]
fn random_gains_scatter_about_one() {
    let camp = Complex64::new(0.04, 0.02);
    let term = random_gains(9, camp, 10, integrations(100));
    assert_eq!(term.kind, JonesKind::Gain);
    for receptor in 0..2 {
        let values = term.values.iter().map(|values| values[receptor]);
        let (re_mean, re_rms) = mean_and_rms(values.clone().map(|g| f64::from(g.re)));
        let (im_mean, im_rms) = mean_and_rms(values.map(|g| f64::from(g.im)));
        // 1,000 draws: means within 5 standard errors.
        assert!((re_mean - 1.0).abs() < 5.0 * camp.re / 1000_f64.sqrt());
        assert!(im_mean.abs() < 5.0 * camp.im / 1000_f64.sqrt());
        assert!((re_rms / camp.re - 1.0).abs() < 0.1, "re rms {re_rms}");
        assert!((im_rms / camp.im - 1.0).abs() < 0.1, "im rms {im_rms}");
    }
    assert_ne!(term.values[0][0], term.values[0][1], "receptors draw apart");
    assert_ne!(term.values[0], term.values[1], "antennas draw apart");
    assert_ne!(term.values[0], term.values[10], "integrations draw apart");
    assert_ne!(
        term.values,
        random_gains(10, camp, 10, integrations(100)).values,
        "seeds draw apart"
    );
}

/// A leakage is the offset plus normal draws scaled by the complex
/// amplitude, the second receptor's offset with its real part negated,
/// and a chunk's draws apart from another's.
#[test]
fn leakages_scatter_about_their_offsets() {
    let mut chunks = Chunks::default();
    for window in 0..200 {
        chunks.push(window, 0.0, 0);
    }
    let camp = Complex64::new(0.05, 0.02);
    let offset = Complex64::new(0.03, 0.01);
    let term = leakages(5, (camp, offset), 10, chunks);
    for (receptor, expected) in [(0, offset), (1, Complex64::new(-0.03, 0.01))] {
        let values = term.values.iter().map(|values| values[receptor]);
        let (re_mean, re_rms) = mean_and_rms(values.clone().map(|d| f64::from(d.re)));
        let (im_mean, im_rms) = mean_and_rms(values.map(|d| f64::from(d.im)));
        // 2,000 draws: means within 5 standard errors.
        assert!((re_mean - expected.re).abs() < 5.0 * camp.re / 2000_f64.sqrt());
        assert!((im_mean - expected.im).abs() < 5.0 * camp.im / 2000_f64.sqrt());
        assert!((re_rms / camp.re - 1.0).abs() < 0.1, "re rms {re_rms}");
        assert!((im_rms / camp.im - 1.0).abs() < 0.1, "im rms {im_rms}");
    }
    assert_ne!(term.values[0], term.values[10], "chunks draw apart");
}
