// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session memory laws of gains (#841 Phase 3): the first law's base
//! and ten-times-the-rows cases, and the patch's rows law, with an `fbm`
//! and a `random` gain term set before the `corrupt`. A row takes its
//! solution at its TIME, which the patch then reads and admits with its
//! batch; the terms themselves are the session's, admitted when they are
//! set (`session_memory/gain_setters.rs`) and held through `close`. Each
//! law shares the binary's counting heap and runs alone as its other laws
//! do ([`super::alone`]).
//!
//! The first law runs again with both terms writing their calibration
//! tables at `close` (#891 review): a table is built in memory and saved,
//! about 2 KB a row, and `close` admits the largest before writing any.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;

use casa_simulation::{GainSpec, Simulator, TroposphereSpec};

use super::{
    ALLOWANCE_BYTES, BUDGET_32, CASES, EXCESS, Ordering, ROWS_ALLOWANCE_BYTES, alone, case_paths,
    charge_process_caches, excess_now, measure, resources, simulate, warm, write_batch_bytes,
};

/// Whether [`set_gains`] sets gain terms: the gain laws set it, each alone
/// in its process.
static GAINS: AtomicBool = AtomicBool::new(false);

/// Where [`set_gains`]'s terms write their tables, once the table law sets
/// it.
static TABLES: OnceLock<PathBuf> = OnceLock::new();

/// Whether [`set_gains`] sets an `individual` tropospheric term
/// (`settrop`): the troposphere laws set it, each alone in its process.
pub(super) static TROPOSPHERE: AtomicBool = AtomicBool::new(false);

/// An `fbm` and a `random` gain term on `sm`, once a gain law sets
/// [`GAINS`], and a tropospheric term once a troposphere law sets
/// [`TROPOSPHERE`], each writing its table under [`TABLES`] when that is
/// set.
pub(super) fn set_gains(sm: &mut Simulator) {
    let table = |name: &str| {
        TABLES.get().map_or_else(String::new, |directory| {
            directory
                .join(format!("{name}.cal"))
                .to_string_lossy()
                .into_owned()
        })
    };
    if GAINS.load(Ordering::Relaxed) {
        for mode in ["fbm", "random"] {
            sm.set_gain(&GainSpec {
                mode: mode.to_string(),
                table: table(mode),
                amplitude: vec![0.05],
            })
            .expect("setgain");
        }
    }
    if TROPOSPHERE.load(Ordering::Relaxed) {
        sm.set_trop(&TroposphereSpec {
            mode: "individual".to_string(),
            table: table("troposphere"),
            ..TroposphereSpec::default()
        })
        .expect("settrop");
    }
}

#[test]
fn a_gain_session_holds_its_admission_and_no_more_for_more_rows() {
    if !alone("jones::a_gain_session_holds_its_admission_and_no_more_for_more_rows") {
        return;
    }
    let root = tempfile::tempdir().expect("temporary directory");
    GAINS.store(true, Ordering::Relaxed);
    session_law(root.path());
}

/// The first law with each term writing its calibration table at `close`.
#[test]
fn a_gain_session_writing_tables_holds_its_admission_and_no_more_for_more_rows() {
    if !alone("jones::a_gain_session_writing_tables_holds_its_admission_and_no_more_for_more_rows")
    {
        return;
    }
    GAINS.store(true, Ordering::Relaxed);
    table_law();
}

/// The first law with the terms [`set_gains`] sets writing their
/// calibration tables at `close`.
pub(super) fn table_law() {
    let root = tempfile::tempdir().expect("temporary directory");
    let tables = root.path().join("tables");
    std::fs::create_dir(&tables).expect("the tables' directory");
    TABLES.set(tables).expect("set once");
    // The tables are built after the MAIN writer's admission is released,
    // below the session's peak, so the peaks alone do not hold them: at
    // every allocation of the ten-times-the-rows session (tables of 12,000
    // rows), the heap stays within what the reservations hold.
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let case = CASES[1];
    warm(case, root.path(), &measures);
    let (directory, model) = case_paths(case, root.path());
    let before = excess_now();
    EXCESS.store(before, Ordering::Relaxed);
    simulate(case, &directory.join("excess.ms"), &model, &measures);
    let beyond = EXCESS.load(Ordering::Relaxed) - before;
    println!(
        "{} writing tables: at most {beyond} bytes beyond the reservations",
        case.name
    );
    assert!(
        beyond <= ALLOWANCE_BYTES as i64,
        "{}: the heap went {beyond} bytes beyond what the reservations held",
        case.name
    );
    std::fs::remove_dir_all(&directory).expect("remove the case");
    session_law(root.path());
}

/// The base and ten-times-the-rows cases with the terms [`set_gains`]
/// sets, run under `root`.
pub(super) fn session_law(root: &Path) {
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let cases = [CASES[0], CASES[1]];
    let passes = cases.map(|case| warm(case, root, &measures));
    let held = cases.map(|case| measure(case, root, &measures));
    for ((case, pass), session) in cases.iter().zip(&passes).zip(&held) {
        println!(
            "{} with gains: {} rows in {} batches of {}: heap {} to {}, admitted {}",
            case.name,
            pass.rows,
            pass.batches,
            pass.batch_rows,
            session.least,
            session.most,
            session.admitted
        );
        assert!(
            session.most <= session.admitted + ALLOWANCE_BYTES,
            "{}: the heap grew {} bytes, beyond the {} admitted and the allowance",
            case.name,
            session.most,
            session.admitted
        );
    }
    // The terms grow with the integrations and hold their admission from
    // `setgain` (#890 review), so `close`'s writer takes its share of what
    // is left and batches smaller: the session's admission need not grow.
    // Ten times the rows hold no more beyond whatever more was admitted
    // but a little.
    let ([few, many], [_, many_pass]) = (held, passes);
    let allowance = ROWS_ALLOWANCE_BYTES + write_batch_bytes(cases[1], &many_pass);
    let admitted_growth = many.admitted.saturating_sub(few.admitted);
    println!(
        "rows x10 - base with gains: {} bytes, {} admitted beside {} (allowed {allowance})",
        many.least as i64 - few.least as i64,
        many.admitted,
        few.admitted
    );
    assert!(
        many.least <= few.least + admitted_growth + allowance,
        "rows x10 with gains: the heap grew {} bytes, against {} for the base and {admitted_growth} \
         more admitted",
        many.least,
        few.least
    );
}

/// The patch's rows law with gains: the patch also reads each row's TIME.
#[test]
fn patching_with_gains_holds_its_admission_and_no_more_for_more_rows() {
    if !alone("jones::patching_with_gains_holds_its_admission_and_no_more_for_more_rows") {
        return;
    }
    GAINS.store(true, Ordering::Relaxed);
    super::patch::patches_hold_their_admission_for_more_rows();
}
