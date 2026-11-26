// SPDX-License-Identifier: LGPL-3.0-or-later
//! Antenna and time selection through the production catalog (tclean
//! contract 34): each selection grids exactly the unflagged MAIN rows CASA's
//! semantics select, counted here from MAIN directly, and the selectors
//! CASA refuses write no images. The grammars themselves are pinned against
//! CASA in `casa-ms` (`antenna_expr`, `time_expr`); the row oracle
//! (`casa-imaging-application/tests/selection_casa_oracle.rs`) checks
//! CASA's own rows on a real MeasurementSet.

use casa_ms::MeasurementSet;
use casa_types::ScalarValue;
use casa_types::quanta::MvTime;
use serde_json::json;

use super::fixture::{Component, Observation};

const POINT: Component = Component {
    offset_px: [6, -4],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

/// One MAIN row: `ANTENNA1`, `ANTENNA2`, `TIME`, and whether `FLAG_ROW`.
type Row = (i32, i32, f64, bool);

fn cell(ms: &MeasurementSet, row: usize, column: &str) -> ScalarValue {
    ms.main_table()
        .cell_accessor(row, column)
        .unwrap()
        .scalar()
        .unwrap()
        .clone()
}

fn main_rows(observation: &Observation) -> (Vec<Row>, Vec<String>) {
    let ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    let rows = (0..ms.row_count())
        .map(|row| {
            let int = |column| match cell(&ms, row, column) {
                ScalarValue::Int32(value) => value,
                other => panic!("{column} is {other:?}"),
            };
            let time = match cell(&ms, row, "TIME") {
                ScalarValue::Float64(value) => value,
                other => panic!("TIME is {other:?}"),
            };
            let flagged = matches!(cell(&ms, row, "FLAG_ROW"), ScalarValue::Bool(true));
            (int("ANTENNA1"), int("ANTENNA2"), time, flagged)
        })
        .collect();
    let antennas = ms.antenna().unwrap();
    let names = (0..antennas.row_count())
        .map(|row| antennas.name(row).unwrap())
        .collect();
    (rows, names)
}

/// A CASA date `YYYY/MM/DD/hh:mm:ss.sss` for `TIME` value `mjd_seconds`.
fn casa_date(mjd_seconds: f64) -> String {
    let time = MvTime::from_mjd_seconds(mjd_seconds);
    format!(
        "{:04}/{:02}/{:02}/{}",
        time.year(),
        time.month(),
        time.month_day(),
        time.format_time(3)
    )
}

/// Images `observation` with `selector` and asserts it grids the share of
/// the unselected run's samples that `selected` rows make of all rows.
fn assert_grids(
    observation: &Observation,
    name: &str,
    selector: serde_json::Value,
    rows: &[Row],
    selected: impl Fn(&Row) -> bool,
) {
    let all = rows.iter().filter(|row| !row.3).count() as u64;
    let held = rows.iter().filter(|row| !row.3 && selected(row)).count() as u64;
    assert!(held > 0 && held < all, "{name}: the case selects some rows");
    let (full, _) = observation.image(&format!("{name}-all"), json!({"niter": 0}));
    let mut controls = json!({"niter": 0});
    controls
        .as_object_mut()
        .unwrap()
        .extend(selector.as_object().unwrap().clone());
    let (report, _) = observation.image(name, controls);
    assert_eq!(
        report.gridded_samples * all,
        full.gridded_samples * held,
        "{name}: {held} of {all} rows"
    );
}

#[test]
fn antenna_selections_grid_the_baselines_casa_selects() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let (rows, names) = main_rows(&observation);
    let cross_with =
        |antenna: i32| move |row: &Row| row.0 != row.1 && (row.0 == antenna || row.1 == antenna);
    assert_grids(
        &observation,
        "antenna-name",
        json!({"antenna": names[0]}),
        &rows,
        cross_with(0),
    );
    assert_grids(
        &observation,
        "antenna-id",
        json!({"antenna": "1"}),
        &rows,
        cross_with(1),
    );
    assert_grids(
        &observation,
        "antenna-baseline",
        json!({"antenna": format!("{}&{}", names[0], names[1])}),
        &rows,
        |row| (row.0, row.1) == (0, 1) || (row.0, row.1) == (1, 0),
    );
    // Negation removes the antenna's cross baselines only: CASA keeps every
    // autocorrelation (MSAntennaParse.cc:91).
    let not_first = cross_with(0);
    assert_grids(
        &observation,
        "antenna-negated",
        json!({"antenna": format!("!{}", names[0])}),
        &rows,
        |row| !not_first(row),
    );
}

#[test]
fn time_selections_grid_the_times_casa_selects() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let (rows, _) = main_rows(&observation);
    let mut times = rows.iter().map(|row| row.2).collect::<Vec<_>>();
    times.sort_by(f64::total_cmp);
    times.dedup();
    assert!(times.len() >= 4, "the fixture holds several times");
    // Bounds between two acquisitions, so no row sits on CASA's millisecond
    // rounding of them.
    let between = |index: usize| (times[index] + times[index + 1]) / 2.0;
    let (low, high) = (between(times.len() / 4), between(3 * times.len() / 4));
    assert_grids(
        &observation,
        "time-before",
        json!({"timerange": format!("<{}", casa_date(low))}),
        &rows,
        |row| row.2 <= low,
    );
    assert_grids(
        &observation,
        "time-after",
        json!({"timerange": format!(">{}", casa_date(high))}),
        &rows,
        |row| row.2 >= high,
    );
    assert_grids(
        &observation,
        "time-range",
        json!({"timerange": format!("{}~{}", casa_date(low), casa_date(high))}),
        &rows,
        |row| row.2 >= low && row.2 <= high,
    );
}

#[test]
fn refused_antenna_and_time_selectors_write_no_images() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    for (index, selector) in [
        json!({"antenna": "  "}),
        json!({"antenna": "none"}),
        json!({"antenna": "NO_SUCH_ANTENNA"}),
        json!({"timerange": "  "}),
        json!({"timerange": "none"}),
        json!({"timerange": "2017-03-01"}),
        json!({"timerange": "09:00:00~08:00:00"}),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("refused-{index}");
        let mut controls = json!({"niter": 0});
        controls
            .as_object_mut()
            .unwrap()
            .extend(selector.as_object().unwrap().clone());
        assert!(
            observation.try_image(&name, controls).is_err(),
            "{selector} is refused"
        );
        assert!(!observation.scratch(&format!("{name}.psf")).exists());
    }
}
