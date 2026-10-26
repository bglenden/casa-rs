// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2/MSv4 trial's padding preparation for the scale check
//! (`docs/measurement-set/trial/README.md`): removes every MAIN row of one
//! antenna from the second half of the times it has rows at, in place, as an
//! antenna that leaves the array early does. The trial adapter then pads
//! those cells, so every later batch takes its placement path.
//!
//! ```text
//! cp -R pilot.ms padded.ms
//! cargo run --release -p casa-ms --example ms_trial_pad -- padded.ms ANTENNA_ID
//! ```

use casa_ms::MeasurementSet;
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};

const USAGE: &str = "usage: ms_trial_pad MS ANTENNA_ID (edits MS in place: run it on a copy)";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or(USAGE)?;
    let antenna: i32 = args.next().ok_or(USAGE)?.parse()?;
    let mut ms = MeasurementSet::open(&path)?;
    let main = ms.main_table();
    let all: Vec<usize> = (0..main.row_count()).collect();
    let (mut time, mut antenna1, mut antenna2) = (Vec::new(), Vec::new(), Vec::new());
    main.required_scalar_columns_for_rows_into(
        &all,
        &mut [
            RequiredScalarColumnDestination::new(
                "TIME",
                RequiredScalarColumnValuesMut::Float64(&mut time),
            ),
            RequiredScalarColumnDestination::new(
                "ANTENNA1",
                RequiredScalarColumnValuesMut::Int32(&mut antenna1),
            ),
            RequiredScalarColumnDestination::new(
                "ANTENNA2",
                RequiredScalarColumnValuesMut::Int32(&mut antenna2),
            ),
        ],
    )?;
    let with: Vec<usize> = all
        .into_iter()
        .filter(|&row| antenna1[row] == antenna || antenna2[row] == antenna)
        .collect();
    let mut times: Vec<f64> = with.iter().map(|&row| time[row]).collect();
    times.sort_by(f64::total_cmp);
    times.dedup();
    let halfway = times
        .get(times.len() / 2)
        .copied()
        .ok_or("the antenna has no rows")?;
    let removed: Vec<usize> = with
        .into_iter()
        .filter(|&row| time[row] >= halfway)
        .collect();
    ms.main_table_mut().remove_rows(&removed)?;
    ms.save()?;
    println!(
        "removed {} of {} rows: antenna {antenna} from its time {} of {}",
        removed.len(),
        time.len(),
        times.len() / 2,
        times.len()
    );
    Ok(())
}
