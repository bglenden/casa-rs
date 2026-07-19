// SPDX-License-Identifier: LGPL-3.0-or-later
//! Query EOP values for a given MJD from the standard runtime tree.
//!
//! Usage: cargo run --example query_eop -p casa-measures-data -- [MJD]
//!
//! If no MJD is given, uses J2000.0 (51544.5).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mjd: f64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(51544.5);

    let runtime = casa_measures_data::MeasuresRuntime::open_discovered(Default::default())?;
    let eop = runtime.eop()?;
    let source = runtime.root().display();
    let summary = eop.summary();
    println!("{summary}");
    println!("source: {source}");
    println!();

    match eop.interpolate(mjd) {
        Some(vals) => {
            println!("EOP values at MJD {mjd:.2}:");
            println!("  dUT1      = {:+.7} s", vals.dut1_seconds);
            println!("  xp        = {:+.6}\"", vals.x_arcsec);
            println!("  yp        = {:+.6}\"", vals.y_arcsec);
            println!("  dX (nut)  = {:+.3} mas", vals.dx_mas);
            println!("  dY (nut)  = {:+.3} mas", vals.dy_mas);
            println!(
                "  status    = {}",
                if vals.is_predicted {
                    "predicted"
                } else {
                    "measured"
                }
            );
        }
        None => {
            let (start, end) = eop.mjd_range();
            eprintln!("MJD {mjd} is outside table range [{start:.0}, {end:.0}]");
            std::process::exit(1);
        }
    }
    Ok(())
}
