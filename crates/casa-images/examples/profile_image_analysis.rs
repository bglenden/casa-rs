// SPDX-License-Identifier: LGPL-3.0-or-later

use std::error::Error;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use casa_images::{ImmomentsRequest, export_fits, imhead, immoments, import_fits, imstat};

const DEFAULT_CONT_IMAGE: &str = "/Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_cont.image";
const DEFAULT_CUBE_IMAGE: &str = "/Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/alma/first-look/twhya/twhya_n2hp.image";

struct Measurement {
    name: &'static str,
    durations: Vec<Duration>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cont_image = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| DEFAULT_CONT_IMAGE.to_string()),
    );
    let cube_image = PathBuf::from(
        std::env::args()
            .nth(2)
            .unwrap_or_else(|| DEFAULT_CUBE_IMAGE.to_string()),
    );
    let out_dir = PathBuf::from(
        std::env::args()
            .nth(3)
            .unwrap_or_else(|| "target/wdad-wave3-120/inprocess-timing".to_string()),
    );
    fs::create_dir_all(&out_dir)?;

    let input_fits = out_dir.join("input_cont.fits");
    black_box(export_fits(&cont_image, &input_fits, false, true)?);

    let mut measurements = Vec::new();
    measurements.push(measure("imhead_cont", 7, || {
        black_box(imhead(&cont_image)?);
        Ok(())
    })?);
    measurements.push(measure("imstat_cont_box", 7, || {
        black_box(imstat(&cont_image, Some("100,100,150,150"), None, None)?);
        Ok(())
    })?);
    measurements.push(measure("exportfits_cont", 7, || {
        let out = out_dir.join("export_cont.fits");
        black_box(export_fits(&cont_image, out, false, true)?);
        Ok(())
    })?);
    measurements.push(measure("importfits_cont", 7, || {
        let out = out_dir.join("import_cont.image");
        black_box(import_fits(&input_fits, out, true)?);
        Ok(())
    })?);
    measurements.push(measure("immoments_mom0", 7, || {
        black_box(immoments(&ImmomentsRequest {
            imagename: cube_image.clone(),
            outfile: out_dir.join("n2hp.mom0"),
            moments: 0,
            chans: Some("4~12".to_string()),
            includepix: Some([0.03, 100.0]),
            overwrite: true,
        })?);
        Ok(())
    })?);
    measurements.push(measure("immoments_mom1", 7, || {
        black_box(immoments(&ImmomentsRequest {
            imagename: cube_image.clone(),
            outfile: out_dir.join("n2hp.mom1"),
            moments: 1,
            chans: Some("4~12".to_string()),
            includepix: Some([0.06, 100.0]),
            overwrite: true,
        })?);
        Ok(())
    })?);

    println!("| Operation | casa-rs in-process median s | raw s |");
    println!("| --- | ---: | --- |");
    for measurement in measurements {
        let raw = measurement
            .durations
            .iter()
            .map(|duration| format!("{:.6}", duration.as_secs_f64()))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "| `{}` | `{:.6}` | `{}` |",
            measurement.name,
            median_seconds(&measurement.durations),
            raw
        );
    }

    Ok(())
}

fn measure<F>(
    name: &'static str,
    iterations: usize,
    mut f: F,
) -> Result<Measurement, Box<dyn Error>>
where
    F: FnMut() -> Result<(), Box<dyn Error>>,
{
    f()?;
    let mut durations = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        f()?;
        durations.push(start.elapsed());
    }
    Ok(Measurement { name, durations })
}

fn median_seconds(durations: &[Duration]) -> f64 {
    let mut seconds = durations
        .iter()
        .map(Duration::as_secs_f64)
        .collect::<Vec<_>>();
    seconds.sort_by(f64::total_cmp);
    seconds[seconds.len() / 2]
}
