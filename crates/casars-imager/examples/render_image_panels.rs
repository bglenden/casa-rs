// SPDX-License-Identifier: LGPL-3.0-or-later

use std::env;
use std::path::{Path, PathBuf};

use casa_images::PagedImage;
use image::{ImageBuffer, Rgb, RgbImage};
use ndarray::{Array2, ArrayD, IxDyn};

struct Args {
    rust_path: PathBuf,
    casa_path: PathBuf,
    output_path: PathBuf,
    channel: usize,
}

#[derive(Clone, Copy, Debug)]
struct PlaneStats {
    min: f32,
    max: f32,
    mean: f32,
    rms: f32,
    min_location: (usize, usize),
    max_location: (usize, usize),
    max_abs: f32,
    max_abs_location: (usize, usize),
}

fn main() -> Result<(), String> {
    let args = parse_args(env::args().skip(1).collect())?;
    let rust_image = read_image(&args.rust_path)?;
    let casa_image = read_image(&args.casa_path)?;
    let rust_plane = extract_channel_plane(&rust_image, args.channel)?;
    let casa_plane = extract_channel_plane(&casa_image, args.channel)?;
    let diff_plane = &rust_plane - &casa_plane;

    let rust_stats = plane_stats(&rust_plane);
    let casa_stats = plane_stats(&casa_plane);
    let diff_stats = plane_stats(&diff_plane);

    let panel = compose_panels(&[
        render_plane(&rust_plane),
        render_plane(&casa_plane),
        render_plane(&diff_plane),
    ]);
    panel
        .save(&args.output_path)
        .map_err(|error| format!("write {}: {error}", args.output_path.display()))?;

    println!(
        "rust min={:.6}@{:?} max={:.6}@{:?} maxabs={:.6}@{:?} mean={:.6} rms={:.6}",
        rust_stats.min,
        rust_stats.min_location,
        rust_stats.max,
        rust_stats.max_location,
        rust_stats.max_abs,
        rust_stats.max_abs_location,
        rust_stats.mean,
        rust_stats.rms
    );
    println!(
        "casa min={:.6}@{:?} max={:.6}@{:?} maxabs={:.6}@{:?} mean={:.6} rms={:.6}",
        casa_stats.min,
        casa_stats.min_location,
        casa_stats.max,
        casa_stats.max_location,
        casa_stats.max_abs,
        casa_stats.max_abs_location,
        casa_stats.mean,
        casa_stats.rms
    );
    println!(
        "diff(rust-casa) min={:.6}@{:?} max={:.6}@{:?} maxabs={:.6}@{:?} mean={:.6} rms={:.6}",
        diff_stats.min,
        diff_stats.min_location,
        diff_stats.max,
        diff_stats.max_location,
        diff_stats.max_abs,
        diff_stats.max_abs_location,
        diff_stats.mean,
        diff_stats.rms
    );
    Ok(())
}

fn parse_args(args: Vec<String>) -> Result<Args, String> {
    let mut rust_path = None;
    let mut casa_path = None;
    let mut output_path = None;
    let mut channel = 0usize;
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--rust" => {
                i += 1;
                rust_path = args.get(i).map(PathBuf::from);
            }
            "--casa" => {
                i += 1;
                casa_path = args.get(i).map(PathBuf::from);
            }
            "--output" => {
                i += 1;
                output_path = args.get(i).map(PathBuf::from);
            }
            "--channel" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "missing value for --channel".to_string())?;
                channel = value
                    .parse::<usize>()
                    .map_err(|error| format!("parse --channel {value:?}: {error}"))?;
            }
            "--help" | "-h" => {
                return Err(help_text().to_string());
            }
            other => {
                return Err(format!("unknown argument {other:?}\n\n{}", help_text()));
            }
        }
        i += 1;
    }

    Ok(Args {
        rust_path: rust_path.ok_or_else(|| "missing --rust".to_string())?,
        casa_path: casa_path.ok_or_else(|| "missing --casa".to_string())?,
        output_path: output_path.ok_or_else(|| "missing --output".to_string())?,
        channel,
    })
}

fn help_text() -> &'static str {
    "Usage: cargo run -p casars-imager --example render_image_panels -- \
  --rust PATH --casa PATH --output PNG [--channel N]"
}

fn read_image(path: &Path) -> Result<ArrayD<f32>, String> {
    let image = PagedImage::<f32>::open(path)
        .map_err(|error| format!("open {}: {error}", path.display()))?;
    image
        .get_slice(&[0, 0, 0, 0], image.shape())
        .map_err(|error| format!("read {}: {error}", path.display()))
}

fn extract_channel_plane(image: &ArrayD<f32>, channel: usize) -> Result<Array2<f32>, String> {
    if image.ndim() != 4 {
        return Err(format!("expected 4-D image, got shape {:?}", image.shape()));
    }
    let shape = image.shape();
    if channel >= shape[3] {
        return Err(format!(
            "channel {} out of range for image with {} channels",
            channel, shape[3]
        ));
    }
    let mut plane = Array2::<f32>::zeros((shape[0], shape[1]));
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            plane[(x, y)] = image[IxDyn(&[x, y, 0, channel])];
        }
    }
    Ok(plane)
}

fn plane_stats(plane: &Array2<f32>) -> PlaneStats {
    let mut min = f32::INFINITY;
    let mut max = f32::NEG_INFINITY;
    let mut sum = 0.0f64;
    let mut sumsq = 0.0f64;
    let mut count = 0usize;
    let mut min_location = (0usize, 0usize);
    let mut max_location = (0usize, 0usize);
    let mut max_abs = 0.0f32;
    let mut max_abs_location = (0usize, 0usize);
    for ((x, y), value) in plane.indexed_iter() {
        let value = *value;
        min = min.min(value);
        max = max.max(value);
        if value == min {
            min_location = (x, y);
        }
        if value == max {
            max_location = (x, y);
        }
        if value.abs() >= max_abs {
            max_abs = value.abs();
            max_abs_location = (x, y);
        }
        sum += value as f64;
        sumsq += (value as f64) * (value as f64);
        count += 1;
    }
    let mean = if count == 0 {
        0.0
    } else {
        (sum / count as f64) as f32
    };
    let rms = if count == 0 {
        0.0
    } else {
        (sumsq / count as f64).sqrt() as f32
    };
    PlaneStats {
        min,
        max,
        mean,
        rms,
        min_location,
        max_location,
        max_abs,
        max_abs_location,
    }
}

fn render_plane(plane: &Array2<f32>) -> RgbImage {
    let width = plane.shape()[0] as u32;
    let height = plane.shape()[1] as u32;
    let stats = plane_stats(plane);
    let mut image = ImageBuffer::<Rgb<u8>, Vec<u8>>::new(width, height);
    for x in 0..width as usize {
        for y in 0..height as usize {
            image.put_pixel(x as u32, y as u32, map_diverging(plane[(x, y)], stats));
        }
    }
    image
}

fn map_diverging(value: f32, stats: PlaneStats) -> Rgb<u8> {
    let white = [255.0f32, 255.0, 255.0];
    let blue = [49.0f32, 130.0, 189.0];
    let red = [222.0f32, 45.0, 38.0];

    if stats.min >= 0.0 {
        let t = if stats.max > 0.0 {
            (value / stats.max).clamp(0.0, 1.0)
        } else {
            0.0
        };
        return lerp_rgb(white, red, t);
    }

    if stats.max <= 0.0 {
        let t = if stats.min < 0.0 {
            (value / stats.min).clamp(0.0, 1.0)
        } else {
            0.0
        };
        return lerp_rgb(white, blue, t);
    }

    if value < 0.0 {
        let t = (value / stats.min).clamp(0.0, 1.0);
        lerp_rgb(white, blue, t)
    } else {
        let t = (value / stats.max).clamp(0.0, 1.0);
        lerp_rgb(white, red, t)
    }
}

fn lerp_rgb(start: [f32; 3], end: [f32; 3], t: f32) -> Rgb<u8> {
    let t = t.clamp(0.0, 1.0);
    Rgb([
        (start[0] + (end[0] - start[0]) * t).round() as u8,
        (start[1] + (end[1] - start[1]) * t).round() as u8,
        (start[2] + (end[2] - start[2]) * t).round() as u8,
    ])
}

fn compose_panels(images: &[RgbImage]) -> RgbImage {
    let panel_gap = 8u32;
    let width = images.iter().map(RgbImage::width).sum::<u32>()
        + panel_gap * images.len().saturating_sub(1) as u32;
    let height = images.iter().map(RgbImage::height).max().unwrap_or(1);
    let mut canvas = ImageBuffer::from_pixel(width, height, Rgb([245, 245, 245]));
    let mut x_offset = 0u32;
    for image in images {
        for x in 0..image.width() {
            for y in 0..image.height() {
                canvas.put_pixel(x_offset + x, y, *image.get_pixel(x, y));
            }
        }
        x_offset += image.width() + panel_gap;
    }
    canvas
}
