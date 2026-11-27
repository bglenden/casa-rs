// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

// ---------------------------------------------------------------------------
// Plane-by-plane I/O through the typed tiled-array storage seam
// ---------------------------------------------------------------------------

#[test]
fn plane_by_plane_perf() {
    if !casacore_oracle_available() {
        eprintln!("skipping plane_by_plane_perf: C++ casacore not available");
        return;
    }

    // Use 256³ by default. Set CASA_RS_LARGE_PERF=1 for 1024³.
    let size: usize = if std::env::var("CASA_RS_LARGE_PERF").is_ok() {
        1024
    } else {
        256
    };
    let tile: usize = 32;
    let shape = vec![size, size, size];
    let tile_shape = vec![tile, tile, tile];
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let tile_i32: Vec<i32> = tile_shape.iter().map(|&s| s as i32).collect();
    let n: usize = shape.iter().product();

    let dir = tempfile::tempdir().unwrap();

    // --- C++ benchmark ---
    let cpp_path = dir.path().join("cpp_pbp.image");
    let (cpp_create_ms, cpp_write_ms, cpp_read_ms) =
        ImageOracle::bench_image_plane_by_plane(&cpp_path, &shape_i32, &tile_i32, 0)
            .expect("C++ plane-by-plane benchmark failed");
    let cpp_total_ms = cpp_create_ms + cpp_write_ms + cpp_read_ms;

    // --- Rust benchmark ---
    let rust_path = dir.path().join("rust_pbp.image");

    let t0 = Instant::now();
    let mut img = Image::create_with_tile_shape(
        shape.clone(),
        tile_shape.clone(),
        Default::default(),
        &rust_path,
    )
    .expect("Rust create_with_tile_shape failed");
    let rust_create_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Write plane by plane (z-planes).
    // Pre-allocate a single plane array (matching C++ which reuses one Array).
    let t0 = Instant::now();
    let plane_size = size * size;
    let mut plane = ArrayD::zeros(IxDyn(&[size, size, 1]));
    for z in 0..size {
        // Fill using raw slice access — matches C++ getStorage/putStorage pattern.
        let slice = plane.as_slice_mut().unwrap();
        for x in 0..size {
            for y in 0..size {
                slice[x * size + y] = (x + y * size + z * plane_size) as f32;
            }
        }
        img.put_slice(&plane, &[0, 0, z]).unwrap();
    }
    img.save().unwrap();
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Read and verify plane by plane.
    let t0 = Instant::now();
    let img = Image::open(&rust_path).expect("Rust open failed");
    for z in 0..size {
        let plane = img.get_slice(&[0, 0, z], &[size, size, 1]).unwrap();
        // Spot-check a few pixels per plane.
        if z == 0 || z == size - 1 {
            for x in [0, size / 2, size - 1] {
                for y in [0, size / 2, size - 1] {
                    let expected = (x + y * size + z * plane_size) as f32;
                    assert_eq!(plane[[x, y, 0]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }
    }
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let rust_total_ms = rust_create_ms + rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Plane-by-plane ({size}³ = {n} pixels, {tile}³ tiles):\n  \
         C++:  create {cpp_create_ms:.1} ms, write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: create {rust_create_ms:.1} ms, write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×"
    );

    if ratio > 2.0 {
        eprintln!("WARNING: Rust plane-by-plane is {ratio:.1}× slower than C++ (threshold: 2.0×)");
    }
    if std::env::var("CASA_RS_ENFORCE_PERF").is_ok() {
        assert!(
            ratio <= 2.0,
            "Rust plane-by-plane ratio {ratio:.2}× exceeds 2.0×"
        );
    }
}

// ---------------------------------------------------------------------------
// Plane-by-plane with bounded tile cache (forces real disk I/O)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "large bounded-cache perf comparison; run explicitly when evaluating steady-state disk I/O"]
fn plane_by_plane_bounded_cache_perf() {
    if !casacore_oracle_available() {
        eprintln!("skipping plane_by_plane_bounded_cache_perf: C++ casacore not available");
        return;
    }

    // 1024³ only — smaller sizes fit in cache anyway.
    let size: usize = 1024;
    let tile: usize = 32;
    let shape = vec![size, size, size];
    let tile_shape = vec![tile, tile, tile];
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let tile_i32: Vec<i32> = tile_shape.iter().map(|&s| s as i32).collect();
    let n: usize = shape.iter().product();

    // 128 MiB cache: 32³ f32 tiles = 128 KB each → 1024 tiles fit → one z-plane.
    let cache_bytes: usize = 128 * 1024 * 1024;
    let cache_mib: i32 = 128;

    let dir = tempfile::tempdir().unwrap();

    // --- Rust benchmark (run first to equalize page cache effects) ---
    let rust_path = dir.path().join("rust_pbp_bounded.image");

    let t0 = Instant::now();
    let mut img = Image::create_with_tile_shape_and_cache(
        shape.clone(),
        tile_shape.clone(),
        Default::default(),
        &rust_path,
        cache_bytes,
    )
    .expect("Rust create_with_tile_shape_and_cache failed");
    let rust_create_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let plane_size = size * size;
    let mut plane = ArrayD::zeros(IxDyn(&[size, size, 1]));
    for z in 0..size {
        let slice = plane.as_slice_mut().unwrap();
        for x in 0..size {
            for y in 0..size {
                slice[x * size + y] = (x + y * size + z * plane_size) as f32;
            }
        }
        img.put_slice(&plane, &[0, 0, z]).unwrap();
    }
    img.save().unwrap();
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Read and verify plane by plane.
    let t0 = Instant::now();
    let img = Image::open_with_cache(&rust_path, cache_bytes).expect("Rust open_with_cache failed");
    for z in 0..size {
        let plane = img.get_slice(&[0, 0, z], &[size, size, 1]).unwrap();
        if z == 0 || z == size - 1 {
            for x in [0, size / 2, size - 1] {
                for y in [0, size / 2, size - 1] {
                    let expected = (x + y * size + z * plane_size) as f32;
                    assert_eq!(plane[[x, y, 0]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }
    }
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // --- C++ benchmark ---
    let cpp_path = dir.path().join("cpp_pbp_bounded.image");
    let (cpp_create_ms, cpp_write_ms, cpp_read_ms) =
        ImageOracle::bench_image_plane_by_plane(&cpp_path, &shape_i32, &tile_i32, cache_mib)
            .expect("C++ bounded-cache benchmark failed");
    let cpp_total_ms = cpp_create_ms + cpp_write_ms + cpp_read_ms;

    let rust_total_ms = rust_create_ms + rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Plane-by-plane bounded cache ({size}³ = {n} pixels, {tile}³ tiles, {} MB cache):\n  \
         C++:  create {cpp_create_ms:.1} ms, write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: create {rust_create_ms:.1} ms, write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×",
        cache_bytes / 1024 / 1024,
    );

    if ratio > 2.0 {
        eprintln!(
            "WARNING: Rust plane-by-plane bounded-cache I/O is {ratio:.1}× slower than C++ (threshold: 2.0×)"
        );
    }
}

fn sync_file_tree(path: &Path) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).unwrap() {
            sync_file_tree(&entry.unwrap().path());
        }
    } else if path.is_file() {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
            .sync_all()
            .unwrap();
    }
}

#[cfg(unix)]
fn allocated_file_tree_bytes(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;

    if path.is_dir() {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| allocated_file_tree_bytes(&entry.unwrap().path()))
            .sum()
    } else {
        path.metadata().unwrap().blocks() * 512
    }
}

fn report_cube_progress(stage: &str, completed: usize, channels: usize, started: Instant) {
    if completed == channels || completed.is_multiple_of(1024) {
        let gib = completed as f64 * 4.0 / 1024.0;
        let mib_per_second = completed as f64 * 4.0 / started.elapsed().as_secs_f64();
        eprintln!(
            "{stage}: {completed}/{channels} planes ({gib:.1} GiB), {mib_per_second:.0} MiB/s"
        );
    }
}

fn fill_cube_plane(plane: &mut ArrayD<f32>, base_pattern: &[u32], channel: usize) {
    let channel_mask = (channel as u32).wrapping_mul(0x9e37_79b9).rotate_left(13) ^ 0xa511_e9b3;
    for (value, &base) in plane
        .as_slice_memory_order_mut()
        .unwrap()
        .iter_mut()
        .zip(base_pattern)
    {
        *value = f32::from_bits(base ^ channel_mask);
    }
}

fn cube_plane_bytes(plane: &ArrayD<f32>) -> &[u8] {
    let values = plane.as_slice_memory_order().unwrap();
    unsafe {
        std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values))
    }
}

#[test]
#[ignore = "full physical 50 GiB cube throughput gate; requires CASA_RS_CUBE_PERF_DIR with at least 110 GiB free"]
fn full_spectral_cube_plane_io_tracks_raw_disk_speed() {
    let channels = std::env::var("CASA_RS_CUBE_CHANNELS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(12_800);
    let plane_shape = [1024, 1024, 1, 1];
    let shape = vec![1024, 1024, 1, channels];
    let mut pattern_state = 0x4d59_5df4_u32;
    let base_pattern = (0..plane_shape.iter().product())
        .map(|_| {
            pattern_state = pattern_state
                .wrapping_mul(1_664_525)
                .wrapping_add(1_013_904_223);
            pattern_state
        })
        .collect::<Vec<_>>();
    let mut plane = ArrayD::from_elem(IxDyn(&plane_shape).f(), 0.0f32);
    let plane_bytes = cube_plane_bytes(&plane).len();
    let total_bytes = plane_bytes * channels;
    let benchmark_root = std::env::var_os("CASA_RS_CUBE_PERF_DIR")
        .expect("CASA_RS_CUBE_PERF_DIR must name a physical volume with at least 110 GiB free");
    let directory = tempfile::Builder::new()
        .prefix("casa-rs-full-cube-")
        .tempdir_in(benchmark_root)
        .unwrap();
    eprintln!(
        "full cube benchmark: shape={shape:?}, tile={plane_shape:?}, {:.1} GiB per cube, root={}",
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        directory.path().display()
    );

    let raw_path = directory.path().join("raw-cube.bin");
    let raw_write_start = Instant::now();
    let mut raw_write_io = Duration::ZERO;
    let mut raw = File::create(&raw_path).unwrap();
    for channel in 0..channels {
        fill_cube_plane(&mut plane, &base_pattern, channel);
        let io_start = Instant::now();
        raw.write_all(cube_plane_bytes(&plane)).unwrap();
        raw_write_io += io_start.elapsed();
        report_cube_progress("raw write", channel + 1, channels, raw_write_start);
    }
    let sync_start = Instant::now();
    raw.sync_all().unwrap();
    raw_write_io += sync_start.elapsed();
    let raw_write_seconds = raw_write_io.as_secs_f64();
    drop(raw);

    let image_path = directory.path().join("spectral-cube.image");
    let mut image = PagedImage::<f32>::create_with_tile_shape_and_cache(
        shape,
        plane_shape.to_vec(),
        CoordinateSystem::new(),
        &image_path,
        64 * 1024 * 1024,
    )
    .unwrap();
    let image_write_start = Instant::now();
    let mut image_write_io = Duration::ZERO;
    for channel in 0..channels {
        fill_cube_plane(&mut plane, &base_pattern, channel);
        let io_start = Instant::now();
        image.put_slice(&plane, &[0, 0, 0, channel]).unwrap();
        image_write_io += io_start.elapsed();
        report_cube_progress("image write", channel + 1, channels, image_write_start);
    }
    let sync_start = Instant::now();
    image.save().unwrap();
    sync_file_tree(&image_path);
    image_write_io += sync_start.elapsed();
    let image_write_seconds = image_write_io.as_secs_f64();
    assert_eq!(
        image.tiled_io_stats().unwrap().direct_tile_write_tiles,
        channels
    );
    drop(image);

    let mut raw = File::open(&raw_path).unwrap();
    let raw_read_start = Instant::now();
    let mut raw_read_io = Duration::ZERO;
    for channel in 0..channels {
        // Match PagedImage::get_slice's owned-result contract.
        let mut raw_plane = vec![0u8; plane_bytes];
        let io_start = Instant::now();
        raw.read_exact(&mut raw_plane).unwrap();
        raw_read_io += io_start.elapsed();
        fill_cube_plane(&mut plane, &base_pattern, channel);
        assert!(
            raw_plane == cube_plane_bytes(&plane),
            "raw pixel pattern mismatch in channel {channel}"
        );
        report_cube_progress("raw read", channel + 1, channels, raw_read_start);
    }
    let raw_read_seconds = raw_read_io.as_secs_f64();

    let image = PagedImage::<f32>::open_with_cache(&image_path, 64 * 1024 * 1024).unwrap();
    let image_read_start = Instant::now();
    let mut image_read_io = Duration::ZERO;
    for channel in 0..channels {
        let io_start = Instant::now();
        let read = image.get_slice(&[0, 0, 0, channel], &plane_shape).unwrap();
        image_read_io += io_start.elapsed();
        fill_cube_plane(&mut plane, &base_pattern, channel);
        assert!(
            cube_plane_bytes(&read) == cube_plane_bytes(&plane),
            "image pixel pattern mismatch in channel {channel}"
        );
        report_cube_progress("image read", channel + 1, channels, image_read_start);
    }
    let image_read_seconds = image_read_io.as_secs_f64();

    let raw_write_mib = total_bytes as f64 / raw_write_seconds / (1024.0 * 1024.0);
    let image_write_mib = total_bytes as f64 / image_write_seconds / (1024.0 * 1024.0);
    let raw_read_mib = total_bytes as f64 / raw_read_seconds / (1024.0 * 1024.0);
    let image_read_mib = total_bytes as f64 / image_read_seconds / (1024.0 * 1024.0);
    let write_fraction = image_write_mib / raw_write_mib;
    let read_fraction = image_read_mib / raw_read_mib;
    #[cfg(unix)]
    {
        let minimum_physical_bytes = total_bytes as u64 * 95 / 100;
        assert!(
            allocated_file_tree_bytes(&raw_path) >= minimum_physical_bytes,
            "raw benchmark file is unexpectedly sparse"
        );
        assert!(
            allocated_file_tree_bytes(&image_path) >= minimum_physical_bytes,
            "image benchmark tree is unexpectedly sparse"
        );
    }
    eprintln!(
        "spectral plane I/O ({:.1} GiB): raw write {:.0} MiB/s, image write {:.0} MiB/s ({:.0}%); raw read {:.0} MiB/s, image read {:.0} MiB/s ({:.0}%)",
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        raw_write_mib,
        image_write_mib,
        write_fraction * 100.0,
        raw_read_mib,
        image_read_mib,
        read_fraction * 100.0,
    );
    eprintln!("timed I/O excludes deterministic pattern generation and verification");
    assert!(
        write_fraction >= 0.60,
        "image plane write throughput is only {:.0}% of raw disk",
        write_fraction * 100.0
    );
    assert!(
        read_fraction >= 0.50,
        "image plane read throughput is only {:.0}% of raw disk",
        read_fraction * 100.0
    );
}

// ---------------------------------------------------------------------------
// Plane-by-plane Complex32 with bounded tile cache
// ---------------------------------------------------------------------------

#[test]
#[ignore = "large bounded-cache Complex32 perf comparison; run explicitly when evaluating steady-state disk I/O"]
fn plane_by_plane_complex32_bounded_cache_perf() {
    if !casacore_oracle_available() {
        eprintln!(
            "skipping plane_by_plane_complex32_bounded_cache_perf: C++ casacore not available"
        );
        return;
    }

    // 1024³ — same size as f32 bounded cache test.
    let size: usize = 1024;
    let tile: usize = 32;
    let shape = vec![size, size, size];
    let tile_shape = vec![tile, tile, tile];
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let tile_i32: Vec<i32> = tile_shape.iter().map(|&s| s as i32).collect();
    let n: usize = shape.iter().product();

    // 256 MiB cache: 32³ Complex32 tiles = 256 KB each → 1024 tiles fit → one z-plane.
    let cache_bytes: usize = 256 * 1024 * 1024;
    let cache_mib: i32 = 256;

    let dir = tempfile::tempdir().unwrap();

    // --- Rust benchmark (run first to equalize page cache effects) ---
    let rust_path = dir.path().join("rust_pbp_c32.image");

    let t0 = Instant::now();
    let mut img = PagedImage::<Complex32>::create_with_tile_shape_and_cache(
        shape.clone(),
        tile_shape.clone(),
        Default::default(),
        &rust_path,
        cache_bytes,
    )
    .expect("Rust create_with_tile_shape_and_cache failed");
    let rust_create_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let plane_size = size * size;
    let mut plane = ArrayD::zeros(IxDyn(&[size, size, 1]));
    for z in 0..size {
        let slice = plane.as_slice_mut().unwrap();
        for x in 0..size {
            for y in 0..size {
                let val = (x + y * size + z * plane_size) as f32;
                slice[x * size + y] = Complex32::new(val, -val);
            }
        }
        img.put_slice(&plane, &[0, 0, z]).unwrap();
    }
    img.save().unwrap();
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Read and verify plane by plane.
    let t0 = Instant::now();
    let img = PagedImage::<Complex32>::open_with_cache(&rust_path, cache_bytes)
        .expect("Rust open_with_cache failed");
    for z in 0..size {
        let plane = img.get_slice(&[0, 0, z], &[size, size, 1]).unwrap();
        if z == 0 || z == size - 1 {
            for x in [0, size / 2, size - 1] {
                for y in [0, size / 2, size - 1] {
                    let val = (x + y * size + z * plane_size) as f32;
                    let expected = Complex32::new(val, -val);
                    assert_eq!(plane[[x, y, 0]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }
    }
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // --- C++ benchmark ---
    let cpp_path = dir.path().join("cpp_pbp_c32.image");
    let (cpp_create_ms, cpp_write_ms, cpp_read_ms) =
        ImageOracle::bench_image_plane_by_plane_complex(
            &cpp_path, &shape_i32, &tile_i32, cache_mib,
        )
        .expect("C++ complex32 plane-by-plane benchmark failed");
    let cpp_total_ms = cpp_create_ms + cpp_write_ms + cpp_read_ms;
    let rust_total_ms = rust_create_ms + rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Plane-by-plane Complex32 bounded cache ({size}³ = {n} pixels, {tile}³ tiles, {} MB cache):\n  \
         C++:  create {cpp_create_ms:.1} ms, write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: create {rust_create_ms:.1} ms, write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×",
        cache_bytes / 1024 / 1024,
    );

    if ratio > 2.0 {
        eprintln!(
            "WARNING: Rust Complex32 plane-by-plane bounded-cache I/O is {ratio:.1}× slower than C++ (threshold: 2.0×)"
        );
    }
}

// ---------------------------------------------------------------------------
// Spectrum-by-spectrum with bounded tile cache (forces real disk I/O)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "large bounded-cache perf comparison; run explicitly when evaluating steady-state disk I/O"]
fn spectrum_by_spectrum_bounded_cache_perf() {
    if !casacore_oracle_available() {
        eprintln!("skipping spectrum_by_spectrum_bounded_cache_perf: C++ casacore not available");
        return;
    }

    // 1024³ only — smaller sizes fit in cache anyway.
    let size: usize = 1024;
    let tile: usize = 32;
    let shape = vec![size, size, size];
    let tile_shape = vec![tile, tile, tile];
    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    let tile_i32: Vec<i32> = tile_shape.iter().map(|&s| s as i32).collect();
    let n: usize = shape.iter().product();
    let nx = size;
    let ny = size;
    let nz = size;
    let plane_size = nx * ny;

    // 128 MiB cache: holds 1024 tiles → covers one full y-strip of z-columns.
    let cache_bytes: usize = 128 * 1024 * 1024;
    let cache_mib: i32 = 128;

    let dir = tempfile::tempdir().unwrap();

    // --- Rust benchmark (run first to equalize page cache effects) ---
    let rust_path = dir.path().join("rust_sbs_bounded.image");

    let t0 = Instant::now();
    let mut img = Image::create_with_tile_shape_and_cache(
        shape.clone(),
        tile_shape.clone(),
        Default::default(),
        &rust_path,
        cache_bytes,
    )
    .expect("Rust create_with_tile_shape_and_cache failed");
    let rust_create_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Write spectrum by spectrum: iterate y then x (matching C++).
    let t0 = Instant::now();
    let mut spectrum = ArrayD::zeros(IxDyn(&[1, 1, nz]));
    for y in 0..ny {
        for x in 0..nx {
            let slice = spectrum.as_slice_mut().unwrap();
            let base = x + y * nx;
            for (z, val) in slice.iter_mut().enumerate() {
                *val = (base + z * plane_size) as f32;
            }
            img.put_slice(&spectrum, &[x, y, 0]).unwrap();
        }
    }
    img.save().unwrap();
    let rust_write_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Read and verify spectrum by spectrum.
    let t0 = Instant::now();
    let img = Image::open_with_cache(&rust_path, cache_bytes).expect("Rust open_with_cache failed");
    for y in 0..ny {
        for x in 0..nx {
            let spectrum = img.get_slice(&[x, y, 0], &[1, 1, nz]).unwrap();
            // Verify a few values per spectrum at the corners of the (y, x) grid.
            if (y == 0 || y == ny - 1) && (x == 0 || x == nx - 1) {
                for z in [0, nz / 2, nz - 1] {
                    let expected = (x + y * nx + z * plane_size) as f32;
                    assert_eq!(spectrum[[0, 0, z]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }
    }
    let rust_read_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // --- C++ benchmark ---
    let cpp_path = dir.path().join("cpp_sbs_bounded.image");
    let (cpp_create_ms, cpp_write_ms, cpp_read_ms) =
        ImageOracle::bench_image_spectrum_by_spectrum(&cpp_path, &shape_i32, &tile_i32, cache_mib)
            .expect("C++ spectrum-by-spectrum benchmark failed");
    let cpp_total_ms = cpp_create_ms + cpp_write_ms + cpp_read_ms;

    let rust_total_ms = rust_create_ms + rust_write_ms + rust_read_ms;
    let ratio = rust_total_ms / cpp_total_ms.max(0.001);

    eprintln!(
        "Spectrum-by-spectrum bounded cache ({size}³ = {n} pixels, {tile}³ tiles, {} MB cache):\n  \
         C++:  create {cpp_create_ms:.1} ms, write {cpp_write_ms:.1} ms, read {cpp_read_ms:.1} ms, total {cpp_total_ms:.1} ms\n  \
         Rust: create {rust_create_ms:.1} ms, write {rust_write_ms:.1} ms, read {rust_read_ms:.1} ms, total {rust_total_ms:.1} ms\n  \
         Ratio: {ratio:.2}×",
        cache_bytes / 1024 / 1024,
    );

    if ratio > 2.0 {
        eprintln!(
            "WARNING: Rust spectrum-by-spectrum bounded-cache I/O is {ratio:.1}× slower than C++ (threshold: 2.0×)"
        );
    }
}
