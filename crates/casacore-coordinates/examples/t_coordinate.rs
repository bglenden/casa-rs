// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstration of the coordinate system API, mirroring C++ `tCoordinateSystem`,
//! `tDirectionCoordinate`, `tSpectralCoordinate`, `tStokesCoordinate`,
//! `tLinearCoordinate`, `tTabularCoordinate`, `tProjection`, and `tObsInfo`.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example t_coordinate -p casacore-coordinates
//! ```

use std::f64::consts::FRAC_PI_4;

use casacore_coordinates::fits::{from_fits_header, to_fits_header};
use casacore_coordinates::{
    Coordinate, CoordinateSystem, CoordinateType, DirectionCoordinate, LinearCoordinate, ObsInfo,
    Projection, ProjectionType, SpectralCoordinate, StokesCoordinate, StokesType,
    TabularCoordinate,
};
use casacore_types::measures::direction::DirectionRef;
use casacore_types::measures::frequency::FrequencyRef;

fn main() {
    direction_coordinate_demo();
    spectral_coordinate_demo();
    stokes_coordinate_demo();
    linear_coordinate_demo();
    tabular_coordinate_demo();
    projection_demo();
    coordinate_system_demo();
    obs_info_demo();
    fits_round_trip_demo();

    println!("\nAll coordinate demos completed successfully.");
}

// -----------------------------------------------------------------------
// 1. DirectionCoordinate
// -----------------------------------------------------------------------

fn direction_coordinate_demo() {
    println!("=== DirectionCoordinate ===");

    // SIN projection centered at RA=0, Dec=+45 deg, 1 arcsec pixels.
    let proj = Projection::new(ProjectionType::SIN);
    let crval = [0.0, FRAC_PI_4]; // RA=0, Dec=45 deg
    let cdelt = [-1.0e-4, 1.0e-4]; // ~20 arcsec
    let crpix = [512.0, 512.0];
    let dir = DirectionCoordinate::new(DirectionRef::J2000, proj, crval, cdelt, crpix);

    println!(
        "  ref_frame={:?}, projection={}, n_axes={}",
        dir.direction_ref(),
        dir.projection().projection_type(),
        dir.n_pixel_axes()
    );

    // Convert reference pixel to world -> should recover crval.
    // Note: RA=0 may come back as 2*PI (equivalent on the circle).
    let world = dir.to_world(&[512.0, 512.0]).unwrap();
    println!(
        "  to_world([512, 512]) = [{:.6}, {:.6}] rad",
        world[0], world[1]
    );
    let ra_diff = (world[0] - crval[0])
        .abs()
        .min((world[0] - crval[0]).abs() % (2.0 * std::f64::consts::PI));
    assert!(ra_diff < 1e-10 || (2.0 * std::f64::consts::PI - ra_diff).abs() < 1e-10);
    assert!((world[1] - crval[1]).abs() < 1e-10);

    // Round-trip: world -> pixel -> world.
    let test_pixel = [500.0, 520.0];
    let w = dir.to_world(&test_pixel).unwrap();
    let p = dir.to_pixel(&w).unwrap();
    assert!((p[0] - test_pixel[0]).abs() < 1e-6);
    assert!((p[1] - test_pixel[1]).abs() < 1e-6);
    println!(
        "  Round-trip pixel [500,520] -> world -> pixel: [{:.4}, {:.4}]",
        p[0], p[1]
    );

    // Coordinate trait methods.
    println!("  axis_names={:?}", dir.axis_names());
    println!("  axis_units={:?}", dir.axis_units());
}

// -----------------------------------------------------------------------
// 2. SpectralCoordinate
// -----------------------------------------------------------------------

fn spectral_coordinate_demo() {
    println!("\n=== SpectralCoordinate ===");

    // LSRK frame, 1.42 GHz reference, 1 MHz channels.
    let rest_freq = 1.42040575e9; // HI 21-cm line
    let spec = SpectralCoordinate::new(FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, rest_freq);

    println!(
        "  ref_frame={:?}, rest_freq={:.3e} Hz",
        spec.frequency_ref(),
        spec.rest_frequency()
    );

    // Pixel 0 -> reference frequency.
    let freq = spec.to_world(&[0.0]).unwrap();
    println!("  pixel 0 -> {:.3e} Hz", freq[0]);
    assert!((freq[0] - 1.42e9).abs() < 1.0);

    // Pixel 100 -> 1.42e9 + 100 * 1e6 = 1.52e9 Hz.
    let freq = spec.to_world(&[100.0]).unwrap();
    println!("  pixel 100 -> {:.3e} Hz", freq[0]);
    assert!((freq[0] - 1.52e9).abs() < 1.0);

    // World -> pixel round-trip.
    let pix = spec.to_pixel(&[1.425e9]).unwrap();
    println!("  1.425 GHz -> pixel {:.1}", pix[0]);
    assert!((pix[0] - 5.0).abs() < 1e-6);
}

// -----------------------------------------------------------------------
// 3. StokesCoordinate
// -----------------------------------------------------------------------

fn stokes_coordinate_demo() {
    println!("\n=== StokesCoordinate ===");

    let stokes = StokesCoordinate::new(vec![
        StokesType::I,
        StokesType::Q,
        StokesType::U,
        StokesType::V,
    ]);

    // Pixel 0 -> I (code 1).
    let w = stokes.to_world(&[0.0]).unwrap();
    println!("  pixel 0 -> code {} (I)", w[0]);
    assert_eq!(w[0] as i32, 1);

    // Pixel 3 -> V (code 4).
    let w = stokes.to_world(&[3.0]).unwrap();
    println!("  pixel 3 -> code {} (V)", w[0]);
    assert_eq!(w[0] as i32, 4);

    // StokesType lookup.
    for st in [StokesType::I, StokesType::RR, StokesType::XX] {
        println!("  {} -> code {}", st.name(), st.code());
    }

    // From code.
    let st = StokesType::from_code(2).unwrap();
    assert_eq!(st, StokesType::Q);
    println!("  from_code(2) -> {:?}", st);
}

// -----------------------------------------------------------------------
// 4. LinearCoordinate
// -----------------------------------------------------------------------

fn linear_coordinate_demo() {
    println!("\n=== LinearCoordinate ===");

    let lin = LinearCoordinate::new(
        2,
        vec!["Wavelength".into(), "Delay".into()],
        vec!["m".into(), "s".into()],
    )
    .with_reference_value(vec![1.0, 0.0])
    .with_increment(vec![0.01, 1e-6])
    .with_reference_pixel(vec![50.0, 0.0]);

    println!(
        "  n_axes={}, names={:?}, units={:?}",
        lin.n_pixel_axes(),
        lin.axis_names(),
        lin.axis_units()
    );

    let w = lin.to_world(&[50.0, 0.0]).unwrap();
    println!("  ref pixel [50, 0] -> world [{}, {}]", w[0], w[1]);
    assert!((w[0] - 1.0).abs() < 1e-12);
    assert!((w[1] - 0.0).abs() < 1e-12);

    let w = lin.to_world(&[60.0, 100.0]).unwrap();
    println!("  pixel [60, 100] -> world [{}, {:.6}]", w[0], w[1]);
    assert!((w[0] - 1.1).abs() < 1e-10);
    assert!((w[1] - 1e-4).abs() < 1e-12);
}

// -----------------------------------------------------------------------
// 5. TabularCoordinate
// -----------------------------------------------------------------------

fn tabular_coordinate_demo() {
    println!("\n=== TabularCoordinate ===");

    // Non-linear wavelength axis.
    let pixels = vec![0.0, 1.0, 2.0, 3.0, 4.0];
    let wavelengths = vec![400.0, 450.0, 530.0, 600.0, 700.0]; // nm
    let tab = TabularCoordinate::new(pixels, wavelengths, "Wavelength", "nm");

    println!("  name={:?}, unit={:?}", tab.axis_names(), tab.axis_units());

    // Exact entries.
    let w = tab.to_world(&[0.0]).unwrap();
    println!("  pixel 0 -> {} nm", w[0]);
    assert!((w[0] - 400.0).abs() < 1e-10);

    let w = tab.to_world(&[2.0]).unwrap();
    println!("  pixel 2 -> {} nm", w[0]);
    assert!((w[0] - 530.0).abs() < 1e-10);

    // Interpolated value: pixel 1.5 -> midpoint of 450 and 530 = 490.
    let w = tab.to_world(&[1.5]).unwrap();
    println!("  pixel 1.5 -> {} nm (interpolated)", w[0]);
    assert!((w[0] - 490.0).abs() < 1e-6);

    // World -> pixel round-trip.
    let p = tab.to_pixel(&[490.0]).unwrap();
    println!("  490 nm -> pixel {:.1}", p[0]);
    assert!((p[0] - 1.5).abs() < 1e-6);
}

// -----------------------------------------------------------------------
// 6. Projection
// -----------------------------------------------------------------------

fn projection_demo() {
    println!("\n=== Projection ===");

    let projections = [
        ProjectionType::SIN,
        ProjectionType::TAN,
        ProjectionType::ARC,
        ProjectionType::CAR,
        ProjectionType::SFL,
        ProjectionType::MER,
        ProjectionType::AIT,
        ProjectionType::ZEA,
        ProjectionType::STG,
        ProjectionType::NCP,
    ];

    for pt in projections {
        let proj = Projection::new(pt);
        print!("  {} ", proj.projection_type());
    }
    println!();

    // Parse from name.
    assert_eq!(ProjectionType::from_name("sin"), Some(ProjectionType::SIN));
    assert_eq!(ProjectionType::from_name("BON"), None);
    println!("  from_name('sin') = SIN, from_name('BON') = None");
}

// -----------------------------------------------------------------------
// 7. CoordinateSystem
// -----------------------------------------------------------------------

fn coordinate_system_demo() {
    println!("\n=== CoordinateSystem ===");

    let mut cs = CoordinateSystem::new();

    // Direction
    let proj = Projection::new(ProjectionType::SIN);
    let dir = DirectionCoordinate::new(
        DirectionRef::J2000,
        proj,
        [0.0, FRAC_PI_4],
        [-1e-4, 1e-4],
        [256.0, 256.0],
    );
    cs.add_coordinate(Box::new(dir));

    // Spectral
    let spec = SpectralCoordinate::new(FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, 1.42040575e9);
    cs.add_coordinate(Box::new(spec));

    // Stokes
    let stokes = StokesCoordinate::new(vec![StokesType::I, StokesType::Q]);
    cs.add_coordinate(Box::new(stokes));

    println!(
        "  n_coordinates={}, n_pixel_axes={}, n_world_axes={}",
        cs.n_coordinates(),
        cs.n_pixel_axes(),
        cs.n_world_axes()
    );
    assert_eq!(cs.n_pixel_axes(), 4); // 2 + 1 + 1
    assert_eq!(cs.n_world_axes(), 4);

    // find_coordinate
    let dir_idx = cs.find_coordinate(CoordinateType::Direction);
    let spec_idx = cs.find_coordinate(CoordinateType::Spectral);
    let stokes_idx = cs.find_coordinate(CoordinateType::Stokes);
    let linear_idx = cs.find_coordinate(CoordinateType::Linear);
    println!(
        "  find: Direction={:?}, Spectral={:?}, Stokes={:?}, Linear={:?}",
        dir_idx, spec_idx, stokes_idx, linear_idx
    );
    assert_eq!(dir_idx, Some(0));
    assert_eq!(spec_idx, Some(1));
    assert_eq!(stokes_idx, Some(2));
    assert_eq!(linear_idx, None);

    // pixel_to_world via coordinate_system
    let pixel = [256.0, 256.0, 0.0, 0.0];
    let world = cs.to_world(&pixel).unwrap();
    println!(
        "  to_world([256,256,0,0]) = [{:.6}, {:.6}, {:.3e}, {}]",
        world[0], world[1], world[2], world[3]
    );
}

// -----------------------------------------------------------------------
// 8. ObsInfo
// -----------------------------------------------------------------------

fn obs_info_demo() {
    println!("\n=== ObsInfo ===");

    let info = ObsInfo::new("ALMA").with_observer("Demo User");

    println!("  telescope={}, observer={}", info.telescope, info.observer);
    assert_eq!(info.telescope, "ALMA");
    assert_eq!(info.observer, "Demo User");

    // Serialize to record and check.
    let rec = info.to_record();
    println!("  record has {} fields", rec.fields().len());

    // Attach to a CoordinateSystem.
    let cs = CoordinateSystem::new().with_obs_info(info);
    assert_eq!(cs.obs_info().telescope, "ALMA");
    println!(
        "  Attached to CoordinateSystem, telescope={}",
        cs.obs_info().telescope
    );
}

// -----------------------------------------------------------------------
// 9. FITS round-trip
// -----------------------------------------------------------------------

fn fits_round_trip_demo() {
    println!("\n=== FITS Round-Trip ===");

    // Build a coordinate system.
    let mut cs = CoordinateSystem::new();

    let proj = Projection::new(ProjectionType::SIN);
    let dir = DirectionCoordinate::new(
        DirectionRef::J2000,
        proj,
        [0.0, FRAC_PI_4],
        [-1e-4, 1e-4],
        [512.0, 512.0],
    );
    cs.add_coordinate(Box::new(dir));

    let spec = SpectralCoordinate::new(FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, 1.42040575e9);
    cs.add_coordinate(Box::new(spec));

    let stokes = StokesCoordinate::new(vec![StokesType::I, StokesType::V]);
    cs.add_coordinate(Box::new(stokes));

    // Convert to FITS header.
    let shape = [1024, 1024, 256, 2];
    let header = to_fits_header(&cs, &shape);
    println!("  Generated FITS header with {} keywords", header.len());

    // Parse back.
    let cs2 = from_fits_header(&header, &shape).unwrap();
    assert_eq!(cs2.n_coordinates(), cs.n_coordinates());
    assert_eq!(cs2.n_pixel_axes(), cs.n_pixel_axes());
    println!(
        "  Round-trip: n_coordinates={}, n_pixel_axes={}",
        cs2.n_coordinates(),
        cs2.n_pixel_axes()
    );

    // Verify direction coordinate round-trips.
    let dir_idx = cs2.find_coordinate(CoordinateType::Direction).unwrap();
    let dir2 = cs2.coordinate(dir_idx);
    let w = dir2.to_world(&[512.0, 512.0]).unwrap();
    // RA=0 may come back as 2*PI (equivalent on the circle).
    let ra_diff = w[0] % (2.0 * std::f64::consts::PI);
    assert!(ra_diff.abs() < 1e-8 || (2.0 * std::f64::consts::PI - ra_diff).abs() < 1e-8);
    assert!((w[1] - FRAC_PI_4).abs() < 1e-8);
    println!(
        "  Direction ref pixel -> world: [{:.6}, {:.6}] rad (matches original)",
        w[0], w[1]
    );
}
