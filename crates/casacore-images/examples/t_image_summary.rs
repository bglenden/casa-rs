// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstration of image metadata summary, mirroring C++ `dImageSummary.cc`.
//!
//! Creates a small test image with coordinates, units, and beam info,
//! then prints a summary of all metadata. This mirrors the C++ casacore
//! `ImageSummary` workflow for inspecting image properties.
//!
//! ```sh
//! cargo run --example t_image_summary -p casacore-images
//! ```

use casacore_coordinates::{
    CoordinateSystem, LinearCoordinate, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casacore_images::{GaussianBeam, Image, ImageBeamSet, ImageInfo, ImageType};
use casacore_types::measures::frequency::FrequencyRef;

fn main() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let img_path = dir.path().join("summary_demo.image");

    // 1. Build a coordinate system: 2-axis spatial + spectral + stokes.
    let mut cs = CoordinateSystem::new();

    let spatial = LinearCoordinate::new(
        2,
        vec!["Right Ascension".into(), "Declination".into()],
        vec!["rad".into(), "rad".into()],
    );
    cs.add_coordinate(Box::new(spatial));

    let spectral = SpectralCoordinate::new(FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, 1.42040575e9);
    cs.add_coordinate(Box::new(spectral));

    let stokes = StokesCoordinate::new(vec![StokesType::I, StokesType::Q]);
    cs.add_coordinate(Box::new(stokes));

    // 2. Create a 16x16x8x2 image (RA x Dec x Freq x Stokes).
    let shape = vec![16, 16, 8, 2];
    let mut img = Image::create(shape, cs, &img_path).expect("create image");

    // 3. Attach metadata.
    img.set_units("Jy/beam").expect("set_units");

    let beam = GaussianBeam::new(1e-4, 5e-5, std::f64::consts::FRAC_PI_4);
    let info = ImageInfo {
        beam_set: ImageBeamSet::new(beam),
        image_type: ImageType::Intensity,
        object_name: "NGC4826".into(),
    };
    img.set_image_info(&info).expect("set_image_info");

    img.save().expect("save image");

    // 4. Reopen and print summary.
    let img = Image::open(&img_path).expect("open image");

    println!("=== Image Summary ===\n");

    // Basic properties.
    println!("Name:       {:?}", img.name().unwrap().display());
    println!("Type:       {}", img.image_type_name());
    println!("Shape:      {:?}", img.shape());
    println!("Ndim:       {}", img.ndim());
    println!("Nelements:  {}", img.nelements());
    println!("Tile shape: {:?}", img.tile_shape());

    // Units.
    println!("Units:      {}", img.units());
    assert_eq!(img.units(), "Jy/beam");

    // Image info.
    let img_info = img.image_info().expect("image_info");
    println!("\nImage Info:");
    println!("  Object name: {}", img_info.object_name);
    println!("  Image type:  {}", img_info.image_type);
    assert_eq!(img_info.object_name, "NGC4826");
    assert_eq!(img_info.image_type, ImageType::Intensity);

    // Beam.
    if let Some(beam) = img_info.beam_set.single_beam() {
        println!("  Beam:");
        println!("    Major axis:      {:.6e} rad", beam.major);
        println!("    Minor axis:      {:.6e} rad", beam.minor);
        println!("    Position angle:  {:.4} rad", beam.position_angle);
        println!("    Area:            {:.6e} sr", beam.area());
        assert!((beam.major - 1e-4).abs() < 1e-10);
        assert!((beam.minor - 5e-5).abs() < 1e-10);
    } else {
        println!("  Beam: (none or multi-beam)");
    }

    // Coordinates.
    let coords = img.coordinates();
    println!("\nCoordinate System:");
    println!("  Number of coordinates: {}", coords.n_coordinates());
    println!("  Total pixel axes:      {}", coords.n_pixel_axes());
    println!("  Total world axes:      {}", coords.n_world_axes());

    for i in 0..coords.n_coordinates() {
        let coord = coords.coordinate(i);
        println!("\n  Coordinate {i}:");
        println!("    Type:          {}", coord.coordinate_type());
        println!("    Pixel axes:    {}", coord.n_pixel_axes());
        println!("    World axes:    {}", coord.n_world_axes());
        println!("    Axis names:    {:?}", coord.axis_names());
        println!("    Axis units:    {:?}", coord.axis_units());
        println!("    Ref. value:    {:?}", coord.reference_value());
        println!("    Ref. pixel:    {:?}", coord.reference_pixel());
        println!("    Increment:     {:?}", coord.increment());
    }

    // Observation info.
    let obs = coords.obs_info();
    println!("\nObservation Info:");
    println!("  Telescope:  {}", obs.telescope);
    println!("  Observer:   {}", obs.observer);

    println!("\n=== Summary Complete ===");
    println!("\nImage summary demo completed successfully.");
}
