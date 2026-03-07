// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demonstration of the `Image` API, mirroring C++ `tPagedImage.cc`.
//!
//! Creates a 2D image, fills it with data, reads it back, attaches
//! metadata, iterates in chunks, and exercises mask operations.

use casacore_coordinates::CoordinateSystem;
use casacore_images::{
    GaussianBeam, Image, ImageBeamSet, ImageChunk, ImageInfo, ImageIter, ImageIterMut, ImageType,
    TempImage,
};
use casacore_types::{RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

fn main() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let img_path = dir.path().join("demo.image");

    // 1. Create a 64×64 2D image with default coordinates.
    let shape = vec![64, 64];
    let cs = CoordinateSystem::new();
    let mut img = Image::create(shape.clone(), cs, &img_path).expect("create image");
    println!(
        "Created image: shape={:?}, tile_shape={:?}, path={:?}",
        img.shape(),
        img.tile_shape(),
        img.name().unwrap()
    );

    // 2. Fill with ramp data via put_slice.
    let ramp: Vec<f32> = (0..64 * 64).map(|i| i as f32).collect();
    let arr = ArrayD::from_shape_vec(IxDyn(&shape), ramp).expect("valid shape");
    img.put_slice(&arr, &[0, 0]).expect("put_slice");
    println!("Filled with ramp data (0..4095)");

    // 3. Read back and verify.
    let data = img.get().expect("get");
    assert_eq!(data.shape(), &[64, 64]);
    assert_eq!(data[[0, 0]], 0.0);
    assert_eq!(data[[63, 63]], 4095.0);
    println!(
        "Verified: [0,0]={}, [63,63]={}",
        data[[0, 0]],
        data[[63, 63]]
    );

    // Single pixel access.
    assert_eq!(img.get_at(&[10, 20]).unwrap(), 10.0 * 64.0 + 20.0);
    // Note: actually [10, 20] = 10 + 20*64 in row-major... let's just use get_at.
    let val = img.get_at(&[10, 20]).unwrap();
    println!("get_at([10,20]) = {val}");

    // 4. Set metadata.
    img.set_units("Jy/beam").expect("set_units");
    println!("Units set to: {}", img.units());

    let beam = GaussianBeam::new(1e-4, 5e-5, std::f64::consts::FRAC_PI_4);
    let info = ImageInfo {
        beam_set: ImageBeamSet::new(beam),
        image_type: ImageType::Intensity,
        object_name: "CasA".into(),
    };
    img.set_image_info(&info).expect("set_image_info");
    println!(
        "Image info: object={}, type={:?}",
        info.object_name, info.image_type
    );

    let mut misc = RecordValue::default();
    misc.upsert(
        "observer",
        Value::Scalar(ScalarValue::String("Demo User".into())),
    );
    misc.upsert(
        "telescope",
        Value::Scalar(ScalarValue::String("VLA".into())),
    );
    img.set_misc_info(misc).expect("set_misc_info");
    println!("Misc info: observer=Demo User, telescope=VLA");

    // 5. Save, reopen, verify all metadata persists.
    img.save().expect("save");
    println!("Saved to disk");

    let img2 = Image::open(&img_path).expect("reopen");
    assert_eq!(img2.shape(), &[64, 64]);
    assert_eq!(img2.units(), "Jy/beam");
    let info2 = img2.image_info().expect("image_info");
    assert_eq!(info2.object_name, "CasA");
    assert_eq!(info2.image_type, ImageType::Intensity);
    let misc2 = img2.misc_info();
    assert_eq!(
        misc2.get("observer"),
        Some(&Value::Scalar(ScalarValue::String("Demo User".into())))
    );
    println!("Reopened and verified metadata");

    // 6. Iterate chunks with ImageIter.
    let mut chunk_sum = 0.0f64;
    let mut chunk_count = 0usize;
    for chunk_result in ImageIter::new(&img2, vec![16, 16]) {
        let ImageChunk {
            data,
            origin,
            shape: cs,
        } = chunk_result.expect("chunk");
        chunk_sum += data.sum() as f64;
        chunk_count += 1;
        if chunk_count <= 2 {
            println!(
                "  Chunk {chunk_count}: origin={origin:?}, shape={cs:?}, sum={:.0}",
                data.sum()
            );
        }
    }
    let expected_sum: f64 = (0..4096).map(|i| i as f64).sum();
    assert!((chunk_sum - expected_sum).abs() < 1.0);
    println!("Iterated {chunk_count} chunks (16×16), total sum={chunk_sum:.0}");

    // 7. Create and apply a mask.
    let mut img3 = Image::open(&img_path).expect("reopen for mask");
    img3.make_mask("quality", true, true).expect("make_mask");
    assert!(img3.has_pixel_mask());
    assert_eq!(img3.default_mask_name().as_deref(), Some("quality"));

    // Flag some pixels (set to false).
    let mut mask = img3.get_mask().expect("get_mask").expect("has mask");
    mask[[0, 0]] = false;
    mask[[63, 63]] = false;
    img3.put_mask("quality", &mask).expect("put_mask");
    let mask_back = img3.get_mask().expect("get_mask").expect("has mask");
    assert!(!mask_back[[0, 0]]);
    assert!(mask_back[[1, 1]]);
    println!("Mask 'quality' created, 2 pixels flagged");

    // 8. Add history entries.
    img3.add_history("Created by t_image demo")
        .expect("add_history");
    img3.add_history("Added ramp data").expect("add_history");
    img3.add_history("Set units and beam").expect("add_history");
    let history = img3.history().expect("history");
    assert_eq!(history.len(), 3);
    println!("History ({} entries):", history.len());
    for entry in &history {
        println!("  - {entry}");
    }

    // 9. Summary stats.
    let all = img3.get().expect("get");
    let min = all.iter().cloned().fold(f32::INFINITY, f32::min);
    let max = all.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
    let mean = all.iter().map(|&v| v as f64).sum::<f64>() / all.len() as f64;
    println!(
        "Stats: min={min}, max={max}, mean={mean:.1}, npix={}",
        all.len()
    );

    // 10. Mutable iteration: scale all pixels by 0.5.
    let mut img4 = Image::open(&img_path).expect("reopen for mut iter");
    let mut iter = ImageIterMut::new(&mut img4, vec![32, 32]);
    while let Some(Ok(mut chunk)) = iter.next_chunk() {
        chunk.data.mapv_inplace(|v| v * 0.5);
        iter.flush_chunk(&chunk).expect("flush_chunk");
    }
    let scaled = img4.get().expect("get after scale");
    assert_eq!(scaled[[0, 0]], 0.0);
    assert!((scaled[[63, 63]] - 2047.5).abs() < 0.01);
    println!("Scaled all pixels by 0.5 via ImageIterMut");

    // 11. TempImage lifecycle.
    println!("\n--- TempImage ---");
    let mut tmp = TempImage::<f32>::new(vec![8, 8], CoordinateSystem::new()).unwrap();
    println!(
        "TempImage: shape={:?}, in_memory={}, persistent={}, type={}",
        tmp.shape(),
        tmp.is_in_memory(),
        tmp.is_persistent(),
        tmp.image_type_name()
    );
    assert!(tmp.name().is_none());

    tmp.set(42.0).unwrap();
    tmp.set_units("K").unwrap();
    tmp.add_history("created by demo").unwrap();
    tmp.make_mask("flags", true, false).unwrap();

    // Materialize to disk.
    let save_path = dir.path().join("temp_saved.image");
    let reopened = tmp.save_as(&save_path).unwrap();
    assert_eq!(reopened.shape(), &[8, 8]);
    assert_eq!(reopened.units(), "K");
    assert_eq!(reopened.get_at(&[0, 0]).unwrap(), 42.0);
    assert_eq!(reopened.history().unwrap(), vec!["created by demo"]);
    println!(
        "Materialized TempImage to {:?}, verified data + metadata",
        save_path
    );

    // Original TempImage is still valid.
    assert_eq!(tmp.get_at(&[0, 0]).unwrap(), 42.0);
    println!("Original TempImage still valid after save_as");

    // Cleanup is automatic (tempdir drops).
    println!("\nDemo complete.");
}
