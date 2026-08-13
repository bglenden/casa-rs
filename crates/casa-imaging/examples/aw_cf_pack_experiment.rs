// SPDX-License-Identifier: LGPL-3.0-or-later
//! Build the experiment-only contiguous pixel representation consumed by
//! `CASA_RS_AWPROJECT_PACKED_CF_EXPERIMENT`.

use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use casa_imaging::AwConvolutionFunctionCache;
use ndarray::Array2;
use num_complex::Complex32;

const MAGIC: [u8; 16] = *b"CASARS_AWCF_V1\0\0";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if !cfg!(target_endian = "little") {
        return Err("packed CF experiment currently requires a little-endian host".into());
    }
    let mut args = env::args_os().skip(1);
    let source = PathBuf::from(
        args.next()
            .ok_or("usage: aw_cf_pack_experiment SOURCE_CFCACHE OUTPUT_FILE")?,
    );
    let output = PathBuf::from(
        args.next()
            .ok_or("usage: aw_cf_pack_experiment SOURCE_CFCACHE OUTPUT_FILE")?,
    );
    if args.next().is_some() {
        return Err("usage: aw_cf_pack_experiment SOURCE_CFCACHE OUTPUT_FILE".into());
    }
    let partial = PathBuf::from(format!("{}.partial", output.display()));
    let started = Instant::now();
    let cache = AwConvolutionFunctionCache::open(&source)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)?;
    let mut writer = BufWriter::with_capacity(8 << 20, file);
    writer.write_all(&MAGIC)?;
    writer.write_all(&cache.identity().metadata_fingerprint.to_le_bytes())?;
    writer.write_all(&(cache.inventory().paired_cells as u64).to_le_bytes())?;

    let keys = cache.keys();
    for (index, key) in keys.iter().copied().enumerate() {
        let cell_started = Instant::now();
        let cell = cache.load(key)?;
        write_plane(&mut writer, &cell.imaging)?;
        write_plane(&mut writer, &cell.weight)?;
        eprintln!(
            "aw_cf_pack_progress pairs_done={} pairs_total={} cell_elapsed_s={:.3} total_elapsed_s={:.3}",
            index + 1,
            keys.len(),
            cell_started.elapsed().as_secs_f64(),
            started.elapsed().as_secs_f64(),
        );
    }
    writer.flush()?;
    let file = writer.into_inner()?;
    file.sync_all()?;
    drop(file);
    fs::rename(&partial, &output)?;
    sync_parent(&output)?;
    eprintln!(
        "aw_cf_pack_complete path={} bytes={} pairs={} elapsed_s={:.3} metadata_fingerprint={:016x}",
        output.display(),
        fs::metadata(&output)?.len(),
        keys.len(),
        started.elapsed().as_secs_f64(),
        cache.identity().metadata_fingerprint,
    );
    Ok(())
}

fn write_plane(
    writer: &mut BufWriter<File>,
    plane: &Array2<Complex32>,
) -> Result<(), Box<dyn std::error::Error>> {
    let values = plane
        .as_slice_memory_order()
        .ok_or("packed CF source plane is not contiguous")?;
    let byte_count = values
        .len()
        .checked_mul(std::mem::size_of::<Complex32>())
        .ok_or("packed CF byte count overflowed")?;
    // SAFETY: `Complex32` is `#[repr(C)]` over two `f32` values and the
    // experiment format is explicitly native little-endian Complex32.
    let bytes = unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), byte_count) };
    writer.write_all(bytes)?;
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let parent = path.parent().ok_or("packed CF output has no parent")?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
