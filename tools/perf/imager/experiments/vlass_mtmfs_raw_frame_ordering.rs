// SPDX-License-Identifier: LGPL-3.0-or-later
//! Offline Float-arithmetic helper for the VLASS MT-MFS ordering certificate.
//!
//! The caller supplies frozen raw TT0/TT1 values, source frequencies, and
//! source phasors. This helper evaluates only the current aligned-frame graph
//! and CASA's raw-frame scale/add graph. It never opens an MS or enters imaging
//! runtime code.

use std::env;
use std::fs;
use std::hint::black_box;
use std::io;
use std::path::Path;

const INPUT_RECORD_BYTES: usize = 48;
const OUTPUT_RECORD_BYTES: usize = 152;

#[derive(Clone, Copy)]
struct Complex32Bits {
    re: f32,
    im: f32,
}

fn rounded(value: f32) -> f32 {
    black_box(value)
}

fn scale_c32(value: Complex32Bits, scalar: f32) -> Complex32Bits {
    Complex32Bits {
        re: rounded(value.re * scalar),
        im: rounded(value.im * scalar),
    }
}

fn add_c32(left: Complex32Bits, right: Complex32Bits) -> Complex32Bits {
    Complex32Bits {
        re: rounded(left.re + right.re),
        im: rounded(left.im + right.im),
    }
}

fn apply_source_phase(value: Complex32Bits, phase: Complex32Bits) -> Complex32Bits {
    let re_re = rounded(value.re * phase.re);
    let im_im = rounded(value.im * phase.im);
    let re_im = rounded(value.re * phase.im);
    let im_re = rounded(value.im * phase.re);
    Complex32Bits {
        re: rounded(re_re - im_im),
        im: rounded(re_im + im_re),
    }
}

fn read_u32(input: &[u8], offset: &mut usize) -> u32 {
    let end = *offset + 4;
    let value = u32::from_le_bytes(input[*offset..end].try_into().expect("four bytes"));
    *offset = end;
    value
}

fn read_u64(input: &[u8], offset: &mut usize) -> u64 {
    let end = *offset + 8;
    let value = u64::from_le_bytes(input[*offset..end].try_into().expect("eight bytes"));
    *offset = end;
    value
}

fn read_c32(input: &[u8], offset: &mut usize) -> Complex32Bits {
    Complex32Bits {
        re: f32::from_bits(read_u32(input, offset)),
        im: f32::from_bits(read_u32(input, offset)),
    }
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_c32(output: &mut Vec<u8>, value: Complex32Bits) {
    push_u32(output, value.re.to_bits());
    push_u32(output, value.im.to_bits());
}

fn push_pair(output: &mut Vec<u8>, values: [Complex32Bits; 2]) {
    for value in values {
        push_c32(output, value);
    }
}

fn describe() {
    println!(
        "{}",
        concat!(
            "{\"schema\":\"casa-rs-vlass-mtmfs-raw-frame-ordering-helper-v1\",",
            "\"input_record_bytes\":48,\"output_record_bytes\":152,",
            "\"float_contract\":",
            "\"separate-rust-f32-operators-with-black-box-rounding-boundaries\",",
            "\"graphs\":[\"current-aligned-frame\",\"casa-raw-frame\"],",
            "\"forms_residual\":false,\"opens_ms\":false,\"enters_imaging\":false}"
        )
    );
}

fn evaluate(input_path: &Path, output_path: &Path, reference_bits: u64) -> io::Result<()> {
    let input = fs::read(input_path)?;
    let mut chunks = input.chunks_exact(INPUT_RECORD_BYTES);
    if !chunks.remainder().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "input length is not an integral record count",
        ));
    }
    let record_count = input.len() / INPUT_RECORD_BYTES;
    let mut output = Vec::with_capacity(record_count * OUTPUT_RECORD_BYTES);
    let reference_frequency = f64::from_bits(reference_bits);

    for record in &mut chunks {
        let mut offset = 0;
        let frequency_bits = read_u64(record, &mut offset);
        let raw_tt0 = [read_c32(record, &mut offset), read_c32(record, &mut offset)];
        let raw_tt1 = [read_c32(record, &mut offset), read_c32(record, &mut offset)];
        let phase = read_c32(record, &mut offset);
        debug_assert_eq!(offset, INPUT_RECORD_BYTES);

        let frequency = f64::from_bits(frequency_bits);
        let freq_f32 = black_box(frequency as f32);
        let delta_f64 = black_box(f64::from(freq_f32) - reference_frequency);
        let mulfactor_f64 = black_box(delta_f64 / reference_frequency);
        let power_f32 = black_box(mulfactor_f64 as f32);

        let aligned_tt0 = raw_tt0.map(|value| apply_source_phase(value, phase));
        let aligned_tt1 = raw_tt1.map(|value| apply_source_phase(value, phase));
        let scaled_current = aligned_tt1.map(|value| scale_c32(value, power_f32));
        let combined_current = [
            add_c32(aligned_tt0[0], scaled_current[0]),
            add_c32(aligned_tt0[1], scaled_current[1]),
        ];

        let scaled_raw = raw_tt1.map(|value| scale_c32(value, power_f32));
        let combined_raw = [
            add_c32(raw_tt0[0], scaled_raw[0]),
            add_c32(raw_tt0[1], scaled_raw[1]),
        ];
        let aligned_scaled_raw = scaled_raw.map(|value| apply_source_phase(value, phase));
        let aligned_combined_raw = combined_raw.map(|value| apply_source_phase(value, phase));

        push_u64(&mut output, frequency_bits);
        push_u32(&mut output, freq_f32.to_bits());
        push_u64(&mut output, mulfactor_f64.to_bits());
        push_u32(&mut output, power_f32.to_bits());
        push_pair(&mut output, aligned_tt0);
        push_pair(&mut output, aligned_tt1);
        push_pair(&mut output, scaled_current);
        push_pair(&mut output, combined_current);
        push_pair(&mut output, scaled_raw);
        push_pair(&mut output, combined_raw);
        push_pair(&mut output, aligned_scaled_raw);
        push_pair(&mut output, aligned_combined_raw);
    }

    debug_assert_eq!(output.len(), record_count * OUTPUT_RECORD_BYTES);
    fs::write(output_path, output)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = env::args().collect();
    if arguments.len() == 2 && arguments[1] == "--describe" {
        describe();
        return Ok(());
    }
    if arguments.len() != 4 {
        return Err("usage: vlass_mtmfs_raw_frame_ordering INPUT OUTPUT REFFREQ_BITS_HEX".into());
    }
    let reference_bits = u64::from_str_radix(&arguments[3], 16)?;
    evaluate(
        Path::new(&arguments[1]),
        Path::new(&arguments[2]),
        reference_bits,
    )?;
    Ok(())
}
