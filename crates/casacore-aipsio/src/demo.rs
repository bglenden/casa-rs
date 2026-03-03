// SPDX-License-Identifier: LGPL-3.0-or-later
//! Demo helpers and runnable outputs for `casacore-aipsio`.
//!
//! This module intentionally keeps demonstration code outside the core
//! `AipsIo` API implementation.

use std::fs::remove_file;
use std::io::Cursor;
use std::path::PathBuf;

use thiserror::Error;

use crate::aipsio::{AipsIo, AipsIoObjectError, AipsOpenOption};
use crate::{Complex32, Complex64};

#[derive(Debug, Error)]
pub enum AipsIoDemoError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Stream(#[from] AipsIoObjectError),
}

pub type AipsIoDemoResult<T> = Result<T, AipsIoDemoError>;

pub fn run_taipsio_like_demo() -> AipsIoDemoResult<String> {
    let mut out = String::new();
    let normal_path = temp_path("tAipsIO_tmp.data");
    let multifile_path = temp_path("tAipsIO_tmp.mf.data");

    append_line(&mut out, "Test using normal files ...");
    {
        let mut io = AipsIo::open(&normal_path, AipsOpenOption::New)?;
        do_io(false, true, &mut io, &mut out)?;
    }
    {
        let mut io = AipsIo::open(&normal_path, AipsOpenOption::Old)?;
        do_io(false, false, &mut io, &mut out)?;
    }
    let _ = remove_file(&normal_path);

    append_line(&mut out, "");
    append_line(&mut out, "Test using MultiFile files ...");
    {
        let mut io = AipsIo::open(&multifile_path, AipsOpenOption::New)?;
        do_io(false, true, &mut io, &mut out)?;
    }
    {
        let mut io = AipsIo::open(&multifile_path, AipsOpenOption::Old)?;
        do_io(false, false, &mut io, &mut out)?;
    }
    let _ = remove_file(&multifile_path);

    append_line(&mut out, "");
    append_line(&mut out, "Test using MemoryIO ...");
    let mut write_mem = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
    do_io(false, true, &mut write_mem, &mut out)?;
    let bytes = write_mem
        .into_inner_typed::<Cursor<Vec<u8>>>()?
        .into_inner();
    let mut read_mem = AipsIo::new_read_only(Cursor::new(bytes));
    do_io(false, false, &mut read_mem, &mut out)?;

    append_line(&mut out, "end");
    Ok(out)
}

fn temp_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    path.push(format!("{name}.{nanos}.{}", std::process::id()));
    path
}

fn append_line(out: &mut String, line: &str) {
    out.push_str(line);
    out.push('\n');
}

fn do_io(do_excp: bool, writing: bool, io: &mut AipsIo, out: &mut String) -> AipsIoDemoResult<()> {
    let _ = do_excp;

    let tbi = true;
    let tci = -1_i8;
    let tuci = 2_u8;
    let tsi = -3_i16;
    let tusi = 4_u16;
    let tii = -5_i32;
    let tuii = 6_u32;
    let tli = -7_i64;
    let tuli = 8_u64;
    let tfi = 3.15_f32;
    let tdi = 6.897_f64;
    let toi = Complex32::new(1.98, -1000.45);
    let tdoi = Complex64::new(93.7, -11.5);

    let mut i = -32768_i32;
    let mut arr = vec![0_i32; 250_001];
    for (idx, value) in arr.iter_mut().enumerate() {
        *value = idx as i32;
    }

    let mut barr = vec![false; 100];
    for (idx, value) in barr.iter_mut().enumerate() {
        if idx % 5 == 1 {
            *value = true;
        }
    }

    let ca = [
        "string000".to_string(),
        "str1".to_string(),
        "strin2".to_string(),
        "stri3".to_string(),
        "string45".to_string(),
        "s".to_string(),
    ];
    let mut sa = [
        "str1".to_string(),
        "strin2".to_string(),
        "stri3".to_string(),
        "string45".to_string(),
        "s".to_string(),
        "s".to_string(),
    ];
    sa[5].push_str("abc");
    append_line(out, &sa[5]);

    if writing {
        append_line(out, &io.putstart("abcdefghij", 20)?.to_string());
        io.put_bool(tbi)?;
        io.put_i8(tci)?;
        io.put_u8(tuci)?;
        io.put_i16(tsi)?;
        io.put_u16(tusi)?;
        io.put_i32(tii)?;
        io.put_u32(tuii)?;
        io.put_i64(tli)?;
        io.put_u64(tuli)?;
        io.put_f32(tfi)?;
        io.put_f64(tdi)?;
        io.put_complex32(toi)?;
        io.put_complex64(tdoi)?;
        io.put_i32(3)?;
        io.put_i32(i)?;
        io.put_i32_slice(&[i], true)?;
        append_line(out, &io.putend()?.to_string());

        append_line(out, &io.putstart("abcdefghij", 20)?.to_string());
        io.put_string("Ger")?;
        io.put_string(" van Diepen")?;
        io.put_i32_slice(&[i], true)?;
        append_line(out, &io.putend()?.to_string());

        append_line(out, &io.putstart("abcdefghij", 20)?.to_string());
        io.put_i32(1)?;
        append_line(out, &io.putstart("klm", 21)?.to_string());
        io.put_i32(2)?;
        append_line(out, &io.putstart("nopq", 22)?.to_string());
        io.put_i32(3)?;
        append_line(out, &io.putend()?.to_string());
        io.put_i32(4)?;
        append_line(out, &io.putstart("r", 23)?.to_string());
        io.put_i32(5)?;
        append_line(out, &io.putend()?.to_string());
        io.put_i32(6)?;
        append_line(out, &io.putend()?.to_string());
        io.put_i32(7)?;
        append_line(out, &io.putend()?.to_string());

        append_line(out, &io.putstart("abcdefghij", 20)?.to_string());
        io.put_i32(3)?;
        io.put_i32(i)?;
        io.put_i32_slice(&[i], true)?;
        for value in arr.iter().take(250_000) {
            io.put_i32(*value)?;
        }
        io.put_i32_slice(&arr[..250_000], true)?;
        io.put_bool_slice(&barr, true)?;
        io.put_i32_slice(&arr[..250_000], true)?;
        io.put_bool_slice(&barr, true)?;
        io.put_string_slice(&ca[..5], true)?;
        io.put_string_slice(&ca[..5], true)?;
        io.put_string_slice(&sa[..5], true)?;
        io.put_string_slice(&sa[..5], true)?;
        append_line(out, &io.putend()?.to_string());
        append_line(out, &format!("Length={}", io.getpos()?));
        return Ok(());
    }

    append_line(
        out,
        &format!(
            "{} {} Version={}",
            io.get_next_type()?,
            io.get_next_type()?,
            io.getstart("abcdefghij")?
        ),
    );

    let tbo = io.get_bool()?;
    let tco = io.get_i8()?;
    let tuco = io.get_u8()?;
    let tso = io.get_i16()?;
    let tuso = io.get_u16()?;
    let tio = io.get_i32()?;
    let tuio = io.get_u32()?;
    let tlo = io.get_i64()?;
    let tulo = io.get_u64()?;
    let tfo = io.get_f32()?;
    let tdo = io.get_f64()?;
    let too = io.get_complex32()?;
    let tdoo = io.get_complex64()?;

    if tbo != tbi {
        append_line(out, &format!("Bool {tbi} {tbo}"));
    }
    if tco != tci {
        append_line(out, &format!("Char {tci} {tco}"));
    }
    if tuco != tuci {
        append_line(out, &format!("uChar {tuci} {tuco}"));
    }
    if tso != tsi {
        append_line(out, &format!("short {tsi} {tso}"));
    }
    if tuso != tusi {
        append_line(out, &format!("ushort {tusi} {tuso}"));
    }
    if tio != tii {
        append_line(out, &format!("int {tii} {tio}"));
    }
    if tuio != tuii {
        append_line(out, &format!("uint {tuii} {tuio}"));
    }
    if tlo != tli {
        append_line(out, &format!("long {tli} {tlo}"));
    }
    if tulo != tuli {
        append_line(out, &format!("ulong {tuli} {tulo}"));
    }
    if tfo != tfi {
        append_line(out, &format!("float {tfi} {tfo}"));
    }
    if tdo != tdi {
        append_line(out, &format!("double {tdi} {tdo}"));
    }
    if too != toi {
        append_line(out, &format!("Complex {:?} {:?}", toi, too));
    }
    if tdoo != tdoi {
        append_line(out, &format!("DComplex {:?} {:?}", tdoi, tdoo));
    }

    let j = io.get_i32()?;
    i = io.get_i32()?;
    append_line(out, &format!("{j} {i}"));
    let len = io.get_u32()?;
    let mut one = [0_i32; 1];
    io.get_i32_into(&mut one)?;
    append_line(out, &format!("{len} {}", one[0]));
    append_line(out, &io.getend()?.to_string());

    append_line(out, &format!("Version={}", io.getstart("abcdefghij")?));
    let a = io.get_string()?;
    let cp = io.get_string()?;
    append_line(out, &(a + &cp));
    let ip = io.getnew_i32()?;
    append_line(out, &format!("{} {}", ip.len(), ip[0]));
    append_line(out, &io.getend()?.to_string());

    append_line(out, &format!("Version={}", io.getstart("abcdefghij")?));
    let val = io.get_i32()?;
    append_line(out, &val.to_string());
    append_line(
        out,
        &format!(
            "{} {} Version={}",
            io.get_next_type()?,
            io.get_next_type()?,
            io.getstart("klm")?
        ),
    );
    append_line(out, &io.get_i32()?.to_string());
    append_line(out, &format!("Version={}", io.getstart("nopq")?));
    append_line(out, &io.get_i32()?.to_string());
    append_line(out, &io.getend()?.to_string());
    append_line(out, &io.get_i32()?.to_string());
    append_line(
        out,
        &format!("{} Version={}", io.get_next_type()?, io.getstart("r")?),
    );
    append_line(out, &io.get_i32()?.to_string());
    append_line(out, &io.getend()?.to_string());
    append_line(out, &io.get_i32()?.to_string());
    append_line(out, &io.getend()?.to_string());
    append_line(out, &io.get_i32()?.to_string());
    append_line(out, &io.getend()?.to_string());

    append_line(out, &format!("Version={}", io.getstart("abcdefghij")?));
    let v1 = io.get_i32()?;
    let v2 = io.get_i32()?;
    append_line(out, &format!("{v1} {v2}"));
    let len = io.get_u32()?;
    let mut one = [0_i32; 1];
    io.get_i32_into(&mut one)?;
    append_line(out, &format!("{len} {}", one[0]));

    for (idx, expected) in arr.iter().take(250_000).enumerate() {
        let lo = io.get_i32()?;
        if lo != *expected {
            append_line(out, &format!("{lo} {idx}"));
        }
    }

    let len = io.get_u32()? as usize;
    append_line(out, &len.to_string());
    let mut arr_read = vec![0_i32; len];
    io.get_i32_into(&mut arr_read)?;
    for (idx, value) in arr_read.iter().enumerate() {
        if *value != idx as i32 {
            append_line(out, &format!("{idx} not equal"));
        }
    }

    let len = io.get_u32()? as usize;
    append_line(out, &len.to_string());
    let mut barri = vec![false; len];
    io.get_bool_into(&mut barri)?;
    for (idx, value) in barri.iter().enumerate() {
        if *value != barr[idx] {
            append_line(out, &format!("{idx} barri not equal"));
        }
    }

    let lp = io.getnew_i32()?;
    append_line(out, &lp.len().to_string());
    for (idx, value) in lp.iter().enumerate() {
        if *value != idx as i32 {
            append_line(out, &format!("{idx} not equal"));
        }
    }

    let barrp = io.getnew_bool()?;
    append_line(out, &barrp.len().to_string());
    for (idx, value) in barrp.iter().enumerate() {
        if *value != barr[idx] {
            append_line(out, &format!("{idx} barrp not equal"));
        }
    }

    let len = io.get_u32()? as usize;
    append_line(out, &len.to_string());
    let mut cap = vec![String::new(); len + 1];
    io.get_string_into(&mut cap[1..=len])?;

    let len2 = io.get_u32()? as usize;
    append_line(out, &len2.to_string());
    let mut sap = vec![String::new(); len2 + 1];
    io.get_string_into(&mut sap[1..=len2])?;

    let sptr = io.getnew_string()?;
    append_line(out, &sptr.len().to_string());
    let cptr = io.getnew_string()?;
    append_line(out, &cptr.len().to_string());

    for idx in 0..cptr.len() {
        append_line(
            out,
            &format!(
                "{} {} {} {}",
                cap[idx + 1],
                sap[idx + 1],
                sptr[idx],
                cptr[idx]
            ),
        );
    }

    append_line(out, &io.getend()?.to_string());
    append_line(out, &format!("Length={}", io.getpos()?));
    io.setpos(1)?;
    io.setpos(0)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::run_taipsio_like_demo;

    #[test]
    fn demo_contains_expected_section_headers() {
        let output = run_taipsio_like_demo().expect("demo should run");
        assert!(output.contains("Test using normal files ..."));
        assert!(output.contains("Test using MultiFile files ..."));
        assert!(output.contains("Test using MemoryIO ..."));
        assert!(output.ends_with("end\n"));
    }
}
