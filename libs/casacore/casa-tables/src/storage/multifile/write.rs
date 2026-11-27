// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pack the files emitted by the existing storage managers in a new table.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use casa_aipsio::AipsIo;

use super::{CONTINUATION_POINTER, LEAD_V2, StorageError, format_error};

struct Member {
    path: PathBuf,
    name: String,
    size: u64,
    first_block: u64,
}

pub(in crate::storage) fn pack_staged_manager_files(
    root: &Path,
    block_size: usize,
) -> Result<(), StorageError> {
    if block_size < LEAD_V2 || block_size > i32::MAX as usize {
        return Err(format_error("invalid MultiFile output block size"));
    }
    let paths = member_paths(root)?;
    let mut next_block = 1u64;
    let mut members = Vec::new();
    for (path, name) in paths {
        let size = fs::metadata(&path)?.len();
        let first_block = next_block;
        next_block = next_block.checked_add(size.div_ceil(block_size as u64))
            .ok_or_else(|| format_error("MultiFile output exceeds the address space"))?;
        members.push(Member { path, name, size, first_block });
    }
    let (header, block_count) = header(&members, block_size, next_block)?;
    let extent = block_count.checked_mul(block_size as u64)
        .ok_or_else(|| format_error("MultiFile output extent overflow"))?;
    let path = root.join("table.mf");
    let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
    output.set_len(extent)?;
    for member in &members {
        let mut source = File::open(&member.path)?;
        output.seek(SeekFrom::Start(member.first_block * block_size as u64))?;
        let written = io::copy(&mut source, &mut output)?;
        if written != member.size {
            return Err(format_error("staged manager file changed during MultiFile packing"));
        }
    }
    write_header(&mut output, &header, block_size, next_block, block_count)?;
    output.sync_all()?;
    for member in members {
        fs::remove_file(member.path)?;
    }
    Ok(())
}

fn member_paths(root: &Path) -> Result<Vec<(PathBuf, String)>, StorageError> {
    let mut files = Vec::new();
    for file in fs::read_dir(root)? {
        let file = file?;
        let name = file.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("table.f") && file.file_type()?.is_file() {
            files.push((file.path(), name.into_owned()));
        }
    }
    files.sort_unstable_by(|a, b| a.1.cmp(&b.1));
    Ok(files)
}

fn header(members: &[Member], block_size: usize, next_block: u64) -> Result<(Vec<u8>, u64), StorageError> {
    let count = u32::try_from(members.len()).map_err(|_| format_error("too many MultiFile members"))?;
    let mut stream = Cursor::new(vec![0; LEAD_V2]);
    stream.set_position(LEAD_V2 as u64);
    let mut io = AipsIo::new_write_only(stream);
    io.putstart("MultiFile", 2)?;
    io.putstart("Block", 1)?;
    io.put_u32(count)?;
    for member in members {
        io.put_string(&member.name)?;
        io.put_i64(i64::try_from(member.size).map_err(|_| format_error("MultiFile member length overflows"))?)?;
        io.put_bool(false)?;
    }
    io.putend()?;
    io.putend()?;
    let mut bytes = io.into_inner_typed::<Cursor<Vec<u8>>>()?.into_inner();
    for member in members {
        let blocks = member.size.div_ceil(block_size as u64);
        let first = i64::try_from(member.first_block).map_err(|_| format_error("MultiFile block number overflows"))?;
        match blocks {
            0 => append_i64s(&mut bytes, [0]),
            1 => append_i64s(&mut bytes, [1, first]),
            _ => {
                let following = i64::try_from(blocks - 1).map_err(|_| format_error("MultiFile block run overflows"))?;
                append_i64s(&mut bytes, [2, first, -following]);
            }
        }
    }
    append_i64s(&mut bytes, [0, 0]); // empty free and checksum vectors
    // Two continuation vector lengths and two counts of used blocks. Every
    // continuation also consumes one pointer in its physical block and one
    // block number in the header's continuation directory.
    let bytes_without_continuations = bytes.len() + 4 * CONTINUATION_POINTER;
    let continuations = bytes_without_continuations.saturating_sub(block_size)
        .div_ceil(block_size - 2 * CONTINUATION_POINTER) as u64;
    let block_count = next_block.checked_add(continuations)
        .filter(|&count| count <= i64::MAX as u64)
        .ok_or_else(|| format_error("MultiFile continuation directory overflows"))?;
    append_i64s(&mut bytes, [continuations as i64]);
    append_i64s(&mut bytes, (next_block..block_count).map(|block| block as i64));
    append_i64s(&mut bytes, [0, continuations as i64, 0]);
    if continuations > 0 {
        bytes[8..16].copy_from_slice(&next_block.to_be_bytes());
    }
    bytes[16..24].copy_from_slice(&1u64.to_be_bytes());
    bytes[24..28].copy_from_slice(&2u32.to_be_bytes());
    let length = bytes.len() as u64;
    bytes[32..40].copy_from_slice(&length.to_be_bytes());
    bytes[40..48].copy_from_slice(&(block_size as u64).to_be_bytes());
    bytes[48..56].copy_from_slice(&block_count.to_be_bytes());
    Ok((bytes, block_count))
}

fn append_i64s(output: &mut Vec<u8>, values: impl IntoIterator<Item = i64>) {
    for value in values {
        output.extend(value.to_be_bytes());
    }
}

fn write_header(output: &mut File, bytes: &[u8], block_size: usize, first: u64, end: u64) -> io::Result<()> {
    let lead = bytes.len().min(block_size);
    let payload = block_size - CONTINUATION_POINTER;
    for (block, piece) in (first..end).zip(bytes[lead..].chunks(payload)) {
        let next = if block + 1 == end { 0 } else { block + 1 };
        output.seek(SeekFrom::Start(block * block_size as u64))?;
        output.write_all(&next.to_be_bytes())?;
        output.write_all(piece)?;
    }
    output.seek(SeekFrom::Start(0))?;
    output.write_all(&bytes[..lead])
}
