// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pack the files emitted by the existing storage managers in a new table.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use casa_aipsio::AipsIo;

use super::{
    CONTINUATION_POINTER, LEAD_V2, StorageError, format_error, manager_sequence, supports_multifile,
};
use crate::storage::TABLE_CONTROL_FILE;
use crate::storage::table_control::{
    ColumnSetStorage, StagedTableDat, TableDatResult, read_table_dat_dispatch,
};

struct Member {
    path: Option<PathBuf>,
    name: String,
    size: u64,
    first_block: u64,
}

pub(crate) fn pack_staged_manager_files(
    root: &Path,
    block_size: usize,
) -> Result<(), StorageError> {
    if block_size < LEAD_V2 || block_size > i32::MAX as usize {
        return Err(format_error("invalid MultiFile output block size"));
    }
    let control = root.join(TABLE_CONTROL_FILE);
    let TableDatResult::Plain(mut descriptor) = read_table_dat_dispatch(&control)? else {
        return Err(format_error("MultiFile output requires a plain table"));
    };
    if descriptor
        .column_set
        .storage
        .is_some_and(|storage| storage.kind != ColumnSetStorage::SEPARATE)
    {
        return Err(format_error(
            "cannot pack a table that already uses a container",
        ));
    }
    let managers = descriptor
        .column_set
        .data_managers
        .iter()
        .filter(|manager| supports_multifile(&manager.type_name))
        .map(|manager| manager.seq_nr)
        .collect::<BTreeSet<_>>();
    let control = StagedTableDat::for_existing_table(root)?;
    if !managers.is_empty() {
        pack_members(
            root,
            block_size,
            1,
            member_paths(root, &managers)?
                .into_iter()
                .map(|(path, name)| (Some(path), name)),
        )?;
    }
    descriptor.column_set.storage = Some(ColumnSetStorage {
        kind: ColumnSetStorage::MULTI_FILE,
        block_size: block_size as i32,
    });
    control.publish(&descriptor)
}

pub(super) fn pack_members(
    root: &Path,
    block_size: usize,
    counter: u64,
    paths: impl IntoIterator<Item = (Option<PathBuf>, String)>,
) -> Result<(), StorageError> {
    let mut next_block = 1u64;
    let mut members = Vec::new();
    for (path, name) in paths {
        let size = path
            .as_deref()
            .map(fs::metadata)
            .transpose()?
            .map_or(0, |meta| meta.len());
        let first_block = next_block;
        next_block = next_block
            .checked_add(size.div_ceil(block_size as u64))
            .ok_or_else(|| format_error("MultiFile output exceeds the address space"))?;
        members.push(Member {
            path,
            name,
            size,
            first_block,
        });
    }
    let (header, block_count) = header(&members, block_size, counter, next_block)?;
    let extent = block_count
        .checked_mul(block_size as u64)
        .ok_or_else(|| format_error("MultiFile output extent overflow"))?;
    let path = root.join("table.mf");
    let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
    output.set_len(extent)?;
    for member in &members {
        let Some(path) = &member.path else {
            continue;
        };
        let mut source = File::open(path)?;
        output.seek(SeekFrom::Start(member.first_block * block_size as u64))?;
        let written = io::copy(&mut source, &mut output)?;
        if written != member.size {
            return Err(format_error(
                "staged manager file changed during MultiFile packing",
            ));
        }
    }
    write_header(&mut output, &header, block_size, next_block, block_count)?;
    output.sync_all()?;
    for path in members.into_iter().filter_map(|member| member.path) {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub(super) fn member_paths(
    root: &Path,
    managers: &BTreeSet<u32>,
) -> Result<Vec<(PathBuf, String)>, StorageError> {
    let mut files = Vec::new();
    for file in fs::read_dir(root)? {
        let file = file?;
        let name = file.file_name();
        let name = name.to_string_lossy();
        if manager_sequence(&name).is_some_and(|sequence| managers.contains(&sequence))
            && file.file_type()?.is_file()
        {
            files.push((file.path(), name.into_owned()));
        }
    }
    files.sort_unstable_by(|a, b| a.1.cmp(&b.1));
    Ok(files)
}

fn header(
    members: &[Member],
    block_size: usize,
    counter: u64,
    next_block: u64,
) -> Result<(Vec<u8>, u64), StorageError> {
    let count =
        u32::try_from(members.len()).map_err(|_| format_error("too many MultiFile members"))?;
    let mut stream = Cursor::new(vec![0; LEAD_V2]);
    stream.set_position(LEAD_V2 as u64);
    let mut io = AipsIo::new_write_only(stream);
    io.putstart("MultiFile", 2)?;
    io.putstart("Block", 1)?;
    io.put_u32(count)?;
    for member in members {
        io.put_string(&member.name)?;
        io.put_i64(
            i64::try_from(member.size)
                .map_err(|_| format_error("MultiFile member length overflows"))?,
        )?;
        io.put_bool(false)?;
    }
    io.putend()?;
    io.putend()?;
    let mut bytes = io.into_inner_typed::<Cursor<Vec<u8>>>()?.into_inner();
    for member in members {
        let blocks = member.size.div_ceil(block_size as u64);
        let first = i64::try_from(member.first_block)
            .map_err(|_| format_error("MultiFile block number overflows"))?;
        match blocks {
            0 => append_i64s(&mut bytes, [0]),
            1 => append_i64s(&mut bytes, [1, first]),
            _ => {
                let following = i64::try_from(blocks - 1)
                    .map_err(|_| format_error("MultiFile block run overflows"))?;
                append_i64s(&mut bytes, [2, first, -following]);
            }
        }
    }
    append_i64s(&mut bytes, [0, 0]); // empty free and checksum vectors
    // Two 64-bit vector lengths and two 32-bit counts of used blocks. Every
    // continuation also consumes one pointer in its physical block and one
    // block number in the header's continuation directory.
    let bytes_without_continuations = bytes.len() + 24;
    let continuations = bytes_without_continuations
        .saturating_sub(block_size)
        .div_ceil(block_size - 2 * CONTINUATION_POINTER) as u64;
    let block_count = next_block
        .checked_add(continuations)
        .filter(|&count| count <= i64::MAX as u64)
        .ok_or_else(|| format_error("MultiFile continuation directory overflows"))?;
    append_i64s(&mut bytes, [continuations as i64]);
    append_i64s(
        &mut bytes,
        (next_block..block_count).map(|block| block as i64),
    );
    append_i64s(&mut bytes, [0]);
    let used = u32::try_from(continuations)
        .map_err(|_| format_error("too many MultiFile continuation blocks"))?;
    bytes.extend(used.to_be_bytes());
    bytes.extend(0u32.to_be_bytes());
    if continuations > 0 {
        bytes[8..16].copy_from_slice(&next_block.to_be_bytes());
    }
    bytes[16..24].copy_from_slice(&counter.to_be_bytes());
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

fn write_header(
    output: &mut File,
    bytes: &[u8],
    block_size: usize,
    first: u64,
    end: u64,
) -> io::Result<()> {
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
