// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Metadata for a TSM data file (maps to C++ `TSMFile`).
#[derive(Debug, Clone)]
pub(super) struct TsmFileInfo {
    pub(super) seq_nr: u32,
    pub(super) length: i64,
}

/// Metadata for a single hypercube (maps to C++ `TSMCube`).
#[derive(Debug, Clone)]
pub(super) struct TsmCubeInfo {
    pub(super) values: RecordValue,
    pub(super) extensible: bool,
    pub(super) cube_shape: Vec<usize>,
    pub(super) tile_shape: Vec<usize>,
    pub(super) file_seq_nr: i32,
    pub(super) file_offset: i64,
}

/// Common base-class header (maps to C++ `TiledStMan`).
#[derive(Debug, Clone)]
pub(super) struct TiledStManHeader {
    pub(super) big_endian: bool,
    pub(super) seq_nr: u32,
    pub(super) nrrow: u64,
    pub(super) col_data_types: Vec<CasacoreDataType>,
    pub(super) hypercolumn_name: String,
    pub(super) max_cache_size: u64,
    pub(super) nrdim: u32,
    pub(super) files: Vec<Option<TsmFileInfo>>,
    pub(super) cubes: Vec<TsmCubeInfo>,
}

/// Variant-specific data for each tiled storage manager type.
#[derive(Debug, Clone)]
pub(super) enum TiledVariant {
    Column {
        default_tile_shape: Vec<i32>,
    },
    Shape {
        default_tile_shape: Vec<i32>,
        nr_used_row_map: u32,
        row_map: Vec<u32>,
        cube_map: Vec<u32>,
        pos_map: Vec<u32>,
    },
    Cell {
        default_tile_shape: Vec<i32>,
    },
    /// TiledDataStMan: user-controlled hypercube assignment.
    ///
    /// Unlike TiledShapeStMan (automatic shape-based grouping), the user
    /// explicitly assigns rows to hypercubes. Found in some older datasets.
    ///
    /// # C++ equivalent
    ///
    /// `TiledDataStMan` in `casacore/tables/DataMan/TiledDataStMan.h`.
    /// Unlike the other variants it has no default tile shape: each
    /// hypercube is added with its own.
    Data {
        nrrow_last: u64,
        row_map: Vec<u64>,
        cube_map: Vec<u32>,
        pos_map: Vec<u32>,
    },
}

impl TiledVariant {
    /// The manager's default tile shape; empty for `TiledDataStMan`, which
    /// has none.
    pub(super) fn default_tile_shape(&self) -> &[i32] {
        match self {
            Self::Column { default_tile_shape }
            | Self::Shape {
                default_tile_shape, ..
            }
            | Self::Cell { default_tile_shape } => default_tile_shape,
            Self::Data { .. } => &[],
        }
    }

    /// The casacore class name of the manager.
    pub(super) fn manager_name(&self) -> &'static str {
        match self {
            Self::Column { .. } => "TiledColumnStMan",
            Self::Shape { .. } => "TiledShapeStMan",
            Self::Cell { .. } => "TiledCellStMan",
            Self::Data { .. } => "TiledDataStMan",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct TiledReadHeader {
    pub(super) variant: TiledVariant,
    pub(super) header: TiledStManHeader,
    pub(super) packed_read_seen: std::sync::OnceLock<()>,
}

/// Immutable tiled-manager metadata retained for the lifetime of one open table.
///
/// The metadata is local to the table capability and contains exactly one
/// parsed header per tiled data manager. It deliberately does not retain tile
/// payloads or participate in the process-wide tile cache, and it cannot grow
/// after table-open admission.
#[derive(Clone, Debug, Default)]
pub(crate) struct TiledReadMetadata {
    pub(super) headers: HashMap<u32, TiledReadHeader>,
}

impl TiledReadMetadata {
    pub(crate) fn open(
        table_path: &Path,
        data_managers: &[DataManagerEntry],
    ) -> Result<Self, StorageError> {
        let tiled_manager_count = data_managers
            .iter()
            .filter(|manager| is_tiled_manager(&manager.type_name))
            .count();
        let mut headers = HashMap::with_capacity(tiled_manager_count);
        for manager in data_managers
            .iter()
            .filter(|manager| is_tiled_manager(&manager.type_name))
        {
            let header_path = table_path.join(format!("table.f{}", manager.seq_nr));
            let (variant, header) = read_tiled_header(&header_path)?;
            headers.insert(
                manager.seq_nr,
                TiledReadHeader {
                    variant,
                    header,
                    packed_read_seen: std::sync::OnceLock::new(),
                },
            );
        }
        Ok(Self { headers })
    }

    pub(super) fn header(&self, dm_seq_nr: u32) -> Result<&TiledReadHeader, StorageError> {
        self.headers.get(&dm_seq_nr).ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "retained tiled read metadata is missing data manager {dm_seq_nr}"
            ))
        })
    }

    pub(crate) fn retained_heap_bytes(&self) -> Option<usize> {
        let mut bytes = self
            .headers
            .capacity()
            .checked_mul(std::mem::size_of::<(u32, TiledReadHeader)>())?;
        for read_header in self.headers.values() {
            bytes = bytes.checked_add(read_header.retained_heap_bytes()?)?;
        }
        Some(bytes)
    }

    #[cfg(test)]
    pub(crate) fn cached_header_count(&self) -> usize {
        self.headers.len()
    }
}

impl TiledReadHeader {
    fn retained_heap_bytes(&self) -> Option<usize> {
        let variant_bytes = match &self.variant {
            TiledVariant::Column { default_tile_shape }
            | TiledVariant::Cell { default_tile_shape } => default_tile_shape
                .capacity()
                .checked_mul(std::mem::size_of::<i32>())?,
            TiledVariant::Shape {
                default_tile_shape,
                row_map,
                cube_map,
                pos_map,
                ..
            } => default_tile_shape
                .capacity()
                .checked_mul(std::mem::size_of::<i32>())?
                .checked_add(row_map.capacity().checked_mul(std::mem::size_of::<u32>())?)?
                .checked_add(
                    cube_map
                        .capacity()
                        .checked_mul(std::mem::size_of::<u32>())?,
                )?
                .checked_add(pos_map.capacity().checked_mul(std::mem::size_of::<u32>())?)?,
            TiledVariant::Data {
                row_map,
                cube_map,
                pos_map,
                ..
            } => row_map
                .capacity()
                .checked_mul(std::mem::size_of::<u64>())?
                .checked_add(
                    cube_map
                        .capacity()
                        .checked_mul(std::mem::size_of::<u32>())?,
                )?
                .checked_add(pos_map.capacity().checked_mul(std::mem::size_of::<u32>())?)?,
        };
        let mut bytes = variant_bytes
            .checked_add(
                self.header
                    .col_data_types
                    .capacity()
                    .checked_mul(std::mem::size_of::<CasacoreDataType>())?,
            )?
            .checked_add(self.header.hypercolumn_name.capacity())?
            .checked_add(
                self.header
                    .files
                    .capacity()
                    .checked_mul(std::mem::size_of::<Option<TsmFileInfo>>())?,
            )?
            .checked_add(
                self.header
                    .cubes
                    .capacity()
                    .checked_mul(std::mem::size_of::<TsmCubeInfo>())?,
            )?;
        for cube in &self.header.cubes {
            bytes = bytes
                .checked_add(cube.values.retained_heap_bytes()?)?
                .checked_add(
                    cube.cube_shape
                        .capacity()
                        .checked_mul(std::mem::size_of::<usize>())?,
                )?
                .checked_add(
                    cube.tile_shape
                        .capacity()
                        .checked_mul(std::mem::size_of::<usize>())?,
                )?;
        }
        Some(bytes)
    }
}

pub(super) fn is_tiled_manager(type_name: &str) -> bool {
    matches!(
        type_name,
        "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan"
    )
}

/// Parse the AipsIO header file for a tiled storage manager.
///
/// Returns the variant-specific data and the common base header.
pub(super) fn read_tiled_header(
    path: &Path,
) -> Result<(TiledVariant, TiledStManHeader), StorageError> {
    let mut io = DataFile::aips(path)?;

    // The outermost AipsIO object is the derived-class envelope.
    let outer_type = io.get_next_type()?;
    let outer_version = io.getstart(&outer_type)?;

    match outer_type.as_str() {
        "TiledColumnStMan" => {
            // TiledColumnStMan: tileShape first, then base class.
            let default_tile_shape = read_iposition(&mut io)?;
            let header = read_tiled_stman_base(&mut io)?;
            io.getend()?;
            io.close()?;
            Ok((TiledVariant::Column { default_tile_shape }, header))
        }
        "TiledShapeStMan" => {
            // TiledShapeStMan: base class first, then variant-specific fields.
            let header = read_tiled_stman_base(&mut io)?;
            let default_tile_shape = read_iposition(&mut io)?;
            let nr_used_row_map = io.get_u32()?;
            let row_map = read_block_u32(&mut io)?;
            let cube_map = read_block_u32(&mut io)?;
            let pos_map = read_block_u32(&mut io)?;
            io.getend()?;
            io.close()?;
            Ok((
                TiledVariant::Shape {
                    default_tile_shape,
                    nr_used_row_map,
                    row_map,
                    cube_map,
                    pos_map,
                },
                header,
            ))
        }
        "TiledCellStMan" => {
            // TiledCellStMan: defaultTileShape first, then base class.
            let default_tile_shape = read_iposition(&mut io)?;
            let header = read_tiled_stman_base(&mut io)?;
            io.getend()?;
            io.close()?;
            Ok((TiledVariant::Cell { default_tile_shape }, header))
        }
        "TiledDataStMan" => {
            // TiledDataStMan::readHeader: the base class, then nrrowLast,
            // rowMap, cubeMap and posMap; there is no default tile shape.
            // Version 1 stored a used count before a uInt row map, and a
            // uInt nrrowLast while its base header predated rownr_t.
            let (header, base_version) = read_tiled_stman_base_versioned(&mut io)?;
            let nrrow_last = if outer_version == 1 && base_version < 3 {
                u64::from(io.get_u32()?)
            } else {
                io.get_u64()?
            };
            let row_map = if outer_version == 1 {
                let _nused = io.get_u32()?;
                read_block_u32(&mut io)?
                    .into_iter()
                    .map(u64::from)
                    .collect()
            } else {
                read_block_u64(&mut io)?
            };
            let cube_map = read_block_u32(&mut io)?;
            let pos_map = read_block_u32(&mut io)?;
            io.getend()?;
            io.close()?;
            Ok((
                TiledVariant::Data {
                    nrrow_last,
                    row_map,
                    cube_map,
                    pos_map,
                },
                header,
            ))
        }
        other => Err(StorageError::FormatMismatch(format!(
            "unsupported tiled storage manager type: {other}"
        ))),
    }
}

pub(crate) fn tiled_2d_channel_tile_width(
    table_path: &Path,
    dm_seq_nr: u32,
) -> Result<Option<usize>, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    for cube in &header.cubes {
        if cube.tile_shape.len() >= 3 && cube.cube_shape.len() >= 3 {
            return Ok(Some(cube.tile_shape[1].max(1)));
        }
    }
    let default_tile_shape = variant.default_tile_shape();
    Ok((default_tile_shape.len() >= 3).then(|| default_tile_shape[1].max(1) as usize))
}

/// The tile shape of the tiled manager `dm_seq_nr`: its first hypercube's,
/// else its default tile shape, in casacore's axis order (the cell's axes,
/// then rows).
pub(crate) fn tiled_tile_shape(
    table_path: &Path,
    dm_seq_nr: u32,
) -> Result<Option<Vec<usize>>, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    if let Some(cube) = header.cubes.iter().find(|cube| !cube.tile_shape.is_empty()) {
        return Ok(Some(cube.tile_shape.clone()));
    }
    let default_tile_shape: Vec<usize> = variant
        .default_tile_shape()
        .iter()
        .map(|&extent| usize::try_from(extent).unwrap_or(0))
        .collect();
    Ok((!default_tile_shape.is_empty()).then_some(default_tile_shape))
}

/// Bytes of the largest bucket (tile) of the tiled manager `dm_seq_nr`,
/// over its hypercubes and its default tile shape: what reading or writing
/// one of its cells holds whole.
pub(crate) fn tiled_bucket_bytes(table_path: &Path, dm_seq_nr: u32) -> Result<usize, StorageError> {
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    let (variant, header) = read_tiled_header(&header_path)?;
    let default_tile_shape: Vec<usize> = variant
        .default_tile_shape()
        .iter()
        .map(|&extent| usize::try_from(extent).unwrap_or(0))
        .collect();
    Ok(header
        .cubes
        .iter()
        .map(|cube| cube.tile_shape.as_slice())
        .chain(std::iter::once(default_tile_shape.as_slice()))
        .map(|tile_shape| compute_tile_layout(&header.col_data_types, tile_shape).0)
        .max()
        .unwrap_or(0))
}

/// Parse the `TiledStMan` base class AipsIO object.
///
/// Handles versions 1 (big-endian, 32-bit), 2 (explicit endian, 32-bit),
/// and 3 (explicit endian, 64-bit).
pub(super) fn read_tiled_stman_base(io: &mut AipsIo) -> Result<TiledStManHeader, StorageError> {
    Ok(read_tiled_stman_base_versioned(io)?.0)
}

/// The tiled manager's name (`DataManager::dataManagerName()`, the
/// hypercolumn name its header holds), read from the head of the header file
/// without the hypercubes. It is the NAME the manager was bound with, which
/// need not be the group its columns' descriptions declare. `None` when the
/// file is missing or does not parse.
pub(crate) fn name_from_file(path: &Path) -> Option<String> {
    let mut io = DataFile::aips(path).ok()?;
    let outer_type = io.get_next_type().ok()?;
    io.getstart(&outer_type).ok()?;
    match outer_type.as_str() {
        // The default tile shape comes before the base class in these.
        "TiledColumnStMan" | "TiledCellStMan" => {
            read_iposition(&mut io).ok()?;
        }
        "TiledShapeStMan" | "TiledDataStMan" => {}
        _ => return None,
    }
    let version = io.getstart("TiledStMan").ok()?;
    if version >= 2 {
        io.get_bool().ok()?;
    }
    io.get_u32().ok()?;
    if version >= 3 {
        io.get_u64().ok()?;
    } else {
        io.get_u32().ok()?;
    }
    let ncol = io.get_u32().ok()?;
    for _ in 0..ncol {
        io.get_i32().ok()?;
    }
    io.get_string().ok()
}

/// [`read_tiled_stman_base`], also returning the `TiledStMan` object
/// version, which `TiledDataStMan` version 1 needs to size `nrrowLast`.
pub(super) fn read_tiled_stman_base_versioned(
    io: &mut AipsIo,
) -> Result<(TiledStManHeader, u32), StorageError> {
    let version = io.getstart("TiledStMan")?;

    let big_endian = if version >= 2 {
        io.get_bool()?
    } else {
        true // v1 is always big-endian
    };

    let seq_nr = io.get_u32()?;

    let nrrow = if version >= 3 {
        io.get_u64()?
    } else {
        io.get_u32()? as u64
    };

    let ncol = io.get_u32()?;
    let mut col_data_types = Vec::with_capacity(ncol as usize);
    for _ in 0..ncol {
        let dt_i32 = io.get_i32()?;
        let dt = CasacoreDataType::from_i32(dt_i32).ok_or_else(|| {
            StorageError::FormatMismatch(format!("unknown TiledStMan column data type: {dt_i32}"))
        })?;
        col_data_types.push(dt);
    }

    let hypercolumn_name = io.get_string()?;

    let max_cache_size = if version >= 3 {
        io.get_u64()?
    } else {
        io.get_u32()? as u64
    };

    let nrdim = io.get_u32()?;

    // Read TSM file entries.
    let nr_files = if version >= 3 {
        io.get_u64()? as usize
    } else {
        io.get_u32()? as usize
    };

    let mut files = Vec::with_capacity(nr_files);
    for _ in 0..nr_files {
        let exists = io.get_bool()?;
        if exists {
            files.push(Some(read_tsm_file_info(io)?));
        } else {
            files.push(None);
        }
    }

    // Read TSM cube entries.
    let nr_cubes = if version >= 3 {
        io.get_u64()? as usize
    } else {
        io.get_u32()? as usize
    };

    let mut cubes = Vec::with_capacity(nr_cubes);
    for _ in 0..nr_cubes {
        cubes.push(read_tsm_cube_info(io)?);
    }

    io.getend()?;

    Ok((
        TiledStManHeader {
            big_endian,
            seq_nr,
            nrrow,
            col_data_types,
            hypercolumn_name,
            max_cache_size,
            nrdim,
            files,
            cubes,
        },
        version,
    ))
}

/// Read inline TSMFile serialization (no AipsIO start/end wrapper).
pub(super) fn read_tsm_file_info(io: &mut AipsIo) -> Result<TsmFileInfo, StorageError> {
    let version = io.get_u32()?;
    let seq_nr = io.get_u32()?;
    let length = if version >= 2 {
        io.get_i64()?
    } else {
        io.get_u32()? as i64
    };
    Ok(TsmFileInfo { seq_nr, length })
}

/// Read inline TSMCube serialization (no AipsIO start/end wrapper).
pub(super) fn read_tsm_cube_info(io: &mut AipsIo) -> Result<TsmCubeInfo, StorageError> {
    let version = io.get_u32()?;
    let values = read_record(io)?;
    let extensible = io.get_bool()?;
    let _nrdim = io.get_u32()?;
    let cube_shape_i32 = read_iposition(io)?;
    let tile_shape_i32 = read_iposition(io)?;
    let file_seq_nr = io.get_i32()?;
    let file_offset = if version >= 2 {
        io.get_i64()?
    } else {
        io.get_u32()? as i64
    };

    Ok(TsmCubeInfo {
        values,
        extensible,
        cube_shape: cube_shape_i32.iter().map(|&v| v as usize).collect(),
        tile_shape: tile_shape_i32.iter().map(|&v| v as usize).collect(),
        file_seq_nr,
        file_offset,
    })
}

/// Read a `Block<uInt>` AipsIO object (used by TiledShapeStMan row maps).
pub(super) fn read_block_u32(io: &mut AipsIo) -> Result<Vec<u32>, StorageError> {
    let _version = io.getstart("Block")?;
    let count = io.get_u32()?;
    let mut values = vec![0u32; count as usize];
    for v in &mut values {
        *v = io.get_u32()?;
    }
    io.getend()?;
    Ok(values)
}

// ---------------------------------------------------------------------------
// Header write
// ---------------------------------------------------------------------------

/// Write the AipsIO header file for a tiled storage manager.
pub(super) fn write_tiled_header(
    path: &Path,
    variant: &TiledVariant,
    header: &TiledStManHeader,
) -> Result<(), StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::New)?;
    write_tiled_header_contents(&mut io, variant, header)?;
    io.close()?;
    Ok(())
}

pub(super) fn serialize_tiled_header(
    variant: &TiledVariant,
    header: &TiledStManHeader,
) -> Result<Box<[u8]>, StorageError> {
    let mut io = AipsIo::new_write_only(std::io::Cursor::new(Vec::new()));
    write_tiled_header_contents(&mut io, variant, header)?;
    let cursor: std::io::Cursor<Vec<u8>> = io.into_inner_typed()?;
    Ok(cursor.into_inner().into_boxed_slice())
}

pub(super) fn write_tiled_header_contents(
    io: &mut AipsIo,
    variant: &TiledVariant,
    header: &TiledStManHeader,
) -> Result<(), StorageError> {
    match variant {
        TiledVariant::Column { default_tile_shape } => {
            io.putstart("TiledColumnStMan", 1)?;
            write_iposition(io, default_tile_shape)?;
            write_tiled_stman_base(io, header)?;
            io.putend()?;
        }
        TiledVariant::Shape {
            default_tile_shape,
            nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
        } => {
            io.putstart("TiledShapeStMan", 1)?;
            write_tiled_stman_base(io, header)?;
            write_iposition(io, default_tile_shape)?;
            io.put_u32(*nr_used_row_map)?;
            write_block_u32(io, row_map)?;
            write_block_u32(io, cube_map)?;
            write_block_u32(io, pos_map)?;
            io.putend()?;
        }
        TiledVariant::Cell { default_tile_shape } => {
            io.putstart("TiledCellStMan", 1)?;
            write_iposition(io, default_tile_shape)?;
            write_tiled_stman_base(io, header)?;
            io.putend()?;
        }
        TiledVariant::Data {
            nrrow_last,
            row_map,
            cube_map,
            pos_map,
        } => {
            // TiledDataStMan::flush always writes version 2.
            io.putstart("TiledDataStMan", 2)?;
            write_tiled_stman_base(io, header)?;
            io.put_u64(*nrrow_last)?;
            write_block_u64(io, row_map)?;
            write_block_u32(io, cube_map)?;
            write_block_u32(io, pos_map)?;
            io.putend()?;
        }
    }

    Ok(())
}

/// Write the `TiledStMan` base class AipsIO object.
pub(super) fn write_tiled_stman_base(
    io: &mut AipsIo,
    header: &TiledStManHeader,
) -> Result<(), StorageError> {
    // Choose version: v1 for big-endian with small tables, v2 for little-endian.
    let version = if header.big_endian { 1u32 } else { 2 };

    io.putstart("TiledStMan", version)?;

    if version >= 2 {
        io.put_bool(header.big_endian)?;
    }

    io.put_u32(header.seq_nr)?;
    io.put_u32(header.nrrow as u32)?;

    io.put_u32(header.col_data_types.len() as u32)?;
    for &dt in &header.col_data_types {
        io.put_i32(dt as i32)?;
    }

    io.put_string(&header.hypercolumn_name)?;
    io.put_u32(header.max_cache_size as u32)?;
    io.put_u32(header.nrdim)?;

    // TSM files.
    io.put_u32(header.files.len() as u32)?;
    for file in &header.files {
        match file {
            Some(f) => {
                io.put_bool(true)?;
                write_tsm_file_info(io, f)?;
            }
            None => {
                io.put_bool(false)?;
            }
        }
    }

    // TSM cubes.
    io.put_u32(header.cubes.len() as u32)?;
    for cube in &header.cubes {
        write_tsm_cube_info(io, cube)?;
    }

    io.putend()?;
    Ok(())
}

/// Write inline TSMFile serialization.
pub(super) fn write_tsm_file_info(io: &mut AipsIo, file: &TsmFileInfo) -> Result<(), StorageError> {
    let version: u32 = if file.length > i32::MAX as i64 { 2 } else { 1 };
    io.put_u32(version)?;
    io.put_u32(file.seq_nr)?;
    if version >= 2 {
        io.put_i64(file.length)?;
    } else {
        io.put_u32(file.length as u32)?;
    }
    Ok(())
}

/// Write inline TSMCube serialization.
pub(super) fn write_tsm_cube_info(io: &mut AipsIo, cube: &TsmCubeInfo) -> Result<(), StorageError> {
    let version: u32 = if cube.file_offset > i32::MAX as i64 {
        2
    } else {
        1
    };
    io.put_u32(version)?;
    write_record(io, &cube.values)?;
    io.put_bool(cube.extensible)?;
    io.put_u32(cube.cube_shape.len() as u32)?;
    let shape_i32: Vec<i32> = cube.cube_shape.iter().map(|&v| v as i32).collect();
    write_iposition(io, &shape_i32)?;
    let tile_i32: Vec<i32> = cube.tile_shape.iter().map(|&v| v as i32).collect();
    write_iposition(io, &tile_i32)?;
    io.put_i32(cube.file_seq_nr)?;
    if version >= 2 {
        io.put_i64(cube.file_offset)?;
    } else {
        io.put_u32(cube.file_offset as u32)?;
    }
    Ok(())
}

/// Write a `Block<uInt>` AipsIO object.
pub(super) fn write_block_u32(io: &mut AipsIo, values: &[u32]) -> Result<(), StorageError> {
    io.putstart("Block", 1)?;
    io.put_u32(values.len() as u32)?;
    for &v in values {
        io.put_u32(v)?;
    }
    io.putend()?;
    Ok(())
}

/// Read a `Block<Int64>` AipsIO object (used by TiledDataStMan row maps).
pub(super) fn read_block_u64(io: &mut AipsIo) -> Result<Vec<u64>, StorageError> {
    let _version = io.getstart("Block")?;
    let count = io.get_u32()?;
    let mut values = vec![0u64; count as usize];
    for v in &mut values {
        *v = io.get_u64()?;
    }
    io.getend()?;
    Ok(values)
}

/// Write a `Block<Int64>` AipsIO object.
pub(super) fn write_block_u64(io: &mut AipsIo, values: &[u64]) -> Result<(), StorageError> {
    io.putstart("Block", 1)?;
    io.put_u32(values.len() as u32)?;
    for &v in values {
        io.put_u64(v)?;
    }
    io.putend()?;
    Ok(())
}
