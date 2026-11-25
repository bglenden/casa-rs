// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the CASA AsdmStMan index and visibility cells in their original ASDM BDFs.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use casa_aipsio::{AipsIo, AipsOpenOption, ByteOrder};
use casa_types::{ArrayValue, Complex32, PrimitiveType};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::StorageError;
use super::table_control::DataManagerEntry;

mod reference;
pub use reference::AsdmBdfIndex;

pub(crate) const TYPE: &str = "AsdmStMan";

#[derive(Clone, Debug, Default)]
pub(super) struct Managers(HashMap<u32, Arc<Manager>>);

#[derive(Debug)]
struct Manager {
    path: PathBuf,
    rows: u64,
    index: OnceLock<Result<Index, String>>,
}

impl Manager {
    fn index(&self) -> Result<&Index, StorageError> {
        self.index
            .get_or_init(|| Index::open(&self.path, self.rows).map_err(|error| error.to_string()))
            .as_ref()
            .map_err(|error| invalid(format!("read index {}: {error}", self.path.display())))
    }
}

impl Managers {
    pub(super) fn from_bindings(
        directory: &Path,
        managers: &[DataManagerEntry],
        rows: u64,
    ) -> Self {
        Self(
            managers
                .iter()
                .filter(|manager| manager.type_name == TYPE)
                .map(|manager| {
                    let path = directory.join(format!("table.f{}asdmindex", manager.seq_nr));
                    (
                        manager.seq_nr,
                        Arc::new(Manager {
                            path,
                            rows,
                            index: OnceLock::new(),
                        }),
                    )
                })
                .collect(),
        )
    }

    pub(super) fn retain_indexes(&self) -> Result<(), StorageError> {
        self.0
            .values()
            .try_for_each(|manager| manager.index().map(|_| ()))
    }

    pub(super) fn get(&self, sequence: u32) -> Result<&Index, StorageError> {
        self.0
            .get(&sequence)
            .ok_or_else(|| invalid(format!("missing index for manager {sequence}")))?
            .index()
    }

    pub(super) fn retained_heap_bytes(&self) -> Option<usize> {
        self.0.values().try_fold(
            self.0
                .capacity()
                .checked_mul(std::mem::size_of::<(u32, Arc<Manager>)>())?,
            |bytes, manager| {
                let index_bytes = match manager.index.get() {
                    Some(Ok(index)) => index.retained_heap_bytes()?,
                    Some(Err(error)) => error.capacity(),
                    None => 0,
                };
                bytes
                    .checked_add(std::mem::size_of::<Manager>())?
                    .checked_add(manager.path.capacity())?
                    .checked_add(index_bytes)
            },
        )
    }
}

#[derive(Debug)]
pub(super) struct Index {
    order: ByteOrder,
    bdfs: Vec<PathBuf>,
    descriptors: Vec<Descriptor>,
}

impl Index {
    fn open(path: &Path, rows: u64) -> Result<Self, StorageError> {
        #[cfg(test)]
        tests::INDEX_OPENS.with(|count| count.set(count.get() + 1));
        let size = std::fs::metadata(path)?.len();
        let mut io = AipsIo::open(path, AipsOpenOption::Old)?;
        let header = IndexHeader::read(&mut io, size)?;
        let order = if header.big_endian {
            ByteOrder::BigEndian
        } else {
            ByteOrder::LittleEndian
        };
        let bdfs = header.bdfs;
        let count = checked_count(&mut io, size, 56)?;
        let mut descriptors = Vec::with_capacity(count);
        let mut previous_end = 0;
        for _ in 0..count {
            let descriptor = Descriptor::read(&mut io, size, bdfs.len())?;
            if descriptor.start < previous_end || descriptor.end > rows {
                return Err(invalid(format!(
                    "invalid row interval {}..{} in a table with {rows} rows",
                    descriptor.start, descriptor.end,
                )));
            }
            previous_end = descriptor.end;
            descriptors.push(descriptor);
        }
        io.getend()?;
        io.close()?;
        Ok(Self {
            order,
            bdfs,
            descriptors,
        })
    }

    fn retained_heap_bytes(&self) -> Option<usize> {
        let fixed = self
            .descriptors
            .capacity()
            .checked_mul(std::mem::size_of::<Descriptor>())?
            .checked_add(
                self.bdfs
                    .capacity()
                    .checked_mul(std::mem::size_of::<PathBuf>())?,
            )?;
        self.bdfs
            .iter()
            .try_fold(fixed, |bytes, path| bytes.checked_add(path.capacity()))
    }

    fn descriptor(&self, row: usize) -> Result<&Descriptor, StorageError> {
        let row = row as u64;
        let upper = self.descriptors.partition_point(|entry| entry.start <= row);
        upper
            .checked_sub(1)
            .and_then(|position| self.descriptors.get(position))
            .filter(|entry| row < entry.end)
            .ok_or_else(|| invalid(format!("index does not cover row {row}")))
    }

    pub(super) fn shapes(
        &self,
        column: Column,
        rows: &[usize],
    ) -> Result<Vec<Option<Vec<usize>>>, StorageError> {
        rows.iter()
            .map(|&row| Ok(Some(self.descriptor(row)?.shape(column)?)))
            .collect()
    }

    pub(super) fn read(
        &self,
        column: Column,
        rows: impl IntoIterator<Item = usize>,
    ) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        let mut reader = self.reader();
        let rows = rows.into_iter().collect::<Vec<_>>();
        let mut cells = Vec::with_capacity(rows.len());
        let mut remaining = rows.as_slice();
        while let Some((&first, tail)) = remaining.split_first() {
            let descriptor = self.descriptor(first)?;
            let count = 1 + tail
                .iter()
                .take_while(|&&row| (descriptor.start..descriptor.end).contains(&(row as u64)))
                .count();
            let (block, rest) = remaining.split_at(count);
            reader.prepare_block(column, descriptor, count)?;
            for &row in block {
                cells.push(Some(reader.decode_cell(column, descriptor, row)?));
            }
            remaining = rest;
        }
        Ok(cells)
    }

    pub(super) fn reader(&self) -> Reader<'_> {
        Reader {
            index: self,
            open: None,
            cached_block: None,
            bytes: Vec::new(),
        }
    }
}

struct IndexHeader {
    version: u32,
    big_endian: bool,
    block_version: u32,
    bdfs: Vec<PathBuf>,
}

impl IndexHeader {
    fn read(io: &mut AipsIo, size: u64) -> Result<Self, StorageError> {
        let version = io.getstart(TYPE)?;
        if version > 1 {
            return Err(invalid(format!("unsupported ASDM index version {version}")));
        }
        let big_endian = io.get_bool()?;
        let block_version = io.getstart("Block")?;
        let count = checked_count(io, size, 4)?;
        let bdfs = (0..count)
            .map(|_| read_bdf_name(io, size))
            .collect::<Result<_, _>>()?;
        io.getend()?;
        Ok(Self {
            version,
            big_endian,
            block_version,
            bdfs,
        })
    }
}

fn checked_count(io: &mut AipsIo, size: u64, min_bytes: u64) -> Result<usize, StorageError> {
    let count = io.get_u32()?;
    if u64::from(count) > size.saturating_sub(io.getpos()?) / min_bytes {
        return Err(invalid(format!("invalid index count {count}")));
    }
    Ok(count as usize)
}

fn read_bdf_name(io: &mut AipsIo, size: u64) -> Result<PathBuf, StorageError> {
    let length = checked_count(io, size, 1)?;
    let mut name = vec![0; length];
    io.get_u8_into(&mut name)?;
    let name = String::from_utf8(name).map_err(|_| invalid("non-UTF-8 BDF path"))?;
    Ok(PathBuf::from(name))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Encoding {
    CrossShort,
    CrossInt,
    CrossFloat,
    Auto,
}

impl Encoding {
    fn read(io: &mut AipsIo) -> Result<Self, StorageError> {
        match io.get_i16()? {
            0 => Ok(Self::CrossShort),
            1 => Ok(Self::CrossInt),
            3 => Ok(Self::CrossFloat),
            10 => Ok(Self::Auto),
            tag => Err(invalid(format!("unsupported encoding {tag}"))),
        }
    }

    fn unit_bytes(self) -> u64 {
        match self {
            Self::CrossShort | Self::Auto => 4,
            Self::CrossInt | Self::CrossFloat => 8,
        }
    }
}

#[derive(Debug)]
struct Descriptor {
    encoding: Encoding,
    file: usize,
    start: u64,
    end: u64,
    antennas: u64,
    channels: usize,
    polarizations: usize,
    step: u64,
    block_start: u64,
    block_end: u64,
    window_offset: u64,
    row_bytes: u64,
    scale: f64,
}

impl Descriptor {
    fn read(io: &mut AipsIo, size: u64, bdf_count: usize) -> Result<Self, StorageError> {
        let version = io.get_i16()?;
        if version != 1 {
            return Err(invalid(format!("unsupported descriptor version {version}")));
        }
        let encoding = Encoding::read(io)?;
        let file = io.get_u32()? as usize;
        let baselines = u64::from(io.get_u32()?);
        let windows = io.get_u32()?;
        let channels = io.get_u32()? as usize;
        let polarizations = io.get_u32()? as usize;
        let step = u64::from(io.get_u32()?);
        let window = io.get_u32()? as usize;
        let start = nonnegative(io.get_i64()?, "row")?;
        let block_start = nonnegative(io.get_i64()?, "BDF offset")?;
        let window_offset = u64::from(io.get_u32()?);
        let scales = checked_count(io, size, 8)?;
        let mut scale = None;
        for slot in 0..scales {
            let value = io.get_f64()?;
            if slot == window {
                scale = Some(value);
            }
        }
        if file >= bdf_count
            || baselines == 0
            || window >= windows as usize
            || channels == 0
            || !(1..=4).contains(&polarizations)
        {
            return Err(invalid(format!(
                "invalid visibility dimensions for row {start}"
            )));
        }
        let scale = if encoding == Encoding::Auto {
            1.
        } else {
            scale
                .filter(|scale| scale.is_finite() && *scale != 0.)
                .ok_or_else(|| {
                    invalid(format!(
                        "invalid scale for row {start}, spectral window {window}"
                    ))
                })?
        };
        let antennas = if encoding == Encoding::Auto {
            0
        } else {
            let antennas = (8 * baselines + 1).isqrt().div_ceil(2);
            if antennas * (antennas - 1) / 2 != baselines {
                return Err(invalid(format!(
                    "non-triangular cross-baseline count {baselines}"
                )));
            }
            antennas
        };
        let samples = match (encoding, polarizations) {
            (Encoding::Auto, 3) => 4,
            (Encoding::Auto, 4) => 6,
            (_, count) => count,
        };
        let row_units = channels as u64 * samples as u64;
        if window_offset + row_units > step {
            return Err(invalid(format!(
                "spectral window at row {start} exceeds BDF baseline stride"
            )));
        }
        let unit_bytes = encoding.unit_bytes();
        let block_end = baselines
            .checked_mul(step)
            .and_then(|units| units.checked_mul(unit_bytes))
            .and_then(|bytes| block_start.checked_add(bytes))
            .ok_or_else(|| invalid("BDF block end overflows"))?;
        Ok(Self {
            encoding,
            file,
            start,
            end: start + baselines,
            antennas,
            channels,
            polarizations,
            step,
            block_start,
            block_end,
            window_offset,
            row_bytes: row_units * unit_bytes,
            scale,
        })
    }

    fn shape(&self, column: Column) -> Result<Vec<usize>, StorageError> {
        let correlations = if self.encoding == Encoding::Auto && self.polarizations == 3 {
            4
        } else {
            self.polarizations
        };
        match column {
            Column::Weight => Ok(vec![correlations]),
            Column::FloatData if self.encoding != Encoding::Auto || correlations > 2 => Err(
                invalid("FLOAT_DATA requires real autocorrelations with one or two polarizations"),
            ),
            _ => Ok(vec![correlations, self.channels]),
        }
    }

    fn offset(&self, row: usize) -> u64 {
        let logical = row as u64 - self.start;
        let baseline = if self.encoding == Encoding::Auto {
            logical
        } else {
            physical_baseline(self.antennas, logical)
        };
        self.block_start + (self.window_offset + baseline * self.step) * self.encoding.unit_bytes()
    }
}

fn nonnegative(value: i64, label: &str) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| invalid(format!("negative {label} {value}")))
}

// MS rows enumerate the lower-numbered antenna first. The BDF enumerates
// the higher-numbered antenna first.
fn physical_baseline(antennas: u64, logical: u64) -> u64 {
    let prefix = |first| first * (2 * antennas - first - 1) / 2;
    let (mut first, mut upper) = (0, antennas - 1);
    while first + 1 < upper {
        let middle = (first + upper) / 2;
        if prefix(middle) <= logical {
            first = middle;
        } else {
            upper = middle;
        }
    }
    let second = first + 1 + logical - prefix(first);
    second * (second - 1) / 2 + first
}

#[derive(Clone, Copy)]
pub(super) enum Column {
    Data,
    FloatData,
    Flag,
    Weight,
}

impl Column {
    pub(super) fn of(name: &str, element: PrimitiveType) -> Result<Self, StorageError> {
        match (name, element) {
            ("DATA", PrimitiveType::Complex32) => Ok(Self::Data),
            ("FLOAT_DATA", PrimitiveType::Float32) => Ok(Self::FloatData),
            ("FLAG", PrimitiveType::Bool) => Ok(Self::Flag),
            ("WEIGHT" | "SIGMA", PrimitiveType::Float32) => Ok(Self::Weight),
            _ => Err(invalid(format!(
                "unrecognised binding for column {name} ({element:?})"
            ))),
        }
    }
}

struct Bdf {
    id: usize,
    file: File,
    len: u64,
    preferred_read_bytes: u64,
}

pub(super) struct Reader<'a> {
    index: &'a Index,
    open: Option<Bdf>,
    cached_block: Option<(usize, u64, u64)>,
    bytes: Vec<u8>,
}

impl Reader<'_> {
    pub(super) fn cell(&mut self, column: Column, row: usize) -> Result<ArrayValue, StorageError> {
        let descriptor = self.index.descriptor(row)?;
        self.decode_cell(column, descriptor, row)
    }

    fn prepare_block(
        &mut self,
        column: Column,
        descriptor: &Descriptor,
        count: usize,
    ) -> Result<(), StorageError> {
        if count < 2 || !matches!(column, Column::Data | Column::FloatData) {
            return Ok(());
        }
        let block = (
            descriptor.file,
            descriptor.block_start,
            descriptor.block_end,
        );
        if self.cached_block == Some(block) {
            return Ok(());
        }
        let size = descriptor.block_end - descriptor.block_start;
        if size > super::STREAMING_TILED_COLUMN_BUFFER_BYTES as u64 {
            return Ok(());
        }
        let bdf = self.open_bdf(descriptor)?;
        // A sparse selection can seek directly to its cells. If loading the
        // integration reads no more than the estimated file I/O of individual
        // cells, load it once and decode in the requested row order.
        let read_bytes = bdf.preferred_read_bytes.max(descriptor.row_bytes);
        if size <= read_bytes.saturating_mul(count as u64) {
            self.read_payload(descriptor.block_start, size)?;
            self.cached_block = Some(block);
        }
        Ok(())
    }

    fn decode_cell(
        &mut self,
        column: Column,
        descriptor: &Descriptor,
        row: usize,
    ) -> Result<ArrayValue, StorageError> {
        let shape = descriptor.shape(column)?;
        let layout = IxDyn(&shape).f();
        match column {
            Column::Flag => return Ok(ArrayValue::Bool(ArrayD::from_elem(layout, false))),
            Column::Weight => return Ok(ArrayValue::Float32(ArrayD::from_elem(layout, 1.))),
            Column::Data | Column::FloatData => {}
        }
        let offset = descriptor.offset(row);
        let order = self.index.order;
        let bytes = if self.cached_block
            == Some((
                descriptor.file,
                descriptor.block_start,
                descriptor.block_end,
            )) {
            let start = (offset - descriptor.block_start) as usize;
            &self.bytes[start..start + descriptor.row_bytes as usize]
        } else {
            self.open_bdf(descriptor)?;
            self.read_payload(offset, descriptor.row_bytes)?;
            &self.bytes
        };
        let value = if matches!(column, Column::FloatData) {
            let values = bytes
                .as_chunks::<4>()
                .0
                .iter()
                .map(|bytes| float32(bytes, order))
                .collect();
            ArrayValue::Float32(ArrayD::from_shape_vec(layout, values).map_err(shape_error)?)
        } else {
            let values = decode_complex(descriptor, bytes, order);
            ArrayValue::Complex32(ArrayD::from_shape_vec(layout, values).map_err(shape_error)?)
        };
        Ok(value)
    }

    fn open_bdf(&mut self, descriptor: &Descriptor) -> Result<&Bdf, StorageError> {
        let name = &self.index.bdfs[descriptor.file];
        if self
            .open
            .as_ref()
            .is_none_or(|bdf| bdf.id != descriptor.file)
        {
            let file = File::open(name).map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!("open ASDM BDF {}: {error}", name.display()),
                )
            })?;
            let metadata = file.metadata()?;
            #[cfg(unix)]
            let preferred_read_bytes = std::os::unix::fs::MetadataExt::blksize(&metadata);
            #[cfg(not(unix))]
            let preferred_read_bytes = 0;
            self.open = Some(Bdf {
                id: descriptor.file,
                len: metadata.len(),
                file,
                preferred_read_bytes,
            });
        }
        let bdf = self.open.as_ref().expect("opened BDF");
        if descriptor.block_end > bdf.len {
            return Err(invalid(format!(
                "block for row {} extends beyond BDF {}",
                descriptor.start,
                name.display()
            )));
        }
        Ok(bdf)
    }

    fn read_payload(&mut self, offset: u64, bytes: u64) -> Result<(), StorageError> {
        let bytes = usize::try_from(bytes).map_err(|_| invalid("payload size overflows"))?;
        self.cached_block = None;
        self.bytes.resize(bytes, 0);
        let bdf = self.open.as_mut().expect("opened BDF");
        bdf.file.seek(SeekFrom::Start(offset))?;
        #[cfg(test)]
        tests::BDF_READS.with(|count| count.set(count.get() + 1));
        bdf.file.read_exact(&mut self.bytes)?;
        Ok(())
    }
}

fn decode_complex(descriptor: &Descriptor, bytes: &[u8], order: ByteOrder) -> Vec<Complex32> {
    if descriptor.encoding == Encoding::Auto {
        return decode_auto(descriptor.polarizations, bytes, order);
    }
    let sample_bytes = descriptor.encoding.unit_bytes() as usize;
    bytes
        .chunks_exact(sample_bytes)
        .map(|sample| {
            let (real, imag) = sample.split_at(sample_bytes / 2);
            let read = |bytes| match descriptor.encoding {
                Encoding::CrossShort => f64::from(int16(bytes, order)),
                Encoding::CrossInt => f64::from(int32(bytes, order)),
                Encoding::CrossFloat => f64::from(float32(bytes, order)),
                Encoding::Auto => unreachable!("autocorrelations decoded separately"),
            };
            Complex32::new(
                (read(real) / descriptor.scale) as f32,
                (read(imag) / descriptor.scale) as f32,
            )
        })
        .collect()
}

fn decode_auto(polarizations: usize, bytes: &[u8], order: ByteOrder) -> Vec<Complex32> {
    let real = |sample| Complex32::new(sample, 0.);
    let lanes = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|bytes| float32(bytes, order));
    if polarizations <= 2 {
        return lanes.map(real).collect();
    }
    let width = if polarizations == 3 { 4 } else { 6 };
    let mut output = Vec::with_capacity(bytes.len() / width);
    for channel in bytes.chunks_exact(width * 4) {
        let get = |lane: usize| float32(&channel[lane * 4..][..4], order);
        let xy = Complex32::new(get(1), get(2));
        let yx = if polarizations == 3 {
            xy.conj()
        } else {
            Complex32::new(get(3), get(4))
        };
        output.extend([real(get(0)), xy, yx, real(get(width - 1))]);
    }
    output
}

fn int16(bytes: &[u8], order: ByteOrder) -> i16 {
    let bytes = bytes
        .try_into()
        .expect("short sample decoded in two-byte chunks");
    match order {
        ByteOrder::BigEndian => i16::from_be_bytes(bytes),
        ByteOrder::LittleEndian => i16::from_le_bytes(bytes),
    }
}

fn int32(bytes: &[u8], order: ByteOrder) -> i32 {
    let bytes = bytes
        .try_into()
        .expect("integer sample decoded in four-byte chunks");
    match order {
        ByteOrder::BigEndian => i32::from_be_bytes(bytes),
        ByteOrder::LittleEndian => i32::from_le_bytes(bytes),
    }
}

fn float32(bytes: &[u8], order: ByteOrder) -> f32 {
    f32::from_bits(int32(bytes, order) as u32)
}

fn shape_error(error: ndarray::ShapeError) -> StorageError {
    invalid(format!("invalid visibility array shape: {error}"))
}

fn invalid(reason: impl std::fmt::Display) -> StorageError {
    StorageError::FormatMismatch(format!("{TYPE}: {reason}"))
}

pub(super) fn fill_rows(
    table: &super::TableDatContents,
    sequence: u32,
    bound: &[(usize, &super::table_control::PlainColumnEntry)],
    rows: &mut [casa_types::RecordValue],
) -> Result<(), StorageError> {
    let mut reader = table.asdm.get(sequence)?.reader();
    let columns = bound
        .iter()
        .map(|(position, binding)| {
            let column = &table.table_desc.columns[*position];
            Ok((binding.original_name.as_str(), table.asdm_column(column)?))
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    for (row, record) in rows.iter_mut().enumerate() {
        for &(name, column) in &columns {
            record.upsert(name, casa_types::Value::Array(reader.cell(column, row)?));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "asdm_stman_tests.rs"]
mod tests;
