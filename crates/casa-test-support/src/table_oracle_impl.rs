// SPDX-License-Identifier: LGPL-3.0-or-later
//! Private casacore C++ oracle implementation.

#[cfg(has_casacore_cpp)]
use crate::oracle_ffi::*;
#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::CasacoreOracleRuntime;

// ===== Safe wrappers for C++ table shim =====

/// Fixture identifiers for C++ table operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CppTableFixture {
    ScalarPrimitives,
    FixedArray,
    Keywords,
    SsmScalarPrimitives,
    SsmFixedArray,
    SsmKeywords,
    /// Verify-only: scalar_primitives with col_str removed (any DM).
    MutationRemovedColumn,
    /// Verify-only: scalar_primitives with row 1 removed (any DM).
    MutationRemovedRows,
    /// Verify-only: scalar_primitives + extra(Float32, 42.0) column (any DM).
    MutationAddedColumn,
    /// Lock interop: table with (id: Int, name: String), created with
    /// PermanentLocking to produce a `table.lock` file with sync data.
    LockFile,
    /// RefTable interop: parent table with 3 rows (id: Int, name: String)
    /// and a RefTable selecting rows 0 and 2. The path argument is a
    /// directory containing `parent.tbl/` and `ref.tbl/`.
    RefTable,
    /// Sorted RefTable interop: parent table with 5 rows (id: Int, name:
    /// String, value: Float), sorted descending by `id` and saved as a
    /// RefTable. The path argument is a directory containing `parent.tbl/`
    /// and `sorted.tbl/`.
    SortedRefTable,
    /// ConcatTable interop: two 3-row tables (id: Int, name: String) concatenated
    /// as a ConcatTable. The path argument is a directory containing `part0.tbl/`,
    /// `part1.tbl/`, and `concat.tbl/`.
    ConcatTable,
    /// Deep copy interop: a 5-row table deep-copied to a different storage
    /// manager. The path argument is a directory containing `original.tbl/`
    /// and `copy.tbl/`.
    DeepCopy,
    /// ColumnsIndex interop: table with `antenna_id` (Int32), 50 rows,
    /// value = `row_index % 10`. Used to verify `ColumnsIndex` lookups on
    /// C++-written data.
    ColumnsIndex,
    /// ISM scalar primitives: same schema as `SsmScalarPrimitives` (Bool, Int,
    /// Double, String) but stored with `IncrementalStMan`.
    IsmScalarPrimitives,
    /// ISM slowly changing: table with `SCAN_NUMBER` (Int) and `FLAG` (Bool),
    /// 10 rows where values repeat across multiple consecutive rows, exercising
    /// the ISM delta-compression semantics.
    IsmSlowlyChanging,
    /// ISM complex scalars: table with `col_c32` (Complex) and `col_c64`
    /// (DComplex), 3 rows, stored with `IncrementalStMan`.
    IsmComplexScalars,
    /// TiledColumnStMan interop: Fixed-shape Float32 \[2,3\] array column,
    /// 3 rows, tile shape \[2,3,2\].
    TiledColumnStMan,
    /// TiledShapeStMan interop: Variable-shape Float32 array column,
    /// 4 rows with two different shapes (\[2,3\] and \[3,2\]).
    TiledShapeStMan,
    /// TiledCellStMan interop: Variable-shape Float32 array column,
    /// 3 rows each with a unique shape (\[2,3\], \[4,2\], \[3,3\]).
    TiledCellStMan,
    /// TiledDataStMan interop: Float32 2-D `data` and Int32 id column `id`
    /// in hypercolumn `TiledData`, 5 rows in two hypercubes: id 10 with
    /// \[2,3\] cells (rows 0-1) and id 20 with \[3,2\] cells (rows 2-4).
    /// Cell `row` holds `100*row + i` at column-major index `i`. Written
    /// little-endian.
    TiledDataStMan,
    /// [`TiledDataStMan`](Self::TiledDataStMan) written big-endian.
    TiledDataStManBigEndian,
    /// [`TiledDataStMan`](Self::TiledDataStMan) with the cells of rows 1
    /// and 3 negated; writes the unpatched table, verifies the patched one.
    TiledDataStManPatched,
    /// TiledDataStMan data columns of equal pixel size: Float `a`, `b`,
    /// Complex `c`, Int `d` (2-D) and id `id`, in hypercolumn `TiledMulti`
    /// defined as data \[b, a, d, c\], so casacore's tile is c, d, a, b.
    /// Hypercube id 10 (\[2,3\] cells, rows 0-1) also holds a stray values
    /// field `a`; id 20 has \[3,2\] cells (rows 2-3). Written little-endian.
    TiledDataStManMulti,
    /// [`TiledDataStManMulti`](Self::TiledDataStManMulti) written big-endian.
    TiledDataStManMultiBigEndian,
    /// [`TiledDataStManMulti`](Self::TiledDataStManMulti) with `a` negated in
    /// rows 1 and 3 and every other cell unchanged; writes the unpatched
    /// table, verifies the patched one.
    TiledDataStManMultiPatched,
    /// A scalar Float data column `s` over a 2-axis TiledDataStMan
    /// hypercube \[2,2\]: rows 0-3 hold 10-13.
    TiledDataStManScalar,
    /// TiledShapeStMan hypercolumn `TiledMixed` defined as data \[flag,
    /// sigma, data, weight\], bound flag (Bool), data (Complex), weight,
    /// sigma (Float), so casacore's tile is data, weight, sigma, flag. Rows
    /// 0-1 have \[2,3\] cells and rows 2-3 \[3,2\]; at column-major index
    /// `i` of row `r`: data `(10r + i, -i)`, weight `0.5(r + 1) + i`, sigma
    /// `100 + 10r + i`, flag `(i + r)` odd. Written little-endian.
    TiledShapeMixed,
    /// [`TiledShapeMixed`](Self::TiledShapeMixed) written big-endian.
    TiledShapeMixedBigEndian,
    /// [`TiledShapeMixed`](Self::TiledShapeMixed) with data negated and flag
    /// inverted in rows 1 and 3; writes the unpatched table, verifies the
    /// patched one.
    TiledShapeMixedPatched,
    /// The [`TiledShapeMixed`](Self::TiledShapeMixed) table under a
    /// TiledCellStMan: the same definition, binding and cells, each cell its
    /// own hypercube of \[2,2\] tiles crossing its edges. Written
    /// little-endian; verified as `TiledShapeMixed`, whose check reads cell
    /// values only, as is the patched form ([`TiledShapeMixedPatched`](Self::TiledShapeMixedPatched)).
    TiledCellMixed,
    /// [`TiledCellMixed`](Self::TiledCellMixed) written big-endian.
    TiledCellMixedBigEndian,
    /// TiledColumnStMan `TiledCol` without a hypercolumn definition, bound
    /// count (Int), flag (Bool), value (Double), amp (Float), fixed \[2,3\],
    /// so casacore's tile is value, amp, count, flag. Three rows; at index
    /// `i` of row `r`: count `1000r + i`, flag `i % 3 == r`, value
    /// `0.25 + r + 10i`, amp `-1.5(i + 1) - r`. Written little-endian.
    TiledColumnNoDef,
    /// [`TiledColumnNoDef`](Self::TiledColumnNoDef) written big-endian.
    TiledColumnNoDefBigEndian,
    /// [`TiledColumnNoDef`](Self::TiledColumnNoDef) with no rows, written
    /// little-endian. Verifying opens it with every column's fixed shape and
    /// one \[2,3,0\] hypercube of \[2,3,2\] tiles, then adds row 0 and reads
    /// it back.
    TiledColumnNoDefEmpty,
    /// [`TiledColumnNoDefEmpty`](Self::TiledColumnNoDefEmpty) written
    /// big-endian.
    TiledColumnNoDefEmptyBigEndian,
    /// ForwardColumnEngine interop: base table with col_value (Double, 3 rows)
    /// and a forwarding table that delegates col_value via ForwardColumnEngine.
    /// The path is the forwarding table directory; base is at `{path}_base`.
    ForwardColumn,
    /// ScaledArrayEngine interop: stored_col (Int array \[2\], 3 rows) and
    /// virtual_col (Double array, via ScaledArrayEngine with scale=2.5, offset=10.0).
    ScaledArray,
    /// AipsIO variable-shape array: Float32 column "data" with ndim=2,
    /// 4 rows with shapes \[2,3\], \[3,2\], \[3,2\], \[2,3\], values 1.0..24.0.
    AipsIOVariableArray,
    /// SSM variable-shape array: same schema and data as `AipsIOVariableArray`
    /// but stored with `StandardStMan`.
    SsmVariableArray,
    /// Undefined scalars: 4-row table (Int, Double, String) where only rows 0
    /// and 2 are written; rows 1 and 3 keep default values (0, 0.0, "").
    UndefinedScalars,
    /// Column keywords: 2-row table (flux: Double, id: Int) with table-level
    /// and per-column keywords. flux has "unit"="Jy" and "ref_frame"="LSRK";
    /// id has "description"="source identifier".
    ColumnKeywords,
    /// Record column: 3-row table (id: Int, meta: Record) with per-row
    /// record values. Row 0: {unit: "Jy", value: 2.5}, Row 1: {flag: true},
    /// Row 2: {} (empty).
    AipsIORecordColumn,
    /// Mixed schema: 2-row table combining scalar (Int, Double), fixed array
    /// (Float32 \[4\]), variable array (Float32 2-D), record column, table
    /// keywords (telescope, version), and column keywords (flux: unit="Jy").
    MixedSchema,
    /// Mixed data managers, as a MeasurementSet has: antenna (Int) in
    /// StandardStMan; flux (Double) and spectrum (variable Float 1-D) in
    /// StManAipsIO, whose sequence number is not 0; time (Double) in
    /// IncrementalStMan; data (variable Complex 2-D) in TiledShapeStMan.
    /// The verifier also checks each column's manager.
    MixedManagers,
    /// TableInfo metadata: 1-row table (id: Int) with TableInfo set to
    /// type="Measurement", subType="UVFITS". Tests `table.info` file interop.
    TableInfoMetadata,
    /// CASA LOG table: canonical `TableLogSink` columns and metadata stored
    /// with `StandardStMan("SSM", 32768)`.
    LogTable,
    /// AipsIO all numeric scalars: 3 rows × 6 cols (uChar, Short, uShort,
    /// uInt, Float, Int64) stored with `StManAipsIO`.
    AipsioAllNumericScalars,
    /// AipsIO complex scalars: 3 rows × 2 cols (Complex, DComplex) stored
    /// with `StManAipsIO`.
    AipsioComplexScalars,
    /// AipsIO typed arrays: 3 rows × 3 cols (Int\[4\], Double\[2,2\], Float32\[3\])
    /// stored with `StManAipsIO`.
    AipsioTypedArrays,
    /// AipsIO 3D fixed array: Float32 \[2,3,4\], 2 rows with ascending values
    /// 1..24 and 25..48, stored with `StManAipsIO`.
    Aipsio3DFixedArray,
    /// SSM all numeric scalars: same as `AipsioAllNumericScalars` but stored
    /// with `StandardStMan`.
    SsmAllNumericScalars,
    /// SSM complex scalars: same as `AipsioComplexScalars` but stored with
    /// `StandardStMan`.
    SsmComplexScalars,
    /// SSM typed arrays: 3 rows × 3 cols (Int\[4\], Double\[2,2\], Complex32\[2\])
    /// stored with `StandardStMan`.
    SsmTypedArrays,
    /// AipsIO Complex32 variable-shape array: 4 rows with shapes \[2,4\], \[4,2\],
    /// \[4,2\], \[2,4\], values (1,0.5)..(32,16.0). Matches MS DATA column pattern.
    AipsIOComplexVariableArray,
    /// SSM Complex32 variable-shape array: same data as `AipsIOComplexVariableArray`
    /// but stored with `StandardStMan`.
    SsmComplexVariableArray,
    /// AipsIO unsigned integer arrays: 3 rows × 3 cols (uChar\[4\], uShort\[4\],
    /// uInt\[4\]) stored with `StManAipsIO`.
    AipsioUnsignedArrays,
    /// SSM unsigned integer arrays: same as `AipsioUnsignedArrays` but stored
    /// with `StandardStMan`.
    SsmUnsignedArrays,
    /// AipsIO string array: 3 rows × 1 col (String\[3\]) stored with `StManAipsIO`.
    AipsioStringArray,
    /// SSM string array: same as `AipsioStringArray` but stored with `StandardStMan`.
    SsmStringArray,
    /// SSM strings longer than a string bucket: 3 rows of a scalar
    /// `String`, a `String[3]` and a variable-shape `String` vector, of
    /// elements up to 100 kB, continued in linked string buckets.
    SsmLongStrings,
    /// AipsIO Complex64 2D array: 3 rows × 1 col (DComplex\[2,2\]) stored with
    /// `StManAipsIO`.
    AipsioComplex64Array2D,
    /// SSM Complex64 2D array: same as `AipsioComplex64Array2D` but stored with
    /// `StandardStMan`.
    SsmComplex64Array2D,
    /// AipsIO empty table: schema with Int32 scalar + Float32\[4\] array, 0 rows,
    /// stored with `StManAipsIO`.
    AipsioEmptyTable,
    /// SSM empty table: same schema as `AipsioEmptyTable` but stored with
    /// `StandardStMan`.
    SsmEmptyTable,
    /// TiledColumnStMan 3D array: Float32 \[2,3,4\], 3 rows, tile shape \[2,3,2,2\].
    TiledColumn3DArray,
    /// ISM typed arrays: 3 rows × 3 cols (Int\[4\], Double\[2,2\], Bool\[3\])
    /// stored with `IncrementalStMan`. Full CC/CR/RC cross-matrix.
    IsmTypedArrays,
    /// ISM complex arrays: 3 rows × 2 cols (Complex32\[2\], Complex64\[2\])
    /// stored with `IncrementalStMan`. Full CC/CR/RC cross-matrix.
    IsmComplexArrays,
    /// ISM column keywords: 2-row table (flux: Double, id: Int) with per-column
    /// keywords, stored with `IncrementalStMan`. Full CC/CR/RC cross-matrix.
    IsmColumnKeywords,
    /// ISM element arrays: 3 rows of direct fixed-shape arrays of every
    /// element type, of shapes \[1\], \[3\] and \[2,3\] (`Float64_1` holds
    /// `[7.0]` in row 0), stored with `IncrementalStMan`. Full CC/CR/RC
    /// cross-matrix.
    IsmElementArrays,
    /// SSM element arrays: the arrays of `IsmElementArrays`, once stored
    /// directly and once indirectly in `table.f0i` (columns `Float64_1` and
    /// `Float64_1_ind`), and variable-shape arrays of every element type
    /// (`Float64_var`), stored with `StandardStMan`. Full CC/CR/RC
    /// cross-matrix.
    SsmElementArrays,
    /// ISM indirect arrays: the indirect fixed-shape and variable-shape
    /// columns of `SsmElementArrays`, which casacore's IncrementalStMan keeps
    /// in `table.f0i` (`ISMIndColumn`), little-endian. CC and CR only:
    /// casa-rs does not write them.
    IsmIndirectArrays,
    /// `IsmIndirectArrays` in a big-endian table.
    IsmIndirectArraysBigEndian,
    /// ISM bucket starts: TIME (Double, `4e9 + row`) and NAME (String,
    /// `r<row>:` and 600 copies of the row's letter), 8 rows, stored with
    /// `IncrementalStMan`. In casa-rs's buckets a value changes at every
    /// bucket's first row. Full CC/CR/RC cross-matrix.
    IsmBucketStarts,
}

/// A C++ fixture entry point: it writes or verifies the table at a path and
/// returns nonzero, with an error message, on failure.
#[cfg(has_casacore_cpp)]
type CppFixtureEntry =
    unsafe extern "C" fn(*const std::ffi::c_char, *mut *mut std::ffi::c_char) -> i32;

/// The C++ writer and verifier of a storage-manager cell fixture.
#[cfg(has_casacore_cpp)]
struct CellFixtureEntries {
    write: CppFixtureEntry,
    verify: CppFixtureEntry,
}

/// The entry points of the storage-manager cell fixtures (every element
/// type, indirect arrays, bucket boundaries), which the fixture matches of
/// [`cpp_table_write_unlocked`] and [`cpp_table_verify_unlocked`] leave to
/// this table.
#[cfg(has_casacore_cpp)]
fn cell_fixture_entries(fixture: CppTableFixture) -> Result<CellFixtureEntries, String> {
    let (write, verify): (CppFixtureEntry, CppFixtureEntry) = match fixture {
        CppTableFixture::IsmElementArrays => (
            cpp_table_write_ism_element_arrays,
            cpp_table_verify_ism_element_arrays,
        ),
        CppTableFixture::SsmElementArrays => (
            cpp_table_write_ssm_element_arrays,
            cpp_table_verify_ssm_element_arrays,
        ),
        CppTableFixture::IsmIndirectArrays => (
            cpp_table_write_ism_indirect_arrays,
            cpp_table_verify_ism_indirect_arrays,
        ),
        CppTableFixture::IsmIndirectArraysBigEndian => (
            cpp_table_write_ism_indirect_arrays_big_endian,
            cpp_table_verify_ism_indirect_arrays,
        ),
        CppTableFixture::IsmBucketStarts => (
            cpp_table_write_ism_bucket_starts,
            cpp_table_verify_ism_bucket_starts,
        ),
        other => return Err(format!("{other:?} has no C++ write and verify")),
    };
    Ok(CellFixtureEntries { write, verify })
}

/// Write a table fixture using C++ casacore. Returns an error string on failure.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_table_write_unlocked(
    fixture: CppTableFixture,
    path: &std::path::Path,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        match fixture {
            CppTableFixture::ScalarPrimitives => {
                cpp_table_write_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::FixedArray => cpp_table_write_fixed_array(c_path.as_ptr(), &mut error),
            CppTableFixture::Keywords => cpp_table_write_keywords(c_path.as_ptr(), &mut error),
            CppTableFixture::SsmScalarPrimitives => {
                cpp_table_write_ssm_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmFixedArray => {
                cpp_table_write_ssm_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmKeywords => {
                cpp_table_write_ssm_keywords(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::LockFile => cpp_table_write_with_lock(c_path.as_ptr(), &mut error),
            CppTableFixture::RefTable => cpp_table_write_ref_table(c_path.as_ptr(), &mut error),
            CppTableFixture::SortedRefTable => {
                cpp_table_write_sorted_ref_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ConcatTable => {
                cpp_table_write_concat_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::DeepCopy => cpp_table_write_deep_copy(c_path.as_ptr(), &mut error),
            CppTableFixture::ColumnsIndex => {
                cpp_table_write_columns_index_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmScalarPrimitives => {
                cpp_table_write_ism_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmSlowlyChanging => {
                cpp_table_write_ism_slowly_changing(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmComplexScalars => {
                cpp_table_write_ism_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumnStMan => {
                cpp_table_write_tiled_column_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledShapeStMan => {
                cpp_table_write_tiled_shape_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledCellStMan => {
                cpp_table_write_tiled_cell_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStMan | CppTableFixture::TiledDataStManPatched => {
                cpp_table_write_tiled_data_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStManBigEndian => {
                cpp_table_write_tiled_data_stman_big_endian(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStManMulti | CppTableFixture::TiledDataStManMultiPatched => {
                cpp_table_write_tiled_data_multi(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStManMultiBigEndian => {
                cpp_table_write_tiled_data_multi_big_endian(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStManScalar => {
                cpp_table_write_tiled_data_scalar(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledShapeMixed | CppTableFixture::TiledShapeMixedPatched => {
                cpp_table_write_tiled_shape_mixed(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledShapeMixedBigEndian => {
                cpp_table_write_tiled_shape_mixed_big_endian(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledCellMixed => {
                cpp_table_write_tiled_cell_mixed(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledCellMixedBigEndian => {
                cpp_table_write_tiled_cell_mixed_big_endian(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumnNoDef => {
                cpp_table_write_tiled_column_nodef(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumnNoDefBigEndian => {
                cpp_table_write_tiled_column_nodef_big_endian(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumnNoDefEmpty => {
                cpp_table_write_tiled_column_nodef_empty(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumnNoDefEmptyBigEndian => {
                cpp_table_write_tiled_column_nodef_empty_big_endian(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ForwardColumn => {
                cpp_table_write_forward_column_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ScaledArray => {
                cpp_table_write_scaled_array_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIOVariableArray => {
                cpp_table_write_aipsio_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmVariableArray => {
                cpp_table_write_ssm_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::UndefinedScalars => {
                cpp_table_write_undefined_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ColumnKeywords => {
                cpp_table_write_column_keywords(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIORecordColumn => {
                cpp_table_write_record_column(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MixedSchema => {
                cpp_table_write_mixed_schema(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MixedManagers => {
                cpp_table_write_mixed_managers(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TableInfoMetadata => {
                cpp_table_write_table_info(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::LogTable => cpp_table_write_log_table(c_path.as_ptr(), &mut error),
            CppTableFixture::AipsioAllNumericScalars => {
                cpp_table_write_aipsio_all_numeric_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioComplexScalars => {
                cpp_table_write_aipsio_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioTypedArrays => {
                cpp_table_write_aipsio_typed_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::Aipsio3DFixedArray => {
                cpp_table_write_aipsio_3d_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmAllNumericScalars => {
                cpp_table_write_ssm_all_numeric_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmComplexScalars => {
                cpp_table_write_ssm_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmTypedArrays => {
                cpp_table_write_ssm_typed_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIOComplexVariableArray => {
                cpp_table_write_aipsio_complex_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmComplexVariableArray => {
                cpp_table_write_ssm_complex_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioUnsignedArrays => {
                cpp_table_write_aipsio_unsigned_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmUnsignedArrays => {
                cpp_table_write_ssm_unsigned_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioStringArray => {
                cpp_table_write_aipsio_string_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmStringArray => {
                cpp_table_write_ssm_string_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmLongStrings => {
                cpp_table_write_ssm_long_strings(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioComplex64Array2D => {
                cpp_table_write_aipsio_complex64_2d_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmComplex64Array2D => {
                cpp_table_write_ssm_complex64_2d_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioEmptyTable => {
                cpp_table_write_aipsio_empty_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmEmptyTable => {
                cpp_table_write_ssm_empty_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumn3DArray => {
                cpp_table_write_tiled_column_3d_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmTypedArrays => {
                cpp_table_write_ism_typed_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmComplexArrays => {
                cpp_table_write_ism_complex_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmColumnKeywords => {
                cpp_table_write_ism_column_keywords(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MutationRemovedColumn
            | CppTableFixture::MutationRemovedRows
            | CppTableFixture::MutationAddedColumn => {
                return Err("mutation fixtures are verify-only (no C++ write)".to_string());
            }
            fixture => (cell_fixture_entries(fixture)?.write)(c_path.as_ptr(), &mut error),
        }
    };

    unsafe {
        CasacoreOracleRuntime::cpp_status("table.table_write", rc, error, cpp_table_free_error)
    }
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Verify a table fixture using C++ casacore. Returns an error string on failure.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_table_verify_unlocked(
    fixture: CppTableFixture,
    path: &std::path::Path,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        match fixture {
            CppTableFixture::ScalarPrimitives => {
                cpp_table_verify_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::FixedArray => {
                cpp_table_verify_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::Keywords => cpp_table_verify_keywords(c_path.as_ptr(), &mut error),
            CppTableFixture::SsmScalarPrimitives => {
                cpp_table_verify_ssm_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmFixedArray => {
                cpp_table_verify_ssm_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmKeywords => {
                cpp_table_verify_ssm_keywords(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MutationRemovedColumn => {
                cpp_table_verify_mutation_removed_column(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MutationRemovedRows => {
                cpp_table_verify_mutation_removed_rows(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MutationAddedColumn => {
                cpp_table_verify_mutation_added_column(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::LockFile => cpp_table_verify_with_lock(c_path.as_ptr(), &mut error),
            CppTableFixture::RefTable => cpp_table_verify_ref_table(c_path.as_ptr(), &mut error),
            CppTableFixture::SortedRefTable => {
                cpp_table_verify_sorted_ref_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ConcatTable => {
                cpp_table_verify_concat_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::DeepCopy => cpp_table_verify_deep_copy(c_path.as_ptr(), &mut error),
            CppTableFixture::IsmScalarPrimitives => {
                cpp_table_verify_ism_scalar_primitives(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmSlowlyChanging => {
                cpp_table_verify_ism_slowly_changing(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmComplexScalars => {
                cpp_table_verify_ism_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ColumnsIndex => {
                return Err(
                    "ColumnsIndex fixture has no C++ verify (Rust does the verification)"
                        .to_string(),
                );
            }
            CppTableFixture::TiledColumnStMan => {
                cpp_table_verify_tiled_column_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledShapeStMan => {
                cpp_table_verify_tiled_shape_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledCellStMan => {
                cpp_table_verify_tiled_cell_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStMan | CppTableFixture::TiledDataStManBigEndian => {
                cpp_table_verify_tiled_data_stman(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStManPatched => {
                cpp_table_verify_tiled_data_stman_patched(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStManMulti
            | CppTableFixture::TiledDataStManMultiBigEndian => {
                cpp_table_verify_tiled_data_multi(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStManMultiPatched => {
                cpp_table_verify_tiled_data_multi_patched(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledDataStManScalar => {
                cpp_table_verify_tiled_data_scalar(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledShapeMixed
            | CppTableFixture::TiledShapeMixedBigEndian
            | CppTableFixture::TiledCellMixed
            | CppTableFixture::TiledCellMixedBigEndian => {
                cpp_table_verify_tiled_shape_mixed(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledShapeMixedPatched => {
                cpp_table_verify_tiled_shape_mixed_patched(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumnNoDef | CppTableFixture::TiledColumnNoDefBigEndian => {
                cpp_table_verify_tiled_column_nodef(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumnNoDefEmpty
            | CppTableFixture::TiledColumnNoDefEmptyBigEndian => {
                cpp_table_verify_tiled_column_nodef_empty(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ForwardColumn => {
                cpp_table_verify_forward_column_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ScaledArray => {
                cpp_table_verify_scaled_array_fixture(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIOVariableArray => {
                cpp_table_verify_aipsio_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmVariableArray => {
                cpp_table_verify_ssm_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::UndefinedScalars => {
                cpp_table_verify_undefined_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::ColumnKeywords => {
                cpp_table_verify_column_keywords(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIORecordColumn => {
                cpp_table_verify_record_column(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MixedSchema => {
                cpp_table_verify_mixed_schema(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::MixedManagers => {
                cpp_table_verify_mixed_managers(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TableInfoMetadata => {
                cpp_table_verify_table_info(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::LogTable => cpp_table_verify_log_table(c_path.as_ptr(), &mut error),
            CppTableFixture::AipsioAllNumericScalars => {
                cpp_table_verify_aipsio_all_numeric_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioComplexScalars => {
                cpp_table_verify_aipsio_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioTypedArrays => {
                cpp_table_verify_aipsio_typed_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::Aipsio3DFixedArray => {
                cpp_table_verify_aipsio_3d_fixed_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmAllNumericScalars => {
                cpp_table_verify_ssm_all_numeric_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmComplexScalars => {
                cpp_table_verify_ssm_complex_scalars(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmTypedArrays => {
                cpp_table_verify_ssm_typed_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsIOComplexVariableArray => {
                cpp_table_verify_aipsio_complex_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmComplexVariableArray => {
                cpp_table_verify_ssm_complex_variable_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioUnsignedArrays => {
                cpp_table_verify_aipsio_unsigned_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmUnsignedArrays => {
                cpp_table_verify_ssm_unsigned_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioStringArray => {
                cpp_table_verify_aipsio_string_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmStringArray => {
                cpp_table_verify_ssm_string_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmLongStrings => {
                cpp_table_verify_ssm_long_strings(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioComplex64Array2D => {
                cpp_table_verify_aipsio_complex64_2d_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmComplex64Array2D => {
                cpp_table_verify_ssm_complex64_2d_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::AipsioEmptyTable => {
                cpp_table_verify_aipsio_empty_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::SsmEmptyTable => {
                cpp_table_verify_ssm_empty_table(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::TiledColumn3DArray => {
                cpp_table_verify_tiled_column_3d_array(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmTypedArrays => {
                cpp_table_verify_ism_typed_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmComplexArrays => {
                cpp_table_verify_ism_complex_arrays(c_path.as_ptr(), &mut error)
            }
            CppTableFixture::IsmColumnKeywords => {
                cpp_table_verify_ism_column_keywords(c_path.as_ptr(), &mut error)
            }
            fixture => (cell_fixture_entries(fixture)?.verify)(c_path.as_ptr(), &mut error),
        }
    };

    unsafe {
        CasacoreOracleRuntime::cpp_status("table.table_verify", rc, error, cpp_table_free_error)
    }
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Open the table at `path` in C++ casacore with `UserLocking`, take, release
/// and retake an explicit read lock, and return `(rows, columns)` as the
/// locked table reports them.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_lock_read_relock(path: &std::path::Path) -> Result<(u64, u32), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut rows: u64 = 0;
    let mut columns: u32 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc =
        unsafe { cpp_table_lock_read_relock(c_path.as_ptr(), &mut rows, &mut columns, &mut error) };
    unsafe {
        CasacoreOracleRuntime::cpp_status("table.lock_read_relock", rc, error, cpp_table_free_error)
    }
    .map_err(|error| error.to_string())?;
    Ok((rows, columns))
}

/// Write the long-string rows in C++ casacore column by column, in
/// StandardStMan buckets of 32 KiB, in the given byte order.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_write_ssm_long_strings_by_column(
    path: &std::path::Path,
    big_endian: bool,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_table_write_ssm_long_strings_by_column(
            c_path.as_ptr(),
            i32::from(big_endian),
            &mut error,
        )
    };
    unsafe {
        CasacoreOracleRuntime::cpp_status(
            "table.write_ssm_long_strings_by_column",
            rc,
            error,
            cpp_table_free_error,
        )
    }
    .map_err(|error| error.to_string())
}

/// Update the long-string table at `path` in C++ casacore, then reopen it
/// there and check every cell.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_update_ssm_long_strings(path: &std::path::Path) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe { cpp_table_update_ssm_long_strings(c_path.as_ptr(), &mut error) };
    unsafe {
        CasacoreOracleRuntime::cpp_status(
            "table.update_ssm_long_strings",
            rc,
            error,
            cpp_table_free_error,
        )
    }
    .map_err(|error| error.to_string())
}

/// C++ casacore's account of the StandardStMan holding column `NAME` of
/// the table at `path`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_ssm_statistics(path: &std::path::Path) -> Result<SsmStatistics, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut statistics = [0i64; 7];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc =
        unsafe { cpp_table_ssm_statistics(c_path.as_ptr(), statistics.as_mut_ptr(), &mut error) };
    unsafe {
        CasacoreOracleRuntime::cpp_status("table.ssm_statistics", rc, error, cpp_table_free_error)
    }
    .map_err(|error| error.to_string())?;
    let [
        buckets,
        free_buckets,
        first_free_bucket,
        last_string_bucket,
        index_buckets,
        first_index_bucket,
        index_bucket_offset,
    ] = statistics;
    Ok(SsmStatistics {
        buckets,
        free_buckets,
        first_free_bucket,
        last_string_bucket,
        index_buckets,
        first_index_bucket,
        index_bucket_offset,
    })
}

/// Open the table that a table keyword of the table at `path` (of its column
/// `column`, when given) refers to in C++ casacore and return its name.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_keyword_table_name(
    path: &std::path::Path,
    column: Option<&str>,
    keyword: &str,
) -> Result<String, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let c_column = CasacoreOracleRuntime::c_string("column name", column.unwrap_or(""))
        .map_err(|error| error.to_string())?;
    let c_keyword = CasacoreOracleRuntime::c_string("keyword name", keyword)
        .map_err(|error| error.to_string())?;
    let mut buf = vec![0 as std::ffi::c_char; 4096];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_table_keyword_table_name(
            c_path.as_ptr(),
            c_column.as_ptr(),
            c_keyword.as_ptr(),
            buf.as_mut_ptr(),
            buf.len() as i32,
            &mut error,
        )
    };
    unsafe {
        CasacoreOracleRuntime::cpp_status(
            "table.keyword_table_name",
            rc,
            error,
            cpp_table_free_error,
        )
    }
    .map_err(|error| error.to_string())?;
    CasacoreOracleRuntime::output_c_char_string("table.keyword_table_name", &buf)
        .map_err(|error| error.to_string())
}

/// Read the Double array table keyword `keyword` of the table at `path` in
/// C++ casacore: its shape, and its elements in casacore's storage order
/// (the first axis fastest).
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_keyword_double_array(
    path: &std::path::Path,
    keyword: &str,
) -> Result<(Vec<usize>, Vec<f64>), String> {
    const MAX_NDIM: usize = 8;
    const MAX_VALUES: usize = 1 << 16;
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let c_keyword = CasacoreOracleRuntime::c_string("keyword name", keyword)
        .map_err(|error| error.to_string())?;
    let mut shape = [0_i64; MAX_NDIM];
    let mut values = vec![0.0_f64; MAX_VALUES];
    let (mut ndim, mut count) = (0_i32, 0_i64);
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_table_keyword_double_array(
            c_path.as_ptr(),
            c_keyword.as_ptr(),
            shape.as_mut_ptr(),
            MAX_NDIM as i32,
            &mut ndim,
            values.as_mut_ptr(),
            MAX_VALUES as i64,
            &mut count,
            &mut error,
        )
    };
    unsafe {
        CasacoreOracleRuntime::cpp_status(
            "table.keyword_double_array",
            rc,
            error,
            cpp_table_free_error,
        )
    }
    .map_err(|error| error.to_string())?;
    values.truncate(count as usize);
    let shape = shape[..ndim as usize].iter().map(|&d| d as usize).collect();
    Ok((shape, values))
}

/// A C++ casacore `Table` and its `ScalarColumn<Int>` "id", opened with
/// `UserLocking` and held open; closed with [`cpp_held_reader_close`].
#[cfg(has_casacore_cpp)]
pub(crate) struct CppHeldReaderHandle(std::ptr::NonNull<std::ffi::c_void>);

/// Open the table at `path` in C++ casacore with `UserLocking`, without
/// taking a lock, and keep it and its Int column "id" open.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_held_reader_open(path: &std::path::Path) -> Result<CppHeldReaderHandle, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut reader: *mut std::ffi::c_void = std::ptr::null_mut();
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe { cpp_table_held_reader_open(c_path.as_ptr(), &mut reader, &mut error) };
    unsafe {
        CasacoreOracleRuntime::cpp_status("table.held_reader_open", rc, error, cpp_table_free_error)
    }
    .map_err(|error| error.to_string())?;
    std::ptr::NonNull::new(reader)
        .map(CppHeldReaderHandle)
        .ok_or_else(|| "the C++ reader returned a null table".to_owned())
}

/// Take a read lock on the held table, read `id(0)` and release the lock,
/// keeping the table open.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_held_reader_read_id(reader: &mut CppHeldReaderHandle) -> Result<i32, String> {
    let mut id: i32 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe { cpp_table_held_reader_read_id(reader.0.as_ptr(), &mut id, &mut error) };
    unsafe {
        CasacoreOracleRuntime::cpp_status(
            "table.held_reader_read_id",
            rc,
            error,
            cpp_table_free_error,
        )
    }
    .map_err(|error| error.to_string())?;
    Ok(id)
}

/// Close the held table.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_held_reader_close(reader: CppHeldReaderHandle) {
    unsafe { cpp_table_held_reader_close(reader.0.as_ptr()) };
}

/// Times `nqueries` exact `ColumnsIndex` lookups for `key_value` on the `"id"`
/// column of the table at `path` using the C++ casacore implementation.
///
/// Returns `(elapsed_ns, match_count)` where `elapsed_ns` is the total wall
/// time for all queries and `match_count` is the number of rows returned by
/// the last lookup.
///
/// Use this alongside the Rust `ColumnsIndex` to compare performance.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_columns_index_time_lookups(
    path: &std::path::Path,
    key_value: i32,
    nqueries: u64,
) -> Result<(u64, u64), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut elapsed_ns: u64 = 0;
    let mut match_count: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        ffi_columns_index_time_lookups(
            c_path.as_ptr(),
            key_value,
            nqueries,
            &mut elapsed_ns,
            &mut match_count,
            &mut error,
        )
    };

    unsafe {
        CasacoreOracleRuntime::cpp_status(
            "table.columns_index_time_lookups",
            rc,
            error,
            cpp_table_free_error,
        )
    }
    .map_err(|error| error.to_string())?;
    Ok((elapsed_ns, match_count))
}

/// Stub for when C++ is unavailable.
/// Benchmark C++ variable-shape array write + read for `nrows` rows.
///
/// Returns `(write_ns, read_ns, total_elems)`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_vararray_bench(
    path: &std::path::Path,
    nrows: u64,
) -> Result<(u64, u64, u64), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut write_ns: u64 = 0;
    let mut read_ns: u64 = 0;
    let mut total_elems: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        cpp_vararray_bench_write_read(
            c_path.as_ptr(),
            nrows,
            &mut write_ns,
            &mut read_ns,
            &mut total_elems,
            &mut error,
        )
    };
    unsafe {
        CasacoreOracleRuntime::cpp_status("table.vararray_bench", rc, error, cpp_table_free_error)
    }
    .map_err(|error| error.to_string())?;
    Ok((write_ns, read_ns, total_elems))
}

/// Stub for when C++ is unavailable.
/// Result of the C++ set algebra benchmark.
pub struct SetAlgebraBenchResult {
    pub union_ns: u64,
    pub intersection_ns: u64,
    pub difference_ns: u64,
    pub union_rows: u64,
    pub intersection_rows: u64,
    pub difference_rows: u64,
}

/// Benchmark C++ `Table::operator|`, `operator&`, `operator-` on row-selected tables.
///
/// Creates a table with `nrows` rows, selects `[0..split_a)` and `[split_b..nrows)`,
/// and times union, intersection, and difference.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_set_algebra_bench(
    path: &std::path::Path,
    nrows: u64,
    split_a: u64,
    split_b: u64,
) -> Result<SetAlgebraBenchResult, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut union_ns: u64 = 0;
    let mut intersection_ns: u64 = 0;
    let mut difference_ns: u64 = 0;
    let mut union_rows: u64 = 0;
    let mut intersection_rows: u64 = 0;
    let mut difference_rows: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        ffi_set_algebra_bench(
            c_path.as_ptr(),
            nrows,
            split_a,
            split_b,
            &mut union_ns,
            &mut intersection_ns,
            &mut difference_ns,
            &mut union_rows,
            &mut intersection_rows,
            &mut difference_rows,
            &mut error,
        )
    };
    unsafe {
        CasacoreOracleRuntime::cpp_status(
            "table.set_algebra_bench",
            rc,
            error,
            cpp_table_free_error,
        )
    }
    .map_err(|error| error.to_string())?;
    Ok(SetAlgebraBenchResult {
        union_ns,
        intersection_ns,
        difference_ns,
        union_rows,
        intersection_rows,
        difference_rows,
    })
}

/// Stub for when C++ is unavailable.
/// Benchmark C++ `TableCopy::copyRows` on a table with `nrows` rows.
///
/// Returns elapsed nanoseconds.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_copy_rows_bench(dir: &std::path::Path, nrows: u64) -> Result<u64, String> {
    let c_dir =
        CasacoreOracleRuntime::c_path("table directory", dir).map_err(|error| error.to_string())?;
    let mut ns: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe { ffi_copy_rows_bench(c_dir.as_ptr(), nrows, &mut ns, &mut error) };
    unsafe {
        CasacoreOracleRuntime::cpp_status("table.copy_rows_bench", rc, error, cpp_table_free_error)
    }
    .map_err(|error| error.to_string())?;
    Ok(ns)
}

/// Stub for when C++ is unavailable.
/// Result of the C++ cell slice benchmark.
pub struct CellSliceBenchResult {
    pub write_ns: u64,
    pub slice_ns: u64,
}

/// Parameters for the cell slice benchmark.
pub struct CellSliceBenchParams {
    pub nrows: u64,
    pub dim0: i64,
    pub dim1: i64,
    pub slice_start0: i64,
    pub slice_start1: i64,
    pub slice_end0: i64,
    pub slice_end1: i64,
}

/// Benchmark C++ `ArrayColumn::getSlice` on `nrows` cells of shape `[dim0, dim1]`.
///
/// Slice region is `[slice_start0..slice_end0, slice_start1..slice_end1]` (end exclusive).
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_cell_slice_bench(
    path: &std::path::Path,
    params: &CellSliceBenchParams,
) -> Result<CellSliceBenchResult, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut write_ns: u64 = 0;
    let mut slice_ns: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        ffi_cell_slice_bench(
            c_path.as_ptr(),
            params.nrows,
            params.dim0,
            params.dim1,
            params.slice_start0,
            params.slice_start1,
            params.slice_end0,
            params.slice_end1,
            &mut write_ns,
            &mut slice_ns,
            &mut error,
        )
    };
    unsafe {
        CasacoreOracleRuntime::cpp_status("table.cell_slice_bench", rc, error, cpp_table_free_error)
    }
    .map_err(|error| error.to_string())?;
    Ok(CellSliceBenchResult { write_ns, slice_ns })
}

/// Stub for when C++ is unavailable.
/// Result from `cpp_bulk_scalar_io_bench`.
pub struct BulkScalarIoBenchResult {
    pub write_ns: u64,
    pub read_ns: u64,
}

/// Result from `cpp_deep_copy_bench`.
pub struct DeepCopyBenchResult {
    pub write_ns: u64,
    pub copy_ns: u64,
}

/// C++ casacore's account of a StandardStMan file
/// ([`crate::TableOracle::ssm_statistics`]), as `SSMBase` keeps it in the
/// file's header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsmStatistics {
    /// Buckets in the file: data, string and index buckets, free ones too.
    pub buckets: i64,
    /// Buckets on the free list, for casacore to reuse.
    pub free_buckets: i64,
    /// The head of the free list, or -1.
    pub first_free_bucket: i64,
    /// The string bucket casacore appends the next string to, or -1.
    pub last_string_bucket: i64,
    /// Buckets holding the index.
    pub index_buckets: i64,
    /// The first of them.
    pub first_index_bucket: i64,
    /// Where the index starts in its one bucket when it fits half of one,
    /// or 0 when it spans whole buckets.
    pub index_bucket_offset: i64,
}

#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_bulk_scalar_io_bench(
    path: &std::path::Path,
    nrows: u64,
) -> Result<BulkScalarIoBenchResult, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("table path", path).map_err(|error| error.to_string())?;
    let mut write_ns: u64 = 0;
    let mut read_ns: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        ffi_bulk_scalar_io_bench(
            c_path.as_ptr(),
            nrows,
            &mut write_ns,
            &mut read_ns,
            &mut error,
        )
    };
    unsafe {
        CasacoreOracleRuntime::cpp_status(
            "table.bulk_scalar_io_bench",
            rc,
            error,
            cpp_table_free_error,
        )
    }
    .map_err(|error| error.to_string())?;
    Ok(BulkScalarIoBenchResult { write_ns, read_ns })
}

#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_deep_copy_bench(
    dir: &std::path::Path,
    nrows: u64,
) -> Result<DeepCopyBenchResult, String> {
    let c_dir =
        CasacoreOracleRuntime::c_path("table directory", dir).map_err(|error| error.to_string())?;
    let mut write_ns: u64 = 0;
    let mut copy_ns: u64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        ffi_deep_copy_bench(
            c_dir.as_ptr(),
            nrows,
            &mut write_ns,
            &mut copy_ns,
            &mut error,
        )
    };
    unsafe {
        CasacoreOracleRuntime::cpp_status("table.deep_copy_bench", rc, error, cpp_table_free_error)
    }
    .map_err(|error| error.to_string())?;
    Ok(DeepCopyBenchResult { write_ns, copy_ns })
}
