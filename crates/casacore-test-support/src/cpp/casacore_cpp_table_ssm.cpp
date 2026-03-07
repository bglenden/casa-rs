// StandardStMan fixtures: ssm_scalar_primitives, ssm_fixed_array, ssm_keywords.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/StandardStMan.h>

using casacore_shim::make_error;

namespace {

void write_ssm_scalar_primitives_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Bool>("col_bool"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("col_i32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("col_f64"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("col_str"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);

    casacore::ScalarColumn<casacore::Bool> colBool(table, "col_bool");
    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    colBool.put(0, casacore::True); colI32.put(0, 42); colF64.put(0, 1.5); colStr.put(0, "hello");
    colBool.put(1, casacore::False); colI32.put(1, -7); colF64.put(1, -99.5); colStr.put(1, "world");
    colBool.put(2, casacore::True); colI32.put(2, 0); colF64.put(2, 0.0); colStr.put(2, "");

    table.rwKeywordSet().define("observer", casacore::String("test-harness"));
    table.flush();
}

void verify_ssm_scalar_primitives_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Bool> colBool(table, "col_bool");
    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    if (colBool(0) != casacore::True) throw std::runtime_error("row 0 col_bool mismatch");
    if (colI32(0) != 42) throw std::runtime_error("row 0 col_i32 mismatch");
    if (colF64(0) != 1.5) throw std::runtime_error("row 0 col_f64 mismatch");
    if (colStr(0) != "hello") throw std::runtime_error("row 0 col_str mismatch");
    if (colBool(1) != casacore::False) throw std::runtime_error("row 1 col_bool mismatch");
    if (colI32(1) != -7) throw std::runtime_error("row 1 col_i32 mismatch");
    if (colF64(1) != -99.5) throw std::runtime_error("row 1 col_f64 mismatch");
    if (colStr(1) != "world") throw std::runtime_error("row 1 col_str mismatch");
    if (colBool(2) != casacore::True) throw std::runtime_error("row 2 col_bool mismatch");
    if (colI32(2) != 0) throw std::runtime_error("row 2 col_i32 mismatch");
    if (colF64(2) != 0.0) throw std::runtime_error("row 2 col_f64 mismatch");
    if (colStr(2) != "") throw std::runtime_error("row 2 col_str mismatch");

    const casacore::TableRecord& kw = table.keywordSet();
    if (!kw.isDefined("observer")) throw std::runtime_error("keyword 'observer' missing");
    if (kw.asString("observer") != "test-harness")
        throw std::runtime_error("keyword 'observer' mismatch");
}

void write_ssm_fixed_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    casacore::IPosition shape(2, 2, 3);
    td.addColumn(
        casacore::ArrayColumnDesc<casacore::Float>("data", shape,
                                                    casacore::ColumnDesc::Direct |
                                                    casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);
    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    {
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 1.0f; arr(casacore::IPosition(2, 1, 0)) = 2.0f;
        arr(casacore::IPosition(2, 0, 1)) = 3.0f; arr(casacore::IPosition(2, 1, 1)) = 4.0f;
        arr(casacore::IPosition(2, 0, 2)) = 5.0f; arr(casacore::IPosition(2, 1, 2)) = 6.0f;
        colData.put(0, arr);
    }
    {
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 7.0f; arr(casacore::IPosition(2, 1, 0)) = 8.0f;
        arr(casacore::IPosition(2, 0, 1)) = 9.0f; arr(casacore::IPosition(2, 1, 1)) = 10.0f;
        arr(casacore::IPosition(2, 0, 2)) = 11.0f; arr(casacore::IPosition(2, 1, 2)) = 12.0f;
        colData.put(1, arr);
    }
    {
        casacore::Array<casacore::Float> arr(shape, 0.0f);
        colData.put(2, arr);
    }
    table.flush();
}

void verify_ssm_fixed_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3) throw std::runtime_error("expected 3 rows");

    casacore::ArrayColumn<casacore::Float> colData(table, "data");
    casacore::IPosition expectedShape(2, 2, 3);

    {
        casacore::Array<casacore::Float> arr = colData(0);
        if (!arr.shape().isEqual(expectedShape)) throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 1.0f) throw std::runtime_error("row 0 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 4.0f) throw std::runtime_error("row 0 [1,1] mismatch");
    }
    {
        casacore::Array<casacore::Float> arr = colData(2);
        if (arr(casacore::IPosition(2, 0, 0)) != 0.0f) throw std::runtime_error("row 2 [0,0] mismatch");
    }
}

void write_ssm_keywords_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 2);
    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    colId.put(0, 1); colId.put(1, 2);

    table.rwKeywordSet().define("telescope", casacore::String("ALMA"));
    table.rwKeywordSet().define("version", casacore::Int(3));
    table.flush();
}

void verify_ssm_keywords_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 2) throw std::runtime_error("expected 2 rows");

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    if (colId(0) != 1) throw std::runtime_error("row 0 id mismatch");
    if (colId(1) != 2) throw std::runtime_error("row 1 id mismatch");

    const casacore::TableRecord& kw = table.keywordSet();
    if (kw.asString("telescope") != "ALMA") throw std::runtime_error("keyword 'telescope' mismatch");
    if (kw.asInt("version") != 3) throw std::runtime_error("keyword 'version' mismatch");
}

// ===== Unsigned integer arrays: 3 rows × 3 cols (uChar[4], uShort[4], uInt[4]) =====

void write_ssm_unsigned_arrays_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::uChar>("arr_u8",
        casacore::IPosition(1, 4), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
    td.addColumn(casacore::ArrayColumnDesc<casacore::uShort>("arr_u16",
        casacore::IPosition(1, 4), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
    td.addColumn(casacore::ArrayColumnDesc<casacore::uInt>("arr_u32",
        casacore::IPosition(1, 4), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);
    casacore::Table table(setup, 3);

    casacore::ArrayColumn<casacore::uChar> colU8(table, "arr_u8");
    casacore::ArrayColumn<casacore::uShort> colU16(table, "arr_u16");
    casacore::ArrayColumn<casacore::uInt> colU32(table, "arr_u32");

    // Row 0
    {
        casacore::Vector<casacore::uChar> v(4); v(0)=255; v(1)=128; v(2)=0; v(3)=1;
        colU8.put(0, v);
        casacore::Vector<casacore::uShort> vs(4); vs(0)=65535; vs(1)=32768; vs(2)=0; vs(3)=1;
        colU16.put(0, vs);
        casacore::Vector<casacore::uInt> vi(4); vi(0)=UINT_MAX; vi(1)=100000; vi(2)=0; vi(3)=1;
        colU32.put(0, vi);
    }
    // Row 1: all zeros
    {
        casacore::Vector<casacore::uChar> v(4, 0); colU8.put(1, v);
        casacore::Vector<casacore::uShort> vs(4, 0); colU16.put(1, vs);
        casacore::Vector<casacore::uInt> vi(4, 0u); colU32.put(1, vi);
    }
    // Row 2
    {
        casacore::Vector<casacore::uChar> v(4); v(0)=1; v(1)=2; v(2)=3; v(3)=4;
        colU8.put(2, v);
        casacore::Vector<casacore::uShort> vs(4); vs(0)=100; vs(1)=200; vs(2)=300; vs(3)=400;
        colU16.put(2, vs);
        casacore::Vector<casacore::uInt> vi(4); vi(0)=1000; vi(1)=2000; vi(2)=3000; vi(3)=4000;
        colU32.put(2, vi);
    }
    table.flush();
}

void verify_ssm_unsigned_arrays_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3) throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

    casacore::ArrayColumn<casacore::uChar> colU8(table, "arr_u8");
    casacore::ArrayColumn<casacore::uShort> colU16(table, "arr_u16");
    casacore::ArrayColumn<casacore::uInt> colU32(table, "arr_u32");

    // Row 0
    { casacore::Vector<casacore::uChar> v(colU8(0)); if (v(0)!=255||v(1)!=128||v(2)!=0||v(3)!=1) throw std::runtime_error("row 0 arr_u8 mismatch"); }
    { casacore::Vector<casacore::uShort> v(colU16(0)); if (v(0)!=65535||v(1)!=32768||v(2)!=0||v(3)!=1) throw std::runtime_error("row 0 arr_u16 mismatch"); }
    { casacore::Vector<casacore::uInt> v(colU32(0)); if (v(0)!=UINT_MAX||v(1)!=100000||v(2)!=0||v(3)!=1) throw std::runtime_error("row 0 arr_u32 mismatch"); }

    // Row 1: all zeros
    { casacore::Vector<casacore::uChar> v(colU8(1)); if (v(0)!=0||v(1)!=0||v(2)!=0||v(3)!=0) throw std::runtime_error("row 1 arr_u8 mismatch"); }
    { casacore::Vector<casacore::uShort> v(colU16(1)); if (v(0)!=0||v(1)!=0||v(2)!=0||v(3)!=0) throw std::runtime_error("row 1 arr_u16 mismatch"); }
    { casacore::Vector<casacore::uInt> v(colU32(1)); if (v(0)!=0||v(1)!=0||v(2)!=0||v(3)!=0) throw std::runtime_error("row 1 arr_u32 mismatch"); }

    // Row 2
    { casacore::Vector<casacore::uChar> v(colU8(2)); if (v(0)!=1||v(1)!=2||v(2)!=3||v(3)!=4) throw std::runtime_error("row 2 arr_u8 mismatch"); }
    { casacore::Vector<casacore::uShort> v(colU16(2)); if (v(0)!=100||v(1)!=200||v(2)!=300||v(3)!=400) throw std::runtime_error("row 2 arr_u16 mismatch"); }
    { casacore::Vector<casacore::uInt> v(colU32(2)); if (v(0)!=1000||v(1)!=2000||v(2)!=3000||v(3)!=4000) throw std::runtime_error("row 2 arr_u32 mismatch"); }
}

// ===== String arrays: 3 rows × 1 col (String[3]) =====

void write_ssm_string_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::String>("arr_str",
        casacore::IPosition(1, 3), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);
    casacore::Table table(setup, 3);

    casacore::ArrayColumn<casacore::String> col(table, "arr_str");

    { casacore::Vector<casacore::String> v(3); v(0)="alpha"; v(1)="beta"; v(2)="gamma"; col.put(0, v); }
    { casacore::Vector<casacore::String> v(3); v(0)=""; v(1)=""; v(2)=""; col.put(1, v); }
    { casacore::Vector<casacore::String> v(3); v(0)="hello world"; v(1)="café"; v(2)="line\nnewline"; col.put(2, v); }

    table.flush();
}

void verify_ssm_string_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3) throw std::runtime_error("expected 3 rows");

    casacore::ArrayColumn<casacore::String> col(table, "arr_str");

    { casacore::Vector<casacore::String> v(col(0)); if (v(0)!="alpha"||v(1)!="beta"||v(2)!="gamma") throw std::runtime_error("row 0 mismatch"); }
    { casacore::Vector<casacore::String> v(col(1)); if (v(0)!=""||v(1)!=""||v(2)!="") throw std::runtime_error("row 1 mismatch"); }
    { casacore::Vector<casacore::String> v(col(2)); if (v(0)!="hello world"||v(1)!="café"||v(2)!="line\nnewline") throw std::runtime_error("row 2 mismatch"); }
}

// ===== Complex64 2D arrays: 3 rows × 1 col (DComplex[2,2]) =====

void write_ssm_complex64_2d_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    casacore::IPosition shape(2, 2, 2);
    td.addColumn(casacore::ArrayColumnDesc<casacore::DComplex>("arr_c64", shape,
        casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);
    casacore::Table table(setup, 3);

    casacore::ArrayColumn<casacore::DComplex> col(table, "arr_c64");

    // Row 0
    {
        casacore::Array<casacore::DComplex> a(shape);
        a(casacore::IPosition(2,0,0)) = casacore::DComplex(1.0, 2.0);
        a(casacore::IPosition(2,1,0)) = casacore::DComplex(3.0, 4.0);
        a(casacore::IPosition(2,0,1)) = casacore::DComplex(5.0, 6.0);
        a(casacore::IPosition(2,1,1)) = casacore::DComplex(7.0, 8.0);
        col.put(0, a);
    }
    // Row 1: all zeros
    {
        casacore::Array<casacore::DComplex> a(shape, casacore::DComplex(0.0, 0.0));
        col.put(1, a);
    }
    // Row 2
    {
        casacore::Array<casacore::DComplex> a(shape);
        a(casacore::IPosition(2,0,0)) = casacore::DComplex(-1.0, 0.5);
        a(casacore::IPosition(2,1,0)) = casacore::DComplex(1e10, -1e10);
        a(casacore::IPosition(2,0,1)) = casacore::DComplex(0.0, 0.0);
        a(casacore::IPosition(2,1,1)) = casacore::DComplex(-0.25, 0.75);
        col.put(2, a);
    }
    table.flush();
}

void verify_ssm_complex64_2d_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3) throw std::runtime_error("expected 3 rows");

    casacore::ArrayColumn<casacore::DComplex> col(table, "arr_c64");
    casacore::IPosition shape(2, 2, 2);

    // Row 0
    {
        casacore::Array<casacore::DComplex> a = col(0);
        if (!a.shape().isEqual(shape)) throw std::runtime_error("row 0 shape mismatch");
        if (a(casacore::IPosition(2,0,0)) != casacore::DComplex(1.0, 2.0)) throw std::runtime_error("row 0 [0,0] mismatch");
        if (a(casacore::IPosition(2,1,1)) != casacore::DComplex(7.0, 8.0)) throw std::runtime_error("row 0 [1,1] mismatch");
    }
    // Row 1: zeros
    {
        casacore::Array<casacore::DComplex> a = col(1);
        if (a(casacore::IPosition(2,0,0)) != casacore::DComplex(0.0, 0.0)) throw std::runtime_error("row 1 [0,0] mismatch");
        if (a(casacore::IPosition(2,1,1)) != casacore::DComplex(0.0, 0.0)) throw std::runtime_error("row 1 [1,1] mismatch");
    }
    // Row 2
    {
        casacore::Array<casacore::DComplex> a = col(2);
        if (a(casacore::IPosition(2,0,0)) != casacore::DComplex(-1.0, 0.5)) throw std::runtime_error("row 2 [0,0] mismatch");
        if (a(casacore::IPosition(2,1,0)) != casacore::DComplex(1e10, -1e10)) throw std::runtime_error("row 2 [1,0] mismatch");
        if (a(casacore::IPosition(2,1,1)) != casacore::DComplex(-0.25, 0.75)) throw std::runtime_error("row 2 [1,1] mismatch");
    }
}

// ===== Empty table: 0 rows, Int32 scalar + Float32[4] array =====

void write_ssm_empty_table_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("col_i32"));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("arr_f32",
        casacore::IPosition(1, 4), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);
    casacore::Table table(setup, 0);  // zero rows
    table.flush();
}

void verify_ssm_empty_table_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 0) throw std::runtime_error("expected 0 rows, got " + std::to_string(table.nrow()));

    // Verify schema exists
    if (!table.tableDesc().isColumn("col_i32")) throw std::runtime_error("col_i32 missing");
    if (!table.tableDesc().isColumn("arr_f32")) throw std::runtime_error("arr_f32 missing");
}

} // anonymous namespace

extern "C" {

int32_t cpp_table_write_ssm_scalar_primitives(const char* path, char** out_error) {
    try { write_ssm_scalar_primitives_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_scalar_primitives(const char* path, char** out_error) {
    try { verify_ssm_scalar_primitives_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_ssm_fixed_array(const char* path, char** out_error) {
    try { write_ssm_fixed_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_fixed_array(const char* path, char** out_error) {
    try { verify_ssm_fixed_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_ssm_keywords(const char* path, char** out_error) {
    try { write_ssm_keywords_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_keywords(const char* path, char** out_error) {
    try { verify_ssm_keywords_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

// ===== AllNumericScalars: 3 rows × 6 cols (uChar, Short, uShort, uInt, Float, Int64) =====

void write_ssm_all_numeric_scalars_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::uChar>("col_u8"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Short>("col_i16"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::uShort>("col_u16"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::uInt>("col_u32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Float>("col_f32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int64>("col_i64"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);

    casacore::ScalarColumn<casacore::uChar> colU8(table, "col_u8");
    casacore::ScalarColumn<casacore::Short> colI16(table, "col_i16");
    casacore::ScalarColumn<casacore::uShort> colU16(table, "col_u16");
    casacore::ScalarColumn<casacore::uInt> colU32(table, "col_u32");
    casacore::ScalarColumn<casacore::Float> colF32(table, "col_f32");
    casacore::ScalarColumn<casacore::Int64> colI64(table, "col_i64");

    colU8.put(0, 255); colI16.put(0, -1234); colU16.put(0, 65535);
    colU32.put(0, 100000); colF32.put(0, 2.75f); colI64.put(0, INT64_MAX);

    colU8.put(1, 0); colI16.put(1, 0); colU16.put(1, 0);
    colU32.put(1, 0); colF32.put(1, 0.0f); colI64.put(1, 0);

    colU8.put(2, 128); colI16.put(2, SHRT_MIN); colU16.put(2, 32768);
    colU32.put(2, UINT_MAX); colF32.put(2, -1e10f); colI64.put(2, INT64_MIN);

    table.flush();
}

void verify_ssm_all_numeric_scalars_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::uChar> colU8(table, "col_u8");
    casacore::ScalarColumn<casacore::Short> colI16(table, "col_i16");
    casacore::ScalarColumn<casacore::uShort> colU16(table, "col_u16");
    casacore::ScalarColumn<casacore::uInt> colU32(table, "col_u32");
    casacore::ScalarColumn<casacore::Float> colF32(table, "col_f32");
    casacore::ScalarColumn<casacore::Int64> colI64(table, "col_i64");

    if (colU8(0) != 255) throw std::runtime_error("row 0 col_u8 mismatch");
    if (colI16(0) != -1234) throw std::runtime_error("row 0 col_i16 mismatch");
    if (colU16(0) != 65535) throw std::runtime_error("row 0 col_u16 mismatch");
    if (colU32(0) != 100000u) throw std::runtime_error("row 0 col_u32 mismatch");
    if (colF32(0) != 2.75f) throw std::runtime_error("row 0 col_f32 mismatch");
    if (colI64(0) != INT64_MAX) throw std::runtime_error("row 0 col_i64 mismatch");

    if (colU8(1) != 0) throw std::runtime_error("row 1 col_u8 mismatch");
    if (colI16(1) != 0) throw std::runtime_error("row 1 col_i16 mismatch");
    if (colU16(1) != 0) throw std::runtime_error("row 1 col_u16 mismatch");
    if (colU32(1) != 0u) throw std::runtime_error("row 1 col_u32 mismatch");
    if (colF32(1) != 0.0f) throw std::runtime_error("row 1 col_f32 mismatch");
    if (colI64(1) != 0) throw std::runtime_error("row 1 col_i64 mismatch");

    if (colU8(2) != 128) throw std::runtime_error("row 2 col_u8 mismatch");
    if (colI16(2) != SHRT_MIN) throw std::runtime_error("row 2 col_i16 mismatch");
    if (colU16(2) != 32768) throw std::runtime_error("row 2 col_u16 mismatch");
    if (colU32(2) != UINT_MAX) throw std::runtime_error("row 2 col_u32 mismatch");
    if (colF32(2) != -1e10f) throw std::runtime_error("row 2 col_f32 mismatch");
    if (colI64(2) != INT64_MIN) throw std::runtime_error("row 2 col_i64 mismatch");
}

// ===== ComplexScalars: 3 rows × 2 cols (Complex, DComplex) =====

void write_ssm_complex_scalars_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Complex>("col_c32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::DComplex>("col_c64"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);

    casacore::ScalarColumn<casacore::Complex> colC32(table, "col_c32");
    casacore::ScalarColumn<casacore::DComplex> colC64(table, "col_c64");

    colC32.put(0, casacore::Complex(1.0f, 2.0f));
    colC64.put(0, casacore::DComplex(3.0, 4.0));

    colC32.put(1, casacore::Complex(0.0f, 0.0f));
    colC64.put(1, casacore::DComplex(0.0, 0.0));

    colC32.put(2, casacore::Complex(-5.5f, 7.25f));
    colC64.put(2, casacore::DComplex(-1e10, 1e-10));

    table.flush();
}

void verify_ssm_complex_scalars_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Complex> colC32(table, "col_c32");
    casacore::ScalarColumn<casacore::DComplex> colC64(table, "col_c64");

    if (colC32(0) != casacore::Complex(1.0f, 2.0f)) throw std::runtime_error("row 0 col_c32 mismatch");
    if (colC64(0) != casacore::DComplex(3.0, 4.0)) throw std::runtime_error("row 0 col_c64 mismatch");

    if (colC32(1) != casacore::Complex(0.0f, 0.0f)) throw std::runtime_error("row 1 col_c32 mismatch");
    if (colC64(1) != casacore::DComplex(0.0, 0.0)) throw std::runtime_error("row 1 col_c64 mismatch");

    if (colC32(2) != casacore::Complex(-5.5f, 7.25f)) throw std::runtime_error("row 2 col_c32 mismatch");
    if (colC64(2) != casacore::DComplex(-1e10, 1e-10)) throw std::runtime_error("row 2 col_c64 mismatch");
}

// ===== TypedArrays: 3 rows × 3 cols (Int[4], Double[2,2], Complex32[2]) =====

void write_ssm_typed_arrays_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Int>("arr_i32",
        casacore::IPosition(1, 4), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Double>("arr_f64",
        casacore::IPosition(2, 2, 2), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Complex>("arr_c32",
        casacore::IPosition(1, 2), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);

    casacore::ArrayColumn<casacore::Int> colI32(table, "arr_i32");
    casacore::ArrayColumn<casacore::Double> colF64(table, "arr_f64");
    casacore::ArrayColumn<casacore::Complex> colC32(table, "arr_c32");

    // Row 0
    {
        casacore::Vector<casacore::Int> v(4);
        v(0)=10; v(1)=20; v(2)=30; v(3)=40;
        colI32.put(0, v);

        casacore::Array<casacore::Double> a(casacore::IPosition(2, 2, 2));
        a(casacore::IPosition(2, 0, 0))=1.1; a(casacore::IPosition(2, 1, 0))=2.2;
        a(casacore::IPosition(2, 0, 1))=3.3; a(casacore::IPosition(2, 1, 1))=4.4;
        colF64.put(0, a);

        casacore::Vector<casacore::Complex> c(2);
        c(0) = casacore::Complex(1.0f, 2.0f);
        c(1) = casacore::Complex(3.0f, 4.0f);
        colC32.put(0, c);
    }
    // Row 1
    {
        casacore::Vector<casacore::Int> v(4);
        v(0)=-1; v(1)=-2; v(2)=-3; v(3)=-4;
        colI32.put(1, v);

        casacore::Array<casacore::Double> a(casacore::IPosition(2, 2, 2));
        a(casacore::IPosition(2, 0, 0))=5.5; a(casacore::IPosition(2, 1, 0))=6.6;
        a(casacore::IPosition(2, 0, 1))=7.7; a(casacore::IPosition(2, 1, 1))=8.8;
        colF64.put(1, a);

        casacore::Vector<casacore::Complex> c(2);
        c(0) = casacore::Complex(-1.0f, -2.0f);
        c(1) = casacore::Complex(0.0f, 0.0f);
        colC32.put(1, c);
    }
    // Row 2
    {
        casacore::Vector<casacore::Int> v(4, 0);
        colI32.put(2, v);

        casacore::Array<casacore::Double> a(casacore::IPosition(2, 2, 2), 0.0);
        colF64.put(2, a);

        casacore::Vector<casacore::Complex> c(2, casacore::Complex(0.0f, 0.0f));
        colC32.put(2, c);
    }

    table.flush();
}

void verify_ssm_typed_arrays_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

    casacore::ArrayColumn<casacore::Int> colI32(table, "arr_i32");
    casacore::ArrayColumn<casacore::Double> colF64(table, "arr_f64");
    casacore::ArrayColumn<casacore::Complex> colC32(table, "arr_c32");

    // Row 0
    {
        casacore::Vector<casacore::Int> v(colI32(0));
        if (v(0) != 10 || v(1) != 20 || v(2) != 30 || v(3) != 40)
            throw std::runtime_error("row 0 arr_i32 mismatch");

        casacore::Array<casacore::Double> a = colF64(0);
        if (a(casacore::IPosition(2,0,0)) != 1.1 || a(casacore::IPosition(2,1,0)) != 2.2 ||
            a(casacore::IPosition(2,0,1)) != 3.3 || a(casacore::IPosition(2,1,1)) != 4.4)
            throw std::runtime_error("row 0 arr_f64 mismatch");

        casacore::Vector<casacore::Complex> c(colC32(0));
        if (c(0) != casacore::Complex(1.0f, 2.0f) || c(1) != casacore::Complex(3.0f, 4.0f))
            throw std::runtime_error("row 0 arr_c32 mismatch");
    }
    // Row 1
    {
        casacore::Vector<casacore::Int> v(colI32(1));
        if (v(0) != -1 || v(1) != -2 || v(2) != -3 || v(3) != -4)
            throw std::runtime_error("row 1 arr_i32 mismatch");

        casacore::Array<casacore::Double> a = colF64(1);
        if (a(casacore::IPosition(2,0,0)) != 5.5 || a(casacore::IPosition(2,1,0)) != 6.6 ||
            a(casacore::IPosition(2,0,1)) != 7.7 || a(casacore::IPosition(2,1,1)) != 8.8)
            throw std::runtime_error("row 1 arr_f64 mismatch");

        casacore::Vector<casacore::Complex> c(colC32(1));
        if (c(0) != casacore::Complex(-1.0f, -2.0f) || c(1) != casacore::Complex(0.0f, 0.0f))
            throw std::runtime_error("row 1 arr_c32 mismatch");
    }
    // Row 2
    {
        casacore::Vector<casacore::Int> v(colI32(2));
        if (v(0) != 0 || v(1) != 0 || v(2) != 0 || v(3) != 0)
            throw std::runtime_error("row 2 arr_i32 mismatch");

        casacore::Array<casacore::Double> a = colF64(2);
        if (a(casacore::IPosition(2,0,0)) != 0.0 || a(casacore::IPosition(2,1,1)) != 0.0)
            throw std::runtime_error("row 2 arr_f64 mismatch");

        casacore::Vector<casacore::Complex> c(colC32(2));
        if (c(0) != casacore::Complex(0.0f, 0.0f) || c(1) != casacore::Complex(0.0f, 0.0f))
            throw std::runtime_error("row 2 arr_c32 mismatch");
    }
}

int32_t cpp_table_write_ssm_all_numeric_scalars(const char* path, char** out_error) {
    try { write_ssm_all_numeric_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_all_numeric_scalars(const char* path, char** out_error) {
    try { verify_ssm_all_numeric_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_ssm_complex_scalars(const char* path, char** out_error) {
    try { write_ssm_complex_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_complex_scalars(const char* path, char** out_error) {
    try { verify_ssm_complex_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_ssm_typed_arrays(const char* path, char** out_error) {
    try { write_ssm_typed_arrays_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_typed_arrays(const char* path, char** out_error) {
    try { verify_ssm_typed_arrays_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

int32_t cpp_table_write_ssm_unsigned_arrays(const char* path, char** out_error) {
    try { write_ssm_unsigned_arrays_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_unsigned_arrays(const char* path, char** out_error) {
    try { verify_ssm_unsigned_arrays_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_ssm_string_array(const char* path, char** out_error) {
    try { write_ssm_string_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_string_array(const char* path, char** out_error) {
    try { verify_ssm_string_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_ssm_complex64_2d_array(const char* path, char** out_error) {
    try { write_ssm_complex64_2d_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_complex64_2d_array(const char* path, char** out_error) {
    try { verify_ssm_complex64_2d_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_ssm_empty_table(const char* path, char** out_error) {
    try { write_ssm_empty_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_empty_table(const char* path, char** out_error) {
    try { verify_ssm_empty_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

} // extern "C"
