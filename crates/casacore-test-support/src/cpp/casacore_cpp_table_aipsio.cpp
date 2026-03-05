// AipsIO storage manager fixtures: scalar_primitives, fixed_array, keywords, records.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <casacore/tables/Tables/ScaRecordColDesc.h>
#include <casacore/tables/Tables/ScaRecordColData.h>

using casacore_shim::make_error;
static casacore_shim::TerminateGuard g_terminate_guard_aipsio;

namespace {

void write_scalar_primitives_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Bool>("col_bool"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("col_i32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("col_f64"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("col_str"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);

    casacore::ScalarColumn<casacore::Bool> colBool(table, "col_bool");
    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    colBool.put(0, casacore::True);
    colI32.put(0, 42);
    colF64.put(0, 1.5);
    colStr.put(0, "hello");

    colBool.put(1, casacore::False);
    colI32.put(1, -7);
    colF64.put(1, -99.5);
    colStr.put(1, "world");

    colBool.put(2, casacore::True);
    colI32.put(2, 0);
    colF64.put(2, 0.0);
    colStr.put(2, "");

    table.rwKeywordSet().define("observer", casacore::String("test-harness"));
    table.flush();
}

void verify_scalar_primitives_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 3) {
        throw std::runtime_error("expected 3 rows, got " +
                                 std::to_string(table.nrow()));
    }

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

void write_fixed_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    casacore::IPosition shape(2, 2, 3);
    td.addColumn(
        casacore::ArrayColumnDesc<casacore::Float>("data", shape,
                                                    casacore::ColumnDesc::Direct |
                                                    casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);
    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    {
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 1.0f;
        arr(casacore::IPosition(2, 1, 0)) = 2.0f;
        arr(casacore::IPosition(2, 0, 1)) = 3.0f;
        arr(casacore::IPosition(2, 1, 1)) = 4.0f;
        arr(casacore::IPosition(2, 0, 2)) = 5.0f;
        arr(casacore::IPosition(2, 1, 2)) = 6.0f;
        colData.put(0, arr);
    }
    {
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 7.0f;
        arr(casacore::IPosition(2, 1, 0)) = 8.0f;
        arr(casacore::IPosition(2, 0, 1)) = 9.0f;
        arr(casacore::IPosition(2, 1, 1)) = 10.0f;
        arr(casacore::IPosition(2, 0, 2)) = 11.0f;
        arr(casacore::IPosition(2, 1, 2)) = 12.0f;
        colData.put(1, arr);
    }
    {
        casacore::Array<casacore::Float> arr(shape, 0.0f);
        colData.put(2, arr);
    }

    table.flush();
}

void verify_fixed_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 3) throw std::runtime_error("expected 3 rows");

    casacore::ArrayColumn<casacore::Float> colData(table, "data");
    casacore::IPosition expectedShape(2, 2, 3);

    {
        casacore::Array<casacore::Float> arr = colData(0);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 1.0f)
            throw std::runtime_error("row 0 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 4.0f)
            throw std::runtime_error("row 0 [1,1] mismatch");
    }
    {
        casacore::Array<casacore::Float> arr = colData(2);
        if (arr(casacore::IPosition(2, 0, 0)) != 0.0f)
            throw std::runtime_error("row 2 [0,0] mismatch");
    }
}

void write_keywords_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 2);
    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    colId.put(0, 1);
    colId.put(1, 2);

    table.rwKeywordSet().define("telescope", casacore::String("ALMA"));
    table.rwKeywordSet().define("version", casacore::Int(3));

    table.flush();
}

void verify_keywords_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 2) throw std::runtime_error("expected 2 rows");

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    if (colId(0) != 1) throw std::runtime_error("row 0 id mismatch");
    if (colId(1) != 2) throw std::runtime_error("row 1 id mismatch");

    const casacore::TableRecord& kw = table.keywordSet();
    if (kw.asString("telescope") != "ALMA")
        throw std::runtime_error("keyword 'telescope' mismatch");
    if (kw.asInt("version") != 3)
        throw std::runtime_error("keyword 'version' mismatch");
}

// ===== Undefined scalars: 4 rows, only rows 0 and 2 written =====
// In C++ StManAipsIO, unwritten scalar cells get default values (0, false, "").

void write_undefined_scalars_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("col_i32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("col_f64"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("col_str"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 4);
    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    // Only write rows 0 and 2; rows 1 and 3 keep defaults.
    colI32.put(0, 100);
    colF64.put(0, 1.5);
    colStr.put(0, "written");

    colI32.put(2, 200);
    colF64.put(2, 2.5);
    colStr.put(2, "also_written");

    table.flush();
}

void verify_undefined_scalars_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 4)
        throw std::runtime_error("expected 4 rows, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    // Row 0: written values
    if (colI32(0) != 100) throw std::runtime_error("row 0 col_i32 mismatch");
    if (colF64(0) != 1.5) throw std::runtime_error("row 0 col_f64 mismatch");
    if (colStr(0) != "written") throw std::runtime_error("row 0 col_str mismatch");

    // Row 1: default values (unwritten)
    if (colI32(1) != 0) throw std::runtime_error("row 1 col_i32 should be 0");
    if (colF64(1) != 0.0) throw std::runtime_error("row 1 col_f64 should be 0.0");
    if (colStr(1) != "") throw std::runtime_error("row 1 col_str should be empty");

    // Row 2: written values
    if (colI32(2) != 200) throw std::runtime_error("row 2 col_i32 mismatch");
    if (colF64(2) != 2.5) throw std::runtime_error("row 2 col_f64 mismatch");
    if (colStr(2) != "also_written") throw std::runtime_error("row 2 col_str mismatch");

    // Row 3: default values (unwritten)
    if (colI32(3) != 0) throw std::runtime_error("row 3 col_i32 should be 0");
    if (colF64(3) != 0.0) throw std::runtime_error("row 3 col_f64 should be 0.0");
    if (colStr(3) != "") throw std::runtime_error("row 3 col_str should be empty");
}

// ===== Column keywords: two columns with per-column keywords =====

void write_column_keywords_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("flux"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 2);
    casacore::ScalarColumn<casacore::Double> colFlux(table, "flux");
    casacore::ScalarColumn<casacore::Int> colId(table, "id");

    colFlux.put(0, 1.5);  colId.put(0, 1);
    colFlux.put(1, 2.7);  colId.put(1, 2);

    // Table keyword
    table.rwKeywordSet().define("telescope", casacore::String("VLA"));

    // Column keywords
    colFlux.rwKeywordSet().define("unit", casacore::String("Jy"));
    colFlux.rwKeywordSet().define("ref_frame", casacore::String("LSRK"));
    colId.rwKeywordSet().define("description", casacore::String("source identifier"));

    table.flush();
}

void verify_column_keywords_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 2)
        throw std::runtime_error("expected 2 rows, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Double> colFlux(table, "flux");
    casacore::ScalarColumn<casacore::Int> colId(table, "id");

    if (colFlux(0) != 1.5) throw std::runtime_error("row 0 flux mismatch");
    if (colId(0) != 1) throw std::runtime_error("row 0 id mismatch");
    if (colFlux(1) != 2.7) throw std::runtime_error("row 1 flux mismatch");
    if (colId(1) != 2) throw std::runtime_error("row 1 id mismatch");

    // Table keyword
    const casacore::TableRecord& kw = table.keywordSet();
    if (kw.asString("telescope") != "VLA")
        throw std::runtime_error("table keyword 'telescope' mismatch");

    // Column keywords
    const casacore::TableRecord& fluxKw = colFlux.keywordSet();
    if (fluxKw.asString("unit") != "Jy")
        throw std::runtime_error("flux keyword 'unit' mismatch");
    if (fluxKw.asString("ref_frame") != "LSRK")
        throw std::runtime_error("flux keyword 'ref_frame' mismatch");

    const casacore::TableRecord& idKw = colId.keywordSet();
    if (idKw.asString("description") != "source identifier")
        throw std::runtime_error("id keyword 'description' mismatch");
}

// Mixed schema: 2-row table with scalar, fixed array, variable array,
// record column, table keywords, and column keywords.
void write_mixed_schema_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("flux"));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("spectrum",
        casacore::IPosition(1, 4), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("vis", 2));
    td.addColumn(casacore::ScalarRecordColumnDesc("meta"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);
    casacore::Table table(setup, 2);

    // Scalar columns
    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    casacore::ScalarColumn<casacore::Double> colFlux(table, "flux");
    colId.put(0, 1);   colFlux.put(0, 1.5);
    colId.put(1, 2);   colFlux.put(1, 2.7);

    // Fixed array column [4]
    casacore::ArrayColumn<casacore::Float> colSpec(table, "spectrum");
    {
        casacore::Vector<casacore::Float> v0(4);
        v0(0)=10.0f; v0(1)=20.0f; v0(2)=30.0f; v0(3)=40.0f;
        colSpec.put(0, v0);
        casacore::Vector<casacore::Float> v1(4);
        v1(0)=50.0f; v1(1)=60.0f; v1(2)=70.0f; v1(3)=80.0f;
        colSpec.put(1, v1);
    }

    // Variable array column (2-D)
    casacore::ArrayColumn<casacore::Float> colVis(table, "vis");
    {
        // Row 0: shape [2,3], values 1..6
        casacore::IPosition sh0(2, 2, 3);
        colVis.setShape(0, sh0);
        casacore::Array<casacore::Float> a0(sh0);
        a0(casacore::IPosition(2,0,0))=1.0f; a0(casacore::IPosition(2,1,0))=2.0f;
        a0(casacore::IPosition(2,0,1))=3.0f; a0(casacore::IPosition(2,1,1))=4.0f;
        a0(casacore::IPosition(2,0,2))=5.0f; a0(casacore::IPosition(2,1,2))=6.0f;
        colVis.put(0, a0);

        // Row 1: shape [3,2], values 7..12
        casacore::IPosition sh1(2, 3, 2);
        colVis.setShape(1, sh1);
        casacore::Array<casacore::Float> a1(sh1);
        a1(casacore::IPosition(2,0,0))=7.0f;  a1(casacore::IPosition(2,1,0))=8.0f;
        a1(casacore::IPosition(2,2,0))=9.0f;  a1(casacore::IPosition(2,0,1))=10.0f;
        a1(casacore::IPosition(2,1,1))=11.0f; a1(casacore::IPosition(2,2,1))=12.0f;
        colVis.put(1, a1);
    }

    // Record column
    casacore::ScalarColumn<casacore::TableRecord> colMeta(table, "meta");
    {
        casacore::TableRecord r0;
        r0.define("source", casacore::String("CasA"));
        r0.define("priority", 1);
        colMeta.put(0, r0);

        casacore::TableRecord r1;
        r1.define("source", casacore::String("CygA"));
        colMeta.put(1, r1);
    }

    // Table keywords
    table.rwKeywordSet().define("telescope", casacore::String("ALMA"));
    table.rwKeywordSet().define("version", casacore::Int(3));

    // Column keywords
    casacore::TableColumn fluxCol(table, "flux");
    fluxCol.rwKeywordSet().define("unit", casacore::String("Jy"));

    table.flush();
}

void verify_mixed_schema_impl(const std::string& path) {
    casacore::Table table(path);
    if (table.nrow() != 2)
        throw std::runtime_error("expected 2 rows, got " + std::to_string(table.nrow()));

    // Verify scalars
    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    casacore::ScalarColumn<casacore::Double> colFlux(table, "flux");
    if (colId(0) != 1 || colId(1) != 2)
        throw std::runtime_error("id mismatch");
    if (colFlux(0) != 1.5 || colFlux(1) != 2.7)
        throw std::runtime_error("flux mismatch");

    // Verify fixed array
    casacore::ArrayColumn<casacore::Float> colSpec(table, "spectrum");
    {
        casacore::Array<casacore::Float> a0 = colSpec(0);
        if (a0.shape() != casacore::IPosition(1, 4))
            throw std::runtime_error("spectrum shape mismatch row 0");
        casacore::Vector<casacore::Float> v0(a0);
        if (v0(0) != 10.0f || v0(1) != 20.0f || v0(2) != 30.0f || v0(3) != 40.0f)
            throw std::runtime_error("spectrum values mismatch row 0");

        casacore::Array<casacore::Float> a1 = colSpec(1);
        casacore::Vector<casacore::Float> v1(a1);
        if (v1(0) != 50.0f || v1(1) != 60.0f || v1(2) != 70.0f || v1(3) != 80.0f)
            throw std::runtime_error("spectrum values mismatch row 1");
    }

    // Verify variable array
    casacore::ArrayColumn<casacore::Float> colVis(table, "vis");
    {
        casacore::Array<casacore::Float> a0 = colVis(0);
        if (a0.shape() != casacore::IPosition(2, 2, 3))
            throw std::runtime_error("vis shape mismatch row 0");
        if (a0(casacore::IPosition(2,0,0)) != 1.0f || a0(casacore::IPosition(2,1,2)) != 6.0f)
            throw std::runtime_error("vis values mismatch row 0");

        casacore::Array<casacore::Float> a1 = colVis(1);
        if (a1.shape() != casacore::IPosition(2, 3, 2))
            throw std::runtime_error("vis shape mismatch row 1");
        if (a1(casacore::IPosition(2,0,0)) != 7.0f || a1(casacore::IPosition(2,2,1)) != 12.0f)
            throw std::runtime_error("vis values mismatch row 1");
    }

    // Verify record column
    casacore::ScalarColumn<casacore::TableRecord> colMeta(table, "meta");
    {
        casacore::TableRecord r0 = colMeta(0);
        if (r0.asString("source") != "CasA")
            throw std::runtime_error("meta row 0 source mismatch");
        if (r0.asInt("priority") != 1)
            throw std::runtime_error("meta row 0 priority mismatch");

        casacore::TableRecord r1 = colMeta(1);
        if (r1.asString("source") != "CygA")
            throw std::runtime_error("meta row 1 source mismatch");
    }

    // Verify table keywords
    const casacore::TableRecord& kw = table.keywordSet();
    if (kw.asString("telescope") != "ALMA")
        throw std::runtime_error("table keyword 'telescope' mismatch");
    if (kw.asInt("version") != 3)
        throw std::runtime_error("table keyword 'version' mismatch");

    // Verify column keywords
    casacore::TableColumn fluxCol(table, "flux");
    const casacore::TableRecord& fkw = fluxCol.keywordSet();
    if (fkw.asString("unit") != "Jy")
        throw std::runtime_error("flux column keyword 'unit' mismatch");
}

} // anonymous namespace

extern "C" {

int32_t cpp_table_write_scalar_primitives(const char* path, char** out_error) {
    try { write_scalar_primitives_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_scalar_primitives"); return -1; }
}

int32_t cpp_table_verify_scalar_primitives(const char* path, char** out_error) {
    try { verify_scalar_primitives_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_scalar_primitives"); return -1; }
}

int32_t cpp_table_write_fixed_array(const char* path, char** out_error) {
    try { write_fixed_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_fixed_array"); return -1; }
}

int32_t cpp_table_verify_fixed_array(const char* path, char** out_error) {
    try { verify_fixed_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_fixed_array"); return -1; }
}

int32_t cpp_table_write_keywords(const char* path, char** out_error) {
    try { write_keywords_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_keywords"); return -1; }
}

int32_t cpp_table_verify_keywords(const char* path, char** out_error) {
    try { verify_keywords_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_keywords"); return -1; }
}

int32_t cpp_table_write_undefined_scalars(const char* path, char** out_error) {
    try { write_undefined_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_undefined_scalars"); return -1; }
}

int32_t cpp_table_verify_undefined_scalars(const char* path, char** out_error) {
    try { verify_undefined_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_undefined_scalars"); return -1; }
}

// Record column: 3-row table with scalar Int "id" and record "meta".
// Row 0: meta = {unit: "Jy", value: 2.5}
// Row 1: meta = {flag: True}
// Row 2: meta = {} (empty record)
void write_record_column_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
    td.addColumn(casacore::ScalarRecordColumnDesc("meta"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);
    casacore::Table table(setup, 3);

    // Write scalar "id" column.
    casacore::ScalarColumn<casacore::Int> id_col(table, "id");
    id_col.put(0, 1);
    id_col.put(1, 2);
    id_col.put(2, 3);

    // Write record "meta" column using ScalarColumn<TableRecord>.
    casacore::ScalarColumn<casacore::TableRecord> meta_col(table, "meta");

    // Row 0: {unit: "Jy", value: 2.5}
    casacore::TableRecord rec0;
    rec0.define("unit", casacore::String("Jy"));
    rec0.define("value", 2.5);
    meta_col.put(0, rec0);

    // Row 1: {flag: True}
    casacore::TableRecord rec1;
    rec1.define("flag", casacore::True);
    meta_col.put(1, rec1);

    // Row 2: empty record
    casacore::TableRecord rec2;
    meta_col.put(2, rec2);
}

void verify_record_column_impl(const std::string& path) {
    casacore::Table table(path);
    if (table.nrow() != 3) {
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));
    }

    // Verify scalar id column.
    casacore::ScalarColumn<casacore::Int> id_col(table, "id");
    if (id_col(0) != 1 || id_col(1) != 2 || id_col(2) != 3) {
        throw std::runtime_error("id column values mismatch");
    }

    // Verify record column using ScalarColumn<TableRecord>.
    casacore::ScalarColumn<casacore::TableRecord> meta_col(table, "meta");

    // Row 0: {unit: "Jy", value: 2.5}
    casacore::TableRecord r0 = meta_col(0);
    if (r0.asString("unit") != "Jy") {
        throw std::runtime_error("row 0 meta.unit != Jy");
    }
    if (r0.asDouble("value") != 2.5) {
        throw std::runtime_error("row 0 meta.value != 2.5");
    }

    // Row 1: {flag: True}
    casacore::TableRecord r1 = meta_col(1);
    if (!r1.asBool("flag")) {
        throw std::runtime_error("row 1 meta.flag != True");
    }

    // Row 2: empty record
    casacore::TableRecord r2 = meta_col(2);
    if (r2.nfields() != 0) {
        throw std::runtime_error("row 2 meta is not empty: nfields=" +
            std::to_string(r2.nfields()));
    }
}

int32_t cpp_table_write_record_column(const char* path, char** out_error) {
    try { write_record_column_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_record_column"); return -1; }
}

int32_t cpp_table_verify_record_column(const char* path, char** out_error) {
    try { verify_record_column_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_record_column"); return -1; }
}

int32_t cpp_table_write_column_keywords(const char* path, char** out_error) {
    try { write_column_keywords_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_column_keywords"); return -1; }
}

int32_t cpp_table_verify_column_keywords(const char* path, char** out_error) {
    try { verify_column_keywords_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_column_keywords"); return -1; }
}

// Mixed schema: 2-row table combining scalar, fixed array, variable array,
// record column, table keywords, and column keywords.
// Columns:
//   id       : Int32   (scalar)
//   flux     : Float64 (scalar)
//   spectrum : Float32 [4] (fixed array)
//   vis      : Float32 variable 2-D (variable array, ndim=2)
//   meta     : Record  (record column)
// Table keywords: telescope="ALMA", version=3
// Column keywords: flux: unit="Jy"
int32_t cpp_table_write_mixed_schema(const char* path, char** out_error) {
    try { write_mixed_schema_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_mixed_schema"); return -1; }
}

int32_t cpp_table_verify_mixed_schema(const char* path, char** out_error) {
    try { verify_mixed_schema_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_mixed_schema"); return -1; }
}

// ===== AllNumericScalars: 3 rows × 6 cols (uChar, Short, uShort, uInt, Float, Int64) =====

void write_aipsio_all_numeric_scalars_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::uChar>("col_u8"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Short>("col_i16"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::uShort>("col_u16"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::uInt>("col_u32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Float>("col_f32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int64>("col_i64"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
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

void verify_aipsio_all_numeric_scalars_impl(const std::string& path) {
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

void write_aipsio_complex_scalars_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Complex>("col_c32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::DComplex>("col_c64"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
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

void verify_aipsio_complex_scalars_impl(const std::string& path) {
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

// ===== TypedArrays: 3 rows × 3 cols (Int[4], Double[2,2], Bool[3]) =====

void write_aipsio_typed_arrays_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Int>("arr_i32",
        casacore::IPosition(1, 4), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Double>("arr_f64",
        casacore::IPosition(2, 2, 2), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("arr_f32",
        casacore::IPosition(1, 3), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);

    casacore::ArrayColumn<casacore::Int> colI32(table, "arr_i32");
    casacore::ArrayColumn<casacore::Double> colF64(table, "arr_f64");
    casacore::ArrayColumn<casacore::Float> colF32(table, "arr_f32");

    // Row 0
    {
        casacore::Vector<casacore::Int> v(4);
        v(0)=10; v(1)=20; v(2)=30; v(3)=40;
        colI32.put(0, v);

        casacore::Array<casacore::Double> a(casacore::IPosition(2, 2, 2));
        a(casacore::IPosition(2, 0, 0))=1.1; a(casacore::IPosition(2, 1, 0))=2.2;
        a(casacore::IPosition(2, 0, 1))=3.3; a(casacore::IPosition(2, 1, 1))=4.4;
        colF64.put(0, a);

        casacore::Vector<casacore::Float> f(3);
        f(0)=1.5f; f(1)=2.5f; f(2)=3.5f;
        colF32.put(0, f);
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

        casacore::Vector<casacore::Float> f(3);
        f(0)=-1.5f; f(1)=-2.5f; f(2)=-3.5f;
        colF32.put(1, f);
    }
    // Row 2
    {
        casacore::Vector<casacore::Int> v(4, 0);
        colI32.put(2, v);

        casacore::Array<casacore::Double> a(casacore::IPosition(2, 2, 2), 0.0);
        colF64.put(2, a);

        casacore::Vector<casacore::Float> f(3, 0.0f);
        colF32.put(2, f);
    }

    table.flush();
}

void verify_aipsio_typed_arrays_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

    casacore::ArrayColumn<casacore::Int> colI32(table, "arr_i32");
    casacore::ArrayColumn<casacore::Double> colF64(table, "arr_f64");
    casacore::ArrayColumn<casacore::Float> colF32(table, "arr_f32");

    // Row 0
    {
        casacore::Vector<casacore::Int> v(colI32(0));
        if (v(0) != 10 || v(1) != 20 || v(2) != 30 || v(3) != 40)
            throw std::runtime_error("row 0 arr_i32 mismatch");

        casacore::Array<casacore::Double> a = colF64(0);
        if (a(casacore::IPosition(2,0,0)) != 1.1 || a(casacore::IPosition(2,1,0)) != 2.2 ||
            a(casacore::IPosition(2,0,1)) != 3.3 || a(casacore::IPosition(2,1,1)) != 4.4)
            throw std::runtime_error("row 0 arr_f64 mismatch");

        casacore::Vector<casacore::Float> f(colF32(0));
        if (f(0) != 1.5f || f(1) != 2.5f || f(2) != 3.5f)
            throw std::runtime_error("row 0 arr_f32 mismatch");
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

        casacore::Vector<casacore::Float> f(colF32(1));
        if (f(0) != -1.5f || f(1) != -2.5f || f(2) != -3.5f)
            throw std::runtime_error("row 1 arr_f32 mismatch");
    }
    // Row 2
    {
        casacore::Vector<casacore::Int> v(colI32(2));
        if (v(0) != 0 || v(1) != 0 || v(2) != 0 || v(3) != 0)
            throw std::runtime_error("row 2 arr_i32 mismatch");

        casacore::Array<casacore::Double> a = colF64(2);
        if (a(casacore::IPosition(2,0,0)) != 0.0 || a(casacore::IPosition(2,1,1)) != 0.0)
            throw std::runtime_error("row 2 arr_f64 mismatch");

        casacore::Vector<casacore::Float> f(colF32(2));
        if (f(0) != 0.0f || f(1) != 0.0f || f(2) != 0.0f)
            throw std::runtime_error("row 2 arr_f32 mismatch");
    }
}

// ===== 3D FixedArray: Float32 [2,3,4], 2 rows, ascending values =====

void write_aipsio_3d_fixed_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    casacore::IPosition shape(3, 2, 3, 4);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("data", shape,
        casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 2);
    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    // Row 0: values 1.0..24.0 in Fortran order
    {
        casacore::Array<casacore::Float> arr(shape);
        casacore::Float val = 1.0f;
        for (int k = 0; k < 4; ++k)
            for (int j = 0; j < 3; ++j)
                for (int i = 0; i < 2; ++i)
                    arr(casacore::IPosition(3, i, j, k)) = val++;
        colData.put(0, arr);
    }
    // Row 1: values 25.0..48.0
    {
        casacore::Array<casacore::Float> arr(shape);
        casacore::Float val = 25.0f;
        for (int k = 0; k < 4; ++k)
            for (int j = 0; j < 3; ++j)
                for (int i = 0; i < 2; ++i)
                    arr(casacore::IPosition(3, i, j, k)) = val++;
        colData.put(1, arr);
    }

    table.flush();
}

void verify_aipsio_3d_fixed_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 2)
        throw std::runtime_error("expected 2 rows, got " + std::to_string(table.nrow()));

    casacore::ArrayColumn<casacore::Float> colData(table, "data");
    casacore::IPosition expectedShape(3, 2, 3, 4);

    {
        casacore::Array<casacore::Float> arr = colData(0);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(3, 0, 0, 0)) != 1.0f)
            throw std::runtime_error("row 0 [0,0,0] mismatch");
        if (arr(casacore::IPosition(3, 1, 2, 3)) != 24.0f)
            throw std::runtime_error("row 0 [1,2,3] mismatch");
    }
    {
        casacore::Array<casacore::Float> arr = colData(1);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 1 shape mismatch");
        if (arr(casacore::IPosition(3, 0, 0, 0)) != 25.0f)
            throw std::runtime_error("row 1 [0,0,0] mismatch");
        if (arr(casacore::IPosition(3, 1, 2, 3)) != 48.0f)
            throw std::runtime_error("row 1 [1,2,3] mismatch");
    }
}

int32_t cpp_table_write_aipsio_all_numeric_scalars(const char* path, char** out_error) {
    try { write_aipsio_all_numeric_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_aipsio_all_numeric_scalars(const char* path, char** out_error) {
    try { verify_aipsio_all_numeric_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_aipsio_complex_scalars(const char* path, char** out_error) {
    try { write_aipsio_complex_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_aipsio_complex_scalars(const char* path, char** out_error) {
    try { verify_aipsio_complex_scalars_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_aipsio_typed_arrays(const char* path, char** out_error) {
    try { write_aipsio_typed_arrays_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_aipsio_typed_arrays(const char* path, char** out_error) {
    try { verify_aipsio_typed_arrays_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_aipsio_3d_fixed_array(const char* path, char** out_error) {
    try { write_aipsio_3d_fixed_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_aipsio_3d_fixed_array(const char* path, char** out_error) {
    try { verify_aipsio_3d_fixed_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

} // extern "C"
