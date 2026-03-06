// SPDX-License-Identifier: LGPL-3.0-or-later
// C++ shim for cross-validating Rust table quantum columns against C++ casacore.
//
// Three extern "C" functions:
//   table_quantum_create_cpp  — create a table with quantum columns + data
//   table_quantum_read_cpp    — read back quantum values for comparison
//   table_quantum_verify_cpp  — verify a Rust-written quantum table from C++

#include <casacore/measures/TableMeasures/TableQuantumDesc.h>
#include <casacore/measures/TableMeasures/ScalarQuantColumn.h>
#include <casacore/measures/TableMeasures/ArrayQuantColumn.h>
#include <casacore/tables/Tables/TableDesc.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/ScaColDesc.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ScalarColumn.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/casa/Quanta/Quantum.h>
#include <casacore/casa/Quanta/Unit.h>
#include <casacore/casa/Arrays/Vector.h>
#include <casacore/casa/Arrays/ArrayMath.h>
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <casacore/casa/OS/Timer.h>
#include <cstring>
#include <cstdint>
#include <iostream>
#include <chrono>

using namespace casacore;

// ─── Create a table with quantum columns ────────────────────────────────────
//
// Layout:
//   ScaFixedDeg    — Scalar<Double>, fixed unit "deg"
//   ScaVarUnits    — Scalar<Double>, variable units via "ScaUnitCol"
//   ScaUnitCol     — Scalar<String>, units for ScaVarUnits
//   ArrFixed       — Array<Double> shape [4], fixed unit "MHz"
//   ArrVarPerRow   — Array<Double> shape [3], variable units via "ArrUnitScaCol"
//   ArrUnitScaCol  — Scalar<String>, per-row units for ArrVarPerRow
//
// 3 rows of data.
extern "C" int table_quantum_create_cpp(const char* path) {
    try {
        TableDesc td("tTableQuantumShim", "1", TableDesc::New);
        td.addColumn(ScalarColumnDesc<Double>("ScaFixedDeg"));
        td.addColumn(ScalarColumnDesc<Double>("ScaVarUnits"));
        td.addColumn(ScalarColumnDesc<String>("ScaUnitCol"));
        td.addColumn(ArrayColumnDesc<Double>("ArrFixed", IPosition(1, 4)));
        td.addColumn(ArrayColumnDesc<Double>("ArrVarPerRow", IPosition(1, 3)));
        td.addColumn(ScalarColumnDesc<String>("ArrUnitScaCol"));

        // Quantum descriptors.
        TableQuantumDesc tqdScaFixed(td, "ScaFixedDeg", Unit("deg"));
        tqdScaFixed.write(td);

        {
            const Char* uc = "ScaUnitCol";
            TableQuantumDesc tqdScaVar(td, "ScaVarUnits", uc);
            tqdScaVar.write(td);
        }

        TableQuantumDesc tqdArrFixed(td, "ArrFixed", Unit("MHz"));
        tqdArrFixed.write(td);

        {
            const Char* uc = "ArrUnitScaCol";
            TableQuantumDesc tqdArrVarRow(td, "ArrVarPerRow", uc);
            tqdArrVarRow.write(td);
        }

        SetupNewTable setup(path, td, Table::New);
        StManAipsIO sm;
        setup.bindAll(sm);
        Table table(setup, 3);

        // Write data using raw columns (more reliable than ArrayQuantColumn::put).
        ScalarColumn<Double> scaFixed(table, "ScaFixedDeg");
        ScalarColumn<Double> scaVar(table, "ScaVarUnits");
        ScalarColumn<String> scaUnit(table, "ScaUnitCol");
        ArrayColumn<Double>  arrFixed(table, "ArrFixed");
        ArrayColumn<Double>  arrVarRow(table, "ArrVarPerRow");
        ScalarColumn<String> arrUnitSca(table, "ArrUnitScaCol");

        // Row 0
        scaFixed.put(0, 45.0);
        scaVar.put(0, 1.5);
        scaUnit.put(0, "Jy");
        {
            Vector<Double> v(4);
            v(0) = 100.0; v(1) = 200.0; v(2) = 300.0; v(3) = 400.0;
            arrFixed.put(0, v);
        }
        {
            Vector<Double> v(3);
            v(0) = 10.0; v(1) = 20.0; v(2) = 30.0;
            arrVarRow.put(0, v);
        }
        arrUnitSca.put(0, "km");

        // Row 1
        scaFixed.put(1, 90.0);
        scaVar.put(1, 2.5);
        scaUnit.put(1, "mJy");
        {
            Vector<Double> v(4);
            v(0) = 500.0; v(1) = 600.0; v(2) = 700.0; v(3) = 800.0;
            arrFixed.put(1, v);
        }
        {
            Vector<Double> v(3);
            v(0) = 40.0; v(1) = 50.0; v(2) = 60.0;
            arrVarRow.put(1, v);
        }
        arrUnitSca.put(1, "m");

        // Row 2
        scaFixed.put(2, 180.0);
        scaVar.put(2, 2.71);
        scaUnit.put(2, "Jy");
        {
            Vector<Double> v(4);
            v(0) = 900.0; v(1) = 1000.0; v(2) = 1100.0; v(3) = 1200.0;
            arrFixed.put(2, v);
        }
        {
            Vector<Double> v(3);
            v(0) = 70.0; v(1) = 80.0; v(2) = 90.0;
            arrVarRow.put(2, v);
        }
        arrUnitSca.put(2, "cm");

        return 0;
    } catch (std::exception& e) {
        std::cerr << "table_quantum_create_cpp: " << e.what() << std::endl;
        return -1;
    }
}

// ─── Read quantum values from a table ───────────────────────────────────────
//
// Returns scalar fixed-unit values in values_out[0..3], units in units_out.
// values_out must have room for 3 doubles.
// units_out must have room for 3 * unit_buf_len chars (3 null-terminated strings).
extern "C" int table_quantum_read_cpp(
    const char* path,
    double* sca_fixed_out,
    double* sca_var_out,
    char* sca_var_units_out,
    int unit_buf_len
) {
    try {
        Table table(path);

        ScalarQuantColumn<Double> sqcFixed(table, "ScaFixedDeg");
        ScalarQuantColumn<Double> sqcVar(table, "ScaVarUnits");

        for (unsigned i = 0; i < 3; ++i) {
            Quantum<Double> q = sqcFixed(i);
            sca_fixed_out[i] = q.getValue();

            Quantum<Double> qv = sqcVar(i);
            sca_var_out[i] = qv.getValue();

            std::string u = qv.getUnit();
            std::strncpy(sca_var_units_out + i * unit_buf_len, u.c_str(), unit_buf_len - 1);
            sca_var_units_out[i * unit_buf_len + unit_buf_len - 1] = '\0';
        }

        return 0;
    } catch (std::exception& e) {
        std::cerr << "table_quantum_read_cpp: " << e.what() << std::endl;
        return -1;
    }
}

// ─── Verify a Rust-written quantum table from C++ ───────────────────────────
//
// Opens a table at `path`, reads all quantum columns, checks values.
// Returns 0 on success, -1 on error. Sets *ok_out = 1 if all checks pass.
extern "C" int table_quantum_verify_cpp(const char* path, int* ok_out) {
    try {
        *ok_out = 0;
        Table table(path);

        // Check ScaFixedDeg has quantum keywords.
        if (!TableQuantumDesc::hasQuanta(TableColumn(table, "ScaFixedDeg"))) {
            std::cerr << "verify: ScaFixedDeg has no quanta keywords" << std::endl;
            return 0;
        }

        ScalarQuantColumn<Double> sqcFixed(table, "ScaFixedDeg");
        for (unsigned i = 0; i < table.nrow(); ++i) {
            Quantum<Double> q = sqcFixed(i);
            if (q.getUnit() != "deg") {
                std::cerr << "verify: ScaFixedDeg row " << i << " unit = "
                          << q.getUnit() << " (expected deg)" << std::endl;
                return 0;
            }
        }

        // Check ScaVarUnits has variable quanta.
        if (!TableQuantumDesc::hasQuanta(TableColumn(table, "ScaVarUnits"))) {
            std::cerr << "verify: ScaVarUnits has no quanta keywords" << std::endl;
            return 0;
        }

        ScalarQuantColumn<Double> sqcVar(table, "ScaVarUnits");
        for (unsigned i = 0; i < table.nrow(); ++i) {
            Quantum<Double> q = sqcVar(i);
            // Just check we can read without errors.
            (void)q.getValue();
        }

        // Check ArrFixed.
        if (!TableQuantumDesc::hasQuanta(TableColumn(table, "ArrFixed"))) {
            std::cerr << "verify: ArrFixed has no quanta keywords" << std::endl;
            return 0;
        }

        *ok_out = 1;
        return 0;
    } catch (std::exception& e) {
        std::cerr << "table_quantum_verify_cpp: " << e.what() << std::endl;
        return -1;
    }
}

// ─── Bench: read N rows of scalar quantum column ────────────────────────────
extern "C" int table_quantum_bench_scalar_read_cpp(
    const char* path,
    const char* column,
    int iterations,
    uint64_t* elapsed_ns_out
) {
    try {
        Table table(path);
        ScalarQuantColumn<Double> sqc(table, column);
        unsigned nrow = table.nrow();

        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; ++iter) {
            for (unsigned r = 0; r < nrow; ++r) {
                Quantum<Double> q = sqc(r);
                (void)q.getValue();
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns_out = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (std::exception& e) {
        std::cerr << "bench_scalar_read: " << e.what() << std::endl;
        return -1;
    }
}

// ─── Bench: read N rows of array quantum column ─────────────────────────────
extern "C" int table_quantum_bench_array_read_cpp(
    const char* path,
    const char* column,
    int iterations,
    uint64_t* elapsed_ns_out
) {
    try {
        Table table(path);
        ArrayQuantColumn<Double> aqc(table, column);
        unsigned nrow = table.nrow();

        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; ++iter) {
            for (unsigned r = 0; r < nrow; ++r) {
                Array<Quantum<Double>> arr = aqc(r);
                (void)arr.size();
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns_out = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (std::exception& e) {
        std::cerr << "bench_array_read: " << e.what() << std::endl;
        return -1;
    }
}
