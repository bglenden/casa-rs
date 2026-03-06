// MeasurementSet interop fixtures and benchmarks for C++ casacore.
#include "casacore_cpp_common.h"

#include <casacore/casa/Arrays/Matrix.h>
#include <casacore/casa/Arrays/Vector.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/DataMan/StManAipsIO.h>

#include <chrono>
#include <cmath>

using casacore_shim::make_error;
static casacore_shim::TerminateGuard g_terminate_guard_ms;

namespace {

constexpr casacore::uInt kNumCorr = 4;
constexpr casacore::uInt kNumChan = 16;
constexpr double kBaseTimeSeconds = 59000.0 * 86400.0;

casacore::Array<casacore::Complex> make_data_array(size_t row)
{
    casacore::IPosition shape(2, kNumCorr, kNumChan);
    casacore::Array<casacore::Complex> data(shape);
    const float offset = static_cast<float>(row * kNumCorr * kNumChan);
    size_t i = 0;
    for (casacore::uInt chan = 0; chan < kNumChan; ++chan) {
        for (casacore::uInt corr = 0; corr < kNumCorr; ++corr) {
            const float value = offset + static_cast<float>(i);
            data(casacore::IPosition(2, corr, chan)) =
                casacore::Complex(value, -0.5f * value);
            ++i;
        }
    }
    return data;
}

void populate_fixture_subtables(casacore::MeasurementSet& ms)
{
    {
        casacore::MSAntenna antenna = ms.antenna();
        antenna.addRow(2);

        casacore::ScalarColumn<casacore::String> name(antenna, "NAME");
        casacore::ScalarColumn<casacore::String> station(antenna, "STATION");
        casacore::ScalarColumn<casacore::String> type(antenna, "TYPE");
        casacore::ScalarColumn<casacore::String> mount(antenna, "MOUNT");
        casacore::ArrayColumn<casacore::Double> position(antenna, "POSITION");
        casacore::ArrayColumn<casacore::Double> offset(antenna, "OFFSET");
        casacore::ScalarColumn<casacore::Double> dish(antenna, "DISH_DIAMETER");
        casacore::ScalarColumn<casacore::Bool> flagRow(antenna, "FLAG_ROW");

        casacore::Vector<casacore::Double> pos0(3);
        pos0(0) = 0.0;
        pos0(1) = 10.0;
        pos0(2) = 20.0;
        casacore::Vector<casacore::Double> pos1(3);
        pos1(0) = 100.0;
        pos1(1) = 110.0;
        pos1(2) = 120.0;
        casacore::Vector<casacore::Double> zeroOffset(3);
        zeroOffset = 0.0;

        name.put(0, "ANT0");
        station.put(0, "STA0");
        type.put(0, "GROUND-BASED");
        mount.put(0, "ALT-AZ");
        position.put(0, pos0);
        offset.put(0, zeroOffset);
        dish.put(0, 12.0);
        flagRow.put(0, false);

        name.put(1, "ANT1");
        station.put(1, "STA1");
        type.put(1, "GROUND-BASED");
        mount.put(1, "ALT-AZ");
        position.put(1, pos1);
        offset.put(1, zeroOffset);
        dish.put(1, 13.0);
        flagRow.put(1, false);
    }

    {
        casacore::MSField field = ms.field();
        field.addRow(1);

        casacore::ScalarColumn<casacore::String> name(field, "NAME");
        casacore::ScalarColumn<casacore::String> code(field, "CODE");
        casacore::ScalarColumn<casacore::Int> numPoly(field, "NUM_POLY");
        casacore::ArrayColumn<casacore::Double> delayDir(field, "DELAY_DIR");
        casacore::ArrayColumn<casacore::Double> phaseDir(field, "PHASE_DIR");
        casacore::ArrayColumn<casacore::Double> refDir(field, "REFERENCE_DIR");
        casacore::ScalarColumn<casacore::Int> sourceId(field, "SOURCE_ID");
        casacore::ScalarColumn<casacore::Double> time(field, "TIME");
        casacore::ScalarColumn<casacore::Bool> flagRow(field, "FLAG_ROW");

        casacore::Matrix<casacore::Double> direction(2, 1);
        direction(0, 0) = 1.0;
        direction(1, 0) = 0.5;

        name.put(0, "TEST_FIELD");
        code.put(0, "T");
        numPoly.put(0, 0);
        delayDir.put(0, direction);
        phaseDir.put(0, direction);
        refDir.put(0, direction);
        sourceId.put(0, -1);
        time.put(0, kBaseTimeSeconds);
        flagRow.put(0, false);
    }

    {
        casacore::MSPolarization pol = ms.polarization();
        pol.addRow(1);

        casacore::ScalarColumn<casacore::Int> numCorr(pol, "NUM_CORR");
        casacore::ArrayColumn<casacore::Int> corrType(pol, "CORR_TYPE");
        casacore::ArrayColumn<casacore::Int> corrProduct(pol, "CORR_PRODUCT");
        casacore::ScalarColumn<casacore::Bool> flagRow(pol, "FLAG_ROW");

        casacore::Vector<casacore::Int> corrTypeVals(kNumCorr);
        corrTypeVals(0) = 5;
        corrTypeVals(1) = 6;
        corrTypeVals(2) = 7;
        corrTypeVals(3) = 8;

        casacore::Matrix<casacore::Int> corrProductVals(2, kNumCorr);
        corrProductVals(0, 0) = 0;
        corrProductVals(1, 0) = 0;
        corrProductVals(0, 1) = 0;
        corrProductVals(1, 1) = 1;
        corrProductVals(0, 2) = 1;
        corrProductVals(1, 2) = 0;
        corrProductVals(0, 3) = 1;
        corrProductVals(1, 3) = 1;

        numCorr.put(0, static_cast<casacore::Int>(kNumCorr));
        corrType.put(0, corrTypeVals);
        corrProduct.put(0, corrProductVals);
        flagRow.put(0, false);
    }

    {
        casacore::MSSpectralWindow spw = ms.spectralWindow();
        spw.addRow(1);

        casacore::ScalarColumn<casacore::Int> numChan(spw, "NUM_CHAN");
        casacore::ScalarColumn<casacore::String> name(spw, "NAME");
        casacore::ScalarColumn<casacore::Double> refFrequency(spw, "REF_FREQUENCY");
        casacore::ScalarColumn<casacore::Double> totalBandwidth(spw, "TOTAL_BANDWIDTH");
        casacore::ArrayColumn<casacore::Double> chanFreq(spw, "CHAN_FREQ");
        casacore::ArrayColumn<casacore::Double> chanWidth(spw, "CHAN_WIDTH");
        casacore::ArrayColumn<casacore::Double> effectiveBw(spw, "EFFECTIVE_BW");
        casacore::ArrayColumn<casacore::Double> resolution(spw, "RESOLUTION");
        casacore::ScalarColumn<casacore::Int> measFreqRef(spw, "MEAS_FREQ_REF");
        casacore::ScalarColumn<casacore::Int> netSideband(spw, "NET_SIDEBAND");
        casacore::ScalarColumn<casacore::Int> freqGroup(spw, "FREQ_GROUP");
        casacore::ScalarColumn<casacore::String> freqGroupName(spw, "FREQ_GROUP_NAME");
        casacore::ScalarColumn<casacore::Int> ifConvChain(spw, "IF_CONV_CHAIN");
        casacore::ScalarColumn<casacore::Bool> flagRow(spw, "FLAG_ROW");

        casacore::Vector<casacore::Double> freqs(kNumChan);
        casacore::Vector<casacore::Double> widths(kNumChan);
        for (casacore::uInt chan = 0; chan < kNumChan; ++chan) {
            freqs(chan) = 1.0e9 + chan * 1.0e6;
            widths(chan) = 1.0e6;
        }

        numChan.put(0, static_cast<casacore::Int>(kNumChan));
        name.put(0, "SPW0");
        refFrequency.put(0, 1.0e9);
        totalBandwidth.put(0, static_cast<casacore::Double>(kNumChan) * 1.0e6);
        chanFreq.put(0, freqs);
        chanWidth.put(0, widths);
        effectiveBw.put(0, widths);
        resolution.put(0, widths);
        measFreqRef.put(0, 5);
        netSideband.put(0, 1);
        freqGroup.put(0, 0);
        freqGroupName.put(0, "");
        ifConvChain.put(0, 0);
        flagRow.put(0, false);
    }

    {
        casacore::MSDataDescription dd = ms.dataDescription();
        dd.addRow(1);

        casacore::ScalarColumn<casacore::Int> spectralWindowId(dd, "SPECTRAL_WINDOW_ID");
        casacore::ScalarColumn<casacore::Int> polarizationId(dd, "POLARIZATION_ID");
        casacore::ScalarColumn<casacore::Bool> flagRow(dd, "FLAG_ROW");

        spectralWindowId.put(0, 0);
        polarizationId.put(0, 0);
        flagRow.put(0, false);
    }
}

void populate_fixture_rows(casacore::MeasurementSet& ms, size_t nrows)
{
    ms.addRow(nrows);

    casacore::ScalarColumn<casacore::Int> antenna1(ms, "ANTENNA1");
    casacore::ScalarColumn<casacore::Int> antenna2(ms, "ANTENNA2");
    casacore::ScalarColumn<casacore::Int> arrayId(ms, "ARRAY_ID");
    casacore::ScalarColumn<casacore::Int> dataDescId(ms, "DATA_DESC_ID");
    casacore::ScalarColumn<casacore::Double> exposure(ms, "EXPOSURE");
    casacore::ScalarColumn<casacore::Int> feed1(ms, "FEED1");
    casacore::ScalarColumn<casacore::Int> feed2(ms, "FEED2");
    casacore::ScalarColumn<casacore::Int> fieldId(ms, "FIELD_ID");
    casacore::ArrayColumn<casacore::Bool> flag(ms, "FLAG");
    casacore::ArrayColumn<casacore::Bool> flagCategory(ms, "FLAG_CATEGORY");
    casacore::ScalarColumn<casacore::Bool> flagRow(ms, "FLAG_ROW");
    casacore::ScalarColumn<casacore::Double> interval(ms, "INTERVAL");
    casacore::ScalarColumn<casacore::Int> observationId(ms, "OBSERVATION_ID");
    casacore::ScalarColumn<casacore::Int> processorId(ms, "PROCESSOR_ID");
    casacore::ScalarColumn<casacore::Int> scanNumber(ms, "SCAN_NUMBER");
    casacore::ArrayColumn<casacore::Float> sigma(ms, "SIGMA");
    casacore::ScalarColumn<casacore::Int> stateId(ms, "STATE_ID");
    casacore::ScalarColumn<casacore::Double> time(ms, "TIME");
    casacore::ScalarColumn<casacore::Double> timeCentroid(ms, "TIME_CENTROID");
    casacore::ArrayColumn<casacore::Double> uvw(ms, "UVW");
    casacore::ArrayColumn<casacore::Float> weight(ms, "WEIGHT");
    casacore::ArrayColumn<casacore::Complex> data(ms, "DATA");

    casacore::Matrix<casacore::Bool> flagVals(kNumCorr, kNumChan);
    flagVals = false;
    casacore::Array<casacore::Bool> flagCategoryVals(casacore::IPosition(3, 1, kNumCorr, kNumChan));
    flagCategoryVals = false;
    casacore::Vector<casacore::Float> sigmaVals(kNumCorr);
    sigmaVals = 1.0f;
    casacore::Vector<casacore::Float> weightVals(kNumCorr);
    weightVals = 1.0f;
    casacore::Vector<casacore::Double> uvwVals(3);
    uvwVals = 0.0;

    for (size_t row = 0; row < nrows; ++row) {
        antenna1.put(row, 0);
        antenna2.put(row, 1);
        arrayId.put(row, 0);
        dataDescId.put(row, 0);
        exposure.put(row, 10.0);
        feed1.put(row, 0);
        feed2.put(row, 0);
        fieldId.put(row, 0);
        flag.put(row, flagVals);
        flagCategory.put(row, flagCategoryVals);
        flagRow.put(row, false);
        interval.put(row, 10.0);
        observationId.put(row, 0);
        processorId.put(row, 0);
        scanNumber.put(row, static_cast<casacore::Int>(row + 1));
        sigma.put(row, sigmaVals);
        stateId.put(row, 0);
        time.put(row, kBaseTimeSeconds + static_cast<double>(row));
        timeCentroid.put(row, kBaseTimeSeconds + static_cast<double>(row));
        uvw.put(row, uvwVals);
        weight.put(row, weightVals);
        data.put(row, make_data_array(row));
    }
}

void write_fixture_impl(const std::string& path, size_t nrows)
{
    casacore::TableDesc td(casacore::MS::requiredTableDesc());
    casacore::MS::addColumnToDesc(td, casacore::MS::DATA, 2);

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::MeasurementSet ms(setup, 0);
    ms.createDefaultSubtables(casacore::Table::New);
    populate_fixture_subtables(ms);
    populate_fixture_rows(ms, nrows);
    ms.flush(true);
}

void verify_complex_sample(const casacore::Array<casacore::Complex>& data,
                           casacore::uInt corr,
                           casacore::uInt chan,
                           float expectedReal,
                           float expectedImag)
{
    const casacore::Complex sample = data(casacore::IPosition(2, corr, chan));
    if (std::fabs(sample.real() - expectedReal) > 1e-5f ||
        std::fabs(sample.imag() - expectedImag) > 1e-5f) {
        throw std::runtime_error(
            "DATA sample mismatch at [" + std::to_string(corr) + "," +
            std::to_string(chan) + "]: expected (" + std::to_string(expectedReal) +
            "," + std::to_string(expectedImag) + "), got (" +
            std::to_string(sample.real()) + "," + std::to_string(sample.imag()) + ")");
    }
}

void verify_fixture_impl(const std::string& path)
{
    casacore::MeasurementSet ms(path, casacore::Table::Old);
    if (!ms.validate()) {
        throw std::runtime_error("MeasurementSet::validate() reported false");
    }
    if (ms.tableInfo().type() != "Measurement Set") {
        throw std::runtime_error("unexpected table.info type: " +
                                 std::string(ms.tableInfo().type().c_str()));
    }
    if (!ms.keywordSet().isDefined("ANTENNA") ||
        !ms.keywordSet().isDefined("DATA_DESCRIPTION") ||
        !ms.keywordSet().isDefined("FIELD") ||
        !ms.keywordSet().isDefined("POLARIZATION") ||
        !ms.keywordSet().isDefined("SPECTRAL_WINDOW")) {
        throw std::runtime_error("missing required MS subtable keyword");
    }
    if (!ms.keywordSet().isDefined("MS_VERSION")) {
        throw std::runtime_error("MS_VERSION keyword missing");
    }
    if (ms.nrow() != 6) {
        throw std::runtime_error("expected 6 main rows, got " + std::to_string(ms.nrow()));
    }
    if (ms.antenna().nrow() != 2) {
        throw std::runtime_error("expected 2 antenna rows, got " +
                                 std::to_string(ms.antenna().nrow()));
    }
    if (ms.field().nrow() != 1) {
        throw std::runtime_error("expected 1 field row, got " +
                                 std::to_string(ms.field().nrow()));
    }
    if (ms.polarization().nrow() != 1) {
        throw std::runtime_error("expected 1 polarization row, got " +
                                 std::to_string(ms.polarization().nrow()));
    }
    if (ms.spectralWindow().nrow() != 1) {
        throw std::runtime_error("expected 1 spectral-window row, got " +
                                 std::to_string(ms.spectralWindow().nrow()));
    }
    if (ms.dataDescription().nrow() != 1) {
        throw std::runtime_error("expected 1 data-description row, got " +
                                 std::to_string(ms.dataDescription().nrow()));
    }

    casacore::ScalarColumn<casacore::String> antennaName(ms.antenna(), "NAME");
    if (antennaName(0) != "ANT0" || antennaName(1) != "ANT1") {
        throw std::runtime_error("ANTENNA names do not match expected fixture");
    }

    casacore::ScalarColumn<casacore::String> fieldName(ms.field(), "NAME");
    if (fieldName(0) != "TEST_FIELD") {
        throw std::runtime_error("FIELD name mismatch");
    }

    casacore::ScalarColumn<casacore::Int> numCorr(ms.polarization(), "NUM_CORR");
    if (numCorr(0) != static_cast<casacore::Int>(kNumCorr)) {
        throw std::runtime_error("NUM_CORR mismatch");
    }

    casacore::ScalarColumn<casacore::Int> numChan(ms.spectralWindow(), "NUM_CHAN");
    casacore::ScalarColumn<casacore::String> spwName(ms.spectralWindow(), "NAME");
    if (numChan(0) != static_cast<casacore::Int>(kNumChan) || spwName(0) != "SPW0") {
        throw std::runtime_error("SPECTRAL_WINDOW contents mismatch");
    }

    casacore::ArrayColumn<casacore::Complex> data(ms, "DATA");
    casacore::Array<casacore::Complex> row0 = data(0);
    casacore::Array<casacore::Complex> row5 = data(5);
    if (row0.shape() != casacore::IPosition(2, kNumCorr, kNumChan)) {
        throw std::runtime_error("row 0 DATA shape mismatch");
    }
    if (row5.shape() != casacore::IPosition(2, kNumCorr, kNumChan)) {
        throw std::runtime_error("row 5 DATA shape mismatch");
    }
    verify_complex_sample(row0, 0, 0, 0.0f, -0.0f);
    verify_complex_sample(row0, 3, 15, 63.0f, -31.5f);
    verify_complex_sample(row5, 0, 0, 320.0f, -160.0f);
    verify_complex_sample(row5, 3, 15, 383.0f, -191.5f);
}

void bench_create_open_impl(const std::string& path,
                            uint64_t nrows,
                            uint64_t* out_create_ns,
                            uint64_t* out_open_ns,
                            uint64_t* out_read_ns)
{
    auto t0 = std::chrono::steady_clock::now();
    write_fixture_impl(path, static_cast<size_t>(nrows));
    auto t1 = std::chrono::steady_clock::now();
    *out_create_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

    t0 = std::chrono::steady_clock::now();
    casacore::MeasurementSet ms(path, casacore::Table::Old);
    t1 = std::chrono::steady_clock::now();
    *out_open_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

    casacore::ArrayColumn<casacore::Complex> data(ms, "DATA");
    volatile float sink = 0.0f;
    t0 = std::chrono::steady_clock::now();
    for (uint64_t row = 0; row < nrows; ++row) {
        casacore::Array<casacore::Complex> sample = data(row);
        sink += sample(casacore::IPosition(2, 0, 0)).real();
    }
    t1 = std::chrono::steady_clock::now();
    *out_read_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

    if (sink < 0.0f) {
        throw std::runtime_error("unexpected negative sink in benchmark");
    }
}

}  // namespace

extern "C" {

int32_t cpp_ms_write_basic_fixture(const char* path, char** out_error)
{
    try {
        write_fixture_impl(path, 6);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_write_basic_fixture");
        return -1;
    }
}

int32_t cpp_ms_verify_basic_fixture(const char* path, char** out_error)
{
    try {
        verify_fixture_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_verify_basic_fixture");
        return -1;
    }
}

int32_t cpp_ms_bench_create_open(
    const char* path,
    uint64_t nrows,
    uint64_t* out_create_ns,
    uint64_t* out_open_ns,
    uint64_t* out_read_ns,
    char** out_error)
{
    try {
        bench_create_open_impl(path, nrows, out_create_ns, out_open_ns, out_read_ns);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_ms_bench_create_open");
        return -1;
    }
}

}  // extern "C"
