// SPDX-License-Identifier: LGPL-3.0-or-later
#include <synthesis/ImagerObjects/SIImageStoreMultiTerm.h>
#include <synthesis/MeasurementEquations/MatrixCleaner.h>
#include <casacore/images/Images/PagedImage.h>
#include <casacore/casa/Arrays/ArrayMath.h>
#include <casacore/casa/System/AppState.h>
#include <casacore/measures/Measures/MeasTable.h>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <sys/resource.h>

using namespace casacore;
using namespace casa;
using Clock = std::chrono::steady_clock;

struct LocalMeasures : AppState {
    std::string root;
    explicit LocalMeasures(std::string value) : root(std::move(value)) {}
    std::string measuresDir() const override { return root; }
    std::list<std::string> dataPath() const override { return {root}; }
    bool initialized() const override { return true; }
};

struct NativeStore : SIImageStoreMultiTerm {
    using SIImageStoreMultiTerm::SIImageStoreMultiTerm;
    using SIImageStore::getPbMax;
    Double last_pb_scale() const { return itsPBScaleFactor; }
};

struct NativeScale : MatrixCleaner {
    using MatrixCleaner::makeScale;
};

Matrix<Float> read_fixture(const std::filesystem::path& root, const std::string& name) {
    static_assert(std::endian::native == std::endian::little);
    const auto path = root / (name + ".f32le");
    if (std::filesystem::file_size(path) != 512 * 512 * sizeof(Float))
        throw std::runtime_error("unexpected fixture length: " + path.string());
    std::ifstream input(path, std::ios::binary);
    Matrix<Float> result(512, 512);
    for (int x = 0; x < 512; ++x)
        for (int y = 0; y < 512; ++y) {
            Float value;
            input.read(reinterpret_cast<char*>(&value), sizeof(value));
            if (!input || !std::isfinite(value))
                throw std::runtime_error("invalid fixture input: " + name);
            result(x, y) = value;
        }
    return result;
}

void export_plane(const std::filesystem::path& root, const std::string& name,
                  const Matrix<Float>& pixels) {
    std::ofstream output(root / (name + ".f32le"), std::ios::binary);
    for (int x = 0; x < 512; ++x)
        for (int y = 0; y < 512; ++y) {
            const Float value = pixels(x, y);
            if (!std::isfinite(value)) throw std::runtime_error("nonfinite native output");
            output.write(reinterpret_cast<const char*>(&value), sizeof(value));
        }
    if (!output) throw std::runtime_error("fixture export failed");
}

void put_model(NativeStore& store, unsigned term, const Matrix<Float>& value) {
    store.model(term)->put(value.reform(IPosition(4, 512, 512, 1, 1)));
}

Matrix<Float> get_model(NativeStore& store, unsigned term) {
    Array<Float> stored;
    store.model(term)->get(stored);
    return Matrix<Float>(stored.nonDegenerate()).copy();
}

void summarize(const std::string& stage, unsigned term, const Matrix<Float>& pixels) {
    std::size_t nonzero = 0;
    for (auto value : pixels) nonzero += value != 0;
    std::cout << "array\tstage=" << stage << "\tterm=" << term
              << "\tnonzero=" << nonzero << "\tsum=" << sum(pixels)
              << "\tpeak_abs=" << max(abs(pixels)) << '\n';
}

void convert_and_roundtrip(NativeStore& store, const std::filesystem::path& output,
                          const std::string& name, Float pblimit,
                          const std::array<Matrix<Float>, 2>& apparent) {
    for (unsigned term = 0; term < 2; ++term) {
        put_model(store, term, apparent[term]);
        export_plane(output, name + "_apparent" + std::to_string(term), apparent[term]);
        summarize(name + "_apparent", term, apparent[term]);
    }
    store.divideModelByWeight(pblimit, "flatnoise");
    std::cout << "native_divide\tcase=" << name << "\tpblimit_float=" << pblimit
              << "\tpb_scale_double=" << store.last_pb_scale()
              << "\tpb_scale_float_bits="
              << std::bit_cast<std::uint32_t>(Float(store.last_pb_scale())) << '\n';
    for (unsigned term = 0; term < 2; ++term) {
        const auto physical = get_model(store, term);
        export_plane(output, name + "_physical" + std::to_string(term), physical);
        summarize(name + "_physical", term, physical);
    }
    store.multiplyModelByWeight(pblimit, "flatnoise");
    for (unsigned term = 0; term < 2; ++term) {
        const auto roundtrip = get_model(store, term);
        export_plane(output, name + "_roundtrip" + std::to_string(term), roundtrip);
        double squared_error = 0, squared_reference = 0;
        std::size_t changed = 0;
        for (int x = 0; x < 512; ++x)
            for (int y = 0; y < 512; ++y) {
                const double reference = apparent[term](x, y);
                const double difference = double(roundtrip(x, y)) - reference;
                squared_error += difference * difference;
                squared_reference += reference * reference;
                changed += difference != 0;
            }
        std::cout << "native_roundtrip\tcase=" << name << "\tterm=" << term
                  << "\tchanged_pixels=" << changed << "\tnrms="
                  << std::sqrt(squared_error / squared_reference) << '\n';
    }
}

int main(int argc, char** argv) try {
    if (argc != 5)
        throw std::runtime_error("retained_fixture dirty_prefix new_output_directory measures_root");
    const auto started = Clock::now();
    std::thread([started] {
        for (;;) {
            rusage usage{};
            getrusage(RUSAGE_SELF, &usage);
            if (usage.ru_maxrss > 2LL * 1024 * 1024 * 1024 ||
                Clock::now() - started > std::chrono::seconds(120)) {
                std::cerr << "native model normalization resource ceiling exceeded\n";
                std::_Exit(124);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }).detach();
    const std::filesystem::path input(argv[1]), output(argv[3]);
    if (!std::filesystem::is_empty(output))
        throw std::runtime_error("output directory must already exist and be empty");
    AppStateSource::initialize(new LocalMeasures(argv[4]));
    MPosition observatory;
    if (!MeasTable::Observatory(observatory, "VLA"))
        throw std::runtime_error("native VLA measures unavailable");
    PagedImage<Float> coordinate_source(std::string(argv[2]) + ".weight.tt0");
    PagedImage<Float> sumwt_source(std::string(argv[2]) + ".sumwt.tt0");
    const auto shape = coordinate_source.shape();
    if (shape != IPosition(4, 512, 512, 1, 1) ||
        sumwt_source.shape() != IPosition(4, 1, 1, 1, 1))
        throw std::runtime_error("unexpected retained image shape");
    const Float sumwt = sumwt_source.getAt(IPosition(4, 0, 0, 0, 0));
    auto weight = read_fixture(input, "weight0");
    auto mask = read_fixture(input, "mask");
    std::array<Matrix<Float>, 2> native_model{
        read_fixture(input, "native_model0"), read_fixture(input, "native_model1")};
    NativeStore store((output / "native").string(), coordinate_source.coordinates(),
                      shape, "T51 retained native normalization probe", Record(),
                      1, false, 2, true);
    store.weight(0)->put(weight.reform(shape));
    store.sumwt(0)->set(sumwt);
    const Float pblimit = 0.0001f;
    std::cout << std::setprecision(17)
              << "inputs\tfixture=" << input << "\tdirty_prefix=" << argv[2]
              << "\tshape=512,512\tweight_min=" << min(weight)
              << "\tweight_max=" << max(weight) << "\tmask_pixels=" << sum(mask)
              << "\tsumwt0_float=" << sumwt << "\tsumwt0_float_bits="
              << std::bit_cast<std::uint32_t>(sumwt)
              << "\tgetPbMax_double=" << store.getPbMax()
              << "\tgetPbMax_float_bits="
              << std::bit_cast<std::uint32_t>(Float(store.getPbMax())) << '\n';
    export_plane(output, "weight0", weight);
    export_plane(output, "mask", mask);
    convert_and_roundtrip(store, output, "native_first3", pblimit, native_model);

    NativeScale scale_maker;
    Matrix<Float> centered_scale(512, 512), scale12(512, 512);
    scale_maker.makeScale(centered_scale, 12.0f);
    int peak_x = 0, peak_y = 0;
    for (int x = 0; x < 512; ++x)
        for (int y = 0; y < 512; ++y)
            if (std::abs(native_model[0](x, y)) > std::abs(native_model[0](peak_x, peak_y))) {
                peak_x = x;
                peak_y = y;
            }
    scale12 = 0.0f;
    for (int x = 0; x < 512; ++x)
        for (int y = 0; y < 512; ++y) {
            const int sx = x - peak_x + 256, sy = y - peak_y + 256;
            if (sx >= 0 && sx < 512 && sy >= 0 && sy < 512)
                scale12(x, y) = centered_scale(sx, sy);
        }
    export_plane(output, "scale12_native", scale12);
    std::cout << "scale12\tcentre_x=" << peak_x << "\tcentre_y=" << peak_y
              << "\tvolume=" << sum(scale12) << "\tpeak=" << max(scale12) << '\n';
    std::array<Matrix<Float>, 2> baseline{Matrix<Float>(512, 512), Matrix<Float>(512, 512)};
    const std::array<Float, 2> baseline_flux{0.000125f, -0.0000625f};
    const std::array<Float, 2> scale_flux{0.003125f, -0.00125f};
    for (unsigned term = 0; term < 2; ++term) {
        for (int x = 0; x < 512; ++x)
            for (int y = 0; y < 512; ++y)
                baseline[term](x, y) = mask(x, y) > 0 ? baseline_flux[term] : 0.0f;
        put_model(store, term, baseline[term]);
        export_plane(output, "baseline_physical" + std::to_string(term), baseline[term]);
    }
    store.multiplyModelByWeight(pblimit, "flatnoise");
    std::array<Matrix<Float>, 2> composite;
    for (unsigned term = 0; term < 2; ++term) {
        const auto apparent_baseline = get_model(store, term);
        export_plane(output, "baseline_apparent" + std::to_string(term), apparent_baseline);
        composite[term] = apparent_baseline.copy();
        for (int x = 0; x < 512; ++x)
            for (int y = 0; y < 512; ++y)
                composite[term](x, y) += native_model[term](x, y) + scale_flux[term] * scale12(x, y);
        std::cout << "composite\tterm=" << term << "\tbaseline_physical_float="
                  << baseline_flux[term] << "\tscale12_apparent_flux_float=" << scale_flux[term] << '\n';
    }
    convert_and_roundtrip(store, output, "baseline_scale12", pblimit, composite);
    store.releaseLocks();
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::cout << "complete\twall_seconds="
              << std::chrono::duration<double>(Clock::now() - started).count()
              << "\tpeak_rss_bytes=" << usage.ru_maxrss << '\n' << std::flush;
    return 0;
} catch (const std::exception& error) {
    std::cerr << "native model normalization probe failed: " << error.what() << '\n';
    return 1;
}
