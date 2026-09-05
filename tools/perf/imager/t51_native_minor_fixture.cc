// SPDX-License-Identifier: LGPL-3.0-or-later
// Native retained-image fixture for T51 (#537); no MeasurementSet execution.
#include <synthesis/MeasurementEquations/MultiTermMatrixCleaner.h>
#include <casacore/images/Images/PagedImage.h>
#include <casacore/casa/Arrays/ArrayMath.h>
#include <casacore/casa/System/AppState.h>
#include <casacore/measures/Measures/MeasTable.h>
#include <array>
#include <bit>
#include <chrono>
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

Matrix<Float> read_plane(const std::string& prefix, const std::string& suffix) {
    PagedImage<Float> image(prefix + suffix);
    Array<Float> stored;
    image.get(stored);
    Matrix<Float> pixels(stored.nonDegenerate());
    if (pixels.shape() != IPosition(2, 512, 512))
        throw std::runtime_error("retained plane has unexpected shape: " + suffix);
    return pixels.copy();
}

void export_plane(const std::filesystem::path& root, const std::string& name,
                  const Matrix<Float>& pixels) {
    static_assert(std::endian::native == std::endian::little);
    std::ofstream output(root / (name + ".f32le"), std::ios::binary);
    for (int x = 0; x < 512; ++x)
        for (int y = 0; y < 512; ++y) {
            const Float value = pixels(x, y);
            output.write(reinterpret_cast<const char*>(&value), sizeof(value));
        }
    if (!output) throw std::runtime_error("fixture export failed");
}

int main(int argc, char** argv) try {
    if (argc != 5) throw std::runtime_error("dirty_prefix mask_prefix new_fixture_directory measures_root");
    const auto started = Clock::now();
    std::thread([started] {
        for (;;) {
            rusage usage{};
            getrusage(RUSAGE_SELF, &usage);
            if (usage.ru_maxrss > 2LL * 1024 * 1024 * 1024 ||
                Clock::now() - started > std::chrono::seconds(120)) {
                std::cerr << "native minor probe resource ceiling exceeded\n";
                std::_Exit(124);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }).detach();
    AppStateSource::initialize(new LocalMeasures(argv[4]));
    MPosition observatory;
    if (!MeasTable::Observatory(observatory, "VLA"))
        throw std::runtime_error("native VLA measures data unavailable");
    const std::filesystem::path fixture(argv[3]);
    if (!std::filesystem::is_empty(fixture))
        throw std::runtime_error("fixture directory must already exist and be empty");
    std::cout << std::setprecision(17);
    std::array<Matrix<Float>, 3> psf;
    std::array<Matrix<Float>, 2> residual, model;
    for (int term = 0; term < 3; ++term)
        psf[term] = read_plane(argv[1], ".psf.tt" + std::to_string(term));
    for (int term = 0; term < 2; ++term) {
        residual[term] = read_plane(argv[1], ".residual.tt" + std::to_string(term));
        model[term].resize(512, 512);
        model[term] = 0.0f;
    }
    auto mask = read_plane(argv[2], ".mask");
    auto weight = read_plane(argv[1], ".weight.tt0");
    std::cout << "inputs\tshape=512,512\tmask_pixels=" << sum(mask)
              << "\tinitial_peak=" << max(abs(residual[0] * mask)) << '\n' << std::flush;
    for (int term = 0; term < 3; ++term)
        export_plane(fixture, "psf" + std::to_string(term), psf[term]);
    for (int term = 0; term < 2; ++term)
        export_plane(fixture, "residual" + std::to_string(term), residual[term]);
    export_plane(fixture, "mask", mask);
    export_plane(fixture, "weight0", weight);

    MultiTermMatrixCleaner cleaner;
    Vector<Float> scales(3); scales[0] = 0; scales[1] = 5; scales[2] = 12;
    cleaner.setscales(scales);
    cleaner.setSmallScaleBias(0.0f);
    cleaner.setntaylorterms(2);
    cleaner.initialise(512, 512);
    for (int term = 0; term < 3; ++term) cleaner.setpsf(term, psf[term]);
    cleaner.setmask(mask);
    for (int term = 0; term < 2; ++term) {
        cleaner.setresidual(term, residual[term]);
        cleaner.setmodel(term, model[term]);
    }
    const Float gain = 0.1f;
    const Float threshold = 0.02846723608672619f;
    const auto solve_started = Clock::now();
    const int iterations = cleaner.mtclean(30, 0.0f, gain, threshold);
    std::cout << "minor\titerations=" << iterations << "\tgain=" << gain
              << "\tthreshold=" << threshold << "\tsolve_seconds="
              << std::chrono::duration<double>(Clock::now() - solve_started).count()
              << '\n' << std::flush;
    if (iterations != 3) throw std::runtime_error("native first-cycle iteration count differs");
    for (int term = 0; term < 2; ++term) {
        cleaner.getmodel(term, model[term]);
        cleaner.getresidual(term, residual[term]);
        export_plane(fixture, "native_model" + std::to_string(term), model[term]);
        export_plane(fixture, "native_residual" + std::to_string(term), residual[term]);
        std::cout << "output\tterm=" << term << "\tmodel_sum=" << sum(model[term])
                  << "\tmodel_peak=" << max(abs(model[term]))
                  << "\tresidual_mask_peak=" << max(abs(residual[term] * mask)) << '\n';
    }
    rusage usage{}; getrusage(RUSAGE_SELF, &usage);
    std::cout << "complete\twall_seconds="
              << std::chrono::duration<double>(Clock::now() - started).count()
              << "\tpeak_rss_bytes=" << usage.ru_maxrss << '\n' << std::flush;
    return 0;
} catch (const std::exception& error) {
    std::cerr << "native minor probe failed: " << error.what() << '\n';
    return 1;
}
