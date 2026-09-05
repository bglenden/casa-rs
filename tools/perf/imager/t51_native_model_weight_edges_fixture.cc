// SPDX-License-Identifier: LGPL-3.0-or-later
// Reuse the immutable retained probe's image/fixture utilities, not its main.
#define main retained_model_weight_probe_main
#include "t51_native_model_weight_fixture.cc"
#undef main
#include <limits>
#include <vector>

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
                std::cerr << "native synthetic edge probe resource ceiling exceeded\n";
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
    if (shape != IPosition(4, 512, 512, 1, 1))
        throw std::runtime_error("unexpected retained shape");
    const Float sumwt = sumwt_source.getAt(IPosition(4, 0, 0, 0, 0));
    const auto retained_weight = read_fixture(input, "weight0");
    NativeStore store((output / "native").string(), coordinate_source.coordinates(),
                      shape, "T51 synthetic PB support edge probe", Record(),
                      1, false, 2, true);
    store.weight(0)->put(retained_weight.reform(shape));
    store.sumwt(0)->set(sumwt);
    const Float pbscale = Float(store.getPbMax());
    const Float cutoff = 0.0001f;
    std::cout << std::setprecision(17)
              << "synthetic_edges\tretained_fixture=" << input
              << "\tpblimit_float=" << cutoff << "\tpbscale_float=" << pbscale
              << "\tpbscale_bits=" << std::bit_cast<std::uint32_t>(pbscale)
              << "\tsumwt0_float=" << sumwt << '\n';

    // The square only seeds fixture candidates; native multiply chooses the
    // actual below/equal/above response values and supplies every expectation.
    std::array<Float, 65> candidates{};
    candidates[32] = Float(std::pow(double(pbscale) * double(cutoff), 2));
    for (int i = 31; i >= 0; --i)
        candidates[i] = std::nextafter(candidates[i + 1], 0.0f);
    for (int i = 33; i < 65; ++i)
        candidates[i] = std::nextafter(candidates[i - 1], std::numeric_limits<Float>::infinity());
    Matrix<Float> calibration_weight(retained_weight.copy());
    for (int i = 0; i < 65; ++i) calibration_weight(1, i) = candidates[i];
    store.weight(0)->put(calibration_weight.reform(shape));
    Matrix<Float> ones(512, 512);
    ones = 1.0f;
    for (unsigned term = 0; term < 2; ++term) put_model(store, term, ones);
    store.multiplyModelByWeight(0.0f, "flatnoise");
    const auto native_calibration = get_model(store, 0);
    int below = -1, equal = -1, above = -1;
    for (int i = 0; i < 65; ++i) {
        const Float response = native_calibration(1, i);
        if (response < cutoff) below = i;
        if (response == cutoff && equal < 0) equal = i;
        if (response > cutoff && above < 0) above = i;
    }
    if (below < 0 || equal < 0 || above < 0)
        throw std::runtime_error("bounded native Float cutoff calibration did not bracket equality");
    std::cout << "native_calibration\tbelow_weight=" << candidates[below]
              << "\tbelow_response=" << native_calibration(1, below)
              << "\tequal_weight=" << candidates[equal]
              << "\tequal_response=" << native_calibration(1, equal)
              << "\tabove_weight=" << candidates[above]
              << "\tabove_response=" << native_calibration(1, above) << '\n';

    struct Edge { const char* name; Float weight; };
    const std::array<Edge, 9> edges{{
        {"zero", 0.0f},
        {"positive_below", candidates[below]},
        {"positive_equal", candidates[equal]},
        {"positive_above", candidates[above]},
        {"negative_below", -candidates[below]},
        {"negative_equal", -candidates[equal]},
        {"negative_above", -candidates[above]},
        {"negative_retained_magnitude", -retained_weight(16, 23)},
        {"retained_positive", retained_weight(16, 24)},
    }};
    Matrix<Float> edge_weight(retained_weight.copy());
    for (int i = 0; i < 9; ++i) edge_weight(16, 16 + i) = edges[i].weight;
    store.weight(0)->put(edge_weight.reform(shape));
    if (Float(store.getPbMax()) != pbscale)
        throw std::runtime_error("synthetic changes altered the retained native PB maximum");
    export_plane(output, "edges_weight0", edge_weight);
    for (unsigned term = 0; term < 2; ++term) put_model(store, term, ones);
    store.multiplyModelByWeight(0.0f, "flatnoise");
    const auto native_response = get_model(store, 0);
    export_plane(output, "edges_native_pb_response", native_response);
    for (unsigned term = 0; term < 2; ++term) put_model(store, term, ones);
    store.divideModelByWeight(cutoff, "flatnoise");
    const auto native_divided_ones = get_model(store, 0);
    Matrix<Float> support(512, 512);
    for (int x = 0; x < 512; ++x)
        for (int y = 0; y < 512; ++y)
            support(x, y) = native_divided_ones(x, y) != 0 ? 1.0f : 0.0f;
    export_plane(output, "edges_expected_support", support);

    std::array<Matrix<Float>, 2> apparent{Matrix<Float>(512, 512), Matrix<Float>(512, 512)};
    for (unsigned term = 0; term < 2; ++term) {
        apparent[term] = 0.0f;
        for (int i = 0; i < 9; ++i)
            apparent[term](16, 16 + i) = term == 0 ? 0.125f : -0.0625f;
    }
    convert_and_roundtrip(store, output, "edges", cutoff, apparent);
    const auto physical0 = read_fixture(output, "edges_physical0");
    const auto roundtrip0 = get_model(store, 0);
    for (unsigned term = 0; term < 2; ++term) {
        put_model(store, term, apparent[term]);
        export_plane(output, "edges_physical_input" + std::to_string(term), apparent[term]);
    }
    store.multiplyModelByWeight(cutoff, "flatnoise");
    for (unsigned term = 0; term < 2; ++term)
        export_plane(output, "edges_apparent_from_physical" + std::to_string(term), get_model(store, term));
    const auto multiplied0 = get_model(store, 0);
    for (int i = 0; i < 9; ++i) {
        const int x = 16, y = 16 + i;
        std::cout << "edge\tname=" << edges[i].name << "\tx=" << x << "\ty=" << y
                  << "\tweight=" << edge_weight(x, y) << "\tresponse=" << native_response(x, y)
                  << "\tsupported=" << support(x, y) << "\tapparent_input=" << apparent[0](x, y)
                  << "\tnative_physical=" << physical0(x, y) << "\tnative_roundtrip=" << roundtrip0(x, y)
                  << "\tphysical_input=" << apparent[0](x, y) << "\tnative_multiplied=" << multiplied0(x, y) << '\n';
    }
    store.releaseLocks();
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::cout << "complete\twall_seconds="
              << std::chrono::duration<double>(Clock::now() - started).count()
              << "\tpeak_rss_bytes=" << usage.ru_maxrss << '\n' << std::flush;
    return 0;
} catch (const std::exception& error) {
    std::cerr << "native synthetic edge probe failed: " << error.what() << '\n';
    return 1;
}
