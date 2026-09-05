// SPDX-License-Identifier: LGPL-3.0-or-later
// Reuse immutable image/fixture utilities without changing the retained probe.
#define main retained_model_weight_probe_main
#include "t51_native_model_weight_fixture.cc"
#undef main

Matrix<Float> get_plane(ImageInterface<Float>& image) {
    Array<Float> stored;
    image.get(stored);
    return Matrix<Float>(stored.nonDegenerate()).copy();
}

Matrix<Float> get_image_mask(ImageInterface<Float>& image) {
    Array<Bool> stored;
    image.getMask(stored);
    Matrix<Bool> mask(stored.nonDegenerate());
    Matrix<Float> result(512, 512);
    for (int x = 0; x < 512; ++x)
        for (int y = 0; y < 512; ++y) result(x, y) = mask(x, y) ? 1.0f : 0.0f;
    return result;
}

int main(int argc, char** argv) try {
    if (argc != 5)
        throw std::runtime_error("copied_edge_fixture dirty_prefix new_output_directory measures_root");
    const auto started = Clock::now();
    std::thread([started] {
        for (;;) {
            rusage usage{};
            getrusage(RUSAGE_SELF, &usage);
            if (usage.ru_maxrss > 2LL * 1024 * 1024 * 1024 ||
                Clock::now() - started > std::chrono::seconds(120)) {
                std::cerr << "native residual/PB edge probe resource ceiling exceeded\n";
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
    const auto shape = coordinate_source.shape();
    if (shape != IPosition(4, 512, 512, 1, 1))
        throw std::runtime_error("unexpected retained shape");
    auto edge_weight = read_fixture(input, "edges_weight0");
    const Float cutoff = 0.0001f;
    const Float intermediate = max(edge_weight) * 0.000001f;
    edge_weight(16, 25) = intermediate;
    edge_weight(16, 26) = -intermediate;
    NativeStore store((output / "native").string(), coordinate_source.coordinates(),
                      shape, "T51 synthetic native residual and PB support probe",
                      Record(), 1, false, 2, true);
    store.weight(0)->put(edge_weight.reform(shape));
    std::array<Float, 2> sumwt{};
    for (unsigned term = 0; term < 2; ++term) {
        PagedImage<Float> source(std::string(argv[2]) + ".sumwt.tt" + std::to_string(term));
        sumwt[term] = source.getAt(IPosition(4, 0, 0, 0, 0));
        store.sumwt(term)->set(sumwt[term]);
    }
    const Float pbscale = Float(store.getPbMax());
    const std::array<Float, 4> scalars{cutoff, pbscale, sumwt[0], sumwt[1]};
    std::ofstream scalar_output(output / "scalars.f32le", std::ios::binary);
    scalar_output.write(reinterpret_cast<const char*>(scalars.data()), sizeof(scalars));
    scalar_output.close();
    if (!scalar_output) throw std::runtime_error("scalar export failed");
    export_plane(output, "weight0", edge_weight);
    std::cout << std::setprecision(17)
              << "synthetic_residual_pb\tinput_fixture=" << input
              << "\tpblimit_float=" << cutoff << "\tpbscale_float=" << pbscale
              << "\tpbscale_bits=" << std::bit_cast<std::uint32_t>(pbscale)
              << "\tsumwt0_float=" << sumwt[0] << "\tsumwt1_float=" << sumwt[1] << '\n';

    // Actual PB generation precedes residual completion, so native residuals
    // also acquire the same PB pixel mask that the image-store pipeline uses.
    store.normalizePrimaryBeam(cutoff);
    const auto native_pb = get_plane(*store.pb(0));
    const auto native_pb_mask = get_image_mask(*store.pb(0));
    export_plane(output, "native_pb0", native_pb);
    export_plane(output, "native_pb_mask", native_pb_mask);
    if (!(native_pb(16, 25) > cutoff && native_pb(16, 25) <= std::sqrt(cutoff)))
        throw std::runtime_error("native intermediate PB does not separate the requested cutoffs");
    std::cout << "native_pb\tpbscale_after_method=" << store.last_pb_scale()
              << "\tdefault_mask=" << store.pb(0)->getDefaultMask()
              << "\tintermediate_positive=" << native_pb(16, 25)
              << "\tintermediate_negative=" << native_pb(16, 26) << '\n';

    std::array<Matrix<Float>, 2> raw{Matrix<Float>(512, 512), Matrix<Float>(512, 512)};
    raw[0] = 0.125f;
    raw[1] = -0.0625f;
    for (unsigned term = 0; term < 2; ++term)
        export_plane(output, "residual_input" + std::to_string(term), raw[term]);
    for (const std::string mode : {"flatnoise", "flatsky"}) {
        for (unsigned term = 0; term < 2; ++term)
            store.residual(term)->put(raw[term].reform(shape));
        store.divideResidualByWeight(cutoff, mode);
        std::cout << "native_residual\tmode=" << mode
                  << "\tpbscale_after_method=" << store.last_pb_scale() << '\n';
        for (unsigned term = 0; term < 2; ++term) {
            const auto completed = get_plane(*store.residual(term));
            const auto pixel_mask = get_image_mask(*store.residual(term));
            Matrix<Float> support(512, 512);
            for (int x = 0; x < 512; ++x)
                for (int y = 0; y < 512; ++y)
                    support(x, y) = completed(x, y) != 0 ? 1.0f : 0.0f;
            export_plane(output, mode + "_residual" + std::to_string(term), completed);
            export_plane(output, mode + "_support" + std::to_string(term), support);
            export_plane(output, mode + "_pixel_mask" + std::to_string(term), pixel_mask);
            summarize(mode + "_residual", term, completed);
            for (int y = 16; y <= 26; ++y)
                std::cout << "edge\tmode=" << mode << "\tterm=" << term << "\tx=16\ty=" << y
                          << "\tweight=" << edge_weight(16, y) << "\tnative_pb=" << native_pb(16, y)
                          << "\tpb_mask=" << native_pb_mask(16, y) << "\traw_input=" << raw[term](16, y)
                          << "\tnative_residual=" << completed(16, y) << "\tpayload_support=" << support(16, y)
                          << "\tresidual_pixel_mask=" << pixel_mask(16, y) << '\n';
        }
    }
    store.releaseLocks();
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::cout << "complete\twall_seconds="
              << std::chrono::duration<double>(Clock::now() - started).count()
              << "\tpeak_rss_bytes=" << usage.ru_maxrss << '\n' << std::flush;
    return 0;
} catch (const std::exception& error) {
    std::cerr << "native residual/PB edge probe failed: " << error.what() << '\n';
    return 1;
}
