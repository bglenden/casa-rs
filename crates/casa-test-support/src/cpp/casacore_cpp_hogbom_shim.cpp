// SPDX-License-Identifier: LGPL-3.0-or-later
#include "casacore_cpp_common.h"

#include <algorithm>
#include <cmath>
#include <vector>

using casacore_shim::make_error;
static casacore_shim::TerminateGuard g_terminate_guard_hogbom;

extern "C" {
void hclean_(
    float* limage,
    float* limagestep,
    float* lpsf,
    int* domask,
    float* lmask,
    int* nx,
    int* ny,
    int* npol,
    int* xbeg,
    int* xend,
    int* ybeg,
    int* yend,
    int* niter,
    int* siter,
    int* iter,
    float* gain,
    float* thres,
    float* cspeedup,
    void* msgput,
    void* stopnow
);
}

namespace {
void hogbom_msgput(int*, int*, int*, int*, int*, float*) {}

void hogbom_stopnow(int* yes) {
    *yes = 0;
}
} // namespace

extern "C" int cpp_hogbom_clean_minor_cycle_2d(
    int nx,
    int ny,
    float gain,
    float threshold,
    int cycle_niter,
    const float* psf_in,
    const float* residual_in,
    float* model_out,
    float* residual_out,
    int max_len,
    int* iterdone_out,
    float* peak_out,
    char** out_error
) {
    try {
        if (
            nx <= 0 || ny <= 0 || cycle_niter < 0 || !psf_in || !residual_in || !model_out
            || !residual_out || !iterdone_out || !peak_out || max_len < nx * ny
        ) {
            if (out_error) *out_error = make_error("invalid hogbom shim buffers");
            return 1;
        }

        const int npix = nx * ny;
        std::vector<float> model(npix, 0.0f);
        std::vector<float> residual(residual_in, residual_in + npix);
        std::vector<float> psf(psf_in, psf_in + npix);
        std::vector<float> mask(npix, 1.0f);

        int domask = 0;
        int npol = 1;
        int xbeg = 1;
        int xend = nx;
        int ybeg = 1;
        int yend = ny;
        int siter = 0;
        int iter = 0;
        float cycle_speedup = -1.0f;
        hclean_(
            model.data(),
            residual.data(),
            psf.data(),
            &domask,
            mask.data(),
            &nx,
            &ny,
            &npol,
            &xbeg,
            &xend,
            &ybeg,
            &yend,
            &cycle_niter,
            &siter,
            &iter,
            &gain,
            &threshold,
            &cycle_speedup,
            reinterpret_cast<void*>(&hogbom_msgput),
            reinterpret_cast<void*>(&hogbom_stopnow)
        );

        float peak = 0.0f;
        for (float value : residual) {
            peak = std::max(peak, std::abs(value));
        }

        std::copy(model.begin(), model.end(), model_out);
        std::copy(residual.begin(), residual.end(), residual_out);
        *iterdone_out = iter;
        *peak_out = peak;
        return 0;
    } catch (const std::exception& err) {
        if (out_error) *out_error = make_error(err.what());
        return 1;
    } catch (...) {
        if (out_error) *out_error = make_error("unknown C++ exception");
        return 1;
    }
}
