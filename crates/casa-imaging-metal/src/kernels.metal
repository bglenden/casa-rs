// SPDX-License-Identifier: LGPL-3.0-or-later
//
// The kernel contract of plan section 5.3 on the device. The host has
// already located every sample (GridGeometry::locate), checked its support
// against the tile, and computed each visibility polarization's inverse
// kernel norm and sumwt; the kernels spread, gather and form residuals.
//
// The lanes serving one sample stride over the taps of its support, so any
// support size runs on the same code: one thread per sample for small
// supports, one SIMD group per sample for wide ones (`Params::lanes`, chosen
// by the host). Grids are interleaved complex f32 in the accumulator layout
// [plane][pol][term][y][x], updated with relaxed atomic adds.

#include <metal_stdlib>
using namespace metal;

#define MAX_TERMS 16
#define MAX_POLS 4

struct Sample {
    uint2 origin;      // first tap, relative to the accumulator tile
    ushort2 fine;      // fine-offset rows ox, oy
    uint plane;        // accumulator-local plane
    uint model_plane;  // model-local plane
    uint table;        // kernel table
    float spectral;    // Taylor variable
    uint flags;        // bit 0: gridding w > 0; bit 1: prediction w > 0
    float2 gradient;   // pointing phase gradient, radians per cell
};

struct Table {
    uint offset;            // first value in the separable or dense arena
    ushort2 support;        // taps per axis
    ushort oversampling;
    ushort mueller_planes;
    uint dense;             // 0: separable real rows; 1: dense complex tiles
};

struct Params {
    uint samples;
    uint npol;
    uint gpols;
    uint terms;             // terms per (plane, pol) of the accumulator
    uint term_base;         // first term of the mode
    uint term_count;        // terms of the mode (data terms for prediction)
    uint model_terms;       // terms per (plane, pol) of the model
    uint weighted;          // 1: spread the weight (PSF, weight image)
    uint write_residual;    // 1: hand the residual samples back
    uint lanes;             // threads per sample: 1, or the SIMD width
    uint batch_offset;      // batch index of a native prediction's first sample
    uint native;            // 1: predict into the batch / subtract the native model
    uint add;               // 1: the native model adds to the cells
    uint stride;            // thread t serves sample t * stride mod samples; 0: sample t
    uint2 tile_shape;
    uint2 tile_origin;
    uint2 grid_shape;
    char adjoint[2][16];    // [w > 0][gpol * 4 + vpol]: Mueller plane or -1
    char forward[2][16];
};

#define NO_CELL 0xffffffffu

// The model at one native cell from a batch of predictions (`NativeTerm`).
struct NativeTerm {
    uint cell;
    uint low;
    uint high;
    float fraction;
    uint flags;             // bit vpol: flagged, the model stays zero
};

// A sample's native endpoints (`NativeSource`).
struct NativeSource {
    uint2 cells;
    float2 factors;
    float2 scale;           // W·e^{iφ}
};

static inline float2 cmul(float2 a, float2 b) {
    return float2(a.x * b.x - a.y * b.y, a.x * b.y + a.y * b.x);
}

static inline float2 cconj(float2 a) {
    return float2(a.x, -a.y);
}

// tap' = (w > 0 ? conj(t) : t) · e^{i(kx gx + ky gy)}, k from the kernel centre
// (CASA conjugates the kernel of a positive-w sample: `wprojgrid.f`,
// `AWVisResampler::DataToGrid`),
// plus the sample's fine offset (off − s/2)/s in cells: the ramp is anchored at
// the sample, as CASA reads the ramped kernel at ix·sampling + off.
static inline float2 tap_value(Table t, device const float *rows, device const float2 *dense,
                               Sample s, uint mueller, uint ix, uint iy, bool w_positive) {
    if (t.dense == 0) {
        uint support = t.support.x;
        float wx = rows[t.offset + uint(s.fine.x) * support + ix];
        float wy = rows[t.offset + uint(s.fine.y) * support + iy];
        return float2(wx * wy, 0.0f);
    }
    uint sx = t.support.x;
    uint sy = t.support.y;
    ulong tile = (ulong(s.fine.y) * ulong(t.oversampling + 1) + ulong(s.fine.x))
                     * ulong(t.mueller_planes) + ulong(mueller);
    float2 tap = dense[ulong(t.offset) + tile * ulong(sx * sy) + ulong(iy * sx + ix)];
    if (w_positive) {
        tap = cconj(tap);
    }
    if (s.gradient.x != 0.0f || s.gradient.y != 0.0f) {
        float mid = float(t.oversampling) * 0.5f;
        float kx = float(int(ix) - int(sx / 2)) + (float(s.fine.x) - mid) / float(t.oversampling);
        float ky = float(int(iy) - int(sy / 2)) + (float(s.fine.y) - mid) / float(t.oversampling);
        float phase = kx * s.gradient.x + ky * s.gradient.y;
        tap = cmul(tap, float2(precise::cos(phase), precise::sin(phase)));
    }
    return tap;
}

static inline uint support_y(Table t) {
    return t.dense == 0 ? t.support.x : t.support.y;
}

static inline void spectral_powers(float spectral, uint count, thread float *powers) {
    float power = 1.0f;
    for (uint term = 0; term < count; ++term) {
        powers[term] = power;
        power *= spectral;
    }
}

#define MAX_ROUTES 16

// The distinct (grid polarization, Mueller plane) routes of one Mueller
// table row set, and which visibility polarizations each carries: the taps
// of a route depend on neither visibility polarization, so a gather or a
// spread through it serves every polarization it carries (both
// correlations of Stokes I through plane 0).
struct Routes {
    uint count;
    uchar gpol[MAX_ROUTES];
    uchar mueller[MAX_ROUTES];
    uchar vpols[MAX_ROUTES];  // bit vpol: the route carries vpol
};

static inline Routes routes_of(constant char *table, uint gpols, uint npol) {
    Routes r;
    r.count = 0;
    for (uint gpol = 0; gpol < gpols; ++gpol) {
        for (uint vpol = 0; vpol < npol; ++vpol) {
            int mueller = table[gpol * 4 + vpol];
            if (mueller < 0) {
                continue;
            }
            uint found = r.count;
            for (uint i = 0; i < r.count; ++i) {
                if (r.gpol[i] == gpol && r.mueller[i] == uint(mueller)) {
                    found = i;
                    break;
                }
            }
            if (found == r.count) {
                r.gpol[found] = uchar(gpol);
                r.mueller[found] = uchar(mueller);
                r.vpols[found] = 0;
                r.count += 1;
            }
            r.vpols[found] |= uchar(1u << vpol);
        }
    }
    return r;
}

// Spread `values` (one per visibility polarization) of sample `s` over its
// support through the adjoint table, lanes striding over the taps. The
// values a route carries are summed first, skipping zero weights, so each
// route reads its taps and adds to the grid once.
static inline void spread_values(Sample s, Table t, thread const float2 *values,
                                 device const float *weights, uint index,
                                 device const float *rows, device const float2 *dense,
                                 device atomic_float *grid, constant Params &p,
                                 uint lane, uint width) {
    float powers[MAX_TERMS];
    spectral_powers(s.spectral, p.term_count, powers);
    uint sx = t.support.x;
    uint taps = sx * support_y(t);
    uint w_positive = s.flags & 1u;
    Routes routes = routes_of(p.adjoint[w_positive], p.gpols, p.npol);
    float2 carried[MAX_ROUTES];
    uint used = 0;
    for (uint r = 0; r < routes.count; ++r) {
        float2 sum = float2(0.0f);
        bool any = false;
        for (uint vpol = 0; vpol < p.npol; ++vpol) {
            if ((routes.vpols[r] >> vpol) & 1u && weights[index * p.npol + vpol] != 0.0f) {
                sum += values[vpol];
                any = true;
            }
        }
        if (any) {
            carried[used] = sum;
            routes.gpol[used] = routes.gpol[r];
            routes.mueller[used] = routes.mueller[r];
            used += 1;
        }
    }
    ulong tile_cells = ulong(p.tile_shape.x) * ulong(p.tile_shape.y);
    for (uint k = lane; k < taps; k += width) {
        uint ix = k % sx;
        uint iy = k / sx;
        ulong cell = ulong(s.origin.y + iy) * ulong(p.tile_shape.x) + ulong(s.origin.x + ix);
        for (uint r = 0; r < used; ++r) {
            uint gpol = routes.gpol[r];
            float2 contribution = cmul(carried[r], tap_value(t, rows, dense, s, routes.mueller[r], ix, iy, w_positive != 0u));
            for (uint term = 0; term < p.term_count; ++term) {
                float2 value = contribution * powers[term];
                ulong block = ulong((s.plane * p.gpols + gpol) * p.terms + p.term_base + term);
                ulong at = 2 * (block * tile_cells + cell);
                atomic_fetch_add_explicit(&grid[at], value.x, memory_order_relaxed);
                atomic_fetch_add_explicit(&grid[at + 1], value.y, memory_order_relaxed);
            }
        }
    }
}

// The prediction numerator Σ_t s^t Σ_{gpol,m} Σ conj(tap') · model of every
// visibility polarization, summed over the sample's lanes and scaled by the
// host's inverse norm; every lane receives the result. Each route is
// gathered once and added to every polarization it carries.
static inline void gather(Sample s, Table t, device const float2 *inverse_norms, uint index,
                          device const float *rows, device const float2 *dense,
                          device const float2 *model, constant Params &p,
                          uint lane, uint width, thread float2 *predicted) {
    float powers[MAX_TERMS];
    spectral_powers(s.spectral, p.term_count, powers);
    float2 sums[MAX_POLS] = {float2(0.0f), float2(0.0f), float2(0.0f), float2(0.0f)};
    uint sx = t.support.x;
    uint taps = sx * support_y(t);
    uint w_positive = (s.flags >> 1u) & 1u;
    Routes routes = routes_of(p.forward[w_positive], p.gpols, p.npol);
    ulong grid_cells = ulong(p.grid_shape.x) * ulong(p.grid_shape.y);
    for (uint k = lane; k < taps; k += width) {
        uint ix = k % sx;
        uint iy = k / sx;
        ulong cell = ulong(s.origin.y + p.tile_origin.y + iy) * ulong(p.grid_shape.x)
                     + ulong(s.origin.x + p.tile_origin.x + ix);
        for (uint r = 0; r < routes.count; ++r) {
            uint gpol = routes.gpol[r];
            float2 tap = cconj(tap_value(t, rows, dense, s, routes.mueller[r], ix, iy, w_positive != 0u));
            float2 gathered = float2(0.0f);
            for (uint term = 0; term < p.term_count; ++term) {
                ulong block = ulong((s.model_plane * p.gpols + gpol) * p.model_terms + term);
                gathered += cmul(tap, model[block * grid_cells + cell]) * powers[term];
            }
            for (uint vpol = 0; vpol < p.npol; ++vpol) {
                if ((routes.vpols[r] >> vpol) & 1u) {
                    sums[vpol] += gathered;
                }
            }
        }
    }
    for (uint vpol = 0; vpol < p.npol; ++vpol) {
        // `lanes` is uniform over the dispatch, so a SIMD group either
        // serves one sample and reduces it, or serves one per lane.
        float2 sum = p.lanes == 1u ? sums[vpol] : simd_sum(sums[vpol]);
        predicted[vpol] = cmul(sum, inverse_norms[index * p.npol + vpol]);
    }
}

// The sample a thread serves, its lane among the sample's threads and their
// count: one thread per sample when `p.lanes` is 1, else one SIMD group.
struct Lane {
    uint index;
    uint lane;
    uint width;
};

// With `p.stride` (coprime to `p.samples`), the k-th sample slot serves
// sample k * stride mod samples, a bijection that sends neighbouring slots to
// samples far apart: neighbouring samples (adjacent channels and
// integrations of one baseline) spread nearly the same cells, and on GPUs
// whose float atomics retry under contention (Apple M1/M2) slots hitting the
// same cells at once serialize (#716).
static inline Lane lane_of(constant Params &p, uint position, uint group, uint simdgroup,
                           uint simdgroups, uint simd_lane, uint simd_width) {
    Lane l;
    if (p.lanes == 1u) {
        l.index = position;
        l.lane = 0;
        l.width = 1;
    } else {
        l.index = group * simdgroups + simdgroup;
        l.lane = simd_lane;
        l.width = simd_width;
    }
    if (p.stride != 0u && l.index < p.samples) {
        l.index = uint((ulong(l.index) * ulong(p.stride)) % ulong(p.samples));
    }
    return l;
}

kernel void spread(device const Sample *samples [[buffer(0)]],
                   device const float2 *values [[buffer(1)]],
                   device const float *weights [[buffer(2)]],
                   device const Table *tables [[buffer(3)]],
                   device const float *rows [[buffer(4)]],
                   device const float2 *dense [[buffer(5)]],
                   device atomic_float *grid [[buffer(6)]],
                   constant Params &p [[buffer(7)]],
                   device const NativeSource *sources [[buffer(8)]],
                   device const float2 *native_model [[buffer(9)]],
                   uint position [[thread_position_in_grid]],
                   uint group [[threadgroup_position_in_grid]],
                   uint simdgroup [[simdgroup_index_in_threadgroup]],
                   uint simdgroups [[simdgroups_per_threadgroup]],
                   uint simd_lane [[thread_index_in_simdgroup]],
                   uint simd_width [[threads_per_simdgroup]]) {
    Lane l = lane_of(p, position, group, simdgroup, simdgroups, simd_lane, simd_width);
    uint index = l.index;
    if (index >= p.samples) {
        return;
    }
    Sample s = samples[index];
    float2 spread[MAX_POLS];
    for (uint vpol = 0; vpol < p.npol; ++vpol) {
        spread[vpol] = p.weighted != 0u ? float2(weights[index * p.npol + vpol], 0.0f)
                                        : values[index * p.npol + vpol];
    }
    if (p.native != 0u) {
        // The observed value less the model at the same native endpoints,
        // with the same factors and W·e^{iφ}: a residual at native channels.
        NativeSource source = sources[index];
        for (uint vpol = 0; vpol < p.npol; ++vpol) {
            float2 model = float2(0.0f);
            for (uint end = 0; end < 2; ++end) {
                uint cell = end == 0 ? source.cells.x : source.cells.y;
                if (cell != NO_CELL) {
                    float factor = end == 0 ? source.factors.x : source.factors.y;
                    model += native_model[ulong(cell) * p.npol + vpol] * factor;
                }
            }
            spread[vpol] -= cmul(source.scale, model);
        }
    }
    spread_values(s, tables[s.table], spread, weights, index, rows, dense, grid, p, l.lane, l.width);
}

// One thread per native term: the model at its cell from the batch's
// predictions, written, or added for a later image domain.
kernel void native_model(device const NativeTerm *terms [[buffer(0)]],
                         constant Params &p [[buffer(7)]],
                         device const float2 *predicted [[buffer(8)]],
                         device float2 *model [[buffer(9)]],
                         uint index [[thread_position_in_grid]]) {
    if (index >= p.samples) {
        return;
    }
    NativeTerm t = terms[index];
    for (uint vpol = 0; vpol < p.npol; ++vpol) {
        float2 m = float2(0.0f);
        if (((t.flags >> vpol) & 1u) == 0u) {
            float2 a = t.low == NO_CELL ? float2(0.0f) : predicted[ulong(t.low) * p.npol + vpol];
            if (t.low == t.high) {
                m = a;
            } else {
                float2 b = t.high == NO_CELL ? float2(0.0f)
                                             : predicted[ulong(t.high) * p.npol + vpol];
                m = a + (b - a) * t.fraction;
            }
        }
        ulong at = ulong(t.cell) * p.npol + vpol;
        model[at] = p.add != 0u ? model[at] + m : m;
    }
}

kernel void predict(device const Sample *samples [[buffer(0)]],
                    device const float2 *inverse_norms [[buffer(1)]],
                    device const float2 *phasors [[buffer(2)]],
                    device const Table *tables [[buffer(3)]],
                    device const float *rows [[buffer(4)]],
                    device const float2 *dense [[buffer(5)]],
                    device const float2 *model [[buffer(6)]],
                    device float2 *predicted [[buffer(8)]],
                    constant Params &p [[buffer(7)]],
                    uint position [[thread_position_in_grid]],
                    uint group [[threadgroup_position_in_grid]],
                    uint simdgroup [[simdgroup_index_in_threadgroup]],
                    uint simdgroups [[simdgroups_per_threadgroup]],
                    uint simd_lane [[thread_index_in_simdgroup]],
                    uint simd_width [[threads_per_simdgroup]]) {
    Lane l = lane_of(p, position, group, simdgroup, simdgroups, simd_lane, simd_width);
    uint index = l.index;
    if (index >= p.samples) {
        return;
    }
    Sample s = samples[index];
    float2 result[MAX_POLS];
    gather(s, tables[s.table], inverse_norms, index, rows, dense, model, p, l.lane, l.width, result);
    if (l.lane == 0) {
        if (p.native != 0u) {
            // A native prediction stays on the device for `native_model`:
            // the whole batch in one buffer, phase-centre phasor applied
            // (computed on the host in f64).
            ulong at = ulong(p.batch_offset + index) * p.npol;
            for (uint vpol = 0; vpol < p.npol; ++vpol) {
                predicted[at + vpol] = cmul(result[vpol], phasors[index]);
            }
        } else {
            for (uint vpol = 0; vpol < p.npol; ++vpol) {
                predicted[index * p.npol + vpol] = result[vpol];
            }
        }
    }
}

kernel void residual(device const Sample *samples [[buffer(0)]],
                     device const float2 *values [[buffer(1)]],
                     device const float *weights [[buffer(2)]],
                     device const Table *tables [[buffer(3)]],
                     device const float *rows [[buffer(4)]],
                     device const float2 *dense [[buffer(5)]],
                     device atomic_float *grid [[buffer(6)]],
                     constant Params &p [[buffer(7)]],
                     device float2 *residuals [[buffer(8)]],
                     device const float2 *model [[buffer(9)]],
                     device const float2 *inverse_norms [[buffer(10)]],
                     uint position [[thread_position_in_grid]],
                     uint group [[threadgroup_position_in_grid]],
                     uint simdgroup [[simdgroup_index_in_threadgroup]],
                     uint simdgroups [[simdgroups_per_threadgroup]],
                     uint simd_lane [[thread_index_in_simdgroup]],
                     uint simd_width [[threads_per_simdgroup]]) {
    Lane l = lane_of(p, position, group, simdgroup, simdgroups, simd_lane, simd_width);
    uint index = l.index;
    if (index >= p.samples) {
        return;
    }
    Sample s = samples[index];
    Table t = tables[s.table];
    float2 residual[MAX_POLS];
    gather(s, t, inverse_norms, index, rows, dense, model, p, l.lane, l.width, residual);
    for (uint vpol = 0; vpol < p.npol; ++vpol) {
        residual[vpol] = values[index * p.npol + vpol]
                         - residual[vpol] * weights[index * p.npol + vpol];
    }
    if (p.write_residual != 0u && l.lane == 0) {
        for (uint vpol = 0; vpol < p.npol; ++vpol) {
            residuals[index * p.npol + vpol] = residual[vpol];
        }
    }
    spread_values(s, t, residual, weights, index, rows, dense, grid, p, l.lane, l.width);
}
