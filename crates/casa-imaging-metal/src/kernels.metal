// SPDX-License-Identifier: LGPL-3.0-or-later
//
// The kernel contract of plan section 5.3 on the device. The host has
// already located every sample (GridGeometry::locate), checked its support
// against the tile, and computed each visibility polarization's inverse
// kernel norm and sumwt; the kernels spread, gather and form residuals.
//
// One SIMD group serves one sample: its lanes stride over the taps of the
// support, so any support size runs on the same code. Grids are interleaved
// complex f32 in the accumulator layout [plane][pol][term][y][x], updated
// with relaxed atomic adds.

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
    uint flags;        // bit 0: w > 0
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
    uint pad;
    uint2 tile_shape;
    uint2 tile_origin;
    uint2 grid_shape;
    char adjoint[2][16];    // [w > 0][gpol * 4 + vpol]: Mueller plane or -1
    char forward[2][16];
};

static inline float2 cmul(float2 a, float2 b) {
    return float2(a.x * b.x - a.y * b.y, a.x * b.y + a.y * b.x);
}

static inline float2 cconj(float2 a) {
    return float2(a.x, -a.y);
}

// tap' = (w > 0 ? t : conj(t)) · e^{i(kx gx + ky gy)}, k from the kernel centre
// plus the sample's fine offset (off − s/2)/s in cells: the ramp is anchored at
// the sample, as CASA reads the ramped kernel at ix·sampling + off.
static inline float2 tap_value(Table t, device const float *rows, device const float2 *dense,
                               Sample s, uint mueller, uint ix, uint iy) {
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
    if ((s.flags & 1u) == 0u) {
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

// Spread `values` (one per visibility polarization) of sample `s` over its
// support through the adjoint table, lanes striding over the taps.
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
    ulong tile_cells = ulong(p.tile_shape.x) * ulong(p.tile_shape.y);
    for (uint k = lane; k < taps; k += width) {
        uint ix = k % sx;
        uint iy = k / sx;
        ulong cell = ulong(s.origin.y + iy) * ulong(p.tile_shape.x) + ulong(s.origin.x + ix);
        for (uint gpol = 0; gpol < p.gpols; ++gpol) {
            for (uint vpol = 0; vpol < p.npol; ++vpol) {
                int mueller = p.adjoint[w_positive][gpol * 4 + vpol];
                if (mueller < 0 || weights[index * p.npol + vpol] == 0.0f) {
                    continue;
                }
                float2 contribution = cmul(values[vpol], tap_value(t, rows, dense, s, uint(mueller), ix, iy));
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
}

// The prediction numerator Σ_t s^t Σ_{gpol,m} Σ conj(tap') · model of every
// visibility polarization, summed over the SIMD group and scaled by the
// host's inverse norm; every lane receives the result.
static inline void gather(Sample s, Table t, device const float2 *inverse_norms, uint index,
                          device const float *rows, device const float2 *dense,
                          device const float2 *model, constant Params &p,
                          uint lane, uint width, thread float2 *predicted) {
    float powers[MAX_TERMS];
    spectral_powers(s.spectral, p.term_count, powers);
    float2 sums[MAX_POLS] = {float2(0.0f), float2(0.0f), float2(0.0f), float2(0.0f)};
    uint sx = t.support.x;
    uint taps = sx * support_y(t);
    uint w_positive = s.flags & 1u;
    ulong grid_cells = ulong(p.grid_shape.x) * ulong(p.grid_shape.y);
    for (uint k = lane; k < taps; k += width) {
        uint ix = k % sx;
        uint iy = k / sx;
        ulong cell = ulong(s.origin.y + p.tile_origin.y + iy) * ulong(p.grid_shape.x)
                     + ulong(s.origin.x + p.tile_origin.x + ix);
        for (uint gpol = 0; gpol < p.gpols; ++gpol) {
            for (uint vpol = 0; vpol < p.npol; ++vpol) {
                int mueller = p.forward[w_positive][gpol * 4 + vpol];
                if (mueller < 0) {
                    continue;
                }
                float2 tap = cconj(tap_value(t, rows, dense, s, uint(mueller), ix, iy));
                for (uint term = 0; term < p.term_count; ++term) {
                    ulong block = ulong((s.model_plane * p.gpols + gpol) * p.model_terms + term);
                    sums[vpol] += cmul(tap, model[block * grid_cells + cell]) * powers[term];
                }
            }
        }
    }
    for (uint vpol = 0; vpol < p.npol; ++vpol) {
        predicted[vpol] = cmul(simd_sum(sums[vpol]), inverse_norms[index * p.npol + vpol]);
    }
}

kernel void spread(device const Sample *samples [[buffer(0)]],
                   device const float2 *values [[buffer(1)]],
                   device const float *weights [[buffer(2)]],
                   device const Table *tables [[buffer(3)]],
                   device const float *rows [[buffer(4)]],
                   device const float2 *dense [[buffer(5)]],
                   device atomic_float *grid [[buffer(6)]],
                   constant Params &p [[buffer(7)]],
                   uint group [[threadgroup_position_in_grid]],
                   uint simdgroup [[simdgroup_index_in_threadgroup]],
                   uint simdgroups [[simdgroups_per_threadgroup]],
                   uint lane [[thread_index_in_simdgroup]],
                   uint width [[threads_per_simdgroup]]) {
    uint index = group * simdgroups + simdgroup;
    if (index >= p.samples) {
        return;
    }
    Sample s = samples[index];
    float2 spread[MAX_POLS];
    for (uint vpol = 0; vpol < p.npol; ++vpol) {
        spread[vpol] = p.weighted != 0u ? float2(weights[index * p.npol + vpol], 0.0f)
                                        : values[index * p.npol + vpol];
    }
    spread_values(s, tables[s.table], spread, weights, index, rows, dense, grid, p, lane, width);
}

kernel void predict(device const Sample *samples [[buffer(0)]],
                    device const float2 *inverse_norms [[buffer(1)]],
                    device const Table *tables [[buffer(3)]],
                    device const float *rows [[buffer(4)]],
                    device const float2 *dense [[buffer(5)]],
                    device const float2 *model [[buffer(6)]],
                    device float2 *predicted [[buffer(8)]],
                    constant Params &p [[buffer(7)]],
                    uint group [[threadgroup_position_in_grid]],
                    uint simdgroup [[simdgroup_index_in_threadgroup]],
                    uint simdgroups [[simdgroups_per_threadgroup]],
                    uint lane [[thread_index_in_simdgroup]],
                    uint width [[threads_per_simdgroup]]) {
    uint index = group * simdgroups + simdgroup;
    if (index >= p.samples) {
        return;
    }
    Sample s = samples[index];
    float2 result[MAX_POLS];
    gather(s, tables[s.table], inverse_norms, index, rows, dense, model, p, lane, width, result);
    if (lane == 0) {
        for (uint vpol = 0; vpol < p.npol; ++vpol) {
            predicted[index * p.npol + vpol] = result[vpol];
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
                     uint group [[threadgroup_position_in_grid]],
                     uint simdgroup [[simdgroup_index_in_threadgroup]],
                     uint simdgroups [[simdgroups_per_threadgroup]],
                     uint lane [[thread_index_in_simdgroup]],
                     uint width [[threads_per_simdgroup]]) {
    uint index = group * simdgroups + simdgroup;
    if (index >= p.samples) {
        return;
    }
    Sample s = samples[index];
    Table t = tables[s.table];
    float2 residual[MAX_POLS];
    gather(s, t, inverse_norms, index, rows, dense, model, p, lane, width, residual);
    for (uint vpol = 0; vpol < p.npol; ++vpol) {
        residual[vpol] = values[index * p.npol + vpol]
                         - residual[vpol] * weights[index * p.npol + vpol];
    }
    if (p.write_residual != 0u && lane == 0) {
        for (uint vpol = 0; vpol < p.npol; ++vpol) {
            residuals[index * p.npol + vpol] = residual[vpol];
        }
    }
    spread_values(s, t, residual, weights, index, rows, dense, grid, p, lane, width);
}
