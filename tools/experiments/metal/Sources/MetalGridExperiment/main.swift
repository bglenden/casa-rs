// SPDX-License-Identifier: LGPL-3.0-or-later

import Dispatch
import Foundation
import Metal

private let shaderSource = """
#include <metal_stdlib>
using namespace metal;

struct MetalGridSample {
    uint center_x;
    uint center_y;
    ushort kernel_u;
    ushort kernel_v;
    ushort support_id;
    ushort grid_plane;
    ushort flags;
    float weight;
    float visibility_re;
    float visibility_im;
};

struct ExperimentParams {
    uint sample_count;
    uint width;
    uint height;
    uint support;
    uint tap_count;
};

struct GridContribution {
    uint cell_index;
    float value_re;
    float value_im;
    uint order;
};

struct ReduceParams {
    uint active_cell_count;
};

struct TileParams {
    uint tile_count;
    uint width;
    uint height;
    uint support;
    uint tap_count;
    uint tile_edge;
    uint tiles_y;
    uint tile_halo_edge;
};

struct TileCellBinParams {
    uint active_cell_count;
    uint sample_count;
    uint tile_count;
    uint width;
    uint height;
    uint support;
    uint tap_count;
    uint tile_edge;
    uint tiles_y;
    uint tile_halo_edge;
};

struct PrefixParams {
    uint element_count;
    uint step;
};

static inline void atomic_add_float(device atomic_uint *address, float value) {
    uint old_bits = atomic_load_explicit(address, memory_order_relaxed);
    while (true) {
        float old_value = as_type<float>(old_bits);
        uint new_bits = as_type<uint>(old_value + value);
        if (atomic_compare_exchange_weak_explicit(
                address,
                &old_bits,
                new_bits,
                memory_order_relaxed,
                memory_order_relaxed)) {
            return;
        }
    }
}

kernel void grid_global_atomic(
    device const MetalGridSample *samples [[buffer(0)]],
    device const float *taps [[buffer(1)]],
    device atomic_uint *grid_re [[buffer(2)]],
    device atomic_uint *grid_im [[buffer(3)]],
    constant ExperimentParams &params [[buffer(4)]],
    uint gid [[thread_position_in_grid]]
) {
    if (gid >= params.sample_count) {
        return;
    }

    MetalGridSample sample = samples[gid];
    for (uint dx = 0; dx < params.tap_count; dx++) {
        int x = int(sample.center_x) + int(dx) - int(params.support);
        if (x < 0 || x >= int(params.width)) {
            continue;
        }
        float wx = taps[dx];
        for (uint dy = 0; dy < params.tap_count; dy++) {
            int y = int(sample.center_y) + int(dy) - int(params.support);
            if (y < 0 || y >= int(params.height)) {
                continue;
            }
            float tap_weight = wx * taps[dy] * sample.weight;
            uint cell = uint(x) * params.height + uint(y);
            atomic_add_float(&grid_re[cell], sample.visibility_re * tap_weight);
            atomic_add_float(&grid_im[cell], sample.visibility_im * tap_weight);
        }
    }
}

kernel void residual_refresh_global_atomic(
    device const MetalGridSample *samples [[buffer(0)]],
    device const float *taps [[buffer(1)]],
    device const float *model_re [[buffer(2)]],
    device const float *model_im [[buffer(3)]],
    device atomic_uint *grid_re [[buffer(4)]],
    device atomic_uint *grid_im [[buffer(5)]],
    constant ExperimentParams &params [[buffer(6)]],
    uint gid [[thread_position_in_grid]]
) {
    if (gid >= params.sample_count) {
        return;
    }

    MetalGridSample sample = samples[gid];
    float predicted_re = 0.0f;
    float predicted_im = 0.0f;
    for (uint dx = 0; dx < params.tap_count; dx++) {
        int x = int(sample.center_x) + int(dx) - int(params.support);
        if (x < 0 || x >= int(params.width)) {
            continue;
        }
        float wx = taps[dx];
        for (uint dy = 0; dy < params.tap_count; dy++) {
            int y = int(sample.center_y) + int(dy) - int(params.support);
            if (y < 0 || y >= int(params.height)) {
                continue;
            }
            float tap_weight = wx * taps[dy];
            uint cell = uint(x) * params.height + uint(y);
            predicted_re += model_re[cell] * tap_weight;
            predicted_im += model_im[cell] * tap_weight;
        }
    }

    float residual_re = sample.visibility_re - predicted_re;
    float residual_im = sample.visibility_im - predicted_im;
    for (uint dx = 0; dx < params.tap_count; dx++) {
        int x = int(sample.center_x) + int(dx) - int(params.support);
        if (x < 0 || x >= int(params.width)) {
            continue;
        }
        float wx = taps[dx];
        for (uint dy = 0; dy < params.tap_count; dy++) {
            int y = int(sample.center_y) + int(dy) - int(params.support);
            if (y < 0 || y >= int(params.height)) {
                continue;
            }
            float tap_weight = wx * taps[dy] * sample.weight;
            uint cell = uint(x) * params.height + uint(y);
            atomic_add_float(&grid_re[cell], residual_re * tap_weight);
            atomic_add_float(&grid_im[cell], residual_im * tap_weight);
        }
    }
}

kernel void grid_cell_owner(
    device const MetalGridSample *samples [[buffer(0)]],
    device const float *taps [[buffer(1)]],
    device float *grid_re [[buffer(2)]],
    device float *grid_im [[buffer(3)]],
    constant ExperimentParams &params [[buffer(4)]],
    uint gid [[thread_position_in_grid]]
) {
    uint cell_count = params.width * params.height;
    if (gid >= cell_count) {
        return;
    }

    uint x = gid / params.height;
    uint y = gid - x * params.height;
    float sum_re = 0.0f;
    float sum_im = 0.0f;

    for (uint sample_index = 0; sample_index < params.sample_count; sample_index++) {
        MetalGridSample sample = samples[sample_index];
        int dx = int(x) - int(sample.center_x) + int(params.support);
        int dy = int(y) - int(sample.center_y) + int(params.support);
        if (dx < 0 || dy < 0 || dx >= int(params.tap_count) || dy >= int(params.tap_count)) {
            continue;
        }
        float tap_weight = taps[uint(dx)] * taps[uint(dy)] * sample.weight;
        sum_re += sample.visibility_re * tap_weight;
        sum_im += sample.visibility_im * tap_weight;
    }

    grid_re[gid] = sum_re;
    grid_im[gid] = sum_im;
}

kernel void grid_sorted_reduce(
    device const GridContribution *contributions [[buffer(0)]],
    device const uint *active_cells [[buffer(1)]],
    device const uint *offsets [[buffer(2)]],
    device float *grid_re [[buffer(3)]],
    device float *grid_im [[buffer(4)]],
    constant ReduceParams &params [[buffer(5)]],
    uint gid [[thread_position_in_grid]]
) {
    if (gid >= params.active_cell_count) {
        return;
    }

    uint start = offsets[gid];
    uint end = offsets[gid + 1];
    uint cell = active_cells[gid];
    float sum_re = 0.0f;
    float sum_im = 0.0f;
    for (uint index = start; index < end; index++) {
        GridContribution contribution = contributions[index];
        sum_re += contribution.value_re;
        sum_im += contribution.value_im;
    }
    grid_re[cell] = sum_re;
    grid_im[cell] = sum_im;
}

kernel void grid_tile_bucket_cell_owner(
    device const MetalGridSample *samples [[buffer(0)]],
    device const uint *tile_offsets [[buffer(1)]],
    device const float *taps [[buffer(2)]],
    device float *tile_re [[buffer(3)]],
    device float *tile_im [[buffer(4)]],
    constant TileParams &params [[buffer(5)]],
    uint gid [[thread_position_in_grid]]
) {
    uint halo_cells = params.tile_halo_edge * params.tile_halo_edge;
    uint total_cells = params.tile_count * halo_cells;
    if (gid >= total_cells) {
        return;
    }

    uint tile_id = gid / halo_cells;
    uint local_cell = gid - tile_id * halo_cells;
    uint local_x = local_cell / params.tile_halo_edge;
    uint local_y = local_cell - local_x * params.tile_halo_edge;
    uint tile_x = tile_id / params.tiles_y;
    uint tile_y = tile_id - tile_x * params.tiles_y;
    int global_x = int(tile_x * params.tile_edge + local_x) - int(params.support);
    int global_y = int(tile_y * params.tile_edge + local_y) - int(params.support);

    if (global_x < 0 || global_y < 0 ||
        global_x >= int(params.width) || global_y >= int(params.height)) {
        tile_re[gid] = 0.0f;
        tile_im[gid] = 0.0f;
        return;
    }

    uint start = tile_offsets[tile_id];
    uint end = tile_offsets[tile_id + 1];
    float sum_re = 0.0f;
    float sum_im = 0.0f;
    for (uint sample_index = start; sample_index < end; sample_index++) {
        MetalGridSample sample = samples[sample_index];
        int dx = global_x - int(sample.center_x) + int(params.support);
        int dy = global_y - int(sample.center_y) + int(params.support);
        if (dx < 0 || dy < 0 || dx >= int(params.tap_count) || dy >= int(params.tap_count)) {
            continue;
        }
        float tap_weight = taps[uint(dx)] * taps[uint(dy)] * sample.weight;
        sum_re += sample.visibility_re * tap_weight;
        sum_im += sample.visibility_im * tap_weight;
    }

    tile_re[gid] = sum_re;
    tile_im[gid] = sum_im;
}

kernel void grid_tile_cell_bins(
    device const MetalGridSample *samples [[buffer(0)]],
    device const uint *active_tile_cells [[buffer(1)]],
    device const uint *cell_offsets [[buffer(2)]],
    device const uint *sample_indices [[buffer(3)]],
    device const float *taps [[buffer(4)]],
    device float *tile_re [[buffer(5)]],
    device float *tile_im [[buffer(6)]],
    constant TileCellBinParams &params [[buffer(7)]],
    uint gid [[thread_position_in_grid]]
) {
    if (gid >= params.active_cell_count) {
        return;
    }

    uint halo_cells = params.tile_halo_edge * params.tile_halo_edge;
    uint tile_cell = active_tile_cells[gid];
    uint tile_id = tile_cell / halo_cells;
    uint local_cell = tile_cell - tile_id * halo_cells;
    uint local_x = local_cell / params.tile_halo_edge;
    uint local_y = local_cell - local_x * params.tile_halo_edge;
    uint tile_x = tile_id / params.tiles_y;
    uint tile_y = tile_id - tile_x * params.tiles_y;
    int global_x = int(tile_x * params.tile_edge + local_x) - int(params.support);
    int global_y = int(tile_y * params.tile_edge + local_y) - int(params.support);

    uint start = cell_offsets[gid];
    uint end = cell_offsets[gid + 1];
    float sum_re = 0.0f;
    float sum_im = 0.0f;
    for (uint index = start; index < end; index++) {
        MetalGridSample sample = samples[sample_indices[index]];
        int dx = global_x - int(sample.center_x) + int(params.support);
        int dy = global_y - int(sample.center_y) + int(params.support);
        if (dx < 0 || dy < 0 || dx >= int(params.tap_count) || dy >= int(params.tap_count)) {
            continue;
        }
        float tap_weight = taps[uint(dx)] * taps[uint(dy)] * sample.weight;
        sum_re += sample.visibility_re * tap_weight;
        sum_im += sample.visibility_im * tap_weight;
    }

    tile_re[tile_cell] = sum_re;
    tile_im[tile_cell] = sum_im;
}

kernel void count_tile_cell_bins(
    device const MetalGridSample *samples [[buffer(0)]],
    device atomic_uint *counts [[buffer(1)]],
    constant TileCellBinParams &params [[buffer(2)]],
    uint gid [[thread_position_in_grid]]
) {
    if (gid >= params.sample_count) {
        return;
    }

    MetalGridSample sample = samples[gid];
    uint tile_x = sample.center_x / params.tile_edge;
    uint tile_y = sample.center_y / params.tile_edge;
    uint tile_id = tile_x * params.tiles_y + tile_y;
    uint halo_cells = params.tile_halo_edge * params.tile_halo_edge;

    for (uint dx = 0; dx < params.tap_count; dx++) {
        int global_x = int(sample.center_x) + int(dx) - int(params.support);
        if (global_x < 0 || global_x >= int(params.width)) {
            continue;
        }
        uint local_x = uint(global_x - int(tile_x * params.tile_edge) + int(params.support));
        for (uint dy = 0; dy < params.tap_count; dy++) {
            int global_y = int(sample.center_y) + int(dy) - int(params.support);
            if (global_y < 0 || global_y >= int(params.height)) {
                continue;
            }
            uint local_y = uint(global_y - int(tile_y * params.tile_edge) + int(params.support));
            uint tile_cell = tile_id * halo_cells + local_x * params.tile_halo_edge + local_y;
            atomic_fetch_add_explicit(&counts[tile_cell], 1u, memory_order_relaxed);
        }
    }
}

kernel void fill_tile_cell_bins(
    device const MetalGridSample *samples [[buffer(0)]],
    device const uint *cell_slots [[buffer(1)]],
    device const uint *cell_offsets [[buffer(2)]],
    device atomic_uint *fill_counts [[buffer(3)]],
    device uint *sample_indices [[buffer(4)]],
    constant TileCellBinParams &params [[buffer(5)]],
    uint gid [[thread_position_in_grid]]
) {
    if (gid >= params.sample_count) {
        return;
    }

    MetalGridSample sample = samples[gid];
    uint tile_x = sample.center_x / params.tile_edge;
    uint tile_y = sample.center_y / params.tile_edge;
    uint tile_id = tile_x * params.tiles_y + tile_y;
    uint halo_cells = params.tile_halo_edge * params.tile_halo_edge;

    for (uint dx = 0; dx < params.tap_count; dx++) {
        int global_x = int(sample.center_x) + int(dx) - int(params.support);
        if (global_x < 0 || global_x >= int(params.width)) {
            continue;
        }
        uint local_x = uint(global_x - int(tile_x * params.tile_edge) + int(params.support));
        for (uint dy = 0; dy < params.tap_count; dy++) {
            int global_y = int(sample.center_y) + int(dy) - int(params.support);
            if (global_y < 0 || global_y >= int(params.height)) {
                continue;
            }
            uint local_y = uint(global_y - int(tile_y * params.tile_edge) + int(params.support));
            uint tile_cell = tile_id * halo_cells + local_x * params.tile_halo_edge + local_y;
            uint slot = cell_slots[tile_cell];
            if (slot == 0xffffffffu) {
                continue;
            }
            uint output = cell_offsets[slot] +
                atomic_fetch_add_explicit(&fill_counts[slot], 1u, memory_order_relaxed);
            sample_indices[output] = gid;
        }
    }
}

kernel void prefix_scan_step(
    device const uint *input [[buffer(0)]],
    device uint *output [[buffer(1)]],
    constant PrefixParams &params [[buffer(2)]],
    uint gid [[thread_position_in_grid]]
) {
    if (gid >= params.element_count) {
        return;
    }
    uint value = input[gid];
    if (gid >= params.step) {
        value += input[gid - params.step];
    }
    output[gid] = value;
}

kernel void fill_tile_cell_bins_all(
    device const MetalGridSample *samples [[buffer(0)]],
    device const uint *inclusive_prefix [[buffer(1)]],
    device atomic_uint *fill_counts [[buffer(2)]],
    device uint *sample_indices [[buffer(3)]],
    constant TileCellBinParams &params [[buffer(4)]],
    uint gid [[thread_position_in_grid]]
) {
    if (gid >= params.sample_count) {
        return;
    }

    MetalGridSample sample = samples[gid];
    uint tile_x = sample.center_x / params.tile_edge;
    uint tile_y = sample.center_y / params.tile_edge;
    uint tile_id = tile_x * params.tiles_y + tile_y;
    uint halo_cells = params.tile_halo_edge * params.tile_halo_edge;

    for (uint dx = 0; dx < params.tap_count; dx++) {
        int global_x = int(sample.center_x) + int(dx) - int(params.support);
        if (global_x < 0 || global_x >= int(params.width)) {
            continue;
        }
        uint local_x = uint(global_x - int(tile_x * params.tile_edge) + int(params.support));
        for (uint dy = 0; dy < params.tap_count; dy++) {
            int global_y = int(sample.center_y) + int(dy) - int(params.support);
            if (global_y < 0 || global_y >= int(params.height)) {
                continue;
            }
            uint local_y = uint(global_y - int(tile_y * params.tile_edge) + int(params.support));
            uint tile_cell = tile_id * halo_cells + local_x * params.tile_halo_edge + local_y;
            uint start = tile_cell == 0 ? 0 : inclusive_prefix[tile_cell - 1];
            uint output = start +
                atomic_fetch_add_explicit(&fill_counts[tile_cell], 1u, memory_order_relaxed);
            sample_indices[output] = gid;
        }
    }
}

kernel void grid_tile_cell_bins_all(
    device const MetalGridSample *samples [[buffer(0)]],
    device const uint *inclusive_prefix [[buffer(1)]],
    device const uint *sample_indices [[buffer(2)]],
    device const float *taps [[buffer(3)]],
    device float *tile_re [[buffer(4)]],
    device float *tile_im [[buffer(5)]],
    constant TileCellBinParams &params [[buffer(6)]],
    uint gid [[thread_position_in_grid]]
) {
    uint halo_cells = params.tile_halo_edge * params.tile_halo_edge;
    uint total_cells = params.tile_count * halo_cells;
    if (gid >= total_cells) {
        return;
    }

    uint tile_id = gid / halo_cells;
    uint local_cell = gid - tile_id * halo_cells;
    uint local_x = local_cell / params.tile_halo_edge;
    uint local_y = local_cell - local_x * params.tile_halo_edge;
    uint tile_x = tile_id / params.tiles_y;
    uint tile_y = tile_id - tile_x * params.tiles_y;
    int global_x = int(tile_x * params.tile_edge + local_x) - int(params.support);
    int global_y = int(tile_y * params.tile_edge + local_y) - int(params.support);

    if (global_x < 0 || global_y < 0 ||
        global_x >= int(params.width) || global_y >= int(params.height)) {
        tile_re[gid] = 0.0f;
        tile_im[gid] = 0.0f;
        return;
    }

    uint start = gid == 0 ? 0 : inclusive_prefix[gid - 1];
    uint end = inclusive_prefix[gid];
    float sum_re = 0.0f;
    float sum_im = 0.0f;
    for (uint index = start; index < end; index++) {
        MetalGridSample sample = samples[sample_indices[index]];
        int dx = global_x - int(sample.center_x) + int(params.support);
        int dy = global_y - int(sample.center_y) + int(params.support);
        if (dx < 0 || dy < 0 || dx >= int(params.tap_count) || dy >= int(params.tap_count)) {
            continue;
        }
        float tap_weight = taps[uint(dx)] * taps[uint(dy)] * sample.weight;
        sum_re += sample.visibility_re * tap_weight;
        sum_im += sample.visibility_im * tap_weight;
    }

    tile_re[gid] = sum_re;
    tile_im[gid] = sum_im;
}
"""

private struct MetalGridSample {
    var centerX: UInt32
    var centerY: UInt32
    var kernelU: UInt16
    var kernelV: UInt16
    var supportId: UInt16
    var gridPlane: UInt16
    var flags: UInt16
    var weight: Float
    var visibilityRe: Float
    var visibilityIm: Float
}

private struct ExperimentParams {
    var sampleCount: UInt32
    var width: UInt32
    var height: UInt32
    var support: UInt32
    var tapCount: UInt32
}

private struct GridContribution {
    var cellIndex: UInt32
    var valueRe: Float
    var valueIm: Float
    var order: UInt32
}

private struct ReduceParams {
    var activeCellCount: UInt32
}

private struct TileParams {
    var tileCount: UInt32
    var width: UInt32
    var height: UInt32
    var support: UInt32
    var tapCount: UInt32
    var tileEdge: UInt32
    var tilesY: UInt32
    var tileHaloEdge: UInt32
}

private struct TileCellBinParams {
    var activeCellCount: UInt32
    var sampleCount: UInt32
    var tileCount: UInt32
    var width: UInt32
    var height: UInt32
    var support: UInt32
    var tapCount: UInt32
    var tileEdge: UInt32
    var tilesY: UInt32
    var tileHaloEdge: UInt32
}

private struct PrefixParams {
    var elementCount: UInt32
    var step: UInt32
}

private struct ReducedContributionPlan {
    var contributions: [GridContribution]
    var activeCells: [UInt32]
    var offsets: [UInt32]
    var prepareSeconds: Double
}

private struct TileBucketPlan {
    var samples: [MetalGridSample]
    var offsets: [UInt32]
    var tileCount: Int
    var tilesY: Int
    var tileEdge: Int
    var tileHaloEdge: Int
    var prepareSeconds: Double
}

private struct TileCellBinPlan {
    var activeTileCells: [UInt32]
    var cellOffsets: [UInt32]
    var sampleIndices: [UInt32]
    var prepareSeconds: Double
}

private struct GpuTileCellBinPrefix {
    var activeTileCells: [UInt32]
    var cellOffsets: [UInt32]
    var cellSlots: [UInt32]
    var sampleRefCount: Int
    var prefixSeconds: Double
}

private struct ComplexGrid {
    var re: [Float]
    var im: [Float]
}

private struct Metrics {
    let maxAbsError: Double
    let rmsError: Double
    let relativeRmsError: Double
}

private struct RunConfig {
    var samples: Int = 20_000
    var imsize: Int = 512
    var support: Int = 3
    var distribution: String = "uniform"
    var repeats: Int = 1
    var tileEdge: Int = 64
    var preparedSamplesJSON: String?
    var cellArcsec: Double = 1.0
    var skipSlowBaselines = false

    static func parse() throws -> RunConfig {
        var config = RunConfig()
        var args = Array(CommandLine.arguments.dropFirst())
        while let arg = args.first {
            args.removeFirst()
            func requireValue() throws -> String {
                guard let value = args.first else {
                    throw ExperimentError.invalidArgument("missing value after \(arg)")
                }
                args.removeFirst()
                return value
            }
            switch arg {
            case "--samples":
                config.samples = try Int(requireValue()).requirePositive(arg)
            case "--imsize":
                config.imsize = try Int(requireValue()).requirePositive(arg)
            case "--support":
                config.support = try Int(requireValue()).requirePositive(arg)
            case "--distribution":
                config.distribution = try requireValue()
            case "--repeats":
                config.repeats = try Int(requireValue()).requirePositive(arg)
            case "--tile-edge":
                config.tileEdge = try Int(requireValue()).requirePositive(arg)
            case "--prepared-samples-json":
                config.preparedSamplesJSON = try requireValue()
            case "--cell-arcsec":
                guard let value = Double(try requireValue()), value > 0 else {
                    throw ExperimentError.invalidArgument("--cell-arcsec must be a positive number")
                }
                config.cellArcsec = value
            case "--skip-slow-baselines":
                config.skipSlowBaselines = true
            case "--help", "-h":
                printUsageAndExit()
            default:
                throw ExperimentError.invalidArgument("unknown argument \(arg)")
            }
        }
        if !["uniform", "cluster", "boundary"].contains(config.distribution) {
            throw ExperimentError.invalidArgument(
                "--distribution must be one of uniform, cluster, boundary"
            )
        }
        if config.imsize <= 2 * config.support {
            throw ExperimentError.invalidArgument("--imsize must be larger than 2 * --support")
        }
        return config
    }
}

private enum ExperimentError: Error, CustomStringConvertible {
    case invalidArgument(String)
    case metalUnavailable
    case metalFailure(String)
    case fixtureFailure(String)

    var description: String {
        switch self {
        case .invalidArgument(let message):
            return "invalid argument: \(message)"
        case .metalUnavailable:
            return "no Metal device is available"
        case .metalFailure(let message):
            return "Metal failure: \(message)"
        case .fixtureFailure(let message):
            return "fixture failure: \(message)"
        }
    }
}

private struct PreparedVisibilitySampleTrace: Decodable {
    let imagingUVWm: [Double]
    let outputFrequencyHz: Double
    let visibilityRe: Float
    let visibilityIm: Float
    let weight: Float
    let sumwtFactor: Float
    let gridable: Bool

    private enum CodingKeys: String, CodingKey {
        case imagingUVWm = "imaging_uvw_m"
        case outputFrequencyHz = "output_frequency_hz"
        case visibilityRe = "visibility_re"
        case visibilityIm = "visibility_im"
        case weight
        case sumwtFactor = "sumwt_factor"
        case gridable
    }
}

private extension Int? {
    func requirePositive(_ name: String) throws -> Int {
        guard let value = self, value > 0 else {
            throw ExperimentError.invalidArgument("\(name) must be a positive integer")
        }
        return value
    }
}

private final class Lcg {
    private var state: UInt64

    init(seed: UInt64) {
        state = seed
    }

    func nextUInt32() -> UInt32 {
        state = state &* 6364136223846793005 &+ 1442695040888963407
        return UInt32(truncatingIfNeeded: state >> 32)
    }

    func nextFloat() -> Float {
        Float(nextUInt32()) / Float(UInt32.max)
    }

    func nextInt(_ upperBound: Int) -> Int {
        Int(nextUInt32() % UInt32(upperBound))
    }
}

private func printUsageAndExit() -> Never {
    print("""
    Usage:
      swift run MetalGridExperiment [--samples N] [--imsize N] [--support N] [--distribution uniform|cluster|boundary] [--tile-edge N] [--repeats N] [--skip-slow-baselines]
      swift run MetalGridExperiment --prepared-samples-json prepared_samples.json [--samples MAX] [--imsize N] [--cell-arcsec ARCSEC] [--skip-slow-baselines]
    """)
    exit(0)
}

private func makeTaps(support: Int) -> [Float] {
    let sigma = max(Float(support) * 0.62, 0.5)
    let values = (-support...support).map { offset -> Float in
        let x = Float(offset) / sigma
        return exp(-0.5 * x * x)
    }
    let sum = values.reduce(0, +)
    return values.map { $0 / sum }
}

private func makeSamples(config: RunConfig) -> [MetalGridSample] {
    let rng = Lcg(seed: 0xc0ffee_991b_0002)
    let low = config.support
    let high = config.imsize - config.support - 1
    let span = high - low + 1
    let center = config.imsize / 2
    let clusterRadius = max(2, config.imsize / 32)

    return (0..<config.samples).map { index in
        let x: Int
        let y: Int
        switch config.distribution {
        case "cluster":
            x = clamp(center + rng.nextInt(clusterRadius * 2 + 1) - clusterRadius, low, high)
            y = clamp(center + rng.nextInt(clusterRadius * 2 + 1) - clusterRadius, low, high)
        case "boundary":
            let edgeBand = max(1, config.support + 1)
            switch rng.nextInt(4) {
            case 0:
                x = low + rng.nextInt(edgeBand)
                y = low + rng.nextInt(span)
            case 1:
                x = high - rng.nextInt(edgeBand)
                y = low + rng.nextInt(span)
            case 2:
                x = low + rng.nextInt(span)
                y = low + rng.nextInt(edgeBand)
            default:
                x = low + rng.nextInt(span)
                y = high - rng.nextInt(edgeBand)
            }
        default:
            x = low + rng.nextInt(span)
            y = low + rng.nextInt(span)
        }

        let phase = Float(index % 1024) * 0.012271846
        let amp = Float(0.5 + rng.nextFloat())
        return MetalGridSample(
            centerX: UInt32(x),
            centerY: UInt32(y),
            kernelU: 0,
            kernelV: 0,
            supportId: 0,
            gridPlane: 0,
            flags: 1,
            weight: Float(0.25 + rng.nextFloat()),
            visibilityRe: cos(phase) * amp,
            visibilityIm: sin(phase) * amp
        )
    }
}

private func loadPreparedSamplesFixture(config: RunConfig) throws -> [MetalGridSample] {
    guard let path = config.preparedSamplesJSON else {
        return makeSamples(config: config)
    }
    let data = try Data(contentsOf: URL(fileURLWithPath: path))
    let traces: [PreparedVisibilitySampleTrace]
    do {
        traces = try JSONDecoder().decode([PreparedVisibilitySampleTrace].self, from: data)
    } catch {
        throw ExperimentError.fixtureFailure("decode \(path): \(error)")
    }

    let cellRad = config.cellArcsec * Double.pi / (180.0 * 3600.0)
    let duLambda = 1.0 / (Double(config.imsize) * cellRad)
    let dvLambda = duLambda
    let center = Double(config.imsize) / 2.0
    let maxSamples = config.samples
    var samples: [MetalGridSample] = []
    samples.reserveCapacity(min(maxSamples, traces.count))

    for trace in traces {
        if samples.count >= maxSamples {
            break
        }
        guard trace.gridable,
              trace.imagingUVWm.count >= 2,
              trace.outputFrequencyHz.isFinite,
              trace.outputFrequencyHz > 0,
              trace.weight.isFinite,
              trace.weight > 0,
              trace.sumwtFactor.isFinite,
              trace.sumwtFactor > 0,
              trace.visibilityRe.isFinite,
              trace.visibilityIm.isFinite
        else {
            continue
        }

        let lambdaScale = trace.outputFrequencyHz / 299_792_458.0
        let uLambda = trace.imagingUVWm[0] * lambdaScale
        let vLambda = trace.imagingUVWm[1] * lambdaScale
        let xAnchor = Int((uLambda / duLambda + center).rounded())
        let yAnchor = Int((-vLambda / dvLambda + center).rounded())
        let startX = xAnchor - config.support
        let startY = yAnchor - config.support
        let endX = xAnchor + config.support
        let endY = yAnchor + config.support
        if startX < 0 || startY < 0 || endX >= config.imsize || endY >= config.imsize {
            continue
        }

        samples.append(
            MetalGridSample(
                centerX: UInt32(xAnchor),
                centerY: UInt32(yAnchor),
                kernelU: 0,
                kernelV: 0,
                supportId: 0,
                gridPlane: 0,
                flags: 1,
                weight: trace.weight * trace.sumwtFactor,
                visibilityRe: trace.visibilityRe,
                visibilityIm: trace.visibilityIm
            )
        )
    }
    if samples.isEmpty {
        throw ExperimentError.fixtureFailure(
            "no gridable samples from \(path); try a larger --imsize or larger --cell-arcsec"
        )
    }
    return samples
}

private func clamp(_ value: Int, _ low: Int, _ high: Int) -> Int {
    min(max(value, low), high)
}

private func cpuReference(samples: [MetalGridSample], taps: [Float], config: RunConfig) -> ComplexGrid {
    var grid = ComplexGrid(
        re: Array(repeating: 0, count: config.imsize * config.imsize),
        im: Array(repeating: 0, count: config.imsize * config.imsize)
    )
    let tapCount = 2 * config.support + 1
    for sample in samples {
        for dx in 0..<tapCount {
            let x = Int(sample.centerX) + dx - config.support
            if x < 0 || x >= config.imsize {
                continue
            }
            for dy in 0..<tapCount {
                let y = Int(sample.centerY) + dy - config.support
                if y < 0 || y >= config.imsize {
                    continue
                }
                let weight = taps[dx] * taps[dy] * sample.weight
                let index = x * config.imsize + y
                grid.re[index] += sample.visibilityRe * weight
                grid.im[index] += sample.visibilityIm * weight
            }
        }
    }
    return grid
}

private func makeModelGrid(config: RunConfig) -> ComplexGrid {
    var grid = ComplexGrid(
        re: Array(repeating: 0, count: config.imsize * config.imsize),
        im: Array(repeating: 0, count: config.imsize * config.imsize)
    )
    let center = Float(config.imsize - 1) * 0.5
    let scale = max(Float(config.imsize) * 0.19, 1)
    for x in 0..<config.imsize {
        let fx = (Float(x) - center) / scale
        for y in 0..<config.imsize {
            let fy = (Float(y) - center) / scale
            let envelope = exp(-0.5 * (fx * fx + fy * fy))
            let ripple = sin(Float(x) * 0.073) * cos(Float(y) * 0.041)
            let index = x * config.imsize + y
            grid.re[index] = 0.025 * envelope * (1.0 + 0.2 * ripple)
            grid.im[index] = 0.0125 * envelope * ripple
        }
    }
    return grid
}

private func cpuResidualRefreshReference(
    samples: [MetalGridSample],
    taps: [Float],
    model: ComplexGrid,
    config: RunConfig
) -> ComplexGrid {
    var grid = ComplexGrid(
        re: Array(repeating: 0, count: config.imsize * config.imsize),
        im: Array(repeating: 0, count: config.imsize * config.imsize)
    )
    let tapCount = 2 * config.support + 1
    for sample in samples {
        var predictedRe: Float = 0
        var predictedIm: Float = 0
        for dx in 0..<tapCount {
            let x = Int(sample.centerX) + dx - config.support
            if x < 0 || x >= config.imsize {
                continue
            }
            for dy in 0..<tapCount {
                let y = Int(sample.centerY) + dy - config.support
                if y < 0 || y >= config.imsize {
                    continue
                }
                let tapWeight = taps[dx] * taps[dy]
                let index = x * config.imsize + y
                predictedRe += model.re[index] * tapWeight
                predictedIm += model.im[index] * tapWeight
            }
        }
        let residualRe = sample.visibilityRe - predictedRe
        let residualIm = sample.visibilityIm - predictedIm
        for dx in 0..<tapCount {
            let x = Int(sample.centerX) + dx - config.support
            if x < 0 || x >= config.imsize {
                continue
            }
            for dy in 0..<tapCount {
                let y = Int(sample.centerY) + dy - config.support
                if y < 0 || y >= config.imsize {
                    continue
                }
                let weight = taps[dx] * taps[dy] * sample.weight
                let index = x * config.imsize + y
                grid.re[index] += residualRe * weight
                grid.im[index] += residualIm * weight
            }
        }
    }
    return grid
}

private func makeReducedContributionPlan(
    samples: [MetalGridSample],
    taps: [Float],
    config: RunConfig
) throws -> ReducedContributionPlan {
    let started = DispatchTime.now().uptimeNanoseconds
    let tapCount = 2 * config.support + 1
    let totalContributions = samples.count * tapCount * tapCount
    if totalContributions > UInt32.max {
        throw ExperimentError.metalFailure("too many expanded tap contributions")
    }
    var contributions: [GridContribution] = []
    contributions.reserveCapacity(totalContributions)
    var order: UInt32 = 0
    for sample in samples {
        for dx in 0..<tapCount {
            let x = Int(sample.centerX) + dx - config.support
            if x < 0 || x >= config.imsize {
                continue
            }
            for dy in 0..<tapCount {
                let y = Int(sample.centerY) + dy - config.support
                if y < 0 || y >= config.imsize {
                    continue
                }
                let weight = taps[dx] * taps[dy] * sample.weight
                let cellIndex = UInt32(x * config.imsize + y)
                contributions.append(
                    GridContribution(
                        cellIndex: cellIndex,
                        valueRe: sample.visibilityRe * weight,
                        valueIm: sample.visibilityIm * weight,
                        order: order
                    )
                )
                order += 1
            }
        }
    }
    contributions.sort {
        if $0.cellIndex == $1.cellIndex {
            return $0.order < $1.order
        }
        return $0.cellIndex < $1.cellIndex
    }

    var activeCells: [UInt32] = []
    var offsets: [UInt32] = [0]
    activeCells.reserveCapacity(min(contributions.count, config.imsize * config.imsize))
    var cursor = 0
    while cursor < contributions.count {
        let cell = contributions[cursor].cellIndex
        activeCells.append(cell)
        repeat {
            cursor += 1
        } while cursor < contributions.count && contributions[cursor].cellIndex == cell
        offsets.append(UInt32(cursor))
    }

    let ended = DispatchTime.now().uptimeNanoseconds
    return ReducedContributionPlan(
        contributions: contributions,
        activeCells: activeCells,
        offsets: offsets,
        prepareSeconds: Double(ended - started) / 1_000_000_000.0
    )
}

private func makeTileBucketPlan(samples: [MetalGridSample], config: RunConfig) -> TileBucketPlan {
    let started = DispatchTime.now().uptimeNanoseconds
    let tilesX = config.imsize.ceilDiv(config.tileEdge)
    let tilesY = config.imsize.ceilDiv(config.tileEdge)
    let tileCount = tilesX * tilesY
    var counts = Array(repeating: 0, count: tileCount)
    for sample in samples {
        let tileX = min(Int(sample.centerX) / config.tileEdge, tilesX - 1)
        let tileY = min(Int(sample.centerY) / config.tileEdge, tilesY - 1)
        counts[tileX * tilesY + tileY] += 1
    }

    var offsets = Array(repeating: UInt32(0), count: tileCount + 1)
    for tile in 0..<tileCount {
        offsets[tile + 1] = offsets[tile] + UInt32(counts[tile])
    }
    var fill = offsets.map(Int.init)
    var bucketedSamples = Array(
        repeating: MetalGridSample(
            centerX: 0,
            centerY: 0,
            kernelU: 0,
            kernelV: 0,
            supportId: 0,
            gridPlane: 0,
            flags: 0,
            weight: 0,
            visibilityRe: 0,
            visibilityIm: 0
        ),
        count: samples.count
    )
    for sample in samples {
        let tileX = min(Int(sample.centerX) / config.tileEdge, tilesX - 1)
        let tileY = min(Int(sample.centerY) / config.tileEdge, tilesY - 1)
        let tile = tileX * tilesY + tileY
        let index = fill[tile]
        bucketedSamples[index] = sample
        fill[tile] += 1
    }
    let ended = DispatchTime.now().uptimeNanoseconds
    return TileBucketPlan(
        samples: bucketedSamples,
        offsets: offsets,
        tileCount: tileCount,
        tilesY: tilesY,
        tileEdge: config.tileEdge,
        tileHaloEdge: config.tileEdge + 2 * config.support,
        prepareSeconds: Double(ended - started) / 1_000_000_000.0
    )
}

private func makeTileCellBinPlan(tilePlan: TileBucketPlan, config: RunConfig) -> TileCellBinPlan {
    let started = DispatchTime.now().uptimeNanoseconds
    let tapCount = 2 * config.support + 1
    let haloCells = tilePlan.tileHaloEdge * tilePlan.tileHaloEdge
    let totalTileCells = tilePlan.tileCount * haloCells
    var counts = Array(repeating: 0, count: totalTileCells)
    var contributionCount = 0

    func visitTileCells(_ body: (Int, Int) -> Void) {
        for tile in 0..<tilePlan.tileCount {
            let tileX = tile / tilePlan.tilesY
            let tileY = tile - tileX * tilePlan.tilesY
            let sampleStart = Int(tilePlan.offsets[tile])
            let sampleEnd = Int(tilePlan.offsets[tile + 1])
            for sampleIndex in sampleStart..<sampleEnd {
                let sample = tilePlan.samples[sampleIndex]
                for dx in 0..<tapCount {
                    let globalX = Int(sample.centerX) + dx - config.support
                    if globalX < 0 || globalX >= config.imsize {
                        continue
                    }
                    let localX = globalX - tileX * tilePlan.tileEdge + config.support
                    for dy in 0..<tapCount {
                        let globalY = Int(sample.centerY) + dy - config.support
                        if globalY < 0 || globalY >= config.imsize {
                            continue
                        }
                        let localY = globalY - tileY * tilePlan.tileEdge + config.support
                        let tileCell = tile * haloCells + localX * tilePlan.tileHaloEdge + localY
                        body(tileCell, sampleIndex)
                    }
                }
            }
        }
    }

    visitTileCells { tileCell, _ in
        counts[tileCell] += 1
        contributionCount += 1
    }

    var activeTileCells: [UInt32] = []
    activeTileCells.reserveCapacity(counts.filter { $0 > 0 }.count)
    var cellOffsets: [UInt32] = [0]
    cellOffsets.reserveCapacity(activeTileCells.capacity + 1)
    var cellSlot = Array(repeating: -1, count: totalTileCells)
    var running = 0
    for (tileCell, count) in counts.enumerated() where count > 0 {
        cellSlot[tileCell] = activeTileCells.count
        activeTileCells.append(UInt32(tileCell))
        running += count
        cellOffsets.append(UInt32(running))
    }

    var fillOffsets = cellOffsets.map(Int.init)
    var sampleIndices = Array(repeating: UInt32(0), count: contributionCount)
    visitTileCells { tileCell, sampleIndex in
        let slot = cellSlot[tileCell]
        let outputIndex = fillOffsets[slot]
        sampleIndices[outputIndex] = UInt32(sampleIndex)
        fillOffsets[slot] += 1
    }

    let ended = DispatchTime.now().uptimeNanoseconds
    return TileCellBinPlan(
        activeTileCells: activeTileCells,
        cellOffsets: cellOffsets,
        sampleIndices: sampleIndices,
        prepareSeconds: Double(ended - started) / 1_000_000_000.0
    )
}

private func makeGpuTileCellBinPrefix(counts: [UInt32]) -> GpuTileCellBinPrefix {
    let started = DispatchTime.now().uptimeNanoseconds
    var activeTileCells: [UInt32] = []
    activeTileCells.reserveCapacity(counts.filter { $0 > 0 }.count)
    var cellSlots = Array(repeating: UInt32.max, count: counts.count)
    var cellOffsets: [UInt32] = [0]
    cellOffsets.reserveCapacity(activeTileCells.capacity + 1)
    var running = 0
    for (tileCell, count) in counts.enumerated() where count > 0 {
        cellSlots[tileCell] = UInt32(activeTileCells.count)
        activeTileCells.append(UInt32(tileCell))
        running += Int(count)
        cellOffsets.append(UInt32(running))
    }
    let ended = DispatchTime.now().uptimeNanoseconds
    return GpuTileCellBinPrefix(
        activeTileCells: activeTileCells,
        cellOffsets: cellOffsets,
        cellSlots: cellSlots,
        sampleRefCount: running,
        prefixSeconds: Double(ended - started) / 1_000_000_000.0
    )
}

private func compare(_ lhs: ComplexGrid, _ rhs: ComplexGrid) -> Metrics {
    var maxAbs = 0.0
    var sumSq = 0.0
    var refSq = 0.0
    for index in lhs.re.indices {
        let dre = Double(lhs.re[index] - rhs.re[index])
        let dim = Double(lhs.im[index] - rhs.im[index])
        let error = hypot(dre, dim)
        maxAbs = max(maxAbs, error)
        sumSq += dre * dre + dim * dim
        refSq += Double(lhs.re[index]) * Double(lhs.re[index])
            + Double(lhs.im[index]) * Double(lhs.im[index])
    }
    let n = Double(lhs.re.count)
    return Metrics(
        maxAbsError: maxAbs,
        rmsError: sqrt(sumSq / n),
        relativeRmsError: refSq > 0 ? sqrt(sumSq / refSq) : 0
    )
}

private extension Int {
    func ceilDiv(_ divisor: Int) -> Int {
        (self + divisor - 1) / divisor
    }
}

private func makeBuffer<T>(
    device: MTLDevice,
    array: [T],
    options: MTLResourceOptions = .storageModeShared
) throws -> MTLBuffer {
    try array.withUnsafeBytes { bytes in
        guard let base = bytes.baseAddress,
              let buffer = device.makeBuffer(bytes: base, length: bytes.count, options: options)
        else {
            throw ExperimentError.metalFailure("failed to allocate buffer")
        }
        return buffer
    }
}

private func makeBuffer<T>(
    device: MTLDevice,
    value: T,
    options: MTLResourceOptions = .storageModeShared
) throws -> MTLBuffer {
    var mutableValue = value
    return try withUnsafeBytes(of: &mutableValue) { bytes in
        guard let base = bytes.baseAddress,
              let buffer = device.makeBuffer(bytes: base, length: bytes.count, options: options)
        else {
            throw ExperimentError.metalFailure("failed to allocate parameter buffer")
        }
        return buffer
    }
}

private func makeEmptyBuffer(device: MTLDevice, bytes: Int) throws -> MTLBuffer {
    guard let buffer = device.makeBuffer(length: bytes, options: .storageModeShared) else {
        throw ExperimentError.metalFailure("failed to allocate output buffer")
    }
    memset(buffer.contents(), 0, bytes)
    return buffer
}

private func runKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    sampleBuffer: MTLBuffer,
    tapBuffer: MTLBuffer,
    reBuffer: MTLBuffer,
    imBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    threadCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(sampleBuffer, offset: 0, index: 0)
    encoder.setBuffer(tapBuffer, offset: 0, index: 1)
    encoder.setBuffer(reBuffer, offset: 0, index: 2)
    encoder.setBuffer(imBuffer, offset: 0, index: 3)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 4)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: threadCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func runResidualRefreshKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    sampleBuffer: MTLBuffer,
    tapBuffer: MTLBuffer,
    modelReBuffer: MTLBuffer,
    modelImBuffer: MTLBuffer,
    reBuffer: MTLBuffer,
    imBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    threadCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create residual refresh command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(sampleBuffer, offset: 0, index: 0)
    encoder.setBuffer(tapBuffer, offset: 0, index: 1)
    encoder.setBuffer(modelReBuffer, offset: 0, index: 2)
    encoder.setBuffer(modelImBuffer, offset: 0, index: 3)
    encoder.setBuffer(reBuffer, offset: 0, index: 4)
    encoder.setBuffer(imBuffer, offset: 0, index: 5)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 6)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: threadCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func runReduceKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    contributionBuffer: MTLBuffer,
    activeCellBuffer: MTLBuffer,
    offsetBuffer: MTLBuffer,
    reBuffer: MTLBuffer,
    imBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    activeCellCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create reduce command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(contributionBuffer, offset: 0, index: 0)
    encoder.setBuffer(activeCellBuffer, offset: 0, index: 1)
    encoder.setBuffer(offsetBuffer, offset: 0, index: 2)
    encoder.setBuffer(reBuffer, offset: 0, index: 3)
    encoder.setBuffer(imBuffer, offset: 0, index: 4)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 5)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: activeCellCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func runTileKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    sampleBuffer: MTLBuffer,
    offsetBuffer: MTLBuffer,
    tapBuffer: MTLBuffer,
    reBuffer: MTLBuffer,
    imBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    threadCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create tile command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(sampleBuffer, offset: 0, index: 0)
    encoder.setBuffer(offsetBuffer, offset: 0, index: 1)
    encoder.setBuffer(tapBuffer, offset: 0, index: 2)
    encoder.setBuffer(reBuffer, offset: 0, index: 3)
    encoder.setBuffer(imBuffer, offset: 0, index: 4)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 5)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: threadCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func runTileCellBinKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    sampleBuffer: MTLBuffer,
    activeCellBuffer: MTLBuffer,
    cellOffsetBuffer: MTLBuffer,
    sampleIndexBuffer: MTLBuffer,
    tapBuffer: MTLBuffer,
    reBuffer: MTLBuffer,
    imBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    activeCellCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create tile cell-bin command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(sampleBuffer, offset: 0, index: 0)
    encoder.setBuffer(activeCellBuffer, offset: 0, index: 1)
    encoder.setBuffer(cellOffsetBuffer, offset: 0, index: 2)
    encoder.setBuffer(sampleIndexBuffer, offset: 0, index: 3)
    encoder.setBuffer(tapBuffer, offset: 0, index: 4)
    encoder.setBuffer(reBuffer, offset: 0, index: 5)
    encoder.setBuffer(imBuffer, offset: 0, index: 6)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 7)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: activeCellCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func runTileCellBinCountKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    sampleBuffer: MTLBuffer,
    countBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    sampleCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create tile cell-bin count command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(sampleBuffer, offset: 0, index: 0)
    encoder.setBuffer(countBuffer, offset: 0, index: 1)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 2)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: sampleCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func runTileCellBinFillKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    sampleBuffer: MTLBuffer,
    cellSlotBuffer: MTLBuffer,
    cellOffsetBuffer: MTLBuffer,
    fillCountBuffer: MTLBuffer,
    sampleIndexBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    sampleCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create tile cell-bin fill command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(sampleBuffer, offset: 0, index: 0)
    encoder.setBuffer(cellSlotBuffer, offset: 0, index: 1)
    encoder.setBuffer(cellOffsetBuffer, offset: 0, index: 2)
    encoder.setBuffer(fillCountBuffer, offset: 0, index: 3)
    encoder.setBuffer(sampleIndexBuffer, offset: 0, index: 4)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 5)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: sampleCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func runPrefixScanStepKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    inputBuffer: MTLBuffer,
    outputBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    elementCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create prefix-scan command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(inputBuffer, offset: 0, index: 0)
    encoder.setBuffer(outputBuffer, offset: 0, index: 1)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 2)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: elementCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func runTileCellBinFillAllKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    sampleBuffer: MTLBuffer,
    prefixBuffer: MTLBuffer,
    fillCountBuffer: MTLBuffer,
    sampleIndexBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    sampleCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create all-cell fill command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(sampleBuffer, offset: 0, index: 0)
    encoder.setBuffer(prefixBuffer, offset: 0, index: 1)
    encoder.setBuffer(fillCountBuffer, offset: 0, index: 2)
    encoder.setBuffer(sampleIndexBuffer, offset: 0, index: 3)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 4)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: sampleCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func runTileCellBinReduceAllKernel(
    queue: MTLCommandQueue,
    pipeline: MTLComputePipelineState,
    sampleBuffer: MTLBuffer,
    prefixBuffer: MTLBuffer,
    sampleIndexBuffer: MTLBuffer,
    tapBuffer: MTLBuffer,
    reBuffer: MTLBuffer,
    imBuffer: MTLBuffer,
    paramsBuffer: MTLBuffer,
    tileCellCount: Int
) throws -> Double {
    guard let commandBuffer = queue.makeCommandBuffer(),
          let encoder = commandBuffer.makeComputeCommandEncoder()
    else {
        throw ExperimentError.metalFailure("failed to create all-cell reduce command buffer")
    }
    encoder.setComputePipelineState(pipeline)
    encoder.setBuffer(sampleBuffer, offset: 0, index: 0)
    encoder.setBuffer(prefixBuffer, offset: 0, index: 1)
    encoder.setBuffer(sampleIndexBuffer, offset: 0, index: 2)
    encoder.setBuffer(tapBuffer, offset: 0, index: 3)
    encoder.setBuffer(reBuffer, offset: 0, index: 4)
    encoder.setBuffer(imBuffer, offset: 0, index: 5)
    encoder.setBuffer(paramsBuffer, offset: 0, index: 6)

    let threadsPerGroup = min(pipeline.maxTotalThreadsPerThreadgroup, max(1, pipeline.threadExecutionWidth))
    encoder.dispatchThreads(
        MTLSize(width: tileCellCount, height: 1, depth: 1),
        threadsPerThreadgroup: MTLSize(width: threadsPerGroup, height: 1, depth: 1)
    )
    encoder.endEncoding()
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ExperimentError.metalFailure(error.localizedDescription)
    }
    return max(0, commandBuffer.gpuEndTime - commandBuffer.gpuStartTime)
}

private func readFloatGrid(reBuffer: MTLBuffer, imBuffer: MTLBuffer, count: Int) -> ComplexGrid {
    let rePtr = reBuffer.contents().bindMemory(to: Float.self, capacity: count)
    let imPtr = imBuffer.contents().bindMemory(to: Float.self, capacity: count)
    return ComplexGrid(
        re: (0..<count).map { rePtr[$0] },
        im: (0..<count).map { imPtr[$0] }
    )
}

private func readUInt32Array(buffer: MTLBuffer, count: Int) -> [UInt32] {
    let pointer = buffer.contents().bindMemory(to: UInt32.self, capacity: count)
    return (0..<count).map { pointer[$0] }
}

private func readAtomicFloatGrid(reBuffer: MTLBuffer, imBuffer: MTLBuffer, count: Int) -> ComplexGrid {
    let rePtr = reBuffer.contents().bindMemory(to: UInt32.self, capacity: count)
    let imPtr = imBuffer.contents().bindMemory(to: UInt32.self, capacity: count)
    return ComplexGrid(
        re: (0..<count).map { Float(bitPattern: rePtr[$0]) },
        im: (0..<count).map { Float(bitPattern: imPtr[$0]) }
    )
}

private func mergeTileGrid(reBuffer: MTLBuffer, imBuffer: MTLBuffer, plan: TileBucketPlan, config: RunConfig) -> ComplexGrid {
    var grid = ComplexGrid(
        re: Array(repeating: 0, count: config.imsize * config.imsize),
        im: Array(repeating: 0, count: config.imsize * config.imsize)
    )
    let cellsPerTile = plan.tileHaloEdge * plan.tileHaloEdge
    let rePtr = reBuffer.contents().bindMemory(to: Float.self, capacity: plan.tileCount * cellsPerTile)
    let imPtr = imBuffer.contents().bindMemory(to: Float.self, capacity: plan.tileCount * cellsPerTile)
    for tile in 0..<plan.tileCount {
        let tileX = tile / plan.tilesY
        let tileY = tile - tileX * plan.tilesY
        let base = tile * cellsPerTile
        for localX in 0..<plan.tileHaloEdge {
            let globalX = tileX * plan.tileEdge + localX - config.support
            if globalX < 0 || globalX >= config.imsize {
                continue
            }
            for localY in 0..<plan.tileHaloEdge {
                let globalY = tileY * plan.tileEdge + localY - config.support
                if globalY < 0 || globalY >= config.imsize {
                    continue
                }
                let localIndex = base + localX * plan.tileHaloEdge + localY
                let globalIndex = globalX * config.imsize + globalY
                grid.re[globalIndex] += rePtr[localIndex]
                grid.im[globalIndex] += imPtr[localIndex]
            }
        }
    }
    return grid
}

private func formatSeconds(_ value: Double) -> String {
    String(format: "%.6f", value)
}

private func formatMetric(_ value: Double) -> String {
    String(format: "%.6e", value)
}

private func main() throws {
    var config = try RunConfig.parse()
    guard MemoryLayout<MetalGridSample>.stride == 32 else {
        throw ExperimentError.metalFailure(
            "unexpected MetalGridSample stride \(MemoryLayout<MetalGridSample>.stride)"
        )
    }
    guard let device = MTLCreateSystemDefaultDevice() else {
        throw ExperimentError.metalUnavailable
    }
    guard let queue = device.makeCommandQueue() else {
        throw ExperimentError.metalFailure("failed to create command queue")
    }

    let library = try device.makeLibrary(source: shaderSource, options: nil)
    let globalFunction = library.makeFunction(name: "grid_global_atomic")!
    let residualRefreshFunction = library.makeFunction(name: "residual_refresh_global_atomic")!
    let cellOwnerFunction = library.makeFunction(name: "grid_cell_owner")!
    let reduceFunction = library.makeFunction(name: "grid_sorted_reduce")!
    let tileFunction = library.makeFunction(name: "grid_tile_bucket_cell_owner")!
    let tileCellBinFunction = library.makeFunction(name: "grid_tile_cell_bins")!
    let tileCellBinCountFunction = library.makeFunction(name: "count_tile_cell_bins")!
    let tileCellBinFillFunction = library.makeFunction(name: "fill_tile_cell_bins")!
    let prefixScanFunction = library.makeFunction(name: "prefix_scan_step")!
    let tileCellBinFillAllFunction = library.makeFunction(name: "fill_tile_cell_bins_all")!
    let tileCellBinReduceAllFunction = library.makeFunction(name: "grid_tile_cell_bins_all")!
    let globalPipeline = try device.makeComputePipelineState(function: globalFunction)
    let residualRefreshPipeline = try device.makeComputePipelineState(function: residualRefreshFunction)
    let cellOwnerPipeline = try device.makeComputePipelineState(function: cellOwnerFunction)
    let reducePipeline = try device.makeComputePipelineState(function: reduceFunction)
    let tilePipeline = try device.makeComputePipelineState(function: tileFunction)
    let tileCellBinPipeline = try device.makeComputePipelineState(function: tileCellBinFunction)
    let tileCellBinCountPipeline = try device.makeComputePipelineState(function: tileCellBinCountFunction)
    let tileCellBinFillPipeline = try device.makeComputePipelineState(function: tileCellBinFillFunction)
    let prefixScanPipeline = try device.makeComputePipelineState(function: prefixScanFunction)
    let tileCellBinFillAllPipeline = try device.makeComputePipelineState(function: tileCellBinFillAllFunction)
    let tileCellBinReduceAllPipeline = try device.makeComputePipelineState(function: tileCellBinReduceAllFunction)

    let prepareStart = DispatchTime.now().uptimeNanoseconds
    let samples = try loadPreparedSamplesFixture(config: config)
    if config.preparedSamplesJSON != nil {
        config.samples = samples.count
        config.distribution = "fixture"
    }
    let taps = makeTaps(support: config.support)
    let reference = cpuReference(samples: samples, taps: taps, config: config)
    let model = makeModelGrid(config: config)
    let residualReference = cpuResidualRefreshReference(
        samples: samples,
        taps: taps,
        model: model,
        config: config
    )
    let prepareEnd = DispatchTime.now().uptimeNanoseconds
    let reductionPlan = try makeReducedContributionPlan(samples: samples, taps: taps, config: config)
    let tilePlan = makeTileBucketPlan(samples: samples, config: config)
    let tileCellBinPlan = makeTileCellBinPlan(tilePlan: tilePlan, config: config)

    let params = ExperimentParams(
        sampleCount: UInt32(config.samples),
        width: UInt32(config.imsize),
        height: UInt32(config.imsize),
        support: UInt32(config.support),
        tapCount: UInt32(2 * config.support + 1)
    )
    let cellCount = config.imsize * config.imsize

    let uploadStart = DispatchTime.now().uptimeNanoseconds
    let sampleBuffer = try makeBuffer(device: device, array: samples)
    let tapBuffer = try makeBuffer(device: device, array: taps)
    let paramsBuffer = try makeBuffer(device: device, value: params)
    let modelReBuffer = try makeBuffer(device: device, array: model.re)
    let modelImBuffer = try makeBuffer(device: device, array: model.im)
    let contributionBuffer = try makeBuffer(device: device, array: reductionPlan.contributions)
    let activeCellBuffer = try makeBuffer(device: device, array: reductionPlan.activeCells)
    let offsetBuffer = try makeBuffer(device: device, array: reductionPlan.offsets)
    let reduceParamsBuffer = try makeBuffer(
        device: device,
        value: ReduceParams(activeCellCount: UInt32(reductionPlan.activeCells.count))
    )
    let tileSampleBuffer = try makeBuffer(device: device, array: tilePlan.samples)
    let tileOffsetBuffer = try makeBuffer(device: device, array: tilePlan.offsets)
    let tileParamsBuffer = try makeBuffer(
        device: device,
        value: TileParams(
            tileCount: UInt32(tilePlan.tileCount),
            width: UInt32(config.imsize),
            height: UInt32(config.imsize),
            support: UInt32(config.support),
            tapCount: UInt32(2 * config.support + 1),
            tileEdge: UInt32(tilePlan.tileEdge),
            tilesY: UInt32(tilePlan.tilesY),
            tileHaloEdge: UInt32(tilePlan.tileHaloEdge)
        )
    )
    let tileCellBinActiveBuffer = try makeBuffer(device: device, array: tileCellBinPlan.activeTileCells)
    let tileCellBinOffsetBuffer = try makeBuffer(device: device, array: tileCellBinPlan.cellOffsets)
    let tileCellBinSampleIndexBuffer = try makeBuffer(device: device, array: tileCellBinPlan.sampleIndices)
    let tileCellBinParamsBuffer = try makeBuffer(
        device: device,
        value: TileCellBinParams(
            activeCellCount: UInt32(tileCellBinPlan.activeTileCells.count),
            sampleCount: UInt32(config.samples),
            tileCount: UInt32(tilePlan.tileCount),
            width: UInt32(config.imsize),
            height: UInt32(config.imsize),
            support: UInt32(config.support),
            tapCount: UInt32(2 * config.support + 1),
            tileEdge: UInt32(tilePlan.tileEdge),
            tilesY: UInt32(tilePlan.tilesY),
            tileHaloEdge: UInt32(tilePlan.tileHaloEdge)
        )
    )
    let uploadEnd = DispatchTime.now().uptimeNanoseconds

    print("device=\(device.name)")
    print("config samples=\(config.samples) imsize=\(config.imsize) support=\(config.support) distribution=\(config.distribution) tile_edge=\(config.tileEdge) repeats=\(config.repeats)")
    if let preparedSamplesJSON = config.preparedSamplesJSON {
        print("fixture_prepared_samples_json=\(preparedSamplesJSON) cell_arcsec=\(config.cellArcsec)")
    }
    print("sample_stride_bytes=\(MemoryLayout<MetalGridSample>.stride)")
    print("host_prepare_s=\(formatSeconds(Double(prepareEnd - prepareStart) / 1_000_000_000.0))")
    print("sorted_reduce_prepare_s=\(formatSeconds(reductionPlan.prepareSeconds)) active_cells=\(reductionPlan.activeCells.count) contributions=\(reductionPlan.contributions.count)")
    print("tile_bucket_prepare_s=\(formatSeconds(tilePlan.prepareSeconds)) tile_count=\(tilePlan.tileCount) tile_halo_edge=\(tilePlan.tileHaloEdge)")
    print("tile_cell_bins_prepare_s=\(formatSeconds(tileCellBinPlan.prepareSeconds)) active_tile_cells=\(tileCellBinPlan.activeTileCells.count) sample_refs=\(tileCellBinPlan.sampleIndices.count)")
    print("upload_buffer_create_s=\(formatSeconds(Double(uploadEnd - uploadStart) / 1_000_000_000.0))")

    for repeatIndex in 0..<config.repeats {
        let atomicRe = try makeEmptyBuffer(device: device, bytes: cellCount * MemoryLayout<UInt32>.stride)
        let atomicIm = try makeEmptyBuffer(device: device, bytes: cellCount * MemoryLayout<UInt32>.stride)
        let globalGpuSeconds = try runKernel(
            queue: queue,
            pipeline: globalPipeline,
            sampleBuffer: sampleBuffer,
            tapBuffer: tapBuffer,
            reBuffer: atomicRe,
            imBuffer: atomicIm,
            paramsBuffer: paramsBuffer,
            threadCount: config.samples
        )
        let globalDownloadStart = DispatchTime.now().uptimeNanoseconds
        let globalGrid = readAtomicFloatGrid(reBuffer: atomicRe, imBuffer: atomicIm, count: cellCount)
        let globalDownloadEnd = DispatchTime.now().uptimeNanoseconds
        let globalMetrics = compare(reference, globalGrid)

        let residualAtomicRe = try makeEmptyBuffer(device: device, bytes: cellCount * MemoryLayout<UInt32>.stride)
        let residualAtomicIm = try makeEmptyBuffer(device: device, bytes: cellCount * MemoryLayout<UInt32>.stride)
        let residualGpuSeconds = try runResidualRefreshKernel(
            queue: queue,
            pipeline: residualRefreshPipeline,
            sampleBuffer: sampleBuffer,
            tapBuffer: tapBuffer,
            modelReBuffer: modelReBuffer,
            modelImBuffer: modelImBuffer,
            reBuffer: residualAtomicRe,
            imBuffer: residualAtomicIm,
            paramsBuffer: paramsBuffer,
            threadCount: config.samples
        )
        let residualDownloadStart = DispatchTime.now().uptimeNanoseconds
        let residualGrid = readAtomicFloatGrid(
            reBuffer: residualAtomicRe,
            imBuffer: residualAtomicIm,
            count: cellCount
        )
        let residualDownloadEnd = DispatchTime.now().uptimeNanoseconds
        let residualMetrics = compare(residualReference, residualGrid)

        let ownerResult: (gpuSeconds: Double, downloadSeconds: Double, metrics: Metrics)?
        if config.skipSlowBaselines {
            ownerResult = nil
        } else {
            let ownerRe = try makeEmptyBuffer(device: device, bytes: cellCount * MemoryLayout<Float>.stride)
            let ownerIm = try makeEmptyBuffer(device: device, bytes: cellCount * MemoryLayout<Float>.stride)
            let ownerGpuSeconds = try runKernel(
                queue: queue,
                pipeline: cellOwnerPipeline,
                sampleBuffer: sampleBuffer,
                tapBuffer: tapBuffer,
                reBuffer: ownerRe,
                imBuffer: ownerIm,
                paramsBuffer: paramsBuffer,
                threadCount: cellCount
            )
            let ownerDownloadStart = DispatchTime.now().uptimeNanoseconds
            let ownerGrid = readFloatGrid(reBuffer: ownerRe, imBuffer: ownerIm, count: cellCount)
            let ownerDownloadEnd = DispatchTime.now().uptimeNanoseconds
            ownerResult = (
                ownerGpuSeconds,
                Double(ownerDownloadEnd - ownerDownloadStart) / 1_000_000_000.0,
                compare(reference, ownerGrid)
            )
        }

        let reduceRe = try makeEmptyBuffer(device: device, bytes: cellCount * MemoryLayout<Float>.stride)
        let reduceIm = try makeEmptyBuffer(device: device, bytes: cellCount * MemoryLayout<Float>.stride)
        let reduceGpuSeconds = try runReduceKernel(
            queue: queue,
            pipeline: reducePipeline,
            contributionBuffer: contributionBuffer,
            activeCellBuffer: activeCellBuffer,
            offsetBuffer: offsetBuffer,
            reBuffer: reduceRe,
            imBuffer: reduceIm,
            paramsBuffer: reduceParamsBuffer,
            activeCellCount: reductionPlan.activeCells.count
        )
        let reduceDownloadStart = DispatchTime.now().uptimeNanoseconds
        let reduceGrid = readFloatGrid(reBuffer: reduceRe, imBuffer: reduceIm, count: cellCount)
        let reduceDownloadEnd = DispatchTime.now().uptimeNanoseconds
        let reduceMetrics = compare(reference, reduceGrid)

        let tileCellCount = tilePlan.tileCount * tilePlan.tileHaloEdge * tilePlan.tileHaloEdge
        let tileRe = try makeEmptyBuffer(device: device, bytes: tileCellCount * MemoryLayout<Float>.stride)
        let tileIm = try makeEmptyBuffer(device: device, bytes: tileCellCount * MemoryLayout<Float>.stride)
        let tileGpuSeconds = try runTileKernel(
            queue: queue,
            pipeline: tilePipeline,
            sampleBuffer: tileSampleBuffer,
            offsetBuffer: tileOffsetBuffer,
            tapBuffer: tapBuffer,
            reBuffer: tileRe,
            imBuffer: tileIm,
            paramsBuffer: tileParamsBuffer,
            threadCount: tileCellCount
        )
        let tileDownloadStart = DispatchTime.now().uptimeNanoseconds
        let tileGrid = mergeTileGrid(reBuffer: tileRe, imBuffer: tileIm, plan: tilePlan, config: config)
        let tileDownloadEnd = DispatchTime.now().uptimeNanoseconds
        let tileMetrics = compare(reference, tileGrid)

        let tileBinRe = try makeEmptyBuffer(device: device, bytes: tileCellCount * MemoryLayout<Float>.stride)
        let tileBinIm = try makeEmptyBuffer(device: device, bytes: tileCellCount * MemoryLayout<Float>.stride)
        let tileBinGpuSeconds = try runTileCellBinKernel(
            queue: queue,
            pipeline: tileCellBinPipeline,
            sampleBuffer: tileSampleBuffer,
            activeCellBuffer: tileCellBinActiveBuffer,
            cellOffsetBuffer: tileCellBinOffsetBuffer,
            sampleIndexBuffer: tileCellBinSampleIndexBuffer,
            tapBuffer: tapBuffer,
            reBuffer: tileBinRe,
            imBuffer: tileBinIm,
            paramsBuffer: tileCellBinParamsBuffer,
            activeCellCount: tileCellBinPlan.activeTileCells.count
        )
        let tileBinDownloadStart = DispatchTime.now().uptimeNanoseconds
        let tileBinGrid = mergeTileGrid(reBuffer: tileBinRe, imBuffer: tileBinIm, plan: tilePlan, config: config)
        let tileBinDownloadEnd = DispatchTime.now().uptimeNanoseconds
        let tileBinMetrics = compare(reference, tileBinGrid)

        let gpuBinCountBuffer = try makeEmptyBuffer(
            device: device,
            bytes: tileCellCount * MemoryLayout<UInt32>.stride
        )
        let gpuBinCountSeconds = try runTileCellBinCountKernel(
            queue: queue,
            pipeline: tileCellBinCountPipeline,
            sampleBuffer: tileSampleBuffer,
            countBuffer: gpuBinCountBuffer,
            paramsBuffer: tileCellBinParamsBuffer,
            sampleCount: config.samples
        )
        let gpuBinPrefixStart = DispatchTime.now().uptimeNanoseconds
        let gpuBinCounts = readUInt32Array(buffer: gpuBinCountBuffer, count: tileCellCount)
        let gpuBinPrefix = makeGpuTileCellBinPrefix(counts: gpuBinCounts)
        let gpuBinPrefixEnd = DispatchTime.now().uptimeNanoseconds
        let gpuBinActiveBuffer = try makeBuffer(device: device, array: gpuBinPrefix.activeTileCells)
        let gpuBinOffsetBuffer = try makeBuffer(device: device, array: gpuBinPrefix.cellOffsets)
        let gpuBinCellSlotBuffer = try makeBuffer(device: device, array: gpuBinPrefix.cellSlots)
        let gpuBinFillCountBuffer = try makeEmptyBuffer(
            device: device,
            bytes: gpuBinPrefix.activeTileCells.count * MemoryLayout<UInt32>.stride
        )
        let gpuBinSampleIndexBuffer = try makeEmptyBuffer(
            device: device,
            bytes: gpuBinPrefix.sampleRefCount * MemoryLayout<UInt32>.stride
        )
        let gpuBinParamsBuffer = try makeBuffer(
            device: device,
            value: TileCellBinParams(
                activeCellCount: UInt32(gpuBinPrefix.activeTileCells.count),
                sampleCount: UInt32(config.samples),
                tileCount: UInt32(tilePlan.tileCount),
                width: UInt32(config.imsize),
                height: UInt32(config.imsize),
                support: UInt32(config.support),
                tapCount: UInt32(2 * config.support + 1),
                tileEdge: UInt32(tilePlan.tileEdge),
                tilesY: UInt32(tilePlan.tilesY),
                tileHaloEdge: UInt32(tilePlan.tileHaloEdge)
            )
        )
        let gpuBinFillSeconds = try runTileCellBinFillKernel(
            queue: queue,
            pipeline: tileCellBinFillPipeline,
            sampleBuffer: tileSampleBuffer,
            cellSlotBuffer: gpuBinCellSlotBuffer,
            cellOffsetBuffer: gpuBinOffsetBuffer,
            fillCountBuffer: gpuBinFillCountBuffer,
            sampleIndexBuffer: gpuBinSampleIndexBuffer,
            paramsBuffer: gpuBinParamsBuffer,
            sampleCount: config.samples
        )
        let gpuBinRe = try makeEmptyBuffer(device: device, bytes: tileCellCount * MemoryLayout<Float>.stride)
        let gpuBinIm = try makeEmptyBuffer(device: device, bytes: tileCellCount * MemoryLayout<Float>.stride)
        let gpuBinReduceSeconds = try runTileCellBinKernel(
            queue: queue,
            pipeline: tileCellBinPipeline,
            sampleBuffer: tileSampleBuffer,
            activeCellBuffer: gpuBinActiveBuffer,
            cellOffsetBuffer: gpuBinOffsetBuffer,
            sampleIndexBuffer: gpuBinSampleIndexBuffer,
            tapBuffer: tapBuffer,
            reBuffer: gpuBinRe,
            imBuffer: gpuBinIm,
            paramsBuffer: gpuBinParamsBuffer,
            activeCellCount: gpuBinPrefix.activeTileCells.count
        )
        let gpuBinDownloadStart = DispatchTime.now().uptimeNanoseconds
        let gpuBinGrid = mergeTileGrid(reBuffer: gpuBinRe, imBuffer: gpuBinIm, plan: tilePlan, config: config)
        let gpuBinDownloadEnd = DispatchTime.now().uptimeNanoseconds
        let gpuBinMetrics = compare(reference, gpuBinGrid)

        let gpuPrefixCountBuffer = try makeEmptyBuffer(
            device: device,
            bytes: tileCellCount * MemoryLayout<UInt32>.stride
        )
        let gpuPrefixCountSeconds = try runTileCellBinCountKernel(
            queue: queue,
            pipeline: tileCellBinCountPipeline,
            sampleBuffer: tileSampleBuffer,
            countBuffer: gpuPrefixCountBuffer,
            paramsBuffer: tileCellBinParamsBuffer,
            sampleCount: config.samples
        )
        let scanBufferA = try makeEmptyBuffer(
            device: device,
            bytes: tileCellCount * MemoryLayout<UInt32>.stride
        )
        var scanInput = gpuPrefixCountBuffer
        var scanOutput = scanBufferA
        var gpuPrefixScanSeconds = 0.0
        let scanWallStart = DispatchTime.now().uptimeNanoseconds
        var step = 1
        while step < tileCellCount {
            let scanParamsBuffer = try makeBuffer(
                device: device,
                value: PrefixParams(elementCount: UInt32(tileCellCount), step: UInt32(step))
            )
            gpuPrefixScanSeconds += try runPrefixScanStepKernel(
                queue: queue,
                pipeline: prefixScanPipeline,
                inputBuffer: scanInput,
                outputBuffer: scanOutput,
                paramsBuffer: scanParamsBuffer,
                elementCount: tileCellCount
            )
            swap(&scanInput, &scanOutput)
            step *= 2
        }
        let scanWallEnd = DispatchTime.now().uptimeNanoseconds
        let gpuPrefixFillCountBuffer = try makeEmptyBuffer(
            device: device,
            bytes: tileCellCount * MemoryLayout<UInt32>.stride
        )
        let maxSampleRefs = config.samples * (2 * config.support + 1) * (2 * config.support + 1)
        let gpuPrefixSampleIndexBuffer = try makeEmptyBuffer(
            device: device,
            bytes: maxSampleRefs * MemoryLayout<UInt32>.stride
        )
        let gpuPrefixFillSeconds = try runTileCellBinFillAllKernel(
            queue: queue,
            pipeline: tileCellBinFillAllPipeline,
            sampleBuffer: tileSampleBuffer,
            prefixBuffer: scanInput,
            fillCountBuffer: gpuPrefixFillCountBuffer,
            sampleIndexBuffer: gpuPrefixSampleIndexBuffer,
            paramsBuffer: tileCellBinParamsBuffer,
            sampleCount: config.samples
        )
        let gpuPrefixRe = try makeEmptyBuffer(device: device, bytes: tileCellCount * MemoryLayout<Float>.stride)
        let gpuPrefixIm = try makeEmptyBuffer(device: device, bytes: tileCellCount * MemoryLayout<Float>.stride)
        let gpuPrefixReduceSeconds = try runTileCellBinReduceAllKernel(
            queue: queue,
            pipeline: tileCellBinReduceAllPipeline,
            sampleBuffer: tileSampleBuffer,
            prefixBuffer: scanInput,
            sampleIndexBuffer: gpuPrefixSampleIndexBuffer,
            tapBuffer: tapBuffer,
            reBuffer: gpuPrefixRe,
            imBuffer: gpuPrefixIm,
            paramsBuffer: tileCellBinParamsBuffer,
            tileCellCount: tileCellCount
        )
        let gpuPrefixDownloadStart = DispatchTime.now().uptimeNanoseconds
        let gpuPrefixGrid = mergeTileGrid(
            reBuffer: gpuPrefixRe,
            imBuffer: gpuPrefixIm,
            plan: tilePlan,
            config: config
        )
        let gpuPrefixDownloadEnd = DispatchTime.now().uptimeNanoseconds
        let gpuPrefixMetrics = compare(reference, gpuPrefixGrid)

        let tapUpdates = Double(config.samples * (2 * config.support + 1) * (2 * config.support + 1))
        let bufferBytes =
            samples.count * MemoryLayout<MetalGridSample>.stride
            + taps.count * MemoryLayout<Float>.stride
            + 4 * cellCount * MemoryLayout<Float>.stride
        let reduceBufferBytes =
            reductionPlan.contributions.count * MemoryLayout<GridContribution>.stride
            + reductionPlan.activeCells.count * MemoryLayout<UInt32>.stride
            + reductionPlan.offsets.count * MemoryLayout<UInt32>.stride
            + 2 * cellCount * MemoryLayout<Float>.stride
        let tileBufferBytes =
            tilePlan.samples.count * MemoryLayout<MetalGridSample>.stride
            + tilePlan.offsets.count * MemoryLayout<UInt32>.stride
            + 2 * tileCellCount * MemoryLayout<Float>.stride
        let tileCellBinBufferBytes =
            tilePlan.samples.count * MemoryLayout<MetalGridSample>.stride
            + tileCellBinPlan.activeTileCells.count * MemoryLayout<UInt32>.stride
            + tileCellBinPlan.cellOffsets.count * MemoryLayout<UInt32>.stride
            + tileCellBinPlan.sampleIndices.count * MemoryLayout<UInt32>.stride
            + 2 * tileCellCount * MemoryLayout<Float>.stride
        let gpuTileCellBinBufferBytes =
            tilePlan.samples.count * MemoryLayout<MetalGridSample>.stride
            + tileCellCount * MemoryLayout<UInt32>.stride
            + gpuBinPrefix.cellSlots.count * MemoryLayout<UInt32>.stride
            + gpuBinPrefix.activeTileCells.count * MemoryLayout<UInt32>.stride
            + gpuBinPrefix.cellOffsets.count * MemoryLayout<UInt32>.stride
            + gpuBinPrefix.sampleRefCount * MemoryLayout<UInt32>.stride
            + 2 * tileCellCount * MemoryLayout<Float>.stride
        let gpuPrefixTileCellBinBufferBytes =
            tilePlan.samples.count * MemoryLayout<MetalGridSample>.stride
            + 4 * tileCellCount * MemoryLayout<UInt32>.stride
            + maxSampleRefs * MemoryLayout<UInt32>.stride
            + 2 * tileCellCount * MemoryLayout<Float>.stride

        print("run=\(repeatIndex + 1) strategy=global_atomic gpu_s=\(formatSeconds(globalGpuSeconds)) download_s=\(formatSeconds(Double(globalDownloadEnd - globalDownloadStart) / 1_000_000_000.0)) samples_per_s=\(formatMetric(Double(config.samples) / max(globalGpuSeconds, 1e-12))) tap_updates_per_s=\(formatMetric(tapUpdates / max(globalGpuSeconds, 1e-12))) max_abs_error=\(formatMetric(globalMetrics.maxAbsError)) rms_error=\(formatMetric(globalMetrics.rmsError)) relative_rms_error=\(formatMetric(globalMetrics.relativeRmsError))")
        let residualTapPasses = tapUpdates * 2.0
        print("run=\(repeatIndex + 1) strategy=residual_refresh_global_atomic gpu_s=\(formatSeconds(residualGpuSeconds)) download_s=\(formatSeconds(Double(residualDownloadEnd - residualDownloadStart) / 1_000_000_000.0)) samples_per_s=\(formatMetric(Double(config.samples) / max(residualGpuSeconds, 1e-12))) tap_passes_per_s=\(formatMetric(residualTapPasses / max(residualGpuSeconds, 1e-12))) max_abs_error=\(formatMetric(residualMetrics.maxAbsError)) rms_error=\(formatMetric(residualMetrics.rmsError)) relative_rms_error=\(formatMetric(residualMetrics.relativeRmsError))")
        if let ownerResult {
            print("run=\(repeatIndex + 1) strategy=cell_owner gpu_s=\(formatSeconds(ownerResult.gpuSeconds)) download_s=\(formatSeconds(ownerResult.downloadSeconds)) samples_per_s=\(formatMetric(Double(config.samples) / max(ownerResult.gpuSeconds, 1e-12))) tap_updates_per_s=\(formatMetric(tapUpdates / max(ownerResult.gpuSeconds, 1e-12))) max_abs_error=\(formatMetric(ownerResult.metrics.maxAbsError)) rms_error=\(formatMetric(ownerResult.metrics.rmsError)) relative_rms_error=\(formatMetric(ownerResult.metrics.relativeRmsError))")
        } else {
            print("run=\(repeatIndex + 1) strategy=cell_owner skipped=true reason=skip_slow_baselines")
        }
        print("run=\(repeatIndex + 1) strategy=sorted_reduce gpu_s=\(formatSeconds(reduceGpuSeconds)) download_s=\(formatSeconds(Double(reduceDownloadEnd - reduceDownloadStart) / 1_000_000_000.0)) samples_per_s=\(formatMetric(Double(config.samples) / max(reduceGpuSeconds, 1e-12))) tap_updates_per_s=\(formatMetric(tapUpdates / max(reduceGpuSeconds, 1e-12))) max_abs_error=\(formatMetric(reduceMetrics.maxAbsError)) rms_error=\(formatMetric(reduceMetrics.rmsError)) relative_rms_error=\(formatMetric(reduceMetrics.relativeRmsError)) reduce_buffer_bytes=\(reduceBufferBytes)")
        print("run=\(repeatIndex + 1) strategy=tile_bucket_cell_owner gpu_s=\(formatSeconds(tileGpuSeconds)) download_s=\(formatSeconds(Double(tileDownloadEnd - tileDownloadStart) / 1_000_000_000.0)) samples_per_s=\(formatMetric(Double(config.samples) / max(tileGpuSeconds, 1e-12))) tap_updates_per_s=\(formatMetric(tapUpdates / max(tileGpuSeconds, 1e-12))) max_abs_error=\(formatMetric(tileMetrics.maxAbsError)) rms_error=\(formatMetric(tileMetrics.rmsError)) relative_rms_error=\(formatMetric(tileMetrics.relativeRmsError)) tile_buffer_bytes=\(tileBufferBytes)")
        print("run=\(repeatIndex + 1) strategy=tile_cell_bins gpu_s=\(formatSeconds(tileBinGpuSeconds)) download_s=\(formatSeconds(Double(tileBinDownloadEnd - tileBinDownloadStart) / 1_000_000_000.0)) samples_per_s=\(formatMetric(Double(config.samples) / max(tileBinGpuSeconds, 1e-12))) tap_updates_per_s=\(formatMetric(tapUpdates / max(tileBinGpuSeconds, 1e-12))) max_abs_error=\(formatMetric(tileBinMetrics.maxAbsError)) rms_error=\(formatMetric(tileBinMetrics.rmsError)) relative_rms_error=\(formatMetric(tileBinMetrics.relativeRmsError)) tile_cell_bin_buffer_bytes=\(tileCellBinBufferBytes)")
        let gpuBinTotalSeconds = gpuBinCountSeconds + gpuBinFillSeconds + gpuBinReduceSeconds
        print("run=\(repeatIndex + 1) strategy=gpu_tile_cell_bins gpu_count_s=\(formatSeconds(gpuBinCountSeconds)) prefix_read_s=\(formatSeconds(Double(gpuBinPrefixEnd - gpuBinPrefixStart) / 1_000_000_000.0)) prefix_cpu_s=\(formatSeconds(gpuBinPrefix.prefixSeconds)) gpu_fill_s=\(formatSeconds(gpuBinFillSeconds)) gpu_reduce_s=\(formatSeconds(gpuBinReduceSeconds)) gpu_total_s=\(formatSeconds(gpuBinTotalSeconds)) download_s=\(formatSeconds(Double(gpuBinDownloadEnd - gpuBinDownloadStart) / 1_000_000_000.0)) samples_per_s=\(formatMetric(Double(config.samples) / max(gpuBinTotalSeconds, 1e-12))) tap_updates_per_s=\(formatMetric(tapUpdates / max(gpuBinTotalSeconds, 1e-12))) max_abs_error=\(formatMetric(gpuBinMetrics.maxAbsError)) rms_error=\(formatMetric(gpuBinMetrics.rmsError)) relative_rms_error=\(formatMetric(gpuBinMetrics.relativeRmsError)) active_tile_cells=\(gpuBinPrefix.activeTileCells.count) sample_refs=\(gpuBinPrefix.sampleRefCount) gpu_tile_cell_bin_buffer_bytes=\(gpuTileCellBinBufferBytes)")
        let gpuPrefixTotalSeconds = gpuPrefixCountSeconds + gpuPrefixScanSeconds + gpuPrefixFillSeconds + gpuPrefixReduceSeconds
        print("run=\(repeatIndex + 1) strategy=gpu_prefix_tile_cell_bins gpu_count_s=\(formatSeconds(gpuPrefixCountSeconds)) gpu_scan_s=\(formatSeconds(gpuPrefixScanSeconds)) scan_wall_s=\(formatSeconds(Double(scanWallEnd - scanWallStart) / 1_000_000_000.0)) gpu_fill_s=\(formatSeconds(gpuPrefixFillSeconds)) gpu_reduce_s=\(formatSeconds(gpuPrefixReduceSeconds)) gpu_total_s=\(formatSeconds(gpuPrefixTotalSeconds)) download_s=\(formatSeconds(Double(gpuPrefixDownloadEnd - gpuPrefixDownloadStart) / 1_000_000_000.0)) samples_per_s=\(formatMetric(Double(config.samples) / max(gpuPrefixTotalSeconds, 1e-12))) tap_updates_per_s=\(formatMetric(tapUpdates / max(gpuPrefixTotalSeconds, 1e-12))) max_abs_error=\(formatMetric(gpuPrefixMetrics.maxAbsError)) rms_error=\(formatMetric(gpuPrefixMetrics.rmsError)) relative_rms_error=\(formatMetric(gpuPrefixMetrics.relativeRmsError)) max_sample_refs=\(maxSampleRefs) gpu_prefix_tile_cell_bin_buffer_bytes=\(gpuPrefixTileCellBinBufferBytes)")
        print("estimated_live_buffer_bytes=\(bufferBytes)")
    }
}

do {
    try main()
} catch {
    fputs("\(error)\n", stderr)
    exit(1)
}
