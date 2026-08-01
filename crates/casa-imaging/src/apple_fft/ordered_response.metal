#include <metal_stdlib>
using namespace metal;

struct LinearParams {
    uint elements;
    uint plane_elements;
    uint _pad0;
    uint _pad1;
};

struct MixerAction {
    uint input_plane;
    uint kernel_plane;
    float coefficient;
    uint _pad0;
};

struct MixerParams {
    uint pixels;
    uint output_planes;
    float response_scale;
    uint _pad1;
};

struct ReductionParams {
    uint pixels;
    uint active_pixels;
    uint imaging_states;
    uint eta_powers;
};

struct ConstructionParams {
    uint side;
    uint states;
    uint coefficients;
    uint support_width;
    uint oversampling;
    uint offset_bias;
    uint group_count;
    uint state_base;
};

struct CompactCorrectionParams {
    uint input_side;
    uint output_side;
    uint planes;
    uint _pad0;
};

struct PlaneCopyParams {
    uint elements;
    uint plane_elements;
    uint output_plane_base;
    uint _pad0;
};

static inline float2 complex_multiply(float2 left, float2 right) {
    return float2(
        left.x * right.x - left.y * right.y,
        left.x * right.y + left.y * right.x
    );
}

kernel void prepare_right_planes(
    device const float *model [[buffer(0)]],
    device const float2 *right_factors [[buffer(1)]],
    device float2 *output [[buffer(2)]],
    constant LinearParams &params [[buffer(3)]],
    uint index [[thread_position_in_grid]]
) {
    if (index >= params.elements) {
        return;
    }
    uint plane = index / params.plane_elements;
    uint pixel = index - plane * params.plane_elements;
    uint model_term = plane & 1u;
    uint factor_plane = plane >> 1u;
    float amplitude =
        model[ulong(model_term) * ulong(params.plane_elements) + ulong(pixel)];
    output[index] =
        amplitude
        * right_factors[
            ulong(factor_plane) * ulong(params.plane_elements) + ulong(pixel)
        ];
}

kernel void mix_ordered_response(
    device const float2 *forward [[buffer(0)]],
    device const float2 *kernels [[buffer(1)]],
    device const uint *offsets [[buffer(2)]],
    device const MixerAction *actions [[buffer(3)]],
    device float2 *mixed [[buffer(4)]],
    constant MixerParams &params [[buffer(5)]],
    uint2 position [[thread_position_in_grid]]
) {
    uint pixel = position.x;
    uint output_plane = position.y;
    if (pixel >= params.pixels || output_plane >= params.output_planes) {
        return;
    }
    float2 accumulator = float2(0.0f);
    uint begin = offsets[output_plane];
    uint end = offsets[output_plane + 1u];
    for (uint action_index = begin; action_index < end; ++action_index) {
        MixerAction action = actions[action_index];
        float2 input_value =
            forward[ulong(action.input_plane) * ulong(params.pixels) + ulong(pixel)];
        float2 kernel_value =
            kernels[ulong(action.kernel_plane) * ulong(params.pixels) + ulong(pixel)];
        accumulator +=
            action.coefficient * complex_multiply(kernel_value, input_value);
    }
    mixed[ulong(output_plane) * ulong(params.pixels) + ulong(pixel)] =
        params.response_scale * accumulator;
}

kernel void reduce_active_left_response(
    device const float2 *inverse [[buffer(0)]],
    device const float2 *left_factors [[buffer(1)]],
    device const uint *active_indices [[buffer(2)]],
    device float2 *feedback [[buffer(3)]],
    constant ReductionParams &params [[buffer(4)]],
    uint2 position [[thread_position_in_grid]]
) {
    uint active = position.x;
    uint output_term = position.y;
    if (active >= params.active_pixels || output_term >= 2u) {
        return;
    }
    uint pixel = active_indices[active];
    float2 accumulator = float2(0.0f);
    for (uint imaging_state = 0u; imaging_state < params.imaging_states; ++imaging_state) {
        for (uint left_power = 0u; left_power < params.eta_powers; ++left_power) {
            uint inverse_plane =
                (imaging_state * params.eta_powers + left_power) * 2u + output_term;
            uint factor_plane = imaging_state * params.eta_powers + left_power;
            ulong inverse_index =
                ulong(inverse_plane) * ulong(params.pixels) + ulong(pixel);
            ulong factor_index =
                ulong(factor_plane) * ulong(params.pixels) + ulong(pixel);
            accumulator += complex_multiply(
                left_factors[factor_index],
                inverse[inverse_index]
            );
        }
    }
    feedback[ulong(output_term) * ulong(params.active_pixels) + ulong(active)] =
        accumulator;
}

kernel void construct_ordered_response_output_owner_es(
    device const uint *bucket_offsets [[buffer(0)]],
    device const float2 *group_meta [[buffer(1)]],
    device const float2 *group_coefficients [[buffer(2)]],
    device const float *kernel_lut [[buffer(3)]],
    device float2 *output [[buffer(4)]],
    constant ConstructionParams &params [[buffer(5)]],
    uint3 position [[thread_position_in_grid]]
) {
    uint x = position.x;
    uint y = position.y;
    uint state = position.z;
    if (x >= params.side || y >= params.side || state >= params.states) {
        return;
    }
    float2 accumulators[9];
    for (uint coefficient = 0u; coefficient < 9u; ++coefficient) {
        accumulators[coefficient] = float2(0.0f);
    }
    int radius = int(params.support_width >> 1);
    int first_x = max(0, int(x) - radius);
    int last_x = min(int(params.side) - 1, int(x) + radius);
    int first_y = max(0, int(y) - radius);
    int last_y = min(int(params.side) - 1, int(y) + radius);
    ulong pixels = ulong(params.side) * ulong(params.side);
    for (int center_y = first_y; center_y <= last_y; ++center_y) {
        for (int center_x = first_x; center_x <= last_x; ++center_x) {
            ulong bucket =
                ulong(state + params.state_base) * pixels
                + ulong(center_y) * ulong(params.side)
                + ulong(center_x);
            uint begin = bucket_offsets[bucket];
            uint end = bucket_offsets[bucket + 1ul];
            int delta_x = int(x) - center_x;
            int delta_y = int(y) - center_y;
            for (uint group = begin; group < end; ++group) {
                float2 subpixel = group_meta[group];
                float lut_position_x =
                    clamp(subpixel.x + 0.5f, 0.0f, 1.0f)
                    * float(params.oversampling);
                float lut_position_y =
                    clamp(subpixel.y + 0.5f, 0.0f, 1.0f)
                    * float(params.oversampling);
                uint lut_low_x = min(
                    uint(floor(lut_position_x)),
                    params.oversampling - 1u
                );
                uint lut_low_y = min(
                    uint(floor(lut_position_y)),
                    params.oversampling - 1u
                );
                float fraction_x = lut_position_x - float(lut_low_x);
                float fraction_y = lut_position_y - float(lut_low_y);
                uint tap_x = uint(delta_x + radius);
                uint tap_y = uint(delta_y + radius);
                uint lut_x0 = lut_low_x * params.support_width + tap_x;
                uint lut_x1 = (lut_low_x + 1u) * params.support_width + tap_x;
                uint lut_y0 = lut_low_y * params.support_width + tap_y;
                uint lut_y1 = (lut_low_y + 1u) * params.support_width + tap_y;
                float weight_x = mix(
                    kernel_lut[lut_x0],
                    kernel_lut[lut_x1],
                    fraction_x
                );
                float weight_y = mix(
                    kernel_lut[lut_y0],
                    kernel_lut[lut_y1],
                    fraction_y
                );
                float weight = weight_x * weight_y;
                ulong coefficient_base =
                    ulong(group) * ulong(params.coefficients);
                for (uint coefficient = 0u;
                     coefficient < params.coefficients;
                     ++coefficient) {
                    accumulators[coefficient] +=
                        weight * group_coefficients[coefficient_base + coefficient];
                }
            }
        }
    }
    uint shifted_x = (x + (params.side >> 1u)) % params.side;
    uint shifted_y = (y + (params.side >> 1u)) % params.side;
    ulong pixel = ulong(shifted_y) * ulong(params.side) + ulong(shifted_x);
    for (uint coefficient = 0u;
         coefficient < params.coefficients;
         ++coefficient) {
        ulong plane = ulong(state) * ulong(params.coefficients) + coefficient;
        output[plane * pixels + pixel] = accumulators[coefficient];
    }
}

kernel void compact_deapodize_ordered_response_lag_domain(
    device const float2 *input [[buffer(0)]],
    device const float *axis_correction [[buffer(1)]],
    device float2 *output [[buffer(2)]],
    constant CompactCorrectionParams &params [[buffer(3)]],
    uint index [[thread_position_in_grid]]
) {
    uint output_pixels = params.output_side * params.output_side;
    uint elements = params.planes * output_pixels;
    if (index >= elements) {
        return;
    }
    uint plane = index / output_pixels;
    uint pixel = index - plane * output_pixels;
    uint x = pixel % params.output_side;
    uint y = pixel / params.output_side;
    int signed_x = int(x);
    int signed_y = int(y);
    if (x >= (params.output_side >> 1u)) {
        signed_x -= int(params.output_side);
    }
    if (y >= (params.output_side >> 1u)) {
        signed_y -= int(params.output_side);
    }
    uint input_x =
        signed_x >= 0 ? uint(signed_x) : uint(int(params.input_side) + signed_x);
    uint input_y =
        signed_y >= 0 ? uint(signed_y) : uint(int(params.input_side) + signed_y);
    uint input_pixels = params.input_side * params.input_side;
    ulong input_index =
        ulong(plane) * ulong(input_pixels)
        + ulong(input_y) * ulong(params.input_side)
        + ulong(input_x);
    output[index] =
        float(input_pixels)
        * axis_correction[x]
        * axis_correction[y]
        * input[input_index];
}

kernel void copy_ordered_response_planes(
    device const float2 *input [[buffer(0)]],
    device float2 *output [[buffer(1)]],
    constant PlaneCopyParams &params [[buffer(2)]],
    uint index [[thread_position_in_grid]]
) {
    if (index >= params.elements) {
        return;
    }
    uint plane = index / params.plane_elements;
    uint pixel = index - plane * params.plane_elements;
    ulong output_index =
        ulong(params.output_plane_base + plane) * ulong(params.plane_elements)
        + ulong(pixel);
    output[output_index] = input[index];
}
