// SPDX-License-Identifier: LGPL-3.0-or-later
//! The cell noise on a Metal device: the same draws as [`super::CellNoise`],
//! in Metal Shading Language, for a backend to compile beside its own
//! kernels.
//!
//! The source follows the CPU code operation for operation: Philox4x32-10
//! on 32-bit words, the logarithm from the word's leading zeros and an
//! integer `m − 1`, the sine and cosine from the octant's low bits, each
//! sign by a flip of its bit, and the same additions into the cell in the
//! same order. Its constants are the CPU's, written as their bits, so the
//! device and the host round the same values. A device compiles it with
//! fast math off, so additions, multiplications, the division and the
//! square root are IEEE-754's, and the source turns off the fusing of a
//! product and a sum into one rounding (`#pragma METAL fp contract(off)`),
//! which Metal does by default and the host never does. Draws then agree
//! with the host's to the bit (the backend's law checks whole cells).
//!
//! Every name carries the `cell_noise_` prefix, so the source can be
//! concatenated with a backend's own. The pragma holds to the end of the
//! source it is compiled in, so the noise goes after the backend's kernels,
//! which keep their contraction.

use super::normal::{
    ATANH, COSINE, LN_2_HIGH, LN_2_LOW, OCTANT_WORDS, SINE, SQRT_HALF_WORD, WORD_SCALE,
};
use super::philox::{MULTIPLIERS, WEYL};

/// One noise term as the kernel reads it: its stream key and σ.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct MetalNoiseTerm {
    /// Philox's key.
    pub key: [u32; 2],
    /// σ of each real and imaginary draw.
    pub sigma: f32,
}

/// One row of cells as the kernel reads it: its row number (the counter's
/// first two words) and whether it is an autocorrelation, which gets real
/// draws only.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetalNoiseRow {
    /// The row's low and high 32 bits.
    pub row: [u32; 2],
    /// 1 for an autocorrelation, else 0.
    pub autocorrelation: u32,
}

/// The kernel's constants: the cells' shape and the terms' count.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetalNoiseParams {
    /// Rows of cells.
    pub rows: u32,
    /// Channels of a row.
    pub channels: u32,
    /// Correlations of a channel.
    pub correlations: u32,
    /// Noise terms, added in order.
    pub terms: u32,
}

/// The kernel's name.
pub const METAL_KERNEL: &str = "cell_noise_add";

/// The Metal source of `cell_noise_add`: one thread per (row, channel,
/// correlation pair) of `params.rows × params.channels` cells of
/// `params.correlations` correlations, interleaved complex `f32`, row by
/// row, channel then correlation fastest. Buffers: 0 the cells (read and
/// written), 1 the rows ([`MetalNoiseRow`]), 2 the terms
/// ([`MetalNoiseTerm`]), 3 the params ([`MetalNoiseParams`]).
#[must_use]
pub fn metal_source() -> String {
    let bits = |value: f32| format!("as_type<float>(0x{:08X}u)", value.to_bits());
    let list = |values: &[f32]| {
        values
            .iter()
            .map(|v| bits(*v))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        r#"
// ---- cell noise (casa-numerics::cell_noise) ----
#include <metal_stdlib>
using namespace metal;
#pragma METAL fp contract(off)

struct cell_noise_row {{ uint row_low; uint row_high; uint autocorrelation; }};
struct cell_noise_term {{ uint key0; uint key1; float sigma; }};
struct cell_noise_params {{ uint rows; uint channels; uint correlations; uint terms; }};

constant float cell_noise_word_scale = {word_scale}; // 2^-32
constant float cell_noise_octant_scale = {octant_scale}; // 2^-29
constant uint cell_noise_octant_words = {octant_words}u;
constant uint cell_noise_sqrt_half_word = {sqrt_half}u;
constant float cell_noise_ln2_high = {ln2_high};
constant float cell_noise_ln2_low = {ln2_low};
constant float cell_noise_atanh[4] = {{ {atanh} }};
constant float cell_noise_sine[5] = {{ {sine} }};
constant float cell_noise_cosine[5] = {{ {cosine} }};

static inline uint4 cell_noise_philox(uint4 c, uint2 k) {{
    for (uint round = 0; round < 10; ++round) {{
        if (round > 0) {{
            k = uint2(k.x + {weyl0}u, k.y + {weyl1}u);
        }}
        uint high0 = mulhi({mult0}u, c.x);
        uint low0 = {mult0}u * c.x;
        uint high1 = mulhi({mult1}u, c.z);
        uint low1 = {mult1}u * c.z;
        c = uint4(high1 ^ c.y ^ k.x, low1, high0 ^ c.w ^ k.y, low0);
    }}
    return c;
}}

static inline float cell_noise_negate_if(float x, bool negate) {{
    return as_type<float>(as_type<uint>(x) ^ (negate ? 0x80000000u : 0u));
}}

static inline float cell_noise_ln_uniform(uint word) {{
    uint v = word + 1u;
    uint shift = clz(v) & 31u;
    uint normal = v << shift;
    bool upper = (normal - 1u) >= (cell_noise_sqrt_half_word - 1u);
    uint numerator = upper ? (0u - normal) : (normal & 0x7FFFFFFFu);
    float scale = upper ? -cell_noise_word_scale : 2.0f * cell_noise_word_scale;
    float f = float(int(numerator)) * scale;
    float k = float(-int(shift) - (upper ? 0 : 1));
    float s = f / (2.0f + f);
    float z = s * s;
    float r = z * (cell_noise_atanh[0] + z * (cell_noise_atanh[1]
        + z * (cell_noise_atanh[2] + z * cell_noise_atanh[3])));
    float half_square = 0.5f * f * f;
    return k * cell_noise_ln2_high
        - ((half_square - (s * (half_square + r) + k * cell_noise_ln2_low)) - f);
}}

static inline float cell_noise_radius(uint word) {{
    return sqrt(-2.0f * cell_noise_ln_uniform(word));
}}

static inline float2 cell_noise_sin_cos(uint word) {{
    uint octant = word >> 29;
    uint within = word & (cell_noise_octant_words - 1u);
    bool odd = (octant & 1u) == 1u;
    uint steps = odd ? (cell_noise_octant_words - within) : within;
    float t = float(int(steps)) * cell_noise_octant_scale;
    float t2 = t * t;
    float sine = t * (cell_noise_sine[0] + t2 * (cell_noise_sine[1] + t2 * (cell_noise_sine[2]
        + t2 * (cell_noise_sine[3] + t2 * cell_noise_sine[4]))));
    float cosine = 1.0f + t2 * (cell_noise_cosine[0] + t2 * (cell_noise_cosine[1]
        + t2 * (cell_noise_cosine[2] + t2 * (cell_noise_cosine[3] + t2 * cell_noise_cosine[4]))));
    bool swapped = ((octant ^ (octant >> 1)) & 1u) == 1u;
    float s = swapped ? cosine : sine;
    float c = swapped ? sine : cosine;
    return float2(cell_noise_negate_if(s, (octant >> 2) == 1u),
                  cell_noise_negate_if(c, ((octant + 2u) >> 2) == 1u));
}}

kernel void {kernel}(device float2 *cells [[buffer(0)]],
                     device const cell_noise_row *rows [[buffer(1)]],
                     device const cell_noise_term *terms [[buffer(2)]],
                     constant cell_noise_params &p [[buffer(3)]],
                     uint index [[thread_position_in_grid]]) {{
    uint pairs = (p.correlations + 1u) / 2u;
    uint per_row = p.channels * pairs;
    if (index >= p.rows * per_row) {{
        return;
    }}
    uint position = index / per_row;
    uint channel = (index % per_row) / pairs;
    uint pair = index % pairs;
    cell_noise_row row = rows[position];
    bool both = p.correlations - 2u * pair >= 2u;
    ulong at = (ulong(position) * p.channels + channel) * p.correlations + 2u * pair;
    uint4 counter = uint4(row.row_low, row.row_high, channel, pair);
    for (uint term = 0; term < p.terms; ++term) {{
        cell_noise_term t = terms[term];
        uint4 words = cell_noise_philox(counter, uint2(t.key0, t.key1));
        float scaled0 = cell_noise_radius(words.x) * t.sigma;
        float2 sc0 = cell_noise_sin_cos(words.y);
        float2 sample0 = cells[at];
        sample0.x += scaled0 * sc0.y;
        if (row.autocorrelation == 0u) {{
            sample0.y += scaled0 * sc0.x;
        }}
        cells[at] = sample0;
        if (both) {{
            float scaled1 = cell_noise_radius(words.z) * t.sigma;
            float2 sc1 = cell_noise_sin_cos(words.w);
            float2 sample1 = cells[at + 1];
            sample1.x += scaled1 * sc1.y;
            if (row.autocorrelation == 0u) {{
                sample1.y += scaled1 * sc1.x;
            }}
            cells[at + 1] = sample1;
        }}
    }}
}}
"#,
        word_scale = bits(WORD_SCALE),
        octant_scale = bits(1.0 / OCTANT_WORDS as f32),
        octant_words = OCTANT_WORDS,
        sqrt_half = SQRT_HALF_WORD,
        ln2_high = bits(LN_2_HIGH),
        ln2_low = bits(LN_2_LOW),
        atanh = list(&ATANH),
        sine = list(&SINE),
        cosine = list(&COSINE),
        weyl0 = WEYL[0],
        weyl1 = WEYL[1],
        mult0 = MULTIPLIERS[0],
        mult1 = MULTIPLIERS[1],
        kernel = METAL_KERNEL,
    )
}
