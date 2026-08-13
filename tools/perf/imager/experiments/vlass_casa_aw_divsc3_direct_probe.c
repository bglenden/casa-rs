#include <complex.h>
#include <dlfcn.h>
#include <fenv.h>
#include <inttypes.h>
#include <limits.h>
#include <mach-o/dyld.h>
#include <mach-o/loader.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef float _Complex (*divsc3_fn)(
    float numerator_re,
    float numerator_im,
    float denominator_re,
    float denominator_im);

typedef struct {
    uint32_t re;
    uint32_t im;
} complex_bits;

static uint32_t float_bits(float value) {
    uint32_t bits = 0;
    memcpy(&bits, &value, sizeof(bits));
    return bits;
}

static float float_from_bits(uint32_t bits) {
    float value = 0.0F;
    memcpy(&value, &bits, sizeof(value));
    return value;
}

static complex_bits call_divsc3(
    divsc3_fn function,
    uint32_t numerator_re,
    uint32_t numerator_im,
    uint32_t denominator_re,
    uint32_t denominator_im) {
    const float _Complex value = function(
        float_from_bits(numerator_re),
        float_from_bits(numerator_im),
        float_from_bits(denominator_re),
        float_from_bits(denominator_im));
    const complex_bits result = {
        .re = float_bits(crealf(value)),
        .im = float_bits(cimagf(value)),
    };
    return result;
}

static complex_bits rust_equivalent_wide_graph(
    uint32_t numerator_re,
    uint32_t numerator_im,
    uint32_t denominator_re,
    uint32_t denominator_im) {
    const double a = (double)float_from_bits(numerator_re);
    const double b = (double)float_from_bits(numerator_im);
    const double c = (double)float_from_bits(denominator_re);
    const double d = (double)float_from_bits(denominator_im);
    const double denominator = fma(c, c, d * d);
    const double real_numerator = fma(a, c, b * d);
    const double imaginary_numerator = fma(b, c, -(a * d));
    const complex_bits result = {
        .re = float_bits((float)(real_numerator / denominator)),
        .im = float_bits((float)(imaginary_numerator / denominator)),
    };
    return result;
}

static uint64_t read_fpcr(void) {
#if defined(__aarch64__)
    uint64_t value = 0;
    __asm__ volatile("mrs %0, fpcr" : "=r"(value));
    return value;
#else
    return 0;
#endif
}

static const struct mach_header_64 *find_loaded_image(
    const char *requested_path,
    intptr_t *slide_out,
    const char **loaded_path_out) {
    char requested_realpath[PATH_MAX];
    if (realpath(requested_path, requested_realpath) == NULL) {
        return NULL;
    }
    const uint32_t image_count = _dyld_image_count();
    for (uint32_t index = 0; index < image_count; ++index) {
        const char *loaded_path = _dyld_get_image_name(index);
        if (loaded_path == NULL) {
            continue;
        }
        char loaded_realpath[PATH_MAX];
        if (realpath(loaded_path, loaded_realpath) == NULL) {
            continue;
        }
        if (strcmp(requested_realpath, loaded_realpath) == 0) {
            const struct mach_header *header = _dyld_get_image_header(index);
            if (header == NULL || header->magic != MH_MAGIC_64) {
                return NULL;
            }
            *slide_out = _dyld_get_image_vmaddr_slide(index);
            *loaded_path_out = loaded_path;
            return (const struct mach_header_64 *)header;
        }
    }
    return NULL;
}

static uint64_t text_vmaddr(const struct mach_header_64 *header) {
    const uint8_t *cursor = (const uint8_t *)(header + 1);
    for (uint32_t index = 0; index < header->ncmds; ++index) {
        const struct load_command *command =
            (const struct load_command *)cursor;
        if (command->cmd == LC_SEGMENT_64) {
            const struct segment_command_64 *segment =
                (const struct segment_command_64 *)cursor;
            if (strncmp(segment->segname, "__TEXT", sizeof(segment->segname)) ==
                0) {
                return segment->vmaddr;
            }
        }
        cursor += command->cmdsize;
    }
    return UINT64_MAX;
}

static int image_uuid(
    const struct mach_header_64 *header,
    char output[37]) {
    const uint8_t *cursor = (const uint8_t *)(header + 1);
    for (uint32_t index = 0; index < header->ncmds; ++index) {
        const struct load_command *command =
            (const struct load_command *)cursor;
        if (command->cmd == LC_UUID) {
            const struct uuid_command *uuid =
                (const struct uuid_command *)cursor;
            const uint8_t *value = uuid->uuid;
            snprintf(
                output,
                37,
                "%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-"
                "%02X%02X%02X%02X%02X%02X",
                value[0],
                value[1],
                value[2],
                value[3],
                value[4],
                value[5],
                value[6],
                value[7],
                value[8],
                value[9],
                value[10],
                value[11],
                value[12],
                value[13],
                value[14],
                value[15]);
            return 0;
        }
        cursor += command->cmdsize;
    }
    return -1;
}

static uintptr_t decode_bl_target(uintptr_t callsite, uint32_t instruction) {
    int64_t immediate = (int64_t)(instruction & 0x03ffffffU);
    if ((immediate & (1LL << 25)) != 0) {
        immediate -= 1LL << 26;
    }
    return (uintptr_t)((int64_t)callsite + immediate * 4);
}

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(
            stderr,
            "usage: %s LIBRARY CALLSITE_VMADDR HELPER_VMADDR\n",
            argv[0]);
        return 2;
    }
    char *end = NULL;
    const uint64_t callsite_vmaddr = strtoull(argv[2], &end, 0);
    if (end == argv[2] || *end != '\0') {
        fprintf(stderr, "invalid call-site vmaddr\n");
        return 2;
    }
    const uint64_t helper_vmaddr = strtoull(argv[3], &end, 0);
    if (end == argv[3] || *end != '\0') {
        fprintf(stderr, "invalid helper vmaddr\n");
        return 2;
    }

    void *handle = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        fprintf(stderr, "dlopen failed: %s\n", dlerror());
        return 3;
    }

    intptr_t slide = 0;
    const char *loaded_path = NULL;
    const struct mach_header_64 *header =
        find_loaded_image(argv[1], &slide, &loaded_path);
    if (header == NULL) {
        fprintf(stderr, "could not identify the exact loaded image\n");
        return 4;
    }
    const uint64_t text_address = text_vmaddr(header);
    if (text_address == UINT64_MAX ||
        (uintptr_t)header != (uintptr_t)(slide + (intptr_t)text_address)) {
        fprintf(stderr, "loaded Mach-O __TEXT mapping is inconsistent\n");
        return 5;
    }

    const uintptr_t callsite_runtime =
        (uintptr_t)(slide + (intptr_t)callsite_vmaddr);
    const uintptr_t helper_runtime =
        (uintptr_t)(slide + (intptr_t)helper_vmaddr);
    uint32_t call_instruction = 0;
    memcpy(
        &call_instruction,
        (const void *)callsite_runtime,
        sizeof(call_instruction));
    if ((call_instruction & 0xfc000000U) != 0x94000000U) {
        fprintf(stderr, "call site is not an AArch64 BL instruction\n");
        return 6;
    }
    const uintptr_t decoded_target =
        decode_bl_target(callsite_runtime, call_instruction);
    if (decoded_target != helper_runtime) {
        fprintf(stderr, "decoded branch target does not match helper\n");
        return 7;
    }
    Dl_info helper_info;
    memset(&helper_info, 0, sizeof(helper_info));
    if (dladdr((const void *)helper_runtime, &helper_info) == 0 ||
        helper_info.dli_fbase != (const void *)header) {
        fprintf(stderr, "helper address is not owned by the loaded image\n");
        return 8;
    }

    char uuid[37];
    if (image_uuid(header, uuid) != 0) {
        fprintf(stderr, "loaded image has no UUID\n");
        return 9;
    }

    const int rounding_before = fegetround();
    const uint64_t fpcr_before = read_fpcr();
    const divsc3_fn function = (divsc3_fn)helper_runtime;
    const complex_bits source_zero = call_divsc3(
        function,
        0x3da00f0fU,
        0x3dc30cdeU,
        0x3f6e1694U,
        0xbd1ed44bU);
    const complex_bits source_1446 = call_divsc3(
        function,
        0x39c7d0f4U,
        0xbe8d50a9U,
        0x3f7a5c92U,
        0x3c71a8aeU);
    const complex_bits comparison = rust_equivalent_wide_graph(
        0x39c7d0f4U,
        0xbe8d50a9U,
        0x3f7a5c92U,
        0x3c71a8aeU);
    const uint64_t fpcr_after = read_fpcr();
    const int rounding_after = fegetround();

    printf(
        "{\"schema\":\"casa-rs-vlass-installed-divsc3-runtime-probe-v1\","
        "\"loaded_path\":\"%s\",\"image_uuid\":\"%s\","
        "\"image_header\":\"0x%016" PRIxPTR "\","
        "\"image_slide\":\"0x%016" PRIxPTR "\","
        "\"text_vmaddr\":\"0x%016" PRIx64 "\","
        "\"callsite_vmaddr\":\"0x%016" PRIx64 "\","
        "\"callsite_runtime\":\"0x%016" PRIxPTR "\","
        "\"call_instruction\":\"0x%08" PRIx32 "\","
        "\"decoded_target\":\"0x%016" PRIxPTR "\","
        "\"helper_vmaddr\":\"0x%016" PRIx64 "\","
        "\"helper_runtime\":\"0x%016" PRIxPTR "\","
        "\"fpcr_before\":\"0x%016" PRIx64 "\","
        "\"fpcr_after\":\"0x%016" PRIx64 "\","
        "\"fegetround_before\":%d,\"fegetround_after\":%d,"
        "\"source_zero\":[%" PRIu32 ",%" PRIu32 "],"
        "\"source_1446\":[%" PRIu32 ",%" PRIu32 "],"
        "\"rust_equivalent_wide_graph\":[%" PRIu32 ",%" PRIu32 "]}\n",
        loaded_path,
        uuid,
        (uintptr_t)header,
        (uintptr_t)slide,
        text_address,
        callsite_vmaddr,
        callsite_runtime,
        call_instruction,
        decoded_target,
        helper_vmaddr,
        helper_runtime,
        fpcr_before,
        fpcr_after,
        rounding_before,
        rounding_after,
        source_zero.re,
        source_zero.im,
        source_1446.re,
        source_1446.im,
        comparison.re,
        comparison.im);
    dlclose(handle);
    return 0;
}
