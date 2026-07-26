// SPDX-License-Identifier: LGPL-3.0-or-later
//
// Local-only FFTW timing hook for casa-imaging's fft_backend_validate example.
// This is an experiment helper, not a production casa-rs FFT backend.

#include <errno.h>
#include <fftw3.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    size_t rows;
    size_t columns;
    const char *precision;
    const char *use_case;
} options_t;

typedef struct {
    double shift_in_ms;
    double execute_ms;
    double scale_ms;
    double shift_out_ms;
    double total_ms;
    double checksum;
} timing_t;

static double monotonic_seconds(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
        perror("clock_gettime");
        exit(2);
    }
    return (double)value.tv_sec + (double)value.tv_nsec * 1.0e-9;
}

static double elapsed_ms(double started) {
    return (monotonic_seconds() - started) * 1.0e3;
}

static size_t parse_size(const char *label, const char *value) {
    errno = 0;
    char *end = NULL;
    uintmax_t parsed = strtoumax(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0 ||
        parsed > SIZE_MAX) {
        fprintf(stderr, "invalid %s: %s\n", label, value);
        exit(2);
    }
    return (size_t)parsed;
}

static int parse_positive_env(const char *name, int fallback) {
    const char *value = getenv(name);
    if (value == NULL || *value == '\0') {
        return fallback;
    }
    size_t parsed = parse_size(name, value);
    if (parsed > INT32_MAX) {
        fprintf(stderr, "%s is too large: %s\n", name, value);
        exit(2);
    }
    return (int)parsed;
}

static options_t parse_options(int argc, char **argv) {
    options_t options = {
        .rows = 0,
        .columns = 0,
        .precision = NULL,
        .use_case = "benchmark",
    };
    for (int index = 1; index < argc; ++index) {
        if (index + 1 >= argc) {
            fprintf(stderr, "missing value after %s\n", argv[index]);
            exit(2);
        }
        const char *name = argv[index];
        const char *value = argv[++index];
        if (strcmp(name, "--precision") == 0) {
            options.precision = value;
        } else if (strcmp(name, "--rows") == 0) {
            options.rows = parse_size("--rows", value);
        } else if (strcmp(name, "--columns") == 0) {
            options.columns = parse_size("--columns", value);
        } else if (strcmp(name, "--use-case") == 0) {
            options.use_case = value;
        } else {
            fprintf(stderr, "unknown argument: %s\n", name);
            exit(2);
        }
    }
    if (options.rows == 0 || options.columns == 0 ||
        options.precision == NULL) {
        fprintf(stderr, "required: --precision --rows --columns\n");
        exit(2);
    }
    if (strcmp(options.precision, "f64") != 0) {
        fprintf(stderr, "local experiment supports only f64\n");
        exit(3);
    }
    if ((options.rows & 1U) != 0 || (options.columns & 1U) != 0) {
        fprintf(stderr, "centered in-place experiment requires even axes\n");
        exit(3);
    }
    if (options.rows > (size_t)INT32_MAX ||
        options.columns > (size_t)INT32_MAX) {
        fprintf(stderr, "FFTW plan axes exceed int range\n");
        exit(3);
    }
    if (options.columns > SIZE_MAX / options.rows) {
        fprintf(stderr, "element-count overflow\n");
        exit(3);
    }
    return options;
}

static void initialize_input(
    fftw_complex *values,
    size_t rows,
    size_t columns
) {
    size_t count = rows * columns;
    for (size_t index = 0; index < count; ++index) {
        uint64_t mixed = (uint64_t)index * UINT64_C(11400714819323198485);
        values[index][0] = (double)(int32_t)(mixed >> 32) * 0x1p-32;
        values[index][1] = (double)(int32_t)mixed * 0x1p-32;
    }
}

static void shift_quadrants(
    fftw_complex *values,
    size_t rows,
    size_t columns
) {
    size_t half_rows = rows / 2;
    size_t half_columns = columns / 2;
    for (size_t row = 0; row < half_rows; ++row) {
        for (size_t column = 0; column < half_columns; ++column) {
            size_t q00 = row * columns + column;
            size_t q11 =
                (row + half_rows) * columns + column + half_columns;
            size_t q10 = (row + half_rows) * columns + column;
            size_t q01 = row * columns + column + half_columns;
            double real = values[q00][0];
            double imag = values[q00][1];
            values[q00][0] = values[q11][0];
            values[q00][1] = values[q11][1];
            values[q11][0] = real;
            values[q11][1] = imag;

            real = values[q10][0];
            imag = values[q10][1];
            values[q10][0] = values[q01][0];
            values[q10][1] = values[q01][1];
            values[q01][0] = real;
            values[q01][1] = imag;
        }
    }
}

static timing_t run_inverse(
    fftw_complex *values,
    fftw_plan plan,
    size_t rows,
    size_t columns
) {
    timing_t timing = {0};
    size_t count = rows * columns;
    double total_started = monotonic_seconds();

    double stage_started = monotonic_seconds();
    shift_quadrants(values, rows, columns);
    timing.shift_in_ms = elapsed_ms(stage_started);

    stage_started = monotonic_seconds();
    fftw_execute(plan);
    timing.execute_ms = elapsed_ms(stage_started);

    stage_started = monotonic_seconds();
    double scale = 1.0 / (double)count;
    for (size_t index = 0; index < count; ++index) {
        values[index][0] *= scale;
        values[index][1] *= scale;
    }
    timing.scale_ms = elapsed_ms(stage_started);

    stage_started = monotonic_seconds();
    shift_quadrants(values, rows, columns);
    timing.shift_out_ms = elapsed_ms(stage_started);
    timing.total_ms = elapsed_ms(total_started);

    size_t middle = (rows / 2) * columns + columns / 2;
    timing.checksum = values[0][0] + values[0][1] +
                      values[middle][0] + values[middle][1];
    return timing;
}

int main(int argc, char **argv) {
    options_t options = parse_options(argc, argv);
    int threads = parse_positive_env("CASA_RS_FFTW_THREADS", 1);
    int repeats = parse_positive_env("CASA_RS_FFTW_REPEATS", 3);
    size_t count = options.rows * options.columns;

    if (fftw_init_threads() == 0) {
        fprintf(stderr, "fftw_init_threads failed\n");
        return 4;
    }
    fftw_plan_with_nthreads(threads);

    fftw_complex *values = fftw_alloc_complex(count);
    if (values == NULL) {
        fprintf(stderr, "fftw_alloc_complex failed for %zu elements\n", count);
        return 4;
    }
    initialize_input(values, options.rows, options.columns);

    int dimensions[2] = {(int)options.rows, (int)options.columns};
    double plan_started = monotonic_seconds();
    fftw_plan plan = fftw_plan_dft(
        2,
        dimensions,
        values,
        values,
        FFTW_BACKWARD,
        FFTW_ESTIMATE
    );
    double plan_ms = elapsed_ms(plan_started);
    if (plan == NULL) {
        fprintf(stderr, "fftw_plan_dft failed\n");
        fftw_free(values);
        return 4;
    }

    timing_t warmup =
        run_inverse(values, plan, options.rows, options.columns);
    timing_t best = {.total_ms = 1.0 / 0.0};
    double total_ms = 0.0;
    for (int repeat = 0; repeat < repeats; ++repeat) {
        initialize_input(values, options.rows, options.columns);
        timing_t timing =
            run_inverse(values, plan, options.rows, options.columns);
        total_ms += timing.total_ms;
        if (timing.total_ms < best.total_ms) {
            best = timing;
        }
    }

    printf(
        "{\"schema_version\":1,\"backend\":\"fftw-local-bench\","
        "\"precision\":\"f64\",\"rows\":%zu,\"columns\":%zu,"
        "\"use_case\":\"%s\",\"threads\":%d,\"repeats\":%d,"
        "\"plan_mode\":\"estimate\",\"plan_ms\":%.6f,"
        "\"warmup_ms\":%.6f,\"mean_total_ms\":%.6f,"
        "\"best_total_ms\":%.6f,\"best_shift_in_ms\":%.6f,"
        "\"best_execute_ms\":%.6f,\"best_scale_ms\":%.6f,"
        "\"best_shift_out_ms\":%.6f,\"checksum\":%.17g}\n",
        options.rows,
        options.columns,
        options.use_case,
        threads,
        repeats,
        plan_ms,
        warmup.total_ms,
        total_ms / (double)repeats,
        best.total_ms,
        best.shift_in_ms,
        best.execute_ms,
        best.scale_ms,
        best.shift_out_ms,
        best.checksum
    );

    fftw_destroy_plan(plan);
    fftw_free(values);
    fftw_cleanup_threads();
    return 0;
}
