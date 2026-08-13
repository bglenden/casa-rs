// Bounded, non-tclean oracle for CASA 6.7.5 MT-MFS arithmetic.
//
// The first mode intentionally exercises the implementation exported by the
// installed libcasa synthesis dylib rather than copying MatrixCleaner logic.

#include <synthesis/MeasurementEquations/MatrixCleaner.h>

#include <casacore/casa/Logging/LogIO.h>
#include <casacore/scimath/Mathematics/FFTServer.h>

// Test-oracle access only: changing C++ access control does not change the
// class layout or the implementation called from the installed CASA dylib.
// It lets this bounded helper print the exact Hessian/component state that the
// production class otherwise exposes only through rounded log messages.
#define private public
#include <synthesis/MeasurementEquations/MultiTermMatrixCleaner.h>
#undef private

#include <casacore/casa/Arrays/Array.h>
#include <casacore/casa/Arrays/Matrix.h>
#include <casacore/casa/Arrays/Vector.h>
#include <casacore/casa/OS/HostInfo.h>
#include <casacore/images/Images/PagedImage.h>

#include <dlfcn.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <complex>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

class ExposedMatrixCleaner final : public casa::MatrixCleaner {
public:
  void make_scale(casacore::Matrix<casacore::Float> &output,
                  casacore::Float scale_size) {
    makeScale(output, scale_size);
  }
};

std::uint32_t float_bits(float value) {
  static_assert(sizeof(value) == sizeof(std::uint32_t));
  std::uint32_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

std::uint64_t double_bits(double value) {
  static_assert(sizeof(value) == sizeof(std::uint64_t));
  std::uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

void hash_word(std::uint64_t &hash, std::uint32_t word) {
  constexpr std::uint64_t kFnvPrime = 1099511628211ULL;
  for (unsigned shift = 0; shift < 32; shift += 8) {
    hash ^= static_cast<std::uint8_t>(word >> shift);
    hash *= kFnvPrime;
  }
}

std::string float_label(float value) {
  std::ostringstream stream;
  stream << std::setprecision(9) << value;
  return stream.str();
}

void print_runtime_provenance() {
  const int host_cpus = casacore::HostInfo::numCPUs();
  const int assumed_fftw_threads = host_cpus > 1 ? host_cpus : 1;
  const bool fftw_thread_symbols =
      dlsym(RTLD_DEFAULT, "fftwf_init_threads") != nullptr &&
      dlsym(RTLD_DEFAULT, "fftwf_plan_with_nthreads") != nullptr;
  std::cout << "runtime_provenance"
            << " hostinfo_num_cpus=" << host_cpus << " fftw_thread_symbols="
            << (fftw_thread_symbols ? "true" : "false")
            << " assumed_effective_fftw_plan_threads=" << assumed_fftw_threads
            << " fftw_plan_flags=FFTW_ESTIMATE"
            << " fftw_initialization=casacore_global_once\n";
}

void print_complex_fingerprint(
    const std::string &label,
    const casacore::Matrix<casacore::Complex> &matrix) {
  const int nx = static_cast<int>(matrix.nrow());
  const int ny = static_cast<int>(matrix.ncolumn());
  constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
  std::uint64_t hash = kFnvOffsetBasis;
  std::uint64_t nonzero = 0;
  casacore::Complex sum(0.0F, 0.0F);
  float maximum_norm = 0.0F;
  int maximum_x = 0;
  int maximum_y = 0;

  for (int y = 0; y < ny; ++y) {
    for (int x = 0; x < nx; ++x) {
      const casacore::Complex value = matrix(x, y);
      const std::uint32_t real_bits = float_bits(value.real());
      const std::uint32_t imaginary_bits = float_bits(value.imag());
      hash_word(hash, real_bits);
      hash_word(hash, imaginary_bits);
      nonzero += real_bits != 0 || imaginary_bits != 0;
      sum += value;
      const float norm = std::norm(value);
      if (norm > maximum_norm) {
        maximum_norm = norm;
        maximum_x = x;
        maximum_y = y;
      }
    }
  }

  const casacore::Complex maximum = matrix(maximum_x, maximum_y);
  std::cout << "complex_matrix_summary"
            << " label=" << label << " nx=" << nx << " ny=" << ny
            << " nonzero=" << nonzero << " sum_real_bits=0x" << std::hex
            << std::setw(8) << std::setfill('0') << float_bits(sum.real())
            << " sum_imag_bits=0x" << std::setw(8)
            << float_bits(sum.imag()) << " max_norm_bits=0x" << std::setw(8)
            << float_bits(maximum_norm) << " max_x=" << std::dec << maximum_x
            << " max_y=" << maximum_y << " max_real_bits=0x" << std::hex
            << std::setw(8) << float_bits(maximum.real())
            << " max_imag_bits=0x" << std::setw(8)
            << float_bits(maximum.imag()) << " full_fnv1a64=0x"
            << std::setw(16) << hash << std::dec << std::setfill(' ') << '\n';

  const std::array<std::array<int, 2>, 16> requested_coordinates = {{
      {{0, 0}},
      {{1, 0}},
      {{0, 1}},
      {{1, 1}},
      {{2, 1}},
      {{3, 7}},
      {{nx / 8, ny / 8}},
      {{nx / 4, ny / 4}},
      {{nx / 2, ny / 4}},
      {{nx - 1, ny / 4}},
      {{nx / 4, ny / 2}},
      {{nx / 2, ny / 2}},
      {{nx - 1, ny / 2}},
      {{1, ny - 1}},
      {{nx / 2, ny - 1}},
      {{nx - 1, ny - 1}},
  }};
  std::vector<std::array<int, 2>> printed_coordinates;
  for (const auto &coordinate : requested_coordinates) {
    const int x = std::clamp(coordinate[0], 0, nx - 1);
    const int y = std::clamp(coordinate[1], 0, ny - 1);
    const std::array<int, 2> bounded = {{x, y}};
    if (std::find(printed_coordinates.begin(), printed_coordinates.end(),
                  bounded) != printed_coordinates.end()) {
      continue;
    }
    printed_coordinates.push_back(bounded);
    const casacore::Complex value = matrix(x, y);
    std::cout << "complex_matrix_cell"
              << " label=" << label << " x=" << x << " y=" << y
              << " real_bits=0x" << std::hex << std::setw(8)
              << std::setfill('0') << float_bits(value.real())
              << " imag_bits=0x" << std::setw(8)
              << float_bits(value.imag()) << std::dec << std::setfill(' ')
              << '\n';
  }
}

void print_complex_difference(
    const std::string &label,
    const casacore::Matrix<casacore::Complex> &left,
    const casacore::Matrix<casacore::Complex> &right) {
  if (left.shape() != right.shape()) {
    throw std::invalid_argument("complex matrix difference shape mismatch");
  }
  constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
  std::uint64_t hash = kFnvOffsetBasis;
  std::uint64_t changed = 0;
  const int nx = static_cast<int>(left.nrow());
  const int ny = static_cast<int>(left.ncolumn());
  for (int y = 0; y < ny; ++y) {
    for (int x = 0; x < nx; ++x) {
      const casacore::Complex left_value = left(x, y);
      const casacore::Complex right_value = right(x, y);
      const std::uint32_t left_real_bits = float_bits(left_value.real());
      const std::uint32_t left_imaginary_bits = float_bits(left_value.imag());
      const std::uint32_t right_real_bits = float_bits(right_value.real());
      const std::uint32_t right_imaginary_bits =
          float_bits(right_value.imag());
      if (left_real_bits != right_real_bits ||
          left_imaginary_bits != right_imaginary_bits) {
        ++changed;
        hash_word(hash, static_cast<std::uint32_t>(x));
        hash_word(hash, static_cast<std::uint32_t>(y));
        hash_word(hash, left_real_bits);
        hash_word(hash, left_imaginary_bits);
        hash_word(hash, right_real_bits);
        hash_word(hash, right_imaginary_bits);
      }
    }
  }
  std::cout << "complex_matrix_difference"
            << " label=" << label << " changed=" << changed
            << " changed_fnv1a64=0x" << std::hex << std::setw(16)
            << std::setfill('0') << hash << std::dec << std::setfill(' ')
            << '\n';
}

void print_scale(int image_size, float scale_size) {
  casacore::Matrix<casacore::Float> scale(image_size, image_size);
  ExposedMatrixCleaner cleaner;
  cleaner.make_scale(scale, scale_size);

  constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
  std::uint64_t full_hash = kFnvOffsetBasis;
  std::uint64_t support_hash = kFnvOffsetBasis;
  std::uint64_t nonzero = 0;
  float sum = 0.0F;

  for (int y = 0; y < image_size; ++y) {
    for (int x = 0; x < image_size; ++x) {
      const float value = scale(x, y);
      const std::uint32_t bits = float_bits(value);
      hash_word(full_hash, bits);
      sum += value;
      if (bits != 0) {
        ++nonzero;
        hash_word(support_hash, static_cast<std::uint32_t>(x));
        hash_word(support_hash, static_cast<std::uint32_t>(y));
        hash_word(support_hash, bits);
      }
    }
  }

  const int center = image_size / 2;
  std::cout << "scale_summary"
            << " image_size=" << image_size
            << " scale_size=" << std::setprecision(9) << scale_size
            << " nonzero=" << nonzero << " sum_bits=0x" << std::hex
            << std::setw(8) << std::setfill('0') << float_bits(sum)
            << " center_bits=0x" << std::setw(8)
            << float_bits(scale(center, center)) << " full_fnv1a64=0x"
            << std::setw(16) << full_hash << " support_fnv1a64=0x"
            << std::setw(16) << support_hash << std::dec << std::setfill(' ')
            << '\n';

  for (int y = 0; y < image_size; ++y) {
    for (int x = 0; x < image_size; ++x) {
      const std::uint32_t bits = float_bits(scale(x, y));
      if (bits != 0) {
        std::cout << "scale_cell"
                  << " scale_size=" << std::setprecision(9) << scale_size
                  << " x=" << x << " y=" << y << " bits=0x" << std::hex
                  << std::setw(8) << std::setfill('0') << bits << std::dec
                  << std::setfill(' ') << '\n';
      }
    }
  }
}

std::array<int, 2>
maximum_absolute_position(const casacore::Matrix<casacore::Float> &matrix) {
  const int nx = static_cast<int>(matrix.nrow());
  const int ny = static_cast<int>(matrix.ncolumn());
  float maximum_absolute = -1.0F;
  std::array<int, 2> position = {{0, 0}};
  for (int y = 0; y < ny; ++y) {
    for (int x = 0; x < nx; ++x) {
      const float absolute = std::abs(matrix(x, y));
      if (absolute > maximum_absolute) {
        maximum_absolute = absolute;
        position = {{x, y}};
      }
    }
  }
  return position;
}

casacore::Matrix<casacore::Float> load_image(const std::string &path) {
  if (!std::filesystem::is_directory(path)) {
    throw std::invalid_argument("CASA image table not found: " + path);
  }
  casacore::PagedImage<casacore::Float> image(path);
  casacore::Array<casacore::Float> pixels = image.get(true);
  if (pixels.ndim() != 2) {
    throw std::invalid_argument("expected two nondegenerate axes in " + path +
                                ", got shape " +
                                pixels.shape().toString().c_str());
  }
  return casacore::Matrix<casacore::Float>(pixels);
}

std::array<casacore::Matrix<casacore::Float>, 3>
load_psfs(const std::string &products);

void print_matrix_fingerprint(const std::string &label,
                              const casacore::Matrix<casacore::Float> &matrix);

void print_c2r_peaks(const std::string &label,
                     const casacore::Matrix<casacore::Float> &matrix,
                     const std::array<int, 2> &reference_peak) {
  const auto maximum = maximum_absolute_position(matrix);
  const int center_x = static_cast<int>(matrix.nrow()) / 2;
  const int center_y = static_cast<int>(matrix.ncolumn()) / 2;
  std::cout << "c2r_peaks"
            << " label=" << label << " reference_x=" << reference_peak[0]
            << " reference_y=" << reference_peak[1]
            << " reference_bits=0x" << std::hex << std::setw(8)
            << std::setfill('0')
            << float_bits(matrix(reference_peak[0], reference_peak[1]))
            << " center_x=" << std::dec << center_x
            << " center_y=" << center_y << " center_bits=0x" << std::hex
            << std::setw(8) << float_bits(matrix(center_x, center_y))
            << " max_abs_x=" << std::dec << maximum[0]
            << " max_abs_y=" << maximum[1] << " max_abs_value_bits=0x"
            << std::hex << std::setw(8)
            << float_bits(matrix(maximum[0], maximum[1])) << std::dec
            << std::setfill(' ') << '\n';
}

void print_spectra(const std::string &products,
                   const std::vector<float> &requested_scales) {
  auto psfs = load_psfs(products);
  const int nx = static_cast<int>(psfs[0].nrow());
  const int ny = static_cast<int>(psfs[0].ncolumn());
  for (const auto &psf : psfs) {
    if (static_cast<int>(psf.nrow()) != nx ||
        static_cast<int>(psf.ncolumn()) != ny) {
      throw std::invalid_argument("PSF shapes do not match");
    }
  }
  casacore::FFTServer<casacore::Float, casacore::Complex> fft(
      casacore::IPosition(2, nx, ny));
  const std::array<int, 2> reference_peak = maximum_absolute_position(psfs[0]);
  std::cout << "spectrum_reference_peak"
            << " x=" << reference_peak[0] << " y=" << reference_peak[1]
            << " psf_tt0_bits=0x" << std::hex << std::setw(8)
            << std::setfill('0')
            << float_bits(psfs[0](reference_peak[0], reference_peak[1]))
            << std::dec << std::setfill(' ') << '\n';

  std::vector<casacore::Matrix<casacore::Complex>> scale_spectra;
  scale_spectra.reserve(requested_scales.size());
  for (const float scale_size : requested_scales) {
    casacore::Matrix<casacore::Float> scale(nx, ny);
    ExposedMatrixCleaner cleaner;
    cleaner.make_scale(scale, scale_size);
    casacore::Matrix<casacore::Complex> scale_spectrum;
    fft.fft0(scale_spectrum, scale, false);
    const std::string label = "scale_" + float_label(scale_size) + "_r2c";
    print_complex_fingerprint(label, scale_spectrum);
    scale_spectra.push_back(scale_spectrum.copy());
  }

  for (int psf_term = 0; psf_term < 3; ++psf_term) {
    casacore::Matrix<casacore::Complex> psf_spectrum;
    fft.fft0(psf_spectrum, psfs[psf_term], false);
    const std::string psf_label =
        "psf_tt" + std::to_string(psf_term) + "_r2c";
    print_complex_fingerprint(psf_label, psf_spectrum);

    for (std::size_t scale_index = 0;
         scale_index < requested_scales.size(); ++scale_index) {
      const float scale_size = requested_scales[scale_index];
      const auto &scale_spectrum = scale_spectra[scale_index];
      const std::string stem = "psf_tt" + std::to_string(psf_term) +
                               "_scale_" + float_label(scale_size);

      casacore::Matrix<casacore::Complex> psf_times_scale;
      psf_times_scale.assign(psf_spectrum * scale_spectrum);
      print_complex_fingerprint(stem + "_after_psf_scale", psf_times_scale);

      casacore::Matrix<casacore::Complex> staged_product;
      staged_product.assign(psf_times_scale * scale_spectrum);
      print_complex_fingerprint(stem + "_after_staged_psf_scale_scale",
                                staged_product);

      casacore::Matrix<casacore::Complex> expression_product;
      expression_product.assign((psf_spectrum * scale_spectrum) *
                                scale_spectrum);
      print_complex_fingerprint(stem + "_after_expression_psf_scale_scale",
                                expression_product);
      print_complex_difference(stem + "_staged_vs_expression",
                               staged_product, expression_product);

      // MultiTermMatrixCleaner preallocates its real work image.  Preserve
      // that exact even-sized logical shape here; an unshaped FFTServer
      // destination cannot infer whether a 2049-bin half spectrum came from
      // 4096 or 4097 real samples.
      casacore::Matrix<casacore::Float> inverse(nx, ny);
      fft.fft0(inverse, expression_product, false);
      print_matrix_fingerprint(stem + "_c2r", inverse);
      print_c2r_peaks(stem + "_c2r", inverse, reference_peak);
    }
  }
}

std::array<casacore::Matrix<casacore::Float>, 3>
load_psfs(const std::string &products) {
  return {
      load_image(products + "/casa.psf.tt0"),
      load_image(products + "/casa.psf.tt1"),
      load_image(products + "/casa.psf.tt2"),
  };
}

void configure_cleaner(casa::MultiTermMatrixCleaner &cleaner,
                       const std::vector<float> &requested_scales,
                       int image_size) {
  casacore::Vector<casacore::Float> scales(requested_scales.size());
  for (std::size_t index = 0; index < requested_scales.size(); ++index) {
    scales[index] = requested_scales[index];
  }
  if (!cleaner.setscales(scales)) {
    throw std::runtime_error("MultiTermMatrixCleaner::setscales failed");
  }
  cleaner.setSmallScaleBias(0.6F);
  if (!cleaner.setntaylorterms(2)) {
    throw std::runtime_error("MultiTermMatrixCleaner::setntaylorterms failed");
  }
  if (!cleaner.initialise(image_size, image_size)) {
    throw std::runtime_error("MultiTermMatrixCleaner::initialise failed");
  }
}

void set_psfs(casa::MultiTermMatrixCleaner &cleaner,
              std::array<casacore::Matrix<casacore::Float>, 3> &psfs) {
  for (int order = 0; order < 3; ++order) {
    if (!cleaner.setpsf(order, psfs[order])) {
      throw std::runtime_error("MultiTermMatrixCleaner::setpsf failed");
    }
  }
}

void print_inverse(float scale,
                   const casacore::Matrix<casacore::Double> &inverse) {
  const int rows = static_cast<int>(inverse.nrow());
  const int columns = static_cast<int>(inverse.ncolumn());
  std::cout << "inverse_hessian"
            << " scale_size=" << std::setprecision(9) << scale
            << " shape=" << inverse.shape() << '\n';
  for (int row = 0; row < rows; ++row) {
    for (int column = 0; column < columns; ++column) {
      const double value = inverse(row, column);
      std::cout << "inverse_hessian_cell"
                << " scale_size=" << std::setprecision(9) << scale
                << " row=" << row << " column=" << column
                << " value=" << std::setprecision(17) << value << " bits=0x"
                << std::hex << std::setw(16) << std::setfill('0')
                << double_bits(value) << " cast_f32_bits=0x" << std::setw(8)
                << float_bits(static_cast<float>(value)) << std::dec
                << std::setfill(' ') << '\n';
    }
  }
}

void print_hessian(float scale,
                   const casacore::Matrix<casacore::Double> &hessian) {
  const int rows = static_cast<int>(hessian.nrow());
  const int columns = static_cast<int>(hessian.ncolumn());
  std::cout << "hessian"
            << " scale_size=" << std::setprecision(9) << scale
            << " shape=" << hessian.shape() << '\n';
  for (int row = 0; row < rows; ++row) {
    for (int column = 0; column < columns; ++column) {
      const double value = hessian(row, column);
      std::cout << "hessian_cell"
                << " scale_size=" << std::setprecision(9) << scale
                << " row=" << row << " column=" << column
                << " value=" << std::setprecision(17) << value << " bits=0x"
                << std::hex << std::setw(16) << std::setfill('0')
                << double_bits(value) << " source_f32_bits=0x" << std::setw(8)
                << float_bits(static_cast<float>(value)) << std::dec
                << std::setfill(' ') << '\n';
    }
  }
}

void print_hessians(const std::string &products,
                    const std::vector<float> &requested_scales) {
  auto psfs = load_psfs(products);
  const int image_size = static_cast<int>(psfs[0].nrow());
  if (static_cast<int>(psfs[0].ncolumn()) != image_size) {
    throw std::invalid_argument("PSF must be square");
  }

  for (const float scale : requested_scales) {
    casa::MultiTermMatrixCleaner cleaner;
    configure_cleaner(cleaner, {scale}, image_size);
    set_psfs(cleaner, psfs);
    const int status = cleaner.computeHessianPeak();
    std::cout << "hessian_status"
              << " scale_size=" << std::setprecision(9) << scale
              << " status=" << status << '\n';
    if (status != 0) {
      throw std::runtime_error(
          "MultiTermMatrixCleaner::computeHessianPeak failed");
    }
    print_hessian(scale, cleaner.matA_p[0]);
    casacore::Matrix<casacore::Double> inverse;
    if (!cleaner.getinvhessian(inverse)) {
      throw std::runtime_error("MultiTermMatrixCleaner::getinvhessian failed");
    }
    print_inverse(scale, inverse);
  }
}

void print_matrix_fingerprint(const std::string &label,
                              const casacore::Matrix<casacore::Float> &matrix) {
  const int nx = static_cast<int>(matrix.nrow());
  const int ny = static_cast<int>(matrix.ncolumn());
  constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
  std::uint64_t hash = kFnvOffsetBasis;
  std::uint64_t nonzero = 0;
  float sum = 0.0F;
  float maximum_absolute = 0.0F;
  for (int y = 0; y < ny; ++y) {
    for (int x = 0; x < nx; ++x) {
      const float value = matrix(x, y);
      hash_word(hash, float_bits(value));
      sum += value;
      maximum_absolute = std::max(maximum_absolute, std::abs(value));
      nonzero += float_bits(value) != 0;
    }
  }
  std::cout << "matrix_summary"
            << " label=" << label << " nx=" << matrix.nrow()
            << " ny=" << matrix.ncolumn() << " nonzero=" << nonzero
            << " sum_bits=0x" << std::hex << std::setw(8) << std::setfill('0')
            << float_bits(sum) << " max_abs_bits=0x" << std::setw(8)
            << float_bits(maximum_absolute) << " full_fnv1a64=0x"
            << std::setw(16) << hash << std::dec << std::setfill(' ') << '\n';
}

void print_matrix_difference(const std::string &label,
                             const casacore::Matrix<casacore::Float> &before,
                             const casacore::Matrix<casacore::Float> &after) {
  if (before.shape() != after.shape()) {
    throw std::invalid_argument("matrix difference shape mismatch");
  }
  const int nx = static_cast<int>(before.nrow());
  const int ny = static_cast<int>(before.ncolumn());
  constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
  std::uint64_t hash = kFnvOffsetBasis;
  std::uint64_t changed = 0;
  int min_x = nx;
  int max_x = -1;
  int min_y = ny;
  int max_y = -1;
  float max_absolute_delta = 0.0F;

  for (int y = 0; y < ny; ++y) {
    for (int x = 0; x < nx; ++x) {
      const std::uint32_t before_bits = float_bits(before(x, y));
      const std::uint32_t after_bits = float_bits(after(x, y));
      if (before_bits != after_bits) {
        ++changed;
        min_x = std::min(min_x, x);
        max_x = std::max(max_x, x);
        min_y = std::min(min_y, y);
        max_y = std::max(max_y, y);
        max_absolute_delta =
            std::max(max_absolute_delta, std::abs(after(x, y) - before(x, y)));
        hash_word(hash, static_cast<std::uint32_t>(x));
        hash_word(hash, static_cast<std::uint32_t>(y));
        hash_word(hash, before_bits);
        hash_word(hash, after_bits);
      }
    }
  }

  std::cout << "matrix_difference"
            << " label=" << label << " changed=" << changed
            << " min_x=" << min_x << " max_x=" << max_x << " min_y=" << min_y
            << " max_y=" << max_y << " max_abs_delta_bits=0x" << std::hex
            << std::setw(8) << std::setfill('0')
            << float_bits(max_absolute_delta) << " changed_fnv1a64=0x"
            << std::setw(16) << hash << std::dec << std::setfill(' ') << '\n';

  if (changed <= 2048) {
    for (int y = 0; y < ny; ++y) {
      for (int x = 0; x < nx; ++x) {
        const std::uint32_t before_bits = float_bits(before(x, y));
        const std::uint32_t after_bits = float_bits(after(x, y));
        if (before_bits != after_bits) {
          std::cout << "matrix_difference_cell"
                    << " label=" << label << " x=" << x << " y=" << y
                    << " before_bits=0x" << std::hex << std::setw(8)
                    << std::setfill('0') << before_bits << " after_bits=0x"
                    << std::setw(8) << after_bits << " delta_bits=0x"
                    << std::setw(8) << float_bits(after(x, y) - before(x, y))
                    << std::dec << std::setfill(' ') << '\n';
        }
      }
    }
  }
}

void run_minor_cycle(const std::string &products, int iterations, float gain,
                     float threshold, float small_scale_bias) {
  auto psfs = load_psfs(products);
  std::array<casacore::Matrix<casacore::Float>, 2> residuals = {
      load_image(products + "/casa.residual.tt0"),
      load_image(products + "/casa.residual.tt1"),
  };
  std::array<casacore::Matrix<casacore::Float>, 2> models = {
      load_image(products + "/casa.model.tt0"),
      load_image(products + "/casa.model.tt1"),
  };
  casacore::Matrix<casacore::Float> mask = load_image(products + "/casa.mask");
  const std::array<casacore::Matrix<casacore::Float>, 2> initial_models = {
      models[0].copy(),
      models[1].copy(),
  };

  const int image_size = static_cast<int>(psfs[0].nrow());
  casa::MultiTermMatrixCleaner cleaner;
  casacore::Vector<casacore::Float> scales(3);
  scales[0] = 0.0F;
  scales[1] = 5.0F;
  scales[2] = 12.0F;
  if (!cleaner.setscales(scales)) {
    throw std::runtime_error("MultiTermMatrixCleaner::setscales failed");
  }
  cleaner.setSmallScaleBias(small_scale_bias);
  if (!cleaner.setntaylorterms(2) ||
      !cleaner.initialise(image_size, image_size)) {
    throw std::runtime_error("MultiTermMatrixCleaner setup failed");
  }
  set_psfs(cleaner, psfs);
  for (int order = 0; order < 2; ++order) {
    if (!cleaner.setresidual(order, residuals[order]) ||
        !cleaner.setmodel(order, models[order])) {
      throw std::runtime_error(
          "MultiTermMatrixCleaner residual/model setup failed");
    }
  }
  if (!cleaner.setmask(mask)) {
    throw std::runtime_error("MultiTermMatrixCleaner::setmask failed");
  }

  const int hessian_status = cleaner.computeHessianPeak();
  std::cout << "hessian_status scale_size=0 status=" << hessian_status << '\n';
  if (hessian_status != 0) {
    throw std::runtime_error(
        "MultiTermMatrixCleaner::computeHessianPeak failed");
  }
  for (int scale_index = 0; scale_index < cleaner.nscales_p; ++scale_index) {
    const float scale = cleaner.scaleSizes_p[scale_index];
    print_hessian(scale, cleaner.matA_p[scale_index]);
    print_inverse(scale, cleaner.invMatA_p[scale_index]);
  }

  for (int order = 0; order < 2; ++order) {
    print_matrix_fingerprint("model_before_tt" + std::to_string(order),
                             initial_models[order]);
    print_matrix_fingerprint("residual_before_tt" + std::to_string(order),
                             residuals[order]);
  }

  const int completed = cleaner.mtclean(iterations, 0.0F, gain, threshold);
  std::cout << "minor_cycle_result"
            << " requested_iterations=" << iterations
            << " completed_iterations=" << completed << " gain_bits=0x"
            << std::hex << std::setw(8) << std::setfill('0') << float_bits(gain)
            << " threshold_bits=0x" << std::setw(8) << float_bits(threshold)
            << " small_scale_bias_bits=0x" << std::setw(8)
            << float_bits(small_scale_bias)
            << std::dec << std::setfill(' ') << " peak_residual_bits=0x"
            << std::hex << std::setw(8) << std::setfill('0')
            << float_bits(cleaner.getpeakresidual()) << std::dec
            << std::setfill(' ') << '\n';
  if (completed < 0) {
    throw std::runtime_error("MultiTermMatrixCleaner::mtclean failed");
  }

  std::cout << "minor_cycle_component"
            << " scale_index=" << cleaner.maxscaleindex_p
            << " scale_size=" << std::setprecision(9)
            << cleaner.scaleSizes_p[cleaner.maxscaleindex_p]
            << " x=" << cleaner.globalmaxpos_p[0]
            << " y=" << cleaner.globalmaxpos_p[1] << " candidate_bits=0x"
            << std::hex << std::setw(8) << std::setfill('0')
            << float_bits(cleaner.globalmaxval_p) << std::dec
            << std::setfill(' ') << '\n';
  for (int term = 0; term < cleaner.ntaylor_p; ++term) {
    const float coefficient =
        cleaner.matCoeffs_p[cleaner.IND2(term, cleaner.maxscaleindex_p)](
            cleaner.globalmaxpos_p);
    std::cout << "minor_cycle_component_coefficient"
              << " term=" << term << " value=" << std::setprecision(9)
              << coefficient << " bits=0x" << std::hex << std::setw(8)
              << std::setfill('0') << float_bits(coefficient) << std::dec
              << std::setfill(' ') << '\n';
  }

  for (int order = 0; order < 2; ++order) {
    if (!cleaner.getmodel(order, models[order]) ||
        !cleaner.getresidual(order, residuals[order])) {
      throw std::runtime_error(
          "MultiTermMatrixCleaner output retrieval failed");
    }
    print_matrix_fingerprint("model_after_tt" + std::to_string(order),
                             models[order]);
    print_matrix_difference("model_tt" + std::to_string(order),
                            initial_models[order], models[order]);
    print_matrix_fingerprint("residual_after_tt" + std::to_string(order),
                             residuals[order]);
  }
}

int parse_positive_int(const char *text, const char *name) {
  try {
    const int value = std::stoi(text);
    if (value <= 0) {
      throw std::invalid_argument("not positive");
    }
    return value;
  } catch (const std::exception &) {
    throw std::invalid_argument(std::string(name) +
                                " must be a positive integer");
  }
}

float parse_nonnegative_float(const char *text, const char *name) {
  try {
    const float value = std::stof(text);
    if (!(value >= 0.0F)) {
      throw std::invalid_argument("negative or NaN");
    }
    return value;
  } catch (const std::exception &) {
    throw std::invalid_argument(std::string(name) +
                                " must be a nonnegative float");
  }
}

float parse_float(const char *text, const char *name) {
  try {
    const float value = std::stof(text);
    if (!std::isfinite(value)) {
      throw std::invalid_argument("not finite");
    }
    return value;
  } catch (const std::exception &) {
    throw std::invalid_argument(std::string(name) + " must be a finite float");
  }
}

} // namespace

int main(int argc, char **argv) {
  try {
    if (argc < 2) {
      std::cerr << "usage: " << argv[0]
                << " scale [image-size [scale-size ...]]\n"
                << "       " << argv[0]
                << " hessian PRODUCT-DIRECTORY [scale-size ...]\n"
                << "       " << argv[0]
                << " spectrum PRODUCT-DIRECTORY [scale-size ...]\n"
                << "       " << argv[0]
                << " minor PRODUCT-DIRECTORY [iterations [gain [threshold "
                   "[small-scale-bias]]]]\n";
      return 2;
    }

    const std::string mode = argv[1];
    print_runtime_provenance();
    if (mode == "scale") {
      const int image_size =
          argc >= 3 ? parse_positive_int(argv[2], "image-size") : 4096;
      if (argc >= 4) {
        for (int argument = 3; argument < argc; ++argument) {
          print_scale(image_size,
                      parse_nonnegative_float(argv[argument], "scale-size"));
        }
      } else {
        print_scale(image_size, 5.0F);
        print_scale(image_size, 12.0F);
      }
      return 0;
    }

    if (mode == "hessian") {
      if (argc < 3) {
        throw std::invalid_argument(
            "hessian mode requires a product directory");
      }
      std::vector<float> scales;
      for (int argument = 3; argument < argc; ++argument) {
        scales.push_back(parse_nonnegative_float(argv[argument], "scale-size"));
      }
      if (scales.empty()) {
        scales = {0.0F, 5.0F, 12.0F};
      }
      print_hessians(argv[2], scales);
      return 0;
    }

    if (mode == "spectrum") {
      if (argc < 3) {
        throw std::invalid_argument(
            "spectrum mode requires a product directory");
      }
      std::vector<float> scales;
      for (int argument = 3; argument < argc; ++argument) {
        scales.push_back(parse_nonnegative_float(argv[argument], "scale-size"));
      }
      if (scales.empty()) {
        scales = {0.0F, 5.0F, 12.0F};
      }
      print_spectra(argv[2], scales);
      return 0;
    }

    if (mode == "minor") {
      if (argc < 3 || argc > 7) {
        throw std::invalid_argument(
            "minor mode requires PRODUCT-DIRECTORY and optionally "
            "iterations, gain, threshold, and small-scale-bias");
      }
      const int iterations =
          argc >= 4 ? parse_positive_int(argv[3], "iterations") : 1;
      const float gain = argc >= 5 ? parse_float(argv[4], "gain") : 0.1F;
      const float threshold =
          argc >= 6 ? parse_float(argv[5], "threshold") : 0.0F;
      const float small_scale_bias =
          argc >= 7 ? parse_float(argv[6], "small-scale-bias") : 0.6F;
      run_minor_cycle(argv[2], iterations, gain, threshold, small_scale_bias);
      return 0;
    }

    throw std::invalid_argument("unknown mode: " + mode);
  } catch (const std::exception &error) {
    std::cerr << "casa_mtmfs_arithmetic_oracle: " << error.what() << '\n';
    return 1;
  }
}
