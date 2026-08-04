// SPDX-License-Identifier: LGPL-3.0-or-later
//
// Fail-closed CASA 6.7.5.18 AWProject residual-input sample probe.

#include <synthesis/TransformMachines2/AWVisResampler.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace {

using casa::refim::AWVisResampler;
using casa::refim::VBStore;
using casacore::Array;
using casacore::Bool;
using casacore::Complex;
using casacore::DComplex;
using casacore::Double;
using casacore::Int;
using casacore::Matrix;

extern "C" void casa_aw_datatogrid_dcomplex(
    AWVisResampler*, Array<DComplex>&, VBStore&, Matrix<Double>&, const Bool&, Bool)
    asm("__ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIdEEEEvRN8"
        "casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb");

std::uint32_t f32_bits(float value) {
  std::uint32_t bits = 0;
  static_assert(sizeof(bits) == sizeof(value));
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

int required_nonnegative_integer(const char* name) {
  const char* text = std::getenv(name);
  if (text == nullptr || *text == '\0') {
    throw std::runtime_error(std::string("missing ") + name);
  }
  char* end = nullptr;
  errno = 0;
  const long value = std::strtol(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || value < 0 ||
      value > static_cast<long>(INT32_MAX)) {
    throw std::runtime_error(std::string("invalid ") + name);
  }
  return static_cast<int>(value);
}

void write_all(int descriptor, const std::string& payload) {
  const char* cursor = payload.data();
  std::size_t remaining = payload.size();
  while (remaining != 0) {
    const ssize_t written = ::write(descriptor, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(std::string("write receipt: ") + std::strerror(errno));
    }
    if (written == 0) {
      throw std::runtime_error("write receipt returned zero");
    }
    cursor += written;
    remaining -= static_cast<std::size_t>(written);
  }
}

void write_no_clobber(const std::string& path, const std::string& payload) {
  if (path.empty() || path.front() != '/') {
    throw std::runtime_error("CASA_AW_DATATOGRID_SAMPLE_OUTPUT must be absolute");
  }
  const int descriptor =
      ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
             S_IRUSR | S_IWUSR);
  if (descriptor < 0) {
    throw std::runtime_error(std::string("create receipt: ") + std::strerror(errno));
  }
  try {
    write_all(descriptor, payload);
    if (::fsync(descriptor) != 0) {
      throw std::runtime_error(std::string("fsync receipt: ") + std::strerror(errno));
    }
  } catch (...) {
    (void)::close(descriptor);
    throw;
  }
  if (::close(descriptor) != 0) {
    throw std::runtime_error(std::string("close receipt: ") + std::strerror(errno));
  }
}

void complex_bits(std::ostringstream& output, const char* label,
                  const Complex& value) {
  output << '"' << label << "_bits\":[" << f32_bits(value.real()) << ','
         << f32_bits(value.imag()) << ']';
}

void optional_cube_bits(std::ostringstream& output, const char* label,
                        const casacore::Cube<Complex>& cube, Int polarization,
                        Int channel, Int row) {
  output << '"' << label << "_shape\":[";
  const casacore::IPosition shape = cube.shape();
  for (std::size_t index = 0; index < shape.size(); ++index) {
    if (index != 0) {
      output << ',';
    }
    output << shape[index];
  }
  output << "],";
  if (shape.size() == 3 && polarization < shape[0] && channel < shape[1] &&
      row < shape[2]) {
    complex_bits(output, label, cube(polarization, channel, row));
  } else {
    output << '"' << label << "_bits\":null";
  }
}

[[noreturn]] void probe_dcomplex(AWVisResampler*, Array<DComplex>& grid,
                                 VBStore& vbs, Matrix<Double>&, const Bool& dopsf,
                                 Bool use_conjugate_frequency_cf) {
  try {
    const char* output_path = std::getenv("CASA_AW_DATATOGRID_SAMPLE_OUTPUT");
    if (output_path == nullptr) {
      throw std::runtime_error("missing CASA_AW_DATATOGRID_SAMPLE_OUTPUT");
    }
    if (dopsf || vbs.dopsf_p) {
      throw std::runtime_error("first DComplex DataToGrid call unexpectedly is PSF");
    }
    const Int row = required_nonnegative_integer("CASA_AW_DATATOGRID_SAMPLE_ROW");
    const Int channel =
        required_nonnegative_integer("CASA_AW_DATATOGRID_SAMPLE_CHANNEL");
    const Int polarization =
        required_nonnegative_integer("CASA_AW_DATATOGRID_SAMPLE_POL");
    if (row < vbs.beginRow_p || row >= vbs.endRow_p ||
        channel >= vbs.visCube_p.shape()[1] ||
        polarization >= vbs.visCube_p.shape()[0]) {
      throw std::runtime_error("requested sample lies outside first DataToGrid block");
    }
    if (vbs.rowFlag_p[row] || vbs.flagCube_p(polarization, channel, row)) {
      throw std::runtime_error("requested DataToGrid sample is flagged");
    }
    if (grid.shape().size() != 4 || grid.shape()[0] != 4096 ||
        grid.shape()[1] != 4096) {
      throw std::runtime_error("unexpected DataToGrid grid geometry");
    }

    std::ostringstream receipt;
    receipt << "{\"schema\":\"casa-vlass-aw-datatogrid-sample-v1\","
            << "\"role\":\"bounded-correctness-trace-not-performance-evidence\","
            << "\"row\":" << row << ",\"channel\":" << channel
            << ",\"polarization\":" << polarization
            << ",\"begin_row\":" << vbs.beginRow_p
            << ",\"end_row\":" << vbs.endRow_p
            << ",\"spw\":" << vbs.spwID_p
            << ",\"frequency_hz\":" << vbs.freq_p[channel]
            << ",\"imaging_weight\":" << vbs.imagingWeight_p(channel, row)
            << ",\"use_corrected\":" << (vbs.useCorrected_p ? "true" : "false")
            << ",\"use_conjugate_frequency_cf\":"
            << (use_conjugate_frequency_cf ? "true" : "false") << ',';
    optional_cube_bits(receipt, "vis_cube", vbs.visCube_p, polarization, channel,
                       row);
    receipt << ',';
    optional_cube_bits(receipt, "model_cube", vbs.modelCube_p, polarization,
                       channel, row);
    receipt << ',';
    optional_cube_bits(receipt, "corrected_cube", vbs.correctedCube_p,
                       polarization, channel, row);
    receipt << "}\n";
    write_no_clobber(output_path, receipt.str());
    std::fflush(nullptr);
    _Exit(86);
  } catch (const std::exception& error) {
    std::fprintf(stderr, "CASA AW DataToGrid sample probe rejected: %s\n",
                 error.what());
    std::fflush(nullptr);
    _Exit(87);
  }
}

#define CASA_DYLD_INTERPOSE(replacement, replacee)                             \
  __attribute__((used)) static const struct {                                  \
    const void* replacement_address;                                            \
    const void* replacee_address;                                               \
  } replacement##_interpose __attribute__((section("__DATA,__interpose"))) = { \
      reinterpret_cast<const void*>(                                            \
          reinterpret_cast<std::uintptr_t>(&replacement)),                      \
      reinterpret_cast<const void*>(                                            \
          reinterpret_cast<std::uintptr_t>(&replacee))}

CASA_DYLD_INTERPOSE(probe_dcomplex, casa_aw_datatogrid_dcomplex);

}  // namespace

extern "C" __attribute__((visibility("default"), used)) std::uint64_t
casa_aw_datatogrid_sample_ready_v1() {
  return UINT64_C(0x4341534141575331);
}
