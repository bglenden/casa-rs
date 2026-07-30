// Bounded CASA 6.7.5.18 AWVisResampler::DataToGrid input oracle.
//
// This diagnostic dylib interposes only the two explicitly-instantiated
// refim::AWVisResampler::DataToGridImpl_p symbols.  For the expected
// DComplex, non-PSF call it traverses CASA's own VBStore, CFBuffer, and
// POINTING phase-gradient state in the same row/channel/polarization/Mueller/
// Y/X order as AWVisResampler.cc.  It never obtains or writes grid storage.
// It atomically writes a receipt and terminates the process with _Exit(86)
// after a configured number of accepted sources, or at the end of the first
// DataToGrid block.  Every unexpected call shape fails closed with _Exit(87).
//
// The portable rolling hash intentionally excludes CASA- or casa-rs-private
// block, row, channel, and pointing-group identifiers.  Its per-role byte
// stream is, in order (all integers little-endian, floats by IEEE bits):
//
//   source ordinal u64, role ordinal u64,
//   frequency/u/v/w f64, imaging weight/Taylor x/TT0 term weight f32,
//   phased residual/value Complex<f32>,
//   CF frequency/W f64, Mueller u32, PA-degrees f64,
//   loc/off x/y i64, conjugate bool,
//   unphased normalization Complex<f32>/Complex<f64>,
//   tap count u64, nested phased-tap FNV-1a-64.
//
// casa-rs can emit this exact projection alongside its richer internal audit.
// Separate component hashes make the first mismatching semantic layer visible.

#include <synthesis/TransformMachines2/AWVisResampler.h>

#include <casacore/casa/BasicSL/Constants.h>
#include <casacore/casa/BasicSL/Complex.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

using casa::CFCell;
using casa::refim::AWVisResampler;
using casa::refim::CFBuffer;
using casa::refim::PolMapType;
using casa::refim::VB2CFBMap;
using casa::refim::VBStore;
using casacore::Array;
using casacore::Bool;
using casacore::Complex;
using casacore::DComplex;
using casacore::Double;
using casacore::Float;
using casacore::Int;
using casacore::Matrix;
using casacore::Vector;
using casacore::uInt;

constexpr std::uint64_t kFnvOffset = UINT64_C(0xcbf29ce484222325);
constexpr std::uint64_t kFnvPrime = UINT64_C(0x00000100000001b3);
constexpr int kCompletedExit = 86;
constexpr int kRejectedExit = 87;
constexpr char kCasaCommit[] = "418bb1a26df7c4aba663ff123b038b75a6fa0295";

class Fnv1a64 {
 public:
  void bytes(const void* pointer, std::size_t size) {
    const auto* octets = static_cast<const unsigned char*>(pointer);
    for (std::size_t index = 0; index < size; ++index) {
      value_ ^= static_cast<std::uint64_t>(octets[index]);
      value_ *= kFnvPrime;
    }
  }

  void u64(std::uint64_t value) {
    std::array<unsigned char, 8> bytes{};
    for (std::size_t index = 0; index < bytes.size(); ++index) {
      bytes[index] = static_cast<unsigned char>((value >> (index * 8)) & 0xffU);
    }
    this->bytes(bytes.data(), bytes.size());
  }

  void i64(std::int64_t value) { u64(static_cast<std::uint64_t>(value)); }

  void u32(std::uint32_t value) {
    std::array<unsigned char, 4> bytes{};
    for (std::size_t index = 0; index < bytes.size(); ++index) {
      bytes[index] = static_cast<unsigned char>((value >> (index * 8)) & 0xffU);
    }
    this->bytes(bytes.data(), bytes.size());
  }

  void boolean(bool value) {
    const unsigned char byte = value ? 1U : 0U;
    bytes(&byte, 1);
  }

  void f32(Float value) {
    static_assert(sizeof(Float) == sizeof(std::uint32_t));
    std::uint32_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    u32(bits);
  }

  void f64(Double value) {
    static_assert(sizeof(Double) == sizeof(std::uint64_t));
    std::uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    u64(bits);
  }

  void complex32(const Complex& value) {
    f32(value.real());
    f32(value.imag());
  }

  void complex64(const DComplex& value) {
    f64(value.real());
    f64(value.imag());
  }

  [[nodiscard]] std::uint64_t value() const { return value_; }

 private:
  std::uint64_t value_ = kFnvOffset;
};

struct Config {
  std::string output;
  std::uint64_t max_sources = 0;
  std::uint64_t checkpoint_interval = 1024;
  Int expected_nxy = 0;
};

struct Checkpoint {
  std::uint64_t sources;
  std::uint64_t portable_hash;
};

struct Hashes {
  Fnv1a64 portable;
  Fnv1a64 source;
  Fnv1a64 value;
  Fnv1a64 cell;
  Fnv1a64 placement;
  Fnv1a64 normalization;
  Fnv1a64 taps;
};

struct Role {
  std::uint64_t ordinal = 0;
  Double frequency_hz = 0.0;
  Double u_lambda = 0.0;
  Double v_lambda = 0.0;
  Double w_lambda = 0.0;
  Float weight = 0.0F;
  Float taylor_x = 0.0F;
  Float term_weight = 0.0F;
  Complex residual = Complex(0.0F);
  Complex value = Complex(0.0F);
  Double cell_frequency_hz = 0.0;
  Double cell_w_lambda = 0.0;
  std::uint32_t mueller = 0;
  Double cell_pa_deg = 0.0;
  std::int64_t loc_x = 0;
  std::int64_t loc_y = 0;
  std::int64_t off_x = 0;
  std::int64_t off_y = 0;
  bool conjugate_for_grid = false;
  Complex normalization32 = Complex(0.0F);
  DComplex normalization64 = DComplex(0.0);
  std::uint64_t tap_count = 0;
  std::uint64_t tap_hash = kFnvOffset;
};

[[noreturn]] void raw_exit(int code) {
  std::fflush(nullptr);
  _Exit(code);
}

std::string json_escape(std::string_view value) {
  std::ostringstream output;
  for (const unsigned char byte : value) {
    switch (byte) {
      case '"':
        output << "\\\"";
        break;
      case '\\':
        output << "\\\\";
        break;
      case '\b':
        output << "\\b";
        break;
      case '\f':
        output << "\\f";
        break;
      case '\n':
        output << "\\n";
        break;
      case '\r':
        output << "\\r";
        break;
      case '\t':
        output << "\\t";
        break;
      default:
        if (byte < 0x20U) {
          constexpr char hex[] = "0123456789abcdef";
          output << "\\u00" << hex[(byte >> 4U) & 0x0fU] << hex[byte & 0x0fU];
        } else {
          output << static_cast<char>(byte);
        }
    }
  }
  return output.str();
}

void write_all(int descriptor, std::string_view payload) {
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

void atomic_receipt(const std::string& path, std::string_view payload) {
  if (path.empty() || path.front() != '/') {
    throw std::runtime_error("receipt path must be absolute");
  }
  if (::access(path.c_str(), F_OK) == 0) {
    throw std::runtime_error("refusing to overwrite existing receipt " + path);
  }
  const std::string temporary =
      path + ".tmp." + std::to_string(static_cast<long long>(::getpid()));
  const int descriptor =
      ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, S_IRUSR | S_IWUSR);
  if (descriptor < 0) {
    throw std::runtime_error("create receipt " + temporary + ": " + std::strerror(errno));
  }
  try {
    write_all(descriptor, payload);
    if (::fsync(descriptor) != 0) {
      throw std::runtime_error(std::string("fsync receipt: ") + std::strerror(errno));
    }
    if (::close(descriptor) != 0) {
      throw std::runtime_error(std::string("close receipt: ") + std::strerror(errno));
    }
    if (::rename(temporary.c_str(), path.c_str()) != 0) {
      throw std::runtime_error(std::string("publish receipt: ") + std::strerror(errno));
    }
  } catch (...) {
    const int saved_errno = errno;
    ::close(descriptor);
    ::unlink(temporary.c_str());
    errno = saved_errno;
    throw;
  }
}

[[noreturn]] void reject(std::string_view reason) {
  const char* configured = std::getenv("CASA_AW_INPUT_HASH_OUTPUT");
  const std::string output = configured == nullptr ? std::string() : std::string(configured);
  std::ostringstream receipt;
  receipt << "{\n"
          << "  \"schema\": \"casa-aw-datagrid-input-hash-v1\",\n"
          << "  \"status\": \"rejected\",\n"
          << "  \"casa_version\": \"6.7.5.18\",\n"
          << "  \"casa_source_commit\": \"" << kCasaCommit << "\",\n"
          << "  \"reason\": \"" << json_escape(reason) << "\"\n"
          << "}\n";
  try {
    if (!output.empty()) {
      atomic_receipt(output, receipt.str());
    }
  } catch (const std::exception& error) {
    const std::string message =
        std::string("CASA AW input probe could not write rejection receipt: ") + error.what() +
        "\n";
    (void)::write(STDERR_FILENO, message.data(), message.size());
  }
  const std::string message =
      std::string("CASA AW input probe rejected call: ") + std::string(reason) + "\n";
  (void)::write(STDERR_FILENO, message.data(), message.size());
  raw_exit(kRejectedExit);
}

std::uint64_t parse_u64(const char* name, bool required) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    if (required) {
      reject(std::string("missing required environment variable ") + name);
    }
    return 0;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0') {
    reject(std::string("invalid unsigned integer in ") + name);
  }
  return static_cast<std::uint64_t>(parsed);
}

Config read_config() {
  const char* output = std::getenv("CASA_AW_INPUT_HASH_OUTPUT");
  if (output == nullptr || *output == '\0') {
    reject("CASA_AW_INPUT_HASH_OUTPUT is required");
  }
  Config config;
  config.output = output;
  if (config.output.front() != '/') {
    reject("CASA_AW_INPUT_HASH_OUTPUT must be absolute");
  }
  config.expected_nxy =
      static_cast<Int>(parse_u64("CASA_AW_INPUT_HASH_EXPECT_NXY", true));
  if (config.expected_nxy <= 0) {
    reject("CASA_AW_INPUT_HASH_EXPECT_NXY must be positive");
  }
  config.max_sources = parse_u64("CASA_AW_INPUT_HASH_MAX_SOURCES", false);
  const std::uint64_t interval =
      parse_u64("CASA_AW_INPUT_HASH_CHECKPOINT_INTERVAL", false);
  if (interval != 0) {
    config.checkpoint_interval = interval;
  }
  return config;
}

// Diagnostic-only layout view.  This type adds no state and is never
// constructed; the static cast is used solely to access AWVisResampler's
// protected, non-virtual helpers and fields on the exact 6.7.5.18 object.
class AwProbeView final : public AWVisResampler {
 public:
  static AwProbeView& from(AWVisResampler* value) {
    return *static_cast<AwProbeView*>(value);
  }

  [[nodiscard]] const Vector<Int>& channel_map() const { return chanMap_p; }
  [[nodiscard]] const Vector<Int>& polarization_map() const { return polMap_p; }
  [[nodiscard]] const Vector<Double>& uvw_scale() const { return uvwScale_p; }
  [[nodiscard]] const Vector<Double>& offset() const { return offset_p; }
  [[nodiscard]] const Vector<Double>& dphase() const { return dphase_p; }

  void source_geometry(Vector<Double>& pos, Vector<Int>& loc, Vector<Double>& off,
                       Complex& phasor, Int row, const Matrix<Double>& uvw, Double phase,
                       Double frequency, const Vector<Float>& sampling) {
    sgrid(pos, loc, off, phasor, row, uvw, phase, frequency, uvwScale_p, offset_p,
          sampling);
  }

  Complex* convolution(Double vb_pa, Vector<Int>& shape, Vector<Int>& support,
                       Int& mueller, casacore::CountedPtr<CFBuffer>& buffer, Double w,
                       Int frequency_index, Int w_index, PolMapType& direct,
                       PolMapType& conjugate, Int polarization, uInt mueller_column) {
    return getConvFunc_p(vb_pa, shape, support, mueller, buffer, w, frequency_index,
                         w_index, direct, conjugate, polarization, mueller_column);
  }
};

[[nodiscard]] bool is_on_grid(Int nx, Int ny, Int nw, const Vector<Int>& loc,
                              const Vector<Int>& support) {
  return loc[0] - support[0] >= 0 && loc[0] + support[0] < nx &&
         loc[1] - support[1] >= 0 && loc[1] + support[1] < ny && loc[2] >= 0 &&
         loc[2] <= nw;
}

[[nodiscard]] Float casa_taylor_x(Double frequency_hz, Double reference_hz) {
  const Double casa_frequency_hz = static_cast<Double>(static_cast<Float>(frequency_hz));
  return static_cast<Float>((casa_frequency_hz - reference_hz) / reference_hz);
}

[[nodiscard]] std::uint64_t role_ordinal(Int mueller) {
  if (mueller == 0) {
    return 0;
  }
  if (mueller == 15) {
    return 1;
  }
  reject("expected only Mueller 0/RR and 15/LL for Stokes I");
}

void hash_role(std::uint64_t source_ordinal, const Role& role, Hashes& hashes) {
  auto hash_source = [&](Fnv1a64& hash) {
    hash.u64(source_ordinal);
    hash.u64(role.ordinal);
    hash.f64(role.frequency_hz);
    hash.f64(role.u_lambda);
    hash.f64(role.v_lambda);
    hash.f64(role.w_lambda);
    hash.f32(role.weight);
    hash.f32(role.taylor_x);
    hash.f32(role.term_weight);
  };
  auto hash_value = [&](Fnv1a64& hash) {
    hash.complex32(role.residual);
    hash.complex32(role.value);
  };
  auto hash_cell = [&](Fnv1a64& hash) {
    hash.f64(role.cell_frequency_hz);
    hash.f64(role.cell_w_lambda);
    hash.u32(role.mueller);
    hash.f64(role.cell_pa_deg);
  };
  auto hash_placement = [&](Fnv1a64& hash) {
    hash.i64(role.loc_x);
    hash.i64(role.loc_y);
    hash.i64(role.off_x);
    hash.i64(role.off_y);
    hash.boolean(role.conjugate_for_grid);
  };
  auto hash_normalization = [&](Fnv1a64& hash) {
    hash.complex32(role.normalization32);
    hash.complex64(role.normalization64);
  };
  auto hash_taps = [&](Fnv1a64& hash) {
    hash.u64(role.tap_count);
    hash.u64(role.tap_hash);
  };

  hash_source(hashes.portable);
  hash_value(hashes.portable);
  hash_cell(hashes.portable);
  hash_placement(hashes.portable);
  hash_normalization(hashes.portable);
  hash_taps(hashes.portable);

  hash_source(hashes.source);
  hash_value(hashes.value);
  hash_cell(hashes.cell);
  hash_placement(hashes.placement);
  hash_normalization(hashes.normalization);
  hash_taps(hashes.taps);
}

std::string completed_receipt(const Config& config, const Array<DComplex>& grid,
                              const VBStore& vbs, const Hashes& hashes,
                              const std::vector<Checkpoint>& checkpoints,
                              std::uint64_t sources, std::uint64_t roles,
                              std::uint64_t taps, std::string_view stop_reason) {
  const auto shape = grid.shape();
  std::ostringstream output;
  output << "{\n"
         << "  \"schema\": \"casa-aw-datagrid-input-hash-v1\",\n"
         << "  \"status\": \"completed-before-grid\",\n"
         << "  \"role\": \"bounded-correctness-oracle-not-performance-evidence\",\n"
         << "  \"casa_version\": \"6.7.5.18\",\n"
         << "  \"casa_source_commit\": \"" << kCasaCommit << "\",\n"
         << "  \"exit_code\": " << kCompletedExit << ",\n"
         << "  \"stop_reason\": \"" << json_escape(stop_reason) << "\",\n"
         << "  \"grid_dispatch\": \"skipped\",\n"
         << "  \"formed_image\": false,\n"
         << "  \"dopsf\": false,\n"
         << "  \"call_ordinal\": 0,\n"
         << "  \"grid_shape\": [" << shape[0] << ", " << shape[1] << ", " << shape[2]
         << ", " << shape[3] << "],\n"
         << "  \"vb_begin_row\": " << vbs.beginRow_p << ",\n"
         << "  \"vb_end_row\": " << vbs.endRow_p << ",\n"
         << "  \"vb_nrow\": " << vbs.nRow_p << ",\n"
         << "  \"max_sources\": " << config.max_sources << ",\n"
         << "  \"source_count\": " << sources << ",\n"
         << "  \"role_count\": " << roles << ",\n"
         << "  \"phased_tap_count\": " << taps << ",\n"
         << "  \"source_order\": \"row-channel-RR-LL-y-x\",\n"
         << "  \"portable_contract\": "
            "\"fnv1a64-source-role-frequency-uvw-weight-taylor-tt0-residual-value-"
            "cfkey-placement-conjugation-normalization-tapcount-nestedtap\",\n"
         << "  \"tap_contract\": \"fnv1a64-y-outer-x-inner-complex32-bits\",\n"
         << "  \"hashes\": {\n"
         << "    \"portable\": " << hashes.portable.value() << ",\n"
         << "    \"source\": " << hashes.source.value() << ",\n"
         << "    \"value\": " << hashes.value.value() << ",\n"
         << "    \"cf_cell\": " << hashes.cell.value() << ",\n"
         << "    \"placement\": " << hashes.placement.value() << ",\n"
         << "    \"normalization\": " << hashes.normalization.value() << ",\n"
         << "    \"phased_taps\": " << hashes.taps.value() << "\n"
         << "  },\n"
         << "  \"checkpoints\": [";
  for (std::size_t index = 0; index < checkpoints.size(); ++index) {
    if (index != 0) {
      output << ", ";
    }
    output << "{\"sources\":" << checkpoints[index].sources
           << ",\"portable\":" << checkpoints[index].portable_hash << "}";
  }
  output << "]\n}\n";
  return output.str();
}

[[noreturn]] void probe_dcomplex(AWVisResampler* object, Array<DComplex>& grid, VBStore& vbs,
                                 Matrix<Double>& sumwt, const Bool& dopsf,
                                 Bool use_conjugate_frequency_cf) {
  (void)sumwt;
  (void)use_conjugate_frequency_cf;
  static std::atomic<std::uint64_t> calls{0};
  const std::uint64_t call = calls.fetch_add(1, std::memory_order_relaxed);
  if (call != 0) {
    reject("more than one DataToGrid call reached the single-block probe");
  }
  const Config config = read_config();
  if (dopsf) {
    reject("first DataToGrid call is a PSF call; residual-only restart was not honored");
  }
  if (grid.ndim() != 4) {
    reject("expected a four-dimensional DComplex grid");
  }
  const auto grid_shape = grid.shape();
  if (grid_shape[0] != config.expected_nxy || grid_shape[1] != config.expected_nxy ||
      grid_shape[2] != 1 || grid_shape[3] != 1) {
    reject("DComplex grid shape does not match expected N x N x 1 x 1");
  }
  if (vbs.vb_p == nullptr) {
    reject("VBStore has no VisBuffer2");
  }
  if (vbs.uvw_p.nelements() == 0) {
    reject("residual probe unexpectedly received an empty UVW matrix");
  }

  auto& probe = AwProbeView::from(object);
  VB2CFBMap& row_to_cf = object->getVBRow2CFBMap();
  if (row_to_cf.nelements() == 0) {
    reject("AWVisResampler has no row-to-CF map");
  }
  const Int nx = grid_shape[0];
  const Int ny = grid_shape[1];
  const Int n_grid_pol = grid_shape[2];
  const Int n_grid_chan = grid_shape[3];
  const Int n_data_pol = vbs.flagCube_p.shape()[0];
  const Int n_data_chan = vbs.flagCube_p.shape()[1];
  if (n_data_pol < 4) {
    reject("Stokes-I AW probe expected at least four input correlations");
  }
  if (probe.dphase().nelements() < static_cast<uInt>(vbs.endRow_p)) {
    reject("AWVisResampler dphase does not cover the VB row range");
  }

  casacore::CountedPtr<CFBuffer> first_buffer = row_to_cf[0];
  if (first_buffer.null()) {
    reject("first row-to-CF buffer is null");
  }
  const bool finite_pointing_offsets = first_buffer->finitePointingOffsets();
  const Int vb_spw = vbs.vb_p->spectralWindows()(0);
  const Double vb_pa = vbs.paQuant_p.getValue("rad");
  Hashes hashes;
  std::vector<Checkpoint> checkpoints;
  std::uint64_t source_count = 0;
  std::uint64_t role_count = 0;
  std::uint64_t tap_count = 0;

  for (Int row = vbs.beginRow_p; row < vbs.endRow_p; ++row) {
    if (vbs.rowFlag_p[row]) {
      continue;
    }
    if (row < 0 || static_cast<std::size_t>(row) >= row_to_cf.vbRow2BLMap_p.size()) {
      reject("row is outside the POINTING baseline map");
    }
    const Int baseline = row_to_cf.vbRow2BLMap_p[static_cast<std::size_t>(row)];
    if (baseline < 0 ||
        static_cast<uInt>(baseline) >= row_to_cf.vectorPhaseGradCalculator_p.nelements() ||
        row_to_cf.vectorPhaseGradCalculator_p[baseline].null()) {
      reject("row has no POINTING phase-gradient calculator");
    }
    const Matrix<Complex>& phase_gradient =
        row_to_cf.vectorPhaseGradCalculator_p[baseline]->field_phaseGrad_p;
    casacore::CountedPtr<CFBuffer> buffer = row_to_cf[row];
    if (buffer.null()) {
      reject("row-to-CF buffer is null");
    }

    PolMapType mueller_values;
    PolMapType mueller_indices;
    PolMapType conjugate_mueller_values;
    PolMapType conjugate_mueller_indices;
    Vector<Double> frequency_values;
    Vector<Double> w_values;
    Double frequency_increment = 0.0;
    Double w_increment = 0.0;
    buffer->getCoordList(frequency_values, w_values, mueller_indices, mueller_values,
                         conjugate_mueller_indices, conjugate_mueller_values,
                         frequency_increment, w_increment);
    (void)frequency_values;
    (void)mueller_values;
    (void)conjugate_mueller_values;
    (void)frequency_increment;
    (void)w_increment;
    const Int nw = w_values.nelements();

    for (Int channel = 0; channel < n_data_chan; ++channel) {
      const Float weight = vbs.imagingWeight_p(channel, row);
      if (weight == 0.0F) {
        continue;
      }
      const Int target_channel = probe.channel_map()[channel];
      if (target_channel < 0 || target_channel >= n_grid_chan) {
        continue;
      }
      const Double frequency_hz = vbs.freq_p[channel];
      const Double data_w_m = vbs.uvw_p(2, row);
      const Double w_lambda = data_w_m * frequency_hz / casacore::C::c;
      const Int w_index = buffer->nearestWNdx(w_lambda);
      const Int frequency_index =
          buffer->nearestFreqNdx(vb_spw, channel, vbs.conjBeams_p);
      Double cf_reference_frequency = 0.0;
      Float raw_sampling = 0.0F;
      Vector<Int> support(2);
      buffer->getParams(cf_reference_frequency, raw_sampling, support[0], support[1],
                        frequency_index, w_index, 0);
      (void)cf_reference_frequency;
      Vector<Float> sampling(2);
      sampling[0] = sampling[1] =
          static_cast<Float>(std::nearbyint(static_cast<Double>(raw_sampling)));
      Vector<Double> position(3);
      Vector<Int> location(3);
      Vector<Double> offset(3);
      Complex phasor;
      probe.source_geometry(position, location, offset, phasor, row, vbs.uvw_p,
                            probe.dphase()[row], frequency_hz, sampling);

      std::array<Role, 2> roles{};
      std::array<bool, 2> present{false, false};
      for (Int polarization = 0; polarization < n_data_pol; ++polarization) {
        if (vbs.flagCube_p(polarization, channel, row)) {
          continue;
        }
        const Int target_polarization = probe.polarization_map()[polarization];
        if (target_polarization < 0 || target_polarization >= n_grid_pol) {
          continue;
        }
        const Vector<Int> conjugate_row = conjugate_mueller_indices[polarization];
        for (uInt mueller_column = 0; mueller_column < conjugate_row.nelements();
             ++mueller_column) {
          Int mueller = -1;
          Vector<Int> cell_shape;
          Vector<Int> role_support = support.copy();
          Complex* pixels = probe.convolution(
              vb_pa, cell_shape, role_support, mueller, buffer, data_w_m, frequency_index,
              w_index, mueller_indices, conjugate_mueller_indices, polarization,
              mueller_column);
          if (pixels == nullptr) {
            reject("CASA returned a null convolution-function storage pointer");
          }
          const Int visibility_element = mueller % n_data_pol;
          if (visibility_element < 0 || visibility_element >= n_data_pol) {
            reject("Mueller element mapped outside the visibility vector");
          }
          if (vbs.flagCube_p(visibility_element, channel, row)) {
            break;
          }
          if (!is_on_grid(nx, ny, nw, location, role_support)) {
            break;
          }
          const std::uint64_t ordinal = role_ordinal(mueller);
          if (present[ordinal]) {
            reject("duplicate RR or LL role in one source");
          }
          const Int polarization_index =
              data_w_m > 0.0 ? mueller_indices[polarization][mueller_column]
                             : conjugate_mueller_indices[polarization][mueller_column];
          casacore::CountedPtr<CFCell>& cell =
              buffer->getCFCellPtr(frequency_index, w_index, polarization_index);
          if (cell.null()) {
            reject("selected CF cell is null");
          }
          const Vector<Int> convolution_origin =
              cell_shape[0] % 2 == 0 && cell_shape[1] % 2 == 0
                  ? cell_shape / 2
                  : cell_shape / 2 + 1;
          if (cell_shape.nelements() < 2) {
            reject("selected CF cell is not at least two-dimensional");
          }
          const Int phase_origin_x = phase_gradient.shape()[0] / 2;
          const Int phase_origin_y = phase_gradient.shape()[0] / 2;
          Fnv1a64 nested_taps;
          Complex normalization32(0.0F);
          DComplex normalization64(0.0);
          std::uint64_t role_taps = 0;
          for (Int iy = -role_support[1]; iy <= role_support[1]; ++iy) {
            const Int local_y =
                static_cast<Int>(sampling[1] * static_cast<Float>(iy) + offset[1]);
            const Int cell_y = local_y + convolution_origin[1];
            for (Int ix = -role_support[0]; ix <= role_support[0]; ++ix) {
              const Int local_x =
                  static_cast<Int>(sampling[0] * static_cast<Float>(ix) + offset[0]);
              const Int cell_x = local_x + convolution_origin[0];
              if (cell_x < 0 || cell_y < 0 || cell_x >= cell_shape[0] ||
                  cell_y >= cell_shape[1]) {
                reject("tap traversal left the selected CF cell");
              }
              Complex tap = pixels[cell_x + cell_y * cell_shape[0]];
              if (data_w_m > 0.0) {
                tap = std::conj(tap);
              }
              normalization32 += tap;
              normalization64 += DComplex(static_cast<Double>(tap.real()),
                                           static_cast<Double>(tap.imag()));
              if (finite_pointing_offsets) {
                const Int phase_x = local_x + phase_origin_x;
                const Int phase_y = local_y + phase_origin_y;
                if (phase_x < 0 || phase_y < 0 ||
                    phase_x >= phase_gradient.shape()[0] ||
                    phase_y >= phase_gradient.shape()[1]) {
                  reject("tap traversal left the POINTING phase-gradient matrix");
                }
                tap *= phase_gradient(phase_x, phase_y);
              }
              nested_taps.complex32(tap);
              ++role_taps;
            }
          }

          Role role;
          role.ordinal = ordinal;
          role.frequency_hz = frequency_hz;
          role.u_lambda = vbs.uvw_p(0, row) * frequency_hz / casacore::C::c;
          role.v_lambda = vbs.uvw_p(1, row) * frequency_hz / casacore::C::c;
          role.w_lambda = w_lambda;
          role.weight = weight;
          role.taylor_x = casa_taylor_x(frequency_hz, vbs.imRefFreq_p);
          role.term_weight = weight;
          role.residual = vbs.visCube_p(visibility_element, channel, row) * phasor;
          role.value = Complex(weight) * role.residual;
          role.cell_frequency_hz = cell->freqValue_p;
          role.cell_w_lambda = cell->wValue_p;
          role.mueller = static_cast<std::uint32_t>(mueller);
          role.cell_pa_deg = cell->pa_p.getValue("deg");
          role.loc_x = location[0];
          role.loc_y = location[1];
          role.off_x = static_cast<std::int64_t>(offset[0]);
          role.off_y = static_cast<std::int64_t>(offset[1]);
          role.conjugate_for_grid = data_w_m > 0.0;
          role.normalization32 = normalization32;
          role.normalization64 = normalization64;
          role.tap_count = role_taps;
          role.tap_hash = nested_taps.value();
          roles[ordinal] = role;
          present[ordinal] = true;
        }
      }
      if (present[0] != present[1]) {
        reject("source contributed only one of RR/LL");
      }
      if (!present[0]) {
        continue;
      }
      for (const Role& role : roles) {
        hash_role(source_count, role, hashes);
        ++role_count;
        tap_count += role.tap_count;
      }
      ++source_count;
      if (source_count == 1 || source_count % config.checkpoint_interval == 0 ||
          (config.max_sources != 0 && source_count == config.max_sources)) {
        checkpoints.push_back(Checkpoint{source_count, hashes.portable.value()});
      }
      if (config.max_sources != 0 && source_count >= config.max_sources) {
        const std::string receipt =
            completed_receipt(config, grid, vbs, hashes, checkpoints, source_count,
                              role_count, tap_count, "configured-source-limit");
        try {
          atomic_receipt(config.output, receipt);
        } catch (const std::exception& error) {
          reject(std::string("could not write completed receipt: ") + error.what());
        }
        raw_exit(kCompletedExit);
      }
    }
  }

  if (source_count == 0) {
    reject("first DataToGrid block contained no accepted RR/LL sources");
  }
  if (checkpoints.empty() || checkpoints.back().sources != source_count) {
    checkpoints.push_back(Checkpoint{source_count, hashes.portable.value()});
  }
  const std::string receipt =
      completed_receipt(config, grid, vbs, hashes, checkpoints, source_count, role_count,
                        tap_count, "end-of-first-datagrid-block");
  try {
    atomic_receipt(config.output, receipt);
  } catch (const std::exception& error) {
    reject(std::string("could not write completed receipt: ") + error.what());
  }
  raw_exit(kCompletedExit);
}

[[noreturn]] void probe_complex(AWVisResampler* object, Array<Complex>& grid, VBStore& vbs,
                                Matrix<Double>& sumwt, const Bool& dopsf,
                                Bool use_conjugate_frequency_cf) {
  (void)object;
  (void)grid;
  (void)vbs;
  (void)sumwt;
  (void)dopsf;
  (void)use_conjugate_frequency_cf;
  reject("unexpected single-precision AW DataToGrid call");
}

extern "C" void casa_aw_datatogrid_dcomplex(
    AWVisResampler*, Array<DComplex>&, VBStore&, Matrix<Double>&, const Bool&, Bool)
    asm("_ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIdEEEEvRN8"
        "casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb");

extern "C" void casa_aw_datatogrid_complex(
    AWVisResampler*, Array<Complex>&, VBStore&, Matrix<Double>&, const Bool&, Bool)
    asm("_ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIfEEEEvRN8"
        "casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb");

#define CASA_DYLD_INTERPOSE(replacement, replacee)                                  \
  __attribute__((used)) static const struct {                                      \
    const void* replacement_address;                                                \
    const void* replacee_address;                                                   \
  } replacement##_interpose __attribute__((section("__DATA,__interpose"))) = {     \
      reinterpret_cast<const void*>(static_cast<std::uintptr_t>(                    \
          reinterpret_cast<std::uintptr_t>(&replacement))),                        \
      reinterpret_cast<const void*>(static_cast<std::uintptr_t>(                    \
          reinterpret_cast<std::uintptr_t>(&replacee)))}

CASA_DYLD_INTERPOSE(probe_dcomplex, casa_aw_datatogrid_dcomplex);
CASA_DYLD_INTERPOSE(probe_complex, casa_aw_datatogrid_complex);

}  // namespace
