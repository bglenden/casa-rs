// CASA 6.7.5.18 AW DataToGrid bracket oracle.
//
// This dylib is intentionally simpler than the tap-level input oracle.  It:
//
//   1. hashes the ordered, pre-CF VBStore input stream for each residual call;
//   2. invokes the exact original DComplex DataToGrid specialization once;
//   3. hashes the cumulative raw grid and sumwt after the TT0/TT1 call pair;
//   4. exits after one or sixteen visibility blocks, before finalizeToSky,
//      normalization, FFT, or image formation.
//
// It never modifies an input, grid, sumwt, CF, or POINTING object itself.
// Shallow Array/Matrix views retain the two MT-MFS term grids between calls;
// the hash traversal is read-only.  Finalize hooks fail closed if the expected
// block count was wrong.  A separate tap interposer should be used only if the
// call-stream hashes match while a cumulative grid hash does not.

#include <synthesis/TransformMachines2/AWVisResampler.h>

#include <casacore/casa/BasicSL/Constants.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {

using casa::refim::AWVisResampler;
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

extern "C" void casa_aw_datatogrid_dcomplex(
    AWVisResampler*, Array<DComplex>&, VBStore&, Matrix<Double>&, const Bool&, Bool)
    asm("__ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIdEEEEvRN8"
        "casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb");

extern "C" void casa_aw_datatogrid_complex(
    AWVisResampler*, Array<Complex>&, VBStore&, Matrix<Double>&, const Bool&, Bool)
    asm("__ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIfEEEEvRN8"
        "casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb");

constexpr std::uint64_t kFnvOffset = UINT64_C(0xcbf29ce484222325);
constexpr std::uint64_t kFnvPrime = UINT64_C(0x00000100000001b3);
constexpr int kCompletedExit = 86;
constexpr int kRejectedExit = 87;
constexpr char kCasaCommit[] = "418bb1a26df7c4aba663ff123b038b75a6fa0295";

class Fnv1a64 {
 public:
  void u64(std::uint64_t value) {
    for (unsigned shift = 0; shift != 64; shift += 8) {
      const auto byte = static_cast<unsigned char>((value >> shift) & 0xffU);
      value_ ^= static_cast<std::uint64_t>(byte);
      value_ *= kFnvPrime;
    }
  }

  void u32(std::uint32_t value) {
    for (unsigned shift = 0; shift != 32; shift += 8) {
      const auto byte = static_cast<unsigned char>((value >> shift) & 0xffU);
      value_ ^= static_cast<std::uint64_t>(byte);
      value_ *= kFnvPrime;
    }
  }

  void boolean(bool value) {
    value_ ^= value ? 1U : 0U;
    value_ *= kFnvPrime;
  }

  void f32(Float value) {
    std::uint32_t bits = 0;
    static_assert(sizeof(bits) == sizeof(value));
    std::memcpy(&bits, &value, sizeof(bits));
    u32(bits);
  }

  void f64(Double value) {
    std::uint64_t bits = 0;
    static_assert(sizeof(bits) == sizeof(value));
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
  Int expected_nxy = 0;
  std::uint64_t target_blocks = 0;
  std::uint64_t terms = 2;
};

struct CallRecord {
  std::uint64_t call = 0;
  std::uint64_t block = 0;
  std::uint64_t term = 0;
  std::uint64_t source_count = 0;
  std::uint64_t stream_hash = kFnvOffset;
  std::uint64_t geometry_hash = kFnvOffset;
  std::uint64_t input_hash = kFnvOffset;
};

struct TermBoundary {
  std::uint64_t grid_hash = kFnvOffset;
  std::uint64_t sumwt_hash = kFnvOffset;
  std::uint64_t grid_values_hashed = 0;
  std::uint64_t sumwt_values_hashed = 0;
};

struct BlockRecord {
  std::uint64_t block = 0;
  std::uint64_t stream_hash = kFnvOffset;
  std::uint64_t input_stream_hash = kFnvOffset;
  std::array<TermBoundary, 2> terms{};
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
    throw std::runtime_error("refusing to overwrite receipt " + path);
  }
  const std::string temporary =
      path + ".tmp." + std::to_string(static_cast<long long>(::getpid()));
  const int descriptor =
      ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, S_IRUSR | S_IWUSR);
  if (descriptor < 0) {
    throw std::runtime_error("create receipt " + temporary + ": " + std::strerror(errno));
  }
  bool descriptor_open = true;
  try {
    write_all(descriptor, payload);
    if (::fsync(descriptor) != 0) {
      throw std::runtime_error(std::string("fsync receipt: ") + std::strerror(errno));
    }
    if (::close(descriptor) != 0) {
      descriptor_open = false;
      throw std::runtime_error(std::string("close receipt: ") + std::strerror(errno));
    }
    descriptor_open = false;
    if (::rename(temporary.c_str(), path.c_str()) != 0) {
      throw std::runtime_error(std::string("publish receipt: ") + std::strerror(errno));
    }
  } catch (...) {
    const int saved_errno = errno;
    if (descriptor_open) {
      ::close(descriptor);
    }
    ::unlink(temporary.c_str());
    errno = saved_errno;
    throw;
  }
}

[[noreturn]] void minimal_reject(std::string_view reason) {
  const char* configured = std::getenv("CASA_AW_BRACKET_OUTPUT");
  const std::string output = configured == nullptr ? std::string() : std::string(configured);
  std::ostringstream receipt;
  receipt << "{\n"
          << "  \"schema\": \"casa-aw-datagrid-bracket-v1\",\n"
          << "  \"status\": \"rejected-before-fft\",\n"
          << "  \"casa_version\": \"6.7.5.18\",\n"
          << "  \"casa_version_string\": \"6.7.5-18\",\n"
          << "  \"casa_source_commit\": \"" << kCasaCommit << "\",\n"
          << "  \"reason\": \"" << json_escape(reason) << "\"\n"
          << "}\n";
  try {
    if (!output.empty()) {
      atomic_receipt(output, receipt.str());
    }
  } catch (const std::exception& error) {
    const std::string message =
        std::string("CASA AW bracket could not write rejection receipt: ") + error.what() +
        "\n";
    (void)::write(STDERR_FILENO, message.data(), message.size());
  }
  const std::string message =
      std::string("CASA AW bracket rejected call: ") + std::string(reason) + "\n";
  (void)::write(STDERR_FILENO, message.data(), message.size());
  raw_exit(kRejectedExit);
}

std::uint64_t parse_u64(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    minimal_reject(std::string("missing required environment variable ") + name);
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0') {
    minimal_reject(std::string("invalid unsigned integer in ") + name);
  }
  return static_cast<std::uint64_t>(parsed);
}

Config read_config() {
  const char* output = std::getenv("CASA_AW_BRACKET_OUTPUT");
  if (output == nullptr || *output == '\0') {
    minimal_reject("CASA_AW_BRACKET_OUTPUT is required");
  }
  Config config;
  config.output = output;
  if (config.output.front() != '/') {
    minimal_reject("CASA_AW_BRACKET_OUTPUT must be absolute");
  }
  config.expected_nxy = static_cast<Int>(parse_u64("CASA_AW_BRACKET_EXPECT_NXY"));
  config.target_blocks = parse_u64("CASA_AW_BRACKET_BLOCKS");
  config.terms = parse_u64("CASA_AW_BRACKET_TERMS");
  if (config.expected_nxy <= 0) {
    minimal_reject("CASA_AW_BRACKET_EXPECT_NXY must be positive");
  }
  if (config.target_blocks != 1 && config.target_blocks != 16) {
    minimal_reject("CASA_AW_BRACKET_BLOCKS must be exactly 1 or 16");
  }
  if (config.terms != 2) {
    minimal_reject("the frozen MT-MFS residual bracket requires exactly two terms");
  }
  return config;
}

// Read-only view of the protected channel/polarization maps.  No object of
// this empty diagnostic subclass is constructed.
class AwProbeView final : public AWVisResampler {
 public:
  static const AwProbeView& from(const AWVisResampler* value) {
    return *static_cast<const AwProbeView*>(value);
  }

  [[nodiscard]] const Vector<Int>& channel_map() const { return chanMap_p; }
  [[nodiscard]] const Vector<Int>& polarization_map() const { return polMap_p; }
  [[nodiscard]] const Vector<Double>& dphase() const { return dphase_p; }
};

std::uint64_t f64_bits(Double value) {
  std::uint64_t bits = 0;
  static_assert(sizeof(bits) == sizeof(value));
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

struct NativeFirstVb {
  std::int64_t begin_row = 0;
  std::int64_t end_row = 0;
  std::int64_t n_row = 0;
  std::int64_t spw_id = 0;
  std::uint64_t row_ids_count = 0;
  std::uint64_t row_ids_hash = kFnvOffset;
  std::uint64_t row_id_first = 0;
  std::uint64_t row_id_last = 0;
  std::vector<std::uint64_t> row_ids;
  std::uint64_t n_data_chan = 0;
  std::uint64_t n_data_pol = 0;
  std::uint64_t chan_map_count = 0;
  std::uint64_t chan_map_hash = kFnvOffset;
  std::uint64_t pol_map_count = 0;
  std::uint64_t pol_map_hash = kFnvOffset;
  std::uint64_t freq_count = 0;
  std::uint64_t freq_hash = kFnvOffset;
  std::uint64_t freq_first_bits = 0;
  std::uint64_t freq_last_bits = 0;
  std::uint64_t row_flags_count = 0;
  std::uint64_t row_flags_hash = kFnvOffset;
  std::uint64_t flagged_rows = 0;

  [[nodiscard]] bool same_identity(const NativeFirstVb& other) const {
    return begin_row == other.begin_row && end_row == other.end_row &&
           n_row == other.n_row && spw_id == other.spw_id &&
           row_ids_count == other.row_ids_count &&
           row_ids_hash == other.row_ids_hash && row_id_first == other.row_id_first &&
           row_id_last == other.row_id_last && row_ids == other.row_ids &&
           n_data_chan == other.n_data_chan && n_data_pol == other.n_data_pol &&
           chan_map_count == other.chan_map_count &&
           chan_map_hash == other.chan_map_hash &&
           pol_map_count == other.pol_map_count &&
           pol_map_hash == other.pol_map_hash && freq_count == other.freq_count &&
           freq_hash == other.freq_hash && freq_first_bits == other.freq_first_bits &&
           freq_last_bits == other.freq_last_bits &&
           row_flags_count == other.row_flags_count &&
           row_flags_hash == other.row_flags_hash && flagged_rows == other.flagged_rows;
  }
};

NativeFirstVb capture_native_first_vb(const AWVisResampler* object,
                                      const VBStore& vbs) {
  const auto& probe = AwProbeView::from(object);
  NativeFirstVb boundary;
  boundary.begin_row = vbs.beginRow_p;
  boundary.end_row = vbs.endRow_p;
  boundary.n_row = vbs.nRow_p;
  boundary.spw_id = vbs.spwID_p;
  boundary.n_data_pol = vbs.flagCube_p.shape()[0];
  boundary.n_data_chan = vbs.flagCube_p.shape()[1];

  const auto& row_ids = vbs.vb_p->rowIds();
  boundary.row_ids_count = row_ids.nelements();
  boundary.row_ids.reserve(row_ids.nelements());
  Fnv1a64 row_ids_hash;
  row_ids_hash.u64(boundary.row_ids_count);
  for (const casacore::rownr_t row_id : row_ids) {
    const auto value = static_cast<std::uint64_t>(row_id);
    boundary.row_ids.push_back(value);
    row_ids_hash.u64(value);
  }
  boundary.row_ids_hash = row_ids_hash.value();
  if (!boundary.row_ids.empty()) {
    boundary.row_id_first = boundary.row_ids.front();
    boundary.row_id_last = boundary.row_ids.back();
  }

  boundary.chan_map_count = probe.channel_map().nelements();
  Fnv1a64 chan_map_hash;
  chan_map_hash.u64(boundary.chan_map_count);
  for (const Int mapped : probe.channel_map()) {
    chan_map_hash.u64(
        static_cast<std::uint64_t>(static_cast<std::int64_t>(mapped)));
  }
  boundary.chan_map_hash = chan_map_hash.value();

  boundary.pol_map_count = probe.polarization_map().nelements();
  Fnv1a64 pol_map_hash;
  pol_map_hash.u64(boundary.pol_map_count);
  for (const Int mapped : probe.polarization_map()) {
    pol_map_hash.u64(
        static_cast<std::uint64_t>(static_cast<std::int64_t>(mapped)));
  }
  boundary.pol_map_hash = pol_map_hash.value();

  boundary.freq_count = vbs.freq_p.nelements();
  Fnv1a64 freq_hash;
  freq_hash.u64(boundary.freq_count);
  for (const Double frequency : vbs.freq_p) {
    freq_hash.f64(frequency);
  }
  boundary.freq_hash = freq_hash.value();
  if (boundary.freq_count != 0) {
    boundary.freq_first_bits = f64_bits(vbs.freq_p[0]);
    boundary.freq_last_bits = f64_bits(vbs.freq_p[boundary.freq_count - 1]);
  }

  boundary.row_flags_count = vbs.rowFlag_p.nelements();
  Fnv1a64 row_flags_hash;
  row_flags_hash.u64(boundary.row_flags_count);
  for (const Bool flagged : vbs.rowFlag_p) {
    row_flags_hash.boolean(flagged);
    if (flagged) {
      ++boundary.flagged_rows;
    }
  }
  boundary.row_flags_hash = row_flags_hash.value();
  return boundary;
}

struct InputHashes {
  std::uint64_t sources = 0;
  std::uint64_t stream = kFnvOffset;
  std::uint64_t geometry = kFnvOffset;
  std::uint64_t input = kFnvOffset;
};

InputHashes hash_call_inputs(const AWVisResampler* object, const Array<DComplex>& grid,
                             const VBStore& vbs, std::uint64_t call,
                             std::uint64_t block, std::uint64_t term,
                             Bool use_conjugate_frequency_cf) {
  const auto& probe = AwProbeView::from(object);
  Fnv1a64 stream;
  Fnv1a64 geometry;
  Fnv1a64 input;
  geometry.u64(call);
  geometry.u64(block);
  geometry.u64(term);
  input.u64(call);
  input.u64(block);
  input.u64(term);
  for (Fnv1a64* hash : {&stream, &geometry, &input}) {
    hash->boolean(use_conjugate_frequency_cf);
    hash->u64(static_cast<std::uint64_t>(vbs.beginRow_p));
    hash->u64(static_cast<std::uint64_t>(vbs.endRow_p));
    hash->u64(static_cast<std::uint64_t>(vbs.nRow_p));
    hash->u64(static_cast<std::uint64_t>(vbs.spwID_p));
    hash->f64(vbs.imRefFreq_p);
    for (const Int extent : grid.shape()) {
      hash->u64(static_cast<std::uint64_t>(extent));
    }
    hash->u64(probe.channel_map().nelements());
    for (const Int mapped : probe.channel_map()) {
      hash->u64(static_cast<std::uint64_t>(static_cast<std::int64_t>(mapped)));
    }
    hash->u64(probe.polarization_map().nelements());
    for (const Int mapped : probe.polarization_map()) {
      hash->u64(static_cast<std::uint64_t>(static_cast<std::int64_t>(mapped)));
    }
    const auto& row_ids = vbs.vb_p->rowIds();
    hash->u64(row_ids.nelements());
    for (const casacore::rownr_t row_id : row_ids) {
      hash->u64(static_cast<std::uint64_t>(row_id));
    }
  }

  const Int n_data_pol = vbs.flagCube_p.shape()[0];
  const Int n_data_chan = vbs.flagCube_p.shape()[1];
  if (probe.dphase().nelements() < static_cast<unsigned long long>(vbs.endRow_p)) {
    minimal_reject("AW dphase does not cover the bracketed VB row range");
  }
  std::uint64_t sources = 0;
  for (Int row = vbs.beginRow_p; row < vbs.endRow_p; ++row) {
    stream.u64(static_cast<std::uint64_t>(row));
    stream.boolean(vbs.rowFlag_p[row]);
    geometry.u64(static_cast<std::uint64_t>(row));
    geometry.boolean(vbs.rowFlag_p[row]);
    input.u64(static_cast<std::uint64_t>(row));
    input.boolean(vbs.rowFlag_p[row]);
    if (vbs.rowFlag_p[row]) {
      continue;
    }
    for (Int axis = 0; axis != 3; ++axis) {
      stream.f64(vbs.uvw_p(axis, row));
      geometry.f64(vbs.uvw_p(axis, row));
      input.f64(vbs.uvw_p(axis, row));
    }
    stream.f64(probe.dphase()[row]);
    geometry.f64(probe.dphase()[row]);
    input.f64(probe.dphase()[row]);
    for (Int channel = 0; channel < n_data_chan; ++channel) {
      const Int target_channel = probe.channel_map()[channel];
      const Float weight = vbs.imagingWeight_p(channel, row);
      if (target_channel < 0 || target_channel >= grid.shape()[3]) {
        continue;
      }
      stream.u64(static_cast<std::uint64_t>(channel));
      stream.f64(vbs.freq_p[channel]);
      for (Int polarization = 0; polarization < n_data_pol; ++polarization) {
        stream.boolean(vbs.flagCube_p(polarization, channel, row));
      }
      if (weight == 0.0F) {
        continue;
      }
      geometry.u64(sources);
      geometry.u64(static_cast<std::uint64_t>(channel));
      geometry.f64(vbs.freq_p[channel]);
      input.u64(sources);
      input.u64(static_cast<std::uint64_t>(channel));
      input.f64(vbs.freq_p[channel]);
      input.f64(vbs.uvw_p(0, row) * vbs.freq_p[channel] / casacore::C::c);
      input.f64(vbs.uvw_p(1, row) * vbs.freq_p[channel] / casacore::C::c);
      input.f64(vbs.uvw_p(2, row) * vbs.freq_p[channel] / casacore::C::c);
      input.f32(weight);
      input.u64(static_cast<std::uint64_t>(n_data_pol));
      for (Int polarization = 0; polarization < n_data_pol; ++polarization) {
        const Bool flagged = vbs.flagCube_p(polarization, channel, row);
        geometry.boolean(flagged);
        input.boolean(flagged);
        input.complex32(vbs.visCube_p(polarization, channel, row));
      }
      ++sources;
    }
  }
  return InputHashes{sources, stream.value(), geometry.value(), input.value()};
}

struct HashedValues {
  std::uint64_t hash = kFnvOffset;
  std::uint64_t values = 0;
};

HashedValues hash_grid(const Array<DComplex>& grid) {
  if (!grid.contiguousStorage()) {
    minimal_reject("raw DComplex grid is unexpectedly non-contiguous");
  }
  Bool delete_storage = false;
  const DComplex* storage = grid.getStorage(delete_storage);
  if (delete_storage) {
    grid.freeStorage(storage, delete_storage);
    minimal_reject("raw DComplex grid hash would require a materialized copy");
  }
  Fnv1a64 hash;
  for (const Int extent : grid.shape()) {
    hash.u64(static_cast<std::uint64_t>(extent));
  }
  for (std::size_t index = 0; index < grid.nelements(); ++index) {
    hash.complex64(storage[index]);
  }
  grid.freeStorage(storage, delete_storage);
  return HashedValues{hash.value(), grid.nelements()};
}

HashedValues hash_sumwt(const Matrix<Double>& sumwt) {
  if (!sumwt.contiguousStorage()) {
    minimal_reject("sumwt is unexpectedly non-contiguous");
  }
  Bool delete_storage = false;
  const Double* storage = sumwt.getStorage(delete_storage);
  if (delete_storage) {
    sumwt.freeStorage(storage, delete_storage);
    minimal_reject("sumwt hash would require a materialized copy");
  }
  Fnv1a64 hash;
  for (const Int extent : sumwt.shape()) {
    hash.u64(static_cast<std::uint64_t>(extent));
  }
  for (std::size_t index = 0; index < sumwt.nelements(); ++index) {
    hash.f64(storage[index]);
  }
  sumwt.freeStorage(storage, delete_storage);
  return HashedValues{hash.value(), sumwt.nelements()};
}

class BracketState {
 public:
  explicit BracketState(Config config) : config_(std::move(config)) {}

  [[nodiscard]] const Config& config() const { return config_; }
  [[nodiscard]] std::uint64_t next_call() const { return calls_.size(); }
  [[nodiscard]] std::uint64_t completed_blocks() const { return blocks_.size(); }
  [[nodiscard]] std::uint64_t expected_term() const { return expected_term_; }

  void accept_dispatch(std::uint64_t term, const DComplex* grid_storage,
                       const InputHashes& hashes,
                       const NativeFirstVb& native_boundary) {
    if (blocks_.size() >= config_.target_blocks) {
      throw std::runtime_error("DataToGrid exceeded the configured block bound");
    }
    if (term != expected_term_) {
      throw std::runtime_error("DataToGrid term did not match the state-machine order");
    }
    if (term == 0) {
      if (blocks_.empty() && !native_first_vb_) {
        native_first_vb_ = std::make_unique<NativeFirstVb>(native_boundary);
      }
      if (term_storage_[0] == nullptr) {
        term_storage_[0] = grid_storage;
      } else if (term_storage_[0] != grid_storage) {
        throw std::runtime_error(
            "TT0 changed raw-grid storage identity between source blocks");
      }
      current_stream_hash_ = hashes.stream;
      active_stream_ = true;
      expected_term_ = 1;
    } else {
      if (blocks_.empty() &&
          (!native_first_vb_ ||
           !native_first_vb_->same_identity(native_boundary))) {
        throw std::runtime_error(
            "TT1 native VB boundary did not match the pending TT0 boundary");
      }
      if (!active_stream_ || hashes.stream != current_stream_hash_) {
        throw std::runtime_error(
            "TT1 source stream did not match the pending TT0 source stream");
      }
      if (term_storage_[1] == nullptr) {
        if (grid_storage == term_storage_[0]) {
          throw std::runtime_error("TT0 and TT1 unexpectedly share one raw-grid storage");
        }
        term_storage_[1] = grid_storage;
      } else if (term_storage_[1] != grid_storage) {
        throw std::runtime_error(
            "TT1 changed raw-grid storage identity between source blocks");
      }
    }
  }

  void record_before(std::uint64_t block, std::uint64_t term,
                     const InputHashes& hashes) {
    const std::uint64_t call = calls_.size();
    calls_.push_back(CallRecord{call, block, term, hashes.sources, hashes.stream,
                                hashes.geometry, hashes.input});
    input_stream_.u64(call);
    input_stream_.u64(block);
    input_stream_.u64(term);
    input_stream_.u64(hashes.sources);
    input_stream_.u64(hashes.stream);
    input_stream_.u64(hashes.geometry);
    input_stream_.u64(hashes.input);
  }

  void retain_term(std::uint64_t term, const Array<DComplex>& grid,
                   const Matrix<Double>& sumwt) {
    if (term >= term_grids_.size()) {
      minimal_reject("term ordinal is outside retained MT-MFS grids");
    }
    if (!term_grids_[term]) {
      term_grids_[term] = std::make_unique<Array<DComplex>>(grid);
      term_sumwts_[term] = std::make_unique<Matrix<Double>>(sumwt);
    } else if (term_grids_[term]->shape() != grid.shape() ||
               term_sumwts_[term]->shape() != sumwt.shape()) {
      minimal_reject("cumulative grid or sumwt shape changed between blocks");
    }
  }

  void finish_block(std::uint64_t block, std::uint64_t stream_hash) {
    if (!active_stream_ || expected_term_ != 1 || stream_hash != current_stream_hash_) {
      throw std::runtime_error("MT-MFS block boundary state is inconsistent");
    }
    BlockRecord record;
    record.block = block;
    record.stream_hash = stream_hash;
    record.input_stream_hash = input_stream_.value();
    for (std::size_t term = 0; term < term_grids_.size(); ++term) {
      if (!term_grids_[term] || !term_sumwts_[term]) {
        throw std::runtime_error(
            "MT-MFS block ended before both term grids were retained");
      }
      const HashedValues grid_values = hash_grid(*term_grids_[term]);
      const HashedValues sumwt_values = hash_sumwt(*term_sumwts_[term]);
      record.terms[term].grid_hash = grid_values.hash;
      record.terms[term].sumwt_hash = sumwt_values.hash;
      record.terms[term].grid_values_hashed = grid_values.values;
      record.terms[term].sumwt_values_hashed = sumwt_values.values;
    }
    blocks_.push_back(record);
    active_stream_ = false;
    current_stream_hash_ = kFnvOffset;
    expected_term_ = 0;
  }

  [[nodiscard]] std::string receipt(std::string_view status,
                                    std::string_view reason) const {
    std::ostringstream output;
    output << "{\n"
           << "  \"schema\": \"casa-aw-datagrid-bracket-v1\",\n"
           << "  \"status\": \"" << json_escape(status) << "\",\n"
           << "  \"reason\": \"" << json_escape(reason) << "\",\n"
           << "  \"role\": \"bounded-correctness-oracle-not-performance-evidence\",\n"
           << "  \"casa_version\": \"6.7.5.18\",\n"
           << "  \"casa_version_string\": \"6.7.5-18\",\n"
           << "  \"casa_source_commit\": \"" << kCasaCommit << "\",\n"
           << "  \"exit_code\": "
           << (status == "completed-before-finalize" ? kCompletedExit : kRejectedExit)
           << ",\n"
           << "  \"original_invocation\": \"two-level-bound-exact-DComplex-specialization\",\n"
           << "  \"dispatch_identity\": \"stable-grid-storage-and-source-stream\",\n"
           << "  \"probe_serialization\": \"global-mutex\",\n"
           << "  \"formed_image\": false,\n"
           << "  \"normalization\": \"not-entered\",\n"
           << "  \"fft\": \"not-entered\",\n"
           << "  \"expected_grid_nxy\": " << config_.expected_nxy << ",\n"
           << "  \"target_blocks\": " << config_.target_blocks << ",\n"
           << "  \"terms_per_block\": " << config_.terms << ",\n"
           << "  \"completed_calls\": " << calls_.size() << ",\n"
           << "  \"completed_blocks\": " << blocks_.size() << ",\n"
           << "  \"input_stream_hash\": " << input_stream_.value() << ",\n"
           << "  \"native_first_vb\": ";
    if (!native_first_vb_) {
      output << "null";
    } else {
      const NativeFirstVb& boundary = *native_first_vb_;
      output << "{\n"
             << "    \"begin_row\": " << boundary.begin_row << ",\n"
             << "    \"end_row\": " << boundary.end_row << ",\n"
             << "    \"n_row\": " << boundary.n_row << ",\n"
             << "    \"spw_id\": " << boundary.spw_id << ",\n"
             << "    \"row_ids_count\": " << boundary.row_ids_count << ",\n"
             << "    \"row_ids_hash\": " << boundary.row_ids_hash << ",\n"
             << "    \"row_id_first\": " << boundary.row_id_first << ",\n"
             << "    \"row_id_last\": " << boundary.row_id_last << ",\n"
             << "    \"row_ids\": [";
      for (std::size_t index = 0; index < boundary.row_ids.size(); ++index) {
        if (index != 0) {
          output << ",";
        }
        output << boundary.row_ids[index];
      }
      output << "],\n"
             << "    \"n_data_chan\": " << boundary.n_data_chan << ",\n"
             << "    \"n_data_pol\": " << boundary.n_data_pol << ",\n"
             << "    \"chan_map_count\": " << boundary.chan_map_count << ",\n"
             << "    \"chan_map_hash\": " << boundary.chan_map_hash << ",\n"
             << "    \"pol_map_count\": " << boundary.pol_map_count << ",\n"
             << "    \"pol_map_hash\": " << boundary.pol_map_hash << ",\n"
             << "    \"freq_count\": " << boundary.freq_count << ",\n"
             << "    \"freq_hash\": " << boundary.freq_hash << ",\n"
             << "    \"freq_first_bits\": " << boundary.freq_first_bits << ",\n"
             << "    \"freq_last_bits\": " << boundary.freq_last_bits << ",\n"
             << "    \"row_flags_count\": " << boundary.row_flags_count << ",\n"
             << "    \"row_flags_hash\": " << boundary.row_flags_hash << ",\n"
             << "    \"flagged_rows\": " << boundary.flagged_rows << "\n"
             << "  }";
    }
    output << ",\n"
           << "  \"calls\": [";
    for (std::size_t index = 0; index < calls_.size(); ++index) {
      const auto& call = calls_[index];
      if (index != 0) {
        output << ",";
      }
      output << "\n    {\"call\":" << call.call << ",\"block\":" << call.block
             << ",\"term\":" << call.term << ",\"source_count\":"
             << call.source_count << ",\"stream_hash\":" << call.stream_hash
             << ",\"geometry_hash\":" << call.geometry_hash
             << ",\"input_hash\":" << call.input_hash << "}";
    }
    if (!calls_.empty()) {
      output << "\n  ";
    }
    output << "],\n  \"block_boundaries\": [";
    for (std::size_t index = 0; index < blocks_.size(); ++index) {
      const auto& block = blocks_[index];
      if (index != 0) {
        output << ",";
      }
      output << "\n    {\"block\":" << block.block
             << ",\"stream_hash\":" << block.stream_hash
             << ",\"input_stream_hash\":" << block.input_stream_hash
             << ",\"terms\":[";
      for (std::size_t term = 0; term < block.terms.size(); ++term) {
        if (term != 0) {
          output << ",";
        }
        output << "{\"term\":" << term << ",\"grid_hash\":"
               << block.terms[term].grid_hash << ",\"sumwt_hash\":"
               << block.terms[term].sumwt_hash << ",\"grid_values_hashed\":"
               << block.terms[term].grid_values_hashed
               << ",\"sumwt_values_hashed\":"
               << block.terms[term].sumwt_values_hashed << "}";
      }
      output << "]}";
    }
    if (!blocks_.empty()) {
      output << "\n  ";
    }
    output << "]\n}\n";
    return output.str();
  }

 private:
  Config config_;
  Fnv1a64 input_stream_;
  std::vector<CallRecord> calls_;
  std::vector<BlockRecord> blocks_;
  std::array<std::unique_ptr<Array<DComplex>>, 2> term_grids_;
  std::array<std::unique_ptr<Matrix<Double>>, 2> term_sumwts_;
  std::unique_ptr<NativeFirstVb> native_first_vb_;
  std::array<const DComplex*, 2> term_storage_{{nullptr, nullptr}};
  std::uint64_t current_stream_hash_ = kFnvOffset;
  std::uint64_t expected_term_ = 0;
  bool active_stream_ = false;
};

BracketState*& bracket_state_slot() {
  static BracketState* state = nullptr;
  return state;
}

BracketState& bracket_state() {
  BracketState*& slot = bracket_state_slot();
  if (slot == nullptr) {
    slot = new BracketState(read_config());
  }
  return *slot;
}

[[noreturn]] void state_reject(std::string_view reason) {
  BracketState* state = bracket_state_slot();
  if (state == nullptr) {
    minimal_reject(reason);
  }
  try {
    atomic_receipt(state->config().output,
                   state->receipt("rejected-before-finalize", reason));
  } catch (const std::exception& error) {
    const std::string message =
        std::string("CASA AW bracket could not write state receipt: ") + error.what() + "\n";
    (void)::write(STDERR_FILENO, message.data(), message.size());
  }
  raw_exit(kRejectedExit);
}

using OriginalDComplex = void (*)(AWVisResampler*, Array<DComplex>&, VBStore&,
                                  Matrix<Double>&, const Bool&, Bool);

OriginalDComplex original_dcomplex() {
  static OriginalDComplex original = [] {
    OriginalDComplex function = &casa_aw_datatogrid_dcomplex;
    void* address = nullptr;
    static_assert(sizeof(function) == sizeof(address));
    std::memcpy(&address, &function, sizeof(address));
    Dl_info information{};
    if (address == nullptr || dladdr(address, &information) == 0 ||
        information.dli_fname == nullptr ||
        std::string_view(information.dli_fname).find(
            "libcasacpp_synthesis.6.dylib") == std::string_view::npos) {
      minimal_reject(
          "two-level DComplex binding does not resolve to CASA synthesis");
    }
    return function;
  }();
  return original;
}

std::mutex& bracket_mutex() {
  static std::mutex mutex;
  return mutex;
}

thread_local bool original_call_active = false;

void bracket_dcomplex(AWVisResampler* object, Array<DComplex>& grid, VBStore& vbs,
                      Matrix<Double>& sumwt, const Bool& dopsf,
                      Bool use_conjugate_frequency_cf) {
  if (original_call_active) {
    state_reject("exact original DComplex binding recursed into the interposer");
  }
  const std::lock_guard<std::mutex> lock(bracket_mutex());
  BracketState& state = bracket_state();
  if (dopsf) {
    state_reject("unexpected PSF DataToGrid call in residual-only bracket");
  }
  if (grid.ndim() != 4 || grid.shape()[0] != state.config().expected_nxy ||
      grid.shape()[1] != state.config().expected_nxy || grid.shape()[2] != 1 ||
      grid.shape()[3] != 1) {
    state_reject("DComplex grid shape does not match the frozen N x N x 1 x 1 row");
  }
  if (vbs.vb_p == nullptr || vbs.uvw_p.nelements() == 0) {
    state_reject("bracketed residual VBStore is incomplete");
  }
  const std::uint64_t call = state.next_call();
  const std::uint64_t block = state.completed_blocks();
  const std::uint64_t term = state.expected_term();
  if (block >= state.config().target_blocks) {
    state_reject("DataToGrid exceeded the configured block bound");
  }
  const InputHashes inputs =
      hash_call_inputs(object, grid, vbs, call, block, term,
                       use_conjugate_frequency_cf);
  const NativeFirstVb native_boundary = capture_native_first_vb(object, vbs);
  try {
    state.accept_dispatch(term, grid.data(), inputs, native_boundary);
  } catch (const std::exception& error) {
    state_reject(error.what());
  }
  state.record_before(block, term, inputs);

  original_call_active = true;
  try {
    original_dcomplex()(object, grid, vbs, sumwt, dopsf,
                        use_conjugate_frequency_cf);
    original_call_active = false;
  } catch (const std::exception& error) {
    original_call_active = false;
    state_reject(std::string("original DComplex DataToGrid threw: ") + error.what());
  } catch (...) {
    original_call_active = false;
    state_reject("original DComplex DataToGrid threw a non-standard exception");
  }

  state.retain_term(term, grid, sumwt);
  if (term + 1 != state.config().terms) {
    return;
  }
  try {
    state.finish_block(block, inputs.stream);
  } catch (const std::exception& error) {
    state_reject(error.what());
  }
  if (state.completed_blocks() != state.config().target_blocks) {
    return;
  }
  try {
    atomic_receipt(
        state.config().output,
        state.receipt("completed-before-finalize", "configured-block-boundary"));
  } catch (const std::exception& error) {
    state_reject(std::string("could not publish completed bracket receipt: ") +
                 error.what());
  }
  raw_exit(kCompletedExit);
}

[[noreturn]] void bracket_complex(AWVisResampler*, Array<Complex>&, VBStore&,
                                  Matrix<Double>&, const Bool&, Bool) {
  state_reject("unexpected single-precision AW DataToGrid call");
}

[[noreturn]] void guard_finalize(void*) {
  BracketState* state = bracket_state_slot();
  if (state == nullptr) {
    minimal_reject("AW finalizeToSky was reached before any bracketed DataToGrid call");
  }
  state_reject("AW finalizeToSky was reached before the configured block boundary");
}

[[noreturn]] void guard_get_image(void*, Matrix<Float>&, Bool) {
  BracketState* state = bracket_state_slot();
  if (state == nullptr) {
    minimal_reject("AW getImage was reached before any bracketed DataToGrid call");
  }
  state_reject("AW getImage was reached before the configured block boundary");
}

extern "C" void casa_awprojectft_finalize(void*)
    asm("__ZN4casa5refim11AWProjectFT13finalizeToSkyEv");

extern "C" void casa_awprojectwbft_finalize(void*)
    asm("__ZN4casa5refim13AWProjectWBFT13finalizeToSkyEv");

extern "C" void casa_awprojectft_get_image(void*, Matrix<Float>&, Bool)
    asm("__ZN4casa5refim11AWProjectFT8getImageERN8casacore6MatrixIfEEb");

extern "C" void casa_awprojectwbft_get_image(void*, Matrix<Float>&, Bool)
    asm("__ZN4casa5refim13AWProjectWBFT8getImageERN8casacore6MatrixIfEEb");

#define CASA_DYLD_INTERPOSE(replacement, replacee)                                  \
  __attribute__((used)) static const struct {                                      \
    const void* replacement_address;                                                \
    const void* replacee_address;                                                   \
  } replacement##_##replacee##_interpose                                            \
      __attribute__((section("__DATA,__interpose"))) = {                           \
          reinterpret_cast<const void*>(                                            \
              reinterpret_cast<std::uintptr_t>(&replacement)),                     \
          reinterpret_cast<const void*>(                                            \
              reinterpret_cast<std::uintptr_t>(&replacee))}

CASA_DYLD_INTERPOSE(bracket_dcomplex, casa_aw_datatogrid_dcomplex);
CASA_DYLD_INTERPOSE(bracket_complex, casa_aw_datatogrid_complex);
CASA_DYLD_INTERPOSE(guard_finalize, casa_awprojectft_finalize);
CASA_DYLD_INTERPOSE(guard_finalize, casa_awprojectwbft_finalize);
CASA_DYLD_INTERPOSE(guard_get_image, casa_awprojectft_get_image);
CASA_DYLD_INTERPOSE(guard_get_image, casa_awprojectwbft_get_image);

}  // namespace

extern "C" __attribute__((visibility("default"), used)) std::uint64_t
casa_aw_datatogrid_bracket_ready_v1() {
  return original_dcomplex() == nullptr ? 0 : UINT64_C(0x4341534141574231);
}
