// CASA 6.7.5.18 first-TT0 native-component oracle.
//
// This interposer observes exactly one non-PSF DComplex
// AWVisResampler::DataToGridImpl_p call. It records the native VBStore values
// needed to isolate the frozen v5 stream/geometry hashes, publishes a
// content-addressed receipt without overwrite, and terminates with _Exit(86).
//
// The original DataToGrid function is never invoked. The grid object is
// received only so its shape can be guarded; its storage is never obtained,
// read, or written. Sumwt is not inspected. Therefore this probe cannot enter
// gridding, sumwt accumulation, normalization, FFT, or product formation.

#include <synthesis/TransformMachines2/AWVisResampler.h>

#include <CommonCrypto/CommonDigest.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <limits>
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
constexpr Int kExpectedNxy = 4096;
constexpr Int kExpectedBeginRow = 0;
constexpr Int kExpectedEndRow = 325;
constexpr Int kExpectedRows = 325;
constexpr Int kExpectedSpw = 2;
constexpr Int kExpectedChannels = 64;
constexpr Int kExpectedPolarizations = 4;
constexpr std::uint64_t kExpectedSources = UINT64_C(12359);
constexpr std::uint64_t kExpectedStream = UINT64_C(4740440223154359747);
constexpr std::array<std::uint64_t, 2> kExpectedGeometry = {
    UINT64_C(15079793846523608377),
    UINT64_C(14381099959812707833),
};
constexpr std::uint64_t kExpectedRowIdsHash = UINT64_C(15058004568616189240);
constexpr std::uint64_t kExpectedChannelMapHash = UINT64_C(2111453637644839429);
constexpr std::uint64_t kExpectedPolarizationMapHash =
    UINT64_C(13222926617229668273);
constexpr std::uint64_t kExpectedFrequencyHash = UINT64_C(17711728193083539473);
constexpr std::uint64_t kExpectedRowFlagsHash = UINT64_C(3526571572021233857);
constexpr std::uint64_t kExpectedFlaggedRows = UINT64_C(48);
constexpr char kCasaCommit[] = "418bb1a26df7c4aba663ff123b038b75a6fa0295";
constexpr char kCasacoreCommit[] = "25b653f6963a78a1dcfc8e16954081e091a50fbe";
constexpr char kFrozenV5Sha[] =
    "fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f";
constexpr char kEnvelopeSchema[] =
    "casa-aw-datagrid-native-components-envelope-v1";
constexpr char kEvidenceSchema[] = "casa-aw-datagrid-native-components-v1";
constexpr char kDComplexSymbol[] =
    "__ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIdEEEEvRN8"
    "casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb";
constexpr std::uint64_t kReadyMagic = UINT64_C(0x4341534141574e31);

class Fnv1a64 {
 public:
  void u64(std::uint64_t value) {
    for (unsigned shift = 0; shift != 64; shift += 8) {
      byte(static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
  }

  void u32(std::uint32_t value) {
    for (unsigned shift = 0; shift != 32; shift += 8) {
      byte(static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
  }

  void boolean(bool value) { byte(value ? 1U : 0U); }

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

  [[nodiscard]] std::uint64_t value() const { return value_; }

 private:
  void byte(std::uint8_t value) {
    value_ ^= static_cast<std::uint64_t>(value);
    value_ *= kFnvPrime;
  }

  std::uint64_t value_ = kFnvOffset;
};

struct Config {
  std::string output;
};

struct RecomputedCall {
  std::uint64_t call = 0;
  std::uint64_t block = 0;
  std::uint64_t term = 0;
  std::uint64_t source_count = 0;
  std::uint64_t stream_hash = kFnvOffset;
  std::uint64_t geometry_hash = kFnvOffset;
};

struct ComponentHashes {
  std::uint64_t header = kFnvOffset;
  std::uint64_t row_ids = kFnvOffset;
  std::uint64_t channel_map = kFnvOffset;
  std::uint64_t polarization_map = kFnvOffset;
  std::uint64_t frequencies = kFnvOffset;
  std::uint64_t row_flags = kFnvOffset;
  std::uint64_t uvw_dphase = kFnvOffset;
  std::uint64_t flag_masks = kFnvOffset;
  std::uint64_t imaging_weights = kFnvOffset;
  std::uint64_t admission = kFnvOffset;
};

struct Counts {
  std::uint64_t flagged_rows = 0;
  std::uint64_t zero_imaging_weights = 0;
  std::uint64_t nonzero_imaging_weights = 0;
  std::uint64_t admitted_channels = 0;
};

[[noreturn]] void raw_exit(int code) {
  std::fflush(nullptr);
  _Exit(code);
}

std::uint64_t f64_bits(Double value) {
  std::uint64_t bits = 0;
  static_assert(sizeof(bits) == sizeof(value));
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

std::uint32_t f32_bits(Float value) {
  std::uint32_t bits = 0;
  static_assert(sizeof(bits) == sizeof(value));
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
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

std::string sha256_hex(std::string_view payload) {
  if (payload.size() > std::numeric_limits<CC_LONG>::max()) {
    throw std::runtime_error("embedded evidence exceeds CommonCrypto length limit");
  }
  std::array<unsigned char, CC_SHA256_DIGEST_LENGTH> digest{};
  if (CC_SHA256(payload.data(), static_cast<CC_LONG>(payload.size()),
                digest.data()) == nullptr) {
    throw std::runtime_error("CommonCrypto SHA-256 failed");
  }
  constexpr char hex[] = "0123456789abcdef";
  std::string output;
  output.reserve(digest.size() * 2);
  for (const unsigned char byte : digest) {
    output.push_back(hex[(byte >> 4U) & 0x0fU]);
    output.push_back(hex[byte & 0x0fU]);
  }
  return output;
}

std::string envelope(std::string_view evidence) {
  const std::string digest = sha256_hex(evidence);
  std::ostringstream output;
  output << "{\"schema\":\"" << kEnvelopeSchema
         << "\",\"content_address\":{\"algorithm\":\"sha256\","
            "\"scope\":\"embedded-evidence-json-utf8\",\"digest\":\""
         << digest << "\"},\"evidence\":" << evidence << "}\n";
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

void atomic_no_clobber_receipt(const std::string& path, std::string_view payload) {
  if (path.empty() || path.front() != '/') {
    throw std::runtime_error("receipt path must be absolute");
  }
  const std::string temporary =
      path + ".tmp." + std::to_string(static_cast<long long>(::getpid()));
  const int descriptor =
      ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
             S_IRUSR | S_IWUSR);
  if (descriptor < 0) {
    throw std::runtime_error("create receipt " + temporary + ": " +
                             std::strerror(errno));
  }
  bool descriptor_open = true;
  try {
    write_all(descriptor, payload);
    if (::fsync(descriptor) != 0) {
      throw std::runtime_error(std::string("fsync receipt: ") +
                               std::strerror(errno));
    }
    if (::close(descriptor) != 0) {
      descriptor_open = false;
      throw std::runtime_error(std::string("close receipt: ") +
                               std::strerror(errno));
    }
    descriptor_open = false;
    if (::link(temporary.c_str(), path.c_str()) != 0) {
      throw std::runtime_error(std::string("publish receipt by no-clobber hard link: ") +
                               std::strerror(errno));
    }
    if (::unlink(temporary.c_str()) != 0) {
      throw std::runtime_error(std::string("remove temporary receipt: ") +
                               std::strerror(errno));
    }
    const std::size_t separator = path.find_last_of('/');
    const std::string parent =
        separator == 0 ? std::string("/") : path.substr(0, separator);
    const int parent_descriptor =
        ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (parent_descriptor < 0) {
      throw std::runtime_error(std::string("open receipt parent for fsync: ") +
                               std::strerror(errno));
    }
    if (::fsync(parent_descriptor) != 0) {
      const int saved_errno = errno;
      (void)::close(parent_descriptor);
      errno = saved_errno;
      throw std::runtime_error(std::string("fsync receipt parent: ") +
                               std::strerror(errno));
    }
    if (::close(parent_descriptor) != 0) {
      throw std::runtime_error(std::string("close receipt parent: ") +
                               std::strerror(errno));
    }
  } catch (...) {
    const int saved_errno = errno;
    if (descriptor_open) {
      (void)::close(descriptor);
    }
    (void)::unlink(temporary.c_str());
    errno = saved_errno;
    throw;
  }
}

[[noreturn]] void reject(std::string_view reason) {
  const char* configured = std::getenv("CASA_AW_NATIVE_COMPONENTS_OUTPUT");
  const std::string output =
      configured == nullptr ? std::string() : std::string(configured);
  std::ostringstream evidence;
  evidence << "{\"schema\":\"" << kEvidenceSchema
           << "\",\"status\":\"rejected-before-grid-dispatch\","
              "\"reason\":\""
           << json_escape(reason)
           << "\",\"casa_version\":\"6.7.5.18\","
              "\"casa_version_string\":\"6.7.5-18\","
              "\"casa_source_commit\":\""
           << kCasaCommit << "\",\"casacore_commit\":\"" << kCasacoreCommit
           << "\",\"frozen_casa_v5_receipt_sha256\":\"" << kFrozenV5Sha
           << "\",\"side_effects\":{\"grid_storage\":\"not-read-or-written\","
              "\"original_datatogrid\":\"not-invoked\","
              "\"sumwt\":\"not-read-or-written\","
              "\"normalization\":\"not-entered\",\"fft\":\"not-entered\","
              "\"products\":\"not-entered\"}}";
  try {
    if (!output.empty()) {
      atomic_no_clobber_receipt(output, envelope(evidence.str()));
    }
  } catch (const std::exception& error) {
    const std::string message =
        std::string("CASA native-component oracle could not write rejection receipt: ") +
        error.what() + "\n";
    (void)::write(STDERR_FILENO, message.data(), message.size());
  }
  const std::string message =
      std::string("CASA native-component oracle rejected call: ") +
      std::string(reason) + "\n";
  (void)::write(STDERR_FILENO, message.data(), message.size());
  raw_exit(kRejectedExit);
}

Config read_config() {
  const char* output = std::getenv("CASA_AW_NATIVE_COMPONENTS_OUTPUT");
  if (output == nullptr || *output == '\0') {
    reject("CASA_AW_NATIVE_COMPONENTS_OUTPUT is required");
  }
  const char* expected_nxy =
      std::getenv("CASA_AW_NATIVE_COMPONENTS_EXPECT_NXY");
  if (expected_nxy == nullptr || std::string_view(expected_nxy) != "4096") {
    reject("CASA_AW_NATIVE_COMPONENTS_EXPECT_NXY must be exactly 4096");
  }
  Config config;
  config.output = output;
  if (config.output.front() != '/') {
    reject("CASA_AW_NATIVE_COMPONENTS_OUTPUT must be absolute");
  }
  if (::access(config.output.c_str(), F_OK) == 0) {
    reject("refusing to overwrite an existing native-component receipt");
  }
  return config;
}

// Read-only access to AWVisResampler's protected maps and dphase. No object of
// this diagnostic subclass is constructed.
class AwProbeView final : public AWVisResampler {
 public:
  static const AwProbeView& from(const AWVisResampler* value) {
    return *static_cast<const AwProbeView*>(value);
  }

  [[nodiscard]] const Vector<Int>& channel_map() const { return chanMap_p; }
  [[nodiscard]] const Vector<Int>& polarization_map() const { return polMap_p; }
  [[nodiscard]] const Vector<Double>& dphase() const { return dphase_p; }
};

template <typename Function>
bool symbol_has_exact_owner(Function function) {
  void* address = nullptr;
  static_assert(sizeof(function) == sizeof(address));
  std::memcpy(&address, &function, sizeof(address));
  Dl_info information{};
  return address != nullptr && dladdr(address, &information) != 0 &&
         information.dli_fname != nullptr &&
         std::string_view(information.dli_fname).find(
             "libcasacpp_synthesis.6.dylib") != std::string_view::npos;
}

void guard_symbol_binding() {
  if (!symbol_has_exact_owner(&casa_aw_datatogrid_dcomplex)) {
    reject("DComplex DataToGrid symbol does not resolve to CASA synthesis 6");
  }
}

std::uint64_t hash_row_ids(const VBStore& vbs) {
  const auto& row_ids = vbs.vb_p->rowIds();
  Fnv1a64 hash;
  hash.u64(row_ids.nelements());
  for (const casacore::rownr_t row_id : row_ids) {
    hash.u64(static_cast<std::uint64_t>(row_id));
  }
  return hash.value();
}

std::uint64_t hash_channel_map(const Vector<Int>& values) {
  Fnv1a64 hash;
  hash.u64(values.nelements());
  for (const Int value : values) {
    hash.u64(static_cast<std::uint64_t>(static_cast<std::int64_t>(value)));
  }
  return hash.value();
}

std::uint64_t hash_frequencies(const Vector<Double>& values) {
  Fnv1a64 hash;
  hash.u64(values.nelements());
  for (const Double value : values) {
    hash.f64(value);
  }
  return hash.value();
}

std::uint64_t hash_row_flags(const Vector<Bool>& values,
                             std::uint64_t& flagged_rows) {
  Fnv1a64 hash;
  hash.u64(values.nelements());
  flagged_rows = 0;
  for (const Bool value : values) {
    hash.boolean(value);
    if (value) {
      ++flagged_rows;
    }
  }
  return hash.value();
}

void guard_first_vb(const AWVisResampler* object, const Array<DComplex>& grid,
                    const VBStore& vbs) {
  if (grid.ndim() != 4 || grid.shape()[0] != kExpectedNxy ||
      grid.shape()[1] != kExpectedNxy || grid.shape()[2] != 1 ||
      grid.shape()[3] != 1) {
    reject("DComplex grid shape does not match frozen 4096 x 4096 x 1 x 1");
  }
  if (vbs.vb_p == nullptr) {
    reject("first VBStore has no visibility buffer");
  }
  if (vbs.beginRow_p != kExpectedBeginRow || vbs.endRow_p != kExpectedEndRow ||
      vbs.nRow_p != kExpectedRows || vbs.spwID_p != kExpectedSpw) {
    reject("first VBStore row range or SPW differs from frozen v5");
  }
  if (vbs.flagCube_p.ndim() != 3 ||
      vbs.flagCube_p.shape()[0] != kExpectedPolarizations ||
      vbs.flagCube_p.shape()[1] != kExpectedChannels ||
      vbs.flagCube_p.shape()[2] != kExpectedRows) {
    reject("first VBStore flag cube is not 4 x 64 x 325");
  }
  if (vbs.imagingWeight_p.nrow() != kExpectedChannels ||
      vbs.imagingWeight_p.ncolumn() != kExpectedRows) {
    reject("first VBStore imagingWeight is not 64 x 325");
  }
  if (vbs.uvw_p.nrow() != 3 || vbs.uvw_p.ncolumn() != kExpectedRows) {
    reject("first VBStore UVW matrix is not 3 x 325");
  }
  if (vbs.rowFlag_p.nelements() != kExpectedRows ||
      vbs.freq_p.nelements() != kExpectedChannels) {
    reject("first VBStore row-flag or frequency extent differs from frozen v5");
  }
  const auto& probe = AwProbeView::from(object);
  if (probe.channel_map().nelements() != kExpectedChannels ||
      probe.polarization_map().nelements() != kExpectedPolarizations ||
      probe.dphase().nelements() < kExpectedRows) {
    reject("first VBStore maps or dphase extent differs from frozen v5");
  }
  const auto& row_ids = vbs.vb_p->rowIds();
  if (row_ids.nelements() != kExpectedRows) {
    reject("first VBStore row-ID extent differs from frozen v5");
  }
  for (Int row = 0; row != kExpectedRows; ++row) {
    if (static_cast<std::uint64_t>(row_ids[row]) !=
        static_cast<std::uint64_t>(row)) {
      reject("first VBStore row IDs are not exactly 0 through 324");
    }
  }
  std::uint64_t flagged_rows = 0;
  if (hash_row_ids(vbs) != kExpectedRowIdsHash ||
      hash_channel_map(probe.channel_map()) != kExpectedChannelMapHash ||
      hash_channel_map(probe.polarization_map()) !=
          kExpectedPolarizationMapHash ||
      hash_frequencies(vbs.freq_p) != kExpectedFrequencyHash ||
      hash_row_flags(vbs.rowFlag_p, flagged_rows) != kExpectedRowFlagsHash ||
      flagged_rows != kExpectedFlaggedRows) {
    reject("first VBStore identity hashes differ from frozen CASA v5");
  }
}

void hash_common_header(Fnv1a64& hash, const AWVisResampler* object,
                        const Array<DComplex>& grid, const VBStore& vbs,
                        Bool use_conjugate_frequency_cf) {
  const auto& probe = AwProbeView::from(object);
  hash.boolean(use_conjugate_frequency_cf);
  hash.u64(static_cast<std::uint64_t>(vbs.beginRow_p));
  hash.u64(static_cast<std::uint64_t>(vbs.endRow_p));
  hash.u64(static_cast<std::uint64_t>(vbs.nRow_p));
  hash.u64(static_cast<std::uint64_t>(vbs.spwID_p));
  hash.f64(vbs.imRefFreq_p);
  for (const Int extent : grid.shape()) {
    hash.u64(static_cast<std::uint64_t>(extent));
  }
  hash.u64(probe.channel_map().nelements());
  for (const Int mapped : probe.channel_map()) {
    hash.u64(static_cast<std::uint64_t>(static_cast<std::int64_t>(mapped)));
  }
  hash.u64(probe.polarization_map().nelements());
  for (const Int mapped : probe.polarization_map()) {
    hash.u64(static_cast<std::uint64_t>(static_cast<std::int64_t>(mapped)));
  }
  const auto& row_ids = vbs.vb_p->rowIds();
  hash.u64(row_ids.nelements());
  for (const casacore::rownr_t row_id : row_ids) {
    hash.u64(static_cast<std::uint64_t>(row_id));
  }
}

RecomputedCall recompute_call(const AWVisResampler* object,
                              const Array<DComplex>& grid, const VBStore& vbs,
                              std::uint64_t call, std::uint64_t term,
                              Bool use_conjugate_frequency_cf) {
  const auto& probe = AwProbeView::from(object);
  Fnv1a64 stream;
  Fnv1a64 geometry;
  geometry.u64(call);
  geometry.u64(0);
  geometry.u64(term);
  hash_common_header(stream, object, grid, vbs, use_conjugate_frequency_cf);
  hash_common_header(geometry, object, grid, vbs, use_conjugate_frequency_cf);

  std::uint64_t sources = 0;
  for (Int row = vbs.beginRow_p; row < vbs.endRow_p; ++row) {
    stream.u64(static_cast<std::uint64_t>(row));
    stream.boolean(vbs.rowFlag_p[row]);
    geometry.u64(static_cast<std::uint64_t>(row));
    geometry.boolean(vbs.rowFlag_p[row]);
    if (vbs.rowFlag_p[row]) {
      continue;
    }
    for (Int axis = 0; axis != 3; ++axis) {
      stream.f64(vbs.uvw_p(axis, row));
      geometry.f64(vbs.uvw_p(axis, row));
    }
    stream.f64(probe.dphase()[row]);
    geometry.f64(probe.dphase()[row]);
    for (Int channel = 0; channel != kExpectedChannels; ++channel) {
      const Int target_channel = probe.channel_map()[channel];
      const Float weight = vbs.imagingWeight_p(channel, row);
      if (target_channel < 0 || target_channel >= grid.shape()[3]) {
        continue;
      }
      stream.u64(static_cast<std::uint64_t>(channel));
      stream.f64(vbs.freq_p[channel]);
      for (Int polarization = 0; polarization != kExpectedPolarizations;
           ++polarization) {
        stream.boolean(vbs.flagCube_p(polarization, channel, row));
      }
      if (weight == 0.0F) {
        continue;
      }
      geometry.u64(sources);
      geometry.u64(static_cast<std::uint64_t>(channel));
      geometry.f64(vbs.freq_p[channel]);
      for (Int polarization = 0; polarization != kExpectedPolarizations;
           ++polarization) {
        geometry.boolean(vbs.flagCube_p(polarization, channel, row));
      }
      ++sources;
    }
  }
  return RecomputedCall{call, 0, term, sources, stream.value(),
                        geometry.value()};
}

ComponentHashes component_hashes(const AWVisResampler* object,
                                 const Array<DComplex>& grid,
                                 const VBStore& vbs,
                                 Bool use_conjugate_frequency_cf,
                                 Counts& counts) {
  const auto& probe = AwProbeView::from(object);
  ComponentHashes result;
  Fnv1a64 header;
  hash_common_header(header, object, grid, vbs, use_conjugate_frequency_cf);
  result.header = header.value();
  result.row_ids = hash_row_ids(vbs);
  result.channel_map = hash_channel_map(probe.channel_map());
  result.polarization_map = hash_channel_map(probe.polarization_map());
  result.frequencies = hash_frequencies(vbs.freq_p);
  result.row_flags = hash_row_flags(vbs.rowFlag_p, counts.flagged_rows);

  Fnv1a64 uvw_dphase;
  Fnv1a64 flag_masks;
  Fnv1a64 imaging_weights;
  Fnv1a64 admission;
  uvw_dphase.u64(kExpectedRows);
  flag_masks.u64(kExpectedRows);
  flag_masks.u64(kExpectedChannels);
  flag_masks.u64(kExpectedPolarizations);
  imaging_weights.u64(kExpectedRows);
  imaging_weights.u64(kExpectedChannels);
  admission.u64(kExpectedRows);
  admission.u64(kExpectedChannels);
  for (Int row = 0; row != kExpectedRows; ++row) {
    uvw_dphase.u64(static_cast<std::uint64_t>(row));
    for (Int axis = 0; axis != 3; ++axis) {
      uvw_dphase.f64(vbs.uvw_p(axis, row));
    }
    uvw_dphase.f64(probe.dphase()[row]);
    for (Int channel = 0; channel != kExpectedChannels; ++channel) {
      flag_masks.u64(static_cast<std::uint64_t>(row));
      flag_masks.u64(static_cast<std::uint64_t>(channel));
      for (Int polarization = 0; polarization != kExpectedPolarizations;
           ++polarization) {
        flag_masks.boolean(vbs.flagCube_p(polarization, channel, row));
      }
      imaging_weights.u64(static_cast<std::uint64_t>(row));
      imaging_weights.u64(static_cast<std::uint64_t>(channel));
      const Float weight = vbs.imagingWeight_p(channel, row);
      imaging_weights.f32(weight);

      const Int mapped = probe.channel_map()[channel];
      const bool target_valid = mapped >= 0 && mapped < grid.shape()[3];
      const bool weight_nonzero = weight != 0.0F;
      const bool admitted =
          !vbs.rowFlag_p[row] && target_valid && weight_nonzero;
      admission.u64(static_cast<std::uint64_t>(row));
      admission.u64(static_cast<std::uint64_t>(channel));
      admission.boolean(!vbs.rowFlag_p[row]);
      admission.boolean(target_valid);
      admission.boolean(weight_nonzero);
      admission.boolean(admitted);
      if (weight_nonzero) {
        ++counts.nonzero_imaging_weights;
      } else {
        ++counts.zero_imaging_weights;
      }
      if (admitted) {
        ++counts.admitted_channels;
      }
    }
  }
  result.uvw_dphase = uvw_dphase.value();
  result.flag_masks = flag_masks.value();
  result.imaging_weights = imaging_weights.value();
  result.admission = admission.value();
  return result;
}

void append_int_vector(std::ostringstream& output, const Vector<Int>& values) {
  output << '[';
  for (std::size_t index = 0; index != values.nelements(); ++index) {
    if (index != 0) {
      output << ',';
    }
    output << values[index];
  }
  output << ']';
}

void append_row_ids(std::ostringstream& output, const VBStore& vbs) {
  const auto& values = vbs.vb_p->rowIds();
  output << '[';
  for (std::size_t index = 0; index != values.nelements(); ++index) {
    if (index != 0) {
      output << ',';
    }
    output << static_cast<std::uint64_t>(values[index]);
  }
  output << ']';
}

void append_frequency_bits(std::ostringstream& output,
                           const Vector<Double>& values) {
  output << '[';
  for (std::size_t index = 0; index != values.nelements(); ++index) {
    if (index != 0) {
      output << ',';
    }
    output << f64_bits(values[index]);
  }
  output << ']';
}

std::string completed_evidence(const AWVisResampler* object,
                               const Array<DComplex>& grid, const VBStore& vbs,
                               Bool use_conjugate_frequency_cf,
                               const ComponentHashes& components,
                               const Counts& counts,
                               const std::array<RecomputedCall, 2>& calls) {
  const auto& probe = AwProbeView::from(object);
  std::ostringstream output;
  output << "{\"schema\":\"" << kEvidenceSchema
         << "\",\"status\":\"completed-controlled-stop\","
            "\"result\":\"completed-native-components-exact-frozen-v5\","
            "\"role\":\"bounded-correctness-oracle-not-performance-evidence\","
            "\"producer\":\"CASA\","
            "\"casa_version\":\"6.7.5.18\","
            "\"casa_version_string\":\"6.7.5-18\","
            "\"casa_source_commit\":\""
         << kCasaCommit << "\",\"casacore_source_commit\":\"" << kCasacoreCommit
         << "\",\"datatogrid_symbol\":\"" << kDComplexSymbol
         << "\",\"symbol_owner\":\"libcasacpp_synthesis.6.dylib\","
            "\"observed_dispatch\":\"first-dcomplex-non-psf\","
            "\"diagnostic_hook_added\":true,"
            "\"normal_execution_behavior_changed\":false,"
            "\"production_science_arithmetic_changed\":false,"
            "\"original_datatogrid\":\"not-invoked\","
            "\"grid_storage\":\"received-not-read-or-written\","
            "\"grid_dispatch\":\"not-entered\","
            "\"sumwt\":\"not-read-or-written\","
            "\"formed_image\":false,"
            "\"normalization\":\"not-entered\","
            "\"fft\":\"not-entered\","
            "\"products\":\"not-entered\","
            "\"completed_calls\":1,"
            "\"terms_observed\":[0],"
            "\"hash_contracts\":{"
            "\"algorithm\":\"fnv1a64\","
            "\"offset_basis\":14695981039346656037,"
            "\"prime\":1099511628211,"
            "\"integer_encoding\":\"little-endian\","
            "\"float_encoding\":\"ieee754-bits-little-endian\","
            "\"boolean_encoding\":\"one-byte-0-or-1\","
            "\"recomposition\":\"casa-6.7.5.18-bracket-hash-call-inputs\"},"
            "\"frozen_parent_receipts\":{\"casa_v5\":{"
            "\"schema\":\"casa-aw-datagrid-bracket-v1\","
            "\"receipt_sha256\":\""
         << kFrozenV5Sha << "\"}},"
            "\"header\":{\"use_conjugate_frequency_cf\":"
         << (use_conjugate_frequency_cf ? "true" : "false")
         << ",\"begin_row\":" << vbs.beginRow_p << ",\"end_row\":"
         << vbs.endRow_p << ",\"n_row\":" << vbs.nRow_p << ",\"spw_id\":"
         << vbs.spwID_p << ",\"im_ref_freq_bits\":"
         << f64_bits(vbs.imRefFreq_p) << ",\"grid_shape\":[";
  for (std::size_t index = 0; index != grid.shape().nelements(); ++index) {
    if (index != 0) {
      output << ',';
    }
    output << grid.shape()[index];
  }
  output << "],\"channel_map\":";
  append_int_vector(output, probe.channel_map());
  output << ",\"polarization_map\":";
  append_int_vector(output, probe.polarization_map());
  output << ",\"row_ids\":";
  append_row_ids(output, vbs);
  output << ",\"frequency_bits\":";
  append_frequency_bits(output, vbs.freq_p);
  output << "},\"component_hashes\":{\"header\":" << components.header
         << ",\"row_ids\":" << components.row_ids << ",\"channel_map\":"
         << components.channel_map << ",\"polarization_map\":"
         << components.polarization_map << ",\"frequencies\":"
         << components.frequencies << ",\"row_flags\":" << components.row_flags
         << ",\"uvw_dphase\":" << components.uvw_dphase
         << ",\"flag_masks\":" << components.flag_masks
         << ",\"imaging_weights\":" << components.imaging_weights
         << ",\"admission\":" << components.admission << "},\"counts\":{"
         << "\"flagged_rows\":" << counts.flagged_rows
         << ",\"zero_imaging_weights\":" << counts.zero_imaging_weights
         << ",\"nonzero_imaging_weights\":" << counts.nonzero_imaging_weights
         << ",\"admitted_channels\":" << counts.admitted_channels
         << "},\"recomputed_frozen_hashes\":[";
  for (std::size_t index = 0; index != calls.size(); ++index) {
    if (index != 0) {
      output << ',';
    }
    const RecomputedCall& call = calls[index];
    output << "{\"origin\":\""
           << (index == 0
                   ? "observed-first-tt0"
                   : "derived-from-observed-tt0-under-frozen-v5-contract")
           << "\",\"call\":" << call.call << ",\"block\":" << call.block
           << ",\"term\":" << call.term << ",\"source_count\":"
           << call.source_count << ",\"stream_hash\":" << call.stream_hash
           << ",\"geometry_hash\":" << call.geometry_hash << '}';
  }
  output << "],\"rows\":[";
  for (Int row = 0; row != kExpectedRows; ++row) {
    if (row != 0) {
      output << ',';
    }
    output << "{\"row\":" << row << ",\"row_flag\":"
           << (vbs.rowFlag_p[row] ? "true" : "false")
           << ",\"uvw_bits\":[" << f64_bits(vbs.uvw_p(0, row)) << ','
           << f64_bits(vbs.uvw_p(1, row)) << ','
           << f64_bits(vbs.uvw_p(2, row)) << "],\"dphase_bits\":"
           << f64_bits(probe.dphase()[row]) << ",\"flag_masks\":[";
    for (Int channel = 0; channel != kExpectedChannels; ++channel) {
      if (channel != 0) {
        output << ',';
      }
      std::uint32_t mask = 0;
      for (Int polarization = 0; polarization != kExpectedPolarizations;
           ++polarization) {
        if (vbs.flagCube_p(polarization, channel, row)) {
          mask |= UINT32_C(1) << static_cast<unsigned>(polarization);
        }
      }
      output << mask;
    }
    output << "],\"imaging_weight_bits\":[";
    for (Int channel = 0; channel != kExpectedChannels; ++channel) {
      if (channel != 0) {
        output << ',';
      }
      output << f32_bits(vbs.imagingWeight_p(channel, row));
    }
    output << "]}";
  }
  output << "]}";
  return output.str();
}

std::atomic<std::uint64_t>& observed_calls() {
  static std::atomic<std::uint64_t> calls{0};
  return calls;
}

void probe_dcomplex(AWVisResampler* object, Array<DComplex>& grid, VBStore& vbs,
                    Matrix<Double>&, const Bool& dopsf,
                    Bool use_conjugate_frequency_cf) {
  if (observed_calls().fetch_add(1, std::memory_order_relaxed) != 0) {
    reject("more than one DataToGrid call reached the first-TT0 oracle");
  }
  const Config config = read_config();
  guard_symbol_binding();
  if (dopsf) {
    reject("first DataToGrid call is a PSF call");
  }
  guard_first_vb(object, grid, vbs);

  const std::array<RecomputedCall, 2> calls = {
      recompute_call(object, grid, vbs, 0, 0, use_conjugate_frequency_cf),
      recompute_call(object, grid, vbs, 1, 1, use_conjugate_frequency_cf),
  };
  if (calls[0].source_count != kExpectedSources ||
      calls[1].source_count != kExpectedSources ||
      calls[0].stream_hash != kExpectedStream ||
      calls[1].stream_hash != kExpectedStream ||
      calls[0].geometry_hash != kExpectedGeometry[0] ||
      calls[1].geometry_hash != kExpectedGeometry[1]) {
    reject("recomposed TT0/TT1 hashes differ from frozen CASA v5");
  }

  Counts counts;
  const ComponentHashes components =
      component_hashes(object, grid, vbs, use_conjugate_frequency_cf, counts);
  if (counts.flagged_rows != kExpectedFlaggedRows ||
      counts.admitted_channels != kExpectedSources) {
    reject("native component counts differ from frozen CASA v5");
  }
  try {
    const std::string evidence =
        completed_evidence(object, grid, vbs, use_conjugate_frequency_cf,
                           components, counts, calls);
    atomic_no_clobber_receipt(config.output, envelope(evidence));
  } catch (const std::exception& error) {
    reject(std::string("could not publish completed receipt: ") + error.what());
  }
  raw_exit(kCompletedExit);
}

[[noreturn]] void probe_complex(AWVisResampler*, Array<Complex>&, VBStore&,
                                Matrix<Double>&, const Bool&, Bool) {
  reject("unexpected single-precision AW DataToGrid call");
}

#define CASA_DYLD_INTERPOSE(replacement, replacee)                                  \
  __attribute__((used)) static const struct {                                      \
    const void* replacement_address;                                               \
    const void* replacee_address;                                                  \
  } replacement##_##replacee##_interpose                                           \
      __attribute__((section("__DATA,__interpose"))) = {                          \
          reinterpret_cast<const void*>(                                           \
              reinterpret_cast<std::uintptr_t>(&replacement)),                    \
          reinterpret_cast<const void*>(                                           \
              reinterpret_cast<std::uintptr_t>(&replacee))}

CASA_DYLD_INTERPOSE(probe_dcomplex, casa_aw_datatogrid_dcomplex);
CASA_DYLD_INTERPOSE(probe_complex, casa_aw_datatogrid_complex);

}  // namespace

extern "C" __attribute__((visibility("default"), used)) std::uint64_t
casa_aw_datatogrid_native_components_ready_v1() {
  return symbol_has_exact_owner(&casa_aw_datatogrid_dcomplex) ? kReadyMagic : 0;
}
