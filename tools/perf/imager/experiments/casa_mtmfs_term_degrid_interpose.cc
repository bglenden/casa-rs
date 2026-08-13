// SPDX-License-Identifier: LGPL-3.0-or-later
//
// CASA 6.7.5.18 MT-MFS term-separated prediction oracle.
//
// This diagnostic interposes refim::MultiTermFTNew::get, executes the exact
// source order with the official sub-FTMs and modifyModelVis implementation,
// captures TT0 raw, TT1 raw, TT1 scaled, and combined visibility cubes, and
// exits from refim::MultiTermFTNew::finalizeToVis before residual gridding or
// image formation.

#include <synthesis/TransformMachines2/MultiTermFTNew.h>

#include <casacore/casa/Arrays/ArrayMath.h>

#include <array>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <type_traits>
#include <unistd.h>
#include <vector>

namespace {

using casa::refim::FTMachine;
using casa::refim::MultiTermFTNew;
using casa::vi::VisBuffer2;
using casacore::Bool;
using casacore::Complex;
using casacore::Cube;
using casacore::Double;
using casacore::Float;
using casacore::Int;
using casacore::uInt;

constexpr int kCompletedExit = 86;
constexpr int kRejectedExit = 87;
constexpr std::uint32_t kRecordBytes = 104;
constexpr char kCasaCommit[] = "418bb1a26df7c4aba663ff123b038b75a6fa0295";
constexpr std::uint64_t kFnvOffset = UINT64_C(0xcbf29ce484222325);
constexpr std::uint64_t kFnvPrime = UINT64_C(0x00000100000001b3);

extern "C" Bool
casa_mtmfs_modify_model_vis(MultiTermFTNew *, VisBuffer2 &, uInt) asm(
    "__ZN4casa5refim14MultiTermFTNew14modifyModelVisERNS_2vi10VisBuffer2Ej");

extern "C" void casa_mtmfs_get(MultiTermFTNew *, VisBuffer2 &, Int) asm(
    "__ZN4casa5refim14MultiTermFTNew3getERNS_2vi10VisBuffer2Ei");

extern "C" void casa_mtmfs_finalize_to_vis(MultiTermFTNew *) asm(
    "__ZN4casa5refim14MultiTermFTNew13finalizeToVisEv");

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

void write_all(int descriptor, const void *data, std::size_t bytes) {
  const auto *cursor = static_cast<const unsigned char *>(data);
  std::size_t remaining = bytes;
  while (remaining != 0) {
    const ssize_t written = ::write(descriptor, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(std::string("write artifact: ") +
                               std::strerror(errno));
    }
    if (written == 0) {
      throw std::runtime_error("write artifact returned zero");
    }
    cursor += written;
    remaining -= static_cast<std::size_t>(written);
  }
}

void atomic_file(const std::string &path, const void *data, std::size_t bytes) {
  if (path.empty() || path.front() != '/') {
    throw std::runtime_error("artifact path must be absolute");
  }
  if (::access(path.c_str(), F_OK) == 0) {
    throw std::runtime_error("refusing to overwrite artifact " + path);
  }
  const std::string temporary =
      path + ".tmp." + std::to_string(static_cast<long long>(::getpid()));
  const int descriptor =
      ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
             S_IRUSR | S_IWUSR);
  if (descriptor < 0) {
    throw std::runtime_error("create artifact " + temporary + ": " +
                             std::strerror(errno));
  }
  bool descriptor_open = true;
  try {
    write_all(descriptor, data, bytes);
    if (::fsync(descriptor) != 0) {
      throw std::runtime_error(std::string("fsync artifact: ") +
                               std::strerror(errno));
    }
    if (::close(descriptor) != 0) {
      descriptor_open = false;
      throw std::runtime_error(std::string("close artifact: ") +
                               std::strerror(errno));
    }
    descriptor_open = false;
    if (::rename(temporary.c_str(), path.c_str()) != 0) {
      throw std::runtime_error(std::string("publish artifact: ") +
                               std::strerror(errno));
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

struct Config {
  std::string binary;
  std::string receipt;
  std::uint64_t max_get_calls;
};

Config read_config() {
  const char *binary = std::getenv("CASA_MTMFS_TERM_DEGRID_BINARY");
  const char *receipt = std::getenv("CASA_MTMFS_TERM_DEGRID_RECEIPT");
  if (binary == nullptr || *binary == '\0' || receipt == nullptr ||
      *receipt == '\0') {
    throw std::runtime_error("CASA_MTMFS_TERM_DEGRID_BINARY and "
                             "CASA_MTMFS_TERM_DEGRID_RECEIPT are required");
  }
  Config config{binary, receipt, 0};
  if (config.binary.front() != '/' || config.receipt.front() != '/') {
    throw std::runtime_error("term-degrid artifact paths must be absolute");
  }
  if (::access(config.binary.c_str(), F_OK) == 0 ||
      ::access(config.receipt.c_str(), F_OK) == 0) {
    throw std::runtime_error(
        "refusing to overwrite a term-degrid oracle artifact");
  }
  const char *max_get_calls_text =
      std::getenv("CASA_MTMFS_TERM_DEGRID_MAX_GET_CALLS");
  config.max_get_calls = 0;
  if (max_get_calls_text != nullptr && *max_get_calls_text != '\0') {
    char *end = nullptr;
    errno = 0;
    const unsigned long long parsed =
        std::strtoull(max_get_calls_text, &end, 10);
    if (errno != 0 || end == max_get_calls_text || *end != '\0' ||
        parsed == 0) {
      throw std::runtime_error(
          "CASA_MTMFS_TERM_DEGRID_MAX_GET_CALLS must be a positive integer");
    }
    config.max_get_calls = static_cast<std::uint64_t>(parsed);
  }
  return config;
}

class MultiTermNewProbeAccess final : public MultiTermFTNew {
public:
  static uInt term_count(MultiTermFTNew &value) {
    return value.*(&MultiTermNewProbeAccess::nterms_p);
  }
  static Double reference_frequency_hz(MultiTermFTNew &value) {
    return value.*(&MultiTermNewProbeAccess::reffreq_p);
  }
  static FTMachine &sub_ftm(MultiTermFTNew &value, uInt term) {
    return *(value.*(&MultiTermNewProbeAccess::subftms_p))[term];
  }
  static Cube<Complex> &model_accumulator(MultiTermFTNew &value) {
    return value.*(&MultiTermNewProbeAccess::modviscube_p);
  }
};

template <typename T>
void append_integer_le(std::vector<unsigned char> &output, T value) {
  static_assert(std::is_integral_v<T>);
  using Unsigned = std::make_unsigned_t<T>;
  Unsigned bits = static_cast<Unsigned>(value);
  for (std::size_t index = 0; index != sizeof(T); ++index) {
    output.push_back(
        static_cast<unsigned char>((bits >> (index * 8U)) & Unsigned{0xff}));
  }
}

void append_f32(std::vector<unsigned char> &output, Float value) {
  std::uint32_t bits = 0;
  static_assert(sizeof(bits) == sizeof(value));
  std::memcpy(&bits, &value, sizeof(bits));
  append_integer_le(output, bits);
}

void append_f64(std::vector<unsigned char> &output, Double value) {
  std::uint64_t bits = 0;
  static_assert(sizeof(bits) == sizeof(value));
  std::memcpy(&bits, &value, sizeof(bits));
  append_integer_le(output, bits);
}

void append_complex(std::vector<unsigned char> &output, const Complex &value) {
  append_f32(output, value.real());
  append_f32(output, value.imag());
}

std::uint64_t fnv1a64(const std::vector<unsigned char> &values) {
  std::uint64_t hash = kFnvOffset;
  for (const unsigned char value : values) {
    hash ^= static_cast<std::uint64_t>(value);
    hash *= kFnvPrime;
  }
  return hash;
}

Float source_taylor_power(Double frequency, Double reference_frequency) {
  // Preserve MultiTermFTNew::modifyModelVis exactly: its channel frequency and
  // multiplier are Float locals, while the reference frequency is Double and
  // the pow call is unqualified with a runtime Int exponent.
  using namespace casacore;
  Float freq = frequency;
  Float mulfactor = (freq - reference_frequency) / reference_frequency;
  return pow(mulfactor, Int{1});
}

template <typename Function> bool symbol_has_exact_owner(Function function) {
  void *address = nullptr;
  static_assert(sizeof(function) == sizeof(address));
  std::memcpy(&address, &function, sizeof(address));
  Dl_info information{};
  return address != nullptr && dladdr(address, &information) != 0 &&
         information.dli_fname != nullptr &&
         std::string_view(information.dli_fname)
                 .find("libcasacpp_synthesis.6.dylib") !=
             std::string_view::npos;
}

class OracleState {
public:
  OracleState() : config_(read_config()) {
    if (!symbol_has_exact_owner(&casa_mtmfs_modify_model_vis)) {
      throw std::runtime_error(
          "modifyModelVis is not two-level bound to CASA synthesis 6");
    }
  }

  void capture_call(VisBuffer2 &vb, const Cube<Complex> &tt0,
                    const Cube<Complex> &tt1_raw,
                    const Cube<Complex> &tt1_scaled,
                    const Cube<Complex> &combined, Double reference_frequency) {
    if (vb.nRows() == 0 || vb.nChannels() <= 0 ||
        vb.visCubeModel().shape() != tt0.shape() ||
        tt0.shape() != tt1_raw.shape() || tt0.shape() != tt1_scaled.shape() ||
        tt0.shape() != combined.shape() || tt0.shape()[0] != 4 ||
        tt0.shape()[1] != vb.nChannels() ||
        tt0.shape()[2] != static_cast<Int>(vb.nRows())) {
      throw std::runtime_error(
          "term-degrid cube topology is not 4 x channel x row");
    }
    if (!std::isfinite(reference_frequency) || reference_frequency <= 0.0) {
      throw std::runtime_error("invalid MT-MFS reference frequency");
    }
    const auto &rows = vb.rowIds();
    const auto &spws = vb.spectralWindows();
    const auto &frequencies = vb.getFrequencies(0);
    if (rows.nelements() != vb.nRows() || spws.nelements() != vb.nRows() ||
        frequencies.nelements() != static_cast<uInt>(vb.nChannels())) {
      throw std::runtime_error(
          "term-degrid row, SPW, or frequency identity extent differs");
    }
    const std::uint32_t call = static_cast<std::uint32_t>(call_count_);
    constexpr std::array<Int, 2> hands{0, 3};
    for (casacore::rownr_t row = 0; row != vb.nRows(); ++row) {
      const auto spw = static_cast<std::uint32_t>(spws[row]);
      for (Int channel = 0; channel != vb.nChannels(); ++channel) {
        const Double frequency = frequencies[channel];
        const Float taylor_power =
            source_taylor_power(frequency, reference_frequency);
        append_integer_le(bytes_, call);
        append_integer_le(bytes_, static_cast<std::uint32_t>(row));
        append_integer_le(bytes_, static_cast<std::uint64_t>(rows[row]));
        append_integer_le(bytes_, spw);
        append_integer_le(bytes_, static_cast<std::uint32_t>(channel));
        append_f64(bytes_, frequency);
        append_f32(bytes_, taylor_power);
        append_integer_le(bytes_, std::uint32_t{0});
        for (const Cube<Complex> *boundary :
             {&tt0, &tt1_raw, &tt1_scaled, &combined}) {
          for (const Int hand : hands) {
            append_complex(bytes_,
                           (*boundary)(hand, channel, static_cast<Int>(row)));
          }
        }
        ++record_count_;
      }
    }
    ++call_count_;
    if (bytes_.size() !=
        record_count_ * static_cast<std::uint64_t>(kRecordBytes)) {
      throw std::runtime_error("term-degrid binary stride drift");
    }
  }

  bool should_complete_after_get() const {
    return config_.max_get_calls != 0 &&
           call_count_ >= config_.max_get_calls;
  }

  [[noreturn]] void complete(std::string_view terminal_boundary) {
    if (call_count_ == 0 || record_count_ == 0) {
      reject(
          "refim::MultiTermFTNew::finalizeToVis arrived without captured get "
          "calls");
    }
    try {
      atomic_file(config_.binary, bytes_.data(), bytes_.size());
      const std::uint64_t binary_fnv = fnv1a64(bytes_);
      std::ostringstream receipt;
      receipt << "{\n"
              << "  \"schema\": "
                 "\"casa-vlass-frozen-model-term-degrid-oracle-v1\",\n"
              << "  \"status\": \""
              << (terminal_boundary == "finalize-to-vis"
                      ? "completed-before-finalize-to-vis"
                      : "completed-before-residual-gridding")
              << "\",\n"
              << "  \"role\": "
                 "\"bounded-correctness-oracle-not-performance-evidence\",\n"
              << "  \"casa_version\": \"6.7.5.18\",\n"
              << "  \"casa_version_string\": \"6.7.5-18\",\n"
              << "  \"casa_source_commit\": \"" << kCasaCommit << "\",\n"
              << "  \"taylor_power_contract\": "
                 "\"source-float-frequency-unqualified-pow-runtime-int-v1\","
                 "\n"
              << "  \"binary\": \"" << json_escape(config_.binary) << "\",\n"
              << "  \"binary_record_size\": " << kRecordBytes << ",\n"
              << "  \"binary_record_count\": " << record_count_ << ",\n"
              << "  \"binary_bytes\": " << bytes_.size() << ",\n"
              << "  \"binary_fnv1a64\": " << binary_fnv << ",\n"
              << "  \"get_calls\": " << call_count_ << ",\n"
              << "  \"configured_max_get_calls\": " << config_.max_get_calls
              << ",\n"
              << "  \"terminal_boundary\": \""
              << json_escape(terminal_boundary) << "\",\n"
              << "  \"term_count\": 2,\n"
              << "  \"formed_residual\": false,\n"
              << "  \"residual_grid_dispatch\": false,\n"
              << "  \"finalize_to_vis\": \""
              << (terminal_boundary == "finalize-to-vis"
                      ? "intercepted-before-original"
                      : "not-entered")
              << "\",\n"
              << "  \"fft\": \"not-entered\",\n"
              << "  \"image_formation\": \"not-entered\",\n"
              << "  \"products\": \"not-entered\",\n"
              << "  \"clean_iterations\": 0\n"
              << "}\n";
      const std::string receipt_text = receipt.str();
      atomic_file(config_.receipt, receipt_text.data(), receipt_text.size());
      std::fprintf(stderr,
                   "casa_mtmfs_term_degrid_oracle status=complete calls=%llu "
                   "records=%llu binary=%s receipt=%s "
                   "terminal_boundary=%.*s\n",
                   static_cast<unsigned long long>(call_count_),
                   static_cast<unsigned long long>(record_count_),
                   config_.binary.c_str(), config_.receipt.c_str(),
                   static_cast<int>(terminal_boundary.size()),
                   terminal_boundary.data());
      raw_exit(kCompletedExit);
    } catch (const std::exception &error) {
      reject(error.what());
    }
  }

  [[noreturn]] void reject(std::string_view reason) {
    try {
      std::ostringstream receipt;
      receipt << "{\n"
              << "  \"schema\": "
                 "\"casa-vlass-frozen-model-term-degrid-oracle-v1\",\n"
              << "  \"status\": \"rejected-before-finalize-to-vis\",\n"
              << "  \"casa_version\": \"6.7.5.18\",\n"
              << "  \"casa_source_commit\": \"" << kCasaCommit << "\",\n"
              << "  \"reason\": \"" << json_escape(reason) << "\",\n"
              << "  \"get_calls\": " << call_count_ << ",\n"
              << "  \"binary_record_count\": " << record_count_ << "\n"
              << "}\n";
      const std::string receipt_text = receipt.str();
      if (::access(config_.receipt.c_str(), F_OK) != 0) {
        atomic_file(config_.receipt, receipt_text.data(), receipt_text.size());
      }
    } catch (...) {
    }
    const std::string message =
        "CASA MT-MFS term-degrid oracle rejected: " + std::string(reason) +
        "\n";
    (void)::write(STDERR_FILENO, message.data(), message.size());
    raw_exit(kRejectedExit);
  }

private:
  Config config_;
  std::vector<unsigned char> bytes_;
  std::uint64_t call_count_ = 0;
  std::uint64_t record_count_ = 0;
};

OracleState *&state_slot() {
  static OracleState *state = nullptr;
  return state;
}

OracleState &state() {
  OracleState *&slot = state_slot();
  if (slot == nullptr) {
    try {
      slot = new OracleState();
    } catch (const std::exception &error) {
      const std::string message =
          "CASA MT-MFS term-degrid oracle initialization failed: " +
          std::string(error.what()) + "\n";
      (void)::write(STDERR_FILENO, message.data(), message.size());
      raw_exit(kRejectedExit);
    }
  }
  return *slot;
}

void term_degrid_get(MultiTermFTNew *object, VisBuffer2 &vb, Int row) {
  OracleState &oracle = state();
  try {
    if (MultiTermNewProbeAccess::term_count(*object) != 2) {
      oracle.reject("frozen MT-MFS oracle requires exactly two terms");
    }
    if (row != -1) {
      oracle.reject("frozen MT-MFS oracle requires whole-buffer get calls");
    }

    MultiTermNewProbeAccess::sub_ftm(*object, 0).get(vb, row);
    Cube<Complex> tt0;
    tt0.assign(vb.visCubeModel());
    MultiTermNewProbeAccess::model_accumulator(*object).assign(
        vb.visCubeModel());

    vb.setVisCubeModel(Complex(0.0, 0.0));
    MultiTermNewProbeAccess::sub_ftm(*object, 1).get(vb, row);
    Cube<Complex> tt1_raw;
    tt1_raw.assign(vb.visCubeModel());

    if (!casa_mtmfs_modify_model_vis(object, vb, 1)) {
      oracle.reject("official CASA modifyModelVis returned false");
    }
    Cube<Complex> tt1_scaled;
    tt1_scaled.assign(vb.visCubeModel());

    MultiTermNewProbeAccess::model_accumulator(*object) += vb.visCubeModel();
    Cube<Complex> combined;
    combined.assign(MultiTermNewProbeAccess::model_accumulator(*object));
    vb.setVisCubeModel(MultiTermNewProbeAccess::model_accumulator(*object));

    oracle.capture_call(
        vb, tt0, tt1_raw, tt1_scaled, combined,
        MultiTermNewProbeAccess::reference_frequency_hz(*object));
    if (oracle.should_complete_after_get()) {
      oracle.complete("bounded-get");
    }
  } catch (const std::exception &error) {
    oracle.reject(error.what());
  } catch (...) {
    oracle.reject("term-degrid get threw a non-standard exception");
  }
}

[[noreturn]] void term_degrid_finalize(MultiTermFTNew *) {
  state().complete("finalize-to-vis");
}

#define CASA_DYLD_INTERPOSE(replacement, replacee)                             \
  __attribute__((used)) static const struct {                                  \
    const void *replacement_address;                                           \
    const void *replacee_address;                                              \
  } replacement##_##replacee##_interpose                                       \
      __attribute__((section("__DATA,__interpose"))) = {                       \
          reinterpret_cast<const void *>(                                      \
              reinterpret_cast<std::uintptr_t>(&replacement)),                 \
          reinterpret_cast<const void *>(                                      \
              reinterpret_cast<std::uintptr_t>(&replacee))}

CASA_DYLD_INTERPOSE(term_degrid_get, casa_mtmfs_get);
CASA_DYLD_INTERPOSE(term_degrid_finalize, casa_mtmfs_finalize_to_vis);

} // namespace

extern "C" __attribute__((visibility("default"), used)) std::uint64_t
casa_mtmfs_term_degrid_oracle_ready_v1() {
  return UINT64_C(0x434153414d544431);
}
