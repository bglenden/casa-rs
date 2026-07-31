// SPDX-License-Identifier: LGPL-3.0-or-later
//
// Emit the exact CASA 6.7.5.18 pre-W EVLA A-term and wideband normal-screen
// families represented by a persisted AWProject CF cache. This is a bounded
// architecture discriminator; it does not run tclean or alter the cache.

#include <synthesis/TransformMachines2/EVLAAperture.h>

#include <casacore/casa/Arrays/Array.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Vector.h>
#include <casacore/casa/BasicSL/Complex.h>
#include <casacore/casa/Containers/RecordInterface.h>
#include <casacore/coordinates/Coordinates/Coordinate.h>
#include <casacore/coordinates/Coordinates/CoordinateSystem.h>
#include <casacore/coordinates/Coordinates/LinearCoordinate.h>
#include <casacore/coordinates/Coordinates/SpectralCoordinate.h>
#include <casacore/coordinates/Coordinates/StokesCoordinate.h>
#include <casacore/images/Images/PagedImage.h>
#include <casacore/images/Images/TempImage.h>
#include <casacore/measures/Measures/MFrequency.h>
#include <casacore/measures/Measures/Stokes.h>

#include <algorithm>
#include <cmath>
#include <complex>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <unistd.h>

namespace {

using casacore::Complex;
using casacore::Coordinate;
using casacore::CoordinateSystem;
using casacore::Double;
using casacore::Float;
using casacore::Int;
using casacore::IPosition;
using casacore::LinearCoordinate;
using casacore::PagedImage;
using casacore::RecordInterface;
using casacore::SpectralCoordinate;
using casacore::StokesCoordinate;
using casacore::String;
using casacore::TempImage;
using casacore::Vector;

constexpr std::size_t kDefaultCropSide = 512;

struct State {
  double frequency_hz = 0.0;
  double conjugate_frequency_hz = 0.0;
  double parallactic_angle_deg = 0.0;
  double diameter_m = 0.0;
  int mueller_element = 0;
  int polarization = 0;
  int conjugate_polarization = 0;
  std::string telescope_name;
  std::string band_name;
};

struct Template {
  std::filesystem::path path;
  CoordinateSystem coordinates;
  IPosition shape;
  std::size_t cells = 0;
};

struct ScreenReceipt {
  double forward_peak = 0.0;
  double normal_peak = 0.0;
  double forward_outside_crop_peak = 0.0;
  double normal_outside_crop_peak = 0.0;
};

[[noreturn]] void fail(const std::string &message);

class MeasuresAipsrcGuard {
public:
  MeasuresAipsrcGuard() {
    const char *configured = std::getenv("CASA_EVLA_MEASURES_DIR");
    std::filesystem::path measures_dir;
    if (configured != nullptr && configured[0] != '\0') {
      measures_dir = configured;
    } else {
      const char *home = std::getenv("HOME");
      if (home == nullptr || home[0] == '\0') {
        fail("set CASA_EVLA_MEASURES_DIR or HOME before running the oracle");
      }
      measures_dir = std::filesystem::path(home) / ".casa" / "data";
    }
    if (!std::filesystem::is_directory(measures_dir / "geodetic" /
                                       "Observatories")) {
      fail("measures directory lacks geodetic/Observatories: " +
           measures_dir.string());
    }
    measures_dir_ = std::filesystem::canonical(measures_dir);
    path_ = std::filesystem::temp_directory_path() /
            ("casa-evla-pre-w-screen-" + std::to_string(getpid()) + ".rc");
    {
      std::ofstream output(path_);
      if (!output) {
        fail("cannot create temporary casacore resource file");
      }
      output << "measures.directory: " << measures_dir_.string() << "\n";
    }
    if (setenv("CASARCFILES", path_.c_str(), 1) != 0) {
      fail("cannot set CASARCFILES for the exact screen oracle");
    }
    if (measures_dir_.filename() != "data") {
      fail("BeamCalc requires CASA_EVLA_MEASURES_DIR to end in /data");
    }
    const std::string casa_root = measures_dir_.parent_path().string();
    if (setenv("CASAPATH", casa_root.c_str(), 1) != 0) {
      fail("cannot set CASAPATH for the exact screen oracle");
    }
  }

  MeasuresAipsrcGuard(const MeasuresAipsrcGuard &) = delete;
  MeasuresAipsrcGuard &operator=(const MeasuresAipsrcGuard &) = delete;

  ~MeasuresAipsrcGuard() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

  const std::filesystem::path &directory() const { return measures_dir_; }

private:
  std::filesystem::path measures_dir_;
  std::filesystem::path path_;
};

[[noreturn]] void fail(const std::string &message) {
  throw std::runtime_error(message);
}

std::string json_escape(const std::string &value) {
  std::ostringstream out;
  for (const unsigned char ch : value) {
    switch (ch) {
    case '"':
      out << "\\\"";
      break;
    case '\\':
      out << "\\\\";
      break;
    case '\b':
      out << "\\b";
      break;
    case '\f':
      out << "\\f";
      break;
    case '\n':
      out << "\\n";
      break;
    case '\r':
      out << "\\r";
      break;
    case '\t':
      out << "\\t";
      break;
    default:
      if (ch < 0x20) {
        out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
            << static_cast<unsigned int>(ch) << std::dec;
      } else {
        out << static_cast<char>(ch);
      }
    }
  }
  return out.str();
}

double spectral_reference_hz(const CoordinateSystem &coordinates) {
  const int index = coordinates.findCoordinate(Coordinate::SPECTRAL);
  if (index < 0) {
    fail("CF image has no spectral coordinate");
  }
  const SpectralCoordinate spectral = coordinates.spectralCoordinate(index);
  const Vector<Double> reference = spectral.referenceValue();
  if (reference.nelements() == 0 || !std::isfinite(reference[0]) ||
      reference[0] <= 0.0) {
    fail("CF image has an invalid spectral reference frequency");
  }
  return reference[0];
}

int stokes_code(const CoordinateSystem &coordinates) {
  const int index = coordinates.findCoordinate(Coordinate::STOKES);
  if (index < 0) {
    fail("CF image has no Stokes coordinate");
  }
  const Vector<Int> stokes = coordinates.stokesCoordinate(index).stokes();
  if (stokes.size() != 1) {
    fail("CF image Stokes coordinate is not degenerate");
  }
  return stokes[0];
}

double required_double(const RecordInterface &record, const char *name) {
  if (!record.isDefined(name)) {
    fail(std::string("CF image misc-info is missing ") + name);
  }
  Double value = 0.0;
  record.get(name, value);
  if (!std::isfinite(value)) {
    fail(std::string("CF image misc-info has non-finite ") + name);
  }
  return value;
}

int required_int(const RecordInterface &record, const char *name) {
  if (!record.isDefined(name)) {
    fail(std::string("CF image misc-info is missing ") + name);
  }
  Int value = 0;
  record.get(name, value);
  return value;
}

std::string required_string(const RecordInterface &record, const char *name) {
  if (!record.isDefined(name)) {
    fail(std::string("CF image misc-info is missing ") + name);
  }
  String value;
  record.get(name, value);
  return value;
}

std::tuple<std::uint64_t, int> state_key(const State &state) {
  std::uint64_t frequency_bits = 0;
  static_assert(sizeof(frequency_bits) == sizeof(state.frequency_hz));
  std::memcpy(&frequency_bits, &state.frequency_hz, sizeof(frequency_bits));
  return {frequency_bits, state.mueller_element};
}

void validate_repeated_state(const State &expected, const State &candidate) {
  const bool equal =
      expected.frequency_hz == candidate.frequency_hz &&
      expected.conjugate_frequency_hz == candidate.conjugate_frequency_hz &&
      expected.parallactic_angle_deg == candidate.parallactic_angle_deg &&
      expected.diameter_m == candidate.diameter_m &&
      expected.mueller_element == candidate.mueller_element &&
      expected.polarization == candidate.polarization &&
      expected.conjugate_polarization == candidate.conjugate_polarization &&
      expected.telescope_name == candidate.telescope_name &&
      expected.band_name == candidate.band_name;
  if (!equal) {
    fail("CF cells disagree on metadata for one frequency/Mueller state");
  }
}

std::pair<std::vector<State>, Template>
inventory_cache(const std::filesystem::path &cache_root) {
  if (!std::filesystem::is_directory(cache_root)) {
    fail("CF cache root is not a directory: " + cache_root.string());
  }

  std::map<std::tuple<std::uint64_t, int>, State> states;
  Template selected_template;
  for (const auto &entry : std::filesystem::directory_iterator(cache_root)) {
    if (!entry.is_directory()) {
      continue;
    }
    const std::string name = entry.path().filename().string();
    if (name.rfind("CFS_", 0) != 0) {
      continue;
    }

    PagedImage<Complex> image(entry.path().string());
    const IPosition shape = image.shape();
    if (shape.size() != 4 || shape[0] <= 0 || shape[1] <= 0 || shape[2] != 1 ||
        shape[3] != 1) {
      fail("CF image has an unexpected shape: " + entry.path().string());
    }
    const std::size_t cells =
        static_cast<std::size_t>(shape[0]) * static_cast<std::size_t>(shape[1]);
    if (cells > selected_template.cells) {
      selected_template = {
          entry.path(),
          image.coordinates(),
          shape,
          cells,
      };
    }

    const RecordInterface &misc = image.miscInfo();
    State state;
    state.frequency_hz = spectral_reference_hz(image.coordinates());
    state.conjugate_frequency_hz = required_double(misc, "ConjFreq");
    state.parallactic_angle_deg = required_double(misc, "ParallacticAngle");
    state.diameter_m = required_double(misc, "Diameter");
    state.mueller_element = required_int(misc, "MuellerElement");
    state.polarization = stokes_code(image.coordinates());
    state.conjugate_polarization = required_int(misc, "ConjPoln");
    state.telescope_name = required_string(misc, "TelescopeName");
    state.band_name = required_string(misc, "BandName");

    const auto key = state_key(state);
    const auto [found, inserted] = states.emplace(key, state);
    if (!inserted) {
      validate_repeated_state(found->second, state);
    }
  }

  if (states.empty() || selected_template.cells == 0) {
    fail("CF cache contains no CFS_ image cells");
  }
  if (selected_template.shape[0] != selected_template.shape[1]) {
    fail("largest CF coordinate template is not square");
  }

  std::vector<State> ordered;
  ordered.reserve(states.size());
  for (const auto &[key, state] : states) {
    static_cast<void>(key);
    ordered.push_back(state);
  }
  std::sort(ordered.begin(), ordered.end(),
            [](const State &left, const State &right) {
              return std::tie(left.frequency_hz, left.mueller_element) <
                     std::tie(right.frequency_hz, right.mueller_element);
            });
  return {ordered, selected_template};
}

CoordinateSystem coordinates_for_state(const CoordinateSystem &base,
                                       const State &state, const int stokes,
                                       const double frequency_hz) {
  CoordinateSystem coordinates(base);
  const int stokes_index = coordinates.findCoordinate(Coordinate::STOKES);
  const int spectral_index = coordinates.findCoordinate(Coordinate::SPECTRAL);
  if (stokes_index < 0 || spectral_index < 0) {
    fail("coordinate template is missing Stokes or spectral coordinates");
  }

  Vector<Int> stokes_values(1);
  stokes_values[0] = stokes;
  coordinates.replaceCoordinate(StokesCoordinate(stokes_values), stokes_index);
  coordinates.replaceCoordinate(
      SpectralCoordinate(casacore::MFrequency::TOPO, frequency_hz, 1.0, 0.0),
      spectral_index);
  static_cast<void>(state);
  return coordinates;
}

casacore::Array<Complex>
make_screen(casa::refim::EVLAAperture &aperture,
            const CoordinateSystem &template_coordinates,
            const IPosition &shape, const State &state, const int stokes,
            const double frequency_hz) {
  TempImage<Complex> image(
      shape,
      coordinates_for_state(template_coordinates, state, stokes, frequency_hz));
  image.set(Complex(1.0F, 0.0F));
  const double pa_rad = state.parallactic_angle_deg * std::acos(-1.0) / 180.0;
  aperture.applySky(image, pa_rad, true, 0, state.mueller_element,
                    frequency_hz);
  return image.get();
}

double peak_amplitude(const casacore::Array<Complex> &screen) {
  double peak = 0.0;
  for (auto iterator = screen.cbegin(); iterator != screen.cend(); ++iterator) {
    peak = std::max(peak, static_cast<double>(std::abs(*iterator)));
  }
  return peak;
}

double write_crop(std::ofstream &output, const casacore::Array<Complex> &screen,
                  const std::size_t crop_start, const std::size_t crop_side) {
  const IPosition shape = screen.shape();
  const std::size_t side = static_cast<std::size_t>(shape[0]);
  double outside_peak = 0.0;
  for (std::size_t y = 0; y < side; ++y) {
    for (std::size_t x = 0; x < side; ++x) {
      const Complex value =
          screen(IPosition(4, static_cast<Int>(x), static_cast<Int>(y), 0, 0));
      const bool inside = x >= crop_start && x < crop_start + crop_side &&
                          y >= crop_start && y < crop_start + crop_side;
      if (inside) {
        const float pair[2] = {value.real(), value.imag()};
        output.write(reinterpret_cast<const char *>(pair), sizeof(pair));
      } else {
        outside_peak =
            std::max(outside_peak, static_cast<double>(std::abs(value)));
      }
    }
  }
  if (!output) {
    fail("failed while writing screen crop");
  }
  return outside_peak;
}

double template_uv_increment(const CoordinateSystem &coordinates,
                             const int axis) {
  const int index = coordinates.findCoordinate(Coordinate::LINEAR);
  if (index < 0) {
    fail("coordinate template has no linear UV coordinate");
  }
  const LinearCoordinate linear = coordinates.linearCoordinate(index);
  const Vector<Double> increment = linear.increment();
  if (increment.size() != 2 || axis < 0 || axis >= 2 ||
      !std::isfinite(increment[axis]) || increment[axis] == 0.0) {
    fail("coordinate template has an invalid UV increment");
  }
  return increment[axis];
}

double template_uv_reference_pixel(const CoordinateSystem &coordinates,
                                   const int axis) {
  const int index = coordinates.findCoordinate(Coordinate::LINEAR);
  if (index < 0) {
    fail("coordinate template has no linear UV coordinate");
  }
  const Vector<Double> reference =
      coordinates.linearCoordinate(index).referencePixel();
  if (reference.size() != 2 || axis < 0 || axis >= 2 ||
      !std::isfinite(reference[axis])) {
    fail("coordinate template has an invalid UV reference pixel");
  }
  return reference[axis];
}

void write_manifest(const std::filesystem::path &path,
                    const std::filesystem::path &cache_root,
                    const std::filesystem::path &measures_directory,
                    const std::filesystem::path &forward_path,
                    const std::filesystem::path &normal_path,
                    const Template &coordinate_template,
                    const std::vector<State> &states,
                    const std::vector<ScreenReceipt> &receipts,
                    const std::size_t crop_start, const std::size_t crop_side) {
  std::ofstream output(path);
  if (!output) {
    fail("cannot create manifest: " + path.string());
  }
  const auto side = static_cast<std::size_t>(coordinate_template.shape[0]);
  const double uv_increment_x =
      template_uv_increment(coordinate_template.coordinates, 0);
  const double uv_increment_y =
      template_uv_increment(coordinate_template.coordinates, 1);
  const double sky_increment_x = 1.0 / (side * std::abs(uv_increment_x));
  const double sky_increment_y = 1.0 / (side * std::abs(uv_increment_y));

  output << std::setprecision(17);
  output << "{\n"
         << "  \"schema\": \"casa-rs-vlass-evla-pre-w-screens/v1\",\n"
         << "  \"role\": \"production-inert-architecture-discriminator\",\n"
         << "  \"casa_semantics\": \"EVLAAperture::applySky before "
            "WTerm::applySky in AWConvFunc::fillConvFuncBuffer2\",\n"
         << "  \"cache_root\": \"" << json_escape(cache_root.string())
         << "\",\n"
         << "  \"measures_directory\": \""
         << json_escape(measures_directory.string()) << "\",\n"
         << "  \"coordinate_template\": \""
         << json_escape(coordinate_template.path.string()) << "\",\n"
         << "  \"full_shape\": [" << side << ", " << side << "],\n"
         << "  \"crop_start\": [" << crop_start << ", " << crop_start << "],\n"
         << "  \"crop_shape\": [" << crop_side << ", " << crop_side << "],\n"
         << "  \"uv_increment_lambda\": [" << uv_increment_x << ", "
         << uv_increment_y << "],\n"
         << "  \"uv_reference_pixel\": ["
         << template_uv_reference_pixel(coordinate_template.coordinates, 0)
         << ", "
         << template_uv_reference_pixel(coordinate_template.coordinates, 1)
         << "],\n"
         << "  \"derived_sky_increment_rad\": [" << sky_increment_x << ", "
         << sky_increment_y << "],\n"
         << "  \"complex_dtype\": \"native-complex64-interleaved\",\n"
         << "  \"state_order\": \"frequency-hz-then-mueller\",\n"
         << "  \"forward_path\": \"" << json_escape(forward_path.string())
         << "\",\n"
         << "  \"normal_path\": \"" << json_escape(normal_path.string())
         << "\",\n"
         << "  \"states\": [\n";
  for (std::size_t index = 0; index < states.size(); ++index) {
    const State &state = states[index];
    const ScreenReceipt &receipt = receipts[index];
    output << "    {\"index\": " << index
           << ", \"frequency_hz\": " << state.frequency_hz
           << ", \"conjugate_frequency_hz\": " << state.conjugate_frequency_hz
           << ", \"parallactic_angle_deg\": " << state.parallactic_angle_deg
           << ", \"mueller_element\": " << state.mueller_element
           << ", \"polarization\": " << state.polarization
           << ", \"conjugate_polarization\": " << state.conjugate_polarization
           << ", \"telescope\": \"" << json_escape(state.telescope_name)
           << "\", \"band\": \"" << json_escape(state.band_name)
           << "\", \"diameter_m\": " << state.diameter_m
           << ", \"forward_peak\": " << receipt.forward_peak
           << ", \"normal_peak\": " << receipt.normal_peak
           << ", \"forward_outside_crop_peak\": "
           << receipt.forward_outside_crop_peak
           << ", \"normal_outside_crop_peak\": "
           << receipt.normal_outside_crop_peak << "}";
    output << (index + 1 == states.size() ? "\n" : ",\n");
  }
  output << "  ]\n}\n";
  if (!output) {
    fail("failed while writing manifest");
  }
}

std::size_t parse_crop_side(const char *value) {
  std::size_t consumed = 0;
  const unsigned long parsed = std::stoul(value, &consumed, 10);
  if (value[consumed] != '\0' || parsed == 0) {
    fail("crop side must be a positive integer");
  }
  return static_cast<std::size_t>(parsed);
}

int run(const int argc, char **argv) {
  if (argc < 3 || argc > 4) {
    std::cerr << "usage: " << argv[0]
              << " <cf-cache-root> <output-prefix> [crop-side]\n";
    return 2;
  }
  const std::filesystem::path cache_root(argv[1]);
  const std::filesystem::path output_prefix(argv[2]);
  const std::size_t crop_side =
      argc == 4 ? parse_crop_side(argv[3]) : kDefaultCropSide;

  const MeasuresAipsrcGuard measures;
  const auto [states, coordinate_template] = inventory_cache(cache_root);
  const std::size_t full_side =
      static_cast<std::size_t>(coordinate_template.shape[0]);
  if (crop_side > full_side || (full_side - crop_side) % 2 != 0) {
    fail("crop side must fit the full screen and preserve an integral center");
  }
  const std::size_t crop_start = (full_side - crop_side) / 2;

  const std::filesystem::path forward_path =
      output_prefix.string() + ".forward.c64";
  const std::filesystem::path normal_path =
      output_prefix.string() + ".normal.c64";
  const std::filesystem::path manifest_path =
      output_prefix.string() + ".manifest.json";
  std::filesystem::create_directories(output_prefix.parent_path());
  std::ofstream forward_output(forward_path, std::ios::binary);
  std::ofstream normal_output(normal_path, std::ios::binary);
  if (!forward_output || !normal_output) {
    fail("cannot create screen output files");
  }

  std::vector<ScreenReceipt> receipts;
  receipts.reserve(states.size());
  for (std::size_t index = 0; index < states.size(); ++index) {
    const State &state = states[index];
    casa::refim::EVLAAperture aperture;
    aperture.cacheVBInfo(state.telescope_name,
                         static_cast<Float>(state.diameter_m));
    const casacore::Array<Complex> forward = make_screen(
        aperture, coordinate_template.coordinates, coordinate_template.shape,
        state, state.polarization, state.frequency_hz);
    const casacore::Array<Complex> conjugate = make_screen(
        aperture, coordinate_template.coordinates, coordinate_template.shape,
        state, state.conjugate_polarization, state.conjugate_frequency_hz);
    casacore::Array<Complex> normal(forward.shape());
    auto normal_iterator = normal.begin();
    auto forward_iterator = forward.cbegin();
    auto conjugate_iterator = conjugate.cbegin();
    for (; normal_iterator != normal.end();
         ++normal_iterator, ++forward_iterator, ++conjugate_iterator) {
      *normal_iterator = *forward_iterator * std::conj(*conjugate_iterator);
    }

    ScreenReceipt receipt;
    receipt.forward_peak = peak_amplitude(forward);
    receipt.normal_peak = peak_amplitude(normal);
    receipt.forward_outside_crop_peak =
        write_crop(forward_output, forward, crop_start, crop_side);
    receipt.normal_outside_crop_peak =
        write_crop(normal_output, normal, crop_start, crop_side);
    receipts.push_back(receipt);

    std::cerr << "evla_pre_w_screen_progress state=" << (index + 1) << "/"
              << states.size() << " frequency_hz=" << state.frequency_hz
              << " mueller=" << state.mueller_element
              << " forward_peak=" << receipt.forward_peak
              << " normal_peak=" << receipt.normal_peak << "\n";
  }
  forward_output.close();
  normal_output.close();
  write_manifest(manifest_path, cache_root, measures.directory(), forward_path,
                 normal_path, coordinate_template, states, receipts, crop_start,
                 crop_side);
  std::cout << manifest_path << "\n";
  return 0;
}

} // namespace

int main(const int argc, char **argv) {
  try {
    return run(argc, argv);
  } catch (const std::exception &error) {
    std::cerr << "casa_evla_pre_w_screen_oracle: " << error.what() << "\n";
    return 1;
  }
}
