// Evaluate the scalar operation graphs that can own CASA MT-MFS Taylor powers.
//
// This helper intentionally uses the same casacore scalar types, unqualified
// pow call, and runtime integer exponent as MultiTermFTNew::modifyModelVis.
// The Python driver supplies aligned frozen frequencies and consumes the
// bit-level output.  No imaging code or MeasurementSet is opened here.

#include <casacore/casa/BasicMath/Math.h>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <type_traits>

using namespace casacore;

#if defined(__clang__) || defined(__GNUC__)
#define CASA_RS_NOINLINE __attribute__((noinline))
#else
#define CASA_RS_NOINLINE
#endif

using SourceExpression = decltype(pow(Float{}, Int{}));
using StandardExpression = decltype(std::pow(Float{}, Int{}));

CASA_RS_NOINLINE Float source_power(Float x, Int order) {
  return pow(x, order);
}

CASA_RS_NOINLINE Float casacore_power(Float x, Int order) {
  return casacore::pow(x, casacore::Double(order));
}

CASA_RS_NOINLINE Float standard_power(Float x, Int order) {
  auto intermediate = std::pow(x, order);
  return Float(intermediate);
}

template <typename To, typename From> To bit_copy(From value) {
  static_assert(sizeof(To) == sizeof(From));
  To output{};
  std::memcpy(&output, &value, sizeof(output));
  return output;
}

template <typename T> void write_scalar(std::ofstream &output, T value) {
  output.write(reinterpret_cast<const char *>(&value), sizeof(value));
  if (!output) {
    throw std::runtime_error("failed to write Taylor graph output");
  }
}

int describe() {
  std::cout << "{"
            << "\"source_expression_size\":" << sizeof(SourceExpression) << ","
            << "\"source_expression_is_float\":"
            << (std::is_same_v<SourceExpression, Float> ? "true" : "false")
            << ","
            << "\"source_expression_is_double\":"
            << (std::is_same_v<SourceExpression, Double> ? "true" : "false")
            << ","
            << "\"standard_expression_size\":" << sizeof(StandardExpression)
            << ","
            << "\"standard_expression_is_float\":"
            << (std::is_same_v<StandardExpression, Float> ? "true" : "false")
            << ","
            << "\"standard_expression_is_double\":"
            << (std::is_same_v<StandardExpression, Double> ? "true" : "false")
            << "}\n";
  return 0;
}

int evaluate(const char *input_path, const char *output_path,
             const char *reference_bits_text, const char *order_text) {
  std::size_t parsed = 0;
  const auto reference_bits = std::stoull(reference_bits_text, &parsed, 16);
  if (parsed != std::strlen(reference_bits_text)) {
    throw std::runtime_error("reference-frequency bits are not hexadecimal");
  }
  const Double reference_frequency = bit_copy<Double>(reference_bits);
  if (!(std::isfinite(reference_frequency) && reference_frequency > 0.0)) {
    throw std::runtime_error("reference frequency is not finite and positive");
  }

  parsed = 0;
  const auto parsed_order = std::stoll(order_text, &parsed, 10);
  if (parsed != std::strlen(order_text) ||
      parsed_order < std::numeric_limits<Int>::min() ||
      parsed_order > std::numeric_limits<Int>::max()) {
    throw std::runtime_error("Taylor order is outside casacore::Int");
  }
  const Int order = Int(parsed_order);

  std::ifstream input(input_path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("failed to open Taylor graph input");
  }
  std::ofstream output(output_path, std::ios::binary | std::ios::trunc);
  if (!output) {
    throw std::runtime_error("failed to open Taylor graph output");
  }

  Double frequency = 0.0;
  while (input.read(reinterpret_cast<char *>(&frequency), sizeof(frequency))) {
    if (!std::isfinite(frequency)) {
      throw std::runtime_error("input frequency is not finite");
    }
    const Float freq = frequency;
    const Double delta = Double(freq) - reference_frequency;
    const Double ratio = delta / reference_frequency;
    const Float x = ratio;
    const Float source = source_power(x, order);
    const Float forced_casacore = casacore_power(x, order);
    const Float forced_standard = standard_power(x, order);
    if (!(std::isfinite(x) && std::isfinite(source) &&
          std::isfinite(forced_casacore) && std::isfinite(forced_standard))) {
      throw std::runtime_error("Taylor graph produced a non-finite value");
    }

    write_scalar(output, bit_copy<std::uint64_t>(frequency));
    write_scalar(output, bit_copy<std::uint32_t>(freq));
    write_scalar(output, bit_copy<std::uint64_t>(delta));
    write_scalar(output, bit_copy<std::uint64_t>(ratio));
    write_scalar(output, bit_copy<std::uint32_t>(x));
    write_scalar(output, bit_copy<std::uint32_t>(source));
    write_scalar(output, bit_copy<std::uint32_t>(forced_casacore));
    write_scalar(output, bit_copy<std::uint32_t>(forced_standard));
  }
  if (!input.eof()) {
    throw std::runtime_error("failed while reading Taylor graph input");
  }
  return 0;
}

int main(int argc, char **argv) {
  try {
    if (argc == 2 && std::string(argv[1]) == "--describe") {
      return describe();
    }
    if (argc != 5) {
      std::cerr << "usage: vlass_taylor_power_graphs "
                   "<frequencies-f64.bin> <output.bin> "
                   "<reference-frequency-u64-hex> <order>\n";
      return 2;
    }
    return evaluate(argv[1], argv[2], argv[3], argv[4]);
  } catch (const std::exception &error) {
    std::cerr << error.what() << "\n";
    return 2;
  }
}
