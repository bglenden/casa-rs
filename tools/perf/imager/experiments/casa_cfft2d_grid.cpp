// SPDX-License-Identifier: LGPL-3.0-or-later
//
// Bounded VLASS correctness diagnostic: run the exact casacore
// LatticeFFT::cfft2d path used by AWProject on a casa-rs prepared model plane.

#include <casacore/casa/Arrays/Array.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/BasicSL/Complex.h>
#include <casacore/lattices/LatticeMath/LatticeFFT.h>
#include <casacore/lattices/Lattices/ArrayLattice.h>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::vector<float> read_f32(const std::string& path, std::size_t count) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("cannot open input " + path);
  }
  std::vector<float> values(count);
  input.read(
      reinterpret_cast<char*>(values.data()),
      static_cast<std::streamsize>(count * sizeof(float)));
  if (input.gcount() != static_cast<std::streamsize>(count * sizeof(float))) {
    throw std::runtime_error("input has the wrong byte count: " + path);
  }
  if (input.peek() != std::ifstream::traits_type::eof()) {
    throw std::runtime_error("input has trailing bytes: " + path);
  }
  return values;
}

void write_complex32(
    const std::string& path,
    const casacore::Array<casacore::Complex>& values,
    std::size_t side) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output) {
    throw std::runtime_error("cannot open output " + path);
  }
  for (std::size_t x = 0; x < side; ++x) {
    for (std::size_t y = 0; y < side; ++y) {
      const auto value = values(casacore::IPosition(2, x, y));
      const float components[2] = {value.real(), value.imag()};
      output.write(
          reinterpret_cast<const char*>(components),
          static_cast<std::streamsize>(sizeof(components)));
    }
  }
  if (!output) {
    throw std::runtime_error("failed while writing output " + path);
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 4) {
      std::cerr << "usage: casa_cfft2d_grid SIDE INPUT_F32 OUTPUT_C32\n";
      return 2;
    }
    const auto side = static_cast<std::size_t>(std::stoull(argv[1]));
    if (side == 0) {
      throw std::runtime_error("SIDE must be positive");
    }
    const auto pixel_count = side * side;
    if (pixel_count / side != side) {
      throw std::runtime_error("SIDE squared overflows size_t");
    }
    const auto input = read_f32(argv[2], pixel_count);

    casacore::Array<casacore::Complex> values(
        casacore::IPosition(2, side, side));
    for (std::size_t x = 0; x < side; ++x) {
      for (std::size_t y = 0; y < side; ++y) {
        values(casacore::IPosition(2, x, y)) =
            casacore::Complex(input[x * side + y], 0.0F);
      }
    }
    casacore::ArrayLattice<casacore::Complex> lattice(values);
    casacore::LatticeFFT::cfft2d(lattice, true);
    write_complex32(argv[3], values, side);
    std::cout << "cfft2d side=" << side << " input=" << argv[2]
              << " output=" << argv[3] << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "casa_cfft2d_grid: " << error.what() << '\n';
    return 1;
  }
}
