// SPDX-License-Identifier: LGPL-3.0-or-later
//
// Interpose the exact CASA 6.7.5.18 refim AWVisResampler::GridToData method
// with the separately compiled and trace-patched GridToDataTraced definition.

#include <synthesis/TransformMachines2/AWVisResampler.h>

#include <cstdint>

namespace {

using casa::refim::AWVisResampler;
using casa::refim::VBStore;
using casacore::Array;
using casacore::Complex;

extern "C" void casa_aw_grid_to_data_original(
    AWVisResampler *, VBStore &,
    const Array<Complex>
        &) asm("__ZN4casa5refim14AWVisResampler10GridToDataERNS0_"
               "7VBStoreERKN8casacore5ArrayINSt3__17complexIfEEEE");

extern "C" void casa_aw_grid_to_data_traced(
    AWVisResampler *, VBStore &,
    const Array<Complex>
        &) asm("__ZN4casa5refim14AWVisResampler16GridToDataTracedERNS0_"
               "7VBStoreERKN8casacore5ArrayINSt3__17complexIfEEEE");

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

CASA_DYLD_INTERPOSE(casa_aw_grid_to_data_traced, casa_aw_grid_to_data_original);

} // namespace

extern "C" __attribute__((visibility("default"), used)) std::uint64_t
casa_aw_degrid_prefix_oracle_ready_v1() {
  return UINT64_C(0x4341534141575031);
}
