// SPDX-License-Identifier: LGPL-3.0-or-later
#[path = "api/array_shape_metadata.rs"]
mod array_shape_metadata;
#[path = "api/casacore_ism_indirect_arrays.rs"]
mod casacore_ism_indirect_arrays;
#[path = "api/fixed_array_storage.rs"]
mod fixed_array_storage;
#[path = "api/generated_array_storage.rs"]
mod generated_array_storage;
#[path = "api/keyword_array_order.rs"]
mod keyword_array_order;
#[path = "api/manager_layout.rs"]
mod manager_layout;
#[path = "api/memory_harness.rs"]
mod memory_harness;
#[path = "api/selected_incremental_arrays.rs"]
mod selected_incremental_arrays;
#[path = "api/selected_read_footprint.rs"]
mod selected_read_footprint;
#[path = "api/ssm_long_strings.rs"]
mod ssm_long_strings;
#[path = "api/streamed_scalar_storage.rs"]
mod streamed_scalar_storage;
#[path = "api/streamed_writer_memory.rs"]
mod streamed_writer_memory;
#[path = "api/table_desc_header.rs"]
mod table_desc_header;
#[path = "api/table_examples.rs"]
mod table_examples;
#[path = "api/table_quantum.rs"]
mod table_quantum;
#[path = "api/taql.rs"]
mod taql;
#[path = "api/taql_simulation_arithmetic.rs"]
mod taql_simulation_arithmetic;
#[path = "api/tiled_endian_reads.rs"]
mod tiled_endian_reads;
#[path = "api/undefined_array_cells.rs"]
mod undefined_array_cells;
#[path = "api/zero_dimensional_cells.rs"]
mod zero_dimensional_cells;

#[cfg(unix)]
mod lock_multiprocess;
