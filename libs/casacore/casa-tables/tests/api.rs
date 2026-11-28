// SPDX-License-Identifier: LGPL-3.0-or-later
#[path = "api_cases/array_shape_metadata.rs"]
mod array_shape_metadata;
#[path = "api_cases/casacore_ism_indirect_arrays.rs"]
mod casacore_ism_indirect_arrays;
#[path = "api_cases/fixed_array_storage.rs"]
mod fixed_array_storage;
#[path = "api_cases/generated_array_storage.rs"]
mod generated_array_storage;
#[path = "api_cases/keyword_array_order.rs"]
mod keyword_array_order;
#[path = "api_cases/manager_layout.rs"]
mod manager_layout;
#[path = "api_cases/memory_harness.rs"]
mod memory_harness;
#[path = "api_cases/selected_incremental_arrays.rs"]
mod selected_incremental_arrays;
#[path = "api_cases/selected_read_footprint.rs"]
mod selected_read_footprint;
#[path = "api_cases/ssm_long_strings.rs"]
mod ssm_long_strings;
#[path = "api_cases/streamed_scalar_storage.rs"]
mod streamed_scalar_storage;
#[path = "api_cases/streamed_writer_memory.rs"]
mod streamed_writer_memory;
#[cfg(unix)]
#[path = "api_cases/table_dat_publication.rs"]
mod table_dat_publication;
#[path = "api_cases/table_desc_header.rs"]
mod table_desc_header;
#[path = "api_cases/table_examples.rs"]
mod table_examples;
#[path = "api_cases/table_quantum.rs"]
mod table_quantum;
#[path = "api_cases/taql.rs"]
mod taql;
#[path = "api_cases/taql_simulation_arithmetic.rs"]
mod taql_simulation_arithmetic;
#[path = "api_cases/tiled_endian_reads.rs"]
mod tiled_endian_reads;
#[path = "api_cases/undefined_array_cells.rs"]
mod undefined_array_cells;
#[path = "api_cases/zero_dimensional_cells.rs"]
mod zero_dimensional_cells;

#[cfg(unix)]
mod lock_multiprocess;
