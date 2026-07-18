// SPDX-License-Identifier: LGPL-3.0-or-later
//! Private raw declarations for the casacore C++ oracle shims.
#![allow(dead_code)]

unsafe extern "C" {
    pub(crate) fn cpp_table_write_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_mutation_removed_column(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_mutation_removed_rows(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_mutation_added_column(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_with_lock(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_with_lock(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ref_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ref_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_sorted_ref_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_sorted_ref_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_concat_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_concat_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_deep_copy(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_deep_copy(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_free_error(ptr: *mut std::ffi::c_char);
    pub(crate) fn cpp_table_write_tiled_column_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_tiled_column_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_tiled_shape_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_tiled_shape_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_tiled_cell_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_tiled_cell_stman(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_columns_index_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_forward_column_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_forward_column_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_scaled_array_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_scaled_array_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_columns_index_time_lookups"]
    pub(crate) fn ffi_columns_index_time_lookups(
        path: *const std::ffi::c_char,
        key_value: i32,
        nqueries: u64,
        out_elapsed_ns: *mut u64,
        out_match_count: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_table_write_ism_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ism_scalar_primitives(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ism_slowly_changing(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ism_slowly_changing(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ism_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ism_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_table_write_undefined_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_undefined_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_column_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_column_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_record_column(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_record_column(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_mixed_schema(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_mixed_schema(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_table_write_aipsio_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_table_write_table_info(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_table_info(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_log_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_log_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_table_write_aipsio_all_numeric_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_all_numeric_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_aipsio_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_aipsio_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_aipsio_3d_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_3d_fixed_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_all_numeric_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_all_numeric_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_complex_scalars(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_table_write_aipsio_complex_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_complex_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_complex_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_complex_variable_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    // --- Stream 1 interop additions ---

    pub(crate) fn cpp_table_write_aipsio_unsigned_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_unsigned_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_unsigned_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_unsigned_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_aipsio_string_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_string_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_string_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_string_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_aipsio_complex64_2d_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_complex64_2d_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_complex64_2d_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_complex64_2d_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_aipsio_empty_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_aipsio_empty_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ssm_empty_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ssm_empty_table(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_tiled_column_3d_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_tiled_column_3d_array(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ism_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ism_typed_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ism_complex_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ism_complex_arrays(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_write_ism_column_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_table_verify_ism_column_keywords(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_vararray_bench_write_read(
        path: *const std::ffi::c_char,
        nrows: u64,
        out_write_ns: *mut u64,
        out_read_ns: *mut u64,
        out_total_elems: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    #[link_name = "cpp_set_algebra_bench"]
    pub(crate) fn ffi_set_algebra_bench(
        path: *const std::ffi::c_char,
        nrows: u64,
        split_a: u64,
        split_b: u64,
        out_union_ns: *mut u64,
        out_intersection_ns: *mut u64,
        out_difference_ns: *mut u64,
        out_union_rows: *mut u64,
        out_intersection_rows: *mut u64,
        out_difference_rows: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    #[link_name = "cpp_copy_rows_bench"]
    pub(crate) fn ffi_copy_rows_bench(
        dir: *const std::ffi::c_char,
        nrows: u64,
        out_ns: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    #[link_name = "cpp_cell_slice_bench"]
    pub(crate) fn ffi_cell_slice_bench(
        path: *const std::ffi::c_char,
        nrows: u64,
        dim0: i64,
        dim1: i64,
        slice_start0: i64,
        slice_start1: i64,
        slice_end0: i64,
        slice_end1: i64,
        out_write_ns: *mut u64,
        out_slice_ns: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    #[link_name = "cpp_bulk_scalar_io_bench"]
    pub(crate) fn ffi_bulk_scalar_io_bench(
        path: *const std::ffi::c_char,
        nrows: u64,
        out_write_ns: *mut u64,
        out_read_ns: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    #[link_name = "cpp_deep_copy_bench"]
    pub(crate) fn ffi_deep_copy_bench(
        dir: *const std::ffi::c_char,
        nrows: u64,
        out_write_ns: *mut u64,
        out_copy_ns: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn casacore_cpp_aipsio_encode(
        primitive: u8,
        is_array: u8,
        byte_order: u8,
        payload_ptr: *const u8,
        payload_len: usize,
        offsets_ptr: *const u32,
        offsets_len: usize,
        out_wire_ptr: *mut *mut u8,
        out_wire_len: *mut usize,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn casacore_cpp_aipsio_decode(
        primitive: u8,
        is_array: u8,
        byte_order: u8,
        wire_ptr: *const u8,
        wire_len: usize,
        out_payload_ptr: *mut *mut u8,
        out_payload_len: *mut usize,
        out_offsets_ptr: *mut *mut u32,
        out_offsets_len: *mut usize,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn casacore_cpp_aipsio_free_bytes(ptr: *mut u8);
    pub(crate) fn casacore_cpp_aipsio_free_offsets(ptr: *mut u32);
    pub(crate) fn casacore_cpp_aipsio_free_error(ptr: *mut std::ffi::c_char);

    // ── TaQL interop shim ──
    pub(crate) fn cpp_taql_write_simple_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_taql_write_array_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_taql_write_varshape_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_taql_query(
        table_path: *const std::ffi::c_char,
        query: *const std::ffi::c_char,
        out_result: *mut *mut std::ffi::c_char,
        out_nrow: *mut u64,
        out_ncol: *mut u64,
        out_elapsed_ns: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_taql_free_result(ptr: *mut std::ffi::c_char);

    // PagedImage shim
    pub(crate) fn cpp_create_pagedimage_float(
        path: *const std::ffi::c_char,
        shape: *const i32,
        ndim: i32,
        data: *const f32,
        ndata: i64,
        units: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_create_pagedimage_float_tiled(
        path: *const std::ffi::c_char,
        shape: *const i32,
        tile: *const i32,
        ndim: i32,
        data: *const f32,
        ndata: i64,
        units: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_create_tempimage_float_materialized(
        path: *const std::ffi::c_char,
        shape: *const i32,
        ndim: i32,
        data: *const f32,
        ndata: i64,
        units: *const std::ffi::c_char,
        object_name: *const std::ffi::c_char,
        image_type: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_float(
        path: *const std::ffi::c_char,
        data_out: *mut f32,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_create_pagedimage_double(
        path: *const std::ffi::c_char,
        shape: *const i32,
        ndim: i32,
        data: *const f64,
        ndata: i64,
        units: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_double(
        path: *const std::ffi::c_char,
        data_out: *mut f64,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_create_pagedimage_complex32(
        path: *const std::ffi::c_char,
        shape: *const i32,
        ndim: i32,
        data: *const f32,
        ncomplex: i64,
        units: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_complex32(
        path: *const std::ffi::c_char,
        data_out: *mut f32,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_create_pagedimage_complex64(
        path: *const std::ffi::c_char,
        shape: *const i32,
        ndim: i32,
        data: *const f64,
        ncomplex: i64,
        units: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_complex64(
        path: *const std::ffi::c_char,
        data_out: *mut f64,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_shape(
        path: *const std::ffi::c_char,
        shape_out: *mut i32,
        max_ndim: i32,
        ndim_out: *mut i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_units(
        path: *const std::ffi::c_char,
        buf: *mut std::ffi::c_char,
        bufsize: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_coordinate_count(
        path: *const std::ffi::c_char,
        count_out: *mut i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_default_mask_name(
        path: *const std::ffi::c_char,
        buf: *mut std::ffi::c_char,
        bufsize: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_default_mask(
        path: *const std::ffi::c_char,
        data_out: *mut u8,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_write_polygon_region"]
    pub(crate) fn ffi_cpp_write_polygon_region(
        path: *const std::ffi::c_char,
        region_name: *const std::ffi::c_char,
        x: *const f64,
        y: *const f64,
        nvertices: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_write_union_region"]
    pub(crate) fn ffi_cpp_write_union_region(
        path: *const std::ffi::c_char,
        region_name: *const std::ffi::c_char,
        x1: *const f64,
        y1: *const f64,
        nvertices1: i32,
        x2: *const f64,
        y2: *const f64,
        nvertices2: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_write_box_region"]
    pub(crate) fn ffi_cpp_write_box_region(
        path: *const std::ffi::c_char,
        region_name: *const std::ffi::c_char,
        x0: f64,
        y0: f64,
        x1: f64,
        y1: f64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_read_region_class"]
    pub(crate) fn ffi_cpp_read_region_class(
        path: *const std::ffi::c_char,
        region_name: *const std::ffi::c_char,
        buf: *mut std::ffi::c_char,
        bufsize: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_read_region_names"]
    pub(crate) fn ffi_cpp_read_region_names(
        path: *const std::ffi::c_char,
        buf: *mut std::ffi::c_char,
        bufsize: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_write_unsupported_region"]
    pub(crate) fn ffi_cpp_write_unsupported_region(
        path: *const std::ffi::c_char,
        region_name: *const std::ffi::c_char,
        kind: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_read_region_statistics"]
    pub(crate) fn ffi_cpp_read_region_statistics(
        path: *const std::ffi::c_char,
        region_name: *const std::ffi::c_char,
        stats_out: *mut f64,
        nstats: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_write_default_mask"]
    pub(crate) fn ffi_cpp_write_default_mask(
        path: *const std::ffi::c_char,
        mask_name: *const std::ffi::c_char,
        data: *const u8,
        ndata: i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_imageinfo_object_name(
        path: *const std::ffi::c_char,
        buf: *mut std::ffi::c_char,
        bufsize: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_imageinfo_type(
        path: *const std::ffi::c_char,
        buf: *mut std::ffi::c_char,
        bufsize: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_read_pagedimage_slice(
        path: *const std::ffi::c_char,
        start: *const i32,
        length: *const i32,
        ndim: i32,
        data_out: *mut f32,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_eval_pagedimage_float_unary_expr(
        path: *const std::ffi::c_char,
        op: i32,
        data_out: *mut f32,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_eval_pagedimage_float_binary_expr(
        lhs_path: *const std::ffi::c_char,
        rhs_path: *const std::ffi::c_char,
        op: i32,
        data_out: *mut f32,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_eval_pagedimage_float_scalar_expr(
        path: *const std::ffi::c_char,
        scalar: f32,
        op: i32,
        data_out: *mut f32,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_eval_pagedimage_float_range_mask_expr(
        path: *const std::ffi::c_char,
        lower_cmp: i32,
        lower: f32,
        logical_op: i32,
        upper_cmp: i32,
        upper: f32,
        data_out: *mut u8,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_eval_pagedimage_float_closeout_expr_slice(
        path: *const std::ffi::c_char,
        start: *const i32,
        length: *const i32,
        ndim: i32,
        data_out: *mut f32,
        max_size: i64,
        nread_out: *mut i64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_eval_lel_expr_float(
        expr: *const std::ffi::c_char,
        data_out: *mut f32,
        max_size: i64,
        nread_out: *mut i64,
        shape_out: *mut i32,
        max_ndim: i32,
        ndim_out: *mut i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_profile_lel_scalar_expr_float(
        expr: *const std::ffi::c_char,
        passes: i32,
        timings_out: *mut f64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_eval_lel_expr_bool(
        expr: *const std::ffi::c_char,
        data_out: *mut u8,
        max_size: i64,
        nread_out: *mut i64,
        shape_out: *mut i32,
        max_ndim: i32,
        ndim_out: *mut i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_save_lel_expr(
        expr: *const std::ffi::c_char,
        save_path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    pub(crate) fn cpp_open_lel_expr_float(
        path: *const std::ffi::c_char,
        data_out: *mut f32,
        max_size: i64,
        nread_out: *mut i64,
        shape_out: *mut i32,
        max_ndim: i32,
        ndim_out: *mut i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_bench_plane_by_plane(
        path: *const std::ffi::c_char,
        shape: *const i32,
        tile: *const i32,
        ndim: i32,
        max_cache_mib: i32,
        timings_out: *mut f64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_bench_spectrum_by_spectrum(
        path: *const std::ffi::c_char,
        shape: *const i32,
        tile: *const i32,
        ndim: i32,
        max_cache_mib: i32,
        timings_out: *mut f64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    pub(crate) fn cpp_bench_plane_by_plane_complex(
        path: *const std::ffi::c_char,
        shape: *const i32,
        tile: *const i32,
        ndim: i32,
        max_cache_mib: i32,
        timings_out: *mut f64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
}
