// SPDX-License-Identifier: LGPL-3.0-or-later
//! Decode SSM strings with one reusable payload bucket per column read.

use super::bucket_reader::LastBucket;
use super::{DataRead, SsmHeader, StorageError, Value, read_i32_be, string_arrays};

const STRING_BUCKET_HEADER: usize = 16;

pub(super) fn read_ssm_string(
    file: &mut impl DataRead,
    header: &SsmHeader,
    buckets: &mut LastBucket,
    bucket_nr: i32,
    offset: i32,
    length: i32,
) -> Result<String, StorageError> {
    let bytes = read_ssm_string_bytes(file, header, buckets, bucket_nr, offset, length)?;
    String::from_utf8(bytes)
        .map_err(|e| StorageError::FormatMismatch(format!("SSM string is not valid UTF-8: {e}")))
}

pub(super) fn read_fixed_ssm_string(bytes: &[u8]) -> Result<String, StorageError> {
    let len = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());
    String::from_utf8(bytes[..len].to_vec()).map_err(|e| {
        StorageError::FormatMismatch(format!("SSM fixed string is not valid UTF-8: {e}"))
    })
}

/// Read `length` bytes from the SSM string bucket chain starting at
/// `offset` in the data area of bucket `bucket_nr`: a string, or a string
/// array's elements. Data longer than a bucket's rest continues at the
/// start of the next bucket its header links (casacore's
/// `SSMStringHandler::getData`).
pub(super) fn read_ssm_string_bytes(
    file: &mut impl DataRead,
    header: &SsmHeader,
    buckets: &mut LastBucket,
    bucket_nr: i32,
    offset: i32,
    length: i32,
) -> Result<Vec<u8>, StorageError> {
    if length == 0 {
        return Ok(Vec::new());
    }
    if bucket_nr < 0 || offset < 0 || length < 0 {
        return Err(StorageError::FormatMismatch(format!(
            "invalid SSM string locator bucket={} offset={} length={}",
            bucket_nr, offset, length
        )));
    }

    let mut result = Vec::with_capacity(length as usize);
    let mut remaining = length as usize;
    let mut cur_bucket = bucket_nr;
    let mut cur_offset = offset as usize;

    while remaining > 0 {
        let bucket = buckets.read(file, header, cur_bucket as u32)?;
        let data_start = STRING_BUCKET_HEADER + cur_offset;
        let Some(available) = bucket.len().checked_sub(data_start) else {
            return Err(StorageError::FormatMismatch(format!(
                "SSM string offset {cur_offset} is beyond bucket {cur_bucket}"
            )));
        };
        let chunk = remaining.min(available);
        result.extend_from_slice(&bucket[data_start..data_start + chunk]);
        remaining -= chunk;

        if remaining > 0 {
            // String bucket metadata (nextBucket) is always canonical (BE)
            // per C++ CanonicalConversion in SSMStringHandler.
            let next_bucket = read_i32_be(&bucket[12..16]);
            if next_bucket < 0 {
                return Err(StorageError::FormatMismatch(
                    "SSM string chain ended prematurely".to_string(),
                ));
            }
            cur_bucket = next_bucket;
            cur_offset = 0;
        }
    }

    Ok(result)
}

pub(super) fn read_ssm_string_array_value(
    file: &mut impl DataRead,
    header: &SsmHeader,
    buckets: &mut LastBucket,
    bucket_nr: i32,
    offset: i32,
    length: i32,
    fixed_shape: Option<&[i32]>,
) -> Result<Option<Value>, StorageError> {
    if length == 0 {
        return Ok(None);
    }
    let block = read_ssm_string_bytes(file, header, buckets, bucket_nr, offset, length)?;
    string_arrays::decode(&block, fixed_shape).map(Some)
}
