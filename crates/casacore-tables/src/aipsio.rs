#![allow(dead_code)]

use casacore_aipsio::{AipsReader, AipsWriter, ByteOrder, TypeTag, Value};
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub(crate) struct AipsIoConfig {
    pub(crate) byte_order: ByteOrder,
}

impl Default for AipsIoConfig {
    fn default() -> Self {
        Self {
            byte_order: ByteOrder::BigEndian,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum AipsIoInternalError {
    #[error(transparent)]
    Codec(#[from] casacore_aipsio::AipsIoError),
}

pub(crate) fn encode_value(
    value: &Value,
    config: AipsIoConfig,
) -> Result<Vec<u8>, AipsIoInternalError> {
    let mut bytes = Vec::new();
    let mut writer = AipsWriter::with_byte_order(&mut bytes, config.byte_order);
    writer.write_value(value)?;
    Ok(bytes)
}

pub(crate) fn decode_value(
    bytes: &[u8],
    type_tag: TypeTag,
    config: AipsIoConfig,
) -> Result<Value, AipsIoInternalError> {
    let mut reader = AipsReader::with_byte_order(bytes, config.byte_order);
    Ok(reader.read_value(type_tag)?)
}
