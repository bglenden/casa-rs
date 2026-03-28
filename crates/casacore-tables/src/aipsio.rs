// SPDX-License-Identifier: LGPL-3.0-or-later
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

#[cfg(test)]
mod tests {
    use super::{AipsIoConfig, decode_value, encode_value};
    use casacore_aipsio::{ByteOrder, ScalarValue, Value};

    #[test]
    fn default_config_uses_big_endian() {
        assert_eq!(AipsIoConfig::default().byte_order, ByteOrder::BigEndian);
    }

    #[test]
    fn encode_decode_round_trip_respects_configured_byte_order() {
        let value = Value::Scalar(ScalarValue::Int32(42));

        for byte_order in [ByteOrder::BigEndian, ByteOrder::LittleEndian] {
            let config = AipsIoConfig { byte_order };
            let encoded = encode_value(&value, config).expect("value should encode");
            let decoded = decode_value(
                &encoded,
                value
                    .type_tag()
                    .expect("scalar values always have a type tag"),
                config,
            )
            .expect("encoded value should decode");
            assert_eq!(decoded, value);
        }
    }
}
