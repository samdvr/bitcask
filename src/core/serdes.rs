use std::convert::TryInto;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Debug, PartialEq, Clone)]
pub struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
    pub timestamp: Vec<u8>,
}

impl KeyValue<Key, Value> {
    pub fn new<K: Into<Key>, V: Into<Value>>(key: K, value: V) -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("error reading system time")
            .as_millis();

        let millis_bytes = millis.to_be_bytes();
        let mut timestamp = vec![0; 8];

        // Copy millis_bytes into the last bytes of the timestamp vector
        let start_idx = timestamp.len().saturating_sub(millis_bytes.len());
        timestamp[start_idx..].copy_from_slice(&millis_bytes[8..]);

        Self {
            key: key.into(),
            value: value.into(),
            timestamp,
        }
    }
}

pub trait Serdes<T> {
    type DeserializeErr;
    type SerializeErr;

    fn deserialize(input: &[u8]) -> Result<T, Self::DeserializeErr>;
    fn serialize(a: &T) -> Result<Vec<u8>, Self::SerializeErr>;
}

#[derive(Debug, Error)]
#[error("DeserializeError: {message}")]
pub struct DeserializeError {
    pub message: String,
}

#[derive(Debug, Error)]
#[error("SerializeError: {message}")]
pub struct SerializeError {
    pub message: String,
}

impl Serdes<KeyValue<Key, Value>> for KeyValue<Key, Value> {
    type DeserializeErr = DeserializeError;
    type SerializeErr = SerializeError;

    fn deserialize(input: &[u8]) -> Result<Self, DeserializeError> {
        let parsed_bytes = parse_input(input)?;
        let expected_crc_bytes = calculate_crc(&parsed_bytes.key, &parsed_bytes.value);
        if parsed_bytes.crc_bytes == expected_crc_bytes {
            let timestamp = parsed_bytes.timestamp_bytes;
            Ok(KeyValue {
                key: parsed_bytes.key,
                value: parsed_bytes.value,
                timestamp,
            })
        } else {
            Err(DeserializeError {
                message: String::from("Invalid CRC32 checksum"),
            })
        }
    }

    fn serialize(a: &Self) -> Result<Vec<u8>, SerializeError> {
        let mut buff = Vec::new();
        buff.extend(calculate_crc(&a.key, &a.value));
        buff.extend(&a.timestamp);
        buff.extend(&(a.key.len() as u16).to_be_bytes());
        buff.extend(&(a.value.len() as u16).to_be_bytes());
        buff.extend(a.key.iter());
        buff.extend(a.value.iter());
        Ok(buff)
    }
}

#[derive(Debug, PartialEq, Clone)]
struct ParsedBytes {
    crc_bytes: Vec<u8>,
    timestamp_bytes: Vec<u8>,
    key_length: usize,
    value_length: usize,
    key: Key,
    value: Value,
}

fn parse_input(input: &[u8]) -> Result<ParsedBytes, DeserializeError> {
    if input.len() < 16 {
        return Err(DeserializeError {
            message: String::from("Input too short"),
        });
    }

    let crc_bytes = input[0..4].to_vec();
    let timestamp_bytes = input[4..12].to_vec();

    let key_length = input[12..14]
        .try_into()
        .map_err(|_| DeserializeError {
            message: String::from("Failed to parse key length"),
        })
        .map(|bytes: [u8; 2]| u16::from_be_bytes(bytes) as usize)?;

    let value_length = input[14..16]
        .try_into()
        .map_err(|_| DeserializeError {
            message: String::from("Failed to parse value length"),
        })
        .map(|bytes: [u8; 2]| u16::from_be_bytes(bytes) as usize)?;

    if input.len() < 16 + key_length + value_length {
        return Err(DeserializeError {
            message: String::from("Input too short"),
        });
    }

    let key = input[16..16 + key_length].to_vec();
    let value = input[16 + key_length..16 + key_length + value_length].to_vec();

    Ok(ParsedBytes {
        crc_bytes,
        timestamp_bytes,
        key_length,
        value_length,
        key,
        value,
    })
}

fn calculate_crc(key: &[u8], value: &[u8]) -> [u8; 4] {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key);
    hasher.update(value);
    let crc_value = hasher.finalize();
    crc_value.to_be_bytes()
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_input_success() {
        let input = vec![
            0x0D, 0x4A, 0x11, 0x85, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
            0x00, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x77, 0x6F, 0x72, 0x6C, 0x64,
        ];
        let expected = ParsedBytes {
            crc_bytes: vec![0x0D, 0x4A, 0x11, 0x85],
            timestamp_bytes: vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            key_length: 5,
            value_length: 5,
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        };
        assert_eq!(parse_input(&input).unwrap(), expected);
    }

    #[test]
    fn test_parse_input_fail_input_too_short() {
        let input = vec![0x00];
        let result = parse_input(&input);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().message, "Input too short".to_string());
    }

    #[test]
    fn test_serialize_deserialize() {
        let kv = KeyValue {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            timestamp: vec![0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68],
        };

        let serialized = KeyValue::serialize(&kv).unwrap();
        let deserialized = KeyValue::deserialize(&serialized).unwrap();

        assert_eq!(deserialized, kv);
    }

    #[test]
    fn test_key_value_new() {
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        let kv = KeyValue::new(key.clone(), value.clone());

        assert_eq!(kv.key, key);
        assert_eq!(kv.value, value);
        assert!(!kv.timestamp.is_empty());

        let serialized = KeyValue::serialize(&kv).unwrap();
        let deserialized = KeyValue::deserialize(&serialized).unwrap();

        assert_eq!(deserialized, kv);
    }
}
