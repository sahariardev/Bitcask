use std::time::{SystemTime, UNIX_EPOCH};
pub mod key;

use crate::util;
use key::Serializable;
use std::mem;

const TOMBSTONE_MARKER_SIZE: usize = mem::size_of::<u8>();
const RESERVED_LENGTH_FOR_KEY_SIZE: usize = mem::size_of::<u32>();
const RESERVED_LENGTH_FOR_VALUE_SIZE: usize = mem::size_of::<u32>();
const RESERVED_TIMESTAMP_SIZE: usize = mem::size_of::<u32>();
struct ValueReference {
    value: Vec<u8>,
    tombstone: u8,
}

pub struct Entry<T: Serializable> {
    key: T,
    value: ValueReference,
    timestamp: u32,
}

impl<T: Serializable> Entry<T> {
    pub fn new(key: T, value: Vec<u8>) -> Entry<T> {
        Entry {
            key,
            value: ValueReference {
                value,
                tombstone: 0,
            },
            timestamp: 0,
        }
    }

    pub fn new_preserving_timestamp(key: T, value: Vec<u8>, timestamp: u32) -> Entry<T> {
        Entry {
            key,
            value: ValueReference {
                value,
                tombstone: 0,
            },
            timestamp,
        }
    }

    pub fn new_deleted_entry(key: T) -> Entry<T> {
        Entry {
            key,
            value: ValueReference {
                value: Vec::new(),
                tombstone: 1,
            },
            timestamp: 0,
        }
    }

    pub fn encode(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let serialized_key = self.key.serialize()?;
        let key_size = serialized_key.len();
        let value_size = self.value.value.len() + TOMBSTONE_MARKER_SIZE;

        let mut encoded = Vec::with_capacity(
            RESERVED_TIMESTAMP_SIZE
                + RESERVED_LENGTH_FOR_KEY_SIZE
                + RESERVED_LENGTH_FOR_VALUE_SIZE
                + key_size
                + value_size,
        );

        let mut timestamp = self.timestamp;

        if self.timestamp == 0 {
            timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32;
        }

        encoded.extend_from_slice(&timestamp.to_le_bytes());
        encoded.extend_from_slice(&(key_size as u32).to_le_bytes());
        encoded.extend_from_slice(&(value_size as u32).to_le_bytes());
        encoded.extend_from_slice(&serialized_key);

        encoded.extend_from_slice(&self.value.value);
        encoded.push(self.value.tombstone);

        Ok(encoded)
    }

    pub fn decode(content: Vec<u8>, offset: u32) -> Result<Entry<T>, std::io::Error> {
        let mut updated_offset = offset;
        let timestamp = util::get_int_from_le_bytes(&content, updated_offset)?;
        updated_offset += RESERVED_TIMESTAMP_SIZE as u32;
        let key_size = util::get_int_from_le_bytes(&content, updated_offset)?;
        updated_offset += RESERVED_LENGTH_FOR_KEY_SIZE as u32;
        let value_size = util::get_int_from_le_bytes(&content, updated_offset)?;
        updated_offset += RESERVED_LENGTH_FOR_VALUE_SIZE as u32;

        let key = content[updated_offset as usize..(updated_offset + key_size) as usize].to_vec();

        updated_offset += key_size;

        let value = content[updated_offset as usize
            ..(updated_offset + value_size) as usize - TOMBSTONE_MARKER_SIZE]
            .to_vec();

        updated_offset += value_size - TOMBSTONE_MARKER_SIZE as u32;

        let tombstone =
            &content[updated_offset as usize..updated_offset as usize + TOMBSTONE_MARKER_SIZE];

        let value_reference = ValueReference {
            value,
            tombstone: tombstone[0],
        };

        Ok(Entry {
            key: T::deserialize(key)?,
            value: value_reference,
            timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::entry::key::Serializable;
    use crate::entry::Entry;
    use std::io::Error;
    use std::time::{SystemTime, UNIX_EPOCH};

    impl Serializable for String {
        fn serialize(&self) -> Result<Vec<u8>, Error> {
            Ok(self.as_bytes().to_vec())
        }

        fn deserialize(bytes: Vec<u8>) -> Result<String, Error> {
            Ok(String::from_utf8(bytes).unwrap())
        }
    }

    #[test]
    fn test_encode_decode_roundtrip_standard() {
        let key = "my-key".to_string();
        let value = vec![1, 2, 3];

        let mut entry = Entry::new(key.clone(), value.clone());
        let encoded = entry.encode().unwrap();
        let decoded_entry = Entry::<String>::decode(encoded, 0).unwrap();

        assert_eq!(decoded_entry.key, key);
        assert_eq!(decoded_entry.value.value, value);
        assert_eq!(decoded_entry.value.tombstone, entry.value.tombstone);
    }

    #[test]
    fn test_encode_decode_roundtrip_standard_preserving_timestamp() {
        let key = "my-key".to_string();
        let value = vec![1, 2, 3];
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        let mut entry = Entry::new_preserving_timestamp(key.clone(), value.clone(), timestamp);
        let encoded = entry.encode().unwrap();
        let decoded_entry = Entry::<String>::decode(encoded, 0).unwrap();

        assert_eq!(decoded_entry.key, key);
        assert_eq!(decoded_entry.value.value, value);
        assert_eq!(decoded_entry.value.tombstone, entry.value.tombstone);
        assert_eq!(decoded_entry.timestamp, entry.timestamp);
    }

    #[test]
    fn test_encode_decode_roundtrip_deleted() {
        let key = "deleted-key".to_string();

        let mut entry = Entry::new_deleted_entry(key.clone());
        let encoded = entry.encode().unwrap();
        let decoded_entry = Entry::<String>::decode(encoded, 0).unwrap();

        assert_eq!(decoded_entry.key, key);
        assert_eq!(decoded_entry.value.tombstone, entry.value.tombstone);
    }

    #[test]
    fn test_decode_insufficient_data() {
        let short_data = vec![0, 1, 2, 3];
        let result = Entry::<String>::decode(short_data, 0);
        assert!(result.is_err());
    }
}
