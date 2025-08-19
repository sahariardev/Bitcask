use std::time::{SystemTime, UNIX_EPOCH};
mod key;

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

struct Entry<T: Serializable> {
    key: T,
    value: ValueReference,
    timestamp: u32,
}

impl<T: Serializable> Entry<T> {
    fn new(key: T, value: Vec<u8>) -> Entry<T> {
        Entry {
            key,
            value: ValueReference {
                value,
                tombstone: 0,
            },
            timestamp: 0,
        }
    }

    fn new_preserving_timestamp(key: T, value: Vec<u8>, timestamp: u32) -> Entry<T> {
        Entry {
            key,
            value: ValueReference {
                value,
                tombstone: 0,
            },
            timestamp,
        }
    }

    fn new_deleted_entry(key: T) -> Entry<T> {
        Entry {
            key,
            value: ValueReference {
                value: Vec::new(),
                tombstone: 1,
            },
            timestamp: 0,
        }
    }

    fn endcode(&mut self) -> Result<Vec<u8>, std::io::Error> {
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
        encoded.extend_from_slice(&key_size.to_le_bytes());
        encoded.extend_from_slice(&value_size.to_le_bytes());
        encoded.extend_from_slice(&serialized_key);

        encoded.extend_from_slice(&self.value.value);
        encoded.push(self.value.tombstone);

        Ok(encoded)
    }
}
