use crate::entry;
use crate::key_directory::KeyDirectory;
use crate::segments::Segments;
use std::sync::{Arc, RwLock};

pub struct KVStore<T: entry::key::Serializable> {
    segments: Arc<RwLock<Segments>>,
    directory: KeyDirectory<T>,
}

impl<T: entry::key::Serializable> KVStore<T> {
    pub fn new(directory: String, max_segment_size: u32) -> Result<KVStore<T>, std::io::Error> {
        let segments = Segments::new(directory, max_segment_size)?;
        let directory = KeyDirectory::new();

        Ok(KVStore {
            segments: Arc::new(RwLock::new(segments)),
            directory,
        })
    }

    pub fn put(&mut self, key: T, value: Vec<u8>) -> Result<(), std::io::Error> {
        let mut segments = self.segments.write().unwrap();
        let result = segments.append(key.clone(), value)?;
        self.directory.put(key, result);
        Ok(())
    }

    pub fn get(&self, key: T) -> Option<Vec<u8>> {
        let append_entry_response = self.directory.get(key)?;
        let mut segments = self.segments.write().unwrap();

        let result = segments
            .read::<T>(
                append_entry_response.file_id,
                append_entry_response.entry_length as usize,
                append_entry_response.offset as u64,
            )
            .unwrap();

        Some(result.value.value)
    }

    pub fn delete(&mut self, key: T) -> Result<(), std::io::Error> {
        let mut segments = self.segments.write().unwrap();
        let _ = segments.append_delete(key.clone());
        self.directory.remove(key);

        Ok(())
    }
}
