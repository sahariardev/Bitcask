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

#[cfg(test)]
mod tests {
    use crate::kv_store::KVStore;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_new_kv_store() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        let kv_store = KVStore::<String>::new(dir_path.to_string(), 1024);
        assert!(kv_store.is_ok());
    }

    #[test]
    fn test_put_single_entry() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        let mut kv_store = KVStore::<String>::new(dir_path.to_string(), 1024).unwrap();
        let key = String::from("key1");
        let value = vec![1, 2, 3];

        let result = kv_store.put(key.clone(), value.clone());
        assert!(result.is_ok());

        let retrived_value = kv_store.get(key);
        assert_eq!(retrived_value, Some(value));
    }

    #[test]
    fn test_put_multiple_entry() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        let mut kv_store = KVStore::<String>::new(dir_path.to_string(), 1024).unwrap();
        let key = String::from("key1");
        let value = vec![1, 2, 3];

        let key2 = String::from("key2");
        let value2 = vec![1, 2, 3, 5];

        let result = kv_store.put(key.clone(), value.clone());
        assert!(result.is_ok());

        let result = kv_store.put(key2.clone(), value2.clone());
        assert!(result.is_ok());

        let retrived_value = kv_store.get(key);
        assert_eq!(retrived_value, Some(value));

        let retrived_value = kv_store.get(key2);
        assert_eq!(retrived_value, Some(value2));
    }

    #[test]
    fn test_new_kv_non_existence() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        let mut kv_store = KVStore::<String>::new(dir_path.to_string(), 1024).unwrap();

        let retrived_value = kv_store.get("non-existence_key".to_string());
        assert_eq!(retrived_value, None);
    }

    #[test]
    fn test_put_override_entry() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        let mut kv_store = KVStore::<String>::new(dir_path.to_string(), 1024).unwrap();
        let key = String::from("key1");
        let value = vec![1, 2, 3];

        let value2 = vec![1, 2, 3, 5];

        let result = kv_store.put(key.clone(), value.clone());
        assert!(result.is_ok());

        let result = kv_store.put(key.clone(), value2.clone());
        assert!(result.is_ok());

        let retrived_value = kv_store.get(key);
        assert_eq!(retrived_value, Some(value2));
    }

    #[test]
    fn test_delete_key() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();
        let mut kv_store = KVStore::<String>::new(dir_path, 1024).unwrap();

        let key = "key-to-delete".to_string();
        let value = vec![10, 20, 30];

        kv_store.put(key.clone(), value.clone()).unwrap();
        let retrieved_value = kv_store.get(key.clone());
        assert_eq!(retrieved_value, Some(value));

        let delete_result = kv_store.delete(key.clone());
        assert!(delete_result.is_ok());

        let retrieved_value_after_delete = kv_store.get(key);
        assert_eq!(retrieved_value_after_delete, None);
    }

    #[test]
    fn test_segment_rollover() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();

        let max_segment_size = 30;
        let mut kv_store = KVStore::<String>::new(dir_path, max_segment_size).unwrap();

        let key1 = "key1".to_string();
        let value1 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 1];
        let key2 = "key2".to_string();
        let value2 = vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20];

        kv_store.put(key1.clone(), value1.clone()).unwrap();

        {
            let segments = kv_store.segments.read().unwrap();
            assert_eq!(segments.inactive_segments.len(), 0);
        }

        thread::sleep(Duration::from_secs(2));

        kv_store.put(key2.clone(), value2.clone()).unwrap();

        {
            let segments = kv_store.segments.read().unwrap();
            assert_eq!(segments.inactive_segments.len(), 1);
        }

        let retrieved_value1 = kv_store.get(key1);

        assert_eq!(retrieved_value1, Some(value1));

        let retrieved_value2 = kv_store.get(key2);
        assert_eq!(retrieved_value2, Some(value2));
    }
}
