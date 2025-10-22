use crate::entry;
use crate::segment::AppendEntryResponse;
use std::collections::HashMap;

pub struct KeyDirectory<T: entry::key::Serializable> {
    entry_by_key: HashMap<T, AppendEntryResponse>,
}

impl<T: entry::key::Serializable> KeyDirectory<T> {
    pub fn new() -> KeyDirectory<T> {
        KeyDirectory {
            entry_by_key: HashMap::new(),
        }
    }

    pub fn put(&mut self, key: T, value: AppendEntryResponse) {
        self.entry_by_key.insert(key, value);
    }

    pub fn get(&self, key: T) -> Option<&AppendEntryResponse> {
        self.entry_by_key.get(&key)
    }

    pub fn remove(&mut self, key: T) -> Option<AppendEntryResponse> {
        self.entry_by_key.remove(&key)
    }
}
