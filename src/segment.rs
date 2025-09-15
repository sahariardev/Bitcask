use crate::entry::key::Serializable;
use crate::entry::Entry;
use crate::store::Store;

const SEGMENT_FILE_PREFIX: &str = "segment";
const SEGMENT_FILE_SUFFIX: &str = ".data";
pub struct Segment {
    pub file_id: u64,
    pub offset: i64,
    pub store: Store,
}

pub struct StoredEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub deleted: bool,
    pub timestamp: i64,
}
pub struct AppendEntryResponse {
    pub file_id: u64,
    pub offset: i64,
    pub entry_length: u32,
}

impl Segment {
    fn append<T: Serializable>(
        mut self,
        mut entry: Entry<T>,
    ) -> Result<AppendEntryResponse, std::io::Error> {
        let encoded = entry.encode()?;
        let offset = self.store.append(encoded.as_slice())?;

        Ok(AppendEntryResponse {
            file_id: self.file_id,
            offset: offset,
            entry_length: encoded.len() as u32,
        })
    }

    fn read(mut self, offset: u64, size: usize) -> Result<StoredEntry, std::io::Error> {
        let bytes = self.store.read(offset, size)?;
        
    }
}
