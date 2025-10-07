use crate::entry::key::Serializable;
use crate::entry::Entry;
use crate::store::Store;
use std::fs::File;
use std::path::PathBuf;

const SEGMENT_FILE_PREFIX: &str = "segment";
const SEGMENT_FILE_SUFFIX: &str = "data";
pub struct Segment {
    pub file_id: u64,
    pub file_path: String,
    pub store: Store,
}

pub struct AppendEntryResponse {
    pub file_id: u64,
    pub offset: i64,
    pub entry_length: u32,
}

impl Segment {
    pub fn new_segment(file_id: u64, directory: &str) -> Result<Segment, std::io::Error> {
        let file_name = format!(
            "{}_{}.{}",
            file_id, SEGMENT_FILE_PREFIX, SEGMENT_FILE_SUFFIX
        );
        let file_path = PathBuf::from(directory).join(file_name);
        let _ = File::create(&file_path);

        let store = Store::new(file_path.to_str().unwrap())?;

        Ok(Segment {
            file_id,
            file_path: file_path.to_str().unwrap().to_string(),
            store,
        })
    }
    pub fn append<T: Serializable>(
        &mut self,
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

    pub fn read<T: Serializable>(
        &mut self,
        offset: u64,
        size: usize,
    ) -> Result<Entry<T>, std::io::Error> {
        let bytes = self.store.read(offset, size)?;
        Entry::decode(bytes, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::tempdir;

    #[test]
    fn test_new_segment() {
        let dir = tempdir().unwrap();
        let dir_path_str = dir.path().to_str().unwrap();

        let segment = Segment::new_segment(1, ".").unwrap();

        assert_eq!(segment.file_id, 1);
        assert!(segment.file_path.contains("1_segment.data"));
    }

    #[test]
    fn test_append_and_read_single_entry() {
        let dir = tempdir().unwrap();
        let mut segment = Segment::new_segment(10, dir.path().to_str().unwrap()).unwrap();

        let entry = Entry::new("hello".to_string(), vec![1, 2, 3]);
        let response = segment.append(entry).unwrap();

        assert_eq!(response.file_id, 10);
        assert_eq!(response.offset, 0);

        // Read it back
        let read_entry: Entry<String> = segment
            .read(response.offset as u64, response.entry_length as usize)
            .unwrap();

        assert_eq!(read_entry.key, "hello");
        assert_eq!(read_entry.value.value, vec![1, 2, 3]);
    }

    // #[test]
    // fn test_append_and_read_multiple_entries() {
    //     let dir = tempdir().unwrap();
    //     let mut segment = Segment::new_segment(20, dir.path().to_str().unwrap()).unwrap();
    //
    //     // First entry
    //     let entry1 = Entry {
    //         key: "first_key".to_string(),
    //         value: TestPayload("first_value".to_string()),
    //     };
    //     let response1 = segment.append(entry1).unwrap();
    //
    //     assert_eq!(response1.offset, 0);
    //
    //     // Second entry
    //     let entry2 = Entry {
    //         key: "second_key".to_string(),
    //         value: TestPayload("a slightly longer second value".to_string()),
    //     };
    //     let response2 = segment.append(entry2).unwrap();
    //
    //     // The second offset should be exactly after the first entry
    //     assert_eq!(response2.offset, response1.entry_length as u64);
    //
    //     // Read back the first entry
    //     let read_entry1: Entry<TestPayload> = segment
    //         .read(response1.offset, response1.entry_length as usize)
    //         .unwrap();
    //     assert_eq!(read_entry1.key, "first_key");
    //     assert_eq!(read_entry1.value, TestPayload("first_value".to_string()));
    //
    //     // Read back the second entry
    //     let read_entry2: Entry<TestPayload> = segment
    //         .read(response2.offset, response2.entry_length as usize)
    //         .unwrap();
    //     assert_eq!(read_entry2.key, "second_key");
    //     assert_eq!(
    //         read_entry2.value,
    //         TestPayload("a slightly longer second value".to_string())
    //     );
    // }
}
