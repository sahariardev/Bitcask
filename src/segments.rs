use crate::entry;
use crate::entry::Entry;
use crate::segment::{AppendEntryResponse, Segment, SEGMENT_FILE_PREFIX, SEGMENT_FILE_SUFFIX};
use crate::time_based_id_generator::TimeBasedIdGenerator;
use std::collections::HashMap;
use std::io::Error;
use std::io::ErrorKind::InvalidData;
use std::{fs, mem};

pub struct Segments {
    pub active_segment: Segment,
    pub inactive_segments: HashMap<u64, Segment>,
    pub directory: String,
    pub max_segment_size: u32,
    pub id_generator: TimeBasedIdGenerator,
}

impl Segments {
    pub fn new(directory: String, max_segment_size: u32) -> Result<Segments, Error> {
        let id_generator = TimeBasedIdGenerator::new();
        let segment = Segment::new_segment(id_generator.next(), directory.as_str())?;

        let mut segments = Segments {
            active_segment: segment,
            id_generator,
            directory,
            max_segment_size,
            inactive_segments: HashMap::new(),
        };

        segments.reload().expect("Segments reload failed");

        Ok(segments)
    }
    pub fn append<T: entry::key::Serializable>(
        &mut self,
        key: T,
        value: Vec<u8>,
    ) -> Result<AppendEntryResponse, std::io::Error> {
        self.maybe_roll_over_active_segment()?;

        self.active_segment.append(Entry::new(key, value))
    }

    pub fn append_delete<T: entry::key::Serializable>(
        &mut self,
        key: T,
    ) -> Result<AppendEntryResponse, std::io::Error> {
        self.maybe_roll_over_active_segment()?;

        self.active_segment.append(Entry::new_deleted_entry(key))
    }

    pub fn read<T: entry::key::Serializable>(
        &mut self,
        file_id: u64,
        size: usize,
        offset: u64,
    ) -> Result<Entry<T>, std::io::Error> {
        if self.active_segment.file_id == file_id {
            return self.active_segment.read(offset, size);
        }

        if !self.inactive_segments.contains_key(&file_id) {
            return Err(Error::new(InvalidData, "file_id not found"));
        }

        let segment = self.inactive_segments.get_mut(&file_id).unwrap();
        segment.read(offset, size)
    }

    pub fn reload(&mut self) -> Result<(), Error> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_str().unwrap();

            if path.is_file()
                && file_name
                    .ends_with(&(SEGMENT_FILE_PREFIX.to_owned() + "." + SEGMENT_FILE_SUFFIX))
            {
                let file_id = String::from(file_name)
                    .split("_")
                    .next()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();

                if self.active_segment.file_id != file_id {
                    let segment =
                        Segment::reload_inactive_segment(file_id, self.directory.as_str())?;
                    self.inactive_segments.insert(file_id, segment);
                }
            }
        }

        Ok(())
    }

    fn maybe_roll_over_segment(&self, segment: &Segment) -> Result<Option<Segment>, Error> {
        if segment.store.current_write_off_set >= self.max_segment_size as i64 {
            let new_segment =
                Segment::new_segment(self.id_generator.next(), self.directory.as_str())?;

            return Ok(Some(new_segment));
        }

        Ok(None)
    }

    fn maybe_roll_over_active_segment(&mut self) -> Result<(), std::io::Error> {
        let new_segment = self.maybe_roll_over_segment(&self.active_segment)?;

        if let Some(segment) = new_segment {
            let old_segment = mem::replace(&mut self.active_segment, segment);
            println!("Rolled over  active segment");
            self.inactive_segments
                .insert(old_segment.file_id, old_segment);
        }

        Ok(())
    }
}
