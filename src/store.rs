use std::fs::{File, OpenOptions, remove_file};
use std::io::ErrorKind::InvalidData;
use std::io::{Error, Read, Seek, SeekFrom};
use std::io::{ErrorKind, Write};

pub struct Store {
    pub writer: Option<File>,
    pub reader: File,
    pub current_write_off_set: i64,
    pub path: String,
}

impl Store {
    pub fn new(filename: &str) -> Result<Self, std::io::Error> {
        let writer = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(filename)?;

        let reader = OpenOptions::new().read(true).open(filename)?;

        let store = Store {
            writer: Some(writer),
            reader,
            current_write_off_set: 0,
            path: filename.to_string(),
        };

        Ok(store)
    }

    pub fn reload(filename: &str) -> Result<Self, std::io::Error> {
        let reader = OpenOptions::new().read(true).open(filename)?;

        let store = Store {
            writer: None,
            reader,
            current_write_off_set: 0,
            path: filename.to_string(),
        };

        Ok(store)
    }
    pub fn append(&mut self, buf: &[u8]) -> Result<i64, Error> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "writing is not allowed"))?;

        let bytes_written = writer.write(buf)?;
        let current_write_off_set = self.current_write_off_set;

        if bytes_written < buf.len() {
            return Err(Error::new(
                InvalidData,
                "written bytes are less than expected",
            ));
        }

        self.current_write_off_set += bytes_written as i64;
        Ok(current_write_off_set)
    }

    pub fn read(&mut self, offset: u64, size: usize) -> Result<Vec<u8>, Error> {
        self.reader.seek(SeekFrom::Start(offset))?;

        let mut buf = vec![0; size];

        let bytes_read = self.reader.read(&mut buf)?;

        buf.truncate(bytes_read);

        Ok(buf)
    }

    pub fn read_full(&mut self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![];

        self.reader.read_to_end(&mut buf)?;

        Ok(buf)
    }
    pub fn sync(&mut self) -> Result<(), Error> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "writing is not allowed"))?;
        writer.flush()?;
        Ok(())
    }

    pub fn remove(&mut self) -> Result<(), Error> {
        remove_file(&self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::store::Store;

    #[test]
    fn test_store_workflow() {
        let filename = "test_store.dat";

        let mut store = Store::new(filename).expect("Failed to create store");

        let data1 = b"hello world";
        let data2 = b"this is ano";

        let size1 = store.append(data1).expect("Failed to append");
        assert_eq!(size1, data1.len() as i64);

        let size2 = store.append(data2).expect("Failed to append");
        assert_eq!(size2, data2.len() as i64);

        let read_data1 = store.read(0, data1.len()).expect("Failed to read");
        assert_eq!(read_data1, data1);

        let read_data2 = store
            .read(data1.len() as u64, data2.len())
            .expect("Failed to read");
        assert_eq!(read_data2, data2);

        let read_full_data = store.read_full().expect("Failed to read");
        assert_eq!(read_full_data, b"hello worldthis is ano");

        store.remove().expect("Failed to remove");
    }
}
