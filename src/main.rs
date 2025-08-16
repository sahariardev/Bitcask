use std::fs::{File, OpenOptions};
use std::io::ErrorKind::InvalidData;
use std::io::Write;
use std::io::{Error, Read, Seek, SeekFrom};
use std::path::Path;

pub struct Store {
    pub writer: File,
    pub reader: File,
    pub current_write_off_set: i64,
}

impl Store {
    pub fn new<P: AsRef<Path>>(filename: &P) -> Result<Self, std::io::Error> {
        let writer = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(filename)?;

        let reader = OpenOptions::new().read(true).open(filename)?;

        let store = Store {
            writer,
            reader,
            current_write_off_set: 0,
        };

        Ok(store)
    }
    pub fn append(&mut self, buf: &[u8]) -> Result<i64, Error> {
        let bytes_written = self.writer.write(buf)?;

        if bytes_written < buf.len() {
            return Err(Error::new(
                InvalidData,
                "written bytes are less than expected",
            ));
        }

        self.current_write_off_set += bytes_written as i64;
        Ok(bytes_written as i64)
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
}
fn main() {
    println!("Hello, world!");
    let mut data_file = File::open("data.txt").unwrap();
}
