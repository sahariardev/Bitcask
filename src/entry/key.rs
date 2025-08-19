pub trait Serializable {
    fn serialize(&self) -> Result<Vec<u8>, std::io::Error>;
}
