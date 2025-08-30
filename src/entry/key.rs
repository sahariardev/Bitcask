pub trait Serializable {
    fn serialize(&self) -> Result<Vec<u8>, std::io::Error>;
    fn deserialize(bytes: Vec<u8>) -> Result<Self, std::io::Error>
    where
        Self: Sized;
}
