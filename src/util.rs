use std::io::Error;
use std::io::ErrorKind::InvalidData;

pub fn get_int_from_le_bytes(content: &Vec<u8>, offset: u32) -> Result<u32, Error> {
    let start = offset as usize;
    let end = start + size_of::<u32>();

    if end > content.len() {
        return Err(Error::new(
            InvalidData,
            "found bytes are less than expected",
        ));
    }

    let expected_data_bytes_slice = &content[start..end];

    match expected_data_bytes_slice.try_into() {
        Ok(expected_data_bytes_array) => Ok(u32::from_le_bytes(expected_data_bytes_array)),
        Err(_) => Err(Error::new(InvalidData, "cannot convert bytes to u32")),
    }
}

#[cfg(test)]
mod tests {
    use crate::util;
    use std::io::ErrorKind::InvalidData;
    #[test]
    fn test_get_int_from_le_bytes_should_success_when_offset_zero() {
        let number: u32 = 123456;
        let bytes = number.to_le_bytes().to_vec();

        assert_eq!(util::get_int_from_le_bytes(&bytes, 0).unwrap(), number)
    }

    #[test]
    fn test_get_int_from_le_bytes_should_success_when_offset_has_value() {
        let number: u32 = 123456;
        let mut content = vec![0, 0];
        content.extend_from_slice(&number.to_le_bytes());
        content.extend_from_slice(&[99, 99]);

        assert_eq!(util::get_int_from_le_bytes(&content, 2).unwrap(), number)
    }

    #[test]
    fn test_get_int_from_le_bytes_should_fail_when_empty() {
        let content = vec![];
        let result = util::get_int_from_le_bytes(&content, 0);

        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e.kind(), InvalidData);
            assert_eq!(e.to_string(), "found bytes are less than expected");
        }
    }
}
