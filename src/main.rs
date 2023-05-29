use std::io::Write;
use std::io::{Seek, SeekFrom};

mod core;

use std::fs::OpenOptions;

use crate::core::serdes::{KeyValue, Serdes};

fn main() -> std::io::Result<()> {
    let kv = KeyValue::new(b"test_key2".to_vec(), b"test_value".to_vec());
    let serialized: Vec<u8> = KeyValue::serialize(&kv).unwrap();

    let data_file_path = "data.bin";

    // todo: add key file updating here

    // Append serialized data to the data file and store the offset in the HashMap
    let mut data_file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(data_file_path)?;

    let _offset = data_file.seek(SeekFrom::End(0))?;
    data_file.write_all(&serialized)?;

    Ok(())
}
