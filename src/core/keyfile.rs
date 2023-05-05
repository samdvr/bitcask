use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct KeyMetadata {
    file_id: u32,
    offset: u64,
    size: u64,
}

pub struct BitcaskKeyFile {
    file_path: String,
    key_map: HashMap<String, KeyMetadata>,
}

impl BitcaskKeyFile {
    pub fn new(file_path: &str) -> Self {
        BitcaskKeyFile {
            file_path: file_path.to_string(),
            key_map: HashMap::new(),
        }
    }

    pub fn load(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if Path::new(&self.file_path).exists() {
            let file = File::open(&self.file_path)?;
            let mut buf_reader = BufReader::new(file);
            let mut buf = Vec::new();
            buf_reader.read_to_end(&mut buf)?;

            self.key_map = deserialize(&buf)?;
        }

        Ok(())
    }

    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.file_path)?;

        let mut buf_writer = BufWriter::new(file);
        let buf = serialize(&self.key_map)?;
        buf_writer.write_all(&buf)?;

        Ok(())
    }

    pub fn add_key(&mut self, key: String, file_id: u32, offset: u64, size: u64) {
        self.key_map.insert(
            key,
            KeyMetadata {
                file_id,
                offset,
                size,
            },
        );
    }

    pub fn get_key_info(&self, key: &str) -> Option<&KeyMetadata> {
        self.key_map.get(key)
    }

    pub fn remove_key(&mut self, key: &str) -> Option<KeyMetadata> {
        self.key_map.remove(key)
    }
}
