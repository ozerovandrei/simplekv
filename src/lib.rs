use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt};
use crc::crc32;

type ByteString = Vec<u8>;
type ByteStr = [u8];

#[derive(Debug, Deserialize, Serialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug)]
pub struct SimpleKV {
    f: File,

    // Mapping between keys and file locations.
    pub index: HashMap<ByteString, u64>,
}

impl SimpleKV {
    pub fn open(path: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;

        Ok(SimpleKV {
            f,
            index: HashMap::new(),
        })
    }

    // Populate the index with mapping.
    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.f);

        loop {
            // Number of bytes from the start of the file. This is used as value of the index.
            let current_position = f.seek(SeekFrom::Current(0))?;

            // Read a record in the file at its current position.
            let maybe_kv = SimpleKV::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => match err.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };

            self.index.insert(kv.key, current_position);
        }

        Ok(())
    }

    // Use "Bitcask" file format for processing records:
    //  1. Read twelve bytes that represents a checksum, key length and value length.
    //  2. Read the rest of the data from disk and verify it.
    //
    //  Fixed-width header   Variable-length body
    //  -----------------   --------------------------
    // /                 \/                           \
    // +=====+=====+=====+====== - - +============= - - +
    // | u32 | u32 | u32 | [u8]      | [u8]             |
    // +=====+=====+=====+====== - - +============= - - +
    // checksum (4 bytes)
    // key_len (4 bytes)
    // val_len (4 bytes)
    // key (key_len bytes)
    // value (val_len bytes)
    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let val_len = f.read_u32::<LittleEndian>()?;
        let data_len = key_len + val_len;

        let mut data = ByteString::with_capacity(data_len as usize);

        {
            // Sidestep ownership issues by using short-lived scope.
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }

        // This test is disabled in optimized build.
        debug_assert_eq!(data.len(), data_len as usize);

        let checksum = crc32::checksum_ieee(&data);
        if checksum != saved_checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "data corruption encountered ({:08x} != {:08x})",
                    checksum, saved_checksum
                ),
            ));
        }

        // Split data Vec<u8> in two at key_len.
        let value = data.split_off(key_len as usize);
        let key = data;

        Ok(KeyValuePair { key, value })
    }
}
