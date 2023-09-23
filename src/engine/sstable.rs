use std::{fs::File, fs::OpenOptions, io::{Write, Seek, SeekFrom, Result, Read, ErrorKind}, collections::BTreeMap};

use super::operation::Operation;

// derive Debug
#[derive(Debug)]
pub struct SSTable {
    file: File,
    path: String,
    index: BTreeMap<String, u64>, // key -> offset
    pub index_every_n_entries: usize,
}

impl SSTable {
    pub fn new(path: &str) -> Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(SSTable {
            file: f,
            path: path.to_string(),
            index: BTreeMap::new(),
            index_every_n_entries: 10,
        })
    }

    pub fn from_file(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        let mut table = SSTable {
            file,
            path: path.to_string(),
            index: BTreeMap::new(),
            index_every_n_entries: 10,
        };

        table.load_index()?;

        Ok(table)
    }

    pub fn get_path(&self) -> String {
        self.path.clone()
    }

    pub fn load_index(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn write_index(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn write_to_index(&mut self, key: String, offset: u64) {
        self.index.insert(key, offset);
    }
    
    // Create the index after writing entries to the SSTable
    pub fn create_index(&mut self) -> Result<()> {
        let mut offset = 0u64;
        let mut buffer = [0; 4];  // To read the u32 lengths of key and value

        // Make sure to start from the beginning of the file
        self.file.seek(SeekFrom::Start(0))?;

        self.index.clear();  // Clear any existing index entries

        loop {
            // Read key length
            match self.file.read_exact(&mut buffer) {
                Ok(_) => {
                    let key_length = u32::from_le_bytes(buffer);
                    
                    // Read value length
                    self.file.read_exact(&mut buffer)?;
                    let value_length = u32::from_le_bytes(buffer);

                    // Read key
                    let mut key = vec![0; key_length as usize];
                    self.file.read_exact(&mut key)?;
                    let key = String::from_utf8_lossy(&key).into_owned();

                    // Skip value (we don't need it for index creation)
                    self.file.seek(SeekFrom::Current(value_length as i64))?;

                    // Insert the key and its corresponding offset into index
                    self.index.insert(key, offset);

                    // Update offset
                    offset += 4 + 4 + key_length as u64 + value_length as u64;  // Key length bytes + Value length bytes + Key bytes + Value bytes
                },
                Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    pub fn get_iterator(&mut self) -> Result<SSTableIterator> {
        let buffer = [0; 4];  // To read the u32 lengths of key and value

        // Make sure to start from the beginning of the file
        let offset = self.file.seek(SeekFrom::Start(0))?;

        // Create the iterator
        let iterator = SSTableIterator {
            file: &mut self.file,
            offset,
            buffer,
        };

        Ok(iterator)
    }

    pub fn write(&mut self, key: &str, operation: &Operation) -> Result<usize> {
        let key_length = key.len() as u32;
        let mut bytes_written = 0;

        let (value, value_length) = match operation {
            Operation::Insert(val) => {
                (val.as_str(), val.len() as u32)
            }
            Operation::Delete => {
                ("TOMBSTONE", "TOMBSTONE".len() as u32)
            }
        };

        self.file.write_all(&key_length.to_le_bytes())?;
        self.file.write_all(&value_length.to_le_bytes())?;
        self.file.write_all(key.as_bytes())?;
        self.file.write_all(value.as_bytes())?;

        bytes_written += 4 + 4 + key_length as usize + value_length as usize;

        Ok(bytes_written)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()
    }

    pub fn get_as_operations(&mut self) -> Result<Vec<(String, Operation)>> {
        let mut buffer = vec![];
        self.file.seek(SeekFrom::Start(0))?;
        let mut operations = vec![];

        loop {
            let mut key_length_bytes = [0u8; 4];
            let mut value_length_bytes = [0u8; 4];
            
            // Read lengths
            let read_key_length = self.file.read_exact(&mut key_length_bytes);
            if read_key_length.is_err() {
                println!("Error reading key length bytes");
                println!("{:?}", read_key_length);
                break;
            }
            if self.file.read_exact(&mut value_length_bytes).is_err() {
                println!("Error reading value length bytes");
                break;
            }
            
            let key_length = u32::from_le_bytes(key_length_bytes);
            let value_length = u32::from_le_bytes(value_length_bytes);
            
            // Read key
            buffer.resize(key_length as usize, 0);
            self.file.read_exact(&mut buffer)?;
            let key = String::from_utf8_lossy(&buffer);
            
            // Read value
            let mut value_buffer = Vec::new();
            value_buffer.resize(value_length as usize, 0);
            self.file.read_exact(&mut value_buffer)?;
            let value = String::from_utf8_lossy(&value_buffer);

            println!("key: {}, value: {}", key, value);
            
            if value == "TOMBSTONE" {
                operations.push((key.into_owned(), Operation::Delete));
            } else {
                operations.push((key.into_owned(), Operation::Insert(value.into_owned())));
            }
        }

        Ok(operations)
    }

    pub fn find_key(&mut self, target_key: &str) -> Result<Option<Operation>> {
        let mut buffer = vec![];
        // binary search self.index (in memory) to find the closest key
        // btreemap keys are sorted, so we can use binary search
        let keys = self.index.keys().collect::<Vec<&String>>();
        let mut start = 0;
        let mut end = keys.len() - 1;
        let mut middle = (start + end) / 2;
        while (end - start) > 1 {
            if keys[middle] == &target_key {
                break;
            } else if keys[middle].as_str().cmp(target_key) == std::cmp::Ordering::Greater {
                end = middle;
            } else {
                start = middle;
            }
            middle = (start + end) / 2;
        }
        let closest_key = keys[middle];
        let start_offset = self.index[closest_key];

        self.file.seek(SeekFrom::Start(start_offset))?;
        
        loop {
            let mut key_length_bytes = [0u8; 4];
            let mut value_length_bytes = [0u8; 4];
            
            // Read lengths
            let read_key_length = self.file.read_exact(&mut key_length_bytes);
            if read_key_length.is_err() {
                println!("Error reading key length bytes");
                println!("{:?}", read_key_length);
                break;
            }
            if self.file.read_exact(&mut value_length_bytes).is_err() {
                println!("Error reading value length bytes");
                break;
            }
            
            let key_length = u32::from_le_bytes(key_length_bytes);
            let value_length = u32::from_le_bytes(value_length_bytes);
            
            // Read key
            buffer.resize(key_length as usize, 0);
            self.file.read_exact(&mut buffer)?;
            let key = String::from_utf8_lossy(&buffer);
            
            // Read value
            let mut value_buffer = Vec::new();
            value_buffer.resize(value_length as usize, 0);
            self.file.read_exact(&mut value_buffer)?;
            let value = String::from_utf8_lossy(&value_buffer);

            println!("key: {}, value: {}, target_key: {}", key, value, target_key);
            
            if key == target_key {
                return if value == "TOMBSTONE" {
                    Ok(Some(Operation::Delete))
                } else {
                    Ok(Some(Operation::Insert(value.into_owned())))
                };
            }
        }

        Ok(None)
    }

}

pub struct SSTableIterator<'a> {
    file: &'a mut File,
    offset: u64,
    buffer: [u8; 4],
}

impl<'a> Iterator for SSTableIterator<'a> {
    type Item = Result<(String, Operation)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read key length
        match self.file.read_exact(&mut self.buffer) {
            Ok(_) => {
                let key_length = u32::from_le_bytes(self.buffer);
                
                // Read value length
                self.file.read_exact(&mut self.buffer).unwrap();
                let value_length = u32::from_le_bytes(self.buffer);

                // Read key
                let mut key = vec![0; key_length as usize];
                self.file.read_exact(&mut key).unwrap();
                let key = String::from_utf8_lossy(&key).into_owned();

                // Read value
                let mut value = vec![0; value_length as usize];
                self.file.read_exact(&mut value).unwrap();
                let value = String::from_utf8_lossy(&value).into_owned();

                // Update offset
                self.offset += 4 + 4 + key_length as u64 + value_length as u64;  // Key length bytes + Value length bytes + Key bytes + Value bytes

                match value.as_str() {
                    "TOMBSTONE" => Some(Ok((key, Operation::Delete))),
                    _ => Some(Ok((key, Operation::Insert(value)))),
                }
            },
            Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}