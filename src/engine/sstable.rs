use std::{io::{SeekFrom, Result, ErrorKind}, collections::BTreeMap};
use tokio::{fs::{File, OpenOptions}, io::{AsyncSeekExt, AsyncReadExt, AsyncWriteExt}};

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
    pub async fn new(path: &str) -> Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path).await?;
        Ok(SSTable {
            file: f,
            path: path.to_string(),
            index: BTreeMap::new(),
            index_every_n_entries: 10,
        })
    }

    pub async fn from_file(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path).await?;
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
    pub async fn create_index(&mut self) -> Result<()> {
        let mut offset = 0u64;
        let mut buffer = [0; 4];  // To read the u32 lengths of key and value

        // Make sure to start from the beginning of the file
        self.file.seek(SeekFrom::Start(0)).await?;

        self.index.clear();  // Clear any existing index entries

        loop {
            // Read key length
            match self.file.read_exact(&mut buffer).await {
                Ok(_) => {
                    let key_length = u32::from_le_bytes(buffer);
                    
                    // Read value length
                    self.file.read_exact(&mut buffer).await?;
                    let value_length = u32::from_le_bytes(buffer);

                    // Read key
                    let mut key = vec![0; key_length as usize];
                    self.file.read_exact(&mut key).await?;
                    let key = String::from_utf8_lossy(&key).into_owned();

                    // Skip value (we don't need it for index creation)
                    self.file.seek(SeekFrom::Current(value_length as i64)).await?;

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

    pub async fn read_item_at(&mut self, byte_offset: usize) -> Result<Option<(String, usize, Operation)>> {
        let mut buffer = [0; 4];  // To read the u32 lengths of key and value
        // seek
        self.file.seek(SeekFrom::Start(byte_offset as u64)).await?;
        match self.file.read_exact(&mut buffer).await {
            Ok(_) => {
                let key_length = u32::from_le_bytes(buffer);
                
                // Read value length
                self.file.read_exact(&mut buffer).await?;
                let value_length = u32::from_le_bytes(buffer);

                // Read key
                let mut key = vec![0; key_length as usize];
                self.file.read_exact(&mut key).await?;
                let key = String::from_utf8_lossy(&key).into_owned();

                // Read value
                let mut value = vec![0; value_length as usize];
                self.file.read_exact(&mut value).await?;
                let value = String::from_utf8_lossy(&value).into_owned();

                // Update offset
                let new_offset = byte_offset +  4 + 4 + key_length as usize + value_length as usize;  // Key length bytes + Value length bytes + Key bytes + Value bytes

                match value.as_str() {
                    "TOMBSTONE" => Ok(Some((key, new_offset, Operation::Delete))),
                    _ => Ok(Some((key, new_offset, Operation::Insert(value)))),
                }
            },
            Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e),
        }    
    }

    pub async fn write(&mut self, key: &str, operation: &Operation) -> Result<usize> {
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

        self.file.write_all(&key_length.to_le_bytes()).await?;
        self.file.write_all(&value_length.to_le_bytes()).await?;
        self.file.write_all(key.as_bytes()).await?;
        self.file.write_all(value.as_bytes()).await?;

        bytes_written += 4 + 4 + key_length as usize + value_length as usize;

        Ok(bytes_written)
    }

    pub async fn sync(&mut self) -> Result<()> {
        self.file.sync_all().await
    }

    pub async fn get_as_operations(&mut self) -> Result<Vec<(String, Operation)>> {
        let mut buffer = vec![];
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut operations = vec![];

        loop {
            let mut key_length_bytes = [0u8; 4];
            let mut value_length_bytes = [0u8; 4];
            
            // Read lengths
            let read_key_length = self.file.read_exact(&mut key_length_bytes).await;
            if read_key_length.is_err() {
                println!("Error reading key length bytes");
                println!("{:?}", read_key_length);
                break;
            }
            if self.file.read_exact(&mut value_length_bytes).await.is_err() {
                println!("Error reading value length bytes");
                break;
            }
            
            let key_length = u32::from_le_bytes(key_length_bytes);
            let value_length = u32::from_le_bytes(value_length_bytes);
            
            // Read key
            buffer.resize(key_length as usize, 0);
            self.file.read_exact(&mut buffer).await?;
            let key = String::from_utf8_lossy(&buffer);
            
            // Read value
            let mut value_buffer = Vec::new();
            value_buffer.resize(value_length as usize, 0);
            self.file.read_exact(&mut value_buffer).await?;
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

    pub async fn find_key(&mut self, target_key: &str) -> Result<Option<Operation>> {
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

        self.file.seek(SeekFrom::Start(start_offset)).await?;
        
        loop {
            let mut key_length_bytes = [0u8; 4];
            let mut value_length_bytes = [0u8; 4];
            
            // Read lengths
            let read_key_length = self.file.read_exact(&mut key_length_bytes).await;
            if read_key_length.is_err() {
                println!("Error reading key length bytes");
                println!("{:?}", read_key_length);
                break;
            }
            if self.file.read_exact(&mut value_length_bytes).await.is_err() {
                println!("Error reading value length bytes");
                break;
            }
            
            let key_length = u32::from_le_bytes(key_length_bytes);
            let value_length = u32::from_le_bytes(value_length_bytes);
            
            // Read key
            buffer.resize(key_length as usize, 0);
            self.file.read_exact(&mut buffer).await?;
            let key = String::from_utf8_lossy(&buffer);
            
            // Read value
            let mut value_buffer = Vec::new();
            value_buffer.resize(value_length as usize, 0);
            self.file.read_exact(&mut value_buffer).await?;
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
