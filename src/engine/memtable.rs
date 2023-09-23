use super::operation::Operation;
use super::wal::Wal;
use std::collections::BTreeMap;
use std::io::Result;

pub struct MemTable {
    store: BTreeMap<String, Operation>,
    flush_threshold_bytes: usize,
    size_bytes: i64,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            store: BTreeMap::new(),
            flush_threshold_bytes: 1024,
            size_bytes: 0,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Operation> {
        self.store.get(key)
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.flush_threshold_bytes as i64
    }

    pub fn delete(&mut self, key: &String, wal: &mut Wal) {
        // Log the delete operation first
        let log_entry = format!("DELETE\t{}\n", key);
        let bytes = log_entry.as_bytes();
        wal.append(bytes).expect("Failed to write to WAL");
        let (existing_key_bytes, existing_value_bytes) = match self.store.get(key) {
            Some(Operation::Insert(value)) => (key.len(), value.len()),
            Some(Operation::Delete) => (key.len(), 0),
            _ => (0, 0),
        };

        let byte_diff = key.len() as i64 - (existing_key_bytes + existing_value_bytes) as i64;
        self.store.insert(key.clone(), Operation::Delete);
        self.size_bytes += byte_diff;
    }

    /// Write data to the MemTable and log it to the Write-Ahead Log.
    pub fn set(&mut self, key: String, value: String, wal: &mut Wal) {
        // Log the write operation first
        let log_entry = format!("INSERT\t{}\t{}\n", key, value);
        let bytes = log_entry.as_bytes();
        wal.append(bytes).expect("Failed to write to WAL");

        let (existing_key_bytes, existing_value_bytes) = match self.store.get(&key) {
            Some(Operation::Insert(existing_value)) => (key.len(), existing_value.len()),
            Some(Operation::Delete) => (key.len(), 0),
            None => (0, 0),
        };

        // Now insert the data into the MemTable
        let byte_diff = key.len() as i64 + value.len() as i64
            - (existing_key_bytes + existing_value_bytes) as i64;
        self.store.insert(key, Operation::Insert(value));
        self.size_bytes += byte_diff;
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    pub fn replay_wal(&mut self, wal: &mut Wal) {
        // get an iterator to WAL lines so we dont have to read the whole file into memory
        let wal_iterator = wal.get_line_iterator();

        for line in wal_iterator {
            let line = line.unwrap();
            let mut parts = line.split("\t");
            let operation = parts.next().unwrap();
            let key = parts.next().unwrap();
            match operation {
                "INSERT" => {
                    let value = parts.next().unwrap();
                    self.store
                        .insert(key.to_string(), Operation::Insert(value.to_string()));
                }
                "DELETE" => {
                    self.store.remove(key);
                }
                _ => panic!("Unknown operation {}", operation),
            }
        }
    }

    pub fn clear(&mut self, wal: &mut Wal) -> Result<()> {
        self.store.clear();
        self.size_bytes = 0;
        wal.clear()
    }

    // return an immutable iterator over the memtable
    pub fn iter(&self) -> std::collections::btree_map::Iter<String, Operation> {
        self.store.iter()
    }
}
