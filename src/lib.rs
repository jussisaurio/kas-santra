pub mod engine;
pub mod network;
pub mod ql;

use engine::memtable::MemTable;
use engine::operation::Operation;
use engine::sstable::SSTable;
use engine::wal::Wal;
use std::collections::{BTreeMap, HashSet, HashMap};
use std::io::Result;
use uuid::Uuid;

pub struct Database {
    wal: Wal,
    memtable: MemTable,
    sstables: Vec<SSTable>,
    pub data_dir: String,
}

impl Database {
    /// Creates a new Database with an empty MemTable and no SSTables.
    pub fn new(data_dir: &str) -> Self {
        // create data dir if doesnt exist
        std::fs::create_dir_all(data_dir).unwrap_or(());
        let wal_path = format!("{}/wal_{}", data_dir, Uuid::new_v4());
        Self {
            wal: Wal::new(&wal_path),
            memtable: MemTable::new(),
            sstables: Vec::new(),
            data_dir: data_dir.to_string(),
        }
    }

    pub fn wal_path(&self) -> String {
        self.wal.path()
    }

    pub fn get_sstable(&mut self, index: usize) -> Option<&mut SSTable> {
        self.sstables.get_mut(index)
    }

    /// Inserts a key-value pair into the MemTable.
    pub fn set(&mut self, key: String, value: String) {
        self.memtable.set(key, value, &mut self.wal);
    }

    pub fn flush_memtable_to_sstable(&mut self) -> Result<()> {
        // Create new SSTable
        let uuid = Uuid::new_v4();
        let sstable_path = format!("{}/sstable_{}_{}", self.data_dir, self.sstables.len(), uuid);
        let mut sstable = SSTable::new(sstable_path.as_str())?;
        
        let mut offset = 0u64;
        let every_n_entries = sstable.index_every_n_entries;
        let mut entry_count = 0usize;
        
        // Your MemTable data is already sorted if you are using a data structure like BTreeMap
        for (key, operation) in self.memtable.iter() {
            // The `write` method in your SSTable implementation should return the number of bytes written
            let bytes_written = sstable.write(key, operation)?;
            
            // Insert into index; assuming `index` is a BTreeMap<String, u64>
            if entry_count % every_n_entries == 0 {
                sstable.write_to_index(key.clone(), offset);
            }

            entry_count += 1;
            offset += bytes_written as u64;
        }
        
        // Optionally, write the index to a separate index file
        sstable.write_index()?;

        // Add the SSTable to the list of SSTables managed by this Database instance
        self.sstables.push(sstable);

        // Clear the MemTable
        self.memtable.clear(&mut self.wal)?;

        Ok(())
    }

    pub fn replay_from_wal(&mut self, path: &str) {
        let mut wal = Wal::from_path(path);
        self.memtable.replay_wal(&mut wal);
    }

    pub fn memtable_is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    pub fn delete(&mut self, key: &String) {
        self.memtable.delete(key, &mut self.wal);
    }

    pub fn delete_sstables(&mut self) -> Result<()> {
        let paths = self.sstables.iter().map(|sstable| sstable.get_path()).collect::<Vec<String>>();
        self.sstables.clear();
        for path in paths {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    // Merge old SSTables into a new SSTable to reduce the number of SSTables
    // and improve read performance + reduce disk space usage.
    pub fn compact_sstables(&mut self) -> Result<()> {
        let uuid = Uuid::new_v4();
        let sstable_path = format!("{}/sstable_{}_{}", self.data_dir, self.sstables.len(), uuid);
        let mut new_sstable = SSTable::new(sstable_path.as_str())?;

        // we can iterate through sstable entries in order because they are sorted by key
        // for this implementation lets iterate through all of them and write them to a new sstable
        // using a mergesort-like merge step

        // we need to keep track of the current key we are looking at in each sstable
        // we can use a HashMap to keep track of the current key for each sstable
        // and the current offset in the sstable
        let mut current_keys = HashMap::new();
        let mut current_operations = HashMap::new();
        let mut current_sstables = HashSet::new();
        let mut final_ops = BTreeMap::new();

        // an sstable has an .get_iterator() method that returns an iterator over (key, Operation) tuples.
        // we can use this to iterate through the sstable entries in order
        let mut iterators =
            self.sstables.iter_mut()
                .map(|sstable| sstable.get_iterator())
                .filter(|i| i.is_ok())
                .map(|i| i.unwrap())
                .collect::<Vec<_>>();

        // initialize the current key and offset for each sstable
        for (i, iterator) in iterators.iter_mut().enumerate() {
            match iterator.next() {
                Some(Err(e)) => panic!("Error reading SSTable: {}", e),
                Some(Ok((key, operation))) => {
                    current_keys.insert(i, key.clone());
                    current_operations.insert(i, operation.clone());
                    current_sstables.insert(i);
                }
                None => (),
            }
        }
        
        // while there are still sstables with entries
        while !current_sstables.is_empty() {
            // find the smallest key
            let (smallest_key_sstable, smallest_key) = current_keys
                .iter()
                // min_by first by value, then by key (key = sstable number, value = key)
                .min_by(|(ssnum1, key1), (ssnum2, key2)| {
                    let cmp = key1.cmp(key2);
                    if cmp == std::cmp::Ordering::Equal {
                        return ssnum1.cmp(ssnum2);
                    }
                    cmp
                })
                .unwrap();

            // get the smallest key's operation
            let smallest_key_sstable = *smallest_key_sstable;
            let smallest_key = smallest_key.clone();
            let smallest_key_operation = current_operations.get(&smallest_key_sstable).unwrap();
            // write the smallest key and operation to final_ops
            final_ops.insert(smallest_key.clone(), smallest_key_operation.clone());

            // if the sstable we just wrote has no more entries, remove it from the current sstables
            let next = iterators[smallest_key_sstable].next();
            match next {
                Some(Err(e)) => panic!("Error reading SSTable: {}", e),
                Some(Ok((key, operation))) => {
                    current_keys.insert(smallest_key_sstable, key);
                    current_operations.insert(smallest_key_sstable, operation);
                }
                None => {
                    current_keys.remove(&smallest_key_sstable);
                    current_operations.remove(&smallest_key_sstable);
                    current_sstables.remove(&smallest_key_sstable);
                }
            }
        }

        let mut offset = 0u64;
        let every_n_entries = new_sstable.index_every_n_entries;
        let mut entry_count = 0usize;

        // write the final_ops to the new sstable
        for (key, operation) in final_ops.iter() {
            let bytes_written = new_sstable.write(key, operation)?;

            // Insert into index; assuming `index` is a BTreeMap<String, u64>
            if entry_count % every_n_entries == 0 {
                new_sstable.write_to_index(key.clone(), offset);
            }
            
            entry_count += 1;
            offset += bytes_written as u64;
        }

        // Delete old SSTables
        let sstable_paths = self.sstables.iter().map(|sstable| sstable.get_path()).collect::<Vec<String>>();
        self.sstables.clear();
        for path in sstable_paths {
            std::fs::remove_file(path).unwrap_or(());
        }
        self.sstables.push(new_sstable);

        Ok(())
    }


    /// Attempts to read a value for a given key from the database.
    /// 
    /// 1. First checks the MemTable.
    /// 2. If not found in the MemTable, checks each SSTable.
    /// 
    /// Returns `Some(value)` if found, `None` otherwise.
    pub fn get(&mut self, key: &str) -> Option<String> {
        // First, look for the key in the MemTable
        match self.memtable.get(key) {
            Some(Operation::Insert(value)) => return Some(value.clone()),
            Some(Operation::Delete) => return None,
            None => (),
        }

        // If the key is not in the MemTable, scan through each SSTable (newest to oldest)
        for sstable in self.sstables.iter_mut().rev() {
            match sstable.find_key(key) {
                Ok(Some(Operation::Insert(value))) => return Some(value),
                Ok(Some(Operation::Delete)) => return None,
                Ok(None) => continue,
                Err(e) => panic!("Error reading SSTable: {}", e),
            }
        }

        // If the key was not found in either the MemTable or SSTables
        None
    }
}
