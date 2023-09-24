pub mod engine;
pub mod network;
pub mod ql;

use engine::memtable::MemTable;
use engine::operation::Operation;
use engine::sstable::SSTable;
use engine::wal::Wal;
use priority_queue::PriorityQueue;
use std::collections::HashSet;
use std::io::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct Database {
    pub wal: Arc<Mutex<Wal>>,
    pub memtable: Arc<Mutex<MemTable>>,
    pub sstables: Arc<Mutex<Vec<SSTable>>>,
    pub sstable_compaction_threshold: usize,
    pub data_dir: String,
}

impl Database {
    /// Creates a new Database with an empty MemTable and no SSTables.
    pub fn new(data_dir: &str) -> Self {
        // create data dir if doesnt exist
        std::fs::create_dir_all(data_dir).unwrap_or(());
        let wal_path = format!("{}/wal_{}", data_dir, Uuid::new_v4());
        Self {
            wal: Arc::new(Mutex::new(Wal::new(wal_path.as_str()))),
            memtable: Arc::new(Mutex::new(MemTable::new())),
            sstables: Arc::new(Mutex::new(Vec::new())),
            sstable_compaction_threshold: 10,
            data_dir: data_dir.to_string(),
        }
    }

    pub async fn wal_path(&self) -> String {
        let foo = self.wal.lock().await;
        foo.path()
    }

    /// Inserts a key-value pair into the MemTable.
    pub async fn set(&self, key: String, value: String) {
        // println!("set: Obtaining lock for memtable");
        let mut memtable = self.memtable.lock().await;
        // println!("set: Obtained lock for memtable");
        // println!("set: Obtaining lock for wal");
        let mut wal = self.wal.lock().await;
        // println!("set: Obtained lock for wal");
        memtable.set(key, value, &mut wal);
        if memtable.is_full() {
            drop(memtable);
            drop(wal);
            self.flush_memtable_to_sstable().await.unwrap();
            // println!("set: Obtaining lock for sstables");
            let sstables = self.sstables.lock().await;
            // println!("set: Obtained lock for sstables");
            if sstables.len() >= self.sstable_compaction_threshold {
                drop(sstables);
                self.compact_sstables().await.unwrap();
            }
        }
    }

    pub async fn flush_memtable_to_sstable(&self) -> Result<()> {
        let flush_start = std::time::Instant::now();
        println!("flush_memtable_to_sstable: Flushing MemTable to SSTable");
        // Create new SSTable
        let uuid = Uuid::new_v4();
        // println!("flush_memtable_to_sstable: obtain lock for sstables");
        let mut sstables = self.sstables.lock().await;
        // println!("flush_memtable_to_sstable: lock obtained for sstables");
        let sstable_path = format!("{}/sstable_{}_{}", self.data_dir, sstables.len(), uuid);
        let mut sstable = SSTable::new(sstable_path.as_str()).await?;

        let every_n_entries = sstable.index_every_n_entries;

        // println!("flush_memtable_to_sstable: obtain lock for memtable");
        let mut memtable = self.memtable.lock().await;
        // println!("flush_memtable_to_sstable: lock obtained for memtable");

        // MemTable data is already sorted if you are using a data structure like BTreeMap
        let vec_of_operations = memtable.iter().collect::<Vec<(&String, &Operation)>>();
        let offsets = sstable.batch_write(&vec_of_operations).await?;

        // for every n entries, add element to index
        for (i, (key, _)) in vec_of_operations.iter().enumerate() {
            if i % every_n_entries == 0 {
                sstable.write_to_index(key.to_string(), offsets[i] as u64);
            }
        }

        // sstable.sync().await?;

        // Optionally, write the index to a separate index file
        sstable.write_index()?;

        // Add the SSTable to the list of SSTables managed by this Database instance
        sstables.push(sstable);
        drop(sstables);

        // Clear the MemTable
        // println!("flush_memtable_to_sstable: obtain lock for wal");
        let mut wal = self.wal.lock().await;
        // println!("flush_memtable_to_sstable: lock obtained for wal");
        memtable.clear(&mut wal)?;

        let flush_end = std::time::Instant::now();

        println!(
            "flush_memtable_to_sstable: Flush took {}ms",
            (flush_end - flush_start).as_millis()
        );

        Ok(())
    }

    pub async fn replay_from_wal(&self, path: &str) {
        let mut wal = Wal::from_path(path);
        let mut memtable = self.memtable.lock().await;
        memtable.replay_wal(&mut wal);
    }

    pub async fn memtable_is_empty(&self) -> bool {
        self.memtable.lock().await.is_empty()
    }

    pub async fn delete(&self, key: &String) {
        let mut memtable = self.memtable.lock().await;
        let mut wal = self.wal.lock().await;
        memtable.delete(key, &mut wal);
        if memtable.is_full() {
            drop(memtable);
            self.flush_memtable_to_sstable().await.unwrap();
            // println!("delete: Obtaining lock for sstables");
            let sstables = self.sstables.lock().await;
            // println!("delete: Obtained lock for sstables");
            if sstables.len() >= self.sstable_compaction_threshold {
                drop(sstables);
                self.compact_sstables().await.unwrap();
            }
        }
    }

    pub async fn delete_sstables(&self) -> Result<()> {
        let mut sstables = self.sstables.lock().await;
        let paths = sstables
            .iter()
            .map(|sstable| sstable.get_path())
            .collect::<Vec<String>>();
        sstables.clear();
        for path in paths {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    // Merge old SSTables into a new SSTable to reduce the number of SSTables
    // and improve read performance + reduce disk space usage.
    pub async fn compact_sstables(&self) -> Result<()> {
        // time how much compaction takes
        let start = std::time::Instant::now();
        println!("Compacting SSTables");

        let mut keys_priority_queue = PriorityQueue::new();
        let uuid = Uuid::new_v4();

        let sstable_path = format!(
            "{}/sstable_{}_{}",
            self.data_dir,
            self.sstables.lock().await.len(),
            uuid
        );
        let mut new_sstable = SSTable::new(sstable_path.as_str()).await?;

        // we can iterate through sstable entries in order because they are sorted by key
        // for this implementation lets iterate through all of them and write them to a new sstable
        // using a mergesort-like merge step

        // we need to keep track of the current key we are looking at in each sstable
        // we can use a HashMap to keep track of the current key for each sstable
        // and the current offset in the sstable
        let mut current_sstables = HashSet::new();
        let mut final_ops = Vec::new();

        // an sstable has a read_item_at() method that you can pass a byte offset, it returns the next offset to read from
        // async iterators aren't a stable feature so not using them for that reason
        let mut sstables = self.sstables.lock().await;
        let mut read_indexes = sstables.iter().map(|_| 0).collect::<Vec<usize>>();

        // initialize the current key and offset for each sstable
        for (i, table) in sstables.iter_mut().enumerate() {
            match table.read_item_at(read_indexes[i]).await {
                Err(e) => panic!("Error reading SSTable: {}", e),
                Ok(Some((key, new_offset, operation))) => {
                    keys_priority_queue.push(
                        i,
                        CompactionPriorityQueueItem {
                            key: key.clone(),
                            sstable_index: i,
                            operation: operation.clone(),
                        },
                    );
                    read_indexes[i] = new_offset;
                    current_sstables.insert(i);
                }
                Ok(None) => (),
            }
        }

        // while there are still sstables with entries
        while keys_priority_queue.len() > 0 {
            // find the sstable and operation associated with the smallest key
            let (_, item) = keys_priority_queue.pop().unwrap();
            loop {
                // pop same items since they are duplicates and we are ordering by newest sstable first
                let next = keys_priority_queue.peek();
                match next {
                    Some((_, next_item)) => {
                        if next_item.key == item.key {
                            keys_priority_queue.pop();
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }

            // get the smallest key's operation
            let smallest_key_sstable = item.sstable_index;
            let smallest_key = item.key;
            let smallest_key_operation = item.operation;
            // write the smallest key and operation to final_ops
            final_ops.push((smallest_key.clone(), smallest_key_operation.clone()));

            // if the sstable we just wrote has no more entries, remove it from the current sstables
            let next = sstables[smallest_key_sstable]
                .read_item_at(read_indexes[smallest_key_sstable])
                .await;
            match next {
                Err(e) => panic!("Error reading SSTable: {}", e),
                Ok(Some((key, new_offset, operation))) => {
                    keys_priority_queue.push(
                        smallest_key_sstable,
                        CompactionPriorityQueueItem {
                            key: key.clone(),
                            sstable_index: smallest_key_sstable,
                            operation: operation.clone(),
                        },
                    );
                    read_indexes[smallest_key_sstable] = new_offset;
                }
                Ok(None) => {
                    current_sstables.remove(&smallest_key_sstable);
                }
            }
        }

        let mut offset = 0u64;
        let every_n_entries = new_sstable.index_every_n_entries;
        let mut entry_count = 0usize;

        let _write_start = std::time::Instant::now();
        // write the final_ops to the new sstable
        for (key, operation) in final_ops.iter() {
            let bytes_written = new_sstable.write(&key, &operation).await?;

            // Insert into index; assuming `index` is a BTreeMap<String, u64>
            if entry_count % every_n_entries == 0 {
                new_sstable.write_to_index(key.clone(), offset);
            }

            entry_count += 1;
            offset += bytes_written as u64;
        }
        // new_sstable.sync().await?;
        let _write_end = std::time::Instant::now();
        // println!("Write took {}ms", (write_end - write_start).as_millis());

        // Delete old SSTables
        let sstable_paths = sstables
            .iter()
            .map(|sstable| sstable.get_path())
            .collect::<Vec<String>>();
        sstables.clear();
        for path in sstable_paths {
            std::fs::remove_file(path).unwrap_or(());
        }
        sstables.push(new_sstable);

        let end = std::time::Instant::now();

        println!("Compaction took {}ms", (end - start).as_millis());

        Ok(())
    }

    /// Attempts to read a value for a given key from the database.
    ///
    /// 1. First checks the MemTable.
    /// 2. If not found in the MemTable, checks each SSTable.
    ///
    /// Returns `Some(value)` if found, `None` otherwise.
    pub async fn get(&self, key: &str) -> Option<String> {
        // First, look for the key in the MemTable
        let memtable = self.memtable.lock().await;
        match memtable.get(key) {
            Some(Operation::Insert(value)) => return Some(value.clone()),
            Some(Operation::Delete) => return None,
            None => (),
        }

        // println!("get: Obtaining lock for sstables");
        let mut sstables = self.sstables.lock().await;
        // println!("get: Obtained lock for sstables");
        // If the key is not in the MemTable, scan through each SSTable (newest to oldest)
        for sstable in sstables.iter_mut().rev() {
            match sstable.find_key(key).await {
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

#[derive(PartialEq, Eq, Debug, Clone)]
struct CompactionPriorityQueueItem {
    key: String,
    sstable_index: usize,
    operation: Operation,
}

impl Ord for CompactionPriorityQueueItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key.cmp(&other.key) {
            std::cmp::Ordering::Equal => self.sstable_index.cmp(&other.sstable_index),
            ordering => ordering,
        }
    }
}

impl PartialOrd for CompactionPriorityQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
