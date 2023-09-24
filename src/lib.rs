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
        let mut sstables = self.sstables.lock().await;
        let mut current_sstables = sstables
            .iter()
            .enumerate()
            .map(|(i, _)| i)
            .collect::<HashSet<_>>();
        let mut final_ops = Vec::new();

        // an sstable has a read_item_at() method that you can pass a byte offset, it returns the next offset to read from
        // async iterators aren't a stable feature so not using them for that reason

        let mut read_indexes = sstables.iter().map(|_| 0).collect::<Vec<usize>>();

        let mut ops_in_queue_per_sstable = sstables.iter().map(|_| 0).collect::<Vec<usize>>();

        // while there are still sstables with entries
        while current_sstables.len() > 0 {
            if keys_priority_queue.len() == 0 {
                // initialize the current key and offset for each sstable
                for (i, table) in sstables.iter_mut().enumerate() {
                    if !current_sstables.contains(&i) {
                        continue;
                    }
                    // println!("Reading this doofus: {:?}", i);
                    match table.batch_read(10, read_indexes[i]).await {
                        Err(e) => panic!("Error reading SSTable: {}", e),
                        Ok((tuples, new_offset)) => {
                            if tuples.len() == 0 {
                                current_sstables.remove(&i);
                                continue;
                            }
                            ops_in_queue_per_sstable[i] += tuples.len();
                            for (key, operation) in tuples {
                                // println!("Adding to queue: {:?}", key);
                                let item = CompactionPriorityQueueItem {
                                    key: key.clone(),
                                    sstable_index: i,
                                    operation: operation.clone(),
                                };
                                keys_priority_queue.push(item.clone(), item);
                            }
                            read_indexes[i] = new_offset;
                            current_sstables.insert(i);
                        }
                    }
                }
            }

            // there might be no more entries in any sstable even though they were in current_sstables at the start of the loop
            if keys_priority_queue.len() == 0 {
                break;
            }
            // find the sstable and operation associated with the smallest key
            let (_, item) = keys_priority_queue.pop().unwrap();
            // println!("Item from queue: {:?}", item);
            // println!("Items in queue per sstable: {:?}", ops_in_queue_per_sstable);
            // println!("Current sstables: {:?}", current_sstables);
            // println!("Smallest key ssstable: {:?}", item.sstable_index);
            // println!("Queue: {:?}", keys_priority_queue.len());
            loop {
                // pop same items since they are duplicates and we are ordering by newest sstable first
                let next = keys_priority_queue.peek();
                match next {
                    Some((_, next_item)) => {
                        if next_item.key == item.key {
                            let item = keys_priority_queue.pop().unwrap();
                            // println!("Dropping duplicate: {:?}", item);
                            ops_in_queue_per_sstable[item.0.sstable_index] -= 1;
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
            final_ops.push((smallest_key, smallest_key_operation));

            // if the sstable that has the smallest key has no more entries in the pq currently, load more entries
            // if there aren't any more to load, remove it from current_sstables
            ops_in_queue_per_sstable[smallest_key_sstable] -= 1;
            if ops_in_queue_per_sstable[smallest_key_sstable] == 0 {
                // println!("Reading this dingus: {:?}", smallest_key_sstable);
                match sstables[smallest_key_sstable]
                    .batch_read(10, read_indexes[smallest_key_sstable])
                    .await
                {
                    Err(e) => panic!("Error reading SSTable: {}", e),
                    Ok((tuples, new_offset)) => {
                        if tuples.len() == 0 {
                            current_sstables.remove(&smallest_key_sstable);
                            continue;
                        }
                        ops_in_queue_per_sstable[smallest_key_sstable] += tuples.len();
                        for (key, operation) in tuples {
                            let item = CompactionPriorityQueueItem {
                                key: key.clone(),
                                sstable_index: smallest_key_sstable,
                                operation: operation.clone(),
                            };
                            keys_priority_queue.push(item.clone(), item);
                        }
                        read_indexes[smallest_key_sstable] = new_offset;
                        current_sstables.insert(smallest_key_sstable);
                    }
                }
            }
        }

        let every_n_entries = new_sstable.index_every_n_entries;

        let vec_of_operations = final_ops
            .iter()
            .map(|(key, op)| (key, op))
            .collect::<Vec<_>>();

        let offsets = new_sstable.batch_write(&vec_of_operations).await?;

        // for every n entries, add element to index
        for (i, (key, _)) in final_ops.iter().enumerate() {
            if i % every_n_entries == 0 {
                new_sstable.write_to_index(key.to_string(), offsets[i] as u64);
            }
        }

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
            Some(Operation::Insert(value)) => {
                println!("get: Found key in memtable");
                return Some(value.clone());
            }
            Some(Operation::Delete) => {
                println!("get: Found key in memtable but it was deleted");
                return None;
            }
            None => {
                println!("get: Key not found in memtable");
            }
        }

        // println!("get: Obtaining lock for sstables");
        let mut sstables = self.sstables.lock().await;
        // println!("get: Obtained lock for sstables");
        // If the key is not in the MemTable, scan through each SSTable (newest to oldest)
        for (i, sstable) in sstables.iter_mut().rev().enumerate() {
            match sstable.find_key(key).await {
                Ok(Some(Operation::Insert(value))) => {
                    println!("get: Found key in sstable {}", i);
                    return Some(value);
                }
                Ok(Some(Operation::Delete)) => {
                    println!("get: Found tombstone in sstable {}", i);
                    return None;
                }
                Ok(None) => {
                    println!("get: Key not found in sstable {}", i);
                }
                Err(e) => panic!("Error reading SSTable: {}", e),
            }
        }

        // If the key was not found in either the MemTable or SSTables
        None
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Hash)]
pub struct CompactionPriorityQueueItem {
    key: String,
    sstable_index: usize,
    operation: Operation,
}

impl Ord for CompactionPriorityQueueItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key.cmp(&other.key) {
            std::cmp::Ordering::Equal => self.sstable_index.cmp(&other.sstable_index),
            ordering => ordering.reverse(),
        }
    }
}

impl PartialOrd for CompactionPriorityQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Test that CompactionPriorityQueueItem is ordered correctly
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_priority_queue_item_ordering() {
        let item1 = CompactionPriorityQueueItem {
            key: "aaa".to_string(),
            sstable_index: 0,
            operation: Operation::Insert("aaa".to_string()),
        };
        let item2 = CompactionPriorityQueueItem {
            key: "bbb".to_string(),
            sstable_index: 0,
            operation: Operation::Insert("bbb".to_string()),
        };

        assert!(item1 > item2);

        let item1 = CompactionPriorityQueueItem {
            key: "bbb".to_string(),
            sstable_index: 0,
            operation: Operation::Insert("bbb".to_string()),
        };

        let item2 = CompactionPriorityQueueItem {
            key: "bbb".to_string(),
            sstable_index: 1,
            operation: Operation::Insert("bbb".to_string()),
        };

        let item3 = CompactionPriorityQueueItem {
            key: "aaa".to_string(),
            sstable_index: 0,
            operation: Operation::Insert("aaa".to_string()),
        };

        assert!(item1 < item2);
        assert!(item2 < item3);

        let mut pq = PriorityQueue::new();

        pq.push(0, item1.clone());
        pq.push(1, item2.clone());
        pq.push(2, item3.clone());

        assert_eq!(pq.pop().unwrap(), (2, item3));
        assert_eq!(pq.pop().unwrap(), (1, item2));
        assert_eq!(pq.pop().unwrap(), (0, item1));
    }
}
