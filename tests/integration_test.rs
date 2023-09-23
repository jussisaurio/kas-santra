use kassantra::Database;
use kassantra::engine::operation::Operation;
use uuid::Uuid;

#[test]
fn test_wal_replay() {
    let ctx = setup();
    let mut database = Database::new(&ctx.data_dir);

    database.set("foo".to_string(), "bar".to_string());
    database.set("foo".to_string(), "baz".to_string());

    let ctx2 = setup();

    let mut database2 = Database::new(&ctx2.data_dir);

    assert_eq!(database2.get("foo"), None);

    database2.replay_from_wal(database.wal_path().as_str());

    assert_eq!(database2.get("foo"), Some("baz".to_string()));
}

#[test]
fn test_read_from_sstable_when_memtable_is_empty() {
    let ctx = setup();
    let mut database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string());

    database.flush_memtable_to_sstable().unwrap();

    assert_eq!(database.memtable_is_empty(), true);

    assert_eq!(database.get("foo"), Some("bar".to_string()));
}

#[test]
fn test_read_from_sstable_find_in_older_sstable() {
    let ctx = setup();
    let mut database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string());

    database.flush_memtable_to_sstable().unwrap();

    database.set("boo".to_string(), "waz".to_string());

    database.flush_memtable_to_sstable().unwrap();

    assert_eq!(database.memtable_is_empty(), true);

    assert_eq!(database.get("foo"), Some("bar".to_string()));
    assert_eq!(database.get("boo"), Some("waz".to_string()));
}

#[test]
fn test_deletions_work_in_memtable() {
    let ctx = setup();
    let mut database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string());
    database.set("boo".to_string(), "waz".to_string());

    database.delete(&"foo".to_string());

    assert_eq!(database.get("foo"), None);
    assert_eq!(database.get("boo"), Some("waz".to_string()));
}

#[test]
fn test_deletions_work_in_sstable() {
    let ctx = setup();
    let mut database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string());
    database.set("boo".to_string(), "waz".to_string());

    database.flush_memtable_to_sstable().unwrap();

    database.delete(&"foo".to_string());

    assert_eq!(database.get("foo"), None);
    assert_eq!(database.get("boo"), Some("waz".to_string()));
}

#[test]
fn test_updates_work_in_memtable() {
    let ctx = setup();
    let mut database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string());
    database.set("foo".to_string(), "baz".to_string());

    assert_eq!(database.get("foo"), Some("baz".to_string()));
}

#[test]
fn test_updates_work_in_sstable() {
    let ctx = setup();
    let mut database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string());
    database.set("foo".to_string(), "baz".to_string());

    database.flush_memtable_to_sstable().unwrap();

    assert_eq!(database.get("foo"), Some("baz".to_string()));
}

#[test]
fn test_sstable_entries_are_written_in_alphabetical_order() {
    let ctx = setup();
    let mut database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string());
    database.set("boo".to_string(), "waz".to_string());
    database.set("baz".to_string(), "qux".to_string());

    database.flush_memtable_to_sstable().unwrap();

    assert_eq!(database.memtable_is_empty(), true);

    let sstable = database.get_sstable(0).unwrap();

    let operations = sstable.get_as_operations().unwrap();

    assert_eq!(operations[0].0, "baz");
    assert_eq!(operations[1].0, "boo");
    assert_eq!(operations[2].0, "foo");
}

#[test]
fn test_sstable_compaction() {
    let ctx = setup();
    let mut database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string());
    database.set("boo".to_string(), "waz".to_string());
    database.set("baz".to_string(), "qux".to_string());

    database.flush_memtable_to_sstable().unwrap();

    database.set("foo".to_string(), "baz2".to_string());
    database.set("boo".to_string(), "waz2".to_string());
    database.delete(&"baz".to_string());

    database.flush_memtable_to_sstable().unwrap();

    assert_eq!(database.memtable_is_empty(), true);

    database.compact_sstables().unwrap();

    let sstable = database.get_sstable(0).unwrap();

    let operations = sstable.get_as_operations().unwrap();

    assert!(operations.len() == 3);
    assert_eq!(operations[0], ("baz".to_string(), Operation::Delete));
    assert_eq!(operations[1], ("boo".to_string(), Operation::Insert("waz2".to_string())));
    assert_eq!(operations[2], ("foo".to_string(), Operation::Insert("baz2".to_string())));

    let nonexistent_second_sstable = database.get_sstable(1);
    assert!(nonexistent_second_sstable.is_none());
}

// after all tests, remove all sstables and wal files
struct Setup {
    data_dir: String,
}

impl Drop for Setup {
    fn drop(&mut self) {
        println!("Running teardown");
        teardown(&self.data_dir);
    }
}

fn setup() -> Setup {
    let random_dir_name = Uuid::new_v4().to_string();
    return Setup {
        data_dir: random_dir_name.clone(),
    };
}

fn teardown(data_dir: &str) {
    // remove data dir
    std::fs::remove_dir_all(data_dir).unwrap();
}