use kassantra::engine::operation::Operation;
use kassantra::Database;
use uuid::Uuid;

#[tokio::test]
async fn test_wal_replay() {
    let ctx = setup().await;
    let database = Database::new(&ctx.data_dir);

    database.set("foo".to_string(), "bar".to_string()).await;
    database.set("foo".to_string(), "baz".to_string()).await;

    let ctx2 = setup().await;

    let database2 = Database::new(&ctx2.data_dir);

    assert_eq!(database2.get("foo").await, None);

    database2
        .replay_from_wal(database.wal_path().await.as_str())
        .await;

    assert_eq!(database2.get("foo").await, Some("baz".to_string()));
}

#[tokio::test]
async fn test_read_from_sstable_when_memtable_is_empty() {
    let ctx = setup().await;
    let database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    assert_eq!(database.memtable_is_empty().await, true);

    assert_eq!(database.get("foo").await, Some("bar".to_string()));
}

#[tokio::test]
async fn test_read_from_sstable_find_in_older_sstable() {
    let ctx = setup().await;
    let database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    database.set("boo".to_string(), "waz".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    assert_eq!(database.memtable_is_empty().await, true);

    assert_eq!(database.get("foo").await, Some("bar".to_string()));
    assert_eq!(database.get("boo").await, Some("waz".to_string()));
}

#[tokio::test]
async fn test_deletions_work_in_memtable() {
    let ctx = setup().await;
    let database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string()).await;
    database.set("boo".to_string(), "waz".to_string()).await;

    database.delete(&"foo".to_string()).await;

    assert_eq!(database.get("foo").await, None);
    assert_eq!(database.get("boo").await, Some("waz".to_string()));
}

#[tokio::test]
async fn test_deletions_work_in_sstable() {
    let ctx = setup().await;
    let database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string()).await;
    database.set("boo".to_string(), "waz".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    database.delete(&"foo".to_string()).await;

    assert_eq!(database.get("foo").await, None);
    assert_eq!(database.get("boo").await, Some("waz".to_string()));
}

#[tokio::test]
async fn test_updates_work_in_memtable() {
    let ctx = setup().await;
    let database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string()).await;
    database.set("foo".to_string(), "baz".to_string()).await;

    assert_eq!(database.get("foo").await, Some("baz".to_string()));
}

#[tokio::test]
async fn test_updates_work_in_sstable() {
    let ctx = setup().await;
    let database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string()).await;
    database.set("foo".to_string(), "baz".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    assert_eq!(database.get("foo").await, Some("baz".to_string()));
}

#[tokio::test]
async fn test_sstable_entries_are_written_in_alphabetical_order() {
    let ctx = setup().await;
    let database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string()).await;
    database.set("boo".to_string(), "waz".to_string()).await;
    database.set("baz".to_string(), "qux".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    assert_eq!(database.memtable_is_empty().await, true);

    let mut sstables = database.sstables.lock().await;
    let sstable = &mut sstables[0];

    let operations = sstable.read_all().await.unwrap();

    assert_eq!(operations[0].0, "baz");
    assert_eq!(operations[1].0, "boo");
    assert_eq!(operations[2].0, "foo");
}

#[tokio::test]
async fn test_sstable_compaction() {
    let ctx = setup().await;
    let database = Database::new(ctx.data_dir.as_str());

    database.set("foo".to_string(), "bar".to_string()).await;
    database.set("boo".to_string(), "waz".to_string()).await;
    database.set("baz".to_string(), "qux".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    database.set("foo".to_string(), "baz2".to_string()).await;
    database.set("boo".to_string(), "waz2".to_string()).await;
    database.delete(&"baz".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    assert_eq!(database.memtable_is_empty().await, true);

    database.compact_sstables().await.unwrap();

    let mut sstables = database.sstables.lock().await;
    let sstable = &mut sstables[0];

    let operations = sstable.read_all().await.unwrap();

    assert!(operations.len() == 3);
    assert_eq!(operations[0], ("baz".to_string(), Operation::Delete));
    assert_eq!(
        operations[1],
        ("boo".to_string(), Operation::Insert("waz2".to_string()))
    );
    assert_eq!(
        operations[2],
        ("foo".to_string(), Operation::Insert("baz2".to_string()))
    );

    let nonexistent_second_sstable = sstables.get(1);
    assert!(nonexistent_second_sstable.is_none());
}

#[tokio::test]
async fn test_sstable_compaction_keys_are_ordered_after_compaction() {
    let ctx = setup().await;
    let database = Database::new(ctx.data_dir.as_str());

    database.set("fff".to_string(), "fff".to_string()).await;
    database.set("eee".to_string(), "eee".to_string()).await;
    database.set("ddd".to_string(), "ddd".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    database.set("ccc".to_string(), "ccc".to_string()).await;
    database.set("bbb".to_string(), "bbb".to_string()).await;
    database.set("aaa".to_string(), "aaa".to_string()).await;

    database.flush_memtable_to_sstable().await.unwrap();

    database.compact_sstables().await.unwrap();

    let mut sstables = database.sstables.lock().await;

    let sstable = &mut sstables[0];

    let operations = sstable.read_all().await.unwrap();

    println!("{:?}", operations);
    assert!(operations.len() == 6);

    assert_eq!(
        operations[0],
        ("aaa".to_string(), Operation::Insert("aaa".to_string()))
    );
    assert_eq!(
        operations[1],
        ("bbb".to_string(), Operation::Insert("bbb".to_string()))
    );
    assert_eq!(
        operations[2],
        ("ccc".to_string(), Operation::Insert("ccc".to_string()))
    );
    assert_eq!(
        operations[3],
        ("ddd".to_string(), Operation::Insert("ddd".to_string()))
    );
    assert_eq!(
        operations[4],
        ("eee".to_string(), Operation::Insert("eee".to_string()))
    );
    assert_eq!(
        operations[5],
        ("fff".to_string(), Operation::Insert("fff".to_string()))
    );
}

// #[tokio::test]
// async fn test_an_arbitrary_sstable_to_see_what_it_contains() {
//     let path = "data/sstable_10_71aed8fe-2119-4216-9cd6-2244963e2861";
//     let mut sstable = kassantra::engine::sstable::SSTable::from_file(path)
//         .await
//         .unwrap();

//     let operations = sstable.read_all().await.unwrap();

//     println!("{:?}", operations);

//     assert!(false);
// }

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

async fn setup() -> Setup {
    let random_dir_name = Uuid::new_v4().to_string();
    return Setup {
        data_dir: random_dir_name.clone(),
    };
}

fn teardown(data_dir: &str) {
    // remove data dir
    std::fs::remove_dir_all(data_dir).unwrap();
}
