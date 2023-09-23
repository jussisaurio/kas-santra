# Kas, Santra!

Kas, Santra! is an educational reimplementation of [Cassandra](https://cassandra.apache.org/) written in Rust. It is a work in progress.

Features so far:

- Memtables
- SSTables
- Wal
- Basic TCP server
- Crappy makeshift-CQL parser
- Facilities for flushing memtables to SSTables
- Facilities for compacting SSTables

Todo:

- [ ] Implement a proper CQL parser
- [ ] Implement a proper CQL query executor
- [ ] Implement automatic memtable flushing
- [ ] Implement automatic SSTable compaction
- [ ] Implement support for multiple columns instead of just a key-value pair
- [ ] Implement support for multiple tables
- [ ] Implement primary key support
- [ ] Implement clustering key support
- [ ] Implement partitioning
- [ ] Implement replication
- [ ] Implement gossip