use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, Result, Lines, BufReader, BufRead};

pub struct Wal {
    file: File,
    path: String,
}

impl Wal {
    pub fn new(path: &str) -> Wal {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        Wal { file, path: path.to_string() }
    }

    pub fn from_path(path: &str) -> Wal {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();

        Wal { file, path: path.to_string() }
    }

    pub fn path(&self) -> String {
        self.path.clone()
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        let result = self.file.write_all(data);
        result
    }

    pub fn get_line_iterator(&mut self) -> Lines<BufReader<File>> {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        let reader = BufReader::new(self.file.try_clone().unwrap());
        reader.lines()
    }

    pub fn read(&mut self, offset: u64, size: usize) -> Vec<u8> {
        let mut buf = vec![0; size];
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.read_exact(&mut buf).unwrap();
        buf
    }

    pub fn clear(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        Ok(())
    }
}